package publication

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

const RootKey = "checkpoint"
const ChunkSize = 256 * 1024
const StateLimit = 64 * 1024 * 1024
const BucketLimit = 1 << 30
const Grace = time.Minute
const MaxEntries = 512
const maxRetired = 2048

var ErrFenced = errors.New("publication: owner fenced")
var ErrOwned = errors.New("publication: active owner")
var ErrBackpressure = errors.New("publication: compaction/retirement backpressure")

type Blob struct {
	ID    string
	Hash  string
	Size  int
	Count int
}
type Entry struct {
	Blob    Blob
	Ordinal uint64
	Seq     uint64
	Raw     uint64
}
type Retired struct {
	Blob   Blob
	At     int64
	Cursor int
}
type Root struct {
	Version    int       `json:"version"`
	Owner      string    `json:"owner"`
	LeaseUntil int64     `json:"lease_until"`
	Base       Entry     `json:"base"`
	Head       uint64    `json:"head"`
	Seq        uint64    `json:"seq"`
	Raw        uint64    `json:"raw"`
	Entries    []Entry   `json:"entries,omitempty"`
	Pending    *Entry    `json:"pending,omitempty"`
	Staging    []Retired `json:"staging,omitempty"`
	Retired    []Retired `json:"retired,omitempty"`
	Cache      *Entry    `json:"cache,omitempty"`
}

func DecodeRoot(data []byte) (Root, error) {
	var r Root
	if len(data) > 512*1024 {
		return r, errors.New("publication: root too large")
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, err
	}
	if r.Version != 2 {
		return r, errors.New("publication: unsupported root version")
	}
	if len(r.Entries) > MaxEntries || len(r.Retired) > maxRetired || len(r.Staging) > 4 || r.Base.Ordinal > r.Head || r.Base.Seq > r.Seq || r.Base.Raw > r.Raw {
		return r, errors.New("publication: root bounds")
	}
	if err := validateBlob(r.Base.Blob); err != nil {
		return r, err
	}
	last := r.Base.Ordinal
	seq, raw := r.Base.Seq, r.Base.Raw
	for _, e := range r.Entries {
		if e.Ordinal != last+1 {
			return r, errors.New("publication: root journal gap")
		}
		if err := validateBlob(e.Blob); err != nil {
			return r, err
		}
		last = e.Ordinal
		if e.Seq < seq || e.Seq == seq && e.Raw != raw || e.Seq > seq && e.Raw != raw+1 {
			return r, errors.New("publication: root sequence bounds")
		}
		seq, raw = e.Seq, e.Raw
	}
	if last != r.Head || seq != r.Seq || raw != r.Raw {
		return r, errors.New("publication: root head mismatch")
	}
	if r.Pending != nil {
		if r.Pending.Ordinal != r.Head+1 {
			return r, errors.New("publication: pending ordinal")
		}
		if err := validateBlob(r.Pending.Blob); err != nil {
			return r, err
		}
	}
	if r.Cache != nil {
		if err := validateBlob(r.Cache.Blob); err != nil {
			return r, err
		}
		if r.Cache.Seq > r.Seq {
			return r, errors.New("publication: cache ahead of authority")
		}
	}
	ids := map[string]bool{r.Base.Blob.ID: true}
	for _, e := range r.Entries {
		if ids[e.Blob.ID] {
			return r, errors.New("publication: duplicate content reference")
		}
		ids[e.Blob.ID] = true
	}
	if r.Pending != nil {
		if ids[r.Pending.Blob.ID] {
			return r, errors.New("publication: duplicate pending reference")
		}
		ids[r.Pending.Blob.ID] = true
	}
	if r.Cache != nil {
		if ids[r.Cache.Blob.ID] {
			return r, errors.New("publication: duplicate cache reference")
		}
		ids[r.Cache.Blob.ID] = true
	}
	for _, v := range append(append([]Retired(nil), r.Staging...), r.Retired...) {
		if ids[v.Blob.ID] {
			return r, errors.New("publication: active/retired content overlap")
		}
		ids[v.Blob.ID] = true
		if err := validateBlob(v.Blob); err != nil {
			return r, err
		}
		if v.Cursor < 0 || v.Cursor > v.Blob.Count {
			return r, errors.New("publication: retirement cursor")
		}
	}
	return r, nil
}
func validateBlob(b Blob) error {
	if len(b.ID) != 22 || len(b.Hash) != 64 || b.Size <= 0 || b.Size > StateLimit || b.Count != (b.Size+ChunkSize-1)/ChunkSize {
		return errors.New("publication: invalid blob descriptor")
	}
	for _, c := range b.ID {
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return errors.New("publication: invalid content id")
		}
	}
	return nil
}
func Encode(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(data) > StateLimit {
		return nil, errors.New("publication: decoded state limit")
	}
	var out bytes.Buffer
	w, _ := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if _, err = w.Write(data); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func Decode(data []byte, v any) error {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer r.Close()
	data, err = io.ReadAll(io.LimitReader(r, StateLimit+1))
	if err != nil {
		return err
	}
	if len(data) > StateLimit {
		return errors.New("publication: decoded state limit")
	}
	return json.Unmarshal(data, v)
}
func blobKey(b Blob, i int) string { return fmt.Sprintf("v2.%s.%d", b.ID, i) }
func newBlob(data []byte) Blob {
	return Blob{ID: nuid.Next(), Hash: fmt.Sprintf("%x", sha256.Sum256(data)), Size: len(data), Count: (len(data) + ChunkSize - 1) / ChunkSize}
}

type Store struct {
	KV        jetstream.KeyValue
	Owner     string
	Lease     time.Duration
	Limit     int64
	Now       func() time.Time
	mu        sync.Mutex
	lastSweep time.Time
	Observe   func(string, time.Duration, int)
}

func (s *Store) observe(kind string, start time.Time, n int) {
	if s.Observe != nil {
		s.Observe(kind, time.Since(start), n)
	}
}

func New(kv jetstream.KeyValue, owner string, lease time.Duration, limit int64) *Store {
	return &Store{KV: kv, Owner: owner, Lease: lease, Limit: limit, Now: time.Now}
}
func (s *Store) ReadRoot(ctx context.Context) (Root, uint64, error) {
	e, err := s.KV.Get(ctx, RootKey)
	if err != nil {
		return Root{}, 0, err
	}
	r, err := DecodeRoot(e.Value())
	return r, e.Revision(), err
}
func (s *Store) update(ctx context.Context, mutate func(*Root) error) (Root, error) {
	for {
		r, rev, err := s.ReadRoot(ctx)
		if err != nil {
			return r, err
		}
		if r.Owner != s.Owner {
			return r, ErrFenced
		}
		if err = mutate(&r); err != nil {
			return r, err
		}
		r.LeaseUntil = s.Now().Add(s.Lease).UnixNano()
		data, err := json.Marshal(r)
		if err != nil {
			return r, err
		}
		if int64(len(data))+1024 > s.Limit {
			return r, errors.New("publication: root payload limit")
		}
		if _, err = DecodeRoot(data); err != nil {
			return r, err
		}
		started := time.Now()
		_, err = s.KV.Update(ctx, RootKey, data, rev)
		s.observe("root_write", started, len(data))
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		return r, err
	}
}
func (s *Store) Renew(ctx context.Context) error {
	started := time.Now()
	defer s.observe("lease", started, 0)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.update(ctx, func(*Root) error { return nil })
	return err
}
func (s *Store) Claim(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, rev, err := s.ReadRoot(ctx)
	if err != nil {
		return err
	}
	if r.Owner != "" && r.Owner != s.Owner && r.LeaseUntil > s.Now().UnixNano() {
		return ErrOwned
	}
	r.Owner, r.LeaseUntil = s.Owner, s.Now().Add(s.Lease).UnixNano()
	for _, b := range r.Staging {
		r.Retired = append(r.Retired, Retired{Blob: b.Blob, At: s.Now().UnixNano()})
	}
	r.Staging = nil
	data, _ := json.Marshal(r)
	_, err = s.KV.Update(ctx, RootKey, data, rev)
	return err
}
func (s *Store) Release(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.update(ctx, func(r *Root) error { r.Owner = ""; r.LeaseUntil = 0; return nil })
	return err
}
func (s *Store) writeBlob(ctx context.Context, b Blob, data []byte) error {
	for i := 0; i < b.Count; i++ {
		if _, err := s.KV.Create(ctx, blobKey(b, i), data[i*ChunkSize:min((i+1)*ChunkSize, len(data))]); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) ReadBlob(ctx context.Context, b Blob) ([]byte, error) {
	if err := validateBlob(b); err != nil {
		return nil, err
	}
	data := make([]byte, 0, b.Size)
	for i := 0; i < b.Count; i++ {
		e, err := s.KV.Get(ctx, blobKey(b, i))
		if err != nil {
			return nil, err
		}
		if len(e.Value()) != min(ChunkSize, b.Size-len(data)) {
			return nil, errors.New("publication: chunk length")
		}
		data = append(data, e.Value()...)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != b.Hash {
		return nil, errors.New("publication: chunk hash")
	}
	return data, nil
}

// Initialize is used once after validated legacy recovery. Root installation is
// atomic; a failed CAS never damages the old authority. A small staging intent
// makes pre-root interrupted initialization chunks discoverable to housekeeping.
func (s *Store) Initialize(ctx context.Context, state *State, previousRevision uint64, pending *Record) error {
	data, err := Encode(state)
	if err != nil {
		return err
	}
	b := newBlob(data)
	intent, _ := json.Marshal(Retired{Blob: b, At: s.Now().UnixNano()})
	if _, err = s.KV.Create(ctx, "stage."+b.ID, intent); err != nil {
		return err
	}
	if err = s.writeBlob(ctx, b, data); err != nil {
		return err
	}
	r := Root{Version: 2, Owner: s.Owner, LeaseUntil: s.Now().Add(s.Lease).UnixNano(), Base: Entry{Blob: b, Ordinal: state.Ordinal, Seq: state.Seq, Raw: state.Raw}, Head: state.Ordinal, Seq: state.Seq, Raw: state.Raw}
	if pending != nil {
		pdata, err := Encode(pending)
		if err != nil {
			return err
		}
		p := newBlob(pdata)
		intent, _ := json.Marshal(Retired{Blob: p, At: s.Now().UnixNano()})
		if _, err = s.KV.Create(ctx, "stage."+p.ID, intent); err != nil {
			return err
		}
		if err = s.writeBlob(ctx, p, pdata); err != nil {
			return err
		}
		r.Pending = &Entry{Blob: p, Ordinal: pending.Ordinal}
	}
	root, _ := json.Marshal(r)
	if previousRevision == 0 {
		_, err = s.KV.Create(ctx, RootKey, root)
	} else {
		_, err = s.KV.Update(ctx, RootKey, root, previousRevision)
	}
	if err == nil {
		_, err = s.KV.Create(ctx, "layout", []byte("2"))
		if errors.Is(err, jetstream.ErrKeyExists) {
			err = nil
		}
	}
	return err
}
func (s *Store) stage(ctx context.Context, data []byte) (Blob, error) {
	if len(data) > StateLimit {
		return Blob{}, errors.New("publication: blob limit")
	}
	b := newBlob(data)
	_, err := s.update(ctx, func(r *Root) error {
		if len(r.Staging) >= 4 || len(r.Retired) >= maxRetired-8 {
			return ErrBackpressure
		}
		r.Staging = append(r.Staging, Retired{Blob: b, At: s.Now().UnixNano()})
		return nil
	})
	if err != nil {
		return b, err
	}
	return b, s.writeBlob(ctx, b, data)
}

func (s *Store) abandon(ctx context.Context, b Blob) {
	if b.ID == "" {
		return
	}
	_, _ = s.update(ctx, func(r *Root) error {
		for _, v := range r.Staging {
			if v.Blob.ID == b.ID {
				removeStage(r, b)
				r.Retired = append(r.Retired, Retired{Blob: b, At: s.Now().UnixNano()})
				break
			}
		}
		return nil
	})
}
func removeStage(r *Root, b Blob) {
	for i, v := range r.Staging {
		if v.Blob.ID == b.ID {
			r.Staging = append(r.Staging[:i], r.Staging[i+1:]...)
			return
		}
	}
}
func (s *Store) Reserve(ctx context.Context, record *Record) error {
	started := time.Now()
	data, err := Encode(record)
	s.observe("journal_encode", started, len(data))
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, _, err := s.ReadRoot(ctx)
	if err != nil {
		return err
	}
	if root.Owner != s.Owner {
		return ErrFenced
	}
	if root.Pending != nil {
		if root.Pending.Ordinal == record.Ordinal && root.Pending.Blob.Hash == fmt.Sprintf("%x", sha256.Sum256(data)) {
			return nil
		}
		return errors.New("publication: outbox already reserved")
	}
	if len(root.Entries) >= MaxEntries {
		return ErrBackpressure
	}
	status, err := s.KV.Status(ctx)
	if err != nil {
		return err
	}
	s.observe("bucket", time.Now(), int(status.Bytes()))
	if status.Bytes() > BucketLimit*3/4 {
		return ErrBackpressure
	}
	b, err := s.stage(ctx, data)
	if err != nil {
		s.abandon(ctx, b)
		return err
	}
	_, err = s.update(ctx, func(r *Root) error {
		if r.Pending != nil || r.Head+1 != record.Ordinal || r.Seq != record.PreviousSeq || r.Raw != record.PreviousRaw {
			return errors.New("publication: stale reservation")
		}
		r.Pending = &Entry{Blob: b, Ordinal: record.Ordinal}
		removeStage(r, b)
		return nil
	})
	if err != nil {
		s.abandon(ctx, b)
	}
	return err
}
func (s *Store) Commit(ctx context.Context, ordinal, seq, raw uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.update(ctx, func(r *Root) error {
		if r.Head == ordinal && r.Seq == seq && r.Raw == raw && r.Pending == nil {
			return nil
		}
		if r.Pending == nil || r.Pending.Ordinal != ordinal {
			return errors.New("publication: commit missing pending")
		}
		data, err := s.ReadBlob(ctx, r.Pending.Blob)
		if err != nil {
			return err
		}
		var record Record
		if err = Decode(data, &record); err != nil {
			return err
		}
		end, wantRaw, err := record.End()
		if err != nil {
			return err
		}
		if record.Ordinal != ordinal || record.PreviousSeq != r.Seq || record.PreviousRaw != r.Raw || end != seq || wantRaw != raw {
			return errors.New("publication: commit watermark does not match pending record")
		}
		e := *r.Pending
		e.Seq, e.Raw = seq, raw
		r.Entries = append(r.Entries, e)
		r.Head, r.Seq, r.Raw = ordinal, seq, raw
		r.Pending = nil
		return nil
	})
	return err
}

func (s *Store) Load(ctx context.Context) (*State, *Record, error) {
	for {
		r, rev, err := s.ReadRoot(ctx)
		if err != nil {
			return nil, nil, err
		}
		state, pending, err := s.load(ctx, r)
		if err == nil {
			return state, pending, nil
		}
		latest, newRev, getErr := s.ReadRoot(ctx)
		if getErr != nil || newRev == rev || latest.Base.Blob.ID == r.Base.Blob.ID && latest.Head == r.Head {
			return nil, nil, err
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
	}
}
func (s *Store) load(ctx context.Context, r Root) (*State, *Record, error) {
	data, err := s.ReadBlob(ctx, r.Base.Blob)
	if err != nil {
		return nil, nil, err
	}
	var state State
	if err = Decode(data, &state); err != nil {
		return nil, nil, err
	}
	if state.Ordinal != r.Base.Ordinal || state.Seq != r.Base.Seq || state.Raw != r.Base.Raw || state.Stories == nil || state.Links == nil || state.Rows == nil || state.Revisions == nil || state.Scores == nil {
		return nil, nil, errors.New("publication: invalid base")
	}
	for _, e := range r.Entries {
		data, err = s.ReadBlob(ctx, e.Blob)
		if err != nil {
			return nil, nil, err
		}
		var record Record
		if err = Decode(data, &record); err != nil {
			return nil, nil, err
		}
		if err = state.Apply(&record, e.Raw); err != nil {
			return nil, nil, err
		}
		if state.Seq != e.Seq || state.Ordinal != e.Ordinal {
			return nil, nil, errors.New("publication: entry watermark")
		}
	}
	if state.Ordinal != r.Head || state.Seq != r.Seq || state.Raw != r.Raw {
		return nil, nil, errors.New("publication: head watermark")
	}
	var pending *Record
	if r.Pending != nil {
		data, err = s.ReadBlob(ctx, r.Pending.Blob)
		if err != nil {
			return nil, nil, err
		}
		pending = &Record{}
		if err = Decode(data, pending); err != nil {
			return nil, nil, err
		}
		if pending.Ordinal != r.Head+1 || pending.PreviousSeq != r.Seq || pending.PreviousRaw != r.Raw {
			return nil, nil, errors.New("publication: pending watermark")
		}
	}
	return &state, pending, nil
}

// Compact encodes a previously captured committed view before taking the store
// lock. Installation preserves all newer committed entries and Pending.
func (s *Store) Compact(ctx context.Context, state *State) error {
	started := time.Now()
	defer s.observe("base_total", started, 0)
	data, err := Encode(state)
	s.observe("base_encode", started, len(data))
	if err != nil {
		return err
	}
	op, done := context.WithTimeout(ctx, 750*time.Millisecond)
	defer done()
	ctx = op
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.stage(ctx, data)
	if err != nil {
		s.abandon(ctx, b)
		return err
	}
	_, err = s.update(ctx, func(r *Root) error {
		if state.Ordinal < r.Base.Ordinal || state.Ordinal > r.Head {
			return errors.New("publication: stale base")
		}
		now := s.Now().UnixNano()
		r.Retired = append(r.Retired, Retired{Blob: r.Base.Blob, At: now})
		entries := r.Entries[:0]
		for _, e := range r.Entries {
			if e.Ordinal <= state.Ordinal {
				r.Retired = append(r.Retired, Retired{Blob: e.Blob, At: now})
			} else {
				entries = append(entries, e)
			}
		}
		r.Entries = entries
		r.Base = Entry{Blob: b, Ordinal: state.Ordinal, Seq: state.Seq, Raw: state.Raw}
		removeStage(r, b)
		return nil
	})
	if err != nil {
		s.abandon(ctx, b)
	}
	return err
}

// GC walks only known retired descriptors; it never fetches chunk values or
// restarts an all-key scan on each lease renewal. Cursor progress is durable.
func (s *Store) GC(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A failed operation can leave a tracked stage when its context expired
	// before cleanup. Store.mu excludes any current local staging writer.
	root, _, err := s.ReadRoot(ctx)
	if err != nil {
		return err
	}
	if root.Owner != s.Owner {
		return ErrFenced
	}
	if len(root.Staging) > 0 {
		if _, err = s.update(ctx, func(r *Root) error {
			for _, v := range r.Staging {
				r.Retired = append(r.Retired, Retired{Blob: v.Blob, At: s.Now().UnixNano()})
			}
			r.Staging = nil
			return nil
		}); err != nil {
			return err
		}
	}
	if s.Now().Sub(s.lastSweep) >= 30*time.Second {
		s.lastSweep = s.Now()
		if err = s.initializationOrphans(ctx); err != nil {
			return err
		}
		// Keys are never reused. Old delete markers are housekeeping, not live
		// authority; deleting them cannot race a newly referenced generation.
		if err = s.KV.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(Grace)); err != nil {
			return err
		}
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		root, _, err := s.ReadRoot(ctx)
		if err != nil {
			return err
		}
		if root.Owner != s.Owner {
			return ErrFenced
		}
		if len(root.Retired) == 0 || s.Now().Sub(time.Unix(0, root.Retired[0].At)) < Grace {
			return nil
		}
		item := root.Retired[0]
		if item.Cursor < item.Blob.Count {
			if err = s.KV.Purge(ctx, blobKey(item.Blob, item.Cursor)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			item.Cursor++
		}
		_, err = s.update(ctx, func(r *Root) error {
			if len(r.Retired) == 0 || r.Retired[0].Blob.ID != item.Blob.ID {
				return errors.New("publication: retirement changed")
			}
			if item.Cursor == item.Blob.Count {
				r.Retired = r.Retired[1:]
			} else {
				r.Retired[0] = item
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}

func (s *Store) initializationOrphans(ctx context.Context) error {
	keys, err := s.KV.ListKeysFiltered(ctx, "stage.>")
	if err != nil {
		return err
	}
	defer keys.Stop()
	for key := range keys.Keys() {
		e, err := s.KV.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var stage Retired
		if err = json.Unmarshal(e.Value(), &stage); err != nil {
			return err
		}
		if err = validateBlob(stage.Blob); err != nil {
			return err
		}
		if s.Now().Sub(time.Unix(0, stage.At)) < Grace {
			continue
		}
		_, err = s.update(ctx, func(r *Root) error {
			protected := r.Base.Blob.ID == stage.Blob.ID
			for _, entry := range r.Entries {
				protected = protected || entry.Blob.ID == stage.Blob.ID
			}
			if r.Pending != nil {
				protected = protected || r.Pending.Blob.ID == stage.Blob.ID
			}
			if r.Cache != nil {
				protected = protected || r.Cache.Blob.ID == stage.Blob.ID
			}
			for _, v := range r.Retired {
				protected = protected || v.Blob.ID == stage.Blob.ID
			}
			if !protected {
				r.Retired = append(r.Retired, stage)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err = s.KV.Purge(ctx, key, jetstream.LastRevision(e.Revision())); err != nil {
			return err
		}
	}
	return ctx.Err()
}
