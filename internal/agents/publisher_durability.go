package agents

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

const publisherCheckpointKey = "checkpoint"
const publisherLease = 1200 * time.Millisecond

// A single CAS record is both the publication outbox and the owner fence.
// Unlike live_state, its bucket never expires. Pending always has one immutable
// payload and expected DELTA stream sequence, even across ownership changes.
type publisherCheckpoint struct {
	Owner            string                       `json:"owner"`
	LeaseUntil       int64                        `json:"lease_until"`
	Snapshot         []byte                       `json:"snapshot"`
	Pending          []byte                       `json:"pending,omitempty"`
	StreamSeq        uint64                       `json:"stream_seq"`
	Revisions        map[string]uint64            `json:"revisions,omitempty"`
	PendingRevisions map[string]uint64            `json:"pending_revisions,omitempty"`
	Scores           map[string]*dorv1.MatchScore `json:"scores,omitempty"`
	PendingScores    map[string]*dorv1.MatchScore `json:"pending_scores,omitempty"`
	Chunks           []string                     `json:"chunks,omitempty"`
	Hash             string                       `json:"hash,omitempty"`
	Size             int                          `json:"size,omitempty"`
}

type publisherBatch struct {
	data      []byte
	deltas    []*dorv1.Delta
	waiters   []chan struct{}
	reserved  bool
	published bool
	streamSeq uint64
	snapshot  []byte
	revisions map[string]uint64
	scores    map[string]*dorv1.MatchScore
}

// Compression keeps whole-state checkpoints below NATS' default payload limit
// for thousands of stories, without changing the public snapshot protocol.
func encodePublisherCheckpoint(cp publisherCheckpoint) ([]byte, error) {
	data, err := json.Marshal(cp)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func decodePublisherCheckpoint(data []byte, cp *publisherCheckpoint) error {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return err
		}
		defer reader.Close()
		data, err = io.ReadAll(io.LimitReader(reader, 64*1024*1024+1))
		if len(data) > 64*1024*1024 {
			return errors.New("publisher: checkpoint exceeds64MiB")
		}
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(data, cp)
}

func (c *publisherCore) accept(ctx context.Context, apply func()) error {
	return c.acceptInput(ctx, nil, "", apply)
}

// The admission lock orders state mutation and barrier registration together.
// Revisions are per entity AND source stream; a newer unrelated entity never
// suppresses an older unseen one. Separate clean/placed keys retain precedence.
func (c *publisherCore) acceptInput(ctx context.Context, msg *Message, key string, apply func()) error {
	c.ingestMu.Lock()
	accepted := true
	if msg != nil && msg.StreamSequence > 0 {
		key = bus.StreamForSubject(msg.Subject) + "/" + key
		c.mu.Lock()
		if msg.StreamSequence <= c.committedRevisions[key] {
			c.mu.Unlock()
			c.ingestMu.Unlock()
			return nil
		}
		accepted = msg.StreamSequence > c.revisions[key]
		if accepted {
			c.revisions[key] = msg.StreamSequence
		}
		c.mu.Unlock()
	}
	if accepted {
		apply()
	}
	c.mu.Lock()
	done := make(chan struct{})
	c.waiters = append(c.waiters, done)
	c.mu.Unlock()
	c.ingestMu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return Retry(ctx.Err())
	}
}

func cloneScores(scores map[string]*dorv1.MatchScore) map[string]*dorv1.MatchScore {
	result := make(map[string]*dorv1.MatchScore, len(scores))
	for id, score := range scores {
		result[id] = proto.Clone(score).(*dorv1.MatchScore)
	}
	return result
}

func (c *publisherCore) restore(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	kv, err := c.bus.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	owner := nuid.Next()
	deadline := time.Now().Add(publisherLease + 200*time.Millisecond)
	var cp publisherCheckpoint
	for {
		entry, getErr := kv.Get(ctx, publisherCheckpointKey)
		var revision uint64
		cp = publisherCheckpoint{}
		if getErr == nil {
			if err := decodePublisherCheckpoint(entry.Value(), &cp); err != nil {
				return fmt.Errorf("publisher checkpoint: %w", err)
			}
			revision = entry.Revision()
			if cp.Owner != "" && cp.LeaseUntil > time.Now().UnixNano() {
				if time.Now().After(deadline) {
					return fmt.Errorf("publisher: owner %s holds the publication lease", cp.Owner)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
		} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return getErr
		}
		if revision != 0 {
			if err := c.hydrateCheckpoint(ctx, kv, &cp); err != nil {
				// A reader delayed beyond chunk retention may have an obsolete root.
				// Re-read only when its revision changed; corruption of the current
				// generation remains a hard restore error, preserving the good cache.
				if latest, getErr := kv.Get(ctx, publisherCheckpointKey); getErr == nil && latest.Revision() != revision {
					continue
				}
				return err
			}
		}
		if revision == 0 {
			snap, streamSeq, err := c.restoreLegacy(ctx)
			if err != nil {
				return err
			}
			cp.Snapshot, err = live.MarshalSnapshot(snap)
			if err != nil {
				return err
			}
			cp.StreamSeq = streamSeq
		}
		var state dorv1.Snapshot
		if err := live.UnmarshalSnapshot(cp.Snapshot, &state); err != nil {
			return fmt.Errorf("publisher: checkpoint snapshot: %w", err)
		}
		cp.Snapshot, err = live.MarshalSnapshot(&state)
		if err != nil {
			return err
		}
		cp.Owner, cp.LeaseUntil = owner, time.Now().Add(publisherLease).UnixNano()
		data, staged, stageErr := c.stageCheckpoint(ctx, kv, cp)
		if stageErr != nil {
			return stageErr
		}
		cp = staged
		if revision == 0 {
			revision, err = kv.Create(ctx, publisherCheckpointKey, data)
		} else {
			revision, err = kv.Update(ctx, publisherCheckpointKey, data, revision)
		}
		if err != nil {
			return fmt.Errorf("publisher: claim owner: %w", err)
		}
		c.owner, c.revision, c.checkpoint = owner, revision, cp
		break
	}
	var snap dorv1.Snapshot
	if err := live.UnmarshalSnapshot(cp.Snapshot, &snap); err != nil {
		return fmt.Errorf("publisher: restore snapshot: %w", err)
	}
	c.mu.Lock()
	c.revisions, c.committedRevisions = maps.Clone(cp.Revisions), maps.Clone(cp.Revisions)
	if c.revisions == nil {
		c.revisions = make(map[string]uint64)
	}
	if c.committedRevisions == nil {
		c.committedRevisions = make(map[string]uint64)
	}
	c.scores = cloneScores(cp.Scores)
	c.placed = make(map[string]bool)
	if cp.PendingRevisions != nil {
		c.revisions = maps.Clone(cp.PendingRevisions)
		c.scores = cloneScores(cp.PendingScores)
	}
	c.seq = snap.Seq
	c.pending, c.order, c.waiters = make(map[string]*dorv1.Delta), nil, nil
	c.stories, c.links, c.wireRows = make(map[string]*dorv1.Story), make(map[string]*dorv1.Link), make(map[string]*dorv1.WireRow)
	c.wire, c.wireOrder = nil, nil
	for _, story := range snap.Stories {
		c.stories[story.Id] = story
	}
	for _, link := range snap.Links {
		c.links[link.A+"|"+link.B] = link
	}
	for _, row := range snap.RecentWire {
		c.wire = append(c.wire, row)
		c.wireRows[row.ArticleId] = row
		c.wireOrder = append(c.wireOrder, row.ArticleId)
	}
	c.mu.Unlock()
	c.inflight = nil
	if len(cp.Pending) > 0 || cp.PendingRevisions != nil {
		var batch dorv1.DeltaBatch
		if err := proto.Unmarshal(cp.Pending, &batch); err != nil {
			return err
		}
		next := snap.Seq + 1
		for _, d := range batch.Deltas {
			if d.Seq != next {
				return fmt.Errorf("publisher: invalid checkpoint delta %d, want %d", d.Seq, next)
			}
			next++
		}
		c.inflight = &publisherBatch{data: cp.Pending, deltas: batch.Deltas, reserved: true, revisions: maps.Clone(cp.PendingRevisions), scores: cloneScores(cp.PendingScores)}
		for _, d := range batch.Deltas {
			if row := d.GetWire(); row != nil {
				c.wireRows[row.ArticleId] = row
			}
		}
	}
	for key := range c.revisions {
		if strings.HasPrefix(key, bus.StreamEnrich+"/placed:") {
			id := strings.TrimPrefix(key, bus.StreamEnrich+"/placed:")
			if c.wireRows[id] != nil {
				c.placed[id] = true
			}
		}
	}
	c.restored = true
	c.fenced.Store(false)
	// Restore before consumers or gateway startup. A pending publication is retried
	// by the flush loop; its inputs remain unacknowledged in their source streams.
	return c.writeSnapshotLocked(ctx)
}

// Upgrade recovery: the old publisher acknowledged between snapshots. Replay
// retained output after the snapshot rather than jumping only the watermark.
func (c *publisherCore) restoreLegacy(ctx context.Context) (*dorv1.Snapshot, uint64, error) {
	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return nil, 0, err
	}
	snap := &dorv1.Snapshot{}
	entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err == nil {
		if err = live.UnmarshalSnapshot(entry.Value(), snap); err != nil {
			return nil, 0, err
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, err
	}
	c.mu.Lock()
	c.seq = snap.Seq
	c.stories, c.links = make(map[string]*dorv1.Story), make(map[string]*dorv1.Link)
	c.wire = nil
	for _, s := range snap.Stories {
		c.stories[s.Id] = s
	}
	for _, l := range snap.Links {
		c.links[l.A+"|"+l.B] = l
	}
	for _, w := range snap.RecentWire {
		c.applyDeltaLocked(&dorv1.Delta{Body: &dorv1.Delta_Wire{Wire: w}})
	}
	c.mu.Unlock()
	stream, err := c.bus.StreamNamed(ctx, bus.StreamDelta)
	if err != nil {
		return nil, 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, 0, err
	}
	for seq := info.State.FirstSeq; seq > 0 && seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		var batch dorv1.DeltaBatch
		if err := proto.Unmarshal(msg.Data, &batch); err != nil {
			return nil, 0, err
		}
		for _, d := range batch.Deltas {
			if d.Seq <= c.seq {
				continue
			}
			if d.Seq != c.seq+1 {
				return nil, 0, fmt.Errorf("publisher: retained output cannot cover snapshot %d (next %d)", c.seq, d.Seq)
			}
			c.applyDeltaLocked(d)
			c.seq = d.Seq
		}
	}
	// A legacy watermark beyond the available snapshot/output must fail closed.
	if entry, err := kv.Get(ctx, bus.KVPublisherSeqKey); err == nil {
		var watermark uint64
		if _, err := fmt.Sscanf(string(entry.Value()), "%d", &watermark); err != nil {
			return nil, 0, err
		}
		if watermark > c.seq {
			return nil, 0, errors.New("publisher: legacy checkpoint is ahead of recoverable state")
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, err
	}
	return c.snapshotLocked(), info.State.LastSeq, nil
}

// saveCheckpoint verifies owner and CASes the whole record. A delayed old owner
// cannot reserve or commit any subsequent sequence once a takeover wins CAS.
func (c *publisherCore) saveCheckpoint(ctx context.Context, cp publisherCheckpoint) error {
	kv, err := c.bus.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	entry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		return err
	}
	var current publisherCheckpoint
	if err := decodePublisherCheckpoint(entry.Value(), &current); err != nil {
		return err
	}
	if current.Owner != c.owner {
		c.fenced.Store(true)
		return errors.New("publisher: publication owner fenced")
	}
	cp.Owner, cp.LeaseUntil = c.owner, time.Now().Add(publisherLease).UnixNano()
	data, staged, err := c.stageCheckpoint(ctx, kv, cp)
	if err != nil {
		return err
	}
	cp = staged
	revision, err := kv.Update(ctx, publisherCheckpointKey, data, entry.Revision())
	if err != nil {
		return err
	}
	c.revision, c.checkpoint = revision, cp
	if time.Since(c.lastChunkGC) >= publisherChunkGrace {
		c.lastChunkGC = time.Now()
		if err := c.gcChunks(ctx, kv); err != nil {
			c.log.Debug("publisher: chunk GC deferred", "err", err)
		}
	}
	return nil
}

func (c *publisherCore) release(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if !c.restored {
		return nil
	}
	cp := c.checkpoint
	// Keep checkpoint/outbox; release only the ownership claim.
	kv, err := c.bus.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	entry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		return err
	}
	var current publisherCheckpoint
	if err := decodePublisherCheckpoint(entry.Value(), &current); err != nil {
		return err
	}
	if current.Owner != c.owner {
		return nil
	}
	cp.Owner, cp.LeaseUntil = "", 0
	data, _, stageErr := c.stageCheckpoint(ctx, kv, cp)
	if stageErr != nil {
		return stageErr
	}
	_, err = kv.Update(ctx, publisherCheckpointKey, data, entry.Revision())
	c.restored = false
	return err
}

func (c *publisherCore) flush(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if !c.restored {
		return errors.New("publisher: state not restored")
	}
	c.ingestMu.Lock()
	c.mu.Lock()
	if c.inflight == nil && (len(c.pending) > 0 || len(c.waiters) > 0) {
		batch := &publisherBatch{waiters: c.waiters, revisions: maps.Clone(c.revisions), scores: cloneScores(c.scores)}
		next := c.seq
		for _, key := range c.order {
			d := proto.Clone(c.pending[key]).(*dorv1.Delta)
			next++
			d.Seq = next
			quantizeDelta(d)
			batch.deltas = append(batch.deltas, d)
		}
		var err error
		batch.data, err = proto.MarshalOptions{Deterministic: true}.Marshal(&dorv1.DeltaBatch{Deltas: batch.deltas})
		if err != nil {
			c.mu.Unlock()
			c.ingestMu.Unlock()
			return err
		}
		c.inflight = batch
		c.pending, c.order, c.waiters = make(map[string]*dorv1.Delta), nil, nil
	}
	c.mu.Unlock()
	c.ingestMu.Unlock()
	batch := c.inflight
	if batch == nil {
		return c.saveCheckpoint(ctx, c.checkpoint)
	}
	if !batch.reserved {
		cp := c.checkpoint
		cp.Pending = batch.data
		cp.PendingRevisions, cp.PendingScores = batch.revisions, batch.scores
		if err := c.saveCheckpoint(ctx, cp); err != nil {
			return err
		}
		batch.reserved = true
	} else if err := c.saveCheckpoint(ctx, c.checkpoint); err != nil {
		return err
	}
	if !batch.published {
		last := c.seq
		if len(batch.deltas) > 0 {
			last = batch.deltas[len(batch.deltas)-1].Seq
		}
		id := fmt.Sprintf("delta:%d", last)
		pubCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		var err error
		if len(batch.deltas) > 0 {
			err = c.publishBatch(pubCtx, batch, id)
		} else {
			batch.streamSeq = c.checkpoint.StreamSeq
		}
		cancel()
		if err != nil {
			return fmt.Errorf("publisher: publish %s: %w", id, err)
		}
		c.mu.Lock()
		for _, d := range batch.deltas {
			c.applyDeltaLocked(d)
		}
		c.seq = last
		snap := c.snapshotLocked()
		c.mu.Unlock()
		batch.snapshot, err = live.MarshalSnapshot(snap)
		if err != nil {
			return err
		}
		batch.published = true
	}
	// Persist the public snapshot before acknowledging the inputs. If either KV
	// write fails, retain the exact batch and retry this commit, not its successors.
	if err := c.writeSnapshotLocked(ctx); err != nil {
		return err
	}
	cp := c.checkpoint
	cp.Snapshot, cp.Pending, cp.StreamSeq = batch.snapshot, nil, batch.streamSeq
	cp.Revisions, cp.Scores = batch.revisions, batch.scores
	cp.PendingRevisions, cp.PendingScores = nil, nil
	if err := c.saveCheckpoint(ctx, cp); err != nil {
		return err
	}
	c.mu.Lock()
	c.committedRevisions = maps.Clone(batch.revisions)
	c.mu.Unlock()
	for _, done := range batch.waiters {
		close(done)
	}
	c.inflight = nil
	return nil
}

func (c *publisherCore) publishBatch(ctx context.Context, batch *publisherBatch, id string) error {
	if c.publish != nil { // fault-injection seam exercises the real retry/outbox path
		if err := c.publish(ctx, batch.data, id); err != nil {
			return err
		}
	}
	// DELTA has a single subject. Stream-level CAS persists beyond message age and
	// the dedupe window: an in-flight stale owner cannot append behind a successor.
	header := nats.Header{}
	header.Set(jetstream.ExpectedLastSeqHeader, fmt.Sprint(c.checkpoint.StreamSeq))
	ack, err := c.bus.Publish(ctx, bus.SubjectDeltaOut, batch.data, bus.WithMsgID(id), bus.WithHeaders(header))
	if err == nil {
		batch.streamSeq = ack.Sequence
		return nil
	}
	// A lost PubAck may outlive the dedupe window. Reconcile the reserved stream
	// position by exact bytes, never skip forward over an unknown publication.
	stream, getErr := c.bus.StreamNamed(ctx, bus.StreamDelta)
	if getErr != nil {
		return err
	}
	msg, getErr := stream.GetMsg(ctx, c.checkpoint.StreamSeq+1)
	if getErr == nil && bytes.Equal(msg.Data, batch.data) {
		batch.streamSeq = msg.Sequence
		return nil
	}
	if errors.Is(getErr, jetstream.ErrMsgNotFound) {
		info, infoErr := stream.Info(ctx)
		if infoErr == nil && info.State.LastSeq == c.checkpoint.StreamSeq+1 {
			batch.streamSeq = info.State.LastSeq
			return nil
		}
	}
	return err
}

func (c *publisherCore) flushLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	delay := every
	next := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Now().Before(next) {
				continue
			}
			if err := c.flush(ctx); err != nil {
				c.log.Warn("publisher: flush will retry", "err", err)
				next = time.Now().Add(delay)
				delay = min(delay*2, 500*time.Millisecond)
			} else {
				delay, next = every, time.Time{}
			}
		}
	}
}

func (c *publisherCore) snapshotLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.writeSnapshot(ctx); err != nil {
				c.log.Warn("publisher: snapshot will retry", "err", err)
			}
			if err := c.pruneInputRevisions(ctx); err != nil {
				c.log.Debug("publisher: input revision pruning deferred", "err", err)
			}
		}
	}
}

func (c *publisherCore) applyDeltaLocked(d *dorv1.Delta) {
	switch body := d.Body.(type) {
	case *dorv1.Delta_StoryUpsert:
		if s := body.StoryUpsert; s != nil {
			c.stories[s.Id] = s
		}
	case *dorv1.Delta_StoryExpire:
		delete(c.stories, body.StoryExpire)
	case *dorv1.Delta_LinkUpsert:
		if l := body.LinkUpsert; l != nil {
			c.links[l.A+"|"+l.B] = l
		}
	case *dorv1.Delta_LinkRemove:
		if l := body.LinkRemove; l != nil {
			delete(c.links, l.A+"|"+l.B)
		}
	case *dorv1.Delta_Wire:
		if row := body.Wire; row != nil {
			for i, prev := range c.wire {
				if prev.ArticleId == row.ArticleId && c.wireRows[row.ArticleId] == prev {
					c.wireRows[row.ArticleId] = row
				}
				if prev.ArticleId == row.ArticleId {
					c.wire = append(c.wire[:i], c.wire[i+1:]...)
					break
				}
			}
			if c.wireRows[row.ArticleId] == nil {
				c.wireRows[row.ArticleId] = row
				c.wireOrder = append(c.wireOrder, row.ArticleId)
			}
			c.wire = append(c.wire, row)
			if len(c.wire) > publisherWireHistory {
				c.wire = append([]*dorv1.WireRow(nil), c.wire[len(c.wire)-publisherWireHistory:]...)
			}
		}
	}
}

func quantizeDelta(d *dorv1.Delta) {
	if s := d.GetStoryUpsert(); s != nil {
		for _, loc := range s.Locations {
			if loc != nil {
				loc.Lat = float32(quantize(float64(loc.Lat)))
				loc.Lon = float32(quantize(float64(loc.Lon)))
			}
		}
	}
	if row := d.GetWire(); row != nil {
		row.Lat, row.Lon = quantize(row.Lat), quantize(row.Lon)
	}
}

func quantize(v float64) float64 { return math.Round(v*1e5) / 1e5 }

// snapshotLocked includes weak links for internal recovery. Public projection
// happens only at write time, so persistence does not change the entity state.
func (c *publisherCore) snapshotLocked() *dorv1.Snapshot {
	snap := &dorv1.Snapshot{Seq: c.seq, BuiltAt: c.now().Unix(), RecentWire: c.wire}
	for _, s := range c.stories {
		snap.Stories = append(snap.Stories, s)
	}
	for _, l := range c.links {
		snap.Links = append(snap.Links, l)
	}
	sort.Slice(snap.Stories, func(i, j int) bool { return snap.Stories[i].Id < snap.Stories[j].Id })
	sort.Slice(snap.Links, func(i, j int) bool { a, b := snap.Links[i], snap.Links[j]; return a.A+"|"+a.B < b.A+"|"+b.B })
	return snap
}

func (c *publisherCore) writeSnapshot(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	return c.writeSnapshotLocked(ctx)
}

func (c *publisherCore) writeSnapshotLocked(ctx context.Context) error {
	if !c.restored {
		return errors.New("publisher: refusing snapshot before state restoration")
	}
	c.mu.Lock()
	snap := proto.Clone(c.snapshotLocked()).(*dorv1.Snapshot)
	c.mu.Unlock()
	links := snap.Links[:0]
	for _, l := range snap.Links {
		if l.Strength >= 0.3 {
			links = append(links, l)
		}
	}
	snap.Links = links
	for _, s := range snap.Stories {
		quantizeDelta(&dorv1.Delta{Body: &dorv1.Delta_StoryUpsert{StoryUpsert: s}})
	}
	for _, row := range snap.RecentWire {
		quantizeDelta(&dorv1.Delta{Body: &dorv1.Delta_Wire{Wire: row}})
	}
	data, err := live.MarshalSnapshot(snap)
	if err != nil {
		return err
	}
	if err := c.checkPayload("snapshot", data); err != nil {
		return err
	}
	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return err
	}
	for {
		entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
		if err == nil {
			var prev dorv1.Snapshot
			if err := live.UnmarshalSnapshot(entry.Value(), &prev); err != nil {
				return err
			}
			if prev.Seq > snap.Seq {
				return nil
			}
			if prev.Seq == snap.Seq && len(prev.Stories) > 0 && len(snap.Stories) == 0 {
				return errors.New("publisher: refusing empty snapshot over live state")
			}
			_, err = kv.Update(ctx, bus.KVLiveStateSnapshotKey, data, entry.Revision())
		} else if errors.Is(err, jetstream.ErrKeyNotFound) {
			_, err = kv.Create(ctx, bus.KVLiveStateSnapshotKey, data)
		}
		if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		return err
	}
}

// Include space for KV publish headers. Oversized state leaves the last good
// snapshot/checkpoint untouched and its input acknowledgements pending.
func (c *publisherCore) checkPayload(kind string, data []byte) error {
	limit := c.bus.NC().MaxPayload()
	if int64(len(data))+1024 > limit {
		return fmt.Errorf("publisher: %s storage payload %d bytes exceeds NATS limit %d (including header allowance)", kind, len(data), limit)
	}
	return nil
}
