package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"google.golang.org/protobuf/proto"
)

var errPublicationLayoutChanged = errors.New("publisher: checkpoint upgraded during claim")

func (c *publisherCore) restore(ctx context.Context) error {
	kv, err := c.bus.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	entry, getErr := kv.Get(ctx, publisherCheckpointKey)
	var header struct {
		Version int `json:"version"`
	}
	if getErr == nil {
		_ = json.Unmarshal(entry.Value(), &header)
	} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
		return getErr
	}
	if header.Version != 0 && header.Version != 2 {
		return errors.New("publisher: unsupported checkpoint version")
	}
	if header.Version == 2 {
		store := publication.New(kv, nuid.Next(), publisherLease, c.bus.NC().MaxPayload())
		deadline := time.Now().Add(publisherLease + 200*time.Millisecond)
		for {
			err = store.Claim(ctx)
			if !errors.Is(err, publication.ErrOwned) || time.Now().After(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if err != nil {
			return err
		}
		state, pending, err := store.Load(ctx)
		if err != nil {
			return err
		}
		c.flushMu.Lock()
		defer c.flushMu.Unlock()
		c.storage, c.owner, c.committed = store, store.Owner, state
		c.installCommitted(state, pending)
		return nil
	}
	// Validate and claim the legacy checkpoint exactly as before. Migration must
	// not require a successful public-cache write to make recovered state usable.
	if err = c.restoreV1(ctx); err != nil {
		if errors.Is(err, errPublicationLayoutChanged) {
			return c.restore(ctx)
		}
		return err
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	var snap dorv1.Snapshot
	if err = live.UnmarshalSnapshot(c.checkpoint.Snapshot, &snap); err != nil {
		return err
	}
	state := publication.NewState(&snap)
	state.Raw = c.checkpoint.StreamSeq
	state.Revisions = maps.Clone(c.checkpoint.Revisions)
	if state.Revisions == nil {
		state.Revisions = map[string]uint64{}
	}
	state.Scores = cloneScores(c.checkpoint.Scores)
	for id := range state.Scores {
		state.ScoreOrder = append(state.ScoreOrder, id)
	}
	slices.Sort(state.ScoreOrder)
	c.scoreOrder = nil
	for id := range c.scores {
		c.scoreOrder = append(c.scoreOrder, id)
	}
	slices.Sort(c.scoreOrder)
	var pending *publication.Record
	if c.inflight != nil {
		pending = c.makeRecord(state, c.inflight.data, c.inflight.revisions, c.inflight.scores, c.wireOrder, c.scoreOrder)
	}
	store := publication.New(kv, c.owner, publisherLease, c.bus.NC().MaxPayload())
	if err = store.Initialize(ctx, state, c.revision, pending); err != nil {
		return err
	}
	c.storage, c.committed = store, state
	c.installCommitted(state, pending)
	return nil
}

func (c *publisherCore) installCommitted(state *publication.State, pending *publication.Record) {
	c.storage.Observe = c.observeStorage
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq = state.Seq
	c.stories, c.links, c.wireRows = maps.Clone(state.Stories), maps.Clone(state.Links), maps.Clone(state.Rows)
	c.wire = append([]*dorv1.WireRow(nil), state.RecentWire...)
	c.wireOrder = append([]string(nil), state.WireOrder...)
	c.revisions, c.committedRevisions = maps.Clone(state.Revisions), maps.Clone(state.Revisions)
	c.scores = cloneScores(state.Scores)
	c.scoreOrder = append([]string(nil), state.ScoreOrder...)
	c.checkpoint = publisherCheckpoint{Owner: c.owner, StreamSeq: state.Raw, Revisions: maps.Clone(state.Revisions), Scores: cloneScores(state.Scores)}
	c.pending, c.order, c.waiters = map[string]*dorv1.Delta{}, nil, nil
	c.inflight = nil
	c.placed = map[string]bool{}
	c.cooledAt = make(map[string]int64, len(c.stories))
	for id := range c.stories {
		c.cooledAt[id] = c.now().UnixNano()
	}
	if pending != nil {
		for id, v := range pending.Revisions {
			c.revisions[id] = v
		}
		for _, id := range pending.RevisionDeletes {
			delete(c.revisions, id)
		}
		for id, v := range pending.Scores {
			c.scores[id] = v
		}
		for _, id := range pending.ScoreDeletes {
			delete(c.scores, id)
		}
		c.wireOrder = pending.WireOrder.Apply(c.wireOrder)
		c.scoreOrder = pending.ScoreOrder.Apply(c.scoreOrder)
		var batch dorv1.DeltaBatch
		_ = proto.Unmarshal(pending.Data, &batch)
		c.inflight = &publisherBatch{data: pending.Data, deltas: batch.Deltas, reserved: true, revisions: maps.Clone(c.revisions), scores: cloneScores(c.scores), record: pending}
		for _, d := range batch.Deltas {
			if row := d.GetWire(); row != nil {
				c.wireRows[row.ArticleId] = row
			}
		}
		c.checkpoint.Pending = pending.Data
	}
	for key := range c.revisions {
		if strings.HasPrefix(key, bus.StreamEnrich+"/placed:") {
			id := strings.TrimPrefix(key, bus.StreamEnrich+"/placed:")
			if c.wireRows[id] != nil {
				c.placed[id] = true
			}
		}
	}
	c.fenceDone = make(chan struct{})
	c.capacity = make(chan struct{})
	c.pendingCredit = 0
	c.fenced.Store(false)
	c.restored = true
}

func (c *publisherCore) makeRecord(state *publication.State, data []byte, revisions map[string]uint64, scores map[string]*dorv1.MatchScore, wireOrder, scoreOrder []string) *publication.Record {
	r := &publication.Record{Ordinal: state.Ordinal + 1, PreviousSeq: state.Seq, PreviousRaw: state.Raw, Data: data, Revisions: map[string]uint64{}, Scores: map[string]*dorv1.MatchScore{}, WireOrder: publication.DiffOrder(state.WireOrder, wireOrder), ScoreOrder: publication.DiffOrder(state.ScoreOrder, scoreOrder)}
	for id, v := range revisions {
		if state.Revisions[id] != v {
			r.Revisions[id] = v
		}
	}
	for id := range state.Revisions {
		if _, ok := revisions[id]; !ok {
			r.RevisionDeletes = append(r.RevisionDeletes, id)
		}
	}
	slices.Sort(r.RevisionDeletes)
	for id, v := range scores {
		if !proto.Equal(state.Scores[id], v) {
			r.Scores[id] = v
		}
	}
	for id := range state.Scores {
		if _, ok := scores[id]; !ok {
			r.ScoreDeletes = append(r.ScoreDeletes, id)
		}
	}
	slices.Sort(r.ScoreDeletes)
	return r
}

func (c *publisherCore) flush(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if !c.restored || c.storage == nil {
		return errors.New("publisher: state not restored")
	}
	opCtx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
	defer cancel()
	ctx = opCtx
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
		if err == nil {
			err = c.checkPayload("DELTA batch before reservation", batch.data)
		}
		if err != nil {
			c.mu.Unlock()
			c.ingestMu.Unlock()
			return err
		}
		batch.record = c.makeRecord(c.committed, batch.data, batch.revisions, batch.scores, c.wireOrder, c.scoreOrder)
		c.inflight = batch
		c.pending, c.order, c.waiters = map[string]*dorv1.Delta{}, nil, nil
		c.pendingCredit = 0
		close(c.capacity)
		c.capacity = make(chan struct{})
	}
	c.mu.Unlock()
	c.ingestMu.Unlock()
	batch := c.inflight
	if batch == nil {
		return c.checkFence(c.storage.Renew(ctx))
	}
	if !batch.reserved {
		if err := c.storage.Reserve(ctx, batch.record); err != nil {
			return c.checkFence(err)
		}
		batch.reserved = true
		c.checkpoint.Pending = batch.data
	} else if err := c.storage.Renew(ctx); err != nil {
		return c.checkFence(err)
	}
	last, raw, err := batch.record.End()
	if err != nil {
		return err
	}
	if !batch.published {
		if len(batch.deltas) > 0 {
			if err = c.publishBatch(ctx, batch, fmt.Sprintf("delta:%d", last)); err != nil {
				return err
			}
			if batch.streamSeq != raw {
				return errors.New("publisher: unexpected DELTA raw sequence")
			}
		} else {
			batch.streamSeq = raw
		}
		batch.published = true
	}
	if err = c.storage.Commit(ctx, batch.record.Ordinal, last, raw); err != nil {
		return c.checkFence(err)
	}
	// Root is authoritative before memory application and before any input ACK.
	if err = c.committed.Apply(batch.record, raw); err != nil {
		return err
	}
	c.mu.Lock()
	for _, d := range batch.deltas {
		c.applyDeltaLocked(d)
	}
	c.seq = last
	c.committedRevisions = maps.Clone(c.committed.Revisions)
	c.mu.Unlock()
	c.checkpoint.StreamSeq, c.checkpoint.Pending = raw, nil
	c.checkpoint.Revisions, c.checkpoint.Scores = maps.Clone(c.committed.Revisions), cloneScores(c.committed.Scores)
	for _, done := range batch.waiters {
		close(done)
	}
	c.inflight = nil
	return nil
}

func (c *publisherCore) saveCheckpoint(ctx context.Context, cp publisherCheckpoint) error {
	if c.storage == nil {
		return c.saveLegacyCheckpoint(ctx, cp)
	}
	r := c.makeRecord(c.committed, nil, cp.Revisions, cp.Scores, c.committed.WireOrder, c.committed.ScoreOrder)
	if len(r.Revisions)+len(r.RevisionDeletes)+len(r.Scores)+len(r.ScoreDeletes) == 0 {
		return c.checkFence(c.storage.Renew(ctx))
	}
	c.inflight = &publisherBatch{record: r, revisions: cp.Revisions, scores: cp.Scores}
	if err := c.storage.Reserve(ctx, r); err != nil {
		return c.checkFence(err)
	}
	c.inflight.reserved = true
	if err := c.storage.Commit(ctx, r.Ordinal, c.committed.Seq, c.committed.Raw); err != nil {
		return c.checkFence(err)
	}
	if err := c.committed.Apply(r, c.committed.Raw); err != nil {
		return err
	}
	c.checkpoint.Revisions, c.checkpoint.Scores = maps.Clone(c.committed.Revisions), cloneScores(c.committed.Scores)
	c.inflight = nil
	return nil
}
func (c *publisherCore) release(ctx context.Context) error {
	if c.storage == nil {
		return c.releaseLegacy(ctx)
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	err := c.storage.Release(ctx)
	if errors.Is(err, publication.ErrFenced) {
		return nil
	}
	c.restored = false
	return err
}

func (c *publisherCore) checkFence(err error) error {
	if errors.Is(err, publication.ErrFenced) && c.fenced.CompareAndSwap(false, true) {
		if c.fenceDone != nil {
			close(c.fenceDone)
		}
		if c.onFence != nil {
			c.onFence()
		}
	}
	return err
}
func (c *publisherCore) recordFlush(err error, d time.Duration) {
	c.errMu.Lock()
	c.flushErr = err
	c.errMu.Unlock()
	if c.metrics != nil {
		outcome := OutcomeOK
		if err != nil {
			outcome = OutcomeRetry
			c.metrics.errors.WithLabelValues(PublisherAgentName).Inc()
		}
		c.metrics.handle.WithLabelValues(PublisherAgentName, outcome).Observe(d.Seconds())
	}
}

func (c *publisherCore) observeStorage(operation string, elapsed time.Duration, n int) {
	if c.metrics == nil {
		return
	}
	if operation == "bucket" {
		c.metrics.publicationStorage.Set(float64(n))
		return
	}
	c.metrics.publicationDuration.WithLabelValues(operation).Observe(elapsed.Seconds())
	if n > 0 {
		c.metrics.publicationWritten.WithLabelValues(operation).Add(float64(n))
	}
}

func (c *publisherCore) storePublicSnapshot(ctx context.Context, snap *dorv1.Snapshot) error {
	op, done := context.WithTimeout(ctx, 750*time.Millisecond)
	defer done()
	ctx = op
	data, err := c.storage.CacheValue(ctx, snap)
	if err != nil {
		return c.checkFence(err)
	}
	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return err
	}
	for {
		entry, getErr := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
		if getErr == nil {
			if prev, e := c.storage.DecodeCache(ctx, entry.Value()); e == nil {
				if prev.Seq > snap.Seq {
					return nil
				}
				if prev.Seq == snap.Seq && len(prev.Stories) > 0 && len(snap.Stories) == 0 {
					return errors.New("publisher: refusing empty snapshot over live state")
				}
			}
			_, err = kv.Update(ctx, bus.KVLiveStateSnapshotKey, data, entry.Revision())
		} else if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			_, err = kv.Create(ctx, bus.KVLiveStateSnapshotKey, data)
		} else {
			return getErr
		}
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
			continue
		}
		return err
	}
}

func (c *publisherCore) compactionLoop(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		c.flushMu.Lock()
		if c.storage == nil || !c.restored {
			c.flushMu.Unlock()
			continue
		}
		readCtx, readDone := context.WithTimeout(ctx, 200*time.Millisecond)
		root, _, err := c.storage.ReadRoot(readCtx)
		readDone()
		var view *publication.State
		bytes := 0
		for _, e := range root.Entries {
			bytes += e.Blob.Size
		}
		if err == nil && c.committed.Ordinal > root.Base.Ordinal && (time.Since(last) >= 30*time.Second || len(root.Entries) >= 128 || bytes >= 4*1024*1024) {
			view = c.committed.Clone()
		}
		c.flushMu.Unlock()
		if view != nil {
			err = c.storage.Compact(ctx, view)
			if err == nil {
				last = time.Now()
			} else {
				c.log.Warn("publisher: compaction deferred", "err", c.checkFence(err))
				if c.metrics != nil {
					c.metrics.errors.WithLabelValues("publisher-compaction").Inc()
				}
			}
		}
		op, done := context.WithTimeout(ctx, 100*time.Millisecond)
		err = c.storage.GC(op)
		done()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			c.log.Debug("publisher: journal retirement deferred", "err", c.checkFence(err))
		}
	}
}
