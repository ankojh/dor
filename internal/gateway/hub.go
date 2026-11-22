package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/maintenance"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
)

const (
	// replayRing is the bounded number of recent deltas kept for resume. At
	// the 50 articles/s design rate a 30 s drop needs ~1500 deltas.
	replayRing = 16384
	// replayChunk bounds one replay message.
	replayChunk = 200
	// snapshotTTL is how long a fetched snapshot is reused for new hellos.
	snapshotTTL = 2 * time.Second
)

// hub fans deltas out to clients and keeps a bounded replay ring. All client
// registration, resume and broadcast decisions are serialised by mu, so a
// delta can never slip between a snapshot and the start of the live stream.
type hub struct {
	bus *bus.Bus
	log *slog.Logger

	mu      sync.Mutex
	clients map[*client]struct{}
	ring    []*dorv1.Delta
	seq     uint64
	stories map[string]*dorv1.Story

	snapMu       sync.Mutex
	snapBytes    []byte
	snapVal      *dorv1.Snapshot
	snapSeq      uint64
	snapAt       time.Time
	snapLoad     chan struct{}
	snapErr      error
	snapRetryAt  time.Time
	snapFailures int
	readSnapshot func(context.Context) (*dorv1.Snapshot, error) // injectable KV read
	// floor is the active maintenance baseline. A client below floorT must
	// receive Resync plus a snapshot at or beyond it, even when the old ring
	// still covers its requested range.
	floorSet bool
	floorT   uint64
	floorD   uint64
	// alertOwner resolves a saved alert's anonymous owner. Alert deltas are
	// private; an empty owner is visible to nobody.
	alertOwner func(string) string
}

func newHub(b *bus.Bus, log *slog.Logger) *hub {
	return &hub{
		bus:     b,
		log:     log,
		clients: make(map[*client]struct{}),
		stories: make(map[string]*dorv1.Story),
	}
}

func (h *hub) addClient(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) removeClient(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	if c.resumeCancel != nil {
		c.resumeCancel()
	}
	h.mu.Unlock()
}

func (h *hub) clientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *hub) latest() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

func (h *hub) ringLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.ring)
}

func (h *hub) snapshotSeq() uint64 {
	h.snapMu.Lock()
	defer h.snapMu.Unlock()
	return h.snapSeq
}

func (h *hub) closeAll() {
	h.mu.Lock()
	clients := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.kick("server shutting down")
	}
}

// suspend invalidates every in-memory recovery surface before a maintenance
// baseline change. A KV watch alone is not a serving fence; this is the local
// half of that fence.
func (h *hub) suspend() {
	h.mu.Lock()
	for c := range h.clients {
		if c.resumeCancel != nil {
			c.resumeCancel()
		}
		c.ready = false
		c.resetQueueLocked()
	}
	h.ring = nil
	h.seq = 0
	h.stories = make(map[string]*dorv1.Story)
	h.mu.Unlock()
	h.snapMu.Lock()
	h.snapBytes, h.snapVal, h.snapSeq, h.snapAt = nil, nil, 0, time.Time{}
	h.snapMu.Unlock()
}

// installBaseline atomically replaces the replay ring, story lookup and floor
// with a decoded snapshot at or beyond the current minimum resume sequence.
func (h *hub) installBaseline(snap *dorv1.Snapshot, floor maintenance.Floor) error {
	if snap == nil {
		return fmt.Errorf("gateway: nil maintenance baseline snapshot")
	}
	if floor.Valid() && snap.Seq < floor.MinimumResumeSeq {
		return fmt.Errorf("gateway: snapshot seq %d is below maintenance floor %d", snap.Seq, floor.MinimumResumeSeq)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(&dorv1.ServerMessage{Body: &dorv1.ServerMessage_Snapshot{Snapshot: snap}})
	if err != nil {
		return err
	}
	h.mu.Lock()
	for c := range h.clients {
		if c.resumeCancel != nil {
			c.resumeCancel()
		}
		c.ready = false
		c.resetQueueLocked()
	}
	h.ring = nil
	h.seq = snap.Seq
	h.stories = make(map[string]*dorv1.Story, len(snap.Stories))
	for _, story := range snap.Stories {
		h.observeStoryLocked(story)
	}
	if floor.Valid() {
		h.floorSet, h.floorT, h.floorD = true, floor.MinimumResumeSeq, floor.BootstrapStreamSeq
	} else {
		h.floorSet, h.floorT, h.floorD = false, 0, 0
	}
	h.mu.Unlock()
	h.snapMu.Lock()
	h.snapBytes, h.snapVal, h.snapSeq, h.snapAt = wire, snap, snap.Seq, time.Now()
	h.snapErr, h.snapFailures, h.snapRetryAt = nil, 0, time.Time{}
	h.snapMu.Unlock()
	return nil
}

// floorForRead returns the currently installed baseline.
func (h *hub) floorForRead() (bool, uint64, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.floorSet, h.floorT, h.floorD
}

func (h *hub) snapshotMeetsFloor(seq uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.floorSet || seq >= h.floorT
}

// addBatch records fresh deltas and broadcasts without blocking on clients.
// Overflow suspends live delivery, discards the queue and starts resync.
func (h *hub) addBatch(batch *dorv1.DeltaBatch) {
	if batch == nil || len(batch.Deltas) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	fresh := &dorv1.DeltaBatch{}
	gap := false
	for _, d := range batch.Deltas {
		if d != nil && d.Seq > h.seq {
			if d.Seq != h.seq+1 {
				gap = true
			}
			fresh.Deltas = append(fresh.Deltas, d)
			h.seq = d.Seq
		}
	}
	if len(fresh.Deltas) == 0 {
		return
	}
	batch = fresh
	msg := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Deltas{Deltas: batch}}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		h.log.Warn("gateway: marshal delta message", "err", err)
		return
	}
	hasWire := false
	for _, d := range batch.Deltas {
		if d.GetWire() != nil {
			hasWire = true
			break
		}
	}
	// Prompt scores and prompt strings belong to the client that set that
	// prompt, and alert hits belong to the session that saved the alert. Even
	// a client with no filters receives a projected batch when either kind of
	// private data is present.
	hasPrivate := batchHasPrivateWire(batch)
	hasAlert := batchHasAlert(batch)

	h.observeBatchLocked(batch)
	for _, d := range batch.Deltas {
		h.ring = append(h.ring, d)
		if d.Seq > h.seq {
			h.seq = d.Seq
		}
	}
	if len(h.ring) > replayRing {
		drop := len(h.ring) - replayRing
		h.ring = append([]*dorv1.Delta(nil), h.ring[drop:]...)
	}
	for c := range h.clients {
		if !c.ready {
			continue
		}
		if gap {
			h.startResumeLocked(c.context(), c, 0, "gap in delta stream")
			continue
		}
		payload := data
		if hasWire || hasAlert {
			if f := c.filterState(); f.active() || hasPrivate || hasAlert {
				filtered := filterDeltaBatch(batch, f, h.stories, c.sessionID, h.alertOwner)
				if filtered == nil {
					continue
				}
				private, err := proto.MarshalOptions{Deterministic: true}.Marshal(
					&dorv1.ServerMessage{Body: &dorv1.ServerMessage_Deltas{Deltas: filtered}})
				if err != nil {
					continue
				}
				payload = private
			}
		}
		if !c.enqueue(payload) {
			h.startResumeLocked(c.context(), c, 0, "slow client")
		}
	}
}

// observeBatchLocked tracks the live story set so min_sources and region
// filters work on wire rows whose story arrived in an earlier batch.
func (h *hub) observeBatchLocked(batch *dorv1.DeltaBatch) {
	for _, d := range batch.Deltas {
		switch body := d.Body.(type) {
		case *dorv1.Delta_StoryUpsert:
			h.observeStoryLocked(body.StoryUpsert)
		case *dorv1.Delta_StoryExpire:
			delete(h.stories, body.StoryExpire)
		}
	}
}

func (h *hub) observeStoryLocked(story *dorv1.Story) {
	if story == nil || story.Id == "" {
		return
	}
	if cur := h.stories[story.Id]; cur == nil || cur.UpdatedAt <= story.UpdatedAt {
		h.stories[story.Id] = story
	}
}

// startResume serializes recovery attempts per client while leaving its reader
// and the hub free to process disconnects, other clients and new deltas.
func (h *hub) startResume(ctx context.Context, c *client, lastSeq uint64, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.startResumeLocked(ctx, c, lastSeq, reason)
}

func (h *hub) startResumeLocked(ctx context.Context, c *client, lastSeq uint64, reason string) {
	if c.resumeCancel != nil {
		c.resumeCancel()
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.resumeCancel = cancel
	c.ready = false
	c.resetQueueLocked()
	go h.resumeContext(runCtx, c, lastSeq, reason)
}

// resume is also useful to synchronous protocol callers.
func (h *hub) resume(c *client, lastSeq uint64) {
	h.resumeContext(c.context(), c, lastSeq, "")
}

func (h *hub) resumeContext(ctx context.Context, c *client, lastSeq uint64, reason string) {
	forceSnapshot := reason != ""
	for {
		if ctx.Err() != nil || c.closed.Load() {
			return
		}
		snapBytes, snap, err := h.loadSnapshot(ctx)
		if err == nil {
			h.mu.Lock()
			if ctx.Err() != nil || c.closed.Load() {
				h.mu.Unlock()
				return
			}
			for _, story := range snap.Stories {
				h.observeStoryLocked(story)
			}
			// A new gateway may not have consumed the retained stream yet. Snapshot
			// provides a baseline; late arrivals <= its seq must not be sent twice.
			if snap.Seq > h.seq {
				h.seq = snap.Seq
			}
			floorSet, floorT := h.floorSet, h.floorT
			from := lastSeq + 1
			snapshotNeeded := forceSnapshot || lastSeq == 0 || lastSeq > h.seq
			// The maintenance floor overrides ring coverage. A client below T,
			// including one exactly at the stale hub sequence, must discard local
			// state and take the corrected snapshot.
			if floorSet && lastSeq < floorT {
				snapshotNeeded = true
				if reason == "" {
					reason = "maintenance floor"
				} else {
					reason += "; maintenance floor"
				}
			}
			deltas, covered := h.sinceLocked(from)
			if !snapshotNeeded && !covered {
				snapshotNeeded = true
				reason = "client behind the replay ring"
			}
			// Coverage alone is insufficient: the bounded socket queue must hold
			// the replay. Escalate now instead of retrying the same large range.
			if !snapshotNeeded && len(deltas) > replayChunk*(cap(c.send)-2) {
				snapshotNeeded = true
				reason = "replay exceeds client queue"
			}
			if snapshotNeeded {
				from = snap.Seq + 1 // seq zero is a real baseline, never a replay sentinel
				deltas, covered = h.sinceLocked(from)
			}
			if covered {
				messages := make([][]byte, 0, 2+(len(deltas)+replayChunk-1)/replayChunk)
				encode := func(msg *dorv1.ServerMessage) {
					data, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
					if err == nil {
						messages = append(messages, data)
					}
				}
				if reason != "" {
					encode(&dorv1.ServerMessage{Body: &dorv1.ServerMessage_Resync{Resync: &dorv1.Resync{Seq: snap.Seq, Reason: reason}}})
				}
				if snapshotNeeded {
					payload := snapBytes
					if f := c.filterState(); f.active() || snapshotHasPrivateWire(snap) {
						if filtered := filterSnapshot(snap, f, h.stories); filtered != nil {
							msg := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Snapshot{Snapshot: filtered}}
							payload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(msg)
						}
					}
					messages = append(messages, payload)
				}
				for start := 0; start < len(deltas); start += replayChunk {
					end := min(start+replayChunk, len(deltas))
					chunk := &dorv1.DeltaBatch{Deltas: deltas[start:end]}
					if f := c.filterState(); f.active() || batchHasPrivateWire(chunk) || batchHasAlert(chunk) {
						chunk = filterDeltaBatch(chunk, f, h.stories, c.sessionID, h.alertOwner)
					}
					if chunk != nil {
						encode(&dorv1.ServerMessage{Body: &dorv1.ServerMessage_Deltas{Deltas: chunk}})
					}
				}
				// Reserve the whole snapshot+tail atomically. Never mark ready after a
				// partial replay; request a newer snapshot if it exceeds the queue.
				if len(messages) <= cap(c.send) {
					c.resetQueueLocked()
					for _, data := range messages {
						c.enqueueRecovery(data)
					}
					c.ready = true
					h.mu.Unlock()
					return
				}
			}
			h.mu.Unlock()
			// Cache might predate the ring; retry from publisher state, without sending
			// an incomplete snapshot or silently declaring this client caught up.
			forceSnapshot = true
			if reason == "" {
				reason = "snapshot tail is unavailable"
			}
			h.requestSnapshotAfter(snap.Seq)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// A delayed client cannot invalidate a newer snapshot fetched by another
// recovery. Reload rate is shared by the hub, rather than multiplied by clients.
func (h *hub) requestSnapshotAfter(seq uint64) {
	h.snapMu.Lock()
	if h.snapSeq <= seq {
		h.snapAt = time.Time{}
	}
	h.snapMu.Unlock()
}

// sinceLocked validates every sequence through the tail, not just the first.
func (h *hub) sinceLocked(from uint64) ([]*dorv1.Delta, bool) {
	if from == 0 {
		return nil, false
	}
	if from > h.seq {
		return nil, true
	}
	idx := sort.Search(len(h.ring), func(i int) bool { return h.ring[i].Seq >= from })
	next := from
	for _, d := range h.ring[idx:] {
		if d.Seq != next {
			return nil, false
		}
		next++
	}
	if next != h.seq+1 {
		return nil, false
	}
	return h.ring[idx:], true
}

// loadSnapshot returns only a successfully decoded publisher snapshot. Missing
// keys, read failures and corrupt bytes leave clients waiting for recovery.
func (h *hub) loadSnapshot(ctx context.Context) ([]byte, *dorv1.Snapshot, error) {
	for {
		h.snapMu.Lock()
		if h.snapVal != nil && time.Since(h.snapAt) < snapshotTTL {
			if !h.snapshotMeetsFloor(h.snapVal.Seq) {
				h.snapAt = time.Time{}
			} else {
				data, snap := append([]byte(nil), h.snapBytes...), h.snapVal
				h.snapMu.Unlock()
				return data, snap, nil
			}
		}
		if loading := h.snapLoad; loading != nil {
			h.snapMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-loading:
				continue
			}
		}
		if time.Now().Before(h.snapRetryAt) {
			err := h.snapErr
			if err == nil {
				err = fmt.Errorf("gateway: waiting for newer snapshot")
			}
			h.snapMu.Unlock()
			return nil, nil, err
		}
		h.snapLoad = make(chan struct{})
		h.snapMu.Unlock()
		data, snap, err := h.fetchSnapshot(context.WithoutCancel(ctx))
		h.snapMu.Lock()
		if err == nil && !h.snapshotMeetsFloor(snap.Seq) {
			err = fmt.Errorf("gateway: snapshot seq %d is below the maintenance floor", snap.Seq)
		}
		if err == nil {
			h.snapBytes, h.snapVal, h.snapSeq, h.snapAt = data, snap, snap.Seq, time.Now()
			h.snapFailures = 0
		} else {
			h.snapFailures++
		}
		h.snapErr = err
		delay := 100 * time.Millisecond
		for i := 1; i < h.snapFailures && delay < 2*time.Second; i++ {
			delay *= 2
		}
		if delay > 2*time.Second {
			delay = 2 * time.Second
		}
		h.snapRetryAt = time.Now().Add(delay)
		close(h.snapLoad)
		h.snapLoad = nil
		h.snapMu.Unlock()
		return data, snap, err
	}
}

func (h *hub) fetchSnapshot(ctx context.Context) ([]byte, *dorv1.Snapshot, error) {
	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var snap *dorv1.Snapshot
	var err error
	if h.readSnapshot != nil {
		snap, err = h.readSnapshot(opCtx)
	} else {
		kv, kvErr := h.bus.KV(opCtx, bus.KVBucketLiveState)
		if kvErr != nil {
			return nil, nil, kvErr
		}
		publisher, getErr := h.bus.KV(opCtx, bus.KVBucketPublisher)
		err = getErr
		if err == nil {
			snap, err = publication.Snapshot(opCtx, publisher, kv)
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if snap == nil {
		return nil, nil, fmt.Errorf("gateway: snapshot unavailable")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(&dorv1.ServerMessage{Body: &dorv1.ServerMessage_Snapshot{Snapshot: snap}})
	if err != nil {
		return nil, nil, err
	}
	return wire, snap, nil
}
