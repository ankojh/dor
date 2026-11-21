package gateway

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
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

	snapMu    sync.Mutex
	snapBytes []byte
	snapVal   *dorv1.Snapshot
	snapSeq   uint64
	snapAt    time.Time
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

// addBatch records a publisher batch in the replay ring and broadcasts it to
// every ready client. A client whose bounded queue is full is disconnected;
// it reconnects with Hello(last_seq) and gets a snapshot if the ring cannot
// cover the gap, so one slow client never blocks the others.
func (h *hub) addBatch(batch *dorv1.DeltaBatch) {
	if batch == nil || len(batch.Deltas) == 0 {
		return
	}
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

	var slow []*client
	h.mu.Lock()
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
		payload := data
		if hasWire {
			if f := c.filterState(); f.active() {
				filtered := filterDeltaBatch(batch, f, h.stories)
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
			slow = append(slow, c)
		}
	}
	h.mu.Unlock()
	for _, c := range slow {
		c.kick("slow client")
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

// resume implements Hello. It runs under the hub lock after fetching the
// snapshot, so live deltas cannot interleave with the snapshot/replay.
func (h *hub) resume(c *client, lastSeq uint64) {
	snapBytes, snap := h.loadSnapshot(context.Background())

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, story := range snap.Stories {
		h.observeStoryLocked(story)
	}

	replay := func(from uint64) {
		deltas, ok := h.sinceLocked(from)
		if !ok || len(deltas) == 0 {
			return
		}
		for start := 0; start < len(deltas); start += replayChunk {
			end := start + replayChunk
			if end > len(deltas) {
				end = len(deltas)
			}
			chunk := &dorv1.DeltaBatch{Deltas: deltas[start:end]}
			if f := c.filterState(); f.active() {
				chunk = filterDeltaBatch(chunk, f, h.stories)
				if chunk == nil {
					continue
				}
			}
			msg := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Deltas{Deltas: chunk}}
			data, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
			if err != nil {
				continue
			}
			if !c.enqueue(data) {
				c.kick("slow client during replay")
				return
			}
		}
	}
	sendSnapshot := func() {
		payload := snapBytes
		if f := c.filterState(); f.active() {
			if filtered := filterSnapshot(snap, f, h.stories); filtered != nil {
				msg := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Snapshot{Snapshot: filtered}}
				if data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg); err == nil {
					payload = data
				}
			}
		}
		if !c.enqueue(payload) {
			c.kick("slow client for snapshot")
			return
		}
		if snap.Seq > 0 && snap.Seq < h.seq {
			replay(snap.Seq + 1)
		}
	}

	if lastSeq == 0 || lastSeq > h.seq {
		sendSnapshot()
		c.ready = true
		return
	}
	if lastSeq == h.seq {
		c.ready = true
		return
	}
	if deltas, ok := h.sinceLocked(lastSeq + 1); ok && len(deltas) > 0 {
		c.ready = true
		replay(lastSeq + 1)
		if !c.closed.Load() {
			return
		}
		return
	}
	// The ring cannot cover the gap: reset the client to the cached snapshot.
	if snap.Seq > 0 && snap.Seq < lastSeq {
		resync := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Resync{Resync: &dorv1.Resync{
			Seq: snap.Seq, Reason: "client behind the replay ring",
		}}}
		opts := proto.MarshalOptions{Deterministic: true}
		if data, err := opts.Marshal(resync); err == nil {
			if !c.enqueue(data) {
				c.kick("slow client for resync")
				return
			}
		}
	}
	sendSnapshot()
	c.ready = true
}

// sinceLocked returns the ring deltas from seq onwards. ok is false when the
// ring cannot cover seq (a gap), in which case the caller sends a snapshot.
func (h *hub) sinceLocked(from uint64) ([]*dorv1.Delta, bool) {
	if from == 0 {
		return nil, false
	}
	if from > h.seq {
		return nil, true
	}
	if len(h.ring) == 0 {
		return nil, false
	}
	if h.ring[0].Seq > from {
		return nil, false
	}
	idx := sort.Search(len(h.ring), func(i int) bool { return h.ring[i].Seq >= from })
	if idx >= len(h.ring) {
		return nil, true
	}
	if h.ring[idx].Seq != from {
		return nil, false
	}
	return h.ring[idx:], true
}

// loadSnapshot returns the publisher's cached snapshot, or an empty snapshot
// at the current seq when none exists yet. The cache is short-lived so a
// reconnect sees a recent snapshot without a KV hit on every hello.
func (h *hub) loadSnapshot(ctx context.Context) ([]byte, *dorv1.Snapshot) {
	h.snapMu.Lock()
	if len(h.snapBytes) > 0 && time.Since(h.snapAt) < snapshotTTL && h.snapVal != nil {
		bytes := append([]byte(nil), h.snapBytes...)
		snap := h.snapVal
		h.snapMu.Unlock()
		return bytes, snap
	}
	h.snapMu.Unlock()

	var bytes []byte
	snap := &dorv1.Snapshot{}
	if kv, err := h.bus.KV(ctx, bus.KVBucketLiveState); err == nil {
		getCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		entry, err := kv.Get(getCtx, bus.KVLiveStateSnapshotKey)
		cancel()
		if err == nil {
			if proto.Unmarshal(entry.Value(), snap) == nil {
				bytes = append([]byte(nil), entry.Value()...)
			}
		}
	}
	if len(bytes) == 0 {
		snap = &dorv1.Snapshot{Seq: h.latest(), BuiltAt: time.Now().Unix()}
		bytes, _ = proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	}
	msg := &dorv1.ServerMessage{Body: &dorv1.ServerMessage_Snapshot{Snapshot: snap}}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		// Fall back to the snapshot alone; a client that cannot decode this
		// reconnects and tries again.
		return bytes, snap
	}
	h.snapMu.Lock()
	h.snapBytes = wire
	h.snapVal = snap
	h.snapSeq = snap.Seq
	h.snapAt = time.Now()
	h.snapMu.Unlock()
	return wire, snap
}
