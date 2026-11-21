package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func durabilityClient(h *hub) (*client, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &client{hub: h, log: slog.New(slog.NewTextHandler(io.Discard, nil)), send: make(chan []byte, clientQueue), ctx: ctx}, cancel
}

func clientReady(c *client) bool {
	c.hub.mu.Lock()
	defer c.hub.mu.Unlock()
	return c.ready
}

func readServerMessages(t *testing.T, c *client) []*dorv1.ServerMessage {
	t.Helper()
	var result []*dorv1.ServerMessage
	for {
		select {
		case data := <-c.send:
			msg := &dorv1.ServerMessage{}
			if err := proto.Unmarshal(data, msg); err != nil {
				t.Fatalf("decode server message: %v", err)
			}
			result = append(result, msg)
		default:
			return result
		}
	}
}

func TestHubResumeSnapshotAtSeqZeroReplaysFromOne(t *testing.T) {
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) { return &dorv1.Snapshot{Seq: 0}, nil }
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{Seq: 1, Body: &dorv1.Delta_StoryExpire{StoryExpire: "gone"}}}})
	c, cancel := durabilityClient(h)
	defer cancel()
	h.resume(c, 0)
	msgs := readServerMessages(t, c)
	if len(msgs) != 2 || msgs[0].GetSnapshot() == nil || msgs[0].GetSnapshot().Seq != 0 || msgs[1].GetDeltas() == nil || msgs[1].GetDeltas().Deltas[0].Seq != 1 {
		t.Fatalf("zero baseline recovery messages=%v", msgs)
	}
}

func TestHubBehindRingReceivesResyncAndSnapshotTail(t *testing.T) {
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) { return &dorv1.Snapshot{Seq: 2}, nil }
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{Seq: 3, Body: &dorv1.Delta_StoryExpire{StoryExpire: "new"}}}})
	c, cancel := durabilityClient(h)
	defer cancel()
	h.resume(c, 1)
	msgs := readServerMessages(t, c)
	if len(msgs) != 3 || msgs[0].GetResync() == nil || msgs[0].GetResync().Seq != 2 || msgs[1].GetSnapshot() == nil || msgs[2].GetDeltas().Deltas[0].Seq != 3 {
		t.Fatalf("behind-ring recovery messages=%v", msgs)
	}
}

func TestHubMissingCorruptAndReadErrorWaitThenRecover(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  error
	}{{"missing", errors.New("missing key")}, {"corrupt", errors.New("corrupt snapshot")}, {"read", errors.New("KV unavailable")}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			var calls atomic.Int32
			h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
				call := calls.Add(1)
				if call == 1 {
					return nil, tc.bad
				}
				return &dorv1.Snapshot{Seq: 0}, nil
			}
			c, cancel := durabilityClient(h)
			defer cancel()
			h.startResume(c.ctx, c, 0, "")
			deadline := time.Now().Add(time.Second)
			for calls.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if len(c.send) != 0 || clientReady(c) {
				t.Fatal("gateway sent or readied a client without a valid snapshot")
			}
			deadline = time.Now().Add(time.Second)
			for !clientReady(c) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !clientReady(c) {
				t.Fatal("gateway did not resume after snapshot became readable")
			}
			msgs := readServerMessages(t, c)
			if len(msgs) != 1 || msgs[0].GetSnapshot() == nil {
				t.Fatalf("recovery messages=%v", msgs)
			}
		})
	}
}

func TestHubRejectsInternalRingSequenceGaps(t *testing.T) {
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.ring = []*dorv1.Delta{{Seq: 1}, {Seq: 3}}
	h.seq = 3
	if _, covered := h.sinceLocked(1); covered {
		t.Fatal("ring with an internal gap reported full coverage")
	}
}

func TestHubSlowClientGetsResyncAndSnapshotWithoutDisconnect(t *testing.T) {
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) { return &dorv1.Snapshot{Seq: 1}, nil }
	c, cancel := durabilityClient(h)
	defer cancel()
	h.addClient(c)
	h.mu.Lock()
	c.ready = true
	h.mu.Unlock()
	for i := 0; i < cap(c.send); i++ {
		c.send <- []byte("queued")
	}
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{Seq: 1, Body: &dorv1.Delta_StoryExpire{StoryExpire: "s"}}}})
	deadline := time.Now().Add(time.Second)
	for !clientReady(c) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !clientReady(c) {
		t.Fatal("slow client did not recover")
	}
	if c.closed.Load() {
		t.Fatal("slow client was disconnected")
	}
	msgs := readServerMessages(t, c)
	if len(msgs) != 2 || msgs[0].GetResync() == nil || msgs[1].GetSnapshot() == nil {
		t.Fatalf("slow-client recovery messages=%v", msgs)
	}
}

func TestHubLoadsCompressedLargeSnapshotFromKV(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: filepath.Join(t.TempDir(), "nats")})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	snapshot := &dorv1.Snapshot{Seq: 57, Stories: make([]*dorv1.Story, 4645), Links: make([]*dorv1.Link, 13302)}
	for i := range snapshot.Stories {
		snapshot.Stories[i] = &dorv1.Story{Id: fmt.Sprintf("story-%d", i), Title: "Production globe story with representative details and location metadata"}
	}
	for i := range snapshot.Links {
		snapshot.Links[i] = &dorv1.Link{A: fmt.Sprintf("story-%d", i%4645), B: fmt.Sprintf("target-%d", i), Strength: .6}
	}
	data, err := live.MarshalSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) >= 1024*1024 {
		t.Fatalf("compressed fixture exceeds 1MiB: %d", len(data))
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, bus.KVLiveStateSnapshotKey, data); err != nil {
		t.Fatal(err)
	}

	h := newHub(b, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, recovered, err := h.loadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Seq != snapshot.Seq || len(recovered.Stories) != len(snapshot.Stories) || len(recovered.Links) != len(snapshot.Links) {
		t.Fatalf("gateway large snapshot recovery seq=%d stories=%d links=%d", recovered.Seq, len(recovered.Stories), len(recovered.Links))
	}
}
