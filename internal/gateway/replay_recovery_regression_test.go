package gateway

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func waitHubClientsReady(t *testing.T, clients []*client, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, c := range clients {
			if !clientReady(c) {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	for i, c := range clients {
		if !clientReady(c) {
			t.Errorf("client %d did not become ready", i)
		}
	}
	t.Fatal("timed out waiting for gateway clients to recover")
}

func replayFixture(n uint64) *hub {
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	batch := &dorv1.DeltaBatch{Deltas: make([]*dorv1.Delta, 0, n)}
	for seq := uint64(1); seq <= n; seq++ {
		batch.Deltas = append(batch.Deltas, &dorv1.Delta{Seq: seq, Body: &dorv1.Delta_StoryExpire{StoryExpire: "expired"}})
	}
	h.addBatch(batch)
	return h
}

func TestHubOversizedCoveredReplaySwitchesToSnapshotAndTail(t *testing.T) {
	const (
		latest     = uint64(13000)
		snapshotAt = uint64(12995)
	)
	h := replayFixture(latest)
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{Seq: snapshotAt}, nil
	}
	c, cancel := durabilityClient(h)
	defer cancel()

	started := time.Now()
	h.startResume(c.ctx, c, 10, "")
	waitHubClientsReady(t, []*client{c}, time.Second)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("oversized replay fallback took %s", elapsed)
	}
	msgs := readServerMessages(t, c)
	if len(msgs) != 3 {
		t.Fatalf("recovery emitted %d messages, want Resync + Snapshot + tail: %#v", len(msgs), msgs)
	}
	if got := msgs[0].GetResync(); got == nil || got.Seq != snapshotAt {
		t.Fatalf("first recovery message = %v, want Resync at %d", msgs[0].GetBody(), snapshotAt)
	}
	if got := msgs[1].GetSnapshot(); got == nil || got.Seq != snapshotAt {
		t.Fatalf("second recovery message = %v, want snapshot at %d", msgs[1].GetBody(), snapshotAt)
	}
	tail := msgs[2].GetDeltas()
	if tail == nil || len(tail.Deltas) != int(latest-snapshotAt) {
		t.Fatalf("tail = %v, want %d deltas", tail, latest-snapshotAt)
	}
	for i, d := range tail.Deltas {
		if want := snapshotAt + 1 + uint64(i); d.Seq != want {
			t.Fatalf("tail delta[%d] seq=%d, want %d", i, d.Seq, want)
		}
	}
}

func TestHubCoalescesSnapshotReadFailuresForWaitingClients(t *testing.T) {
	const clientCount = 24
	h := newHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	h.readSnapshot = func(ctx context.Context) (*dorv1.Snapshot, error) {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if calls.Load() == 1 {
			return nil, context.DeadlineExceeded // transient cache/KV read failure
		}
		return &dorv1.Snapshot{Seq: 0}, nil
	}
	clients := make([]*client, 0, clientCount)
	for i := 0; i < clientCount; i++ {
		c, cancel := durabilityClient(h)
		defer cancel()
		clients = append(clients, c)
	}
	var starts sync.WaitGroup
	starts.Add(clientCount)
	for _, c := range clients {
		go func(c *client) {
			defer starts.Done()
			h.startResume(c.ctx, c, 0, "")
		}(c)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no snapshot read started")
	}
	starts.Wait()
	// All clients are waiting on one blocked read. A per-client read herd is
	// observable here before releasing the injected transient failure.
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent snapshot reads=%d while first read blocked, want 1", got)
	}
	close(release)
	waitHubClientsReady(t, clients, 2*time.Second)
	if got := calls.Load(); got > 2 {
		t.Fatalf("snapshot reads=%d for %d waiting clients and one transient failure; want coalesced retry", got, clientCount)
	}
	for i, c := range clients {
		msgs := readServerMessages(t, c)
		if len(msgs) != 1 || msgs[0].GetSnapshot() == nil {
			t.Fatalf("client %d recovery messages=%v", i, msgs)
		}
	}
}

func TestHubCoalescesLaggingSnapshotReloadForWaitingClients(t *testing.T) {
	const (
		clientCount = 24
		latest      = uint64(13000)
		current     = uint64(12995)
	)
	h := replayFixture(latest)
	enteredReload := make(chan struct{})
	releaseReload := make(chan struct{})
	var calls atomic.Int32
	h.readSnapshot = func(ctx context.Context) (*dorv1.Snapshot, error) {
		switch calls.Add(1) {
		case 1:
			return &dorv1.Snapshot{Seq: 0}, nil // too old to bound replay to the queue
		default:
			select {
			case <-enteredReload:
			default:
				close(enteredReload)
			}
			select {
			case <-releaseReload:
				return &dorv1.Snapshot{Seq: current}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	clients := make([]*client, 0, clientCount)
	for i := 0; i < clientCount; i++ {
		c, cancel := durabilityClient(h)
		defer cancel()
		clients = append(clients, c)
	}
	var starts sync.WaitGroup
	starts.Add(clientCount)
	for _, c := range clients {
		go func(c *client) {
			defer starts.Done()
			h.startResume(c.ctx, c, 0, "")
		}(c)
	}
	starts.Wait()
	select {
	case <-enteredReload:
	case <-time.After(2 * time.Second):
		t.Fatalf("no lagging snapshot reload started (calls=%d)", calls.Load())
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("snapshot reload calls=%d while reload blocked, want one shared reload after initial read", got)
	}
	close(releaseReload)
	waitHubClientsReady(t, clients, 2*time.Second)
	if got := calls.Load(); got > 2 {
		t.Fatalf("snapshot reads=%d for %d clients with one stale cache, want shared reload", got, clientCount)
	}
	for i, c := range clients {
		msgs := readServerMessages(t, c)
		snapshotAt := 0
		if len(msgs) > 0 && msgs[0].GetResync() != nil {
			snapshotAt++
		}
		if len(msgs) != snapshotAt+2 || msgs[snapshotAt].GetSnapshot() == nil || msgs[snapshotAt].GetSnapshot().Seq != current || msgs[snapshotAt+1].GetDeltas() == nil {
			t.Fatalf("client %d recovery messages=%v", i, msgs)
		}
	}
}
