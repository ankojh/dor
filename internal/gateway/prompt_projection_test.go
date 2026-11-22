package gateway

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func projectionLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type projectionSink struct {
	mu    sync.Mutex
	calls [][]string
}

func (s *projectionSink) SetClientPrompts(_ context.Context, _ string, prompts []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, append([]string(nil), prompts...))
}

func (s *projectionSink) ClearClientPrompts(context.Context, string) {}

func (s *projectionSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *projectionSink) last() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return nil
	}
	return s.calls[len(s.calls)-1]
}

func (s *projectionSink) callsSnapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]string, len(s.calls))
	copy(out, s.calls)
	return out
}

func newProjectionClient(h *hub, sink PromptSink) *client {
	c := newClient(h, nil, sink, nil, projectionLogger())
	h.addClient(c)
	h.mu.Lock()
	c.ready = true
	h.mu.Unlock()
	return c
}

func setPromptsMessage(prompts []string) *dorv1.ClientMessage {
	return &dorv1.ClientMessage{Body: &dorv1.ClientMessage_SetPrompts{
		SetPrompts: &dorv1.SetPrompts{Prompts: prompts},
	}}
}

func readRawSend(t *testing.T, c *client) []byte {
	t.Helper()
	select {
	case data := <-c.send:
		return data
	default:
		t.Fatal("client received no message")
		return nil
	}
}

func decodeWireDelta(t *testing.T, data []byte) *dorv1.WireRow {
	t.Helper()
	var msg dorv1.ServerMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode server message: %v", err)
	}
	batch := msg.GetDeltas()
	if batch == nil || len(batch.Deltas) == 0 {
		t.Fatalf("message has no delta batch: %v", &msg)
	}
	wire := batch.Deltas[0].GetWire()
	if wire == nil {
		t.Fatalf("delta has no wire row: %v", batch.Deltas[0])
	}
	return wire
}

func TestLiveBroadcastProjectsPromptDataForEveryClient(t *testing.T) {
	h := newHub(nil, projectionLogger())
	sink := &projectionSink{}
	a := newProjectionClient(h, sink)
	b := newProjectionClient(h, nil)

	a.handle(context.Background(), setPromptsMessage([]string{"secret"}))
	if got := sink.last(); len(got) != 1 || got[0] != "secret" {
		t.Fatalf("client A registration = %v", got)
	}

	wire := &dorv1.WireRow{
		StoryId:       "story-1",
		Title:         "Some story",
		PromptScores:  map[string]float32{"secret": 0.9, "mine": 0.5},
		ApproxPrompts: []string{"secret"},
	}
	batch := &dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq:  1,
		Body: &dorv1.Delta_Wire{Wire: wire},
	}}}
	h.addBatch(batch)

	bRaw := readRawSend(t, b)
	bWire := decodeWireDelta(t, bRaw)
	if len(bWire.PromptScores) != 0 || len(bWire.ApproxPrompts) != 0 {
		t.Fatalf("client B prompt fields = %v / %v, want empty", bWire.PromptScores, bWire.ApproxPrompts)
	}
	if bytes.Contains(bRaw, []byte("secret")) || bytes.Contains(bRaw, []byte("mine")) {
		t.Fatalf("client B wire leaked prompt text: %s", bRaw)
	}

	aRaw := readRawSend(t, a)
	aWire := decodeWireDelta(t, aRaw)
	if len(aWire.PromptScores) != 1 {
		t.Fatalf("client A scores = %v, want exactly its own prompt", aWire.PromptScores)
	}
	if score := aWire.PromptScores["secret"]; score < 0.89 || score > 0.91 {
		t.Fatalf("client A secret score = %v", score)
	}
	if _, ok := aWire.PromptScores["mine"]; ok {
		t.Fatalf("client A received another client's prompt score: %v", aWire.PromptScores)
	}
	if len(aWire.ApproxPrompts) != 1 || aWire.ApproxPrompts[0] != "secret" {
		t.Fatalf("client A approx prompts = %v", aWire.ApproxPrompts)
	}
	if bytes.Contains(aRaw, []byte("mine")) {
		t.Fatalf("client A wire leaked another prompt: %s", aRaw)
	}

	// Projection must clone: the replay ring and the caller's row still carry
	// the full per-prompt data for future evaluations.
	if len(wire.PromptScores) != 2 || len(wire.ApproxPrompts) != 1 {
		t.Fatalf("source wire was mutated: %v / %v", wire.PromptScores, wire.ApproxPrompts)
	}
	h.mu.Lock()
	ringWire := h.ring[0].GetWire()
	h.mu.Unlock()
	if ringWire == nil || len(ringWire.PromptScores) != 2 || len(ringWire.ApproxPrompts) != 1 {
		t.Fatalf("replay ring was mutated: %v", ringWire)
	}
}

func TestResumeSnapshotProjectsAnotherClientsPromptData(t *testing.T) {
	h := newHub(nil, projectionLogger())
	snap := &dorv1.Snapshot{
		Seq: 0,
		RecentWire: []*dorv1.WireRow{{
			StoryId:       "story-2",
			Title:         "Another story",
			PromptScores:  map[string]float32{"secret": 0.9, "mine": 0.5},
			ApproxPrompts: []string{"secret"},
		}},
	}
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) { return snap, nil }

	c, cancel := durabilityClient(h)
	defer cancel()
	h.resume(c, 0)
	raw := readRawSend(t, c)

	if bytes.Contains(raw, []byte("secret")) || bytes.Contains(raw, []byte("mine")) {
		t.Fatalf("resume snapshot leaked prompt text: %s", raw)
	}
	var msg dorv1.ServerMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode resume message: %v", err)
	}
	resumed := msg.GetSnapshot()
	if resumed == nil {
		t.Fatalf("resume message is not a snapshot: %v", &msg)
	}
	if len(resumed.RecentWire) != 1 {
		t.Fatalf("resumed recent wire rows = %d, want 1", len(resumed.RecentWire))
	}
	if len(resumed.RecentWire[0].PromptScores) != 0 || len(resumed.RecentWire[0].ApproxPrompts) != 0 {
		t.Fatalf("resumed snapshot prompt fields = %v / %v", resumed.RecentWire[0].PromptScores, resumed.RecentWire[0].ApproxPrompts)
	}

	// The snapshot cache source must keep the original scores.
	if len(snap.RecentWire[0].PromptScores) != 2 || len(snap.RecentWire[0].ApproxPrompts) != 1 {
		t.Fatalf("cached snapshot was mutated: %v / %v", snap.RecentWire[0].PromptScores, snap.RecentWire[0].ApproxPrompts)
	}
}
