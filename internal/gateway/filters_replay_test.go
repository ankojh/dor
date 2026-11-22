package gateway

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Replay goes through the same projection as live delivery: a filtered wire
// delta keeps its global sequence as a seq-only no-op instead of disappearing.
// Dropping it would make the resumed client see a hole it cannot distinguish
// from lost transport frames.
func TestReplayNoOpsNonMatchingWireForFilteredClient(t *testing.T) {
	h := newHub(nil, projectionLogger())
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{Seq: 0}, nil
	}
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{
		{Seq: 1, Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{
			ArticleId: "keep", StoryId: "story-keep", Category: dorv1.Category_CATEGORY_DISASTER,
		}}},
		{Seq: 2, Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{
			ArticleId: "drop", StoryId: "story-drop", Category: dorv1.Category_CATEGORY_SPORTS,
		}}},
	}})

	c := alertPrivacyClient(h, "session-a")
	c.mu.Lock()
	c.filters = &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}}
	c.mu.Unlock()

	h.resume(c, 0)

	raws := readRawSends(t, c)
	msgs := decodeServerMessages(t, raws)
	var deltas []*dorv1.Delta
	for _, msg := range msgs {
		if batch := msg.GetDeltas(); batch != nil {
			deltas = append(deltas, batch.Deltas...)
		}
	}
	if len(deltas) != 2 {
		t.Fatalf("replayed %d deltas, want 2 (matching body + no-op): %v", len(deltas), deltas)
	}
	if wire := deltas[0].GetWire(); wire == nil || wire.ArticleId != "keep" {
		t.Fatalf("first replayed delta = %+v, want the matching wire body", deltas[0])
	}
	if deltas[1].Seq != 2 || deltas[1].Body != nil {
		t.Fatalf("second replayed delta = %+v, want seq-only no-op 2", deltas[1])
	}
	for _, raw := range raws {
		expectNoLeak(t, raw, "drop", "story-drop")
	}
}

// Same projection, live delivery path: a wire-filtered client sees the
// non-matching delta as a no-op the instant it is broadcast, not only on a
// later resume. This is the exact live shape from the D1 repro (a filtered
// row between two deltas this client does care about), just with a wire
// filter instead of alert ownership.
func TestLiveBroadcastNoOpsNonMatchingWireForFilteredClient(t *testing.T) {
	h := newHub(nil, projectionLogger())

	c := alertPrivacyClient(h, "session-a")
	c.mu.Lock()
	c.filters = &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}}
	c.mu.Unlock()

	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{
		{Seq: 1, Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{
			ArticleId: "drop", StoryId: "story-drop", Title: "private title", Category: dorv1.Category_CATEGORY_SPORTS,
		}}},
		{Seq: 2, Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{
			ArticleId: "keep", StoryId: "story-keep", Category: dorv1.Category_CATEGORY_DISASTER,
		}}},
	}})

	raw := readRawSend(t, c)
	var msg dorv1.ServerMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode server message: %v", err)
	}
	batch := msg.GetDeltas()
	if batch == nil || len(batch.Deltas) != 2 {
		t.Fatalf("live batch = %v, want one no-op and one matching wire body", msg.GetBody())
	}
	if batch.Deltas[0].Seq != 1 || batch.Deltas[0].Body != nil {
		t.Fatalf("first delta = %+v, want seq-only no-op 1", batch.Deltas[0])
	}
	if wire := batch.Deltas[1].GetWire(); wire == nil || wire.ArticleId != "keep" {
		t.Fatalf("second delta = %+v, want the matching wire body", batch.Deltas[1])
	}
	expectNoLeak(t, raw, "drop", "private title")
}
