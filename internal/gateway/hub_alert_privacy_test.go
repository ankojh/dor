package gateway

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func alertPrivacyClient(h *hub, sessionID string) *client {
	c := newClient(h, nil, nil, nil, projectionLogger())
	c.sessionID = sessionID
	h.addClient(c)
	h.mu.Lock()
	c.ready = true
	h.mu.Unlock()
	return c
}

func expectNoopDelta(t *testing.T, c *client, seq uint64) []byte {
	t.Helper()
	data := readRawSend(t, c)
	var msg dorv1.ServerMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode server message: %v", err)
	}
	batch := msg.GetDeltas()
	if batch == nil || len(batch.Deltas) != 1 {
		t.Fatalf("message is not one delta: %v", msg.GetBody())
	}
	delta := batch.Deltas[0]
	if delta.Seq != seq || delta.Body != nil {
		t.Fatalf("delta = seq %d body %v, want seq-only no-op %d", delta.Seq, delta.Body, seq)
	}
	return data
}

// readRawSends drains everything queued, keeping the exact wire bytes so a
// test can assert a filtered body never reached this client.
func readRawSends(t *testing.T, c *client) [][]byte {
	t.Helper()
	var out [][]byte
	for {
		select {
		case data := <-c.send:
			out = append(out, data)
		default:
			return out
		}
	}
}

func decodeServerMessages(t *testing.T, raws [][]byte) []*dorv1.ServerMessage {
	t.Helper()
	out := make([]*dorv1.ServerMessage, 0, len(raws))
	for _, data := range raws {
		msg := &dorv1.ServerMessage{}
		if err := proto.Unmarshal(data, msg); err != nil {
			t.Fatalf("decode server message: %v", err)
		}
		out = append(out, msg)
	}
	return out
}

func noopSeqsFromMessages(msgs []*dorv1.ServerMessage) []uint64 {
	var seqs []uint64
	for _, msg := range msgs {
		batch := msg.GetDeltas()
		if batch == nil {
			continue
		}
		for _, delta := range batch.Deltas {
			if delta.Body == nil {
				seqs = append(seqs, delta.Seq)
			}
		}
	}
	return seqs
}

func expectNoLeak(t *testing.T, raw []byte, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("frame leaked %q: %s", secret, raw)
		}
	}
}

func alertIDFromMessage(t *testing.T, data []byte) string {
	t.Helper()
	var msg dorv1.ServerMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode server message: %v", err)
	}
	batch := msg.GetDeltas()
	if batch == nil || len(batch.Deltas) != 1 {
		t.Fatalf("message has %v deltas, want exactly one", msg.GetBody())
	}
	hit := batch.Deltas[0].GetAlert()
	if hit == nil {
		t.Fatalf("delta is not an alert hit: %v", batch.Deltas[0])
	}
	return hit.AlertId
}

func alertIDsFromMessages(t *testing.T, msgs []*dorv1.ServerMessage) []string {
	t.Helper()
	var ids []string
	for _, msg := range msgs {
		batch := msg.GetDeltas()
		if batch == nil {
			continue
		}
		for _, delta := range batch.Deltas {
			if hit := delta.GetAlert(); hit != nil {
				ids = append(ids, hit.AlertId)
			}
		}
	}
	return ids
}

func TestAlertDeltasAreVisibleOnlyToTheOwningSession(t *testing.T) {
	h := newHub(nil, projectionLogger())
	owners := map[string]string{
		"alert-a": "session-a",
		"alert-b": "session-b",
	}
	h.alertOwner = func(alertID string) string { return owners[alertID] }

	a := alertPrivacyClient(h, "session-a")
	b := alertPrivacyClient(h, "session-b")

	// Live delivery: only the owning client receives the hit.
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq: 1,
		Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{
			AlertId: "alert-a", StoryId: "story-1", Level: "new", Prompt: "wildfire", Title: "Story",
		}},
	}}})
	if got := alertIDFromMessage(t, readRawSend(t, a)); got != "alert-a" {
		t.Fatalf("session A received alert %q, want alert-a", got)
	}
	expectNoLeak(t, expectNoopDelta(t, b, 1), "alert-a", "wildfire")

	// An unowned (legacy) alert reaches nobody, and its prompt never leaks.
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq: 2,
		Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{
			AlertId: "alert-unowned", StoryId: "story-2", Level: "new", Prompt: "private prompt", Title: "Legacy",
		}},
	}}})
	expectNoLeak(t, expectNoopDelta(t, a, 2), "alert-unowned", "private prompt")
	expectNoLeak(t, expectNoopDelta(t, b, 2), "alert-unowned", "private prompt")

	// Replay: the ring still holds both alerts, so a Hello/resume for each
	// session must project the same ownership rule. Every suppressed alert
	// still occupies its sequence as a no-op.
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{Seq: 0}, nil
	}
	h.resume(a, 0)
	h.resume(b, 0)

	aRaw := readRawSends(t, a)
	bRaw := readRawSends(t, b)
	aMsgs := decodeServerMessages(t, aRaw)
	bMsgs := decodeServerMessages(t, bRaw)
	if len(aMsgs) == 0 || aMsgs[0].GetSnapshot() == nil {
		t.Fatalf("session A replay has no snapshot: %v", aMsgs)
	}
	if len(bMsgs) == 0 || bMsgs[0].GetSnapshot() == nil {
		t.Fatalf("session B replay has no snapshot: %v", bMsgs)
	}
	if ids := alertIDsFromMessages(t, aMsgs); len(ids) != 1 || ids[0] != "alert-a" {
		t.Fatalf("session A replay alerts = %v, want [alert-a]", ids)
	}
	if ids := alertIDsFromMessages(t, bMsgs); len(ids) != 0 {
		t.Fatalf("session B replay alerts = %v, want none", ids)
	}
	if got := noopSeqsFromMessages(aMsgs); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("session A replay no-op seqs = %v, want [2]", got)
	}
	if got := noopSeqsFromMessages(bMsgs); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("session B replay no-op seqs = %v, want [1 2]", got)
	}
	for _, raw := range bRaw {
		expectNoLeak(t, raw, "alert-a", "alert-unowned", "wildfire", "private prompt")
	}
	for _, raw := range aRaw {
		expectNoLeak(t, raw, "alert-unowned", "private prompt")
	}
}
