package gateway

import (
	"context"
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

func expectNoServerMessage(t *testing.T, c *client) {
	t.Helper()
	select {
	case data := <-c.send:
		t.Fatalf("client received an unexpected message: %s", data)
	default:
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
	expectNoServerMessage(t, b)

	// An unowned (legacy) alert reaches nobody, and its prompt never leaks.
	h.addBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq: 2,
		Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{
			AlertId: "alert-unowned", StoryId: "story-2", Level: "new", Prompt: "private prompt", Title: "Legacy",
		}},
	}}})
	expectNoServerMessage(t, a)
	expectNoServerMessage(t, b)

	// Replay: the ring still holds both alerts, so a Hello/resume for each
	// session must project the same ownership rule.
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{Seq: 0}, nil
	}
	h.resume(a, 0)
	h.resume(b, 0)

	aMsgs := readServerMessages(t, a)
	bMsgs := readServerMessages(t, b)
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
}
