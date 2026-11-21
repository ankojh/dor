package agents

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

type notifierFakePush struct {
	calls atomic.Int32
	fail  atomic.Int32
}

func (p *notifierFakePush) Send(_ context.Context, _ alert.Alert, _ alert.Hit) error {
	p.calls.Add(1)
	if p.fail.Add(-1) >= 0 {
		return errors.New("temporary push failure")
	}
	return nil
}

func notifierTestStore(t *testing.T, b *bus.Bus) *alert.Store {
	t.Helper()
	st, err := alert.New(context.Background(), b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("open alert store: %v", err)
	}
	return st
}

func notifierTestAlert(t *testing.T, st *alert.Store, channels []string, digest bool, hour int) alert.Alert {
	t.Helper()
	a, err := st.Create(context.Background(), alert.Alert{ID: "alert", Prompt: "wildfire", Enabled: true, Strictness: .8, Channels: channels, DailyDigest: digest, DigestHourUTC: hour, NtfyTopic: "test-topic"})
	if err != nil {
		t.Fatalf("create alert: %v", err)
	}
	return a
}

func notifierHit(alertID, storyID string, generation uint64, at time.Time) alert.Hit {
	return alert.Hit{AlertID: alertID, StoryID: storyID, Level: "new", Generation: generation, FiredAt: at, Title: "Wildfire update", Prompt: "wildfire", SourceCount: 2}
}

func TestNotifierHandleOnlyDurablyEnqueuesAndDuplicateReplayIsIdempotent(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	a := notifierTestAlert(t, st, []string{"push"}, false, 0)
	push := &notifierFakePush{}
	n := &notifier{cfg: NotifierConfig{Alerts: st, Push: push, Now: time.Now, MinInterval: time.Minute}, log: testLogger()}
	h := notifierHit(a.ID, "story", 2, time.Now().UTC())
	wire := &dorv1.AlertHit{AlertId: h.AlertID, StoryId: h.StoryID, Level: h.Level, Generation: h.Generation, FiredAt: h.FiredAt.Unix(), Title: h.Title, Prompt: h.Prompt, SourceCount: uint32(h.SourceCount)}
	data, _ := proto.Marshal(wire)
	msg := &Message{Data: data}
	if err := n.handle(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
	if err := n.handle(context.Background(), msg, nil); err != nil {
		t.Fatalf("redelivery enqueue: %v", err)
	}
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending deliveries = %d, want one idempotent item", len(pending))
	}
	if push.calls.Load() != 0 {
		t.Fatalf("handle sent externally %d times; it should only enqueue", push.calls.Load())
	}
	n.drain(context.Background())
	if push.calls.Load() != 1 {
		t.Fatalf("delivery worker calls = %d, want one", push.calls.Load())
	}
	reopened := notifierTestStore(t, b)
	n.cfg.Alerts = reopened
	n.drain(context.Background())
	if push.calls.Load() != 1 {
		t.Fatalf("replayed completed delivery sent again: calls = %d", push.calls.Load())
	}
	pending, err = reopened.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("reopened store has pending completed item: %+v", pending)
	}
}

func TestNotifierRateLimitedHitStaysPendingAndSendsLater(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	a := notifierTestAlert(t, st, []string{"push"}, false, 0)
	push := &notifierFakePush{}
	now := time.Now().UTC()
	n := &notifier{cfg: NotifierConfig{Alerts: st, Push: push, Now: func() time.Time { return now }, MinInterval: time.Minute}, log: testLogger()}
	for _, id := range []string{"one", "two"} {
		h := notifierHit(a.ID, id, 0, now)
		if err := st.Enqueue(context.Background(), a, h); err != nil {
			t.Fatal(err)
		}
	}
	n.drain(context.Background())
	if got := push.calls.Load(); got != 1 {
		t.Fatalf("first drain sent %d hits, want one due to rate limit", got)
	}
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("rate limited hit was dropped or not retained: %+v", pending)
	}
	// A drain visits an alert once, so a tied second task can remain untouched
	// until the next 250 ms worker pass. That pass must apply the reservation.
	n.drain(context.Background())
	pending, err = st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || !pending[0].DueAt.After(now) {
		t.Fatalf("rate limited hit was dropped or not delayed: %+v", pending)
	}
	now = now.Add(time.Minute + time.Second)
	n.drain(context.Background())
	if got := push.calls.Load(); got != 2 {
		t.Fatalf("later drain sent %d total hits, want both", got)
	}
	pending, err = st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending deliveries after retry = %+v, want empty", pending)
	}
}

func TestNotifierRetryDoesNotResendSuccessfulNtfyWhenPushFails(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	a := notifierTestAlert(t, st, []string{"ntfy", "push"}, false, 0)
	var ntfyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ntfyCalls.Add(1); w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	push := &notifierFakePush{}
	push.fail.Store(1)
	now := time.Now().UTC()
	n := &notifier{cfg: NotifierConfig{Alerts: st, NtfyBaseURL: server.URL, Push: push, HTTP: server.Client(), Now: func() time.Time { return now }, MinInterval: time.Minute}, log: testLogger()}
	h := notifierHit(a.ID, "story", 0, now)
	if err := st.Enqueue(context.Background(), a, h); err != nil {
		t.Fatal(err)
	}
	n.drain(context.Background())
	if ntfyCalls.Load() != 1 || push.calls.Load() != 1 {
		t.Fatalf("first attempt ntfy/push = %d/%d, want 1/1", ntfyCalls.Load(), push.calls.Load())
	}
	now = now.Add(3 * time.Second)
	n.drain(context.Background())
	if ntfyCalls.Load() != 1 || push.calls.Load() != 2 {
		t.Fatalf("retry resent successful ntfy or skipped push: ntfy/push = %d/%d, want 1/2", ntfyCalls.Load(), push.calls.Load())
	}
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after successful retry = %+v", pending)
	}
}

func TestNotifierDigestBatchesByUTCDayAndReopenedStoreRetainsFrozenWork(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	a := notifierTestAlert(t, st, []string{"push"}, true, 16)
	push := &notifierFakePush{}
	// Firing at 15:50 UTC schedules the digest for 16:00 on the same day.
	fire := time.Date(2026, 11, 21, 15, 50, 0, 0, time.UTC)
	now := fire.Add(10 * time.Minute)
	n := &notifier{cfg: NotifierConfig{Alerts: st, Push: push, Now: func() time.Time { return now }, MinInterval: time.Minute}, log: testLogger()}
	for _, id := range []string{"a", "b"} {
		if err := st.Enqueue(context.Background(), a, notifierHit(a.ID, id, 0, fire)); err != nil {
			t.Fatal(err)
		}
	}
	// Pending input is durable over a store reopen and the daily batch sends once.
	reopened := notifierTestStore(t, b)
	n.cfg.Alerts = reopened
	n.drain(context.Background())
	if push.calls.Load() != 1 {
		t.Fatalf("digest sends = %d, want one summary per alert/day/channel", push.calls.Load())
	}
	pending, err := reopened.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending digest members after success = %+v", pending)
	}
}

func TestNotifierEnqueueFailureReturnsErrorSoInputIsNotAcknowledged(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	a := notifierTestAlert(t, st, []string{"push"}, false, 0)
	// Keep the alert readable while removing the durable destination. Lookup can
	// still succeed, but Enqueue must fail and propagate through Handle.
	if err := b.JS().DeleteKeyValue(context.Background(), bus.KVBucketAlertDelivery); err != nil {
		t.Fatalf("delete delivery bucket: %v", err)
	}
	n := &notifier{cfg: NotifierConfig{Alerts: st}, log: testLogger()}
	h := notifierHit(a.ID, "story", 0, time.Now().UTC())
	data, err := proto.Marshal(&dorv1.AlertHit{AlertId: h.AlertID, StoryId: h.StoryID, Level: h.Level, FiredAt: h.FiredAt.Unix(), Title: h.Title, Prompt: h.Prompt})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.handle(context.Background(), &Message{Data: data}, nil); err == nil {
		t.Fatal("handler returned nil after durable enqueue failed; input could be acknowledged and lost")
	}
}

func TestNewNotifierStartsAndStopsConsumerAndDeliveryWorker(t *testing.T) {
	b := testBus(t, t.TempDir())
	st := notifierTestStore(t, b)
	notifierTestAlert(t, st, []string{"push"}, false, 0)
	agent, err := NewNotifier(b, NewMetrics(), testLogger(), NotifierConfig{Alerts: st, Push: &notifierFakePush{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := agent.Start(ctx); err != nil {
		t.Fatalf("start notifier: %v", err)
	}
	running, ok := agent.(interface{ Running() bool })
	if !ok {
		t.Fatal("notifier wrapper does not expose Running")
	}
	if !running.Running() {
		t.Fatal("notifier should report running after both loops start")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	if err := agent.Stop(stopCtx); err != nil {
		t.Fatalf("stop notifier: %v", err)
	}
	if running.Running() {
		t.Fatal("notifier should report stopped after both loops stop")
	}
}
