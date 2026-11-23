package push

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
)

func pushTestStore(t *testing.T) (*bus.Bus, *alert.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	st, err := alert.New(ctx, b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("open alert store: %v", err)
	}
	return b, st
}

type countRoundTripper struct{ calls atomic.Int32 }

func (r *countRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, nil
}

func TestSendReturnsPushSubscriptionReadErrorWithoutMarkingSuccess(t *testing.T) {
	_, st := pushTestStore(t)
	a, err := st.Create(context.Background(), alert.Alert{ID: "alert", Owner: "owner", Prompt: "wildfire", Enabled: true, Channels: []string{"push"}})
	if err != nil {
		t.Fatal(err)
	}
	blob, err := EncryptSubscription(make([]byte, 32), Subscription{Endpoint: "https://push.invalid/subscription"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SavePush(context.Background(), "owner", "subscription", blob); err != nil {
		t.Fatal(err)
	}
	transport := &countRoundTripper{}
	sender := &Sender{alerts: st, key: make([]byte, 32), http: &http.Client{Transport: transport}, timeout: time.Second, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hit := alert.Hit{AlertID: a.ID, StoryID: "story", Level: "new", FiredAt: time.Now(), Title: "Wildfire", Prompt: "wildfire"}
	if err := sender.Send(ctx, a, hit); err == nil {
		t.Fatal("Send returned nil after push-subscription read failed")
	}
	if got := transport.calls.Load(); got != 0 {
		t.Fatalf("external push attempts = %d, want none when subscription lookup fails", got)
	}
	// The receipt must remain absent; a later retry must still be able to send.
	if delivered, err := st.PushDelivered(context.Background(), hit, "subscription"); err != nil || delivered {
		t.Fatalf("push receipt after failed lookup = %v, %v, want false", delivered, err)
	}
}
