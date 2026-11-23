package push

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/ankojh/dor/internal/alert"
)

type pushOwnerTransport struct {
	mu     sync.Mutex
	status int
	urls   []string
	bodies [][]byte
}

func (r *pushOwnerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.urls = append(r.urls, req.URL.String())
	r.bodies = append(r.bodies, body)
	status := r.status
	r.mu.Unlock()
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (r *pushOwnerTransport) snapshot() ([]string, [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	urls := append([]string(nil), r.urls...)
	bodies := make([][]byte, len(r.bodies))
	for i := range r.bodies {
		bodies[i] = append([]byte(nil), r.bodies[i]...)
	}
	return urls, bodies
}

func pushOwnerSubscription(t *testing.T, endpoint string) Subscription {
	t.Helper()
	priv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 16)
	if _, err := rand.Read(auth); err != nil {
		t.Fatal(err)
	}
	sub := Subscription{Endpoint: endpoint}
	sub.Keys.Auth = base64.RawURLEncoding.EncodeToString(auth)
	sub.Keys.P256DH = base64.RawURLEncoding.EncodeToString(priv.PublicKey().Bytes())
	return sub
}

func pushOwnerSender(t *testing.T, st *alert.Store, key []byte, transport *pushOwnerTransport) *Sender {
	t.Helper()
	priv, pub, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(Config{
		Alerts: st, EncryptionKey: key, VAPIDPublic: pub, VAPIDPrivate: priv,
		Subject: "push-owner-test@example.invalid", HTTPClient: &http.Client{Transport: transport},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func TestPushOwnerScopesTransportAndReceiptsByOwnerAndGeneration(t *testing.T) {
	_, st := pushTestStore(t)
	ctx := context.Background()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	a := alert.Alert{ID: "owner-scope-alert", Owner: "owner-A", Prompt: "synthetic private prompt for A", Enabled: true, Channels: []string{"push"}}
	hit := alert.Hit{AlertID: a.ID, StoryID: "synthetic-story", Level: "new", Generation: 4, FiredAt: time.Now(), Title: "Synthetic A hit", Prompt: a.Prompt}
	for id, owner := range map[string]string{"subscription-A": "owner-A", "subscription-B": "owner-B"} {
		blob, err := EncryptSubscription(key, pushOwnerSubscription(t, "https://push.example.invalid/"+id))
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SavePush(ctx, owner, id, blob); err != nil {
			t.Fatal(err)
		}
	}
	// A corrupt foreign blob proves Send never decrypts or deletes records
	// belonging to another owner; PushSubscriptions is the scope boundary.
	if err := st.SavePush(ctx, "owner-B", "subscription-B-corrupt", []byte("not ciphertext")); err != nil {
		t.Fatal(err)
	}
	transport := &pushOwnerTransport{status: http.StatusCreated}
	sender := pushOwnerSender(t, st, key, transport)
	payload, err := json.Marshal(buildNotificationPayload(a, hit))
	if err != nil || !strings.Contains(string(payload), a.Prompt) {
		t.Fatalf("payload = %s, %v; expected A prompt", payload, err)
	}
	if err := sender.Send(ctx, a, hit); err != nil {
		t.Fatal(err)
	}
	urls, bodies := transport.snapshot()
	if len(urls) != 1 || urls[0] != "https://push.example.invalid/subscription-A" {
		t.Fatalf("first send attempted endpoints %v; want A exactly once", urls)
	}
	if len(bodies) != 1 || len(bodies[0]) != 4096 {
		t.Fatalf("encrypted transport bodies sizes = %v; want one 4096-byte encrypted request", bodySizes(bodies))
	}
	if err := sender.Send(ctx, a, hit); err != nil {
		t.Fatal(err)
	}
	urls, _ = transport.snapshot()
	if len(urls) != 1 {
		t.Fatalf("duplicate same generation attempted another delivery: %v", urls)
	}
	if delivered, err := st.PushDelivered(ctx, hit, "subscription-A"); err != nil || !delivered {
		t.Fatalf("A receipt = %v, %v; want recorded", delivered, err)
	}
	if delivered, err := st.PushDelivered(ctx, hit, "subscription-B"); err != nil || delivered {
		t.Fatalf("B receipt = %v, %v; want absent", delivered, err)
	}
	next := hit
	next.Generation++
	if err := sender.Send(ctx, a, next); err != nil {
		t.Fatal(err)
	}
	urls, _ = transport.snapshot()
	if len(urls) != 2 || urls[1] != "https://push.example.invalid/subscription-A" {
		t.Fatalf("next generation attempts = %v; want one new A attempt", urls)
	}
	if delivered, err := st.PushDelivered(ctx, next, "subscription-A"); err != nil || !delivered {
		t.Fatalf("next generation A receipt = %v, %v; want recorded", delivered, err)
	}
	if delivered, err := st.PushDelivered(ctx, next, "subscription-B"); err != nil || delivered {
		t.Fatalf("next generation B receipt = %v, %v; want absent", delivered, err)
	}
	foreign, err := st.PushSubscriptions(ctx, "owner-B")
	if err != nil || len(foreign) != 2 {
		t.Fatalf("foreign B records after A send = %#v, %v; want both intact", foreign, err)
	}
	foreignIDs := map[string]bool{}
	for _, rec := range foreign {
		foreignIDs[rec.ID] = rec.Owner == "owner-B"
	}
	if !foreignIDs["subscription-B"] || !foreignIDs["subscription-B-corrupt"] {
		t.Fatalf("foreign B record IDs after A send = %#v", foreignIDs)
	}
}

func TestPushOwnerUnownedAlertSkipsOwnedSubscriptions(t *testing.T) {
	_, st := pushTestStore(t)
	ctx := context.Background()
	key := make([]byte, 32)
	blob, err := EncryptSubscription(key, pushOwnerSubscription(t, "https://push.example.invalid/owned"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SavePush(ctx, "owner-A", "owned", blob); err != nil {
		t.Fatal(err)
	}
	transport := &pushOwnerTransport{status: http.StatusCreated}
	sender := pushOwnerSender(t, st, key, transport)
	err = sender.Send(ctx, alert.Alert{ID: "legacy-unowned", Prompt: "legacy prompt"}, alert.Hit{AlertID: "legacy-unowned", StoryID: "story", Level: "new", Prompt: "legacy prompt"})
	if err != nil {
		t.Fatalf("Send(unowned alert) = %v, want nil", err)
	}
	urls, _ := transport.snapshot()
	if len(urls) != 0 {
		t.Fatalf("unowned alert attempted delivery to %v", urls)
	}
}

func TestPushOwnerExpiredSubscriptionDeletionDoesNotCrossOwners(t *testing.T) {
	_, st := pushTestStore(t)
	ctx := context.Background()
	key := make([]byte, 32)
	for id, owner := range map[string]string{"expired-A": "owner-A", "still-B": "owner-B"} {
		blob, err := EncryptSubscription(key, pushOwnerSubscription(t, "https://push.example.invalid/"+id))
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SavePush(ctx, owner, id, blob); err != nil {
			t.Fatal(err)
		}
	}
	a := alert.Alert{ID: "expired-alert", Owner: "owner-A", Prompt: "private", Channels: []string{"push"}}
	hit := alert.Hit{AlertID: a.ID, StoryID: "story", Level: "new", Generation: 9, FiredAt: time.Now(), Prompt: a.Prompt}
	transport := &pushOwnerTransport{status: http.StatusGone}
	if err := pushOwnerSender(t, st, key, transport).Send(ctx, a, hit); err != nil {
		t.Fatal(err)
	}
	for owner, wantID := range map[string]string{"owner-A": "", "owner-B": "still-B"} {
		records, err := st.PushSubscriptions(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if wantID == "" && len(records) != 0 {
			t.Fatalf("expired A subscriptions after 410 = %#v; want deleted", records)
		}
		if wantID != "" && (len(records) != 1 || records[0].ID != wantID) {
			t.Fatalf("B subscriptions after A 410 = %#v; want %s untouched", records, wantID)
		}
	}
}

func bodySizes(bodies [][]byte) []int {
	out := make([]int, len(bodies))
	for i := range bodies {
		out[i] = len(bodies[i])
	}
	return out
}
