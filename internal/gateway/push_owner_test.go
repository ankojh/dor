package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/session"
)

func TestPushOwnerComesOnlyFromVerifiedSession(t *testing.T) {
	srv, alerts := newGuardTestServer(t, func(cfg *Config) {
		cfg.Push = PushConfig{PublicKey: "synthetic-vapid-public", EncryptionKey: make([]byte, 32), Delivery: true}
	})
	cookieA, ownerA := issueSession(t, srv)
	cookieB, ownerB := issueSession(t, srv)
	if ownerA == ownerB {
		t.Fatal("issued sessions unexpectedly share an owner")
	}
	body := `{"endpoint":"https://push.example.invalid/a","keys":{"p256dh":"synthetic-p256dh","auth":"synthetic-auth"},"owner":"B_spoofed"}`
	post := func(cookie *http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "http://localhost/api/push/subscriptions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(w, req)
		return w
	}
	responseA := post(cookieA)
	if responseA.Code != http.StatusOK {
		t.Fatalf("session A registration status = %d body=%s; want 200", responseA.Code, responseA.Body.String())
	}
	if strings.Contains(responseA.Body.String(), "B_spoofed") || strings.Contains(responseA.Body.String(), ownerA) {
		t.Fatalf("registration response exposed an owner: %s", responseA.Body.String())
	}
	assertPushOwnerCount(t, alerts, ownerA, 1)
	assertPushOwnerCount(t, alerts, "B_spoofed", 0)

	responseB := post(cookieB)
	if responseB.Code != http.StatusOK {
		t.Fatalf("session B registration status = %d body=%s; want 200", responseB.Code, responseB.Body.String())
	}
	assertPushOwnerCount(t, alerts, ownerA, 1)
	assertPushOwnerCount(t, alerts, ownerB, 1)

	// The session middleware replaces an absent/tampered cookie with a newly
	// signed identity. Registration must bind that fresh verified identity and
	// still ignore the asserted body owner.
	tampered := &http.Cookie{Name: session.CookieName, Value: ownerA + "." + strings.Repeat("0", 64), Path: "/"}
	responseFresh := post(tampered)
	if responseFresh.Code != http.StatusOK {
		t.Fatalf("tampered-session registration status = %d body=%s; want 200", responseFresh.Code, responseFresh.Body.String())
	}
	setCookie := responseFresh.Result().Cookies()
	var freshOwner string
	for _, c := range setCookie {
		if c.Name == session.CookieName {
			freshOwner, _ = srv.sessions.Verify(c.Value)
			break
		}
	}
	if freshOwner == "" || freshOwner == ownerA || freshOwner == ownerB || freshOwner == "B_spoofed" {
		t.Fatalf("tampered session yielded fresh owner %q", freshOwner)
	}
	if _, ok := srv.sessions.Verify(setCookieValue(setCookie)); !ok {
		t.Fatalf("fresh session cookie was not a valid signed token: %#v", setCookie)
	}
	assertPushOwnerCount(t, alerts, freshOwner, 1)
	assertPushOwnerCount(t, alerts, "B_spoofed", 0)
	for _, owner := range []string{ownerA, ownerB, freshOwner} {
		records, err := alerts.PushSubscriptions(context.Background(), owner)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0].Owner != owner {
			t.Fatalf("stored registration for %q = %#v; expected one owner-bound record", owner, records)
		}
		// Stored state contains the encrypted envelope, not endpoint JSON.
		if strings.Contains(string(records[0].Blob), "push.example.invalid") {
			t.Fatalf("stored subscription blob for %q contains plaintext endpoint", owner)
		}
	}
}

func assertPushOwnerCount(t *testing.T, alerts *alert.Store, owner string, want int) {
	t.Helper()
	records, err := alerts.PushSubscriptions(context.Background(), owner)
	if err != nil || len(records) != want {
		t.Fatalf("PushSubscriptions(%q) count = %d, %v; want %d", owner, len(records), err, want)
	}
}

func setCookieValue(cookies []*http.Cookie) string {
	for _, c := range cookies {
		if c.Name == session.CookieName {
			return c.Value
		}
	}
	return ""
}
