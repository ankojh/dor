package session

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
)

func sessionTestSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := NewSigner()
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	return s
}

func TestIssueVerifyRoundTrip(t *testing.T) {
	s := sessionTestSigner(t)
	id, token, err := s.Issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if len(id) != 64 {
		t.Fatalf("id length = %d, want 64", len(id))
	}
	if token != id+"."+s.sign(id) {
		t.Fatalf("token = %q, want id plus its signature", token)
	}
	got, ok := s.Verify(token)
	if !ok || got != id {
		t.Fatalf("Verify(%q) = %q, %v; want %q, true", token, got, ok, id)
	}
	// Whitespace around a pasted cookie value is harmless.
	if got, ok := s.Verify("  " + token + "\n"); !ok || got != id {
		t.Fatalf("Verify with surrounding space = %q, %v; want %q, true", got, ok, id)
	}
}

func TestVerifyRejectsTamperedAndMalformedTokens(t *testing.T) {
	s := sessionTestSigner(t)
	id, token, err := s.Issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	flipLast := func(value string) string {
		last := value[len(value)-1]
		replacement := byte('0')
		if last == '0' {
			replacement = '1'
		}
		return value[:len(value)-1] + string(replacement)
	}

	cases := []struct {
		name  string
		token string
	}{
		{"tampered signature", flipLast(token)},
		{"uppercased digest", strings.ToUpper(token)},
		{"uppercased id", strings.ToUpper(id) + "." + s.sign(id)},
		{"no separator", token[:len(token)-1]},
		{"empty", ""},
		{"short id", id[:63] + "." + s.sign(id)},
		{"short signature", id + "." + token[len(token)-63:]},
		{"non-hex id", strings.Repeat("z", 64) + "." + s.sign(id)},
		{"extra segment", token + ".extra"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := s.Verify(tc.token); ok {
				t.Fatalf("Verify(%q) = %q, true; want rejection", tc.token, got)
			}
		})
	}
}

func TestMiddlewareIssuesSignedHttpOnlyCookieAndContextID(t *testing.T) {
	s := sessionTestSigner(t)
	var seen string
	h := s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = FromContext(r.Context())
		if got := IDFromRequest(r); got != seen {
			t.Errorf("IDFromRequest = %q, want context id %q", got, seen)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost/api/session", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("Set-Cookie count = %d, want 1", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != CookieName {
		t.Fatalf("cookie name = %q, want %q", cookie.Name, CookieName)
	}
	if !cookie.HttpOnly {
		t.Fatal("session cookie is not HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("SameSite = %v, want Lax", cookie.SameSite)
	}
	if cookie.Path != "/" {
		t.Fatalf("Path = %q, want /", cookie.Path)
	}
	id, ok := s.Verify(cookie.Value)
	if !ok {
		t.Fatalf("issued cookie does not verify: %q", cookie.Value)
	}
	if seen != id {
		t.Fatalf("context id = %q, want cookie id %q", seen, id)
	}
}

func TestMiddlewareKeepsValidCookieAndReplacesTamperedCookie(t *testing.T) {
	s := sessionTestSigner(t)
	id, token, err := s.Issue()
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	var seen []string
	h := s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, FromContext(r.Context()))
		w.WriteHeader(http.StatusNoContent)
	}))

	valid := httptest.NewRequest(http.MethodGet, "http://localhost/api/session", nil)
	valid.AddCookie(&http.Cookie{Name: CookieName, Value: token})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, valid)
	if len(w.Result().Cookies()) != 0 {
		t.Fatalf("valid cookie was reissued: %v", w.Result().Cookies())
	}
	if len(seen) != 1 || seen[0] != id {
		t.Fatalf("context ids = %v, want [%q]", seen, id)
	}

	tampered := token[:len(token)-1] + "0"
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/session", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: tampered})
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("tampered cookie did not produce a replacement: %v", cookies)
	}
	replacement, ok := s.Verify(cookies[0].Value)
	if !ok {
		t.Fatalf("replacement cookie does not verify: %q", cookies[0].Value)
	}
	if replacement == id {
		t.Fatal("tampered cookie was accepted and reused its id")
	}
	if len(seen) != 2 || seen[1] != replacement {
		t.Fatalf("context ids = %v, want second id %q", seen, replacement)
	}
}

func sessionTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   t.TempDir(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestLoadOrCreateSharesPersistedSecret(t *testing.T) {
	b := sessionTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := LoadOrCreate(ctx, b)
	if err != nil {
		t.Fatalf("first LoadOrCreate: %v", err)
	}
	second, err := LoadOrCreate(ctx, b)
	if err != nil {
		t.Fatalf("second LoadOrCreate: %v", err)
	}
	if first.secret != second.secret {
		t.Fatal("two signers on one bus did not share the persisted secret")
	}

	id, token, err := first.Issue()
	if err != nil {
		t.Fatalf("issue with first signer: %v", err)
	}
	if got, ok := second.Verify(token); !ok || got != id {
		t.Fatalf("second signer Verify(%q) = %q, %v; want %q, true", token, got, ok, id)
	}

	third, err := LoadOrCreate(ctx, b)
	if err != nil {
		t.Fatalf("third LoadOrCreate: %v", err)
	}
	if third.secret != first.secret {
		t.Fatal("a later LoadOrCreate changed the secret")
	}
	if _, err := LoadOrCreate(ctx, nil); err == nil {
		t.Fatal("LoadOrCreate(nil) returned no error")
	}
}
