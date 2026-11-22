package httpguard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAllowlistMatchesPatternsAndSameOrigin(t *testing.T) {
	a := NewAllowlist([]string{" localhost:* ", "", "127.0.0.1:*"})

	cases := []struct {
		name        string
		origin      string
		requestHost string
		want        bool
	}{
		{"wildcard host:port", "http://localhost:5173", "127.0.0.1:8080", true},
		{"loopback wildcard", "http://127.0.0.1:5173", "localhost:8080", true},
		{"same origin without pattern", "https://app.example.com", "app.example.com", true},
		{"same origin with port", "http://app.example.com:8443", "app.example.com:8443", true},
		{"foreign origin", "https://unrelated.example", "localhost:8080", false},
		{"foreign origin same suffix", "https://notlocalhost.example", "localhost:8080", false},
		{"origin null", "null", "localhost:8080", false},
		{"origin NULL uppercase", "NULL", "localhost:8080", false},
		{"empty origin", "", "localhost:8080", false},
		{"not a URL", "not an origin", "localhost:8080", false},
		{"unsupported scheme", "ftp://localhost:5173", "localhost:8080", false},
		{"missing host", "http://", "localhost:8080", false},
		{"malformed host", "http://[::1", "localhost:8080", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Allows(tc.origin, tc.requestHost); got != tc.want {
				t.Fatalf("Allows(%q, %q) = %v, want %v", tc.origin, tc.requestHost, got, tc.want)
			}
		})
	}
}

func TestMiddlewareRefusesForeignOriginWithoutCORSHeader(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})
	called := false
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/stories", nil)
	req.Header.Set("Origin", "https://unrelated.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
	if called {
		t.Fatal("handler ran for a foreign origin")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}
}

func TestMiddlewareEchoesAllowedOriginNeverWildcard(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	origin := "http://localhost:5173"
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/stories", nil)
	req.Header.Set("Origin", origin)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, origin)
	}
}

func TestMiddlewareAnswersAllowedPreflight(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})
	called := false
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodOptions, "http://localhost/api/alerts", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if called {
		t.Fatal("preflight reached the wrapped handler")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	methods := w.Header().Get("Access-Control-Allow-Methods")
	for _, want := range []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"} {
		if !strings.Contains(methods, want) {
			t.Fatalf("Access-Control-Allow-Methods = %q, missing %s", methods, want)
		}
	}
	headers := w.Header().Get("Access-Control-Allow-Headers")
	for _, want := range []string{"Content-Type", "X-Dor-Ops-Token"} {
		if !strings.Contains(headers, want) {
			t.Fatalf("Access-Control-Allow-Headers = %q, missing %s", headers, want)
		}
	}
}

func TestMiddlewareRefusesForeignPreflight(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})
	h := a.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler ran for a foreign preflight")
	}))
	req := httptest.NewRequest(http.MethodOptions, "http://localhost/api/alerts", nil)
	req.Header.Set("Origin", "https://unrelated.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}
}

func TestMiddlewareMutationFetchMetadata(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	// No Origin: the browser metadata is the only evidence, so a cross-site
	// mutation is refused.
	post := httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts", strings.NewReader(`{}`))
	post.Header.Set("Sec-Fetch-Site", "cross-site")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, post)
	if w.Code != http.StatusForbidden {
		t.Fatalf("no-origin cross-site POST status = %d, want 403", w.Code)
	}

	// An explicitly allowed Origin is the trust decision; a direct API origin
	// may be cross-site (e.g. localhost web to 127.0.0.1 gateway).
	post = httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts", strings.NewReader(`{}`))
	post.Header.Set("Origin", "http://localhost:5173")
	post.Header.Set("Sec-Fetch-Site", "cross-site")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, post)
	if w.Code != http.StatusNoContent {
		t.Fatalf("allowed-origin cross-site POST status = %d, want 204", w.Code)
	}

	get := httptest.NewRequest(http.MethodGet, "http://localhost/api/stories", nil)
	get.Header.Set("Origin", "http://localhost:5173")
	get.Header.Set("Sec-Fetch-Site", "cross-site")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, get)
	if w.Code != http.StatusNoContent {
		t.Fatalf("cross-site GET status = %d, want 204", w.Code)
	}
}

func TestMiddlewareRefusesNonFetchNoOriginAPIRequests(t *testing.T) {
	a := NewAllowlist([]string{"localhost:*"})

	cases := []struct {
		name    string
		origin  string
		site    string
		mode    string
		want    int
		reached bool
	}{
		{"no-cors embed", "", "", "no-cors", http.StatusForbidden, false},
		{"navigate form", "", "", "navigate", http.StatusForbidden, false},
		{"cors dev fetch with allowed origin", "http://localhost:5173", "cross-site", "cors", http.StatusNoContent, true},
		{"cors fetch without origin", "", "", "cors", http.StatusNoContent, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest(http.MethodGet, "http://localhost/api/text?story_id=s1", nil)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.site != "" {
				req.Header.Set("Sec-Fetch-Site", tc.site)
			}
			if tc.mode != "" {
				req.Header.Set("Sec-Fetch-Mode", tc.mode)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			if called != tc.reached {
				t.Fatalf("handler reached = %v, want %v", called, tc.reached)
			}
		})
	}
}

func TestRequireJSON(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
		want        bool
		wantStatus  int
	}{
		{"application/json", "application/json", `{}`, true, 200},
		{"json with charset", "application/json; charset=utf-8", `{}`, true, 200},
		{"suffix json", "application/vnd.dor+json", `{}`, true, 200},
		{"text plain", "text/plain", `{}`, false, http.StatusUnsupportedMediaType},
		{"form urlencoded", "application/x-www-form-urlencoded", `{}`, false, http.StatusUnsupportedMediaType},
		{"empty body no type", "", "", true, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body *strings.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			var req *http.Request
			if body == nil {
				req = httptest.NewRequest(http.MethodPost, "/api/alerts", nil)
			} else {
				req = httptest.NewRequest(http.MethodPost, "/api/alerts", body)
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			w := httptest.NewRecorder()
			got := RequireJSON(w, req)
			if got != tc.want {
				t.Fatalf("RequireJSON = %v, want %v", got, tc.want)
			}
			if !tc.want && w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if !tc.want && w.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatalf("denial leaked Access-Control-Allow-Origin")
			}
		})
	}
}

func TestRequireJSONRejectsUndeclaredBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/alerts", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	if RequireJSON(w, req) {
		t.Fatal("accepted a body without a Content-Type")
	}
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", w.Code)
	}
}

func TestLimiterBurstRefillAndPerKeyIsolation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := NewLimiter(60, 2) // one token per second, burst two
	l.now = func() time.Time { return now }

	if !l.Allow("a") || !l.Allow("a") {
		t.Fatal("burst of two was not admitted")
	}
	if l.Allow("a") {
		t.Fatal("third request inside the burst was admitted")
	}
	now = now.Add(time.Second)
	if !l.Allow("a") {
		t.Fatal("one second of refill did not admit a request")
	}
	if l.Allow("a") {
		t.Fatal("refill admitted more than one token")
	}

	// A separate key has an independent bucket.
	other := NewLimiter(60, 1)
	other.now = func() time.Time { return now }
	if !other.Allow("b") {
		t.Fatal("first request for key b denied")
	}
	if other.Allow("b") {
		t.Fatal("second request for key b admitted")
	}
	if !other.Allow("c") {
		t.Fatal("key c was affected by key b")
	}
}

func TestLimiterMiddlewareReturnsRetryAfter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := NewLimiter(60, 1)
	l.now = func() time.Time { return now }
	h := l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "http://localhost/api/text", nil)
		r.RemoteAddr = "192.0.2.7:1234"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := req(); w.Code != http.StatusNoContent {
		t.Fatalf("first status = %d, want 204", w.Code)
	}
	w := req()
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "60" {
		t.Fatalf("Retry-After = %q, want 60", got)
	}
}

func TestClientKeyStripsPort(t *testing.T) {
	if got := ClientKey("192.0.2.10:1234"); got != "192.0.2.10" {
		t.Fatalf("ClientKey = %q", got)
	}
	if got := ClientKey("192.0.2.10"); got != "192.0.2.10" {
		t.Fatalf("ClientKey = %q", got)
	}
	if got := ClientKey(""); got != "unknown" {
		t.Fatalf("ClientKey = %q, want unknown", got)
	}
}
