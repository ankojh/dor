package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/ops"
)

func guardTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newGuardTestServer builds a gateway without binding a port or starting the
// delta consumer. The embedded NATS server and alert store are the same shape
// the other gateway tests use.
func newGuardTestServer(t *testing.T, mutate func(*Config)) (*Server, *alert.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log := guardTestLogger()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: log})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	st, err := alert.New(ctx, b, nil, log)
	if err != nil {
		t.Fatalf("open alert store: %v", err)
	}
	cfg := Config{
		Addr:    "127.0.0.1:0",
		Bus:     b,
		Origins: []string{"localhost:*", "127.0.0.1:*"},
		Alerts:  st,
		Matcher: matcher.New(b, log, nil, embed.New("", 0), nil, config.DefaultThresholds()),
		Ops:     ops.NewHTTP(b, nil, log),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	return srv, st
}

func addOpsCSRF(r *http.Request, token string) {
	r.AddCookie(&http.Cookie{Name: "dor_ops_csrf", Value: token})
	r.Header.Set("X-Dor-Ops-Token", token)
}

func controlRequest(body, token, header string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/ops/control", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.AddCookie(&http.Cookie{Name: "dor_ops_csrf", Value: token})
	}
	if header != "" {
		req.Header.Set("X-Dor-Ops-Token", header)
	}
	return req
}

func TestGatewayCORSRefusesForeignMutationAndAllowsConfiguredOrigin(t *testing.T) {
	srv, store := newGuardTestServer(t, nil)

	type route struct {
		name   string
		method string
		path   string
		body   string
		csrf   bool
	}
	routes := []route{
		{"create alert", http.MethodPost, "/api/alerts", `{"prompt":"wildfire"}`, false},
		{"backtest alert", http.MethodPost, "/api/alerts/backtest", `{"prompt":"wildfire"}`, false},
		{"patch alert", http.MethodPatch, "/api/alerts/alt_missing", `{"enabled":true}`, false},
		{"delete alert", http.MethodDelete, "/api/alerts/alt_missing", ``, false},
		{"mute alert", http.MethodPost, "/api/alerts/alt_missing/mute", `{"story_id":"story-1"}`, false},
		{"push subscription", http.MethodPost, "/api/push/subscriptions", `{"endpoint":"https://push.example/s","keys":{"p256dh":"p","auth":"a"}}`, false},
		{"ops control", http.MethodPost, "/api/ops/control", `{"action":"pause","agent":"x"}`, true},
		{"wrong link report", http.MethodPost, "/api/report-wrong-link", `{"a":"story-a","b":"story-b"}`, false},
	}

	for _, tc := range routes {
		t.Run("foreign/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", "https://unrelated.example")
			if tc.csrf {
				// A valid ops session must still be refused by the origin guard.
				addOpsCSRF(req, strings.Repeat("a", 64))
			}
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", w.Code, w.Body.String())
			}
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
			}
		})
	}

	// The guard runs before the handlers, so no foreign request may have
	// created an alert or captured a wrong-link report.
	if alerts := store.List(context.Background()); len(alerts) != 0 {
		t.Fatalf("foreign requests created %d alerts", len(alerts))
	}
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost/api/report/wrong-link", nil))
	var reports struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reports); err != nil {
		t.Fatalf("decode wrong-link list: %v (body %s)", err, w.Body.String())
	}
	if reports.Count != 0 {
		t.Fatalf("foreign requests stored %d wrong-link reports", reports.Count)
	}

	const origin = "http://localhost:5173"
	opsToken, _ := fetchOpsToken(t, srv)
	for _, tc := range routes {
		t.Run("allowed/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", origin)
			if tc.csrf {
				addOpsCSRF(req, opsToken)
			}
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, req)
			if w.Code == http.StatusForbidden {
				t.Fatalf("allowed origin was refused (body %s)", w.Body.String())
			}
			if w.Code == http.StatusTooManyRequests {
				t.Fatalf("allowed origin was throttled during the table run")
			}
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
				t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, origin)
			}
		})
	}
}

func TestGatewayReadRoutesNeverEmitWildcardCORS(t *testing.T) {
	srv, _ := newGuardTestServer(t, nil)
	const origin = "http://localhost:5173"
	for _, path := range []string{"/api/text?story_id=s1", "/api/stories", "/api/health"} {
		t.Run(path, func(t *testing.T) {
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil))
			if got := w.Header().Get("Access-Control-Allow-Origin"); got == "*" {
				t.Fatalf("%s returned a wildcard CORS header", path)
			}

			req := httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil)
			req.Header.Set("Origin", origin)
			w = httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, req)
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
				t.Fatalf("%s Access-Control-Allow-Origin = %q, want %q", path, got, origin)
			}
		})
	}
}

func TestGatewayTextPlainJSONIsUnsupported(t *testing.T) {
	srv, _ := newGuardTestServer(t, func(cfg *Config) {
		cfg.Ops = ops.NewHTTP(nil, nil, guardTestLogger())
	})
	const origin = "http://localhost:5173"
	for _, path := range []string{"/api/alerts", "/api/ops/control"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://localhost"+path, strings.NewReader(`{"prompt":"wildfire"}`))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("Origin", origin)
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, req)
			if w.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("status = %d, want 415 (body %s)", w.Code, w.Body.String())
			}
		})
	}
}

var opsTokenMetaRE = regexp.MustCompile(`<meta name="dor-ops-token" content="([0-9a-f]{64}\.[0-9a-f]{64})">`)

// fetchOpsToken loads /ops and returns the CSRF token from both the cookie and
// the page meta, checking that the two agree.
func fetchOpsToken(t *testing.T, srv *Server) (string, *http.Cookie) {
	t.Helper()
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost/ops", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /ops status = %d", w.Code)
	}
	var csrf *http.Cookie
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == "dor_ops_csrf" {
			csrf = cookie
			break
		}
	}
	if csrf == nil {
		t.Fatal("GET /ops did not set dor_ops_csrf")
	}
	match := opsTokenMetaRE.FindStringSubmatch(w.Body.String())
	if match == nil {
		t.Fatal("ops page has no dor-ops-token meta content")
	}
	if match[1] != csrf.Value {
		t.Fatalf("meta token = %q, cookie = %q", match[1], csrf.Value)
	}
	return csrf.Value, csrf
}

// tamperToken flips the last signature character of a well-formed token.
func tamperToken(token string) string {
	if len(token) != 129 {
		return token + "0"
	}
	last := token[len(token)-1]
	replacement := byte('0')
	if last == '0' {
		replacement = '1'
	}
	return token[:len(token)-1] + string(replacement)
}

func TestGatewayOpsCSRFRequiresCookieAndHeader(t *testing.T) {
	srv, _ := newGuardTestServer(t, nil)
	token, csrf := fetchOpsToken(t, srv)
	if !csrf.HttpOnly {
		t.Fatal("dor_ops_csrf is not HttpOnly")
	}

	body := `{"action":"pause","agent":"x"}`
	for _, tc := range []struct {
		name   string
		token  string
		header string
		want   int
	}{
		{"missing header", token, "", http.StatusForbidden},
		{"wrong header", token, strings.Repeat("b", 64), http.StatusForbidden},
		// The supervisor is nil in this process, so a valid control request
		// reaches the handler and reports 503, not 403.
		{"valid session", token, token, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, controlRequest(body, tc.token, tc.header))
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.want, w.Body.String())
			}
			if w.Code == http.StatusServiceUnavailable {
				var payload struct {
					OK    bool   `json:"ok"`
					Error string `json:"error"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
					t.Fatalf("decode control response: %v", err)
				}
				if payload.OK {
					t.Fatal("nil supervisor reported ok")
				}
			}
		})
	}
}

func TestGatewayOpsCSRFRejectsForgedAndTamperedTokens(t *testing.T) {
	srv, _ := newGuardTestServer(t, func(cfg *Config) {
		cfg.Ops = ops.NewHTTP(nil, nil, guardTestLogger())
	})
	realToken, _ := fetchOpsToken(t, srv)
	body := `{"action":"pause","agent":"x"}`

	cases := []struct {
		name  string
		token string
	}{
		{"tampered signature", tamperToken(realToken)},
		{"forged token", strings.Repeat("a", 64) + "." + strings.Repeat("b", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, controlRequest(body, tc.token, tc.token))
			if w.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", w.Code, w.Body.String())
			}
		})
	}
}
