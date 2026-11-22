package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/alert"
)

type alertResponse struct {
	Alert alert.Alert `json:"alert"`
}

type alertListResponse struct {
	Alerts []alert.Alert `json:"alerts"`
}

// hitWire mirrors the HTTP boundary: generation is a decimal string so a
// uint64 generation keeps full precision in JSON.
type hitWire struct {
	AlertID    string `json:"alert_id"`
	StoryID    string `json:"story_id"`
	Level      string `json:"level"`
	Generation string `json:"generation"`
}

type hitListResponse struct {
	Hits []hitWire `json:"hits"`
}

func gatewayJSONRequest(t *testing.T, method, path, body string, cookie *http.Cookie) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	req.RemoteAddr = "192.0.2.70:4000"
	return req
}

func serveGatewayJSON(t *testing.T, srv *Server, method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := gatewayJSONRequest(t, method, path, body, cookie)
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	return w
}

func createOwnedAlert(t *testing.T, srv *Server, cookie *http.Cookie) alert.Alert {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts", strings.NewReader(`{"prompt":"wildfire"}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	req.RemoteAddr = "192.0.2.70:4000"
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("create alert status = %d, want 201 (body %s)", w.Code, w.Body.String())
	}
	var payload alertResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode created alert: %v", err)
	}
	if payload.Alert.ID == "" {
		t.Fatal("created alert has no id")
	}
	return payload.Alert
}

func listOwnedAlerts(t *testing.T, srv *Server, cookie *http.Cookie) []alert.Alert {
	t.Helper()
	w := serveGatewayJSON(t, srv, http.MethodGet, "/api/alerts", "", cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("list alerts status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var payload alertListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode alerts: %v", err)
	}
	return payload.Alerts
}

func listHits(t *testing.T, srv *Server, cookie *http.Cookie, query string) []hitWire {
	t.Helper()
	w := serveGatewayJSON(t, srv, http.MethodGet, "/api/alerts/hits"+query, "", cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("list hits status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var payload hitListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode hits: %v", err)
	}
	return payload.Hits
}

func assertUncacheable(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	vary := strings.Join(w.Header().Values("Vary"), ", ")
	if !strings.Contains(vary, "Cookie") {
		t.Fatalf("Vary = %q, want Cookie", vary)
	}
}

func TestSessionAndAlertReadsAreUncacheable(t *testing.T) {
	srv, _ := newGuardTestServer(t, nil)
	cookie, _ := issueSession(t, srv)
	created := createOwnedAlert(t, srv, cookie)

	// A newly issued session must not be cached or replayed to another client.
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost/api/session", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/session status = %d", w.Code)
	}
	assertUncacheable(t, w)
	if len(w.Result().Cookies()) == 0 {
		t.Fatal("GET /api/session did not issue a session")
	}

	// An existing session's owner-scoped reads must not be cached either.
	w = serveGatewayJSON(t, srv, http.MethodGet, "/api/alerts", "", cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("list alerts status = %d", w.Code)
	}
	assertUncacheable(t, w)
	if len(w.Result().Cookies()) != 0 {
		t.Fatalf("valid session was reissued: %v", w.Result().Cookies())
	}

	w = serveGatewayJSON(t, srv, http.MethodGet, "/api/alerts/hits?alert_id="+created.ID, "", cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("list hits status = %d", w.Code)
	}
	assertUncacheable(t, w)
}

func TestAlertsAreScopedToTheCreatingSession(t *testing.T) {
	srv, store := newGuardTestServer(t, nil)
	cookieA, ownerA := issueSession(t, srv)
	cookieB, _ := issueSession(t, srv)

	created := createOwnedAlert(t, srv, cookieA)
	if created.Owner != ownerA {
		t.Fatalf("created alert owner = %q, want %q", created.Owner, ownerA)
	}
	if err := store.RecordHit(context.Background(), alert.Hit{
		AlertID: created.ID, StoryID: "story-1", Level: "new", FiredAt: time.Now().UTC(),
		Title: "Story one", Prompt: created.Prompt,
	}); err != nil {
		t.Fatalf("record hit: %v", err)
	}

	aAlerts := listOwnedAlerts(t, srv, cookieA)
	if len(aAlerts) != 1 || aAlerts[0].ID != created.ID {
		t.Fatalf("session A alerts = %v, want the created alert", aAlerts)
	}
	if bAlerts := listOwnedAlerts(t, srv, cookieB); len(bAlerts) != 0 {
		t.Fatalf("session B alerts = %v, want none", bAlerts)
	}

	blocked := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"patch", http.MethodPatch, "/api/alerts/" + created.ID, `{"enabled":false}`},
		{"delete", http.MethodDelete, "/api/alerts/" + created.ID, ""},
		{"mute", http.MethodPost, "/api/alerts/" + created.ID + "/mute", `{"story_id":"story-1"}`},
		{"hits", http.MethodGet, "/api/alerts/hits?alert_id=" + created.ID, ""},
	}
	for _, tc := range blocked {
		t.Run("foreign/"+tc.name, func(t *testing.T) {
			w := serveGatewayJSON(t, srv, tc.method, tc.path, tc.body, cookieB)
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %s)", w.Code, w.Body.String())
			}
		})
	}
	if _, ok := store.Get(context.Background(), created.ID); !ok {
		t.Fatal("a foreign DELETE removed session A's alert")
	}

	w := serveGatewayJSON(t, srv, http.MethodPatch, "/api/alerts/"+created.ID, `{"enabled":false}`, cookieA)
	if w.Code != http.StatusOK {
		t.Fatalf("owner patch status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	w = serveGatewayJSON(t, srv, http.MethodPost, "/api/alerts/"+created.ID+"/mute", `{"story_id":"story-1"}`, cookieA)
	if w.Code != http.StatusOK {
		t.Fatalf("owner mute status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	if hits := listHits(t, srv, cookieA, "?alert_id="+created.ID); len(hits) != 1 || hits[0].AlertID != created.ID {
		t.Fatalf("owner hits by alert = %v, want the recorded hit", hits)
	}
	if hits := listHits(t, srv, cookieA, ""); len(hits) != 1 || hits[0].AlertID != created.ID {
		t.Fatalf("owner hits without alert_id = %v, want the recorded hit", hits)
	}
	if hits := listHits(t, srv, cookieB, ""); len(hits) != 0 {
		t.Fatalf("foreign hits = %v, want none", hits)
	}
}

func TestBacktestIsSessionIndependent(t *testing.T) {
	srv, _ := newGuardTestServer(t, nil)
	cookieA, _ := issueSession(t, srv)
	cookieB, _ := issueSession(t, srv)

	const body = `{"prompt":"wildfire"}`
	w := serveGatewayJSON(t, srv, http.MethodPost, "/api/alerts/backtest", body, cookieA)
	if w.Code != http.StatusOK {
		t.Fatalf("session A backtest status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	w = serveGatewayJSON(t, srv, http.MethodPost, "/api/alerts/backtest", body, cookieB)
	if w.Code != http.StatusOK {
		t.Fatalf("session B backtest status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
}
