package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func alertCreateRequest(prompt string, cookie *http.Cookie) *http.Request {
	body, _ := json.Marshal(map[string]string{"prompt": prompt})
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	return req
}

func TestAlertAndBacktestPromptBounds(t *testing.T) {
	srv, store := newGuardTestServer(t, nil)
	cookie, _ := issueSession(t, srv)
	tooLong := strings.Repeat("x", maxAlertPromptRunes+1)
	atLimit := strings.Repeat("x", maxAlertPromptRunes)

	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, alertCreateRequest(tooLong, cookie))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized alert prompt status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	if alerts := store.List(context.Background()); len(alerts) != 0 {
		t.Fatalf("oversized prompt created %d alerts", len(alerts))
	}

	w = httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, alertCreateRequest(atLimit, cookie))
	if w.Code != http.StatusCreated {
		t.Fatalf("at-limit alert prompt status = %d, want 201 (body %s)", w.Code, w.Body.String())
	}
	var created struct {
		Alert struct {
			ID string `json:"id"`
		} `json:"alert"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	backtestBody, _ := json.Marshal(map[string]string{"prompt": tooLong})
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts/backtest", strings.NewReader(string(backtestBody)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized backtest prompt status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}

	patchBody, _ := json.Marshal(map[string]string{"prompt": tooLong})
	req = httptest.NewRequest(http.MethodPatch, "http://localhost/api/alerts/"+created.Alert.ID, strings.NewReader(string(patchBody)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized patch prompt status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
}
