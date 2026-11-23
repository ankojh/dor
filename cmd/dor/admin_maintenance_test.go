package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/maintenance"
)

func TestAdminMaintenanceStateUnavailableWithoutRuntime(t *testing.T) {
	b := adminTestBus(t)
	t.Cleanup(func() {
		maintenance.SetProcessBlocked(false)
		b.ResumeIngress()
		b.ResumeDownstream()
	})
	handler := adminHandler(b, nil, agents.NewMetrics(), nil)
	req := httptest.NewRequest(http.MethodGet, "/internal/maintenance/state", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("maintenance state status = %d, want %d", resp.Code, http.StatusServiceUnavailable)
	}
	if got := resp.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body struct {
		Available bool   `json:"available"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode maintenance state: %v", err)
	}
	if body.Available {
		t.Fatal("maintenance state reports available without a runtime")
	}
	if body.Error == "" {
		t.Fatal("maintenance state unavailable response has no error")
	}
}

func TestAdminMaintenanceStateIncludesRuntime(t *testing.T) {
	b := adminTestBus(t)
	t.Cleanup(func() {
		maintenance.SetProcessBlocked(false)
		b.ResumeIngress()
		b.ResumeDownstream()
	})
	ctx := context.Background()
	runtime, err := maintenance.NewRuntime(ctx, b, "test-runtime", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("create maintenance runtime: %v", err)
	}
	handler := adminHandler(b, nil, agents.NewMetrics(), runtime)
	req := httptest.NewRequest(http.MethodGet, "/internal/maintenance/state", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("maintenance state status = %d, want %d", resp.Code, http.StatusOK)
	}
	if got := resp.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode maintenance state: %v", err)
	}
	if body.ID != "test-runtime" {
		t.Fatalf("maintenance runtime id = %q, want test-runtime", body.ID)
	}
}
