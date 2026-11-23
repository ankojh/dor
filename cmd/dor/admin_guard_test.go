package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/httpguard"
	"github.com/ankojh/dor/internal/ops"
)

func TestAdminHandlerOriginGuardProtectsOpsControl(t *testing.T) {
	b := adminTestBus(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := agents.NewRunner(nil, log)
	if err := runner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runner.Shutdown(ctx)
	})

	opsHTTP := ops.NewHTTP(nil, nil, log)
	handler := httpguard.NewAllowlist([]string{"localhost:*", "127.0.0.1:*"}).Middleware(
		adminHandler(b, runner, agents.NewMetrics(), nil, opsHTTP))

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8091/api/ops/control", strings.NewReader(`{"action":"pause"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://unrelated.example")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("foreign-origin POST /api/ops/control status = %d, want 403 (body %s)", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://localhost:8091/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("no-origin GET /healthz status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
}
