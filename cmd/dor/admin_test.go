package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/bus"
)

func adminTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   t.TempDir(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestAdminHealthOKAndTerminalCounts(t *testing.T) {
	b := adminTestBus(t)
	metrics := agents.NewMetrics()
	echo, err := agents.NewEcho(b, metrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	runner := agents.NewRunner([]agents.Agent{echo}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := runner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runner.Shutdown(ctx)
	})

	server := httptest.NewServer(adminHandler(b, runner, metrics, nil))
	defer server.Close()
	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}
	var body struct {
		Status   string            `json:"status"`
		Bus      string            `json:"bus"`
		NATSURL  string            `json:"nats_url"`
		Agents   map[string]string `json:"agents"`
		Terminal map[string]int64  `json:"terminal"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ok" || body.Bus != "connected" || body.Agents["echo"] != "running" {
		t.Fatalf("health body = %+v", body)
	}
	if _, ok := body.Terminal["echo"]; !ok {
		t.Fatalf("terminal map missing echo: %+v", body.Terminal)
	}
	if body.NATSURL == "" || body.NATSURL != b.RedactedURL() {
		t.Fatalf("nats_url = %q, want redacted %q", body.NATSURL, b.RedactedURL())
	}
}

func TestAdminHealthIs503WhenBusClosed(t *testing.T) {
	b := adminTestBus(t)
	metrics := agents.NewMetrics()
	runner := agents.NewRunner(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := runner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adminHandler(b, runner, metrics, nil))
	defer server.Close()
	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, want 503", resp.StatusCode)
	}
}
