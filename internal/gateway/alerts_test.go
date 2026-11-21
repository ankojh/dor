package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
)

func TestAlertHitsHTTPEncodesUint64GenerationAsExactDecimalString(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	defer b.Close()
	st, err := alert.New(ctx, b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("open alert store: %v", err)
	}
	generation := ^uint64(0)
	if err := st.RecordHit(ctx, alert.Hit{AlertID: "large", StoryID: "story", Level: "escalated", Generation: generation, FiredAt: time.Now(), Title: "Large generation"}); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Addr: "127.0.0.1:0", Bus: b, Alerts: st})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/alerts/hits?alert_id=large", nil)
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET /api/alerts/hits status = %d body=%s", w.Code, w.Body.String())
	}
	var payload struct {
		Hits []map[string]any `json:"hits"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Hits) != 1 {
		t.Fatalf("hits length = %d, want 1", len(payload.Hits))
	}
	if got, ok := payload.Hits[0]["generation"].(string); !ok || got != "18446744073709551615" {
		t.Fatalf("generation JSON value = %#v, want exact uint64 decimal string", payload.Hits[0]["generation"])
	}
}
