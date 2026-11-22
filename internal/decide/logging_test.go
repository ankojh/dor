package decide

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ankojh/dor/config"
)

// TestDecideLogsOneLine checks that both constructors pass their logger into
// ClientConfig: a successful Decide emits exactly one jev.decide record that
// carries the pinned model and the billed cost.
func TestDecideLogsOneLine(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		model string
		build func(config.Models, string, string, Budget, *slog.Logger) (*Client, error)
	}{
		{
			name:  "openrouter",
			body:  okBody,
			model: "typesafe/jev-1.14",
			build: NewClientFromModels,
		},
		{
			name:  "typesafe",
			body:  typeSafeBody,
			model: "jev-1.14.0",
			build: NewTypeSafeClientFromModels,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, nil))
			c, err := tc.build(config.DefaultModels(), srv.URL, "key", &fakeBudget{}, log)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Decide(context.Background(), simpleReq())
			if err != nil {
				t.Fatal(err)
			}
			if resp.Usage.CostUSD <= 0 {
				t.Fatalf("the server body must carry a positive cost, got %+v", resp.Usage)
			}

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) != 1 || lines[0] == "" {
				t.Fatalf("log lines = %d (%q), want exactly one", len(lines), buf.String())
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
				t.Fatalf("log line is not JSON: %v: %q", err, lines[0])
			}
			if rec["msg"] != "jev.decide" {
				t.Errorf("msg = %v, want jev.decide", rec["msg"])
			}
			if rec["model"] != tc.model {
				t.Errorf("model = %v, want %s", rec["model"], tc.model)
			}
			cost, ok := rec["cost_usd"].(float64)
			if !ok {
				t.Fatalf("cost_usd missing or not a number: %v", rec["cost_usd"])
			}
			if math.Abs(cost-resp.Usage.CostUSD) > 1e-12 {
				t.Errorf("logged cost = %v, want %v", cost, resp.Usage.CostUSD)
			}
			if resp.ID != "" && rec["request_id"] != resp.ID {
				t.Errorf("request_id = %v, want %s", rec["request_id"], resp.ID)
			}
		})
	}
}

// A nil logger discards the jev.decide line without panicking.
func TestDecideNilLoggerDoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, okBody)
	}))
	defer srv.Close()

	c, err := NewClientFromModels(config.DefaultModels(), srv.URL, "key", &fakeBudget{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decide(context.Background(), simpleReq()); err != nil {
		t.Fatalf("decide with a nil logger: %v", err)
	}

	ts, err := NewTypeSafeClientFromModels(config.DefaultModels(), srv.URL, "key", &fakeBudget{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts.Decide(context.Background(), simpleReq()); err != nil {
		t.Fatalf("typesafe decide with a nil logger: %v", err)
	}
}
