package agents

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// deadlineStubCompleter is an Enricher that always fails with a fixed error.
type deadlineStubCompleter struct {
	err   error
	calls int
}

func (s *deadlineStubCompleter) Complete(context.Context, llm.Job) (llm.Result, error) {
	s.calls++
	return llm.Result{}, s.err
}

// TestEnricherTooSlowDoesNotWarn checks that a backend declining a
// short-deadline job (llm.ErrCLITooSlow) is not reported as a claude fallback
// failure, while a real error still is, and that both take the deterministic
// fallback path.
func TestEnricherTooSlowDoesNotWarn(t *testing.T) {
	cases := map[string]struct {
		err      error
		wantWarn bool
	}{
		"too slow":         {err: llm.ErrCLITooSlow},
		"wrapped slow":     {err: fmt.Errorf("chain: %w", llm.ErrCLITooSlow)},
		"other error":      {err: errors.New("connection refused"), wantWarn: true},
		"other budget":     {err: &llm.BudgetError{Reason: "daily-cap", API: "claude-cli"}, wantWarn: true},
		"context deadline": {err: context.DeadlineExceeded, wantWarn: true},
	}
	article := &dorv1.Article{
		Id:       "a1",
		Title:    "Parliament passes budget after late vote",
		Lede:     "Lawmakers approved the spending plan on Thursday.",
		Dateline: "London",
	}

	for name, tc := range cases {
		t.Run("classify/"+name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			stub := &deadlineStubCompleter{err: tc.err}
			c := &classifier{deps: Deps{Enricher: stub}, log: log}

			if got, ok := c.fromClaude(context.Background(), article, "en"); ok || got != nil {
				t.Fatalf("fromClaude = %+v, %v; want nil,false", got, ok)
			}
			if stub.calls != 1 {
				t.Fatalf("enricher calls = %d, want 1", stub.calls)
			}
			assertFallbackWarning(t, buf.String(), "classify: claude fallback failed", tc.wantWarn)

			// classify still falls back deterministically.
			out := c.classify(context.Background(), article)
			if out.GetBackend() != "rule" || !out.GetFallback() || !out.GetDegraded() {
				t.Errorf("classify fallback = backend %q fallback %v degraded %v, want rule/true/true",
					out.GetBackend(), out.GetFallback(), out.GetDegraded())
			}
		})

		t.Run("plot/"+name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			stub := &deadlineStubCompleter{err: tc.err}
			p := &plotter{deps: Deps{Enricher: stub}, log: log}

			if got, ok := p.fromClaude(context.Background(), article, nil, ""); ok || got != nil {
				t.Fatalf("fromClaude = %+v, %v; want nil,false", got, ok)
			}
			if stub.calls != 1 {
				t.Fatalf("enricher calls = %d, want 1", stub.calls)
			}
			assertFallbackWarning(t, buf.String(), "plot: claude fallback failed", tc.wantWarn)

			// place still returns the deterministic fallback.
			placed := p.place(context.Background(), article)
			if !placed.GetFallback() || !placed.GetDegraded() || placed.GetBackend() == "claude" {
				t.Errorf("plot fallback = backend %q fallback %v degraded %v, want a degraded deterministic result",
					placed.GetBackend(), placed.GetFallback(), placed.GetDegraded())
			}
		})
	}
}

func assertFallbackWarning(t *testing.T, logs, msg string, want bool) {
	t.Helper()
	got := strings.Contains(logs, msg)
	if got != want {
		t.Errorf("log %q contains %q = %v, want %v", logs, msg, got, want)
	}
}
