package llm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubCompleter struct {
	mu    sync.Mutex
	calls int
	errs  []error // one per call; the last repeats
	text  string
}

func (s *stubCompleter) Complete(context.Context, Job) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	var err error
	if len(s.errs) > 0 {
		err = s.errs[min(s.calls, len(s.errs))-1]
	}
	if err != nil {
		return Result{}, err
	}
	return Result{Text: s.text}, nil
}

func newTestChain(clock *fakeClock, backends ...Backend) *Chain {
	c := NewChain(nil, backends...)
	c.now = clock.now
	return c
}

func TestChainFirstAnswerWins(t *testing.T) {
	a := &stubCompleter{text: "a"}
	b := &stubCompleter{text: "b"}
	c := newTestChain(&fakeClock{t: time.Unix(0, 0)}, Backend{"claude-cli", a}, Backend{"codex-cli", b})
	res, err := c.Complete(context.Background(), cliJob())
	if err != nil || res.Text != "a" || b.calls != 0 {
		t.Errorf("res=%q err=%v b.calls=%d", res.Text, err, b.calls)
	}
	if got := strings.Join(c.Names(), ","); got != "claude-cli,codex-cli" {
		t.Errorf("names = %s", got)
	}
}

func TestChainFailsOverAndCoolsDown(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	a := &stubCompleter{errs: []error{errors.New("usage limit reached")}, text: "a"}
	b := &stubCompleter{text: "b"}
	c := newTestChain(clock, Backend{"claude-cli", a}, Backend{"codex-cli", b})

	res, err := c.Complete(context.Background(), cliJob())
	if err != nil || res.Text != "b" || a.calls != 1 {
		t.Fatalf("res=%q err=%v a.calls=%d", res.Text, err, a.calls)
	}
	// While cooling, the failed backend is not started again.
	clock.advance(29 * time.Second)
	if res, _ := c.Complete(context.Background(), cliJob()); res.Text != "b" || a.calls != 1 {
		t.Errorf("cooling: res=%q a.calls=%d", res.Text, a.calls)
	}
	// After 30 s it is retried; a second failure doubles the cooldown.
	clock.advance(2 * time.Second)
	_, _ = c.Complete(context.Background(), cliJob())
	if a.calls != 2 {
		t.Fatalf("a.calls = %d, want 2", a.calls)
	}
	clock.advance(59 * time.Second)
	_, _ = c.Complete(context.Background(), cliJob())
	if a.calls != 2 {
		t.Errorf("second cooldown too short: a.calls = %d", a.calls)
	}
	// Recovery resets the count.
	a.mu.Lock()
	a.errs = []error{nil}
	a.mu.Unlock()
	clock.advance(2 * time.Second)
	if res, _ := c.Complete(context.Background(), cliJob()); res.Text != "a" {
		t.Errorf("recovered res = %q", res.Text)
	}
	if c.fails[0] != 0 {
		t.Errorf("fails = %d after success", c.fails[0])
	}
}

func TestChainCooldownIsCapped(t *testing.T) {
	c := newTestChain(&fakeClock{t: time.Unix(0, 0)})
	c.fails = []int{0}
	c.until = []time.Time{{}}
	var last time.Duration
	for range 40 {
		last = c.failed(0)
	}
	if last != 10*time.Minute {
		t.Errorf("cooldown = %v, want 10m", last)
	}
}

func TestChainTypedStopsDoNotCoolDown(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	a := &stubCompleter{errs: []error{&StopError{Kind: ErrRefusal, StopReason: "refusal"}, &BudgetError{Reason: "daily-cap", API: "claude-haiku"}}}
	b := &stubCompleter{text: "b"}
	c := newTestChain(clock, Backend{"api", a}, Backend{"claude-cli", b})
	for range 2 {
		if res, err := c.Complete(context.Background(), cliJob()); err != nil || res.Text != "b" {
			t.Fatalf("res=%q err=%v", res.Text, err)
		}
	}
	if a.calls != 2 || c.fails[0] != 0 {
		t.Errorf("a.calls=%d fails=%d", a.calls, c.fails[0])
	}
}

func TestChainAllFail(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	a := &stubCompleter{errs: []error{errors.New("a down")}}
	b := &stubCompleter{errs: []error{errors.New("b down")}}
	c := newTestChain(clock, Backend{"claude-cli", a}, Backend{"codex-cli", b})
	_, err := c.Complete(context.Background(), cliJob())
	if err == nil || !strings.Contains(err.Error(), "claude-cli: a down") || !strings.Contains(err.Error(), "codex-cli: b down") {
		t.Errorf("err = %v", err)
	}
	if _, err := c.Complete(context.Background(), cliJob()); !errors.Is(err, ErrNoBackend) {
		t.Errorf("all cooling err = %v", err)
	}
}

func TestChainAllBackendsTooSlow(t *testing.T) {
	cases := map[string]struct {
		err  error
		warn bool
	}{
		"sentinel": {err: ErrCLITooSlow},
		// A backend may add context around the sentinel; errors.Is still sees it.
		"wrapped": {err: fmt.Errorf("claude-cli: %w", ErrCLITooSlow)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			clock := &fakeClock{t: time.Unix(0, 0)}
			a := &stubCompleter{errs: []error{tc.err}}
			b := &stubCompleter{errs: []error{tc.err}}
			c := NewChain(log, Backend{"claude-cli", a}, Backend{"codex-cli", b})
			c.now = clock.now

			_, err := c.Complete(context.Background(), cliJob())
			if err != ErrCLITooSlow {
				t.Fatalf("err = %v, want the bare ErrCLITooSlow", err)
			}
			if a.calls != 1 || b.calls != 1 {
				t.Errorf("calls = %d/%d, want 1/1 (the next backend is still tried)", a.calls, b.calls)
			}
			// Too slow is the configured setup, not a failure: no cooldown.
			if c.fails[0] != 0 || c.fails[1] != 0 {
				t.Errorf("fails = %v, want both 0", c.fails)
			}
			if !c.until[0].IsZero() || !c.until[1].IsZero() {
				t.Errorf("until = %v, want no cooldown", c.until)
			}
			if buf.Len() != 0 {
				t.Errorf("logged %q, want silence for ErrCLITooSlow", buf.String())
			}
			// With no cooldown set, the next call retries every backend.
			if _, err := c.Complete(context.Background(), cliJob()); err != ErrCLITooSlow {
				t.Errorf("second call err = %v, want ErrCLITooSlow", err)
			}
			if a.calls != 2 || b.calls != 2 {
				t.Errorf("after retry calls = %d/%d, want 2/2", a.calls, b.calls)
			}
		})
	}
}

func TestChainTooSlowFallsThroughToAnswer(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	a := &stubCompleter{errs: []error{ErrCLITooSlow}}
	b := &stubCompleter{text: "b"}
	c := newTestChain(clock, Backend{"claude-cli", a}, Backend{"codex-cli", b})

	res, err := c.Complete(context.Background(), cliJob())
	if err != nil || res.Text != "b" {
		t.Fatalf("res=%q err=%v, want the later backend's answer", res.Text, err)
	}
	if a.calls != 1 || b.calls != 1 {
		t.Errorf("calls = %d/%d, want 1/1", a.calls, b.calls)
	}
	if c.fails[0] != 0 {
		t.Errorf("fails[0] = %d, want 0 (too slow must not cool down)", c.fails[0])
	}
}

func TestChainTooSlowMixedWithRealError(t *testing.T) {
	cases := map[string]struct {
		first, last error
		wantFails   [2]int
	}{
		"too slow then real": {first: ErrCLITooSlow, last: errors.New("connection refused"), wantFails: [2]int{0, 1}},
		"real then too slow": {first: errors.New("connection refused"), last: ErrCLITooSlow, wantFails: [2]int{1, 0}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clock := &fakeClock{t: time.Unix(0, 0)}
			a := &stubCompleter{errs: []error{tc.first}}
			b := &stubCompleter{errs: []error{tc.last}}
			c := newTestChain(clock, Backend{"claude-cli", a}, Backend{"codex-cli", b})

			_, err := c.Complete(context.Background(), cliJob())
			if err == nil || !strings.Contains(err.Error(), "connection refused") {
				t.Fatalf("err = %v, want the real failure in the joined error", err)
			}
			if errors.Is(err, ErrCLITooSlow) {
				t.Errorf("err = %v, must not match ErrCLITooSlow when a real failure exists", err)
			}
			if strings.Contains(err.Error(), ErrCLITooSlow.Error()) {
				t.Errorf("err = %v, must not include the too-slow text", err)
			}
			if c.fails[0] != tc.wantFails[0] || c.fails[1] != tc.wantFails[1] {
				t.Errorf("fails = %v, want %v", c.fails, tc.wantFails)
			}
		})
	}
}

func TestChainStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := &stubCompleter{errs: []error{context.Canceled}}
	b := &stubCompleter{text: "b"}
	c := newTestChain(&fakeClock{t: time.Unix(0, 0)}, Backend{"claude-cli", a}, Backend{"codex-cli", b})
	if _, err := c.Complete(ctx, cliJob()); !errors.Is(err, context.Canceled) || b.calls != 0 || c.fails[0] != 0 {
		t.Errorf("err=%v b.calls=%d fails=%d", err, b.calls, c.fails[0])
	}
}

// fakeCloser is a Completer that also holds a resource, like a CLI workdir.
type fakeCloser struct {
	calls int
	err   error
}

func (f *fakeCloser) Complete(context.Context, Job) (Result, error) { return Result{}, nil }

func (f *fakeCloser) Close() error {
	f.calls++
	return f.err
}

func TestChainCloseClosesResourceHolders(t *testing.T) {
	boom := errors.New("workdir busy")
	a, b := &fakeCloser{}, &fakeCloser{err: boom}
	plain := &stubCompleter{text: "plain"}
	c := NewChain(nil,
		Backend{"claude-cli", a},
		Backend{"api", plain}, // no Close method: ignored
		Backend{"codex-cli", b},
	)
	err := c.Close()
	if a.calls != 1 || b.calls != 1 {
		t.Errorf("close calls = %d/%d, want 1/1", a.calls, b.calls)
	}
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if !strings.Contains(err.Error(), "codex-cli") {
		t.Errorf("err = %v, want the backend name", err)
	}
	if strings.Contains(err.Error(), "api:") {
		t.Errorf("err = %v, a backend without Close must not be reported", err)
	}

	// Without failures Close reports nil, and a repeated Close is harmless.
	ok := NewChain(nil, Backend{"claude-cli", &fakeCloser{}})
	if err := ok.Close(); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
	if err := ok.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}
