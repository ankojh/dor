package llm

import (
	"context"
	"errors"
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
