package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Completer is the surface agents and the text service use: the API client,
// a CLI backend or a Chain of them.
type Completer interface {
	Complete(ctx context.Context, job Job) (Result, error)
}

// Backend is one named entry in a Chain.
type Backend struct {
	Name      string
	Completer Completer
}

// ErrNoBackend is returned when every backend in a Chain is cooling down.
var ErrNoBackend = errors.New("llm: every backend is cooling down")

// Chain tries backends in order and returns the first answer.
//
// A backend that fails for an infrastructure reason (process error, quota or
// rate limit, timeout) is skipped for a cooldown that doubles per consecutive
// failure, so an exhausted subscription does not cost a process start per
// item. A typed stop (refusal, max_tokens) or a budget denial moves to the
// next backend without a cooldown: the backend itself is healthy.
type Chain struct {
	backends []Backend
	log      *slog.Logger
	now      func() time.Time
	minCool  time.Duration
	maxCool  time.Duration

	mu    sync.Mutex
	fails []int
	until []time.Time
}

// NewChain returns a Chain over the given backends, in order.
func NewChain(log *slog.Logger, backends ...Backend) *Chain {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Chain{
		backends: backends,
		log:      log,
		now:      time.Now,
		minCool:  30 * time.Second,
		maxCool:  10 * time.Minute,
		fails:    make([]int, len(backends)),
		until:    make([]time.Time, len(backends)),
	}
}

// Names lists the backends in order.
func (c *Chain) Names() []string {
	out := make([]string, len(c.backends))
	for i, b := range c.backends {
		out[i] = b.Name
	}
	return out
}

// Close closes every backend that holds resources, such as a CLI workdir.
func (c *Chain) Close() error {
	var errs []error
	for _, b := range c.backends {
		closer, ok := b.Completer.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b.Name, err))
		}
	}
	return errors.Join(errs...)
}

// Complete runs the job on the first backend that answers.
func (c *Chain) Complete(ctx context.Context, job Job) (Result, error) {
	var errs []error
	tried, tooSlow := 0, 0
	for i, b := range c.backends {
		if c.cooling(i) {
			continue
		}
		tried++
		res, err := b.Completer.Complete(ctx, job)
		if err == nil {
			c.succeeded(i)
			return res, nil
		}
		if ctx.Err() != nil {
			return res, err
		}
		if errors.Is(err, ErrCLITooSlow) {
			// Expected for every short-deadline job; not worth a log line.
			tooSlow++
			continue
		}
		errs = append(errs, fmt.Errorf("%s: %w", b.Name, err))
		var stop *StopError
		if errors.As(err, &stop) || errors.Is(err, ErrBudget) {
			c.log.Info("llm backend declined; trying next", "backend", b.Name, "job", job.Name, "err", err)
			continue
		}
		wait := c.failed(i)
		c.log.Warn("llm backend failed; cooling down", "backend", b.Name, "job", job.Name, "cooldown", wait, "err", err)
	}
	if tried == 0 {
		return Result{}, ErrNoBackend
	}
	if len(errs) == 0 && tooSlow > 0 {
		// Every backend that was tried declined the deadline: callers treat
		// this as "no model for this job", not as a failure.
		return Result{}, ErrCLITooSlow
	}
	return Result{}, errors.Join(errs...)
}

func (c *Chain) cooling(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now().Before(c.until[i])
}

func (c *Chain) succeeded(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fails[i] = 0
	c.until[i] = time.Time{}
}

func (c *Chain) failed(i int) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fails[i]++
	wait := c.maxCool
	if shift := c.fails[i] - 1; shift < 20 {
		wait = min(c.minCool<<shift, c.maxCool)
	}
	c.until[i] = c.now().Add(wait)
	return wait
}
