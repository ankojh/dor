package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Runner owns a selected set of agents for one process.
type Runner struct {
	agents []Agent
	log    *slog.Logger

	mu      sync.RWMutex
	started bool
}

// NewRunner creates a runner. Agents start in the order given.
func NewRunner(agents []Agent, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{agents: agents, log: log}
}

// Start starts every selected agent. If one fails, already-started agents are
// stopped again.
func (r *Runner) Start(ctx context.Context) error {
	started := make([]Agent, 0, len(r.agents))
	for _, a := range r.agents {
		if err := a.Start(ctx); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_ = started[i].Stop(stopCtx)
				cancel()
			}
			return fmt.Errorf("agents: start %s: %w", a.Name(), err)
		}
		started = append(started, a)
	}
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
	return nil
}

// Shutdown stops agents in reverse order, draining each one.
func (r *Runner) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.started = false
	r.mu.Unlock()
	var errs []error
	for i := len(r.agents) - 1; i >= 0; i-- {
		if err := r.agents[i].Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.agents[i].Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Healthy reports whether every requested agent is running and the runner was
// started. Unknown health is unhealthy.
func (r *Runner) Healthy() (bool, map[string]string) {
	r.mu.RLock()
	started := r.started
	r.mu.RUnlock()
	statuses := make(map[string]string, len(r.agents))
	all := started
	for _, a := range r.agents {
		status := "unknown"
		reporter, ok := a.(HealthReporter)
		if !ok {
			all = false
			statuses[a.Name()] = status
			continue
		}
		if err := reporter.LastError(); err != nil {
			status = "error: " + err.Error()
			all = false
		} else if !reporter.Running() {
			status = "stopped"
			all = false
		} else {
			status = "running"
		}
		statuses[a.Name()] = status
	}
	return all, statuses
}

// TerminalCounts returns the number of DLQ-terminated messages per agent.
func (r *Runner) TerminalCounts() map[string]int64 {
	out := make(map[string]int64, len(r.agents))
	for _, a := range r.agents {
		if reporter, ok := a.(HealthReporter); ok {
			out[a.Name()] = reporter.TerminalCount()
		}
	}
	return out
}

// Names returns the selected agent names.
func (r *Runner) Names() []string {
	names := make([]string, len(r.agents))
	for i, a := range r.agents {
		names[i] = a.Name()
	}
	return names
}
