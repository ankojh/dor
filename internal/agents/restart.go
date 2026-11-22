package agents

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// This file adds the in-process Restart surface for the agents that compose
// several loops or Base instances. The Supervisor calls Restart after a health
// sample reports the agent stopped or errored; without these methods it logs a
// permanent advisory and retries with backoff. Restart must rebuild the same
// durable consumer, not just set a flag.

// Restart re-arms the clusterer's two producer loops and its consumer. The
// durable story/join state lives in NATS and the in-memory story view is kept
// across a restart; only entries in the ready-queue dedupe map are dropped so
// joins still in the KV bucket are re-enqueued by the next sweep.
func (c *clusterer) Restart(ctx context.Context) error {
	if cancel := c.cancel; cancel != nil {
		cancel()
	}
	c.wg.Wait()
	c.queuedMu.Lock()
	c.queued = make(map[string]bool)
	c.retryAt = make(map[string]time.Time)
	c.queuedMu.Unlock()

	if err := c.base.Restart(ctx); err != nil {
		return err
	}
	c.stopped.Store(false)

	// The caller's restart deadline bounds waiting for the old loop only. The
	// new producer loops must outlive the Supervisor's short restart budget.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.ctx, c.cancel = runCtx, cancel
	c.wg.Add(2)
	go c.processor(runCtx)
	go c.sweeper(runCtx)
	c.log.Info("cluster restarted")
	return nil
}

// Restart re-arms the embedder's batch loop after its consumer. The embedding
// cache is retained: it is content-addressed and independent of consumer
// offsets.
func (e *embedder) Restart(ctx context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	if err := e.base.Restart(ctx); err != nil {
		return err
	}
	e.stopped.Store(false)
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	e.cancel = cancel
	e.wg.Add(1)
	go e.batchLoop(runCtx)
	e.log.Info("embed restarted")
	return nil
}

// Restart re-arms the matcher's two consumers, its prompt-refresh ticker and
// its prompt watch. Both durable consumers keep their names, so no input is
// skipped by the restart.
func (m *matcherAgent) Restart(ctx context.Context) error {
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	m.running.Store(false)
	m.stopped.Store(false)

	for _, a := range []Agent{m.articles, m.stories} {
		restartable, ok := a.(Restartable)
		if !ok {
			return fmt.Errorf("agents: %s does not support in-process restart", a.Name())
		}
		if err := restartable.Restart(ctx); err != nil {
			return fmt.Errorf("agents: restart %s: %w", a.Name(), err)
		}
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	m.svc.Refresh(runCtx)
	m.running.Store(true)
	m.startRuntime(runCtx)
	return nil
}

// Restart re-arms both notifier Bases. The promoted Base.Restart would restart
// only the input consumer and leave the delivery worker stopped, so it is
// overridden here.
func (n *notifierAgent) Restart(ctx context.Context) error {
	var errs []error
	if err := n.Base.Restart(ctx); err != nil {
		errs = append(errs, fmt.Errorf("notifier consumer: %w", err))
	}
	if err := n.worker.Restart(ctx); err != nil {
		errs = append(errs, fmt.Errorf("notifier deliveries: %w", err))
	}
	return errors.Join(errs...)
}
