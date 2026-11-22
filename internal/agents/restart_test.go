package agents

import (
	"context"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/matcher"
)

// The Supervisor's restart path is only meaningful for agents that implement
// Restartable. A compile-time assertion catches a method-set regression.
var (
	_ Restartable = (*clusterer)(nil)
	_ Restartable = (*embedder)(nil)
	_ Restartable = (*matcherAgent)(nil)
	_ Restartable = (*notifierAgent)(nil)
)

func restartContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 20*time.Second)
}

func TestClusterRestartRearmsConsumerAndLoops(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{})
	if err != nil {
		t.Fatalf("new clusterer: %v", err)
	}
	cluster := agent.(*clusterer)
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	if !cluster.Running() {
		t.Fatal("cluster not running after Start")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := restartContext(t)
		err := runner.Restart(ctx, ClusterDurable)
		cancel()
		if err != nil {
			t.Fatalf("restart %d: %v", i+1, err)
		}
		if !cluster.Running() {
			t.Fatalf("cluster not running after restart %d", i+1)
		}
	}
}

func TestEmbedRestartRearmsConsumerAndBatchLoop(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	agent, err := NewEmbedder(b, NewMetrics(), testLogger(), Deps{})
	if err != nil {
		t.Fatalf("new embedder: %v", err)
	}
	embed := agent.(*embedder)
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	if !embed.Running() {
		t.Fatal("embedder not running after Start")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := restartContext(t)
		err := runner.Restart(ctx, EmbedDurable)
		cancel()
		if err != nil {
			t.Fatalf("restart %d: %v", i+1, err)
		}
		if !embed.Running() {
			t.Fatalf("embedder not running after restart %d", i+1)
		}
	}
}

func TestMatcherRestartRearmsBothConsumers(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	svc := matcher.New(b, testLogger(), nil, nil, nil, config.DefaultThresholds())
	agent, err := NewMatcher(b, NewMetrics(), testLogger(), svc)
	if err != nil {
		t.Fatalf("new matcher: %v", err)
	}
	m := agent.(*matcherAgent)
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	if !m.Running() {
		t.Fatal("matcher not running after Start")
	}

	ctx, cancel := restartContext(t)
	defer cancel()
	if err := runner.Restart(ctx, MatcherAgentName); err != nil {
		t.Fatalf("restart matcher: %v", err)
	}
	if !m.Running() {
		t.Fatal("matcher not running after Restart")
	}
}

func TestNotifierRestartRearmsConsumerAndDeliveryWorker(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	st := notifierTestStore(t, b)
	agent, err := NewNotifier(b, NewMetrics(), testLogger(), NotifierConfig{Alerts: st})
	if err != nil {
		t.Fatalf("new notifier: %v", err)
	}
	n := agent.(*notifierAgent)
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	if !n.Running() {
		t.Fatal("notifier not running after Start")
	}

	ctx, cancel := restartContext(t)
	defer cancel()
	if err := runner.Restart(ctx, NotifierAgentName); err != nil {
		t.Fatalf("restart notifier: %v", err)
	}
	if !n.Base.Running() {
		t.Error("notifier consumer is not running after Restart")
	}
	if !n.worker.Running() {
		t.Error("notifier delivery worker is not running after Restart")
	}
	if !n.Running() {
		t.Error("notifier reports not running after Restart")
	}
}

// A worker that loses the race for a closed gate must stop the heartbeat it was
// started with. This is the forced-shutdown path where the message stays
// unacknowledged for redelivery; a leaked heartbeat would keep calling
// InProgress forever.
func TestWorkerClosesHeartbeatWhenGateIsClosed(t *testing.T) {
	a := &Base{
		jobs:         make(chan job, 1),
		forceCh:      make(chan struct{}),
		fullyStopped: make(chan struct{}),
	}
	gate := newConcurrencyGate(1, 1)
	gate.close()

	a.workers.Add(1)
	go a.worker(context.Background(), context.Background(), gate)

	done := make(chan struct{})
	a.jobsWG.Add(1)
	a.jobs <- job{heartbeatDone: done}
	close(a.jobs)
	a.workers.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not close heartbeatDone after the concurrency gate closed")
	}
}
