package agents

import (
	"context"
	"testing"
)

// The clusterer and embedder are HealthReporters, so /healthz reports them
// instead of "unknown". A compile-time assertion catches a regression in the
// method set before any test runs.
var (
	_ HealthReporter = (*clusterer)(nil)
	_ HealthReporter = (*embedder)(nil)
)

func TestClusterAndEmbedReportHealth(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()

	clusterAgent, err := NewClusterer(b, metrics, testLogger(), Deps{})
	if err != nil {
		t.Fatalf("new clusterer: %v", err)
	}
	embedAgent, err := NewEmbedder(b, metrics, testLogger(), Deps{})
	if err != nil {
		t.Fatalf("new embedder: %v", err)
	}
	cluster, ok := clusterAgent.(*clusterer)
	if !ok {
		t.Fatalf("clusterer type = %T", clusterAgent)
	}
	embed, ok := embedAgent.(*embedder)
	if !ok {
		t.Fatalf("embedder type = %T", embedAgent)
	}

	// Before Start, neither loop is active.
	if cluster.Running() {
		t.Error("cluster reports running before Start")
	}
	if embed.Running() {
		t.Error("embed reports running before Start")
	}
	if err := cluster.LastError(); err != nil {
		t.Errorf("cluster LastError before Start = %v", err)
	}
	if n := cluster.TerminalCount(); n != 0 {
		t.Errorf("cluster TerminalCount before Start = %d", n)
	}

	runner := NewRunner([]Agent{clusterAgent, embedAgent}, testLogger())
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("start runner: %v", err)
	}

	// Start returns once each consumer is live.
	if !cluster.Running() {
		t.Error("cluster does not report running after Start")
	}
	if !embed.Running() {
		t.Error("embed does not report running after Start")
	}
	healthy, statuses := runner.Healthy()
	if !healthy {
		t.Errorf("Healthy = false, statuses = %v", statuses)
	}
	for _, name := range []string{ClusterDurable, EmbedDurable} {
		if got := statuses[name]; got != "running" {
			t.Errorf("health of %q = %q, want running", name, got)
		}
	}
	if names := runner.Names(); len(names) != 2 {
		t.Errorf("runner names = %v, want cluster,embed", names)
	}

	stopRunner(t, runner)

	// Stop clears Running and Healthy reports the stopped state.
	if cluster.Running() {
		t.Error("cluster reports running after Stop")
	}
	if embed.Running() {
		t.Error("embed reports running after Stop")
	}
	healthy, statuses = runner.Healthy()
	if healthy {
		t.Errorf("Healthy = true after shutdown, statuses = %v", statuses)
	}
	for _, name := range []string{ClusterDurable, EmbedDurable} {
		if got := statuses[name]; got != "stopped" {
			t.Errorf("health of %q after stop = %q, want stopped", name, got)
		}
	}
}

// Names in the health map come from Agent.Name, which is the durable name.
func TestClusterAndEmbedNamesMatchDurables(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	for _, build := range []struct {
		name     string
		newAgent func() (Agent, error)
	}{
		{ClusterDurable, func() (Agent, error) { return NewClusterer(b, metrics, testLogger(), Deps{}) }},
		{EmbedDurable, func() (Agent, error) { return NewEmbedder(b, metrics, testLogger(), Deps{}) }},
	} {
		agent, err := build.newAgent()
		if err != nil {
			t.Fatalf("%s: %v", build.name, err)
		}
		if got := agent.Name(); got != build.name {
			t.Errorf("Name() = %q, want %q", got, build.name)
		}
		if _, ok := agent.(HealthReporter); !ok {
			t.Errorf("%s does not implement HealthReporter", build.name)
		}
	}
}

// The embedder and clusterer are the only two agents the health path previously
// reported as "unknown"; assert the interface check the runner uses.
func TestHealthReporterSatisfied(t *testing.T) {
	for name, agent := range map[string]any{
		"clusterer": (*clusterer)(nil),
		"embedder":  (*embedder)(nil),
	} {
		if _, ok := agent.(HealthReporter); !ok {
			t.Errorf("%s is not a HealthReporter", name)
		}
	}
}
