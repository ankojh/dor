package agents

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ankojh/dor/internal/bus"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testBus(t *testing.T, store string) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   store,
		Logger:     testLogger(),
	})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	return b
}

func startRunner(t *testing.T, agentList ...Agent) *Runner {
	t.Helper()
	runner := NewRunner(agentList, testLogger())
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	return runner
}

func stopRunner(t *testing.T, runner *Runner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := runner.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown runner: %v", err)
	}
}

func publishInput(t *testing.T, b *bus.Bus, id, body string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := b.Publish(ctx, bus.SubjectEchoIn, []byte(body), bus.WithMsgID(id))
	if err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
	return ack.Sequence
}

func waitSubjectMsgs(t *testing.T, b *bus.Bus, subject string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		streamName := bus.StreamForSubject(subject)
		stream, err := b.StreamNamed(ctx, streamName)
		var info *jetstream.StreamInfo
		if err == nil {
			info, err = stream.Info(ctx, jetstream.WithSubjectFilter(subject))
		}
		cancel()
		if err == nil && info.State.Subjects[subject] >= want {
			return
		}
		if time.Now().After(deadline) {
			got := uint64(0)
			if err == nil {
				got = info.State.Subjects[subject]
			}
			t.Fatalf("timed out waiting for %d messages on %s (got %d, err %v)", want, subject, got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func collectSubject(t *testing.T, b *bus.Bus, subject string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	streamName := bus.StreamForSubject(subject)
	if streamName == "" {
		t.Fatalf("subject %s belongs to no stream", subject)
	}
	consumer, err := b.JS().CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		FilterSubject:     subject,
		AckPolicy:         jetstream.AckExplicitPolicy,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     10000,
		InactiveThreshold: time.Minute,
	})
	if err != nil {
		t.Fatalf("create collector: %v", err)
	}
	var out []string
	for {
		msg, err := consumer.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			break
		}
		out = append(out, string(msg.Data()))
		_ = msg.Ack()
	}
	return out
}

func waitConsumerAcked(t *testing.T, b *bus.Bus, durable string, streamSeq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Look the consumer up; EnsureConsumer would rewrite its config with defaults.
	consumer, err := b.JS().Consumer(ctx, bus.StreamForSubject(bus.SubjectEchoIn), durable)
	if err != nil {
		t.Fatalf("lookup consumer %s: %v", durable, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := consumer.Info(ctx)
		if err == nil && info.AckFloor.Stream >= streamSeq {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer %s did not ack stream seq %d", durable, streamSeq)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, family, agent string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "agent" && label.GetValue() == agent {
					if m.GetCounter() == nil {
						t.Fatalf("%s is not a counter", family)
					}
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s{agent=%q} not found", family, agent)
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, family, agent string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "agent" && label.GetValue() == agent {
					if m.GetHistogram() == nil {
						t.Fatalf("%s is not a histogram", family)
					}
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	t.Fatalf("metric %s{agent=%q} not found", family, agent)
	return 0
}

func gaugeExists(t *testing.T, reg *prometheus.Registry, family, agent string) bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "agent" && label.GetValue() == agent {
					return m.GetGauge() != nil
				}
			}
		}
	}
	return false
}

func TestEchoOutputMsgIDIsStableAndDistinct(t *testing.T) {
	first := EchoOutputMsgID("input-1")
	second := EchoOutputMsgID("input-1")
	other := EchoOutputMsgID("input-2")
	if first != second {
		t.Fatal("echo output id is not deterministic")
	}
	if first == other {
		t.Fatal("echo output ids collide")
	}
	if first == "input-1" {
		t.Fatal("echo output id must differ from the input id (same stream)")
	}
}

func TestEchoDurableResumeAndMetrics(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	metrics := NewMetrics()
	echo, err := NewEcho(b, metrics, testLogger())
	if err != nil {
		t.Fatalf("new echo: %v", err)
	}
	runner := startRunner(t, echo)

	for i := 0; i < 20; i++ {
		publishInput(t, b, fmt.Sprintf("resume-a-%d", i), fmt.Sprintf("resume-a-%d", i))
	}
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 20)
	bodies := collectSubject(t, b, bus.SubjectEchoOut)
	if len(bodies) != 20 {
		t.Fatalf("echo.out has %d messages, want 20", len(bodies))
	}
	seen := make(map[string]bool, len(bodies))
	for _, body := range bodies {
		if seen[body] {
			t.Fatalf("duplicate body %q", body)
		}
		seen[body] = true
	}

	if got := counterValue(t, metrics.Registry(), "dor_agent_in_total", "echo"); got != 20 {
		t.Errorf("dor_agent_in_total = %v, want 20", got)
	}
	if got := counterValue(t, metrics.Registry(), "dor_agent_out_total", "echo"); got != 20 {
		t.Errorf("dor_agent_out_total = %v, want 20", got)
	}
	if got := counterValue(t, metrics.Registry(), "dor_agent_errors_total", "echo"); got != 0 {
		t.Errorf("dor_agent_errors_total = %v, want 0", got)
	}
	if got := histogramCount(t, metrics.Registry(), "dor_agent_handle_seconds", "echo"); got != 20 {
		t.Errorf("dor_agent_handle_seconds count = %d, want 20", got)
	}
	if !gaugeExists(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "echo") {
		t.Error("dor_agent_queue_lag_seconds is missing")
	}
	if !gaugeExists(t, metrics.Registry(), "dor_agent_pending", "echo") {
		t.Error("dor_agent_pending is missing")
	}

	stopRunner(t, runner)

	// While the agent is down messages accumulate on the durable consumer.
	for i := 0; i < 15; i++ {
		publishInput(t, b, fmt.Sprintf("resume-b-%d", i), fmt.Sprintf("resume-b-%d", i))
	}

	metrics2 := NewMetrics()
	echo2, err := NewEcho(b, metrics2, testLogger())
	if err != nil {
		t.Fatalf("new echo after restart: %v", err)
	}
	runner2 := startRunner(t, echo2)
	defer stopRunner(t, runner2)

	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 35)
	all := collectSubject(t, b, bus.SubjectEchoOut)
	if len(all) != 35 {
		t.Fatalf("echo.out has %d messages after resume, want 35", len(all))
	}
	unique := make(map[string]bool, len(all))
	for _, body := range all {
		unique[body] = true
	}
	if len(unique) != 35 {
		t.Fatalf("echo.out has %d unique bodies, want 35 (duplicates after resume)", len(unique))
	}
	if got := counterValue(t, metrics2.Registry(), "dor_agent_in_total", "echo"); got != 15 {
		t.Errorf("second process dor_agent_in_total = %v, want 15", got)
	}
}

// Two independent durable consumers receive the same input (the same situation
// as a crash redelivery). Both derive the same output id, so JetStream stores
// exactly one echo.out message.
func TestEchoRedeliveryDeduplicates(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	metrics := NewMetrics()
	specA := echoSpec()
	specA.Name = "echo-a"
	specA.Durable = "echo-a"
	a, err := NewBase(b, metrics, testLogger(), specA)
	if err != nil {
		t.Fatalf("new echo-a: %v", err)
	}
	specB := echoSpec()
	specB.Name = "echo-b"
	specB.Durable = "echo-b"
	bb, err := NewBase(b, metrics, testLogger(), specB)
	if err != nil {
		t.Fatalf("new echo-b: %v", err)
	}
	runner := startRunner(t, a, bb)
	defer stopRunner(t, runner)

	seq := publishInput(t, b, "dup-input", "dup-body")
	waitConsumerAcked(t, b, "echo-a", seq)
	waitConsumerAcked(t, b, "echo-b", seq)
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(bus.SubjectEchoOut))
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(bus.SubjectEchoOut))
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if got := info.State.Subjects[bus.SubjectEchoOut]; got != 1 {
		t.Fatalf("echo.out stored %d messages after two deliveries, want 1", got)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	metrics := NewMetrics()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	slow, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "slow",
		Durable:       "slow",
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 4,
		AckWait:       30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			once.Do(func() { close(started) })
			<-release
			return out.Publish(ctx, bus.SubjectEchoOut, msg.Data, bus.WithMsgID("slow-"+msg.ID()))
		},
	})
	if err != nil {
		t.Fatalf("new slow agent: %v", err)
	}
	runner := startRunner(t, slow)

	seq := publishInput(t, b, "slow-input", "slow-body")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		stopped <- runner.Shutdown(ctx)
	}()

	select {
	case err := <-stopped:
		t.Fatalf("shutdown returned before the handler finished: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish after the handler was released")
	}

	waitConsumerAcked(t, b, "slow", seq)
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 1)
	if got := counterValue(t, metrics.Registry(), "dor_agent_out_total", "slow"); got != 1 {
		t.Fatalf("dor_agent_out_total = %v, want 1", got)
	}
}

func TestRegistrySelect(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()

	registry := NewRegistry()
	echo, _ := NewEcho(b, metrics, testLogger())
	if err := registry.Register(echo); err != nil {
		t.Fatalf("register echo: %v", err)
	}
	spec := echoSpec()
	spec.Name = "other"
	spec.Durable = "other"
	other, _ := NewBase(b, metrics, testLogger(), spec)
	if err := registry.Register(other); err != nil {
		t.Fatalf("register other: %v", err)
	}
	if err := registry.Register(other); err == nil {
		t.Fatal("duplicate registration succeeded")
	}

	all, err := registry.Select("all")
	if err != nil || len(all) != 2 {
		t.Fatalf("Select(all) = %d agents, err %v", len(all), err)
	}
	none, err := registry.Select("none")
	if err != nil || len(none) != 0 {
		t.Fatalf("Select(none) = %d agents, err %v", len(none), err)
	}
	one, err := registry.Select("other")
	if err != nil || len(one) != 1 || one[0].Name() != "other" {
		t.Fatalf("Select(other) = %v, err %v", names(one), err)
	}
	if _, err := registry.Select("missing"); err == nil {
		t.Fatal("Select(missing) did not fail")
	}
	if _, err := registry.Select("all,echo"); err == nil {
		t.Fatal("Select(all,echo) did not fail")
	}
}

func names(list []Agent) []string {
	out := make([]string, len(list))
	for i, a := range list {
		out[i] = a.Name()
	}
	return out
}

func TestHealthyReflectsRunningState(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	echo, _ := NewEcho(b, metrics, testLogger())
	runner := NewRunner([]Agent{echo}, testLogger())

	ok, statuses := runner.Healthy()
	if ok {
		t.Fatal("runner is healthy before Start")
	}
	if statuses["echo"] != "stopped" {
		t.Fatalf("status before start = %q, want stopped", statuses["echo"])
	}

	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	ok, statuses = runner.Healthy()
	if !ok || statuses["echo"] != "running" {
		t.Fatalf("health after start = %v %v", ok, statuses)
	}

	stopRunner(t, runner)
	ok, statuses = runner.Healthy()
	if ok || statuses["echo"] != "stopped" {
		t.Fatalf("health after stop = %v %v", ok, statuses)
	}
}
