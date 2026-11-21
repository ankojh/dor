package agents

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ankojh/dor/internal/bus"
)

func testBusWithOptions(t *testing.T, store string, opts bus.Options) *bus.Bus {
	t.Helper()
	opts.ListenAddr = "127.0.0.1:0"
	opts.StoreDir = store
	opts.Logger = testLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, opts)
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	return b
}

func TestTerminalOutcomeWritesDLQAndTerminates(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "terminal",
		FilterSubject: bus.SubjectEchoIn,
		Handle: func(context.Context, *Message, *Emitter) error {
			return Terminal(errors.New("malformed payload"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	publishInput(t, b, "term-1", "bad")
	waitSubjectMsgs(t, b, bus.DLQSubject("terminal"), 1)
	waitConsumerAcked(t, b, "terminal", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := b.StreamNamed(ctx, bus.StreamDLQ)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(bus.DLQSubject("terminal")))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.State.Subjects[bus.DLQSubject("terminal")]; got != 1 {
		t.Fatalf("DLQ has %d messages, want 1", got)
	}
	msg := fetchOne(t, b, bus.DLQSubject("terminal"))
	if got := msg.Headers().Get(bus.HeaderOriginalSubject); got != bus.SubjectEchoIn {
		t.Errorf("original subject = %q", got)
	}
	if got := msg.Headers().Get(bus.HeaderOriginalMsgID); got != "term-1" {
		t.Errorf("original msg id = %q", got)
	}
	if got := msg.Headers().Get(bus.HeaderError); got == "" {
		t.Error("missing DLQ error header")
	}
	if string(msg.Data()) != "bad" {
		t.Errorf("DLQ payload = %q, want original payload", msg.Data())
	}
	if got := counterValue(t, metrics.Registry(), "dor_agent_terminal_total", "terminal"); got != 1 {
		t.Errorf("terminal total = %v, want 1", got)
	}
	if agent.TerminalCount() != 1 {
		t.Errorf("health terminal count = %d, want 1", agent.TerminalCount())
	}
}

func TestDependencyOutageBeyondTenAttemptsRecovers(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	var calls atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "outage",
		FilterSubject: bus.SubjectEchoIn,
		RetryBase:     5 * time.Millisecond,
		RetryMax:      20 * time.Millisecond,
		Handle: func(context.Context, *Message, *Emitter) error {
			if calls.Add(1) <= 12 {
				return errors.New("dependency unavailable")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	publishInput(t, b, "outage-1", "x")

	deadline := time.Now().Add(10 * time.Second)
	for calls.Load() < 13 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if calls.Load() < 13 {
		t.Fatalf("only %d attempts; unlimited redelivery did not recover", calls.Load())
	}
	waitConsumerAcked(t, b, "outage", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "outage",
		FilterSubject: bus.SubjectEchoIn,
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxDeliver != -1 {
		t.Errorf("MaxDeliver = %d, want -1 (unlimited)", info.Config.MaxDeliver)
	}
}

func TestOrderedRetryNeverOvertaken(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	subject := bus.StoryChangedSubjectForShard(0)
	if _, err := b.Publish(context.Background(), subject, []byte("old"), bus.WithMsgID("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), subject, []byte("new"), bus.WithMsgID("new")); err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	order := make(chan string, 2)
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "ordered",
		FilterSubject: subject,
		Ordered:       true,
		RetryBase:     10 * time.Millisecond,
		RetryMax:      10 * time.Millisecond,
		Handle: func(_ context.Context, msg *Message, _ *Emitter) error {
			if string(msg.Data) == "old" && !failed.Swap(true) {
				return errors.New("temporary")
			}
			order <- string(msg.Data)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	var got []string
	for i := 0; i < 2; i++ {
		select {
		case s := <-order:
			got = append(got, s)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out, got %v", got)
		}
	}
	if got[0] != "old" || got[1] != "new" {
		t.Fatalf("order = %v, want [old new]", got)
	}
}

func TestConcurrencyProcessesInParallel(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 4; i++ {
		publishInput(t, b, fmt.Sprintf("conc-%d", i), "x")
	}
	var active, maxActive, done atomic.Int32
	release := make(chan struct{})
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "conc",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   4,
		MaxAckPending: 4,
		AckWait:       5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			n := active.Add(1)
			for {
				old := maxActive.Load()
				if n <= old || maxActive.CompareAndSwap(old, n) {
					break
				}
			}
			<-release
			active.Add(-1)
			done.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	deadline := time.Now().Add(5 * time.Second)
	for active.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if active.Load() < 4 {
		t.Fatalf("only %d handlers active, want 4", active.Load())
	}
	if maxActive.Load() < 4 {
		t.Fatalf("max active = %d, want 4", maxActive.Load())
	}
	close(release)
	deadline = time.Now().Add(5 * time.Second)
	for done.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if done.Load() != 4 {
		t.Fatalf("finished %d, want 4", done.Load())
	}
}

func TestProducerAgentShape(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:     "producer",
		Producer: true,
		Produce: func(ctx context.Context, out *Emitter) error {
			if err := out.Publish(ctx, bus.SubjectEchoOut, []byte("tick"), bus.WithMsgID("producer:1")); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 1)
	ok, statuses := runner.Healthy()
	if !ok || statuses["producer"] != "running" {
		t.Fatalf("producer health = %v %v", ok, statuses)
	}
	if got := counterValue(t, metrics.Registry(), "dor_agent_out_total", "producer"); got != 1 {
		t.Errorf("producer out = %v, want 1", got)
	}
}

func TestMultipleFilterSubjects(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	var mu sync.Mutex
	got := map[string]int{}
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:           "multi",
		FilterSubjects: []string{bus.SubjectArticlePlace, bus.SubjectArticleClass, bus.SubjectArticleVec},
		Handle: func(_ context.Context, msg *Message, _ *Emitter) error {
			mu.Lock()
			got[msg.Subject]++
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	for _, subject := range []string{bus.SubjectArticlePlace, bus.SubjectArticleClass, bus.SubjectArticleVec} {
		if _, err := b.Publish(context.Background(), subject, []byte(subject), bus.WithMsgID("multi:"+subject)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDrainWaitsForBufferedCallbacks(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 16; i++ {
		publishInput(t, b, fmt.Sprintf("drain-%d", i), "x")
	}
	release := make(chan struct{})
	entered := make(chan struct{}, 16)
	var calls, finished atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "drain",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   16,
		MaxAckPending: 16,
		AckWait:       5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			calls.Add(1)
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			finished.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	<-entered
	time.Sleep(20 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := agent.consumer.Info(context.Background())
		if err == nil && info.NumAckPending == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d ack pending", info.NumAckPending)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- runner.Shutdown(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if finished.Load() != 16 {
		t.Fatalf("finished %d of 16", finished.Load())
	}
	select {
	case <-agent.cc.Closed():
	default:
		t.Fatal("Stop returned before the consumer closed")
	}
}

func TestLagAgesWhileStalledAndResetsWhenIdle(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 3; i++ {
		publishInput(t, b, fmt.Sprintf("lag-%d", i), "x")
	}
	release := make(chan struct{})
	var once sync.Once
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "lag-reset",
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 1,
		AckWait:       5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			once.Do(func() { <-release })
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	waitFor(t, "lag > 1s", func() bool {
		return gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag-reset") > 1
	})
	if got := gaugeValue(t, metrics.Registry(), "dor_agent_ack_pending", "lag-reset"); got < 1 {
		t.Errorf("ack_pending = %v, want >= 1", got)
	}
	if got := gaugeValue(t, metrics.Registry(), "dor_agent_inflight", "lag-reset"); got < 1 {
		t.Errorf("inflight = %v, want >= 1", got)
	}
	close(release)
	waitFor(t, "lag reset", func() bool {
		return gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag-reset") == 0 &&
			gaugeValue(t, metrics.Registry(), "dor_agent_pending", "lag-reset") == 0 &&
			gaugeValue(t, metrics.Registry(), "dor_agent_ack_pending", "lag-reset") == 0
	})
	if got := gaugeValue(t, metrics.Registry(), "dor_agent_inflight", "lag-reset"); got != 0 {
		t.Errorf("inflight after idle = %v, want 0", got)
	}
}

func TestHandleHistogramHasOutcomeLabelsAndBuckets(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "hist",
		FilterSubject: bus.SubjectEchoIn,
		Handle: func(_ context.Context, msg *Message, _ *Emitter) error {
			if string(msg.Data) == "bad" {
				return Terminal(errors.New("bad"))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	publishInput(t, b, "hist-ok", "ok")
	publishInput(t, b, "hist-bad", "bad")
	waitFor(t, "terminal", func() bool { return agent.TerminalCount() == 1 })
	waitFor(t, "histogram", func() bool {
		return histogramOutcomeCount(t, metrics.Registry(), "hist") == 2
	})
	mfs, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sawTerminal bool
	for _, mf := range mfs {
		if mf.GetName() != "dor_agent_handle_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var outcome string
			for _, label := range m.GetLabel() {
				if label.GetName() == "outcome" {
					outcome = label.GetValue()
				}
			}
			if outcome == OutcomeTerminal && m.GetHistogram().GetSampleCount() == 1 {
				sawTerminal = true
			}
			buckets := m.GetHistogram().GetBucket()
			if len(buckets) == 0 || buckets[len(buckets)-1].GetUpperBound() < 120 {
				t.Errorf("terminal histogram buckets do not reach 120s: %v", buckets)
			}
		}
	}
	if !sawTerminal {
		t.Error("no terminal outcome observation")
	}
}

func histogramOutcomeCount(t *testing.T, reg *prometheus.Registry, agent string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, mf := range mfs {
		if mf.GetName() != "dor_agent_handle_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "agent" && label.GetValue() == agent {
					total += m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return total
}

func TestUnknownHealthIsUnhealthy(t *testing.T) {
	runner := NewRunner([]Agent{unknownAgent{}}, testLogger())
	if err := runner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ok, statuses := runner.Healthy()
	if ok {
		t.Fatal("unknown health treated as healthy")
	}
	if statuses["unknown"] != "unknown" {
		t.Fatalf("status = %q", statuses["unknown"])
	}
}

type unknownAgent struct{}

func (unknownAgent) Name() string                                                        { return "unknown" }
func (unknownAgent) Start(context.Context) error                                         { return nil }
func (unknownAgent) Stop(context.Context) error                                          { return nil }
func (unknownAgent) Consume(context.Context) error                                       { return nil }
func (unknownAgent) Publish(context.Context, string, []byte, ...bus.PublishOption) error { return nil }

func TestCloseBusDuringHandlerDoesNotPanic(t *testing.T) {
	b := testBus(t, t.TempDir())
	release := make(chan struct{})
	entered := make(chan struct{})
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "close-race",
		FilterSubject: bus.SubjectEchoIn,
		AckWait:       30 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			close(entered)
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	publishInput(t, b, "close-race", "x")
	<-entered
	if err := b.Close(); err != nil {
		t.Fatalf("bus close: %v", err)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = runner.Shutdown(ctx)
}

// Simulates a crash after the output PubAck but before the input Ack using the
// same durable the replacement handler will use. The replacement must really
// receive the redelivery, and the output id must keep the stored output at one.
func TestPublishBeforeAckCrashInsideHorizonDoesNotDuplicate(t *testing.T) {
	b := testBusWithOptions(t, t.TempDir(), bus.Options{DuplicateWindow: 30 * time.Second})
	defer b.Close()
	if bus.DefaultDuplicateWindow != 20*time.Minute {
		t.Fatalf("documented processing dedupe horizon = %s, want 20m", bus.DefaultDuplicateWindow)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spec := echoSpec()
	spec.AckWait = 50 * time.Millisecond
	seq := publishInput(t, b, "crash-1", "crash-body")

	// Read the input without acking it, as a crashed process would have.
	consumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       spec.Durable,
		FilterSubject: bus.SubjectEchoIn,
		AckWait:       spec.AckWait,
		MaxAckPending: spec.MaxAckPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	inputID := msg.Headers().Get(bus.HeaderMsgID)
	if inputID != "crash-1" {
		t.Fatalf("input id = %q, want crash-1", inputID)
	}
	if _, err := b.Publish(ctx, bus.SubjectEchoOut, msg.Data(), bus.WithMsgID(EchoOutputMsgID(inputID))); err != nil {
		t.Fatal(err)
	}
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 1)

	// Restart the same durable. It must receive the unacked input, run the
	// replacement handler, and republish the same output id.
	metrics := NewMetrics()
	echo, err := NewBase(b, metrics, testLogger(), spec)
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, echo)
	defer stopRunner(t, runner)
	waitConsumerAcked(t, b, spec.Durable, seq)
	waitFor(t, "same-durable redelivery", func() bool {
		return counterValue(t, metrics.Registry(), "dor_agent_redelivered_total", spec.Durable) >= 1
	})

	stream, err := b.StreamNamed(ctx, bus.StreamEcho)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(bus.SubjectEchoOut))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.State.Subjects[bus.SubjectEchoOut]; got != 1 {
		t.Fatalf("output count = %d after same-durable crash/restart, want 1", got)
	}
}

func TestPrefetchDoesNotCauseAckWaitStorm(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 8; i++ {
		publishInput(t, b, fmt.Sprintf("prefetch-%d", i), "x")
	}
	var calls, redeliveries atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "prefetch",
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 8,
		AckWait:       40 * time.Millisecond,
		Handle: func(_ context.Context, msg *Message, _ *Emitter) error {
			calls.Add(1)
			if msg.NumDelivered > 1 {
				redeliveries.Add(1)
			}
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	time.Sleep(1200 * time.Millisecond)
	stopRunner(t, runner)
	if redeliveries.Load() != 0 {
		t.Fatalf("calls=%d redeliveries=%d; prefetched messages expired behind the handler", calls.Load(), redeliveries.Load())
	}
	if calls.Load() != 8 {
		t.Fatalf("calls=%d, want 8", calls.Load())
	}
}

func TestHandlerPanicIsRecoveredAndRetried(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	var calls atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "panic",
		FilterSubject: bus.SubjectEchoIn,
		RetryBase:     10 * time.Millisecond,
		RetryMax:      10 * time.Millisecond,
		Handle: func(context.Context, *Message, *Emitter) error {
			if calls.Add(1) == 1 {
				panic("intentional test panic")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	publishInput(t, b, "panic-1", "x")
	waitFor(t, "retry after panic", func() bool { return calls.Load() >= 2 })
	waitConsumerAcked(t, b, "panic", 1)
}

func TestImmediateBusCloseWithBufferedCallbacks(t *testing.T) {
	b := testBus(t, t.TempDir())
	for i := 0; i < 8; i++ {
		publishInput(t, b, fmt.Sprintf("immediate-close-%d", i), "x")
	}
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	var once sync.Once
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "immediate-close",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   8,
		MaxAckPending: 8,
		AckWait:       5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	<-entered
	if err := b.Close(); err != nil {
		t.Fatalf("bus close: %v", err)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = runner.Shutdown(ctx)
}

func TestSaturatedPoolHeartbeatsWaitingDelivery(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 6; i++ {
		publishInput(t, b, fmt.Sprintf("saturated-%d", i), "x")
	}
	var calls, redeliveries atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "saturated",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   2,
		MaxAckPending: 6,
		AckWait:       50 * time.Millisecond,
		Handle: func(_ context.Context, msg *Message, _ *Emitter) error {
			calls.Add(1)
			if msg.NumDelivered > 1 {
				redeliveries.Add(1)
			}
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	time.Sleep(1800 * time.Millisecond)
	stopRunner(t, runner)
	if redeliveries.Load() != 0 {
		t.Fatalf("calls=%d redeliveries=%d; a delivery waiting for a worker expired", calls.Load(), redeliveries.Load())
	}
	if calls.Load() != 6 {
		t.Fatalf("calls=%d, want 6", calls.Load())
	}
}

func TestOrderedRetryBackoffProgresses(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	times := make(chan time.Time, 4)
	var calls atomic.Int32
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "ordered-backoff",
		FilterSubject: bus.SubjectEchoIn,
		Ordered:       true,
		RetryBase:     30 * time.Millisecond,
		RetryMax:      240 * time.Millisecond,
		Handle: func(context.Context, *Message, *Emitter) error {
			times <- time.Now()
			if calls.Add(1) < 4 {
				return errors.New("temporary")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	publishInput(t, b, "ordered-backoff-1", "x")

	var got []time.Time
	for i := 0; i < 4; i++ {
		select {
		case ts := <-times:
			got = append(got, ts)
		case <-time.After(3 * time.Second):
			t.Fatalf("missing retry %d; got %d attempts", i+1, len(got))
		}
	}
	gaps := []time.Duration{got[1].Sub(got[0]), got[2].Sub(got[1]), got[3].Sub(got[2])}
	if gaps[0] < 20*time.Millisecond || gaps[1] < 50*time.Millisecond || gaps[2] < 100*time.Millisecond {
		t.Fatalf("ordered retry gaps = %v, want approximately 30ms, 60ms, 120ms", gaps)
	}
}

func TestShutdownCancelsCooperativeHandler(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	cancelled := make(chan struct{})
	finished := make(chan struct{})
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "shutdown-cancel",
		FilterSubject: bus.SubjectEchoIn,
		DrainTimeout:  50 * time.Millisecond,
		Handle: func(ctx context.Context, _ *Message, _ *Emitter) error {
			close(entered)
			select {
			case <-ctx.Done():
				close(cancelled)
			case <-release:
			}
			close(finished)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	publishInput(t, b, "shutdown-cancel-1", "x")
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = runner.Shutdown(ctx)
	elapsed := time.Since(start)
	wasCancelled := false
	select {
	case <-cancelled:
		wasCancelled = true
	default:
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}
	select {
	case <-agent.done:
	case <-time.After(time.Second):
		t.Fatal("agent did not finish draining")
	}
	if !wasCancelled || elapsed > time.Second {
		t.Fatalf("shutdown elapsed=%s err=%v cancelled=%v; want cancellation within the shutdown budget", elapsed, err, wasCancelled)
	}
}

func TestRetryWaitKeepsQueueLag(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	attempted := make(chan struct{}, 8)
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "retry-lag",
		FilterSubject: bus.SubjectEchoIn,
		RetryBase:     1500 * time.Millisecond,
		RetryMax:      1500 * time.Millisecond,
		Handle: func(context.Context, *Message, *Emitter) error {
			attempted <- struct{}{}
			return errors.New("dependency unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	publishInput(t, b, "retry-lag-1", "x")
	<-attempted
	time.Sleep(150 * time.Millisecond)
	agent.updateStats(context.Background())
	first := gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "retry-lag")
	time.Sleep(450 * time.Millisecond)
	agent.updateStats(context.Background())
	lag := gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "retry-lag")
	info, err := agent.consumer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending > 0 {
		if first <= 0 || lag <= first {
			t.Fatalf("during retry delay lag did not grow: first=%.3fs later=%.3fs", first, lag)
		}
		if lag < 0.4 {
			t.Fatalf("during retry delay: NumPending=%d NumAckPending=%d inflight=%d lag=%v; want lag to age",
				info.NumPending, info.NumAckPending, agent.inflightCount.Load(), lag)
		}
	}
}

// TestForcedShutdownAlwaysJoinsWorkers is the structural-cleanup regression:
// even when the shutdown budget has already expired, callbacks are unblocked,
// jobs is closed, workers are joined, and fullyStopped closes.
func TestForcedShutdownAlwaysJoinsWorkers(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	for i := 0; i < 3; i++ {
		publishInput(t, b, fmt.Sprintf("forced-cleanup-%d", i), "x")
	}
	entered := make(chan struct{})
	var once sync.Once
	agent, err := NewBase(b, NewMetrics(), testLogger(), Spec{
		Name:          "forced-cleanup",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   2,
		DrainTimeout:  30 * time.Millisecond,
		Handle: func(ctx context.Context, _ *Message, _ *Emitter) error {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer func() {
		// Keep a failed assertion from leaking a blocked pool into b.Close.
		agent.forceStop()
		agent.callbacks.Wait()
		agent.closeJobs()
		agent.workers.Wait()
	}()
	<-entered
	waitFor(t, "waiting callbacks behind a busy worker", func() bool {
		info, err := agent.consumer.Info(context.Background())
		return err == nil && info.NumAckPending >= 2
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = runner.Shutdown(ctx) // Stop may report the expired drain budget; cleanup still must finish.

	select {
	case <-agent.fullyStopped:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("fullyStopped did not close after forced shutdown")
	}
	workersDone := make(chan struct{})
	go func() {
		agent.workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("worker pool did not join after forced shutdown")
	}
}

// TestQueueLagHealthyContinuousLoad verifies the server-authoritative metric
// does not measure a continuous busy period. At ~50/s with 8 workers and 80ms
// handlers, any in-flight message is younger than the 0.5s bound.
func TestQueueLagHealthyContinuousLoad(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "lag-healthy",
		FilterSubject: bus.SubjectEchoIn,
		Concurrency:   8,
		MaxAckPending: 16,
		Handle: func(context.Context, *Message, *Emitter) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	for i := 0; i < 80; i++ {
		publishInput(t, b, fmt.Sprintf("lag-healthy-%d", i), "x")
		time.Sleep(20 * time.Millisecond)
	}
	waitFor(t, "healthy load has an active handler", func() bool {
		return agent.inflightCount.Load() > 0
	})
	agent.updateStats(context.Background())
	info, err := agent.consumer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lag := gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag-healthy")
	t.Logf("healthy load: NumPending=%d NumAckPending=%d lag=%.3fs", info.NumPending, info.NumAckPending, lag)
	if lag >= 0.5 {
		t.Fatalf("healthy load reported lag %.3fs; want <0.5s", lag)
	}
}

// TestQueueLagSurvivesUnackedConsumerRestart models a process that fetched a
// message and died before acking. A new agent on the same durable must still
// report the age from the server before redelivery.
func TestQueueLagSurvivesUnackedConsumerRestart(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	publishInput(t, b, "lag-crash-1", "x")
	time.Sleep(150 * time.Millisecond) // Age the message before the simulated process crash.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "lag-crash",
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 1,
		AckWait:       5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("fetch before restart: %v", err)
	}
	if msg == nil {
		t.Fatal("fetch before restart returned no message")
	}
	// Deliberately do not Ack: this is the crashed process's outstanding work.

	release := make(chan struct{})
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:          "lag-crash",
		Durable:       "lag-crash",
		FilterSubject: bus.SubjectEchoIn,
		AckWait:       5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer func() {
		close(release)
		stopRunner(t, runner)
	}()
	waitFor(t, "unacked message survives restart", func() bool {
		info, err := agent.consumer.Info(context.Background())
		return err == nil && info.NumAckPending >= 1
	})
	agent.updateStats(context.Background())
	lag := gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag-crash")
	info, err := agent.consumer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after unacked restart: NumPending=%d NumAckPending=%d lag=%.3fs", info.NumPending, info.NumAckPending, lag)
	if lag <= 0 {
		t.Fatal("queue lag after an unacked restart is zero; server-authoritative age was lost")
	}
}

// TestQueueLagUsesOldestAcrossFilterSubjects ensures the server lookup covers
// every configured filter and takes the minimum publish time, not just the
// first filter.
func TestQueueLagUsesOldestAcrossFilterSubjects(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Publish(ctx, bus.SubjectEchoOut, []byte("old"), bus.WithMsgID("filter-old")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	publishInput(t, b, "filter-new", "new")

	release := make(chan struct{})
	metrics := NewMetrics()
	agent, err := NewBase(b, metrics, testLogger(), Spec{
		Name:           "lag-filters",
		FilterSubjects: []string{bus.SubjectEchoIn, bus.SubjectEchoOut},
		MaxAckPending:  1,
		AckWait:        5 * time.Second,
		Handle: func(context.Context, *Message, *Emitter) error {
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer func() {
		close(release)
		stopRunner(t, runner)
	}()
	waitFor(t, "filtered consumer has an in-flight message", func() bool {
		info, err := agent.consumer.Info(context.Background())
		return err == nil && info.NumAckPending >= 1
	})
	agent.updateStats(context.Background())
	lag := gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag-filters")
	if lag <= 0.2 {
		t.Fatalf("lag=%0.3fs; want age of the oldest filter subject (>0.2s)", lag)
	}
}
