package agents

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ankojh/dor/internal/bus"
)

func gaugeValue(t *testing.T, reg *prometheus.Registry, family, agent string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "agent" && l.GetValue() == agent {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s{agent=%q} not found", family, agent)
	return 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRegistrySelectLists(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	registry := NewRegistry()
	for _, n := range []string{"zeta", "alpha", "mid"} {
		spec := echoSpec()
		spec.Name, spec.Durable = n, n
		a, err := NewBase(b, metrics, testLogger(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(registry.Names(), ","); got != "zeta,alpha,mid" {
		t.Errorf("Names = %s, want registration order", got)
	}
	all, _ := registry.Select("all")
	if got := strings.Join(names(all), ","); got != "alpha,mid,zeta" {
		t.Errorf("Select(all) = %s, want sorted", got)
	}
	got, err := registry.Select(" zeta , alpha,zeta,")
	if err != nil || strings.Join(names(got), ",") != "zeta,alpha" {
		t.Errorf("Select(list) = %v, %v; want zeta,alpha deduped in request order", names(got), err)
	}
	for _, empty := range []string{"", "  ", "none"} {
		got, err := registry.Select(empty)
		if err != nil || len(got) != 0 {
			t.Errorf("Select(%q) = %v, %v; want none", empty, names(got), err)
		}
	}
	if _, err := registry.Select(",,"); err == nil {
		t.Error("Select(\",,\") should fail: resolves to no agents")
	}
	_, err = registry.Select("alpha,nope")
	if err == nil || !strings.Contains(err.Error(), `"nope"`) || !strings.Contains(err.Error(), "zeta") {
		t.Errorf("unknown agent error should name it and list known agents: %v", err)
	}
	if err := registry.Register(nil); err == nil {
		t.Error("registering nil succeeded")
	}
}

func TestNewBaseValidation(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	m := NewMetrics()
	h := func(context.Context, *Message, *Emitter) error { return nil }
	cases := map[string]Spec{
		"no name":    {FilterSubject: "echo.in", Handle: h},
		"no subject": {Name: "x", Handle: h},
		"no handler": {Name: "x", FilterSubject: "echo.in"},
	}
	for name, spec := range cases {
		if _, err := NewBase(b, m, nil, spec); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := NewBase(nil, m, nil, Spec{}); err == nil {
		t.Error("nil bus accepted")
	}
	if _, err := NewBase(b, nil, nil, Spec{}); err == nil {
		t.Error("nil metrics accepted")
	}
}

// Every agent label is present with zero values before any message arrives.
func TestMetricsPresentBeforeTraffic(t *testing.T) {
	m := NewMetrics()
	m.RegisterAgent("idle")
	reg := m.Registry()
	for _, fam := range []string{"dor_agent_in_total", "dor_agent_out_total", "dor_agent_errors_total"} {
		if got := counterValue(t, reg, fam, "idle"); got != 0 {
			t.Errorf("%s = %v, want 0", fam, got)
		}
	}
	if got := histogramCount(t, reg, "dor_agent_handle_seconds", "idle"); got != 0 {
		t.Errorf("handle histogram count = %d", got)
	}
	for _, fam := range []string{"dor_agent_queue_lag_seconds", "dor_agent_pending"} {
		if !gaugeExists(t, reg, fam, "idle") {
			t.Errorf("%s missing", fam)
		}
	}
}

func TestHandlerErrorNaksAndCountsErrors(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	var calls atomic.Int32
	var maxDelivered atomic.Uint64
	a, err := NewBase(b, metrics, testLogger(), Spec{
		Name: "flaky", FilterSubject: bus.SubjectEchoIn, AckWait: 5 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			maxDelivered.Store(msg.NumDelivered)
			if calls.Add(1) == 1 {
				return errors.New("boom")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, a)
	defer stopRunner(t, runner)
	seq := publishInput(t, b, "flaky-1", "x")
	waitConsumerAcked(t, b, "flaky", seq)
	reg := metrics.Registry()
	errs := counterValue(t, reg, "dor_agent_errors_total", "flaky")
	in := counterValue(t, reg, "dor_agent_in_total", "flaky")
	ok := histogramCount(t, reg, "dor_agent_handle_seconds", "flaky")
	if errs != 1 {
		t.Errorf("errors = %v, want 1", errs)
	}
	// Under load JetStream may redeliver more than once; the invariant is that
	// every delivery is either an error or a timed success.
	if in < 2 || float64(ok) != in-errs {
		t.Errorf("in=%v errors=%v handle observations=%d; want in>=2 and observations=in-errors", in, errs, ok)
	}
	if maxDelivered.Load() < 2 {
		t.Errorf("redelivery NumDelivered = %d, want >= 2", maxDelivered.Load())
	}
}

func TestQueueLagAndPendingValues(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	// Backlog exists before the agent starts, so lag is measurably > 0.
	for i := 0; i < 3; i++ {
		publishInput(t, b, "lag-"+string(rune('a'+i)), "x")
	}
	time.Sleep(50 * time.Millisecond)
	release := make(chan struct{})
	a, err := NewBase(b, metrics, testLogger(), Spec{
		Name: "lag", FilterSubject: bus.SubjectEchoIn, MaxAckPending: 1,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, a)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		stopRunner(t, runner)
	}()
	waitFor(t, "queue lag > 0", func() bool {
		return gaugeValue(t, metrics.Registry(), "dor_agent_queue_lag_seconds", "lag") > 0
	})
	// One message in flight, MaxAckPending=1: the rest stay pending.
	waitFor(t, "pending >= 2", func() bool {
		return gaugeValue(t, metrics.Registry(), "dor_agent_pending", "lag") >= 2
	})
	close(release)
	released = true
}

func TestStopBeforeStartAndDoubleStart(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	a, err := NewBase(b, NewMetrics(), testLogger(), echoSpec())
	if err != nil {
		t.Fatal(err)
	}
	if a.Running() {
		t.Error("running before start")
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !a.Running() {
		t.Error("not running after start")
	}
	if err := a.Start(context.Background()); err == nil {
		t.Error("second Start should fail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if a.Running() {
		t.Error("running after stop")
	}
	if err := a.Stop(ctx); err != nil {
		t.Errorf("second Stop should be a no-op: %v", err)
	}
	if err := a.LastError(); err != nil {
		t.Errorf("LastError after clean stop = %v", err)
	}
	// Stop on an agent that never started is harmless.
	fresh, _ := NewBase(b, NewMetrics(), testLogger(), echoSpec())
	if err := fresh.Stop(ctx); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
}

func fetchOne(t *testing.T, b *bus.Bus, subject string) jetstream.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := bus.StreamForSubject(subject)
	if streamName == "" {
		t.Fatalf("subject %s belongs to no stream", subject)
	}
	c, err := b.JS().CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		FilterSubject: subject, AckPolicy: jetstream.AckNonePolicy, InactiveThreshold: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := c.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("fetch %s: %v", subject, err)
	}
	return msg
}

func TestEchoOutputCarriesHeaders(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	echo, err := NewEcho(b, NewMetrics(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, echo)
	defer stopRunner(t, runner)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Publish(ctx, bus.SubjectEchoIn, []byte("hi"), bus.WithMsgID("hdr-in"),
		bus.WithHeaders(map[string][]string{bus.HeaderArticleID: {"art-42"}})); err != nil {
		t.Fatal(err)
	}
	waitSubjectMsgs(t, b, bus.SubjectEchoOut, 1)
	msg := fetchOne(t, b, bus.SubjectEchoOut)
	if got, want := msg.Headers().Get(bus.HeaderMsgID), EchoOutputMsgID("hdr-in"); got != want {
		t.Errorf("Nats-Msg-Id = %q, want %q", got, want)
	}
	if got := msg.Headers().Get(bus.HeaderArticleID); got != "art-42" {
		t.Errorf("Dor-Article-Id = %q, want art-42", got)
	}
	if string(msg.Data()) != "hi" {
		t.Errorf("payload = %q", msg.Data())
	}
}

// Two agents chained echo.in -> echo.out (stage1) and echo.out -> echo.out
// copy is not useful, so stage 2 reads echo.out and writes delta.out.
func TestTraceAndArticleIDPropagateAcrossAgents(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	b := testBus(t, t.TempDir())
	defer b.Close()
	metrics := NewMetrics()
	first, err := NewBase(b, metrics, testLogger(), echoSpec())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewBase(b, metrics, testLogger(), Spec{
		Name: "second", FilterSubject: bus.SubjectEchoOut,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			return out.Publish(ctx, bus.SubjectDeltaOut, msg.Data, bus.WithMsgID("second-"+msg.ID()))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, first, second)
	defer stopRunner(t, runner)

	ctx, root := tp.Tracer("test").Start(context.Background(), "root")
	rootSC := root.SpanContext()
	h := map[string][]string{bus.HeaderArticleID: {"art-7"}}
	injectTrace(ctx, h)
	if _, err := b.Publish(ctx, bus.SubjectEchoIn, []byte("chain"), bus.WithMsgID("chain-in"), bus.WithHeaders(h)); err != nil {
		t.Fatal(err)
	}
	root.End()
	waitSubjectMsgs(t, b, bus.SubjectDeltaOut, 1)

	msg := fetchOne(t, b, bus.SubjectDeltaOut)
	if got := msg.Headers().Get(bus.HeaderArticleID); got != "art-7" {
		t.Errorf("final Dor-Article-Id = %q", got)
	}
	// otel writes the lowercase W3C key; nats.Header does not canonicalise it.
	tp2 := msg.Headers().Get("traceparent")
	if !strings.Contains(tp2, rootSC.TraceID().String()) {
		t.Errorf("final traceparent %q does not carry trace %s", tp2, rootSC.TraceID())
	}

	waitFor(t, "two consumer spans", func() bool {
		n := 0
		for _, s := range exp.GetSpans() {
			if s.Name == "agent.handle" {
				n++
			}
		}
		return n >= 2
	})
	var handle []tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.Name == "agent.handle" {
			handle = append(handle, s)
		}
	}
	byAgent := map[string]tracetest.SpanStub{}
	for _, s := range handle {
		for _, a := range s.Attributes {
			if a.Key == "dor.agent" {
				byAgent[a.Value.AsString()] = s
			}
		}
	}
	for _, name := range []string{"echo", "second"} {
		s, ok := byAgent[name]
		if !ok {
			t.Fatalf("no span for agent %s", name)
		}
		if s.SpanContext.TraceID() != rootSC.TraceID() {
			t.Errorf("%s span trace %s != root %s", name, s.SpanContext.TraceID(), rootSC.TraceID())
		}
		if !hasAttr(s.Attributes, "dor.article_id", "art-7") {
			t.Errorf("%s span lacks dor.article_id=art-7: %v", name, s.Attributes)
		}
	}
	if byAgent["echo"].Parent.SpanID() != rootSC.SpanID() {
		t.Error("echo span is not a child of the publisher span")
	}
	if byAgent["second"].Parent.SpanID() != byAgent["echo"].SpanContext.SpanID() {
		t.Error("second span is not a child of the echo span")
	}
}

func TestSpanOmitsArticleIDWhenAbsent(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	b := testBus(t, t.TempDir())
	defer b.Close()
	echo, _ := NewEcho(b, NewMetrics(), testLogger())
	runner := startRunner(t, echo)
	defer stopRunner(t, runner)
	seq := publishInput(t, b, "no-article", "x")
	waitConsumerAcked(t, b, "echo", seq)
	waitFor(t, "span", func() bool { return len(exp.GetSpans()) >= 1 })
	for _, s := range exp.GetSpans() {
		for _, a := range s.Attributes {
			if a.Key == "dor.article_id" {
				t.Errorf("unexpected dor.article_id %v", a.Value)
			}
		}
	}
}

func hasAttr(attrs []attribute.KeyValue, key, val string) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == val {
			return true
		}
	}
	return false
}
