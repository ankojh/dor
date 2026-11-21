package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

// Agent is the contract every worker implements. Start binds a durable
// consumer (or starts a producer loop) and begins delivery, Consume is the
// blocking form of the same loop, Stop drains in-flight work, and Publish
// writes an idempotent message.
type Agent interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Consume(ctx context.Context) error
	Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error
}

// HealthReporter is implemented by agents that can say whether their consumer
// (or producer loop) is currently active. Unknown health is unhealthy.
type HealthReporter interface {
	Running() bool
	LastError() error
	TerminalCount() int64
}

// Handler processes one message. out.Publish is the only way an agent should
// write to the bus; it carries trace context and the article id.
//
// A plain error means retry (transient). Return Terminal(err) for a permanent
// failure; the message is copied to the DLQ and terminated.
type Handler func(ctx context.Context, msg *Message, out *Emitter) error

type outcome int

const (
	outcomeRetry outcome = iota
	outcomeTerminal
)

type handlerError struct {
	outcome outcome
	err     error
}

func (e *handlerError) Error() string { return e.err.Error() }
func (e *handlerError) Unwrap() error { return e.err }

// Retry marks a handler error as transient (the default).
func Retry(err error) error {
	if err == nil {
		return nil
	}
	return &handlerError{outcome: outcomeRetry, err: err}
}

// Terminal marks a handler error as permanent. The framework writes the
// message to the DLQ, confirms the write, then terminates the input.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &handlerError{outcome: outcomeTerminal, err: err}
}

func classify(err error) (outcome, error) {
	if err == nil {
		return outcomeRetry, nil
	}
	var he *handlerError
	if errors.As(err, &he) {
		return he.outcome, he.err
	}
	return outcomeRetry, err
}

// Spec describes one agent instance.
type Spec struct {
	// Name is the registry key and the agent metric label.
	Name string
	// Durable is the JetStream consumer name. Defaults to Name.
	Durable string
	// FilterSubject is a single subject. Prefer FilterSubjects for new agents.
	FilterSubject string
	// FilterSubjects are the subjects or wildcards this consumer binds. All
	// filters must belong to one stream.
	FilterSubjects []string
	// MaxAckPending defaults to the bus default.
	MaxAckPending int
	// AckWait defaults to 30 s.
	AckWait time.Duration
	// RetryBase is the first NAK delay. Default 1 s.
	RetryBase time.Duration
	// RetryMax caps the exponential NAK delay. Default 2 min.
	RetryMax time.Duration
	// DrainTimeout bounds consumer drain on shutdown. Default 30 s.
	DrainTimeout time.Duration
	// Concurrency is the bounded worker pool for unordered stages. Default 1.
	Concurrency int
	// Ordered serialises a shard consumer: one handler at a time and retries
	// happen in place, so a newer message never overtakes a failed older one.
	Ordered bool
	// Producer runs a scheduled/polling agent with no input consumer. Produce
	// must honour ctx and return when it is cancelled.
	Producer bool
	// Produce is the producer body when Producer is true.
	Produce func(ctx context.Context, out *Emitter) error
	// Handle is the consumer body for non-producer agents.
	Handle Handler
}

// Message is one delivered JetStream message.
type Message struct {
	Subject        string
	Data           []byte
	Header         nats.Header
	Timestamp      time.Time
	StreamSequence uint64
	NumDelivered   uint64
}

// ID returns the publisher's Nats-Msg-Id, or a stable stream sequence when the
// producer did not set one. Agents derive their own output ids from this value
// so redelivery is deduplicated by JetStream.
func (m *Message) ID() string {
	if m == nil {
		return ""
	}
	if id := m.Header.Get(bus.HeaderMsgID); id != "" {
		return id
	}
	return fmt.Sprintf("stream-seq-%d", m.StreamSequence)
}

// ArticleID returns the Dor-Article-Id header.
func (m *Message) ArticleID() string {
	if m == nil {
		return ""
	}
	return m.Header.Get(bus.HeaderArticleID)
}

// Emitter publishes from inside a handler.
type Emitter struct {
	agent *Base
	ctx   context.Context
}

// Publish writes to JetStream. The article id of the handled message is
// carried automatically, and the active span becomes the parent of the next
// agent's span.
func (e *Emitter) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	if ctx == nil {
		ctx = e.ctx
	}
	if ArticleIDFrom(ctx) == "" {
		if id := ArticleIDFrom(e.ctx); id != "" {
			ctx = WithArticleID(ctx, id)
		}
	}
	return e.agent.Publish(ctx, subject, data, opts...)
}

// job is one delivered message plus the heartbeat that protects it from
// AckWait while it waits for a worker or is being handled.
type job struct {
	msg           jetstream.Msg
	heartbeatDone chan struct{}
}

// Base is the shared implementation of Agent. Concrete agents supply a
// Spec.Handle (consumer) or Spec.Produce (scheduled producer).
type Base struct {
	spec    Spec
	bus     *bus.Bus
	metrics *Metrics
	log     *slog.Logger

	consumer         jetstream.Consumer
	cc               jetstream.ConsumeContext
	jobs             chan job
	jobsWG           sync.WaitGroup
	workers          sync.WaitGroup
	callbacks        sync.WaitGroup
	callbackMu       sync.Mutex
	callbacksClosed  bool
	jobsOnce         sync.Once
	forceCh          chan struct{}
	forceOnce        sync.Once
	handlerCancel    context.CancelFunc
	fullyStopped     chan struct{}
	fullyStoppedOnce sync.Once

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	runCtx   context.Context
	lastErr  error
	ready    chan error
	terminal atomic.Int64

	shutdownMu    sync.Mutex
	shutdownCtx   context.Context
	inflightCount atomic.Int64
}

// NewBase validates a Spec and prepares metrics. It does not connect.
func NewBase(b *bus.Bus, metrics *Metrics, log *slog.Logger, spec Spec) (*Base, error) {
	if b == nil {
		return nil, errors.New("agents: bus is required")
	}
	if metrics == nil {
		return nil, errors.New("agents: metrics are required")
	}
	if spec.Name == "" {
		return nil, errors.New("agents: name is required")
	}
	if spec.Durable == "" {
		spec.Durable = spec.Name
	}
	if log == nil {
		log = slog.Default()
	}
	if spec.Concurrency < 0 {
		return nil, fmt.Errorf("agents: %s concurrency cannot be negative", spec.Name)
	}
	if spec.Concurrency == 0 {
		spec.Concurrency = 1
	}
	if spec.Ordered {
		if spec.Concurrency > 1 {
			return nil, fmt.Errorf("agents: %s cannot be ordered and concurrent", spec.Name)
		}
		spec.Concurrency = 1
		spec.MaxAckPending = 1
	}
	if spec.AckWait <= 0 {
		spec.AckWait = 30 * time.Second
	}
	if spec.RetryBase <= 0 {
		spec.RetryBase = time.Second
	}
	if spec.RetryMax <= 0 {
		spec.RetryMax = 2 * time.Minute
	}
	if spec.RetryMax < spec.RetryBase {
		return nil, fmt.Errorf("agents: %s retry max is smaller than retry base", spec.Name)
	}
	if spec.DrainTimeout <= 0 {
		spec.DrainTimeout = 30 * time.Second
	}
	if spec.Producer {
		if spec.Produce == nil {
			return nil, fmt.Errorf("agents: %s is a producer but has no Produce function", spec.Name)
		}
		if spec.Handle != nil {
			return nil, fmt.Errorf("agents: %s cannot be both producer and consumer", spec.Name)
		}
	} else {
		if len(filtersForSpec(spec)) == 0 {
			return nil, fmt.Errorf("agents: %s has no filter subject", spec.Name)
		}
		if spec.Handle == nil {
			return nil, fmt.Errorf("agents: %s has no handler", spec.Name)
		}
	}
	metrics.RegisterAgent(spec.Name)
	return &Base{
		spec:         spec,
		bus:          b,
		metrics:      metrics,
		log:          log.With("agent", spec.Name),
		ready:        make(chan error, 1),
		forceCh:      make(chan struct{}),
		fullyStopped: make(chan struct{}),
	}, nil
}

func filtersForSpec(spec Spec) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	add(spec.FilterSubject)
	for _, s := range spec.FilterSubjects {
		add(s)
	}
	return out
}

// Name implements Agent.
func (a *Base) Name() string { return a.spec.Name }

// Running reports whether the consumer or producer loop is active.
func (a *Base) Running() bool { return a.running.Load() && !a.stopped.Load() }

// LastError returns the last consumer/producer loop error.
func (a *Base) LastError() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastErr
}

// TerminalCount returns messages terminated after a DLQ write.
func (a *Base) TerminalCount() int64 { return a.terminal.Load() }

// Start creates the consumer or producer loop and returns once it is live.
func (a *Base) Start(ctx context.Context) error {
	if !a.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: %s already started", a.spec.Name)
	}
	if a.stopped.Load() {
		return fmt.Errorf("agents: %s is stopped", a.spec.Name)
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.cancel = cancel
	a.runCtx = runCtx
	a.done = make(chan struct{})
	done := a.done
	a.mu.Unlock()

	go func() {
		defer close(done)
		if err := a.Consume(runCtx); err != nil {
			a.setError(err)
			a.log.Error("agent stopped", "err", err)
		}
	}()

	select {
	case err := <-a.ready:
		if err != nil {
			cancel()
			return err
		}
		return nil
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	case <-time.After(10 * time.Second):
		cancel()
		return fmt.Errorf("agents: %s did not subscribe within 10s", a.spec.Name)
	}
}

// Consume subscribes (or runs the producer) and blocks until ctx is cancelled.
// It drains in-flight work and acknowledges it before returning.
func (a *Base) Consume(ctx context.Context) error {
	a.mu.Lock()
	a.runCtx = ctx
	a.mu.Unlock()
	if a.spec.Producer {
		return a.runProducer(ctx)
	}

	consumer, err := a.bus.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:        a.spec.Durable,
		FilterSubjects: filtersForSpec(a.spec),
		MaxAckPending:  a.spec.MaxAckPending,
		AckWait:        a.spec.AckWait,
		MaxDeliver:     -1,
	})
	if err != nil {
		a.closeFullyStopped()
		a.signalReady(err)
		return err
	}
	a.mu.Lock()
	a.consumer = consumer
	a.mu.Unlock()

	// Handler contexts are independent of the pull loop: a normal Stop cancels
	// pulling but lets in-flight handlers finish. The shutdown budget cancels
	// them explicitly, and drain waits for real consumer closure and workers.
	handlerCtx, handlerCancel := context.WithCancel(context.WithoutCancel(ctx))
	a.mu.Lock()
	a.handlerCancel = handlerCancel
	a.mu.Unlock()
	defer handlerCancel()

	// Unbuffered: a callback blocks until a worker is free. The callback starts
	// the heartbeat immediately, before it blocks, so a delivered message is
	// protected even while every worker is busy.
	a.jobs = make(chan job)
	a.workers.Add(a.spec.Concurrency)
	for i := 0; i < a.spec.Concurrency; i++ {
		go a.worker(ctx, handlerCtx)
	}

	cc, err := consumer.Consume(a.enqueue,
		jetstream.PullMaxMessages(1),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if errors.Is(err, jetstream.ErrConsumerDeleted) ||
				errors.Is(err, jetstream.ErrConsumerNotFound) ||
				errors.Is(err, jetstream.ErrStreamNotFound) {
				a.setError(err)
				a.log.Error("consumer terminal error", "err", err)
				a.running.Store(false)
			} else {
				a.log.Warn("consumer transient error", "err", err)
			}
		}),
	)
	if err != nil {
		a.closeJobs()
		a.workers.Wait()
		a.closeFullyStopped()
		a.signalReady(err)
		return err
	}
	a.mu.Lock()
	a.cc = cc
	a.mu.Unlock()
	a.running.Store(true)
	a.signalReady(nil)
	a.log.Info("agent started",
		"durable", a.spec.Durable,
		"subjects", filtersForSpec(a.spec),
		"concurrency", a.spec.Concurrency,
		"ordered", a.spec.Ordered,
	)

	go a.monitor(ctx)

	<-ctx.Done()
	a.running.Store(false)
	a.drain()
	return nil
}

func (a *Base) runProducer(ctx context.Context) error {
	a.running.Store(true)
	a.signalReady(nil)
	a.log.Info("producer started")
	go a.monitor(ctx)
	err := a.spec.Produce(ctx, &Emitter{agent: a, ctx: ctx})
	a.running.Store(false)
	a.closeFullyStopped()
	if err != nil && ctx.Err() == nil {
		a.setError(err)
		a.log.Error("producer stopped", "err", err)
		return err
	}
	return nil
}

// Stop cancels the loop and waits for drain completion, bounded by ctx. The
// drain budget is the smaller of ctx and Spec.DrainTimeout; when it expires,
// handler contexts are cancelled, the consumer is stopped, and the agent
// returns without the old unconditional five-second wait.
func (a *Base) Stop(ctx context.Context) error {
	if a.stopped.Swap(true) {
		return nil
	}
	a.running.Store(false)
	a.mu.Lock()
	cancel := a.cancel
	done := a.done
	a.mu.Unlock()
	if cancel == nil {
		a.closeFullyStopped()
		return nil
	}
	drainCtx, drainCancel := a.newDrainContext(ctx)
	a.shutdownMu.Lock()
	a.shutdownCtx = drainCtx
	a.shutdownMu.Unlock()
	cancel()
	if done == nil {
		drainCancel()
		return nil
	}
	select {
	case <-done:
		drainCancel()
		a.waitFullyStopped()
		return nil
	case <-ctx.Done():
		a.cancelHandlers()
		a.forceStop()
		// Stop is bounded even when the drain loop is still finishing in the
		// background. Waiting for fullyStopped (rather than done) lets a
		// cooperative handler observe cancellation before Stop returns, while a
		// non-cooperative handler cannot hold the caller beyond the grace.
		a.waitFullyStopped()
		drainCancel()
		return ctx.Err()
	}
}

// newDrainContext bounds draining by both the caller's shutdown budget and the
// agent's configured DrainTimeout.
func (a *Base) newDrainContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := a.spec.DrainTimeout
	if deadline, ok := parent.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(parent, timeout)
}

func (a *Base) cancelHandlers() {
	a.mu.Lock()
	cancel := a.handlerCancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (a *Base) forceStop() {
	a.forceOnce.Do(func() { close(a.forceCh) })
	a.cancelHandlers()
	a.mu.Lock()
	cc := a.cc
	a.mu.Unlock()
	if cc != nil {
		cc.Stop()
	}
}

// Publish writes one message, preserving the article id and trace context.
func (a *Base) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	h := nats.Header{}
	if id := ArticleIDFrom(ctx); id != "" {
		h.Set(bus.HeaderArticleID, id)
	}
	injectTrace(ctx, h)
	opts = append(opts, bus.WithHeaders(h))
	if _, err := a.bus.Publish(ctx, subject, data, opts...); err != nil {
		return err
	}
	a.metrics.out.WithLabelValues(a.spec.Name).Inc()
	return nil
}

func (a *Base) enqueue(msg jetstream.Msg) {
	a.callbackMu.Lock()
	if a.callbacksClosed {
		a.callbackMu.Unlock()
		return
	}
	a.callbacks.Add(1)
	a.callbackMu.Unlock()
	defer a.callbacks.Done()

	heartbeatDone := a.startHeartbeat(msg)
	a.jobsWG.Add(1)
	select {
	case a.jobs <- job{msg: msg, heartbeatDone: heartbeatDone}:
	case <-a.forceCh:
		// Forced shutdown: the worker pool may be stuck in a handler. Do not
		// leave an unacknowledged message heartbeating forever, and do not
		// block the consumer callback.
		closeHeartbeat(heartbeatDone)
		a.jobsWG.Done()
	}
}

func (a *Base) worker(runCtx, handlerCtx context.Context) {
	defer a.workers.Done()
	for j := range a.jobs {
		a.handleMessage(runCtx, handlerCtx, j.msg, j.heartbeatDone)
		a.jobsWG.Done()
	}
}

const forcedShutdownGrace = 250 * time.Millisecond

// drain stops pulling and lets queued workers finish. The normal path waits
// for the consumer's real closed signal; the forced path starts structural
// cleanup immediately and still waits only a bounded grace for that signal.
// There is no unconditional extra wait.
func (a *Base) drain() {
	a.shutdownMu.Lock()
	ctx := a.shutdownCtx
	a.shutdownMu.Unlock()
	var cancel context.CancelFunc
	if ctx == nil {
		ctx, cancel = context.WithTimeout(context.Background(), a.spec.DrainTimeout)
		defer cancel()
	}

	stopWatcher := make(chan struct{})
	defer close(stopWatcher)
	go func() {
		select {
		case <-ctx.Done():
			a.cancelHandlers()
			a.forceStop()
		case <-stopWatcher:
		}
	}()

	a.mu.Lock()
	cc := a.cc
	a.mu.Unlock()
	if cc == nil {
		a.finishDrain(ctx)
		return
	}

	go func() { cc.Drain() }()
	select {
	case <-cc.Closed():
		a.finishDrain(ctx)
	case <-ctx.Done():
		a.cancelHandlers()
		cc.Stop()
		// Start structural cleanup immediately after forcing. The consumer's
		// closed signal can lag; it must not delay closing jobs and joining
		// workers. Callbacks are already fenced by callbacksClosed, and forceCh
		// unblocks any send waiting for a busy worker.
		a.finishDrain(ctx)
		select {
		case <-cc.Closed():
		case <-time.After(forcedShutdownGrace):
			a.log.Warn("consumer closed signal did not arrive within the shutdown budget")
		}
	}
}

func (a *Base) finishDrain(ctx context.Context) {
	a.callbackMu.Lock()
	a.callbacksClosed = true
	a.callbackMu.Unlock()

	// Structural cleanup is never skipped. Once callbacks have returned no new
	// send can enter jobs, so it is safe to close the channel and join workers.
	// If the public shutdown budget is already gone, this runs in the background
	// and Stop stays bounded; the worker pool still terminates.
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		a.callbacks.Wait()
		a.closeJobs()
		a.workers.Wait()
		a.closeFullyStopped()
	}()

	select {
	case <-cleanupDone:
	case <-ctx.Done():
		a.cancelHandlers()
		a.forceStop()
	}
}

func (a *Base) closeFullyStopped() {
	a.fullyStoppedOnce.Do(func() { close(a.fullyStopped) })
}

// waitFullyStopped gives cooperative handlers a bounded chance to observe
// cancellation before Stop returns. Structural cleanup continues in the
// background if a handler refuses to exit.
func (a *Base) waitFullyStopped() {
	select {
	case <-a.fullyStopped:
	case <-time.After(forcedShutdownGrace):
	}
}

func (a *Base) closeJobs() {
	a.jobsOnce.Do(func() {
		if a.jobs != nil {
			close(a.jobs)
		}
	})
}

func (a *Base) handleMessage(runCtx, handlerCtx context.Context, msg jetstream.Msg, heartbeatDone chan struct{}) {
	start := time.Now()
	metrics := a.metrics
	name := a.spec.Name
	defer closeHeartbeat(heartbeatDone)

	header := cloneHeader(msg.Headers())
	md, err := msg.Metadata()
	if err != nil {
		metrics.in.WithLabelValues(name).Inc()
		metrics.errors.WithLabelValues(name).Inc()
		metrics.handle.WithLabelValues(name, OutcomeRetry).Observe(time.Since(start).Seconds())
		a.log.Warn("message without metadata", "err", err)
		if a.deliverDLQ(handlerCtx, msg.Subject(), msg.Headers().Get(bus.HeaderMsgID), msg.Data(), nil, fmt.Errorf("metadata: %w", err)) {
			_ = msg.Term()
			a.terminal.Add(1)
			metrics.terminal.WithLabelValues(name).Inc()
		} else {
			_ = msg.NakWithDelay(a.retryDelay(1))
		}
		return
	}
	metrics.in.WithLabelValues(name).Inc()
	if md.NumDelivered > 1 {
		metrics.redelivery.WithLabelValues(name).Inc()
	}

	in := &Message{
		Subject:        msg.Subject(),
		Data:           msg.Data(),
		Header:         header,
		Timestamp:      md.Timestamp,
		StreamSequence: md.Sequence.Stream,
		NumDelivered:   md.NumDelivered,
	}
	handlerCtx, span := startMessageSpan(handlerCtx, name, msg.Subject(), in.ID(), in.StreamSequence, header)
	handlerCtx = WithArticleID(handlerCtx, in.ArticleID())
	out := &Emitter{agent: a, ctx: handlerCtx}

	a.trackInflight(in.StreamSequence, in.Timestamp)
	defer func() {
		a.endInflight(in.StreamSequence)
		span.End()
	}()

	attemptStart := start
	finish := func(outcomeLabel string) {
		metrics.handle.WithLabelValues(name, outcomeLabel).Observe(time.Since(attemptStart).Seconds())
	}

	if a.spec.Ordered {
		attempt := 0
		for {
			attemptStart = time.Now()
			hErr := a.invoke(handlerCtx, in, out)
			outcomeKind, cause := classify(hErr)
			if hErr == nil {
				finish(OutcomeOK)
				if err := msg.Ack(); err != nil {
					metrics.errors.WithLabelValues(name).Inc()
					failSpan(span, err)
					a.log.Warn("ack failed", "subject", in.Subject, "msg_id", in.ID(), "err", err)
				}
				return
			}
			if outcomeKind == outcomeTerminal {
				finish(OutcomeTerminal)
				metrics.errors.WithLabelValues(name).Inc()
				failSpan(span, cause)
				if a.terminateWithDLQ(handlerCtx, msg, in, cause) {
					return
				}
				attempt++
				_ = msg.NakWithDelay(a.retryDelay(uint64(attempt)))
				return
			}
			attempt++
			metrics.errors.WithLabelValues(name).Inc()
			finish(OutcomeRetry)
			failSpan(span, cause)
			a.log.Warn("ordered handler retrying", "subject", in.Subject, "msg_id", in.ID(), "attempt", attempt, "err", cause)
			select {
			case <-runCtx.Done():
				return
			case <-time.After(a.retryDelay(uint64(attempt))):
			}
		}
	}

	hErr := a.invoke(handlerCtx, in, out)
	outcomeKind, cause := classify(hErr)
	if hErr == nil {
		finish(OutcomeOK)
		if err := msg.Ack(); err != nil {
			metrics.errors.WithLabelValues(name).Inc()
			failSpan(span, err)
			a.log.Warn("ack failed", "subject", in.Subject, "msg_id", in.ID(), "err", err)
		}
		return
	}
	metrics.errors.WithLabelValues(name).Inc()
	failSpan(span, cause)
	if outcomeKind == outcomeTerminal {
		finish(OutcomeTerminal)
		if a.terminateWithDLQ(handlerCtx, msg, in, cause) {
			return
		}
		_ = msg.NakWithDelay(a.retryDelay(md.NumDelivered))
		return
	}
	finish(OutcomeRetry)
	a.log.Warn("handler retrying", "subject", in.Subject, "msg_id", in.ID(), "attempt", md.NumDelivered, "err", cause)
	_ = msg.NakWithDelay(a.retryDelay(md.NumDelivered))
}

func (a *Base) invoke(ctx context.Context, msg *Message, out *Emitter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return a.spec.Handle(ctx, msg, out)
}

func (a *Base) terminateWithDLQ(ctx context.Context, msg jetstream.Msg, in *Message, cause error) bool {
	if !a.deliverDLQ(ctx, in.Subject, in.ID(), in.Data, in.Header, cause) {
		return false
	}
	if err := msg.Term(); err != nil {
		a.metrics.errors.WithLabelValues(a.spec.Name).Inc()
		a.log.Warn("term failed", "subject", in.Subject, "msg_id", in.ID(), "err", err)
		return false
	}
	a.terminal.Add(1)
	a.metrics.terminal.WithLabelValues(a.spec.Name).Inc()
	a.log.Warn("message terminated", "subject", in.Subject, "msg_id", in.ID(), "err", cause)
	return true
}

// deliverDLQ copies the original message to dlq.<agent>. The deterministic id
// makes repeated DLQ attempts idempotent. It returns true only after a PubAck.
func (a *Base) deliverDLQ(parent context.Context, originalSubject, msgID string, data []byte, original nats.Header, cause error) bool {
	subject := bus.DLQSubject(a.spec.Name)
	headers := nats.Header{}
	headers.Set(bus.HeaderOriginalSubject, originalSubject)
	headers.Set(bus.HeaderOriginalMsgID, msgID)
	headers.Set(bus.HeaderError, cause.Error())
	if original != nil {
		if article := original.Get(bus.HeaderArticleID); article != "" {
			headers.Set(bus.HeaderArticleID, article)
		}
	}
	dlqID := fmt.Sprintf("dlq:%s:%s", a.spec.Name, msgID)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if _, err := a.bus.Publish(ctx, subject, data, bus.WithMsgID(dlqID), bus.WithHeaders(headers)); err != nil {
		a.metrics.errors.WithLabelValues(a.spec.Name).Inc()
		a.log.Warn("DLQ write failed", "err", err)
		return false
	}
	return true
}

func (a *Base) retryDelay(numDelivered uint64) time.Duration {
	if numDelivered < 1 {
		numDelivered = 1
	}
	delay := a.spec.RetryBase
	for i := uint64(1); i < numDelivered; i++ {
		delay *= 2
		if delay >= a.spec.RetryMax || delay <= 0 {
			return a.spec.RetryMax
		}
	}
	if delay > a.spec.RetryMax {
		return a.spec.RetryMax
	}
	return delay
}

func (a *Base) startHeartbeat(msg jetstream.Msg) chan struct{} {
	done := make(chan struct{})
	interval := a.spec.AckWait / 8
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					return
				}
			}
		}
	}()
	return done
}

func closeHeartbeat(done chan struct{}) {
	if done != nil {
		close(done)
	}
}

func (a *Base) trackInflight(_ uint64, _ time.Time) {
	count := a.inflightCount.Add(1)
	a.metrics.inflight.WithLabelValues(a.spec.Name).Set(float64(count))
}

func (a *Base) endInflight(_ uint64) {
	count := a.inflightCount.Add(-1)
	a.metrics.inflight.WithLabelValues(a.spec.Name).Set(float64(count))
}

func (a *Base) monitor(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	a.updateStats(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.updateStats(ctx)
		}
	}
}

func (a *Base) updateStats(ctx context.Context) {
	a.mu.Lock()
	consumer := a.consumer
	a.mu.Unlock()
	if a.spec.Producer || consumer == nil {
		a.metrics.pending.WithLabelValues(a.spec.Name).Set(0)
		a.metrics.ackPending.WithLabelValues(a.spec.Name).Set(0)
		a.metrics.queueLag.WithLabelValues(a.spec.Name).Set(0)
		return
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		// Keep the previous value: an info failure is not evidence that the
		// queue drained. The next successful poll corrects it.
		a.log.Debug("queue stats: consumer info failed", "err", err)
		return
	}
	a.metrics.pending.WithLabelValues(a.spec.Name).Set(float64(info.NumPending))
	a.metrics.ackPending.WithLabelValues(a.spec.Name).Set(float64(info.NumAckPending))

	if info.NumPending == 0 && info.NumAckPending == 0 {
		a.metrics.queueLag.WithLabelValues(a.spec.Name).Set(0)
		return
	}

	oldest, ok := a.oldestUnackedTime(ctx, info)
	if !ok {
		// Do not turn an unknown age into zero; that would under-report. The
		// next poll retries the server-authoritative lookup.
		return
	}
	lag := time.Since(oldest)
	if lag < 0 {
		lag = 0
	}
	a.metrics.queueLag.WithLabelValues(a.spec.Name).Set(lag.Seconds())
}

// oldestUnackedTime asks the server for the first message after the consumer's
// ack floor for every filter subject. The minimum publish time is the age of
// the oldest unacked, undelivered, retry-waiting, or crashed-process message.
// This is deliberately server-authoritative: no local delivery clock can drift
// into a continuous busy period or race a stale snapshot.
func (a *Base) oldestUnackedTime(ctx context.Context, info *jetstream.ConsumerInfo) (time.Time, bool) {
	filters := consumerFilters(info.Config)
	if len(filters) == 0 || info.Stream == "" {
		return time.Time{}, false
	}
	stream, err := a.bus.StreamNamed(ctx, info.Stream)
	if err != nil {
		a.log.Debug("queue stats: stream lookup failed", "stream", info.Stream, "err", err)
		return time.Time{}, false
	}
	start := info.AckFloor.Stream + 1
	var oldest time.Time
	for _, filter := range filters {
		msg, err := stream.GetMsg(ctx, start, jetstream.WithGetMsgSubject(filter))
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			a.log.Debug("queue stats: next message lookup failed", "filter", filter, "err", err)
			continue
		}
		if oldest.IsZero() || msg.Time.Before(oldest) {
			oldest = msg.Time
		}
	}
	return oldest, !oldest.IsZero()
}

func consumerFilters(cfg jetstream.ConsumerConfig) []string {
	seen := make(map[string]bool, len(cfg.FilterSubjects)+1)
	out := make([]string, 0, len(cfg.FilterSubjects)+1)
	add := func(filter string) {
		if filter == "" || seen[filter] {
			return
		}
		seen[filter] = true
		out = append(out, filter)
	}
	add(cfg.FilterSubject)
	for _, filter := range cfg.FilterSubjects {
		add(filter)
	}
	return out
}

func (a *Base) signalReady(err error) {
	select {
	case a.ready <- err:
	default:
	}
}

func (a *Base) setError(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	a.lastErr = err
	a.mu.Unlock()
}

func cloneHeader(h nats.Header) nats.Header {
	if h == nil {
		return nats.Header{}
	}
	out := make(nats.Header, len(h))
	for k, vs := range h {
		out[k] = append([]string(nil), vs...)
	}
	return out
}
