package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

// processBlocked is the process-wide writer fence set by Runtime. Publishers
// check it before producing DELTA so a maintenance run cannot race an old
// publisher that has not been stopped yet.
var processBlocked atomic.Bool

// currentPolicy is the durable per-article policy visible to normalize,
// cluster and publisher ingress in this process. Runtime refreshes it from the
// publisher bucket; writes without a loaded policy pass through unchanged.
var currentPolicy atomic.Pointer[WireTimePolicy]

// ProcessBlocked reports the process-wide maintenance writer fence.
func ProcessBlocked() bool { return processBlocked.Load() }

// SetProcessBlocked is exported for tests/embedders that do not run a Runtime.
func SetProcessBlocked(blocked bool) { processBlocked.Store(blocked) }

// SetCurrentPolicy publishes the durable policy to process-local ingress.
func SetCurrentPolicy(p *WireTimePolicy) { currentPolicy.Store(p) }

// ApplyCurrentPolicy applies the loaded policy to one article timestamp.
func ApplyCurrentPolicy(articleID string, at int64) int64 {
	if p := currentPolicy.Load(); p != nil {
		return ApplyPolicy(p, articleID, at)
	}
	return at
}

// Runtime is the in-process maintenance participant. It watches the durable
// control record, pauses the shared raw producer boundary and blocks new DB
// writer admissions, then advertises its pause/in-flight/writer state so the
// coordinator can prove the fleet is drained instead of inferring it from an
// empty stream.
type Runtime struct {
	ID      string
	Bus     *bus.Bus
	control jetstream.KeyValue
	leases  jetstream.KeyValue
	log     *slog.Logger

	writerProbe   func() WriterCounters
	writerBlocked func(bool)

	mu       sync.Mutex
	last     Control
	barrier  bus.IngestBarrier
	writer   WriterCounters
	healthy  bool
	lastErr  string
	lastAt   int64
	policyAt time.Time
}

// RuntimeState is the admin/health view of one participant.
type RuntimeState struct {
	ID                 string         `json:"id"`
	Phase              string         `json:"phase"`
	Epoch              uint64         `json:"epoch"`
	Blocked            bool           `json:"blocked"`
	Healthy            bool           `json:"healthy"`
	IngressClosed      bool           `json:"ingress_closed"`
	DownstreamClosed   bool           `json:"downstream_closed"`
	IngressInflight    int64          `json:"ingress_inflight"`
	DownstreamInflight int64          `json:"downstream_inflight"`
	First              uint64         `json:"ingest_first"`
	Last               uint64         `json:"ingest_last"`
	Writer             WriterCounters `json:"writer"`
	Error              string         `json:"error,omitempty"`
	At                 int64          `json:"at"`
}

// NewRuntime binds the publisher control and producer-lease buckets.
func NewRuntime(ctx context.Context, b *bus.Bus, id string, log *slog.Logger) (*Runtime, error) {
	if b == nil || id == "" {
		return nil, errors.New("maintenance: runtime requires a bus and id")
	}
	controlKV, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return nil, err
	}
	leases, err := b.KV(ctx, bus.KVBucketProducerLease)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	control, _, cerr := ReadControl(ctx, controlKV)
	if cerr != nil {
		b.PauseIngress()
		b.PauseDownstream()
		SetProcessBlocked(true)
	} else {
		if control.IngressShut() {
			b.PauseIngress()
		}
		if control.DownstreamShut() {
			b.PauseDownstream()
			SetProcessBlocked(true)
		}
	}
	if policy, perr := LoadPolicy(ctx, controlKV); perr == nil {
		SetCurrentPolicy(policy)
	}
	return &Runtime{ID: id, Bus: b, control: controlKV, leases: leases, log: log}, nil
}

// SetWriter wires the in-process DB writer barrier and admission fence.
func (r *Runtime) SetWriter(probe func() WriterCounters, blocked func(bool)) {
	r.mu.Lock()
	r.writerProbe = probe
	r.writerBlocked = blocked
	r.mu.Unlock()
}

// Start watches the control record until ctx is cancelled. It pauses producers
// immediately when the control is unreadable or fenced.
func (r *Runtime) Start(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.sample(ctx)
		select {
		case <-ctx.Done():
			r.shutdown()
			return
		case <-ticker.C:
		}
	}
}

func (r *Runtime) sample(ctx context.Context) {
	policyErr := r.refreshPolicy(ctx)
	control, _, ctrlErr := ReadControl(ctx, r.control)
	healthy := ctrlErr == nil && policyErr == nil
	ingressClosed := true
	downstreamClosed := true
	writerClosed := true
	phase := "control_unavailable"
	epoch := uint64(0)
	if healthy {
		ingressClosed = control.IngressShut()
		downstreamClosed = control.DownstreamShut()
		writerClosed = control.WriterShut()
		phase = control.Phase
		epoch = control.FleetEpoch
	}
	if ingressClosed {
		r.Bus.PauseIngress()
	} else {
		r.Bus.ResumeIngress()
	}
	if downstreamClosed {
		r.Bus.PauseDownstream()
	} else {
		r.Bus.ResumeDownstream()
	}
	SetProcessBlocked(downstreamClosed)
	r.mu.Lock()
	admission := r.writerBlocked
	probe := r.writerProbe
	r.mu.Unlock()
	if admission != nil {
		admission(writerClosed)
	}
	barrier, berr := r.Bus.IngestBarrier(ctx)
	if berr != nil {
		healthy = false
	}
	writer := WriterCounters{}
	if probe != nil {
		writer = probe()
	} else {
		healthy = false
	}
	errText := ""
	if ctrlErr != nil {
		errText = ctrlErr.Error()
	} else if policyErr != nil {
		errText = "policy: " + policyErr.Error()
	} else if berr != nil {
		errText = berr.Error()
	}
	r.mu.Lock()
	r.last = Control{Phase: phase, FleetEpoch: epoch, FenceActive: ingressClosed || downstreamClosed}
	r.barrier = barrier
	r.writer = writer
	r.healthy = healthy
	r.lastErr = errText
	r.lastAt = time.Now().UnixNano()
	lease := ProducerLease{
		Version:            ControlVersion,
		ID:                 r.ID,
		Epoch:              epoch,
		Healthy:            healthy,
		Error:              errText,
		Paused:             downstreamClosed,
		IngressClosed:      ingressClosed,
		DownstreamClosed:   downstreamClosed,
		IngestInflight:     barrier.IngressInflight,
		DownstreamInflight: barrier.DownstreamInflight,
		IngestFirst:        barrier.First,
		IngestLast:         barrier.Last,
		Writer:             writer,
	}
	r.mu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if werr := WriteProducerLease(writeCtx, r.leases, lease); werr != nil {
		r.log.Warn("maintenance runtime lease write failed", "id", r.ID, "err", werr)
		_ = DeleteProducerLease(writeCtx, r.leases, r.ID)
	}
}

func (r *Runtime) refreshPolicy(ctx context.Context) error {
	r.mu.Lock()
	due := time.Since(r.policyAt) >= 2*time.Second
	r.mu.Unlock()
	if !due {
		return nil
	}
	policy, err := LoadPolicy(ctx, r.control)
	if err != nil {
		return err
	}
	SetCurrentPolicy(policy)
	r.mu.Lock()
	r.policyAt = time.Now()
	r.mu.Unlock()
	return nil
}

func (r *Runtime) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	control, _, err := ReadControl(ctx, r.control)
	healthy := err == nil
	ingressClosed := true
	downstreamClosed := true
	writerClosed := true
	epoch := r.last.FleetEpoch
	if healthy {
		ingressClosed = control.IngressShut()
		downstreamClosed = control.DownstreamShut()
		writerClosed = control.WriterShut()
		epoch = control.FleetEpoch
	}
	if ingressClosed {
		r.Bus.PauseIngress()
	}
	if downstreamClosed {
		r.Bus.PauseDownstream()
	}
	SetProcessBlocked(downstreamClosed)
	r.mu.Lock()
	admission := r.writerBlocked
	r.mu.Unlock()
	if admission != nil {
		admission(writerClosed)
	}
	r.mu.Lock()
	lease := ProducerLease{
		Version:            ControlVersion,
		ID:                 r.ID,
		Epoch:              epoch,
		Healthy:            healthy,
		Paused:             downstreamClosed,
		IngressClosed:      ingressClosed,
		DownstreamClosed:   downstreamClosed,
		IngestInflight:     r.barrier.IngressInflight,
		DownstreamInflight: r.barrier.DownstreamInflight,
		IngestFirst:        r.barrier.First,
		IngestLast:         r.barrier.Last,
		Writer:             r.writer,
	}
	if !healthy {
		lease.Error = "control unavailable during shutdown"
	}
	r.mu.Unlock()
	_ = WriteProducerLease(ctx, r.leases, lease)
}

// State returns the last observed participant state.
func (r *Runtime) State() RuntimeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RuntimeState{
		ID:                 r.ID,
		Phase:              r.last.Phase,
		Epoch:              r.last.FleetEpoch,
		Blocked:            r.last.FenceActive,
		Healthy:            r.healthy,
		IngressClosed:      r.barrier.IngressClosed,
		DownstreamClosed:   r.barrier.DownstreamClosed,
		IngressInflight:    r.barrier.IngressInflight,
		DownstreamInflight: r.barrier.DownstreamInflight,
		First:              r.barrier.First,
		Last:               r.barrier.Last,
		Writer:             r.writer,
		Error:              r.lastErr,
		At:                 r.lastAt,
	}
}
