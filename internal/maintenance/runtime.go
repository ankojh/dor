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

// ProcessBlocked reports the process-wide maintenance writer fence.
func ProcessBlocked() bool { return processBlocked.Load() }

// SetProcessBlocked is exported for tests/embedders that do not run a Runtime.
func SetProcessBlocked(blocked bool) { processBlocked.Store(blocked) }

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

	mu      sync.Mutex
	last    Control
	barrier bus.IngestBarrier
	writer  WriterCounters
	lastErr string
	lastAt  int64
}

// RuntimeState is the admin/health view of one participant.
type RuntimeState struct {
	ID       string         `json:"id"`
	Phase    string         `json:"phase"`
	Epoch    uint64         `json:"epoch"`
	Blocked  bool           `json:"blocked"`
	Paused   bool           `json:"paused"`
	Inflight int64          `json:"ingest_inflight"`
	First    uint64         `json:"ingest_first"`
	Last     uint64         `json:"ingest_last"`
	Writer   WriterCounters `json:"writer"`
	Error    string         `json:"error,omitempty"`
	At       int64          `json:"at"`
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
	if cerr != nil || control.Blocked() {
		b.PauseIngest()
		SetProcessBlocked(true)
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
	control, _, err := ReadControl(ctx, r.control)
	blocked := true
	phase := ""
	epoch := uint64(0)
	if err == nil {
		blocked = control.Blocked()
		phase = control.Phase
		epoch = control.FleetEpoch
	} else {
		phase = "control_unavailable"
	}
	if blocked {
		r.Bus.PauseIngest()
	} else {
		r.Bus.ResumeIngest()
	}
	SetProcessBlocked(blocked)
	r.mu.Lock()
	admission := r.writerBlocked
	probe := r.writerProbe
	r.mu.Unlock()
	if admission != nil {
		admission(blocked)
	}
	barrier, berr := r.Bus.IngestBarrier(ctx)
	if berr != nil {
		err = berr
	}
	writer := WriterCounters{}
	if probe != nil {
		writer = probe()
	}
	r.mu.Lock()
	r.last = Control{Phase: phase, FleetEpoch: epoch, FenceActive: blocked}
	r.barrier = barrier
	r.writer = writer
	if err != nil {
		r.lastErr = err.Error()
	} else {
		r.lastErr = ""
	}
	r.lastAt = time.Now().UnixNano()
	lease := ProducerLease{
		Version:        ControlVersion,
		ID:             r.ID,
		Epoch:          epoch,
		Paused:         blocked,
		IngestInflight: barrier.Inflight,
		IngestFirst:    barrier.First,
		IngestLast:     barrier.Last,
		Writer:         writer,
	}
	r.mu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if werr := WriteProducerLease(writeCtx, r.leases, lease); werr != nil {
		r.log.Warn("maintenance runtime lease write failed", "id", r.ID, "err", werr)
	}
}

func (r *Runtime) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	control, _, err := ReadControl(ctx, r.control)
	blocked := err != nil || control.Blocked()
	if blocked {
		r.Bus.PauseIngest()
	}
	SetProcessBlocked(blocked)
	r.mu.Lock()
	lease := ProducerLease{
		Version:        ControlVersion,
		ID:             r.ID,
		Epoch:          r.last.FleetEpoch,
		Paused:         blocked || r.barrier.Paused,
		IngestInflight: r.barrier.Inflight,
		IngestFirst:    r.barrier.First,
		IngestLast:     r.barrier.Last,
		Writer:         r.writer,
	}
	r.mu.Unlock()
	_ = WriteProducerLease(ctx, r.leases, lease)
}

// State returns the last observed participant state.
func (r *Runtime) State() RuntimeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RuntimeState{
		ID:       r.ID,
		Phase:    r.last.Phase,
		Epoch:    r.last.FleetEpoch,
		Blocked:  r.last.FenceActive,
		Paused:   r.barrier.Paused,
		Inflight: r.barrier.Inflight,
		First:    r.barrier.First,
		Last:     r.barrier.Last,
		Writer:   r.writer,
		Error:    r.lastErr,
		At:       r.lastAt,
	}
}
