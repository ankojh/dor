package fetch

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// State is the fetch state of one source.
type State int

// Source states, ordered by severity for metrics.
const (
	StateOK State = iota
	StateBackoff
	StateQuarantined
)

func (s State) String() string {
	switch s {
	case StateOK:
		return "ok"
	case StateBackoff:
		return "backoff"
	case StateQuarantined:
		return "quarantined"
	default:
		return "unknown"
	}
}

// KVBucketSourceHealth is the KV bucket holding one JSON HealthRecord per
// source. It is provisioned with the other buckets by the bus owner.
// (The name lives in the bus package with the rest of the bucket names.)

// HealthRecord is the KV value for one source and the shape M1·2 copies into
// Postgres `sources.health_json`.
type HealthRecord struct {
	SourceID            string  `json:"source_id"`
	Kind                string  `json:"kind"`
	State               string  `json:"state"`
	LastAttempt         int64   `json:"last_attempt_unix"`
	LastSuccess         int64   `json:"last_success_unix"`
	LastError           string  `json:"last_error,omitempty"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	ParseFailures       int     `json:"parse_failures"`
	ItemsTotal          int64   `json:"items_total"`
	ItemsHour           int64   `json:"items_hour"`
	IntervalSeconds     int64   `json:"interval_seconds,omitempty"`
	FetchesHour         int64   `json:"fetches_hour"`
	ErrorsHour          int64   `json:"errors_hour"`
	ErrorRate           float64 `json:"error_rate"`
	ETag                string  `json:"etag,omitempty"`
	LastModified        string  `json:"last_modified,omitempty"`
	LastFile            string  `json:"last_file,omitempty"`
	NextAttempt         int64   `json:"next_attempt_unix,omitempty"`
	QuarantinedUntil    int64   `json:"quarantined_until_unix,omitempty"`
	UpdatedAt           int64   `json:"updated_at_unix"`
}

// Update is one fetch attempt's outcome.
type Update struct {
	SourceID            string
	Kind                string
	State               State
	Success             bool
	Items               int
	Interval            time.Duration
	Err                 string
	ETag                string
	LastModified        string
	LastFile            string
	NextAttempt         time.Time
	QuarantinedUntil    time.Time
	ConsecutiveFailures int
	ParseFailures       int
}

type hourCounters struct {
	hour    int64
	items   int64
	fetches int64
	errors  int64
}

// HealthStore keeps per-source health in memory for the current hour and
// mirrors it to KV and Prometheus. A KV write failure is logged and ignored:
// health is observability, not a correctness path.
type HealthStore struct {
	kv      jetstream.KeyValue
	metrics *Metrics
	log     *slog.Logger
	now     func() time.Time

	mu      sync.Mutex
	counts  map[string]*hourCounters
	total   map[string]int64
	success map[string]int64
}

// NewHealthStore builds a store. kv may be nil (tests, or a degraded start);
// KV persistence is then skipped.
func NewHealthStore(kv jetstream.KeyValue, metrics *Metrics, log *slog.Logger) *HealthStore {
	if log == nil {
		log = slog.Default()
	}
	return &HealthStore{
		kv:      kv,
		metrics: metrics,
		log:     log,
		now:     time.Now,
		counts:  make(map[string]*hourCounters),
		total:   make(map[string]int64),
		success: make(map[string]int64),
	}
}

// Load reads the last persisted record for sourceID.
func (h *HealthStore) Load(ctx context.Context, sourceID string) (HealthRecord, bool) {
	if h.kv == nil {
		return HealthRecord{}, false
	}
	entry, err := h.kv.Get(ctx, sourceID)
	if err != nil {
		return HealthRecord{}, false
	}
	var rec HealthRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return HealthRecord{}, false
	}
	return rec, true
}

// Restore seeds the in-memory counters and metrics from a persisted record, so
// a restart does not reset last-success, item totals or the current-hour error
// rate. Call it once per source after Load and before the first Record.
func (h *HealthStore) Restore(rec HealthRecord) {
	if rec.SourceID == "" {
		return
	}
	now := h.now().UTC()
	h.mu.Lock()
	if rec.ItemsTotal > 0 {
		h.total[rec.SourceID] = rec.ItemsTotal
	}
	if rec.LastSuccess > 0 {
		h.success[rec.SourceID] = rec.LastSuccess
	}
	// Current-hour counters are only restored when the record is from this
	// hour; an older record's counters belong to an hour that has passed.
	if rec.UpdatedAt > 0 && time.Unix(rec.UpdatedAt, 0).UTC().Truncate(time.Hour) == now.Truncate(time.Hour) {
		h.counts[rec.SourceID] = &hourCounters{
			hour:    now.Truncate(time.Hour).Unix(),
			items:   rec.ItemsHour,
			fetches: rec.FetchesHour,
			errors:  rec.ErrorsHour,
		}
	}
	h.mu.Unlock()

	if h.metrics == nil {
		return
	}
	h.metrics.ensureSource(rec.SourceID)
	h.metrics.setState(rec.SourceID, parseState(rec.State))
	if rec.LastSuccess > 0 {
		h.metrics.lastSuccess.WithLabelValues(rec.SourceID).Set(float64(rec.LastSuccess))
	}
	if rec.ItemsTotal > 0 {
		h.metrics.items.WithLabelValues(rec.SourceID).Add(float64(rec.ItemsTotal))
	}
	h.metrics.itemsHour.WithLabelValues(rec.SourceID).Set(float64(rec.ItemsHour))
	h.metrics.errorRate.WithLabelValues(rec.SourceID).Set(rec.ErrorRate)
}

func parseState(s string) State {
	switch s {
	case "ok":
		return StateOK
	case "backoff":
		return StateBackoff
	case "quarantined":
		return StateQuarantined
	default:
		return StateOK
	}
}

// Record stores one attempt, updating metrics and KV, and returns the record
// that was written.
func (h *HealthStore) Record(ctx context.Context, u Update) HealthRecord {
	now := h.now().UTC()
	h.mu.Lock()
	c := h.counts[u.SourceID]
	if c == nil {
		c = &hourCounters{hour: now.Truncate(time.Hour).Unix()}
		h.counts[u.SourceID] = c
	}
	if hour := now.Truncate(time.Hour).Unix(); c.hour != hour {
		*c = hourCounters{hour: hour}
	}
	c.fetches++
	if u.Success {
		c.items += int64(u.Items)
		h.total[u.SourceID] += int64(u.Items)
	} else {
		c.errors++
	}
	itemsHour := c.items
	fetchesHour := c.fetches
	errorsHour := c.errors
	itemsTotal := h.total[u.SourceID]
	lastSuccess := h.success[u.SourceID]
	if u.Success {
		lastSuccess = now.Unix()
		h.success[u.SourceID] = lastSuccess
	}
	h.mu.Unlock()

	rec := HealthRecord{
		SourceID:            u.SourceID,
		Kind:                u.Kind,
		State:               u.State.String(),
		LastAttempt:         now.Unix(),
		LastSuccess:         lastSuccess,
		LastError:           u.Err,
		ConsecutiveFailures: u.ConsecutiveFailures,
		ParseFailures:       u.ParseFailures,
		ItemsTotal:          itemsTotal,
		ItemsHour:           itemsHour,
		IntervalSeconds:     int64(u.Interval / time.Second),
		FetchesHour:         fetchesHour,
		ErrorsHour:          errorsHour,
		ETag:                u.ETag,
		LastModified:        u.LastModified,
		LastFile:            u.LastFile,
		UpdatedAt:           now.Unix(),
	}
	if fetchesHour > 0 {
		rec.ErrorRate = float64(errorsHour) / float64(fetchesHour)
	}
	if !u.NextAttempt.IsZero() {
		rec.NextAttempt = u.NextAttempt.Unix()
	}
	if !u.QuarantinedUntil.IsZero() {
		rec.QuarantinedUntil = u.QuarantinedUntil.Unix()
	}

	if h.metrics != nil {
		h.metrics.ensureSource(u.SourceID)
		h.metrics.observeFetch(u.SourceID)
		h.metrics.setState(u.SourceID, u.State)
		h.metrics.itemsHour.WithLabelValues(u.SourceID).Set(float64(itemsHour))
		h.metrics.setInterval(u.SourceID, u.Interval)
		h.metrics.errorRate.WithLabelValues(u.SourceID).Set(rec.ErrorRate)
		if u.Success {
			h.metrics.observeItems(u.SourceID, u.Items)
			h.metrics.lastSuccess.WithLabelValues(u.SourceID).Set(float64(now.Unix()))
		} else {
			h.metrics.observeError(u.SourceID)
		}
	}

	if h.kv != nil {
		if payload, err := json.Marshal(rec); err == nil {
			writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if _, err := h.kv.Put(writeCtx, u.SourceID, payload); err != nil {
				h.log.Debug("source health KV write failed", "source", u.SourceID, "err", err)
			}
		}
	}
	return rec
}
