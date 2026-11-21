package fetch

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the per-source fetch metrics. Source is the reviewed source id
// from deploy/sources/sources.yaml (~300 values), so the cardinality is
// bounded by the seed list, not by URLs or feed items. That is deliberate: the
// ticket asks for per-source health, and 300 sources times six series is a few
// thousand samples, which is cheap for Prometheus and lets an operator answer
// "which feed is broken" without a join.
type Metrics struct {
	state       *prometheus.GaugeVec
	lastSuccess *prometheus.GaugeVec
	items       *prometheus.CounterVec
	itemsHour   *prometheus.GaugeVec
	interval    *prometheus.GaugeVec
	errors      *prometheus.CounterVec
	fetches     *prometheus.CounterVec
	errorRate   *prometheus.GaugeVec

	recordFrames  prometheus.Counter
	recordBytes   prometheus.Counter
	recordDropped prometheus.Counter
	recordErrors  prometheus.Counter
	recordQueued  prometheus.Gauge
}

// NewMetrics registers the fetch metric family on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		state: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_source_state",
			Help: "Per-source fetch state: 0 ok, 1 backoff, 2 quarantined.",
		}, []string{"source"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_source_last_success_timestamp_seconds",
			Help: "Unix time of the last successful fetch that produced items (or a valid empty feed).",
		}, []string{"source"}),
		items: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_source_items_total",
			Help: "Raw items published by a source since process start.",
		}, []string{"source"}),
		itemsHour: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_source_items_hour",
			Help: "Raw items published by a source in the current wall-clock hour.",
		}, []string{"source"}),
		interval: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_source_interval_seconds",
			Help: "Current adaptive polling interval per source (bounded by interval_s and 5 minutes).",
		}, []string{"source"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_source_errors_total",
			Help: "Failed fetch attempts per source (network, HTTP, parse).",
		}, []string{"source"}),
		fetches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_source_fetches_total",
			Help: "Fetch attempts per source, including 304s and failures.",
		}, []string{"source"}),
		errorRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_source_error_rate",
			Help: "Failed attempts divided by attempts in the current wall-clock hour, 0..1.",
		}, []string{"source"}),
		recordFrames: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_record_frames_total",
			Help: "Raw messages written to record-mode files.",
		}),
		recordBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_record_bytes_total",
			Help: "Uncompressed frame bytes written to record-mode files.",
		}),
		recordDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_record_dropped_total",
			Help: "Raw messages dropped because the record queue was full.",
		}),
		recordErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_record_write_errors_total",
			Help: "Raw messages lost to a record-file disk error.",
		}),
		recordQueued: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dor_record_queue_depth",
			Help: "Raw messages waiting for the record writer.",
		}),
	}
	reg.MustRegister(m.state, m.lastSuccess, m.items, m.itemsHour, m.interval, m.errors,
		m.fetches, m.errorRate, m.recordFrames, m.recordBytes, m.recordDropped,
		m.recordErrors, m.recordQueued)
	return m
}

// ensureSource creates the zero series so /metrics shows an idle source too.
func (m *Metrics) ensureSource(source string) {
	m.state.WithLabelValues(source)
	m.lastSuccess.WithLabelValues(source)
	m.items.WithLabelValues(source)
	m.itemsHour.WithLabelValues(source)
	m.interval.WithLabelValues(source)
	m.errors.WithLabelValues(source)
	m.fetches.WithLabelValues(source)
	m.errorRate.WithLabelValues(source)
}

func (m *Metrics) setState(source string, state State) {
	m.state.WithLabelValues(source).Set(float64(state))
}

func (m *Metrics) observeFetch(source string) {
	m.fetches.WithLabelValues(source).Inc()
}

func (m *Metrics) observeItems(source string, n int) {
	if n <= 0 {
		return
	}
	m.items.WithLabelValues(source).Add(float64(n))
}

func (m *Metrics) setInterval(source string, interval time.Duration) {
	if interval > 0 {
		m.interval.WithLabelValues(source).Set(interval.Seconds())
	}
}

func (m *Metrics) observeError(source string) {
	m.errors.WithLabelValues(source).Inc()
}

// RecordWritten implements record.Metrics.
func (m *Metrics) RecordWritten(bytes int) {
	m.recordFrames.Inc()
	m.recordBytes.Add(float64(bytes))
}

// RecordQueueDrop implements record.Metrics (queue full).
func (m *Metrics) RecordQueueDrop() { m.recordDropped.Inc() }

// RecordWriteError implements record.Metrics (disk error).
func (m *Metrics) RecordWriteError() { m.recordErrors.Inc() }

// RecordQueueDepth implements record.Metrics.
func (m *Metrics) RecordQueueDepth(n int) { m.recordQueued.Set(float64(n)) }
