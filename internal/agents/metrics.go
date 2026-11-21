package agents

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	// OutcomeOK, OutcomeRetry and OutcomeTerminal are the handler outcome metric
	// labels. Low cardinality by contract.
	OutcomeOK       = "ok"
	OutcomeRetry    = "retry"
	OutcomeTerminal = "terminal"
)

// Metrics owns every dor agent metric and the registry exposed on /metrics.
type Metrics struct {
	registry *prometheus.Registry

	in                  *prometheus.CounterVec
	out                 *prometheus.CounterVec
	errors              *prometheus.CounterVec
	terminal            *prometheus.CounterVec
	redelivery          *prometheus.CounterVec
	handle              *prometheus.HistogramVec
	queueLag            *prometheus.GaugeVec
	pending             *prometheus.GaugeVec
	ackPending          *prometheus.GaugeVec
	inflight            *prometheus.GaugeVec
	publicationDuration *prometheus.HistogramVec
	publicationWritten  *prometheus.CounterVec
	publicationStorage  prometheus.Gauge
}

// NewMetrics builds a registry with the process and Go collectors plus the
// dor_agent_* family.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		publicationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "dor_publisher_storage_seconds", Help: "Publisher journal, base, lease and cache encode/write duration.", Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, []string{"operation"}),
		publicationWritten:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dor_publisher_storage_bytes_total", Help: "Encoded publisher storage bytes by operation."}, []string{"operation"}),
		publicationStorage:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "dor_publisher_storage_bytes", Help: "Current publisher KV physical message bytes; backpressure begins before the bucket limit."}),
		registry:            reg,
		in: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_agent_in_total",
			Help: "Messages delivered to an agent, including redeliveries.",
		}, []string{"agent"}),
		out: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_agent_out_total",
			Help: "Messages published by an agent.",
		}, []string{"agent"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_agent_errors_total",
			Help: "Handler, publish, ack and DLQ errors per agent.",
		}, []string{"agent"}),
		terminal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_agent_terminal_total",
			Help: "Messages classified terminal, written to the DLQ and terminated.",
		}, []string{"agent"}),
		redelivery: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_agent_redelivered_total",
			Help: "Messages redelivered by JetStream (NumDelivered > 1).",
		}, []string{"agent"}),
		handle: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "dor_agent_handle_seconds",
			Help: "Duration of every completed handler attempt. Use histogram_quantile(0.95, ...) for p95.",
			Buckets: []float64{
				0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
				10, 30, 60, 120, 300,
			},
		}, []string{"agent", "outcome"}),
		queueLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_agent_queue_lag_seconds",
			Help: "Age since publish of the oldest unacked message matching the consumer's filters (undelivered, in-flight or waiting to retry). Zero when idle.",
		}, []string{"agent"}),
		pending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_agent_pending",
			Help: "Consumer num_pending: messages matching the filter not yet delivered.",
		}, []string{"agent"}),
		ackPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_agent_ack_pending",
			Help: "Consumer num_ack_pending: delivered but not yet acknowledged.",
		}, []string{"agent"}),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dor_agent_inflight",
			Help: "Handlers currently running in this process.",
		}, []string{"agent"}),
	}
	reg.MustRegister(m.in, m.out, m.errors, m.terminal, m.redelivery, m.handle,
		m.queueLag, m.pending, m.ackPending, m.inflight, m.publicationDuration, m.publicationWritten, m.publicationStorage)
	return m
}

// Registry returns the Prometheus registry for /metrics.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// RegisterAgent creates the zero-valued series for an agent so its metrics are
// present on /metrics before the first message arrives.
func (m *Metrics) RegisterAgent(name string) {
	m.in.WithLabelValues(name)
	m.out.WithLabelValues(name)
	m.errors.WithLabelValues(name)
	m.terminal.WithLabelValues(name)
	m.redelivery.WithLabelValues(name)
	m.handle.WithLabelValues(name, OutcomeOK)
	m.handle.WithLabelValues(name, OutcomeRetry)
	m.handle.WithLabelValues(name, OutcomeTerminal)
	m.queueLag.WithLabelValues(name).Set(0)
	m.pending.WithLabelValues(name).Set(0)
	m.ackPending.WithLabelValues(name).Set(0)
	m.inflight.WithLabelValues(name).Set(0)
}
