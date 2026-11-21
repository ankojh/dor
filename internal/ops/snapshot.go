// Package ops implements the M6 Supervisor and the backend /ops view.
//
// The Supervisor reads the same Prometheus series the agents export, the
// Runner's health surface and the real budget manager. Its actions are
// runtime controls on the agents in this process: an unordered consumer's
// worker limit, an in-process restart, and GDELT backpressure. When a control
// is absent the Supervisor records an explicit advisory; it never logs an
// action it did not perform.
package ops

import (
	"sort"

	dto "github.com/prometheus/client_model/go"
)

// AgentRow is one agent as the ops page shows it. Numeric fields are nil when
// the metric is unavailable, so the page can print an explicit dash instead of
// inventing a zero.
type AgentRow struct {
	Name             string   `json:"name"`
	Status           string   `json:"status"`
	StatusDetail     string   `json:"status_detail,omitempty"`
	Producer         bool     `json:"producer"`
	MetricsAvailable bool     `json:"metrics_available"`
	InPerSec         *float64 `json:"in_per_sec,omitempty"`
	OutPerSec        *float64 `json:"out_per_sec,omitempty"`
	ErrorRate        *float64 `json:"error_rate,omitempty"`
	QueueLagMS       *float64 `json:"queue_lag_ms,omitempty"`
	P95MS            *float64 `json:"p95_ms,omitempty"`
	Pending          *float64 `json:"pending,omitempty"`
	AckPending       *float64 `json:"ack_pending,omitempty"`
	Inflight         *float64 `json:"inflight,omitempty"`
	ErrorsTotal      *float64 `json:"errors_total,omitempty"`
	TerminalTotal    *float64 `json:"terminal_total,omitempty"`
	Redelivered      *float64 `json:"redelivered_total,omitempty"`
	Concurrency      *int     `json:"concurrency,omitempty"`
	MaxConcurrency   *int     `json:"max_concurrency,omitempty"`
	ScaleSupported   bool     `json:"scale_supported"`
}

// BudgetView mirrors budget.Manager's real daily counter. The shed stages are
// enforced by Manager.Check; the Supervisor only reports the current state and
// logs transitions.
type BudgetView struct {
	Available      bool     `json:"available"`
	Error          string   `json:"error,omitempty"`
	Day            string   `json:"day,omitempty"`
	SpendUSD       float64  `json:"spend_usd"`
	CapUSD         float64  `json:"cap_usd"`
	Ratio          float64  `json:"ratio"`
	ShedLevel      int      `json:"shed_level"`
	Shedding       []string `json:"shedding"`
	Calls          int64    `json:"calls"`
	Pending        int      `json:"pending"`
	StaleCollected int64    `json:"stale_collected"`
	BriefAt        float64  `json:"shed_brief_at"`
	WhyAt          float64  `json:"shed_why_at"`
	ThematicAt     float64  `json:"shed_thematic_at"`
}

// Action is one supervisor action or advisory. Advisory actions are controls
// the process does not expose; they are labelled so the log cannot be mistaken
// for a real intervention.
type Action struct {
	At       string `json:"at"`
	Kind     string `json:"kind"`
	Target   string `json:"target,omitempty"`
	Message  string `json:"message"`
	Advisory bool   `json:"advisory"`
	Error    string `json:"error,omitempty"`
}

// Alarm is an active sustained-lag alarm. The plan alarm is lag > 5 s for one
// minute.
type Alarm struct {
	Agent        string  `json:"agent"`
	LagMS        float64 `json:"lag_ms"`
	Since        string  `json:"since"`
	ActiveForSec float64 `json:"active_for_seconds"`
	ThresholdMS  float64 `json:"threshold_ms"`
}

// Thresholds publishes the exact settings the Supervisor used for this process
// so operators can distinguish an alarm from a page-rendered heuristic.
type Thresholds struct {
	SampleMS            int64 `json:"sample_ms"`
	ScaleLagMS          int64 `json:"scale_lag_ms"`
	AlarmLagMS          int64 `json:"alarm_lag_ms"`
	AlarmForMS          int64 `json:"alarm_for_ms"`
	ScaleCooldownMS     int64 `json:"scale_cooldown_ms"`
	BacklogForMS        int64 `json:"backlog_for_ms"`
	RecoveryForMS       int64 `json:"recovery_for_ms"`
	MaxConcurrency      int   `json:"max_concurrency"`
	RestartBackoffMinMS int64 `json:"restart_backoff_min_ms"`
	RestartBackoffMaxMS int64 `json:"restart_backoff_max_ms"`
}

// BackpressureView reports the real GDELT control state.
type BackpressureView struct {
	Supported bool   `json:"supported"`
	Level     int    `json:"level"`
	Label     string `json:"label"`
	Since     string `json:"since,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Snapshot is the Supervisor's current view. Available is always true when a
// supervisor is wired; the HTTP layer distinguishes an absent supervisor.
type Snapshot struct {
	GeneratedAt  string           `json:"generated_at"`
	UptimeSec    float64          `json:"uptime_seconds"`
	Agents       []AgentRow       `json:"agents"`
	Budget       BudgetView       `json:"budget"`
	Actions      []Action         `json:"actions"`
	Alarms       []Alarm          `json:"alarms"`
	Thresholds   Thresholds       `json:"thresholds"`
	GDELT        BackpressureView `json:"gdelt"`
	MetricsError string           `json:"metrics_error,omitempty"`
}

type agentMetrics struct {
	in, out, errors, terminal, redelivered float64
	lag, pending, ackPending, inflight     float64
	hist                                   map[float64]uint64
	histTotal                              uint64
}

func newAgentMetrics() *agentMetrics {
	return &agentMetrics{hist: make(map[float64]uint64)}
}

func (m *agentMetrics) merge(other *agentMetrics) {
	if other == nil {
		return
	}
	m.in += other.in
	m.out += other.out
	m.errors += other.errors
	m.terminal += other.terminal
	m.redelivered += other.redelivered
	if other.lag > m.lag {
		m.lag = other.lag
	}
	m.pending += other.pending
	m.ackPending += other.ackPending
	m.inflight += other.inflight
	for ub, n := range other.hist {
		m.hist[ub] += n
	}
	m.histTotal += other.histTotal
}

// readMetrics converts the dor_agent_* families into a per-agent view. Other
// families are ignored.
func readMetrics(families []*dto.MetricFamily) map[string]*agentMetrics {
	out := make(map[string]*agentMetrics)
	get := func(name string) *agentMetrics {
		am := out[name]
		if am == nil {
			am = newAgentMetrics()
			out[name] = am
		}
		return am
	}
	agentLabel := func(m *dto.Metric) string {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "agent" {
				return lp.GetValue()
			}
		}
		return ""
	}
	for _, family := range families {
		name := family.GetName()
		for _, m := range family.GetMetric() {
			agent := agentLabel(m)
			if agent == "" {
				continue
			}
			am := get(agent)
			switch name {
			case "dor_agent_in_total":
				am.in += m.GetCounter().GetValue()
			case "dor_agent_out_total":
				am.out += m.GetCounter().GetValue()
			case "dor_agent_errors_total":
				am.errors += m.GetCounter().GetValue()
			case "dor_agent_terminal_total":
				am.terminal += m.GetCounter().GetValue()
			case "dor_agent_redelivered_total":
				am.redelivered += m.GetCounter().GetValue()
			case "dor_agent_queue_lag_seconds":
				if v := m.GetGauge().GetValue(); v > am.lag {
					am.lag = v
				}
			case "dor_agent_pending":
				am.pending += m.GetGauge().GetValue()
			case "dor_agent_ack_pending":
				am.ackPending += m.GetGauge().GetValue()
			case "dor_agent_inflight":
				am.inflight += m.GetGauge().GetValue()
			case "dor_agent_handle_seconds":
				h := m.GetHistogram()
				for _, b := range h.GetBucket() {
					am.hist[b.GetUpperBound()] += b.GetCumulativeCount()
				}
				am.histTotal += h.GetSampleCount()
			}
		}
	}
	return out
}

// mergeForAgent aggregates a runner agent with its sub-consumers. Publisher is
// one registry agent backed by publisher-enrich and publisher-story.
func mergeForAgent(all map[string]*agentMetrics, name string) *agentMetrics {
	merged := newAgentMetrics()
	found := false
	for metricName, am := range all {
		if metricName == name || (len(metricName) > len(name) && metricName[:len(name)+1] == name+"-") {
			merged.merge(am)
			found = true
		}
	}
	if !found {
		return nil
	}
	return merged
}

func histogramQuantile(hist map[float64]uint64, total uint64, q float64) (float64, bool) {
	if total == 0 || len(hist) == 0 {
		return 0, false
	}
	upper := make([]float64, 0, len(hist))
	for ub := range hist {
		upper = append(upper, ub)
	}
	sort.Float64s(upper)
	rank := q * float64(total)
	for _, ub := range upper {
		if float64(hist[ub]) >= rank {
			return ub, true
		}
	}
	return upper[len(upper)-1], true
}

func ptr(v float64) *float64 { return &v }
func intPtr(v int) *int      { return &v }
