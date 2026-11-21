package ops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/budget"
)

// Config tunes the Supervisor. Zero values use the plan defaults.
type Config struct {
	// Interval is the sample cadence.
	Interval time.Duration
	// ScaleLag is the queue lag that requests one more handler (plan: 2 s).
	ScaleLag time.Duration
	// AlarmLag and AlarmFor define the sustained-lag alarm (plan: >5 s for
	// 1 minute).
	AlarmLag time.Duration
	AlarmFor time.Duration
	// ScaleCooldown bounds how often one agent is scaled.
	ScaleCooldown time.Duration
	// BacklogFor is how long pipeline lag must persist before GDELT is
	// reduced or paused. RecoveryFor is how long it must stay below 1 s before
	// GDELT returns to normal.
	BacklogFor  time.Duration
	RecoveryFor time.Duration
	// MaxConcurrency caps any one runtime scale action.
	MaxConcurrency int
	// RestartBackoffMin/Max bound the failed-agent restart backoff.
	RestartBackoffMin time.Duration
	RestartBackoffMax time.Duration
	// ActionLimit bounds the in-memory action log.
	ActionLimit int
}

// DefaultConfig returns the M6 defaults.
func DefaultConfig() Config {
	return Config{
		Interval:          2 * time.Second,
		ScaleLag:          2 * time.Second,
		AlarmLag:          5 * time.Second,
		AlarmFor:          time.Minute,
		ScaleCooldown:     30 * time.Second,
		BacklogFor:        20 * time.Second,
		RecoveryFor:       time.Minute,
		MaxConcurrency:    32,
		RestartBackoffMin: 5 * time.Second,
		RestartBackoffMax: 5 * time.Minute,
		ActionLimit:       200,
	}
}

// Deps is the live surface the Supervisor reads and controls.
type Deps struct {
	Runner    *agents.Runner
	Metrics   *agents.Metrics
	Budget    *budget.Manager
	BudgetCfg config.Budget
	Logger    *slog.Logger
}

// Supervisor is the single in-process supervisor for this dor process.
type Supervisor struct {
	cfg  Config
	dep  Deps
	log  *slog.Logger
	now  func() time.Time
	upAt time.Time

	mu      sync.RWMutex
	snap    Snapshot
	actions []Action

	controlMu      sync.Mutex
	prev           map[string]agentCounters
	lagSince       map[string]time.Time
	alarmOn        map[string]bool
	scaleFrom      map[string]time.Time
	lastScale      map[string]time.Time
	restartAt      map[string]time.Time
	restartBackoff map[string]time.Duration

	gdeltLevel       int
	gdeltSince       time.Time
	gdeltSupported   bool
	gdeltErr         string
	gdeltAdvisoryFor int
	backlogSince     time.Time
	recoverySince    time.Time
	shedLevel        int

	stop context.CancelFunc
	wg   sync.WaitGroup
}

type agentCounters struct {
	in, out, errors float64
	at              time.Time
}

// New builds a Supervisor. Start samples once and then runs its loop.
func New(cfg Config, dep Deps) *Supervisor {
	def := DefaultConfig()
	if cfg.Interval <= 0 {
		cfg.Interval = def.Interval
	}
	if cfg.ScaleLag <= 0 {
		cfg.ScaleLag = def.ScaleLag
	}
	if cfg.AlarmLag <= 0 {
		cfg.AlarmLag = def.AlarmLag
	}
	if cfg.AlarmFor <= 0 {
		cfg.AlarmFor = def.AlarmFor
	}
	if cfg.ScaleCooldown <= 0 {
		cfg.ScaleCooldown = def.ScaleCooldown
	}
	if cfg.BacklogFor <= 0 {
		cfg.BacklogFor = def.BacklogFor
	}
	if cfg.RecoveryFor <= 0 {
		cfg.RecoveryFor = def.RecoveryFor
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = def.MaxConcurrency
	}
	if cfg.RestartBackoffMin <= 0 {
		cfg.RestartBackoffMin = def.RestartBackoffMin
	}
	if cfg.RestartBackoffMax < cfg.RestartBackoffMin {
		cfg.RestartBackoffMax = def.RestartBackoffMax
	}
	if cfg.ActionLimit <= 0 {
		cfg.ActionLimit = def.ActionLimit
	}
	log := dep.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Supervisor{
		cfg:              cfg,
		dep:              dep,
		log:              log.With("supervisor", "ops"),
		now:              time.Now,
		prev:             make(map[string]agentCounters),
		lagSince:         make(map[string]time.Time),
		alarmOn:          make(map[string]bool),
		scaleFrom:        make(map[string]time.Time),
		lastScale:        make(map[string]time.Time),
		restartAt:        make(map[string]time.Time),
		restartBackoff:   make(map[string]time.Duration),
		shedLevel:        -1,
		gdeltAdvisoryFor: -1,
	}
}

// Start begins sampling. It performs one synchronous sample so /ops is useful
// on its first request, then loops until ctx ends or Stop is called.
func (s *Supervisor) Start(ctx context.Context) {
	s.controlMu.Lock()
	if s.upAt.IsZero() {
		s.upAt = s.now()
	}
	s.controlMu.Unlock()
	runCtx, cancel := context.WithCancel(ctx)
	s.stop = cancel
	sampleCtx, sampleCancel := context.WithTimeout(runCtx, 5*time.Second)
	s.Sample(sampleCtx)
	sampleCancel()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(runCtx, 10*time.Second)
				s.Sample(ctx)
				cancel()
			}
		}
	}()
}

// Stop ends the sampling loop.
func (s *Supervisor) Stop() {
	if s.stop != nil {
		s.stop()
	}
	s.wg.Wait()
}

// Sample refreshes the metrics, health, budget and control state once.
func (s *Supervisor) Sample(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("supervisor sample panicked", "panic", fmt.Sprint(r))
		}
	}()
	now := s.now().UTC()
	statuses := map[string]string{}
	if s.dep.Runner != nil {
		_, statuses = s.dep.Runner.Healthy()
	}

	metrics := map[string]*agentMetrics{}
	metricsErr := ""
	if s.dep.Metrics != nil {
		if families, err := s.dep.Metrics.Registry().Gather(); err != nil {
			metricsErr = err.Error()
		} else {
			metrics = readMetrics(families)
		}
	} else {
		metricsErr = "metrics registry not wired"
	}

	bv := s.budgetView(ctx)
	rows := s.agentRows(metrics, statuses, now)

	s.controlMu.Lock()
	alarms := s.evaluate(ctx, rows, bv, now)
	level, since, supported, gdeltErr := s.gdeltLevel, s.gdeltSince, s.gdeltSupported, s.gdeltErr
	s.controlMu.Unlock()

	uptime := 0.0
	if !s.upAt.IsZero() {
		uptime = now.Sub(s.upAt).Seconds()
	}
	snap := Snapshot{
		GeneratedAt:  now.Format(time.RFC3339),
		UptimeSec:    uptime,
		Agents:       rows,
		Budget:       bv,
		Alarms:       alarms,
		Thresholds:   s.thresholdView(),
		MetricsError: metricsErr,
		GDELT: BackpressureView{
			Supported: supported,
			Level:     level,
			Label:     gdeltLabel(level),
			Since:     timeString(since),
			Error:     gdeltErr,
		},
	}
	s.mu.Lock()
	actions := make([]Action, len(s.actions))
	copy(actions, s.actions)
	s.mu.Unlock()
	for i, j := 0, len(actions)-1; i < j; i, j = i+1, j-1 {
		actions[i], actions[j] = actions[j], actions[i]
	}
	snap.Actions = actions
	s.mu.Lock()
	s.snap = snap
	s.mu.Unlock()
}

// Snapshot returns the latest sample. The slices are copies.
func (s *Supervisor) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := s.snap
	snap.Actions = append([]Action(nil), snap.Actions...)
	snap.Agents = append([]AgentRow(nil), snap.Agents...)
	snap.Alarms = append([]Alarm(nil), snap.Alarms...)
	return snap
}

func (s *Supervisor) thresholdView() Thresholds {
	return Thresholds{
		SampleMS:            s.cfg.Interval.Milliseconds(),
		ScaleLagMS:          s.cfg.ScaleLag.Milliseconds(),
		AlarmLagMS:          s.cfg.AlarmLag.Milliseconds(),
		AlarmForMS:          s.cfg.AlarmFor.Milliseconds(),
		ScaleCooldownMS:     s.cfg.ScaleCooldown.Milliseconds(),
		BacklogForMS:        s.cfg.BacklogFor.Milliseconds(),
		RecoveryForMS:       s.cfg.RecoveryFor.Milliseconds(),
		MaxConcurrency:      s.cfg.MaxConcurrency,
		RestartBackoffMinMS: s.cfg.RestartBackoffMin.Milliseconds(),
		RestartBackoffMaxMS: s.cfg.RestartBackoffMax.Milliseconds(),
	}
}

func (s *Supervisor) budgetView(ctx context.Context) BudgetView {
	bv := BudgetView{
		CapUSD:     s.dep.BudgetCfg.DailyUSD,
		BriefAt:    s.dep.BudgetCfg.ShedBriefAt,
		WhyAt:      s.dep.BudgetCfg.ShedWhyAt,
		ThematicAt: s.dep.BudgetCfg.ShedThematicAt,
	}
	if s.dep.Budget == nil {
		bv.Error = "budget manager not wired"
		return bv
	}
	daily, err := s.dep.Budget.Snapshot(ctx)
	if err != nil {
		bv.Error = err.Error()
		return bv
	}
	bv.Available = true
	bv.Day = daily.Day
	bv.SpendUSD = daily.USD
	bv.Calls = daily.Calls
	bv.Pending = len(daily.Pending)
	bv.StaleCollected = s.dep.Budget.StaleCollected()
	if bv.CapUSD > 0 {
		bv.Ratio = daily.USD / bv.CapUSD
	}
	bv.ShedLevel = s.dep.Budget.ShedLevel(bv.Ratio)
	bv.Shedding = shedStages(bv.ShedLevel)
	return bv
}

func shedStages(level int) []string {
	switch level {
	case 1:
		return []string{"briefs"}
	case 2:
		return []string{"briefs", "why"}
	case 3:
		return []string{"briefs", "why", "thematic"}
	default:
		return []string{}
	}
}

func (s *Supervisor) agentRows(metrics map[string]*agentMetrics, statuses map[string]string, now time.Time) []AgentRow {
	if s.dep.Runner == nil {
		return nil
	}
	names := s.dep.Runner.Names()
	rows := make([]AgentRow, 0, len(names))
	for _, name := range names {
		row := AgentRow{Name: name, Status: "unknown"}
		if raw, ok := statuses[name]; ok {
			s.applyStatus(&row, raw)
		}
		if am := mergeForAgent(metrics, name); am != nil {
			row.MetricsAvailable = true
			row.Inflight = ptr(am.inflight)
			row.Pending = ptr(am.pending)
			row.AckPending = ptr(am.ackPending)
			row.ErrorsTotal = ptr(am.errors)
			row.TerminalTotal = ptr(am.terminal)
			row.Redelivered = ptr(am.redelivered)
			row.QueueLagMS = ptr(am.lag * 1000)
			if p95, ok := histogramQuantile(am.hist, am.histTotal, 0.95); ok {
				row.P95MS = ptr(p95 * 1000)
			}
			if prev, ok := s.prev[name]; ok {
				elapsed := now.Sub(prev.at).Seconds()
				if elapsed > 0 {
					din := am.in - prev.in
					dout := am.out - prev.out
					derr := am.errors - prev.errors
					if din < 0 {
						din = 0
					}
					if dout < 0 {
						dout = 0
					}
					if derr < 0 {
						derr = 0
					}
					row.InPerSec = ptr(din / elapsed)
					row.OutPerSec = ptr(dout / elapsed)
					rate := 0.0
					if din > 0 {
						rate = derr / din
					}
					row.ErrorRate = ptr(rate)
				}
			}
			s.prev[name] = agentCounters{in: am.in, out: am.out, errors: am.errors, at: now}
		}
		if a, ok := s.dep.Runner.Agent(name); ok {
			row.LastStatusError(a)
			if producer, ok := a.(interface{ Producer() bool }); ok {
				row.Producer = producer.Producer()
			}
			if scalable, ok := a.(agents.Scalable); ok {
				current, max, supported := scalable.Concurrency()
				row.ScaleSupported = supported
				if supported {
					row.Concurrency = intPtr(current)
					row.MaxConcurrency = intPtr(max)
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func (s *Supervisor) applyStatus(row *AgentRow, raw string) {
	switch {
	case raw == "running":
		row.Status = "running"
	case strings.HasPrefix(raw, "error:"):
		row.Status = "error"
		row.StatusDetail = strings.TrimSpace(strings.TrimPrefix(raw, "error:"))
	case raw == "stopped":
		row.Status = "stopped"
	case strings.HasPrefix(raw, "unknown"):
		row.Status = "unknown"
	default:
		row.Status = raw
	}
}

// LastStatusError attaches the health reporter's last error without
// duplicating the status label.
func (row *AgentRow) LastStatusError(a agents.Agent) {
	if reporter, ok := a.(agents.HealthReporter); ok {
		if err := reporter.LastError(); err != nil && row.StatusDetail == "" {
			row.StatusDetail = err.Error()
		}
	}
}

func (s *Supervisor) evaluate(ctx context.Context, rows []AgentRow, bv BudgetView, now time.Time) []Alarm {
	s.evaluateAlarms(rows, now)
	s.evaluateScale(ctx, rows, now)
	s.evaluateRestarts(ctx, rows, now)
	s.evaluateGDELT(ctx, rows, now)
	s.evaluateBudget(bv)

	var alarms []Alarm
	for _, row := range rows {
		if !s.alarmOn[row.Name] {
			continue
		}
		since := s.lagSince[row.Name]
		lag := 0.0
		if row.QueueLagMS != nil {
			lag = *row.QueueLagMS
		}
		alarms = append(alarms, Alarm{
			Agent:        row.Name,
			LagMS:        lag,
			Since:        timeString(since),
			ActiveForSec: now.Sub(since).Seconds(),
			ThresholdMS:  float64(s.cfg.AlarmLag.Milliseconds()),
		})
	}
	return alarms
}

func (s *Supervisor) evaluateAlarms(rows []AgentRow, now time.Time) {
	for _, row := range rows {
		if row.QueueLagMS == nil {
			continue
		}
		lag := time.Duration(*row.QueueLagMS) * time.Millisecond
		if lag >= s.cfg.AlarmLag {
			if s.lagSince[row.Name].IsZero() {
				s.lagSince[row.Name] = now
			}
			if !s.alarmOn[row.Name] && now.Sub(s.lagSince[row.Name]) >= s.cfg.AlarmFor {
				s.alarmOn[row.Name] = true
				s.addAction(Action{
					At:      now.Format(time.RFC3339),
					Kind:    "alarm",
					Target:  row.Name,
					Message: fmt.Sprintf("queue lag %s above %s for %s", lag.Round(time.Millisecond), s.cfg.AlarmLag, s.cfg.AlarmFor),
				})
				s.log.Warn("queue lag alarm", "agent", row.Name, "lag", lag, "for", s.cfg.AlarmFor)
			}
			continue
		}
		delete(s.lagSince, row.Name)
		if s.alarmOn[row.Name] {
			s.alarmOn[row.Name] = false
			s.addAction(Action{
				At:      now.Format(time.RFC3339),
				Kind:    "recovery",
				Target:  row.Name,
				Message: "queue lag recovered below the alarm threshold",
			})
		}
	}
}

func (s *Supervisor) evaluateScale(ctx context.Context, rows []AgentRow, now time.Time) {
	for _, row := range rows {
		if !row.ScaleSupported || row.Concurrency == nil || row.MaxConcurrency == nil || row.QueueLagMS == nil {
			delete(s.scaleFrom, row.Name)
			continue
		}
		lag := time.Duration(*row.QueueLagMS) * time.Millisecond
		if lag < s.cfg.ScaleLag || *row.Concurrency >= *row.MaxConcurrency || *row.Concurrency >= s.cfg.MaxConcurrency {
			delete(s.scaleFrom, row.Name)
			continue
		}
		if s.scaleFrom[row.Name].IsZero() {
			s.scaleFrom[row.Name] = now
		}
		if now.Sub(s.scaleFrom[row.Name]) < 10*time.Second {
			continue
		}
		if last := s.lastScale[row.Name]; !last.IsZero() && now.Sub(last) < s.cfg.ScaleCooldown {
			continue
		}
		a, ok := s.dep.Runner.Agent(row.Name)
		if !ok {
			continue
		}
		scalable, ok := a.(agents.Scalable)
		if !ok {
			continue
		}
		from := *row.Concurrency
		next := from + 1
		if next > s.cfg.MaxConcurrency {
			next = s.cfg.MaxConcurrency
		}
		err := scalable.SetConcurrency(next)
		s.lastScale[row.Name] = now
		s.scaleFrom[row.Name] = now
		action := Action{
			At:     now.Format(time.RFC3339),
			Kind:   "scale",
			Target: row.Name,
			Message: fmt.Sprintf("concurrency %d → %d · queue lag %s > %s",
				from, next, lag.Round(time.Millisecond), s.cfg.ScaleLag),
		}
		if err != nil {
			action.Advisory = true
			action.Error = err.Error()
			action.Message = fmt.Sprintf("scale %d → %d unavailable · %s", from, next, lag.Round(time.Millisecond))
			s.lastScale[row.Name] = now.Add(s.cfg.ScaleCooldown - 10*time.Second)
		}
		s.addAction(action)
		s.log.Info("supervisor scale action", "agent", row.Name, "from", from, "to", next, "err", err)
	}
}

func (s *Supervisor) evaluateRestarts(ctx context.Context, rows []AgentRow, now time.Time) {
	for _, row := range rows {
		if row.Status == "running" {
			delete(s.restartAt, row.Name)
			delete(s.restartBackoff, row.Name)
			continue
		}
		next, seen := s.restartAt[row.Name]
		if !seen {
			next = now
		}
		if now.Before(next) {
			continue
		}
		backoff := s.restartBackoff[row.Name]
		if backoff <= 0 {
			backoff = s.cfg.RestartBackoffMin
		}
		restartCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		err := error(nil)
		if s.dep.Runner != nil {
			err = s.dep.Runner.Restart(restartCtx, row.Name)
		} else {
			err = errors.New("runner not wired")
		}
		cancel()
		action := Action{
			At:      now.Format(time.RFC3339),
			Kind:    "restart",
			Target:  row.Name,
			Message: fmt.Sprintf("restarted %s after status %q", row.Name, row.Status),
		}
		if err != nil {
			action.Error = err.Error()
			action.Advisory = strings.Contains(err.Error(), "does not support in-process restart")
			action.Message = fmt.Sprintf("restart %s failed; next attempt in %s", row.Name, backoff.Round(time.Second))
		} else {
			action.Message = fmt.Sprintf("restarted %s · status was %q", row.Name, row.Status)
		}
		// A successful restart does not prove the agent is healthy: if it
		// reports unhealthy again on the next sample, back off further. A
		// healthy sample clears both maps above.
		s.restartAt[row.Name] = now.Add(backoff)
		backoff *= 2
		if backoff > s.cfg.RestartBackoffMax {
			backoff = s.cfg.RestartBackoffMax
		}
		s.restartBackoff[row.Name] = backoff
		s.addAction(action)
		s.log.Info("supervisor restart action", "agent", row.Name, "err", err)
	}
}

func (s *Supervisor) evaluateGDELT(ctx context.Context, rows []AgentRow, now time.Time) {
	if s.dep.Runner == nil {
		return
	}
	if _, ok := s.dep.Runner.Agent("gdelt"); !ok {
		// GDELT backpressure belongs to the process that runs the GDELT
		// producer. Other split processes must not claim they reduced it.
		s.gdeltSupported = false
		s.gdeltErr = "gdelt agent is not running in this process"
		s.gdeltLevel = 0
		s.gdeltSince = time.Time{}
		s.backlogSince = time.Time{}
		s.recoverySince = time.Time{}
		return
	}
	maxLag := time.Duration(0)
	for _, row := range rows {
		if row.QueueLagMS == nil {
			continue
		}
		if lag := time.Duration(*row.QueueLagMS) * time.Millisecond; lag > maxLag {
			maxLag = lag
		}
	}
	target := s.gdeltLevel
	switch {
	case maxLag >= 5*time.Second:
		s.recoverySince = time.Time{}
		if s.backlogSince.IsZero() {
			s.backlogSince = now
		}
		if now.Sub(s.backlogSince) >= s.cfg.BacklogFor {
			target = 2
		}
	case maxLag >= 2*time.Second:
		s.recoverySince = time.Time{}
		if s.backlogSince.IsZero() {
			s.backlogSince = now
		}
		if now.Sub(s.backlogSince) >= s.cfg.BacklogFor {
			target = 1
		}
	default:
		s.backlogSince = time.Time{}
		if maxLag < time.Second {
			if s.recoverySince.IsZero() {
				s.recoverySince = now
			}
			if now.Sub(s.recoverySince) >= s.cfg.RecoveryFor {
				target = 0
			}
		}
	}
	if target == s.gdeltLevel {
		return
	}
	err := s.setGDELTLevel(target)
	if err != nil {
		// Do not claim the requested level took effect. Keep the applied
		// level, report the missing control, and retry on a later sample
		// without appending the same advisory again.
		s.gdeltSupported = false
		s.gdeltErr = err.Error()
		if s.gdeltAdvisoryFor != target {
			s.addAction(Action{
				At:       now.Format(time.RFC3339),
				Kind:     "gdelt",
				Target:   "gdelt",
				Advisory: true,
				Error:    err.Error(),
				Message: fmt.Sprintf("GDELT backpressure %d unavailable · max pipeline lag %s",
					target, maxLag.Round(time.Millisecond)),
			})
			s.gdeltAdvisoryFor = target
		}
		s.log.Warn("supervisor gdelt control unavailable", "level", target, "err", err)
		return
	}
	s.gdeltLevel = target
	s.gdeltSince = now
	s.gdeltSupported = true
	s.gdeltErr = ""
	s.gdeltAdvisoryFor = -1
	s.addAction(Action{
		At:     now.Format(time.RFC3339),
		Kind:   "gdelt",
		Target: "gdelt",
		Message: fmt.Sprintf("GDELT backpressure %d (%s) · max pipeline lag %s",
			target, gdeltLabel(target), maxLag.Round(time.Millisecond)),
	})
	s.log.Info("supervisor gdelt action", "level", target, "max_lag", maxLag)
}

func (s *Supervisor) evaluateBudget(bv BudgetView) {
	if !bv.Available {
		return
	}
	if s.shedLevel < 0 {
		s.shedLevel = bv.ShedLevel
		if bv.ShedLevel > 0 {
			s.addAction(Action{
				At:      time.Now().UTC().Format(time.RFC3339),
				Kind:    "budget",
				Target:  "model",
				Message: fmt.Sprintf("shed level %d already active at %.0f%% of the daily cap: %s", bv.ShedLevel, bv.Ratio*100, strings.Join(bv.Shedding, ", ")),
			})
		}
		return
	}
	if bv.ShedLevel == s.shedLevel {
		return
	}
	old := s.shedLevel
	s.shedLevel = bv.ShedLevel
	msg := fmt.Sprintf("shed level %d → %d at %.0f%% of the daily model cap", old, bv.ShedLevel, bv.Ratio*100)
	if len(bv.Shedding) > 0 {
		msg += " · shedding " + strings.Join(bv.Shedding, ", ")
	}
	s.addAction(Action{
		At:      time.Now().UTC().Format(time.RFC3339),
		Kind:    "budget",
		Target:  "model",
		Message: msg,
	})
}

func (s *Supervisor) setGDELTLevel(level int) error {
	if s.dep.Runner == nil {
		return errors.New("runner not wired")
	}
	a, ok := s.dep.Runner.Agent("gdelt")
	if !ok {
		return errors.New("gdelt agent is not running in this process")
	}
	control, ok := a.(interface{ SetBackpressure(int) })
	if !ok {
		return errors.New("gdelt agent has no runtime backpressure control")
	}
	control.SetBackpressure(level)
	return nil
}

// Control applies one manual runtime action. It is the narrow surface the ops
// page calls; unknown or unavailable controls return an explicit error.
func (s *Supervisor) Control(ctx context.Context, action, agent string, value int) (string, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	now := s.now().UTC()
	if s.dep.Runner == nil {
		return "", errors.New("agent runner not wired")
	}
	switch action {
	case "gdelt.backpressure":
		if value < 0 || value > 2 {
			return "", fmt.Errorf("backpressure level %d is outside 0..2", value)
		}
		if err := s.setGDELTLevel(value); err != nil {
			s.addAction(Action{At: now.Format(time.RFC3339), Kind: "manual", Target: "gdelt", Message: "manual GDELT backpressure unavailable", Advisory: true, Error: err.Error()})
			s.gdeltSupported = false
			s.gdeltErr = err.Error()
			return "", err
		}
		s.gdeltLevel = value
		s.gdeltSince = now
		s.gdeltSupported = true
		s.gdeltErr = ""
		s.addAction(Action{At: now.Format(time.RFC3339), Kind: "manual", Target: "gdelt", Message: "manual GDELT backpressure " + gdeltLabel(value)})
		return "GDELT backpressure set to " + gdeltLabel(value), nil
	case "concurrency":
		a, ok := s.dep.Runner.Agent(agent)
		if !ok {
			return "", fmt.Errorf("unknown agent %q", agent)
		}
		scalable, ok := a.(agents.Scalable)
		if !ok {
			return "", fmt.Errorf("%s has no runtime concurrency control", agent)
		}
		if err := scalable.SetConcurrency(value); err != nil {
			return "", err
		}
		current, max, _ := scalable.Concurrency()
		s.addAction(Action{At: now.Format(time.RFC3339), Kind: "manual", Target: agent, Message: fmt.Sprintf("concurrency set to %d (max %d)", current, max)})
		return fmt.Sprintf("%s concurrency set to %d", agent, current), nil
	case "restart":
		if err := s.dep.Runner.Restart(ctx, agent); err != nil {
			s.addAction(Action{At: now.Format(time.RFC3339), Kind: "manual", Target: agent, Message: "manual restart failed", Error: err.Error(), Advisory: true})
			return "", err
		}
		delete(s.restartAt, agent)
		delete(s.restartBackoff, agent)
		s.addAction(Action{At: now.Format(time.RFC3339), Kind: "manual", Target: agent, Message: "manual restart completed"})
		return agent + " restarted", nil
	default:
		return "", fmt.Errorf("unknown control action %q", action)
	}
}

func (s *Supervisor) addAction(a Action) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, a)
	if len(s.actions) > s.cfg.ActionLimit {
		drop := len(s.actions) - s.cfg.ActionLimit
		s.actions = append([]Action(nil), s.actions[drop:]...)
	}
}

func gdeltLabel(level int) string {
	switch level {
	case 0:
		return "normal"
	case 1:
		return "reduced (50 items/file)"
	case 2:
		return "paused (new files held)"
	default:
		return fmt.Sprintf("level %d", level)
	}
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
