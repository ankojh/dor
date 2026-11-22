package ops

import (
	"context"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/bus"
)

// fakeAgent is a controllable agent for the Supervisor's control paths. It is
// deliberately not Restartable; restartableFake adds that method.
type fakeAgent struct {
	name      string
	running   bool
	lastErr   error
	terminal  int64
	current   int
	max       int
	supported bool
	setCalls  []int
	setErr    error
}

func (a *fakeAgent) Name() string                  { return a.name }
func (a *fakeAgent) Start(context.Context) error   { a.running = true; return nil }
func (a *fakeAgent) Stop(context.Context) error    { a.running = false; return nil }
func (a *fakeAgent) Consume(context.Context) error { return nil }
func (a *fakeAgent) Running() bool                 { return a.running }
func (a *fakeAgent) LastError() error              { return a.lastErr }
func (a *fakeAgent) TerminalCount() int64          { return a.terminal }
func (a *fakeAgent) Concurrency() (int, int, bool) { return a.current, a.max, a.supported }
func (a *fakeAgent) Publish(context.Context, string, []byte, ...bus.PublishOption) error {
	return nil
}
func (a *fakeAgent) SetConcurrency(n int) error {
	a.setCalls = append(a.setCalls, n)
	if a.setErr != nil {
		return a.setErr
	}
	a.current = n
	return nil
}

type restartableFake struct {
	*fakeAgent
	restarts   int
	restartErr error
}

func (a *restartableFake) Restart(context.Context) error {
	a.restarts++
	if a.restartErr != nil {
		return a.restartErr
	}
	a.running = true
	return nil
}

func supervisorWithAgents(t *testing.T, cfg Config, list ...agents.Agent) *Supervisor {
	t.Helper()
	return New(cfg, Deps{Runner: agents.NewRunner(list, nil)})
}

func stoppedRow(name string) AgentRow {
	return AgentRow{Name: name, Status: "stopped"}
}

func countActions(s *Supervisor) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.actions)
}

func lastAction(s *Supervisor) Action {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.actions) == 0 {
		return Action{}
	}
	return s.actions[len(s.actions)-1]
}

func TestEvaluateRestartsSuccessfulRestartable(t *testing.T) {
	agent := &restartableFake{fakeAgent: &fakeAgent{name: "cluster"}}
	s := supervisorWithAgents(t, DefaultConfig(), agent)
	now := time.Unix(1_700_000_000, 0).UTC()

	s.evaluateRestarts(context.Background(), []AgentRow{stoppedRow("cluster")}, now)

	if agent.restarts != 1 {
		t.Fatalf("restarts = %d, want 1", agent.restarts)
	}
	if got := countActions(s); got != 1 {
		t.Fatalf("actions = %d, want 1", got)
	}
	action := lastAction(s)
	if action.Kind != "restart" || action.Target != "cluster" || action.Advisory || action.Error != "" {
		t.Fatalf("restart action = %+v, want a non-advisory success", action)
	}
	if _, ok := s.restartAt["cluster"]; !ok {
		t.Fatal("restart deadline was not recorded")
	}
	if s.restartBackoff["cluster"] <= s.cfg.RestartBackoffMin {
		t.Fatalf("restart backoff was not advanced: %v", s.restartBackoff["cluster"])
	}
}

func TestEvaluateRestartsBacksOffUntilDeadline(t *testing.T) {
	agent := &restartableFake{fakeAgent: &fakeAgent{name: "embed"}}
	s := supervisorWithAgents(t, DefaultConfig(), agent)
	now := time.Unix(1_700_000_000, 0).UTC()
	row := stoppedRow("embed")

	s.evaluateRestarts(context.Background(), []AgentRow{row}, now)
	if agent.restarts != 1 {
		t.Fatalf("first restarts = %d", agent.restarts)
	}

	// Before the deadline the Supervisor must not hammer the agent.
	s.evaluateRestarts(context.Background(), []AgentRow{row}, now.Add(time.Second))
	if agent.restarts != 1 {
		t.Fatalf("restart repeated before the backoff deadline: %d", agent.restarts)
	}
	if got := countActions(s); got != 1 {
		t.Fatalf("actions before the deadline = %d, want 1", got)
	}

	// After the deadline one more attempt is allowed and the backoff doubles.
	s.evaluateRestarts(context.Background(), []AgentRow{row}, now.Add(s.cfg.RestartBackoffMin+time.Second))
	if agent.restarts != 2 {
		t.Fatalf("restarts after the deadline = %d, want 2", agent.restarts)
	}
	if s.restartBackoff["embed"] <= s.cfg.RestartBackoffMin {
		t.Fatalf("backoff did not grow past the minimum: %v", s.restartBackoff["embed"])
	}
}

func TestEvaluateRestartsAdvisoryForNonRestartable(t *testing.T) {
	agent := &fakeAgent{name: "echo"}
	s := supervisorWithAgents(t, DefaultConfig(), agent)

	s.evaluateRestarts(context.Background(), []AgentRow{stoppedRow("echo")}, time.Unix(1_700_000_000, 0).UTC())

	action := lastAction(s)
	if !action.Advisory {
		t.Fatalf("non-restartable action is not advisory: %+v", action)
	}
	if action.Error == "" || action.Kind != "restart" {
		t.Fatalf("non-restartable action = %+v, want a restart error", action)
	}
	if got := lastAction(s).Message; got == "" {
		t.Fatal("advisory action has no message")
	}
}

func TestEvaluateRestartsHealthyRowClearsBackoff(t *testing.T) {
	agent := &restartableFake{fakeAgent: &fakeAgent{name: "cluster"}}
	s := supervisorWithAgents(t, DefaultConfig(), agent)
	now := time.Unix(1_700_000_000, 0).UTC()
	s.restartAt["cluster"] = now.Add(time.Minute)
	s.restartBackoff["cluster"] = time.Minute

	s.evaluateRestarts(context.Background(), []AgentRow{{Name: "cluster", Status: "running"}}, now)

	if _, ok := s.restartAt["cluster"]; ok {
		t.Fatal("healthy sample did not clear restartAt")
	}
	if _, ok := s.restartBackoff["cluster"]; ok {
		t.Fatal("healthy sample did not clear restartBackoff")
	}
	if agent.restarts != 0 {
		t.Fatalf("healthy agent was restarted %d times", agent.restarts)
	}
}

func TestEvaluateScaleCallsSetConcurrencyWhenLagSustained(t *testing.T) {
	agent := &fakeAgent{name: "plot", current: 2, max: 8, supported: true}
	s := supervisorWithAgents(t, DefaultConfig(), agent)
	now := time.Unix(1_700_000_000, 0).UTC()
	row := AgentRow{
		Name:           "plot",
		Status:         "running",
		ScaleSupported: true,
		Concurrency:    intPtr(2),
		MaxConcurrency: intPtr(8),
		QueueLagMS:     ptr(3000),
	}
	s.scaleFrom["plot"] = now.Add(-11 * time.Second)

	s.evaluateScale(context.Background(), []AgentRow{row}, now)

	if len(agent.setCalls) != 1 || agent.setCalls[0] != 3 {
		t.Fatalf("SetConcurrency calls = %v, want [3]", agent.setCalls)
	}
	action := lastAction(s)
	if action.Kind != "scale" || action.Target != "plot" || action.Advisory {
		t.Fatalf("scale action = %+v", action)
	}
}

func TestEvaluateScaleSkipsUnsupportedAndCoolingDown(t *testing.T) {
	tests := []struct {
		name string
		row  AgentRow
		seed func(s *Supervisor, now time.Time)
	}{
		{
			name: "unsupported",
			row: AgentRow{
				Name:           "ordered",
				Status:         "running",
				ScaleSupported: false,
				QueueLagMS:     ptr(30_000),
			},
			seed: func(s *Supervisor, now time.Time) { s.scaleFrom["ordered"] = now.Add(-time.Minute) },
		},
		{
			name: "cooldown",
			row: AgentRow{
				Name:           "plot",
				Status:         "running",
				ScaleSupported: true,
				Concurrency:    intPtr(2),
				MaxConcurrency: intPtr(8),
				QueueLagMS:     ptr(3000),
			},
			seed: func(s *Supervisor, now time.Time) {
				s.scaleFrom["plot"] = now.Add(-time.Minute)
				s.lastScale["plot"] = now.Add(-time.Second)
			},
		},
		{
			name: "at max concurrency",
			row: AgentRow{
				Name:           "plot",
				Status:         "running",
				ScaleSupported: true,
				Concurrency:    intPtr(8),
				MaxConcurrency: intPtr(8),
				QueueLagMS:     ptr(3000),
			},
			seed: func(*Supervisor, time.Time) {},
		},
		{
			name: "lag below threshold",
			row: AgentRow{
				Name:           "plot",
				Status:         "running",
				ScaleSupported: true,
				Concurrency:    intPtr(2),
				MaxConcurrency: intPtr(8),
				QueueLagMS:     ptr(100),
			},
			seed: func(*Supervisor, time.Time) {},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agent := &fakeAgent{name: tc.row.Name, current: 2, max: 8, supported: tc.row.ScaleSupported}
			s := supervisorWithAgents(t, DefaultConfig(), agent)
			now := time.Unix(1_700_000_000, 0).UTC()
			tc.seed(s, now)

			s.evaluateScale(context.Background(), []AgentRow{tc.row}, now)

			if len(agent.setCalls) != 0 {
				t.Fatalf("SetConcurrency calls = %v, want none", agent.setCalls)
			}
			if got := countActions(s); got != 0 {
				t.Fatalf("actions = %d, want none", got)
			}
		})
	}
}

func TestEvaluateBudgetTransitionsOncePerLevel(t *testing.T) {
	s := New(DefaultConfig(), Deps{})
	s.shedLevel = 0
	bv := BudgetView{Available: true, ShedLevel: 1, Shedding: []string{"briefs"}, Ratio: 0.8}

	s.evaluateBudget(bv)
	if got := countActions(s); got != 1 {
		t.Fatalf("actions after a shed transition = %d, want 1", got)
	}
	action := lastAction(s)
	if action.Kind != "budget" || action.Target != "model" || action.Error != "" {
		t.Fatalf("budget action = %+v", action)
	}

	// The same level on the next sample is not a transition.
	s.evaluateBudget(bv)
	if got := countActions(s); got != 1 {
		t.Fatalf("unchanged shed level appended another action: %d", got)
	}

	s.evaluateBudget(BudgetView{Available: true, ShedLevel: 0, Ratio: 0.2})
	if got := countActions(s); got != 2 {
		t.Fatalf("recovery transition actions = %d, want 2", got)
	}
}

func TestEvaluateBudgetIgnoresUnavailableView(t *testing.T) {
	s := New(DefaultConfig(), Deps{})
	s.shedLevel = 0
	s.evaluateBudget(BudgetView{Available: false, ShedLevel: 3})
	if got := countActions(s); got != 0 {
		t.Fatalf("unavailable budget appended %d actions", got)
	}
	if s.shedLevel != 0 {
		t.Fatalf("unavailable budget changed shed level to %d", s.shedLevel)
	}
}

func TestEvaluateBudgetActionsRespectActionLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ActionLimit = 2
	s := New(cfg, Deps{})
	s.shedLevel = 0

	for level := 1; level <= 4; level++ {
		s.evaluateBudget(BudgetView{Available: true, ShedLevel: level, Shedding: shedStages(level), Ratio: 0.9})
	}
	if got := countActions(s); got != 2 {
		t.Fatalf("action log = %d, want the ActionLimit of 2", got)
	}
}

func TestNewClampsConfig(t *testing.T) {
	s := New(Config{
		Interval:          -1,
		ScaleLag:          0,
		AlarmLag:          -2,
		AlarmFor:          0,
		ScaleCooldown:     -1,
		BacklogFor:        0,
		RecoveryFor:       -1,
		MaxConcurrency:    0,
		RestartBackoffMin: 0,
		RestartBackoffMax: time.Millisecond,
		ActionLimit:       -1,
	}, Deps{})

	def := DefaultConfig()
	if s.cfg.Interval != def.Interval || s.cfg.ScaleLag != def.ScaleLag || s.cfg.AlarmLag != def.AlarmLag ||
		s.cfg.AlarmFor != def.AlarmFor || s.cfg.ScaleCooldown != def.ScaleCooldown ||
		s.cfg.BacklogFor != def.BacklogFor || s.cfg.RecoveryFor != def.RecoveryFor ||
		s.cfg.MaxConcurrency != def.MaxConcurrency || s.cfg.RestartBackoffMin != def.RestartBackoffMin ||
		s.cfg.RestartBackoffMax != def.RestartBackoffMax || s.cfg.ActionLimit != def.ActionLimit {
		t.Fatalf("New did not clamp to DefaultConfig: %+v", s.cfg)
	}
	if s.shedLevel != -1 {
		t.Fatalf("initial shed level = %d, want -1 (unknown)", s.shedLevel)
	}
}

func TestNewKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		Interval:          7 * time.Second,
		ScaleLag:          3 * time.Second,
		AlarmLag:          9 * time.Second,
		AlarmFor:          2 * time.Minute,
		ScaleCooldown:     10 * time.Second,
		BacklogFor:        time.Minute,
		RecoveryFor:       2 * time.Minute,
		MaxConcurrency:    4,
		RestartBackoffMin: 2 * time.Second,
		RestartBackoffMax: time.Minute,
		ActionLimit:       5,
	}
	s := New(cfg, Deps{})
	if s.cfg != cfg {
		t.Fatalf("New changed explicit config:\n got %+v\nwant %+v", s.cfg, cfg)
	}
}

func TestShedStages(t *testing.T) {
	tests := []struct {
		level int
		want  []string
	}{
		{level: -1},
		{level: 0},
		{level: 1, want: []string{"briefs"}},
		{level: 2, want: []string{"briefs", "why"}},
		{level: 3, want: []string{"briefs", "why", "thematic"}},
		{level: 4},
	}
	for _, tc := range tests {
		got := shedStages(tc.level)
		if len(got) != len(tc.want) {
			t.Fatalf("shedStages(%d) = %v, want %v", tc.level, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("shedStages(%d) = %v, want %v", tc.level, got, tc.want)
			}
		}
	}
}

func TestEvaluateRestartsUnknownAgentRecordsError(t *testing.T) {
	s := supervisorWithAgents(t, DefaultConfig(), &fakeAgent{name: "echo"})
	s.evaluateRestarts(context.Background(), []AgentRow{stoppedRow("missing")}, time.Unix(1_700_000_000, 0).UTC())
	action := lastAction(s)
	if action.Error == "" {
		t.Fatalf("unknown agent action = %+v, want a recorded restart error", action)
	}
	if action.Message == "" {
		t.Fatal("unknown agent action has no message")
	}
}

func TestEvaluateRestartsRunnerNotWired(t *testing.T) {
	s := New(DefaultConfig(), Deps{})
	s.evaluateRestarts(context.Background(), []AgentRow{stoppedRow("cluster")}, time.Unix(1_700_000_000, 0).UTC())
	action := lastAction(s)
	if action.Error == "" {
		t.Fatalf("missing runner action = %+v, want an error", action)
	}
}
