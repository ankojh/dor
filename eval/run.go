package eval

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/llm"
)

// Harness runs both backends over one dataset.
type Harness struct {
	DS         *Dataset
	Jev        decide.Decider
	JevKind    string // "live" or "mock"
	Claude     *llm.Client
	ClaudeKind string // "live" or "mock"
	Thresholds config.Thresholds
	Now        func() time.Time
	Log        *slog.Logger
}

// RelationObservation is one pair's raw answers, independent of any current
// threshold. Threshold selection recomputes the accepted set from these.
type RelationObservation struct {
	ID           string
	RelatedP     float64
	Type         string
	TypeAnswered bool
	GoldRelated  bool
	GoldType     string
}

// TaskResult is one task on one backend.
type TaskResult struct {
	Task    string `json:"task"`
	Backend string `json:"backend"`
	Kind    string `json:"kind"`

	N          int `json:"n"`
	Calls      int `json:"calls"`
	Answered   int `json:"answered"`
	Unanswered int `json:"unanswered"`
	Refusals   int `json:"refusals"`
	Errors     int `json:"errors"`
	Correct    int `json:"correct"`

	Accuracy    float64 `json:"accuracy"`
	AccuracyAll float64 `json:"accuracy_all"`

	LatencyP50MS      float64        `json:"latency_p50_ms"`
	LatencyP95MS      float64        `json:"latency_p95_ms"`
	LastLatencyP50MS  float64        `json:"last_attempt_latency_p50_ms"`
	LastLatencyP95MS  float64        `json:"last_attempt_latency_p95_ms"`
	CostUSD           float64        `json:"cost_usd"`
	CostPer1kUSD      float64        `json:"cost_per_1k_usd"`
	InputTokens       int64          `json:"input_tokens"`
	OutputTokens      int64          `json:"output_tokens"`
	CacheReadTokens   int64          `json:"cache_read_input_tokens"`
	CacheCreateTokens int64          `json:"cache_creation_input_tokens"`
	ServedModels      map[string]int `json:"served_models,omitempty"`

	// Event calibration: probability against observed truth.
	Bins []Bin   `json:"bins,omitempty"`
	ECE  float64 `json:"ece,omitempty"`
	// Decision calibration at DecisionThreshold: p for a predicted yes,
	// 1-p for a predicted no. Empty for choice-only tasks where event and
	// decision calibration coincide.
	DecisionBins      []Bin   `json:"decision_bins,omitempty"`
	DecisionECE       float64 `json:"decision_ece,omitempty"`
	DecisionThreshold float64 `json:"decision_threshold,omitempty"`

	Coverage []CoveragePoint `json:"coverage,omitempty"`

	Extra map[string]float64 `json:"extra,omitempty"`

	ErrorSamples []string `json:"error_samples,omitempty"`

	Observations            []Observation         `json:"-"`
	ObservationsAreDecision bool                  `json:"-"`
	Binary                  []BinaryObservation   `json:"-"`
	DecisionObservations    []Observation         `json:"-"`
	Relations               []RelationObservation `json:"-"`
	FallbackClassify        []FallbackClassify    `json:"-"`
	FallbackPlot            []FallbackPlot        `json:"-"`

	latencies      []time.Duration
	lastLatencies  []time.Duration
	concentrations []float64
}

// RunResult is the output of one eval run.
type RunResult struct {
	Tasks      []TaskResult
	StartedAt  time.Time
	FinishedAt time.Time
	JevKind    string
	ClaudeKind string
}

// Mock reports whether any backend was a mock.
func (r RunResult) Mock() bool {
	return r.JevKind == "mock" || r.ClaudeKind == "mock"
}

// Find returns the result for one task/backend pair.
func (r RunResult) Find(task, backend string) (TaskResult, bool) {
	for _, t := range r.Tasks {
		if t.Task == task && t.Backend == backend {
			return t, true
		}
	}
	return TaskResult{}, false
}

func newTask(task, backend, kind string) TaskResult {
	return TaskResult{Task: task, Backend: backend, Kind: kind, Extra: map[string]float64{}}
}

func (t *TaskResult) addCall(latency, last time.Duration, cost float64, u llm.Usage, model string) {
	t.Calls++
	t.CostUSD += cost
	t.InputTokens += u.InputTokens
	t.OutputTokens += u.OutputTokens
	t.CacheReadTokens += u.CacheReadInputTokens
	t.CacheCreateTokens += u.CacheCreationInputTokens
	if latency > 0 {
		t.latencies = append(t.latencies, latency)
	}
	if last > 0 {
		t.lastLatencies = append(t.lastLatencies, last)
	}
	t.observeModel(model)
}

func (t *TaskResult) addJevCall(resp decide.Response) {
	t.Calls++
	t.CostUSD += resp.Usage.CostUSD
	t.InputTokens += resp.Usage.InputTokens
	t.OutputTokens += resp.Usage.OutputTokens
	t.CacheReadTokens += resp.Usage.CachedTokens
	if resp.Latency > 0 {
		t.latencies = append(t.latencies, resp.Latency)
	}
	if resp.LastAttemptLatency > 0 {
		t.lastLatencies = append(t.lastLatencies, resp.LastAttemptLatency)
	}
	t.observeModel(resp.Model)
}

func (t *TaskResult) observeModel(model string) {
	if model == "" {
		model = "unknown"
	}
	if t.ServedModels == nil {
		t.ServedModels = make(map[string]int)
	}
	t.ServedModels[model]++
}

func (t *TaskResult) addItem(answered, correct bool) {
	if !answered {
		t.Unanswered++
		return
	}
	t.Answered++
	if correct {
		t.Correct++
	}
}

func (t *TaskResult) addConcentration(c float64) {
	if c > 0 {
		t.concentrations = append(t.concentrations, c)
	}
}

func (t *TaskResult) addError(err error) {
	t.Errors++
	if len(t.ErrorSamples) < 3 {
		t.ErrorSamples = append(t.ErrorSamples, err.Error())
	}
}

func (t *TaskResult) finalize() {
	if t.Answered > 0 {
		t.Accuracy = float64(t.Correct) / float64(t.Answered)
	}
	if t.N > 0 {
		t.AccuracyAll = float64(t.Correct) / float64(t.N)
		t.CostPer1kUSD = CostPer1k(t.CostUSD, t.N)
	}
	t.LatencyP50MS = ms(Percentile(t.latencies, 50))
	t.LatencyP95MS = ms(Percentile(t.latencies, 95))
	t.LastLatencyP50MS = ms(Percentile(t.lastLatencies, 50))
	t.LastLatencyP95MS = ms(Percentile(t.lastLatencies, 95))

	switch {
	case len(t.Observations) > 0 && t.ObservationsAreDecision:
		t.DecisionBins, t.DecisionECE = Calibrate(t.Observations)
	case len(t.Observations) > 0:
		t.Bins, t.ECE = Calibrate(t.Observations)
	case len(t.Binary) > 0:
		t.Bins, t.ECE, t.DecisionBins, t.DecisionECE = CalibrateBinary(t.Binary, t.DecisionThreshold)
	}
	if len(t.DecisionObservations) > 0 {
		t.DecisionBins, t.DecisionECE = Calibrate(t.DecisionObservations)
		if len(t.Coverage) == 0 {
			t.Coverage = AccuracyAtCoverage(t.DecisionObservations, ThresholdGrid())
		}
	}

	switch {
	case len(t.Observations) > 0:
		t.Coverage = AccuracyAtCoverage(t.Observations, ThresholdGrid())
	case len(t.Binary) > 0:
		t.Coverage = BinaryCoverage(t.Binary, ThresholdGrid())
	}

	if len(t.concentrations) > 0 {
		sum := 0.0
		for _, c := range t.concentrations {
			sum += c
		}
		t.Extra["mean_concentration"] = sum / float64(len(t.concentrations))
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Run executes classify, plot, relate and match for both backends.
func (h *Harness) Run(ctx context.Context) RunResult {
	if h.Now == nil {
		h.Now = time.Now
	}
	if h.Log == nil {
		h.Log = slog.Default()
	}
	out := RunResult{
		StartedAt:  h.Now(),
		JevKind:    h.JevKind,
		ClaudeKind: h.ClaudeKind,
	}
	if h.Jev == nil {
		h.JevKind = "unavailable"
		out.JevKind = h.JevKind
	}
	if h.Claude == nil {
		h.ClaudeKind = "unavailable"
		out.ClaudeKind = h.ClaudeKind
	}
	if h.Jev != nil {
		out.Tasks = append(out.Tasks,
			h.runClassifyJev(ctx),
			h.runPlotJev(ctx),
			h.runRelateJev(ctx),
			h.runMatchJev(ctx),
		)
	}
	if h.Claude != nil {
		out.Tasks = append(out.Tasks,
			h.runClassifyClaude(ctx),
			h.runPlotClaude(ctx),
			h.runRelateClaude(ctx),
			h.runMatchClaude(ctx),
		)
	}
	out.FinishedAt = h.Now()
	return out
}

func (h *Harness) runClassifyJev(ctx context.Context) TaskResult {
	t := newTask("classify", "jev", h.JevKind)
	accepted, acceptedCorrect := 0, 0
	for _, a := range h.DS.Articles {
		label, ok := h.DS.ArticleLabels[a.ID]
		if !ok {
			continue
		}
		t.N++
		resp, err := h.Jev.Decide(ctx, decide.ClassifyRequest(decideArticle(a)))
		t.addJevCall(resp)
		if err != nil {
			t.addError(err)
			t.addItem(false, false)
			t.FallbackClassify = append(t.FallbackClassify, FallbackClassify{ID: a.ID})
			continue
		}
		res := decide.CombineClassify(resp, h.Thresholds)
		t.addConcentration(res.Concentration)
		if !res.Answered {
			if refused(resp, "category") {
				t.Refusals++
			}
			t.addItem(false, false)
			t.FallbackClassify = append(t.FallbackClassify, FallbackClassify{ID: a.ID})
			continue
		}
		correct := res.Category == label.Category
		t.addItem(true, correct)
		t.FallbackClassify = append(t.FallbackClassify, FallbackClassify{
			ID: a.ID, P: res.Probability, OK: res.HasProbability,
		})
		if res.HasProbability {
			t.Observations = append(t.Observations, Observation{ID: a.ID, P: res.Probability, Correct: correct})
		}
		if res.Accepted {
			accepted++
			if correct {
				acceptedCorrect++
			}
		}
	}
	// A label whose article id is missing from the candidate file still stays
	// in the denominator and fails closed as a fallback.
	seen := make(map[string]bool, len(t.FallbackClassify))
	for _, f := range t.FallbackClassify {
		seen[f.ID] = true
	}
	for id := range h.DS.ArticleLabels {
		if !seen[id] {
			t.N++
			t.addItem(false, false)
			t.FallbackClassify = append(t.FallbackClassify, FallbackClassify{ID: id})
		}
	}
	t.Extra["accept_rate"] = ratio(accepted, t.N)
	t.Extra["accepted_accuracy"] = ratio(acceptedCorrect, accepted)
	t.DecisionThreshold = h.Thresholds.Classify.Accept
	t.finalize()
	return t
}

func (h *Harness) runClassifyClaude(ctx context.Context) TaskResult {
	t := newTask("classify", "claude", h.ClaudeKind)
	t.ObservationsAreDecision = true
	for _, a := range h.DS.Articles {
		label, ok := h.DS.ArticleLabels[a.ID]
		if !ok {
			continue
		}
		t.N++
		res, err := h.Claude.Complete(ctx, claudeClassifyJob(a))
		t.addCall(res.Latency, res.LastAttemptLatency, res.CostUSD, res.Usage, res.Model)
		if err != nil {
			if errors.Is(err, llm.ErrRefusal) {
				t.Refusals++
			} else {
				t.addError(err)
			}
			t.addItem(false, false)
			continue
		}
		var out claudeClassifyOut
		if err := res.Decode(&out); err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		if out.Category == "" {
			t.addItem(false, false)
			continue
		}
		correct := out.Category == label.Category
		conf := clamp01(out.Confidence)
		t.addItem(true, correct)
		t.Observations = append(t.Observations, Observation{ID: a.ID, P: conf, Correct: correct})
	}
	t.DecisionThreshold = 0 // self-reported answer confidence, not a threshold
	t.finalize()
	return t
}

func (h *Harness) runPlotJev(ctx context.Context) TaskResult {
	t := newTask("plot", "jev", h.JevKind)
	var precN, precOK, fallback int
	var tp, fp, fn int
	for _, a := range h.DS.Articles {
		label, ok := h.DS.ArticleLabels[a.ID]
		if !ok {
			continue
		}
		t.N++
		resp, err := h.Jev.Decide(ctx, decide.PlotRequest(decideArticle(a), decideCandidates(a.Candidates)))
		t.addJevCall(resp)
		if err != nil {
			t.addError(err)
			t.addItem(false, false)
			fallback++
			t.FallbackPlot = append(t.FallbackPlot, FallbackPlot{ID: a.ID, Structural: true})
			continue
		}
		res := decide.CombinePlot(resp, decideCandidates(a.Candidates), h.Thresholds)
		t.addConcentration(res.Concentration)
		if !res.Answered {
			if refused(resp, "primary") {
				t.Refusals++
			}
			t.addItem(false, false)
			fallback++
			t.FallbackPlot = append(t.FallbackPlot, FallbackPlot{ID: a.ID, Structural: true})
			continue
		}
		pred := res.PrimaryID
		if pred == "" {
			pred = "none"
		}
		correct := pred == label.PrimaryPlace
		t.addItem(true, correct)
		structural := res.PrimaryID == "" || !res.HasProbability || (res.PrimaryID != "" && res.Precision == "")
		t.FallbackPlot = append(t.FallbackPlot, FallbackPlot{
			ID: a.ID, P: res.PrimaryP, OK: res.HasProbability, Structural: structural,
		})
		if res.HasProbability && pred != "none" {
			t.Observations = append(t.Observations, Observation{ID: a.ID, P: res.PrimaryP, Correct: correct})
		}
		if res.Accepted {
			precN++
			if res.Precision == label.Precision {
				precOK++
			}
		}
		if res.Fallback {
			fallback++
		}
		p, f, n := setCounts(res.Parties, label.Parties)
		tp += p
		fp += f
		fn += n
	}
	seenPlot := make(map[string]bool, len(t.FallbackPlot))
	for _, f := range t.FallbackPlot {
		seenPlot[f.ID] = true
	}
	for id := range h.DS.ArticleLabels {
		if !seenPlot[id] {
			t.N++
			t.addItem(false, false)
			fallback++
			t.FallbackPlot = append(t.FallbackPlot, FallbackPlot{ID: id, Structural: true})
		}
	}
	t.Extra["fallback_rate"] = ratio(fallback, t.N)
	t.Extra["precision_accuracy"] = ratio(precOK, precN)
	t.Extra["party_f1"] = f1(tp, fp, fn)
	t.Extra["party_precision"] = ratio(tp, tp+fp)
	t.Extra["party_recall"] = ratio(tp, tp+fn)
	t.DecisionThreshold = h.Thresholds.Plot.Primary
	t.finalize()
	return t
}

func (h *Harness) runPlotClaude(ctx context.Context) TaskResult {
	t := newTask("plot", "claude", h.ClaudeKind)
	t.ObservationsAreDecision = true
	var precN, precOK int
	var tp, fp, fn int
	precisions := map[string]bool{"country": true, "region": true, "city": true, "street": true, "point": true}
	for _, a := range h.DS.Articles {
		label, ok := h.DS.ArticleLabels[a.ID]
		if !ok {
			continue
		}
		t.N++
		job, keyToID := claudePlotJob(a)
		res, err := h.Claude.Complete(ctx, job)
		t.addCall(res.Latency, res.LastAttemptLatency, res.CostUSD, res.Usage, res.Model)
		if err != nil {
			if errors.Is(err, llm.ErrRefusal) {
				t.Refusals++
			} else {
				t.addError(err)
			}
			t.addItem(false, false)
			continue
		}
		var out claudePlotOut
		if err := res.Decode(&out); err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		pred := "none"
		if out.Primary != "" && out.Primary != "none" {
			pred = keyToID[out.Primary]
			if pred == "" {
				t.addError(fmt.Errorf("claude plot returned unknown candidate %q", out.Primary))
				t.addItem(false, false)
				continue
			}
		}
		correct := pred == label.PrimaryPlace
		conf := clamp01(out.Confidence)
		t.addItem(true, correct)
		if pred != "none" {
			t.Observations = append(t.Observations, Observation{ID: a.ID, P: conf, Correct: correct})
		}
		if pred != "none" && precisions[out.Precision] {
			precN++
			if out.Precision == label.Precision {
				precOK++
			}
		}
		parties := make([]string, 0, len(out.Parties))
		for _, k := range out.Parties {
			if id, ok := keyToID[k]; ok {
				parties = append(parties, id)
			}
		}
		p, f, n := setCounts(parties, label.Parties)
		tp += p
		fp += f
		fn += n
	}
	t.Extra["precision_accuracy"] = ratio(precOK, precN)
	t.Extra["party_f1"] = f1(tp, fp, fn)
	t.Extra["party_precision"] = ratio(tp, tp+fp)
	t.Extra["party_recall"] = ratio(tp, tp+fn)
	t.DecisionThreshold = 0 // self-reported answer confidence, not a threshold
	t.finalize()
	return t
}

func (h *Harness) runRelateJev(ctx context.Context) TaskResult {
	t := newTask("relate", "jev", h.JevKind)
	var tpRel, fpRel, fnRel, acceptedN, acceptedOK int
	var dirN, dirOK, strengthN int
	var strengthAbs float64
	duplicates := 0
	for _, p := range h.DS.Pairs {
		label, ok := h.DS.PairLabels[p.ID]
		if !ok {
			continue
		}
		if label.Duplicate {
			duplicates++
			continue
		}
		a, okA := findArticle(h.DS.Articles, p.A)
		b, okB := findArticle(h.DS.Articles, p.B)
		if !okA || !okB {
			t.N++
			t.addError(fmt.Errorf("pair %s references a missing article", p.ID))
			t.addItem(false, false)
			continue
		}
		t.N++
		resp, err := h.Jev.Decide(ctx, decide.RelateRequest(decideStory(a), decideStory(b)))
		t.addJevCall(resp)
		if err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		rel, relOK := resp.Answer("related", decide.TypeNoul)
		if !relOK {
			if refused(resp, "related") {
				t.Refusals++
			}
			t.addItem(false, false)
			continue
		}
		relatedP := rel.Noul
		typ, typOK := resp.Answer("type", decide.TypeChoice)
		typeUsable := typOK && validRelationType(typ.Choice)
		typKey := ""
		if typeUsable {
			typKey = typ.Choice
		}
		t.Binary = append(t.Binary, BinaryObservation{ID: p.ID, P: relatedP, Gold: label.Related})
		t.Relations = append(t.Relations, RelationObservation{
			ID: p.ID, RelatedP: relatedP, Type: typKey, TypeAnswered: typeUsable,
			GoldRelated: label.Related, GoldType: label.Type,
		})

		accepted := relatedP >= h.Thresholds.Relate.Accept && typeUsable && typKey != "none"
		correct := accepted == label.Related && (!label.Related || (typeUsable && typKey == label.Type))
		t.addItem(true, correct)
		if accepted {
			acceptedN++
			if label.Related && typeUsable && typKey == label.Type {
				acceptedOK++
			}
		}
		switch {
		case accepted && label.Related:
			tpRel++
		case accepted && !label.Related:
			fpRel++
		case !accepted && label.Related:
			fnRel++
		}
		if accepted {
			dirN++
			dir := "unclear"
			if d, ok := resp.Answer("direction", decide.TypeChoice); ok && d.Choice != "" {
				dir = d.Choice
			}
			if dir == label.Direction {
				dirOK++
			}
			if score, ok := resp.Answer("strength", decide.TypeScore); ok {
				strengthAbs += abs(score.Score - float64(label.Strength))
				strengthN++
			}
		}
	}
	t.Extra["related_f1"] = f1(tpRel, fpRel, fnRel)
	t.Extra["related_precision"] = ratio(tpRel, tpRel+fpRel)
	t.Extra["related_recall"] = ratio(tpRel, tpRel+fnRel)
	t.Extra["direction_accuracy"] = ratio(dirOK, dirN)
	t.Extra["accepted_type_accuracy"] = ratio(acceptedOK, acceptedN)
	t.Extra["accept_rate"] = ratio(acceptedN, t.N)
	t.Extra["duplicate_pairs_skipped"] = float64(duplicates)
	if strengthN > 0 {
		t.Extra["strength_mae"] = strengthAbs / float64(strengthN)
	}
	t.DecisionThreshold = h.Thresholds.Relate.Accept
	t.finalize()
	return t
}

func (h *Harness) runRelateClaude(ctx context.Context) TaskResult {
	t := newTask("relate", "claude", h.ClaudeKind)
	var tpRel, fpRel, fnRel int
	var dirN, dirOK, strengthN int
	var strengthAbs float64
	duplicates := 0
	for _, p := range h.DS.Pairs {
		label, ok := h.DS.PairLabels[p.ID]
		if !ok {
			continue
		}
		if label.Duplicate {
			duplicates++
			continue
		}
		a, okA := findArticle(h.DS.Articles, p.A)
		b, okB := findArticle(h.DS.Articles, p.B)
		if !okA || !okB {
			t.N++
			t.addError(fmt.Errorf("pair %s references a missing article", p.ID))
			t.addItem(false, false)
			continue
		}
		t.N++
		res, err := h.Claude.Complete(ctx, claudeRelateJob(a, b))
		t.addCall(res.Latency, res.LastAttemptLatency, res.CostUSD, res.Usage, res.Model)
		if err != nil {
			if errors.Is(err, llm.ErrRefusal) {
				t.Refusals++
			} else {
				t.addError(err)
			}
			t.addItem(false, false)
			continue
		}
		var out claudeRelateOut
		if err := res.Decode(&out); err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		typ := out.Type
		if typ == "" {
			typ = "none"
		}
		related := out.Related && typ != "none"
		correct := related == label.Related && (!label.Related || typ == label.Type)
		t.addItem(true, correct)
		conf := clamp01(out.Confidence)
		// Claude's confidence already means P(this answer is correct):
		// relatedness AND type for the relate question. Never flip it.
		t.DecisionObservations = append(t.DecisionObservations,
			Observation{ID: p.ID, P: conf, Correct: correct})
		switch {
		case related && label.Related:
			tpRel++
		case related && !label.Related:
			fpRel++
		case !related && label.Related:
			fnRel++
		}
		if related {
			dirN++
			if out.Direction == label.Direction {
				dirOK++
			}
		}
		if label.Related {
			strengthAbs += abs(float64(out.Strength) - float64(label.Strength))
			strengthN++
		}
	}
	t.Extra["related_f1"] = f1(tpRel, fpRel, fnRel)
	t.Extra["related_precision"] = ratio(tpRel, tpRel+fpRel)
	t.Extra["related_recall"] = ratio(tpRel, tpRel+fnRel)
	t.Extra["direction_accuracy"] = ratio(dirOK, dirN)
	t.Extra["duplicate_pairs_skipped"] = float64(duplicates)
	if strengthN > 0 {
		t.Extra["strength_mae"] = strengthAbs / float64(strengthN)
	}
	t.DecisionThreshold = 0 // self-reported answer confidence
	t.finalize()
	return t
}

func (h *Harness) runMatchJev(ctx context.Context) TaskResult {
	t := newTask("match", "jev", h.JevKind)
	var tp, fp, fn int
	for _, m := range h.DS.Matches {
		a, ok := findArticle(h.DS.Articles, m.ArticleID)
		if !ok {
			t.N++
			t.addError(fmt.Errorf("match %s references a missing article", m.ID))
			t.addItem(false, false)
			continue
		}
		t.N++
		resp, err := h.Jev.Decide(ctx, decide.MatchRequest(decideStory(a), m.Prompt))
		t.addJevCall(resp)
		if err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		res := decide.CombineMatch(resp, h.Thresholds)
		if !res.Answered {
			if refused(resp, "match") {
				t.Refusals++
			}
			t.addItem(false, false)
			continue
		}
		correct := res.Accepted == m.Relevant
		t.addItem(true, correct)
		t.Binary = append(t.Binary, BinaryObservation{ID: m.ID, P: res.MatchP, Gold: m.Relevant})
		switch {
		case res.Accepted && m.Relevant:
			tp++
		case res.Accepted && !m.Relevant:
			fp++
		case !res.Accepted && m.Relevant:
			fn++
		}
	}
	t.Extra["match_f1"] = f1(tp, fp, fn)
	t.Extra["match_precision"] = ratio(tp, tp+fp)
	t.Extra["match_recall"] = ratio(tp, tp+fn)
	t.DecisionThreshold = h.Thresholds.Match.Strictness
	t.finalize()
	return t
}

func (h *Harness) runMatchClaude(ctx context.Context) TaskResult {
	t := newTask("match", "claude", h.ClaudeKind)
	var tp, fp, fn int
	for _, m := range h.DS.Matches {
		a, ok := findArticle(h.DS.Articles, m.ArticleID)
		if !ok {
			t.N++
			t.addError(fmt.Errorf("match %s references a missing article", m.ID))
			t.addItem(false, false)
			continue
		}
		t.N++
		res, err := h.Claude.Complete(ctx, claudeMatchJob(m, a))
		t.addCall(res.Latency, res.LastAttemptLatency, res.CostUSD, res.Usage, res.Model)
		if err != nil {
			if errors.Is(err, llm.ErrRefusal) {
				t.Refusals++
			} else {
				t.addError(err)
			}
			t.addItem(false, false)
			continue
		}
		var out claudeMatchOut
		if err := res.Decode(&out); err != nil {
			t.addError(err)
			t.addItem(false, false)
			continue
		}
		correct := out.Match == m.Relevant
		t.addItem(true, correct)
		conf := clamp01(out.Confidence)
		// Confidence is P(this answer is correct); do not flip it for a
		// predicted no.
		t.DecisionObservations = append(t.DecisionObservations,
			Observation{ID: m.ID, P: conf, Correct: correct})
		switch {
		case out.Match && m.Relevant:
			tp++
		case out.Match && !m.Relevant:
			fp++
		case !out.Match && m.Relevant:
			fn++
		}
	}
	t.Extra["match_f1"] = f1(tp, fp, fn)
	t.Extra["match_precision"] = ratio(tp, tp+fp)
	t.Extra["match_recall"] = ratio(tp, tp+fn)
	t.DecisionThreshold = 0 // self-reported answer confidence
	t.finalize()
	return t
}

// Selections is the threshold selection outcome.
type Selections struct {
	Skipped       bool              `json:"skipped"`
	Reason        string            `json:"reason,omitempty"`
	Classify      Selection         `json:"classify"`
	Plot          Selection         `json:"plot"`
	Relate        Selection         `json:"relate"`
	HasProjection bool              `json:"has_projection"`
	FallbackRate  float64           `json:"projected_fallback_rate"`
	FallbackMet   bool              `json:"projected_fallback_met"`
	Applied       config.Thresholds `json:"applied"`
}

// SelectRelationThreshold recomputes the accepted link set from raw answers
// at every candidate threshold, including false positives on gold-unrelated
// pairs. Accuracy is accepted type accuracy: a gold-unrelated pair accepted as
// a link is wrong, whatever its type.
func SelectRelationThreshold(obs []RelationObservation, target float64, minAccepted int) Selection {
	grid := ThresholdGrid()
	if minAccepted < 1 {
		minAccepted = 1
	}
	sel := Selection{Threshold: grid[len(grid)-1], Target: target, Insufficient: true}
	var bestAcc Selection
	foundAcc := false
	for _, t := range grid {
		accepted, correct := 0, 0
		for _, o := range obs {
			if !o.TypeAnswered || o.Type == "" || o.Type == "none" {
				continue
			}
			if o.RelatedP < t {
				continue
			}
			accepted++
			if o.GoldRelated && o.Type == o.GoldType {
				correct++
			}
		}
		if accepted < minAccepted {
			continue
		}
		sel.Insufficient = false
		s := Selection{Threshold: t, Target: target, Accepted: accepted}
		if len(obs) > 0 {
			s.Coverage = float64(accepted) / float64(len(obs))
		}
		s.Accuracy = float64(correct) / float64(accepted)
		if s.Accuracy >= target {
			s.Met = true
			return s
		}
		if !foundAcc || s.Accuracy > bestAcc.Accuracy ||
			(s.Accuracy == bestAcc.Accuracy && s.Threshold < bestAcc.Threshold) {
			bestAcc = s
			foundAcc = true
		}
	}
	if foundAcc {
		return bestAcc
	}
	return sel
}

// SelectThresholds applies the acceptance targets to the jev observations.
// Mock runs never select: mock probabilities are meaningless, so the applied
// thresholds stay at the configured starting values.
func SelectThresholds(run RunResult, base config.Thresholds, mock bool) Selections {
	sel := Selections{Applied: base}
	if mock {
		sel.Skipped = true
		sel.Reason = "mock backends: thresholds stay at the starting values"
		return sel
	}
	classify, okC := run.Find("classify", "jev")
	plot, okP := run.Find("plot", "jev")
	relate, okR := run.Find("relate", "jev")
	if !okC || !okP || !okR {
		sel.Skipped = true
		sel.Reason = "missing jev task results"
		return sel
	}
	if len(classify.Observations) == 0 || len(plot.Observations) == 0 ||
		len(classify.FallbackClassify) == 0 || len(plot.FallbackPlot) == 0 {
		// No usable probabilities: do not select, but still project the
		// fallback at the configured thresholds. Errors and missing answers
		// count as fallback.
		sel.Skipped = true
		sel.Reason = "no usable jev observations to select from"
		sel.HasProjection = len(classify.FallbackClassify) > 0 && len(plot.FallbackPlot) > 0
		sel.FallbackRate = UnionFallbackRate(classify.FallbackClassify, plot.FallbackPlot,
			base.Classify.Accept, base.Plot.Primary)
		sel.FallbackMet = sel.HasProjection && sel.FallbackRate < base.Targets.ClaudeFallbackRate
		return sel
	}
	sel.Plot = SelectThreshold(plot.Observations, base.Targets.PlacePickAccuracy, 10)
	sel.Relate = SelectRelationThreshold(relate.Relations, base.Targets.RelationTypeAccuracy, 10)
	if !sel.Plot.Insufficient {
		sel.Applied.Plot.Primary = sel.Plot.Threshold
	}
	if !sel.Relate.Insufficient {
		sel.Applied.Relate.Accept = sel.Relate.Threshold
	}

	// The classify threshold is chosen to keep the projected Claude fallback
	// rate under target while accepting as much as possible.
	sel.Classify = Selection{Target: base.Targets.ClaudeFallbackRate}
	bestAcc := Selection{Target: base.Targets.ClaudeFallbackRate}
	foundAcc, haveBest := false, false
	plotThreshold := sel.Applied.Plot.Primary
	for _, t := range ThresholdGrid() {
		accepted, correct := 0, 0
		for _, o := range classify.Observations {
			if o.P >= t {
				accepted++
				if o.Correct {
					correct++
				}
			}
		}
		if accepted == 0 {
			continue
		}
		acc := float64(correct) / float64(accepted)
		fallback := UnionFallbackRate(classify.FallbackClassify, plot.FallbackPlot, t, plotThreshold)
		if fallback < base.Targets.ClaudeFallbackRate {
			sel.Classify = Selection{
				Threshold: t, Target: base.Targets.ClaudeFallbackRate,
				Accepted: accepted, Coverage: ratio(accepted, len(classify.Observations)),
				Accuracy: acc, Met: true,
			}
			sel.Applied.Classify.Accept = t
			foundAcc = true
			break
		}
		if !haveBest || acc > bestAcc.Accuracy {
			haveBest = true
			bestAcc = Selection{
				Threshold: t, Target: base.Targets.ClaudeFallbackRate,
				Accepted: accepted, Coverage: ratio(accepted, len(classify.Observations)),
				Accuracy: acc,
			}
		}
	}
	if !foundAcc && bestAcc.Accepted > 0 {
		sel.Classify = bestAcc
		sel.Applied.Classify.Accept = bestAcc.Threshold
	}
	sel.HasProjection = len(classify.FallbackClassify) > 0 && len(plot.FallbackPlot) > 0
	sel.FallbackRate = UnionFallbackRate(classify.FallbackClassify, plot.FallbackPlot,
		sel.Applied.Classify.Accept, sel.Applied.Plot.Primary)
	sel.FallbackMet = sel.HasProjection && sel.FallbackRate < base.Targets.ClaudeFallbackRate
	sel.Applied.Provisional = true
	selectedAt := run.FinishedAt
	if selectedAt.IsZero() {
		selectedAt = time.Now()
	}
	sel.Applied.SelectedAt = selectedAt.UTC().Format("2006-01-02")
	return sel
}

func refused(resp decide.Response, key string) bool {
	a, ok := resp.Answers[key]
	return ok && a.Refusal
}

func decideArticle(a Article) decide.Article {
	return decide.Article{ID: a.ID, Title: a.Title, Lede: a.Snippet, Source: a.Source}
}

func decideStory(a Article) decide.Story {
	return decide.Story{ID: a.ID, Title: a.Title, Brief: a.Snippet}
}

func decideCandidates(cs []Candidate) []decide.Candidate {
	out := make([]decide.Candidate, 0, len(cs))
	for _, c := range cs {
		out = append(out, decide.Candidate{
			ID: c.ID, Name: c.Name, Admin: c.Admin, Country: c.Country, Population: c.Population,
		})
	}
	return out
}

func findArticle(articles []Article, id string) (Article, bool) {
	for _, a := range articles {
		if a.ID == id {
			return a, true
		}
	}
	return Article{}, false
}

func ratio(num, den int) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func f1(tp, fp, fn int) float64 {
	prec := ratio(tp, tp+fp)
	rec := ratio(tp, tp+fn)
	if prec+rec == 0 {
		return 0
	}
	return 2 * prec * rec / (prec + rec)
}

func setCounts(pred, gold []string) (tp, fp, fn int) {
	p := make(map[string]struct{}, len(pred))
	g := make(map[string]struct{}, len(gold))
	for _, s := range pred {
		p[s] = struct{}{}
	}
	for _, s := range gold {
		g[s] = struct{}{}
	}
	for s := range p {
		if _, ok := g[s]; ok {
			tp++
		} else {
			fp++
		}
	}
	for s := range g {
		if _, ok := p[s]; !ok {
			fn++
		}
	}
	return tp, fp, fn
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func validRelationType(s string) bool {
	switch s {
	case "causal", "reaction", "thematic", "none":
		return true
	default:
		return false
	}
}

// LabelSummary renders the label sources for the report.
func LabelSummary(sources []LabelSource) string {
	if len(sources) == 0 {
		return "no labels"
	}
	parts := make([]string, 0, len(sources))
	for _, s := range sources {
		prov := ""
		if s.Provisional {
			prov = ", provisional"
		}
		parts = append(parts, fmt.Sprintf("%s%s (articles %d, pairs %d)", s.Labeler, prov, s.Articles, s.Pairs))
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}
