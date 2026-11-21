package eval

import (
	"fmt"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
)

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("i%02d", i)
	}
	return out
}

func fcFrom(idList []string, p func(i int) float64) []FallbackClassify {
	out := make([]FallbackClassify, len(idList))
	for i, id := range idList {
		pv := p(i)
		out[i] = FallbackClassify{ID: id, P: pv, OK: pv > 0}
	}
	return out
}

func fpFrom(idList []string, p func(i int) float64) []FallbackPlot {
	out := make([]FallbackPlot, len(idList))
	for i, id := range idList {
		pv := p(i)
		out[i] = FallbackPlot{ID: id, P: pv, OK: pv > 0, Structural: pv == 0}
	}
	return out
}

func flat(v float64) func(int) float64 { return func(int) float64 { return v } }

func relations(prefix string, n, correct int, p float64, typ string, goldRelated bool) []RelationObservation {
	out := make([]RelationObservation, n)
	for i := range out {
		goldType := typ
		if i >= correct {
			goldType = "reaction"
		}
		out[i] = RelationObservation{
			ID: fmt.Sprintf("%s%02d", prefix, i), RelatedP: p, Type: typ, TypeAnswered: true,
			GoldRelated: goldRelated, GoldType: goldType,
		}
	}
	return out
}

func liveRun(classifyObs []Observation, fc []FallbackClassify, plotObs []Observation, fp []FallbackPlot, rel []RelationObservation) RunResult {
	return RunResult{
		JevKind: "live", ClaudeKind: "live",
		FinishedAt: time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC),
		Tasks: []TaskResult{
			{Task: "classify", Backend: "jev", Observations: classifyObs, FallbackClassify: fc},
			{Task: "plot", Backend: "jev", Observations: plotObs, FallbackPlot: fp},
			{Task: "relate", Backend: "jev", Relations: rel},
		},
	}
}

func TestSelectThresholdsMockKeepsStartingValues(t *testing.T) {
	base := config.DefaultThresholds()
	for _, kinds := range [][2]string{{"mock", "mock"}, {"mock", "live"}, {"live", "mock"}} {
		run := liveRun(repeat("c", 20, 20, 0.9), fcFrom(ids(20), flat(0.9)), repeat("p", 20, 20, 0.9), fpFrom(ids(20), flat(0.9)), relations("r", 20, 20, 0.9, "causal", true))
		run.JevKind, run.ClaudeKind = kinds[0], kinds[1]
		sel := SelectThresholds(run, base, run.Mock())
		if !sel.Skipped || sel.Reason == "" {
			t.Fatalf("%v: selection = %+v", kinds, sel)
		}
		if sel.Applied != base {
			t.Errorf("%v: applied thresholds changed in a mock/mixed run:\n got %+v\nwant %+v", kinds, sel.Applied, base)
		}
	}
}

func TestSelectThresholdsMissingTasksSkipped(t *testing.T) {
	base := config.DefaultThresholds()
	run := RunResult{JevKind: "live", Tasks: []TaskResult{{Task: "classify", Backend: "jev"}}}
	sel := SelectThresholds(run, base, false)
	if !sel.Skipped || sel.Applied != base || sel.HasProjection {
		t.Errorf("selection = %+v", sel)
	}
}

func TestSelectThresholdsAppliesTargets(t *testing.T) {
	base := config.DefaultThresholds()
	if base.Targets.PlacePickAccuracy != 0.90 || base.Targets.RelationTypeAccuracy != 0.85 || base.Targets.ClaudeFallbackRate != 0.15 {
		t.Fatalf("targets = %+v", base.Targets)
	}
	plot := concat(repeat("p", 10, 5, 0.55), repeat("q", 10, 10, 0.85))
	// 10 false links at P .55, then 12 true links at P .80 of which 11 have the right type.
	rel := append(relations("fp", 10, 0, 0.55, "causal", false), relations("tp", 12, 11, 0.80, "causal", true)...)
	idList := ids(20)
	sel := SelectThresholds(liveRun(repeat("c", 20, 18, 0.9), fcFrom(idList, flat(0.9)), plot, fpFrom(idList, flat(0.9)), rel), base, false)
	if sel.Skipped {
		t.Fatalf("live selection skipped: %+v", sel)
	}
	if !sel.Plot.Met || sel.Plot.Threshold != 0.6 || sel.Applied.Plot.Primary != 0.6 || sel.Plot.Target != 0.90 {
		t.Errorf("plot = %+v applied %v", sel.Plot, sel.Applied.Plot.Primary)
	}
	// At 0.50 and 0.55 the false links are accepted (11/22 correct); from 0.60 only the true links are (11/12).
	if !sel.Relate.Met || sel.Relate.Threshold != 0.6 || sel.Applied.Relate.Accept != 0.6 || sel.Relate.Target != 0.85 || sel.Relate.Accepted != 12 {
		t.Errorf("relate = %+v applied %v", sel.Relate, sel.Applied.Relate.Accept)
	}
	if !sel.Classify.Met || sel.Classify.Target != 0.15 || !sel.FallbackMet || !sel.HasProjection || sel.FallbackRate != 0 {
		t.Errorf("classify = %+v fallback %v met=%v", sel.Classify, sel.FallbackRate, sel.FallbackMet)
	}
	if sel.Applied.Plot.Party != base.Plot.Party || sel.Applied.Match.Strictness != base.Match.Strictness || sel.Applied.Targets != base.Targets {
		t.Errorf("unrelated thresholds changed: %+v", sel.Applied)
	}
	if !sel.Applied.Provisional || sel.Applied.SelectedAt != "2026-12-01" {
		t.Errorf("provisional=%v selected_at=%q", sel.Applied.Provisional, sel.Applied.SelectedAt)
	}
	if err := sel.Applied.Validate(); err != nil {
		t.Errorf("selected thresholds are invalid: %v", err)
	}
}

func TestSelectThresholdsTargetNotMetOrInsufficientKeepsBase(t *testing.T) {
	base := config.DefaultThresholds()
	plot := concat(repeat("p", 10, 5, 0.6), repeat("q", 10, 6, 0.9))
	rel := relations("r", 5, 5, 0.9, "causal", true) // too few
	idList := ids(10)
	sel := SelectThresholds(liveRun(repeat("c", 10, 9, 0.9), fcFrom(idList, flat(0.9)), plot, fpFrom(idList, flat(0.9)), rel), base, false)
	if sel.Plot.Met || sel.Plot.Insufficient {
		t.Errorf("plot = %+v", sel.Plot)
	}
	if sel.Applied.Plot.Primary != sel.Plot.Threshold {
		t.Errorf("best-effort plot threshold not applied: %v vs %v", sel.Applied.Plot.Primary, sel.Plot.Threshold)
	}
	if !sel.Relate.Insufficient || sel.Applied.Relate.Accept != base.Relate.Accept {
		t.Errorf("insufficient relate must keep the starting value: %+v applied %v", sel.Relate, sel.Applied.Relate.Accept)
	}
}

func TestSelectThresholdsClassifyKeepsUnionFallbackUnderTarget(t *testing.T) {
	base := config.DefaultThresholds()
	idList := ids(20)
	plot := repeat("p", 10, 10, 0.9)
	rel := relations("r", 12, 12, 0.9, "causal", true)
	classObs := repeat("c", 18, 18, 0.9)

	// 2 of 20 classify items unanswered: 10% < 15%.
	fc := fcFrom(idList, func(i int) float64 {
		if i < 2 {
			return 0
		}
		return 0.9
	})
	good := fpFrom(idList, flat(0.9))
	sel := SelectThresholds(liveRun(classObs, fc, plot, good, rel), base, false)
	if !sel.Classify.Met || sel.Applied.Classify.Accept != 0.5 || !sel.FallbackMet || !near(sel.FallbackRate, 0.10) {
		t.Errorf("under target: classify=%+v rate=%v", sel.Classify, sel.FallbackRate)
	}

	// 2 further items have no place (structural): the union (4/20 = 20%) is over target.
	weak := fpFrom(idList, func(i int) float64 {
		if i == 2 || i == 3 {
			return 0
		}
		return 0.9
	})
	sel = SelectThresholds(liveRun(classObs, fc, plot, weak, rel), base, false)
	if sel.FallbackMet || !near(sel.FallbackRate, 0.20) || sel.Classify.Met {
		t.Errorf("union over target: met=%v rate=%v classify=%+v", sel.FallbackMet, sel.FallbackRate, sel.Classify)
	}
}

func TestSelectThresholdsUnmetClassifyPicksMostAccurate(t *testing.T) {
	base := config.DefaultThresholds()
	idList := ids(20)
	// 20% of items have P .3, so no threshold keeps the fallback under 15%.
	var classifyObs []Observation
	for i, id := range idList {
		switch {
		case i < 4:
			classifyObs = append(classifyObs, Observation{ID: id, P: 0.3})
		case i < 12:
			classifyObs = append(classifyObs, Observation{ID: id, P: 0.6, Correct: i%2 == 0})
		default:
			classifyObs = append(classifyObs, Observation{ID: id, P: 0.9, Correct: true})
		}
	}
	fc := make([]FallbackClassify, len(classifyObs))
	for i, o := range classifyObs {
		fc[i] = FallbackClassify{ID: o.ID, P: o.P, OK: true}
	}
	sel := SelectThresholds(liveRun(classifyObs, fc, repeat("p", 10, 10, 0.9), fpFrom(idList, flat(0.9)), relations("r", 12, 12, 0.9, "causal", true)), base, false)
	if sel.Classify.Met || sel.FallbackMet {
		t.Fatalf("expected the fallback target to be missed: %+v", sel)
	}
	if sel.Classify.Threshold != 0.65 || !near(sel.Classify.Accuracy, 1) || sel.Applied.Classify.Accept != 0.65 {
		t.Errorf("classify = %+v applied %v", sel.Classify, sel.Applied.Classify.Accept)
	}
}

// Fallback projection with no usable probabilities: selection is skipped but
// the projection at the configured thresholds still counts every labelled item.
func TestSelectThresholdsProjectsFallbackWithoutObservations(t *testing.T) {
	base := config.DefaultThresholds()
	idList := ids(10)
	// All no-place: the place side is structural for everyone.
	plotFB := make([]FallbackPlot, len(idList))
	for i, id := range idList {
		plotFB[i] = FallbackPlot{ID: id, P: 0.95, OK: true, Structural: true}
	}
	run := liveRun(repeat("c", 10, 10, 0.95), fcFrom(idList, flat(0.95)), nil, plotFB, nil)
	sel := SelectThresholds(run, base, false)
	if !sel.Skipped || !sel.HasProjection || sel.FallbackRate != 1 || sel.FallbackMet {
		t.Errorf("all no-place: %+v", sel)
	}
	if sel.Applied != base {
		t.Error("nothing is selected without observations")
	}
	// A projection exists only when both sides have records.
	run = liveRun(nil, nil, nil, nil, nil)
	sel = SelectThresholds(run, base, false)
	if !sel.Skipped || sel.HasProjection || sel.FallbackMet {
		t.Errorf("no records: %+v", sel)
	}
}

func TestSelectRelationThresholdCountsGoldUnrelatedAsWrong(t *testing.T) {
	// 10 gold-related causal pairs and 10 gold-unrelated pairs, all P(related)=.99 and typed causal.
	obs := append(relations("g", 10, 10, 0.99, "causal", true), relations("u", 10, 0, 0.99, "causal", false)...)
	s := SelectRelationThreshold(obs, 0.85, 1)
	if s.Met || s.Accepted != 20 || !near(s.Accuracy, 0.5) {
		t.Errorf("selection = %+v, want accuracy 0.5 (gold-unrelated links are wrong whatever their type)", s)
	}
	// Types the model never answered or called none are not links.
	none := []RelationObservation{
		{ID: "a", RelatedP: 0.99, Type: "none", TypeAnswered: true, GoldRelated: false},
		{ID: "b", RelatedP: 0.99, TypeAnswered: false, GoldRelated: true, GoldType: "causal"},
	}
	if s := SelectRelationThreshold(none, 0.85, 1); !s.Insufficient {
		t.Errorf("none/unanswered must not be accepted: %+v", s)
	}
	if s := SelectRelationThreshold(nil, 0.85, 1); !s.Insufficient {
		t.Errorf("no observations: %+v", s)
	}
	// The wrong-type case: a gold-related pair with the wrong type is wrong too.
	wrong := relations("w", 10, 0, 0.9, "causal", true)
	if s := SelectRelationThreshold(wrong, 0.5, 1); s.Met || s.Accuracy != 0 {
		t.Errorf("wrong types: %+v", s)
	}
}
