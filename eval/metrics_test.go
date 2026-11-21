package eval

import (
	"math"
	"testing"
	"time"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// repeat builds n observations with the given confidence, k of them correct.
func repeat(prefix string, n, k int, conf float64) []Observation {
	out := make([]Observation, n)
	for i := range out {
		out[i] = Observation{ID: prefix + string(rune('a'+i%26)) + string(rune('0'+i/26)), P: conf, Correct: i < k}
	}
	return out
}

func concat(parts ...[]Observation) []Observation {
	var out []Observation
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestCalibrationPerfect(t *testing.T) {
	obs := concat(repeat("a", 100, 85, 0.85), repeat("b", 100, 55, 0.55))
	bins, ece := Calibrate(obs)
	if len(bins) != 10 {
		t.Fatalf("bins = %d", len(bins))
	}
	if !near(ece, 0) {
		t.Errorf("ECE = %v, want ~0", ece)
	}
	if bins[8].N != 100 || !near(bins[8].MeanConf, 0.85) || !near(bins[8].Accuracy, 0.85) {
		t.Errorf("bin 8 = %+v", bins[8])
	}
	if !near(bins[8].Lo, 0.8) || !near(bins[8].Hi, 0.9) {
		t.Errorf("bin bounds = %v..%v", bins[8].Lo, bins[8].Hi)
	}
}

func TestCalibrationOverAndUnderConfident(t *testing.T) {
	over := repeat("o", 100, 50, 0.95)
	if _, ece := Calibrate(over); !near(ece, 0.45) {
		t.Errorf("overconfident ECE = %v, want 0.45", ece)
	}
	under := repeat("u", 100, 100, 0.55)
	if _, ece := Calibrate(under); !near(ece, 0.45) {
		t.Errorf("underconfident ECE = %v, want 0.45", ece)
	}
	// ECE weights bins by their share of the data.
	mixed := concat(repeat("m", 90, 90, 0.95), repeat("n", 10, 0, 0.95), repeat("p", 100, 100, 0.15))
	_, ece := Calibrate(mixed)
	want := 100.0/200*0.05 + 100.0/200*0.85
	if !near(ece, want) {
		t.Errorf("weighted ECE = %v, want %v", ece, want)
	}
}

func TestCalibrationEmptyBinsAndEdges(t *testing.T) {
	obs := []Observation{{P: 0.25, Correct: true}, {P: 0.26, Correct: false}}
	bins, ece := Calibrate(obs)
	empty := 0
	for _, b := range bins {
		if b.N == 0 {
			empty++
			if b.MeanConf != 0 || b.Accuracy != 0 {
				t.Errorf("an empty bin reports values: %+v", b)
			}
		}
	}
	if empty != 9 {
		t.Errorf("empty bins = %d, want 9", empty)
	}
	if !near(ece, math.Abs(0.5-0.255)) {
		t.Errorf("ECE = %v", ece)
	}
	// Confidence 1.0 lands in the last bin; out-of-range values clamp.
	bins, _ = Calibrate([]Observation{{P: 1, Correct: true}, {P: -0.2}, {P: 1.7}})
	if bins[9].N != 2 || bins[0].N != 1 {
		t.Errorf("edge bins: first=%d last=%d", bins[0].N, bins[9].N)
	}
	// No observations.
	bins, ece = Calibrate(nil)
	if len(bins) != 10 || ece != 0 {
		t.Errorf("empty input: %d bins, ece %v", len(bins), ece)
	}
}

func TestAccuracyAtCoverage(t *testing.T) {
	obs := concat(repeat("a", 10, 5, 0.6), repeat("b", 10, 10, 0.9))
	pts := AccuracyAtCoverage(obs, []float64{0.5, 0.6, 0.7, 0.9, 0.95})
	want := []struct {
		accepted int
		coverage float64
		acc      float64
	}{{20, 1, 0.75}, {20, 1, 0.75}, {10, 0.5, 1}, {10, 0.5, 1}, {0, 0, 0}}
	for i, w := range want {
		p := pts[i]
		if p.Accepted != w.accepted || !near(p.Coverage, w.coverage) || !near(p.Accuracy, w.acc) {
			t.Errorf("point %d (t=%v) = %+v, want %+v", i, p.Threshold, p, w)
		}
	}
	if got := AccuracyAtCoverage(nil, []float64{0.5}); got[0].Coverage != 0 || got[0].Accuracy != 0 {
		t.Errorf("empty input = %+v", got)
	}
	if len(AccuracyAtCoverage(obs, nil)) != 0 {
		t.Error("no thresholds, no points")
	}
}

func TestThresholdGrid(t *testing.T) {
	g := ThresholdGrid()
	if len(g) != 10 || g[0] != 0.50 || g[9] != 0.95 {
		t.Fatalf("grid = %v", g)
	}
	for i := 1; i < len(g); i++ {
		if !near(g[i]-g[i-1], 0.05) {
			t.Errorf("step %d = %v", i, g[i]-g[i-1])
		}
	}
	if round2(0.6500000001) != 0.65 {
		t.Error("round2")
	}
}

func TestSelectThresholdPicksLowestThatMeetsTarget(t *testing.T) {
	obs := concat(repeat("a", 10, 5, 0.6), repeat("b", 10, 10, 0.8))
	s := SelectThreshold(obs, 0.9, 1)
	if !s.Met || s.Insufficient || s.Threshold != 0.65 || s.Accepted != 10 || !near(s.Accuracy, 1) || !near(s.Coverage, 0.5) || s.Target != 0.9 {
		t.Errorf("selection = %+v", s)
	}
	// A looser target accepts everything: the lowest grid value.
	if s := SelectThreshold(obs, 0.7, 1); !s.Met || s.Threshold != 0.5 || s.Accepted != 20 {
		t.Errorf("loose target = %+v", s)
	}
}

func TestSelectThresholdBoundaryIsInclusive(t *testing.T) {
	// Confidence exactly at a grid value is accepted by that threshold.
	obs := concat(repeat("a", 10, 10, 0.65), repeat("b", 10, 0, 0.55))
	s := SelectThreshold(obs, 0.95, 1)
	if !s.Met || s.Threshold != 0.6 || s.Accepted != 10 { // 0.65 >= 0.60 accepted, 0.55 not
		t.Errorf("selection = %+v", s)
	}
	obs = concat(repeat("a", 10, 10, 0.65), repeat("b", 10, 0, 0.55))
	if s := SelectThreshold(obs, 0.95, 1); s.Threshold != 0.6 {
		t.Errorf("a confidence between grid points is accepted by the lower one: %+v", s)
	}
	// Accuracy exactly equal to the target meets it.
	obs = repeat("c", 10, 9, 0.8)
	if s := SelectThreshold(obs, 0.9, 1); !s.Met || s.Threshold != 0.5 {
		t.Errorf("accuracy == target: %+v", s)
	}
}

func TestSelectThresholdTargetNotMet(t *testing.T) {
	obs := concat(repeat("a", 10, 5, 0.6), repeat("b", 10, 6, 0.9))
	s := SelectThreshold(obs, 0.95, 1)
	if s.Met || s.Insufficient {
		t.Fatalf("selection = %+v", s)
	}
	// Best accuracy is 0.6, first reached at 0.65 (ties go to the lowest threshold).
	if s.Threshold != 0.65 || !near(s.Accuracy, 0.6) || s.Accepted != 10 {
		t.Errorf("best-effort selection = %+v", s)
	}
}

func TestSelectThresholdMinAccepted(t *testing.T) {
	obs := concat(repeat("a", 10, 5, 0.6), repeat("b", 3, 3, 0.9))
	// With a floor of 5, the 3 confident cases are never enough.
	s := SelectThreshold(obs, 0.9, 5)
	if s.Met || s.Insufficient || s.Threshold != 0.5 {
		t.Errorf("floor 5: %+v", s)
	}
	if s.Accepted != 13 {
		t.Errorf("accepted = %d, want 13", s.Accepted)
	}
	// With no floor they are.
	if s := SelectThreshold(obs, 0.9, 1); !s.Met || s.Threshold != 0.65 {
		t.Errorf("floor 1: %+v", s)
	}
	// A floor of zero behaves as 1.
	if s := SelectThreshold(obs, 0.9, 0); !s.Met || s.Threshold != 0.65 {
		t.Errorf("floor 0: %+v", s)
	}
}

func TestSelectThresholdInsufficient(t *testing.T) {
	s := SelectThreshold(repeat("a", 4, 4, 0.9), 0.9, 10)
	if !s.Insufficient || s.Met || s.Accepted != 0 {
		t.Errorf("too few cases: %+v", s)
	}
	if s.Threshold != 0.95 {
		t.Errorf("insufficient falls back to the grid maximum, got %v", s.Threshold)
	}
	if s := SelectThreshold(nil, 0.9, 1); !s.Insufficient {
		t.Errorf("no observations: %+v", s)
	}
}

func TestProjectedFallbackRate(t *testing.T) {
	obs := concat(repeat("a", 3, 3, 0.4), repeat("b", 7, 7, 0.8))
	if got := ProjectedFallbackRate(obs, 0.5); !near(got, 0.3) {
		t.Errorf("rate = %v", got)
	}
	if got := ProjectedFallbackRate(obs, 0.8); !near(got, 0.3) {
		t.Errorf("a confidence equal to the threshold is accepted: %v", got)
	}
	if got := ProjectedFallbackRate(obs, 0.81); !near(got, 1) {
		t.Errorf("rate = %v", got)
	}
	if ProjectedFallbackRate(nil, 0.5) != 0 {
		t.Error("empty")
	}
}

func fcs(rows ...FallbackClassify) []FallbackClassify { return rows }

func TestUnionFallbackRate(t *testing.T) {
	classify := fcs(
		FallbackClassify{ID: "a", P: 0.6, OK: true}, FallbackClassify{ID: "b", P: 0.9, OK: true}, FallbackClassify{ID: "c", P: 0.9, OK: true},
		FallbackClassify{ID: "d", P: 0.9, OK: true}, FallbackClassify{ID: "e", P: 0.9, OK: true})
	plot := []FallbackPlot{
		{ID: "a", P: 0.9, OK: true}, {ID: "b", P: 0.9, OK: true}, {ID: "c", P: 0.5, OK: true},
		{ID: "d", OK: false, Structural: true}, {ID: "e", P: 0.9, OK: true},
	}
	// a: classify below. b: fine. c: plot below. d: structural (none/missing). e: fine.
	if got := UnionFallbackRate(classify, plot, 0.7, 0.7); !near(got, 3.0/5) {
		t.Errorf("union = %v, want 0.6", got)
	}
	// An item below both thresholds counts once.
	classify[2].P = 0.1
	if got := UnionFallbackRate(classify, plot, 0.7, 0.7); !near(got, 3.0/5) {
		t.Errorf("double-below counted twice: %v", got)
	}
	// Lower thresholds shrink it, but a structural miss always falls back.
	if got := UnionFallbackRate(classify, plot, 0.05, 0.05); !near(got, 1.0/5) {
		t.Errorf("union at 0.05 = %v, want 0.2 (the structural plot item)", got)
	}
	// The classify set is the denominator; a missing plot record falls back.
	if got := UnionFallbackRate(classify, nil, 0.05, 0.05); got != 1 {
		t.Errorf("no plot records: %v, want 1 (missing ids stay in the denominator)", got)
	}
	if UnionFallbackRate(nil, plot, 0.7, 0.7) != 0 {
		t.Error("empty classify set")
	}
	// An unanswered classify item falls back; so does an answer with no usable probability.
	if got := UnionFallbackRate(fcs(FallbackClassify{ID: "x"}), []FallbackPlot{{ID: "x", P: 0.99, OK: true}}, 0.5, 0.5); got != 1 {
		t.Errorf("unanswered classify = %v", got)
	}
	if got := UnionFallbackRate(fcs(FallbackClassify{ID: "x", P: 0.99}), []FallbackPlot{{ID: "x", P: 0.99, OK: true}}, 0.5, 0.5); got != 1 {
		t.Errorf("classify without a usable probability = %v", got)
	}
	// Boundary: P == threshold is accepted.
	if got := UnionFallbackRate(fcs(FallbackClassify{ID: "x", P: 0.7, OK: true}), []FallbackPlot{{ID: "x", P: 0.6, OK: true}}, 0.7, 0.6); got != 0 {
		t.Errorf("P at the threshold must not fall back: %v", got)
	}
}

func TestPercentile(t *testing.T) {
	ms := time.Millisecond
	d := []time.Duration{40 * ms, 10 * ms, 30 * ms, 20 * ms}
	cases := map[float64]time.Duration{0: 10 * ms, 50: 25 * ms, 100: 40 * ms, -5: 10 * ms, 150: 40 * ms}
	for p, want := range cases {
		if got := Percentile(d, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if got := Percentile(d, 95); got < 38*ms || got > 39*ms {
		t.Errorf("p95 = %v, want 38.5ms", got)
	}
	if d[0] != 40*ms {
		t.Error("Percentile must not reorder its input")
	}
	if Percentile(nil, 50) != 0 {
		t.Error("empty")
	}
	if got := Percentile([]time.Duration{7 * ms}, 95); got != 7*ms {
		t.Errorf("single = %v", got)
	}
}

func TestCostPer1k(t *testing.T) {
	if got := CostPer1k(0.5, 250); !near(got, 2) {
		t.Errorf("cost/1k = %v", got)
	}
	if CostPer1k(10, 0) != 0 {
		t.Error("no items")
	}
	if CostPer1k(0, 100) != 0 {
		t.Error("no cost")
	}
}

func TestSetCountsF1Ratio(t *testing.T) {
	tp, fp, fn := setCounts([]string{"a", "b", "b", "c"}, []string{"b", "c", "d"})
	if tp != 2 || fp != 1 || fn != 1 {
		t.Errorf("tp=%d fp=%d fn=%d", tp, fp, fn)
	}
	if !near(f1(2, 1, 1), 2.0/3) {
		t.Errorf("f1 = %v", f1(2, 1, 1))
	}
	if f1(0, 0, 0) != 0 || f1(0, 3, 3) != 0 {
		t.Error("degenerate f1 is 0")
	}
	if ratio(1, 0) != 0 || !near(ratio(1, 4), 0.25) {
		t.Error("ratio")
	}
}
