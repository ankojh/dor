package eval

import (
	"sort"
	"time"
)

// Observation is one selected-answer probability and whether that answer was
// correct. Used for choice tasks (classify, plot) and for Claude's
// self-reported answer confidence.
type Observation struct {
	ID      string
	P       float64
	Correct bool
}

// BinaryObservation is one event probability and the binary truth. Used for
// match (P(match) vs relevant) and relate (P(related) vs a gold link).
type BinaryObservation struct {
	ID   string
	P    float64
	Gold bool
}

// Bin is one reliability bin.
type Bin struct {
	Lo       float64 `json:"lo"`
	Hi       float64 `json:"hi"`
	N        int     `json:"n"`
	MeanConf float64 `json:"mean_confidence"`
	Accuracy float64 `json:"accuracy"`
}

// Calibrate returns equal-width reliability bins and ECE for P vs correctness.
// It is the event-probability calibration for a choice task; for binary tasks
// use CalibrateBinary.
func Calibrate(obs []Observation) ([]Bin, float64) {
	return calibrate(obs)
}

func calibrate(obs []Observation) ([]Bin, float64) {
	const bins = 10
	out := make([]Bin, bins)
	for i := range out {
		out[i] = Bin{Lo: float64(i) / float64(bins), Hi: float64(i+1) / float64(bins)}
	}
	sumConf := make([]float64, bins)
	sumAcc := make([]float64, bins)
	counts := make([]int, bins)
	for _, o := range obs {
		idx := int(o.P * float64(bins))
		if idx >= bins {
			idx = bins - 1
		}
		if idx < 0 {
			idx = 0
		}
		counts[idx]++
		sumConf[idx] += o.P
		if o.Correct {
			sumAcc[idx]++
		}
	}
	ece := 0.0
	total := len(obs)
	for i := range out {
		out[i].N = counts[i]
		if counts[i] == 0 {
			continue
		}
		out[i].MeanConf = sumConf[i] / float64(counts[i])
		out[i].Accuracy = sumAcc[i] / float64(counts[i])
		ece += float64(counts[i]) / float64(total) * abs(out[i].Accuracy-out[i].MeanConf)
	}
	return out, ece
}

// CalibrateBinary returns event-probability calibration (P vs truth) and
// decision-confidence calibration at threshold (p for a predicted yes, 1-p for
// a predicted no).
func CalibrateBinary(obs []BinaryObservation, threshold float64) (eventBins []Bin, eventECE float64, decisionBins []Bin, decisionECE float64) {
	event := make([]Observation, 0, len(obs))
	decision := make([]Observation, 0, len(obs))
	for _, o := range obs {
		event = append(event, Observation{ID: o.ID, P: o.P, Correct: o.Gold})
		yes := o.P >= threshold
		conf := o.P
		if !yes {
			conf = 1 - o.P
		}
		decision = append(decision, Observation{ID: o.ID, P: conf, Correct: yes == o.Gold})
	}
	eventBins, eventECE = calibrate(event)
	decisionBins, decisionECE = calibrate(decision)
	return
}

// CoveragePoint is one accuracy/precision point.
type CoveragePoint struct {
	Threshold float64 `json:"threshold"`
	Coverage  float64 `json:"coverage"`
	Accepted  int     `json:"accepted"`
	Accuracy  float64 `json:"accuracy"`
	Precision float64 `json:"precision,omitempty"`
	Recall    float64 `json:"recall,omitempty"`
	TP        int     `json:"tp,omitempty"`
	FP        int     `json:"fp,omitempty"`
	FN        int     `json:"fn,omitempty"`
}

// AccuracyAtCoverage evaluates a choice observation set at every threshold.
func AccuracyAtCoverage(obs []Observation, thresholds []float64) []CoveragePoint {
	out := make([]CoveragePoint, 0, len(thresholds))
	for _, t := range thresholds {
		accepted, correct := 0, 0
		for _, o := range obs {
			if o.P >= t {
				accepted++
				if o.Correct {
					correct++
				}
			}
		}
		p := CoveragePoint{Threshold: t, Accepted: accepted}
		if len(obs) > 0 {
			p.Coverage = float64(accepted) / float64(len(obs))
		}
		if accepted > 0 {
			p.Accuracy = float64(correct) / float64(accepted)
			p.Precision = p.Accuracy
		}
		out = append(out, p)
	}
	return out
}

// BinaryCoverage recomputes precision, recall and coverage from raw
// probabilities and labels at each threshold. A predicted yes is P >= t.
func BinaryCoverage(obs []BinaryObservation, thresholds []float64) []CoveragePoint {
	out := make([]CoveragePoint, 0, len(thresholds))
	for _, t := range thresholds {
		var tp, fp, fn int
		for _, o := range obs {
			yes := o.P >= t
			switch {
			case yes && o.Gold:
				tp++
			case yes && !o.Gold:
				fp++
			case !yes && o.Gold:
				fn++
			}
		}
		accepted := tp + fp
		p := CoveragePoint{Threshold: t, Accepted: accepted, TP: tp, FP: fp, FN: fn}
		if len(obs) > 0 {
			p.Coverage = float64(accepted) / float64(len(obs))
		}
		p.Precision = ratio(tp, tp+fp)
		p.Recall = ratio(tp, tp+fn)
		p.Accuracy = p.Precision
		out = append(out, p)
	}
	return out
}

// Selection is the result of choosing one threshold.
type Selection struct {
	Threshold    float64 `json:"threshold"`
	Coverage     float64 `json:"coverage"`
	Accepted     int     `json:"accepted"`
	Accuracy     float64 `json:"accuracy"`
	Target       float64 `json:"target"`
	Met          bool    `json:"met"`
	Insufficient bool    `json:"insufficient"`
}

// SelectThreshold picks the lowest threshold whose accepted cases reach the
// target accuracy, with at least minAccepted accepted cases. When no threshold
// meets the target it returns the most accurate one and Met=false.
func SelectThreshold(obs []Observation, target float64, minAccepted int) Selection {
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
			if o.P >= t {
				accepted++
				if o.Correct {
					correct++
				}
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

// ThresholdGrid is the selection grid: 0.50 to 0.95 in 0.05 steps.
func ThresholdGrid() []float64 {
	var out []float64
	for t := 0.50; t <= 0.9501; t += 0.05 {
		out = append(out, round2(t))
	}
	return out
}

// ProjectedFallbackRate is the share of observations below the threshold.
func ProjectedFallbackRate(obs []Observation, threshold float64) float64 {
	if len(obs) == 0 {
		return 0
	}
	below := 0
	for _, o := range obs {
		if o.P < threshold {
			below++
		}
	}
	return float64(below) / float64(len(obs))
}

// FallbackClassify is one article's category side of the fallback projection.
type FallbackClassify struct {
	ID string
	P  float64
	OK bool
}

// FallbackPlot is one article's place side of the fallback projection.
type FallbackPlot struct {
	ID string
	P  float64
	OK bool
	// Structural marks a primary that is none, missing or unusable, or a
	// missing/invalid precision for a picked place.
	Structural bool
}

// UnionFallbackRate evaluates the full production fallback predicate at the
// candidate thresholds. Every labelled article is in the denominator; a
// missing plot record counts as a fallback.
func UnionFallbackRate(classify []FallbackClassify, plot []FallbackPlot, classifyThreshold, plotThreshold float64) float64 {
	if len(classify) == 0 {
		return 0
	}
	byID := make(map[string]FallbackPlot, len(plot))
	for _, p := range plot {
		byID[p.ID] = p
	}
	fallback := 0
	for _, c := range classify {
		if !c.OK || c.P < classifyThreshold {
			fallback++
			continue
		}
		p, ok := byID[c.ID]
		if !ok || !p.OK || p.Structural || p.P < plotThreshold {
			fallback++
		}
	}
	return float64(fallback) / float64(len(classify))
}

// Percentile returns the p-th percentile (0..100) by linear interpolation.
func Percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	vals := make([]float64, len(durations))
	for i, d := range durations {
		vals[i] = float64(d) / float64(time.Millisecond)
	}
	sort.Float64s(vals)
	if p <= 0 {
		return time.Duration(vals[0] * float64(time.Millisecond))
	}
	if p >= 100 {
		return time.Duration(vals[len(vals)-1] * float64(time.Millisecond))
	}
	pos := p / 100 * float64(len(vals)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(vals) {
		hi = lo
	}
	frac := pos - float64(lo)
	ms := vals[lo] + (vals[hi]-vals[lo])*frac
	return time.Duration(ms * float64(time.Millisecond))
}

// CostPer1k scales a total cost to 1000 labelled items. The denominator is N
// (labelled items), not answered items, so refusals and errors are included.
func CostPer1k(totalUSD float64, n int) float64 {
	if n == 0 {
		return 0
	}
	return totalUSD / float64(n) * 1000
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}
