package decide

import (
	"fmt"
	"strconv"

	"github.com/ankojh/dor/config"
)

// ClassifyResult is the combined category decision.
type ClassifyResult struct {
	Answered       bool
	Accepted       bool
	Category       string
	Probability    float64
	HasProbability bool
	Concentration  float64
}

// CombineClassify applies the category threshold to the selected category's
// probability. jev's confidence (a normalized concentration) is recorded but
// never gates the answer.
func CombineClassify(resp Response, t config.Thresholds) ClassifyResult {
	a, ok := resp.Answer("category", TypeChoice)
	if !ok || !validCategory(a.Choice) {
		return ClassifyResult{}
	}
	res := ClassifyResult{Answered: true, Category: a.Choice, Concentration: a.Concentration()}
	p, ok := a.SelectedProbability()
	if !ok {
		return res
	}
	res.Probability = p
	res.HasProbability = true
	res.Accepted = p >= t.Classify.Accept
	return res
}

// PlotResult is the combined place decision.
type PlotResult struct {
	Answered bool
	Accepted bool
	// Fallback is true when Claude should extract the place instead.
	Fallback bool
	// PrimaryID is empty when the article names no place.
	PrimaryID      string
	PrimaryName    string
	PrimaryIndex   int // -1 for none
	PrimaryP       float64
	HasProbability bool
	Concentration  float64
	Precision      string
	PrecisionOK    bool
	Parties        []string
	PartyP         map[string]float64
}

// CombinePlot applies the place thresholds. Precision and parties only count
// when the primary place is accepted; a missing party answer is "not a party"
// (fail closed on that attribute).
func CombinePlot(resp Response, candidates []Candidate, t config.Thresholds) PlotResult {
	res := PlotResult{PrimaryIndex: -1, PartyP: make(map[string]float64), Fallback: true}
	a, ok := resp.Answer("primary", TypeChoice)
	if !ok {
		// Missing, refused or unknown primary: fail closed and ask Claude.
		res.Answered = false
		return res
	}
	if len(candidates) > MaxCandidates {
		candidates = candidates[:MaxCandidates]
	}
	res.Concentration = a.Concentration()
	p, hasP := a.SelectedProbability()
	res.PrimaryP, res.HasProbability = p, hasP
	switch {
	case a.Choice == "none":
		res.Answered = true
		res.Fallback = true
		return res
	case len(a.Choice) < 2 || a.Choice[0] != 'c':
		res.Answered = false
		return res
	}
	idx, err := strconv.Atoi(a.Choice[1:])
	if err != nil || idx < 0 || idx >= len(candidates) {
		res.Answered = false
		return res
	}
	res.Answered = true
	res.PrimaryIndex = idx
	res.PrimaryID = candidates[idx].ID
	res.PrimaryName = candidates[idx].Name
	if !hasP {
		res.Fallback = true
		return res
	}
	res.Accepted = res.PrimaryP >= t.Plot.Primary
	res.Fallback = !res.Accepted

	prec, ok := resp.Answer("precision", TypeChoice)
	if ok && validPrecision(prec.Choice) {
		res.Precision = prec.Choice
		res.PrecisionOK = res.Accepted
	}
	if res.Accepted && (!ok || !validPrecision(prec.Choice)) {
		// The place is accepted but its precision is missing or invalid;
		// Claude should fill it in rather than leaving a half-answer.
		res.Fallback = true
	}
	for i, c := range candidates {
		pa, ok := resp.Answer(fmt.Sprintf("party_c%d", i), TypeNoul)
		if !ok {
			continue
		}
		res.PartyP[c.ID] = pa.Noul
		if res.Accepted && pa.Noul >= t.Plot.Party {
			res.Parties = append(res.Parties, c.ID)
		}
	}
	if !res.Accepted {
		res.Fallback = true
	}
	return res
}

// RelateResult is the combined pair decision.
type RelateResult struct {
	Answered bool
	Related  bool
	Accepted bool
	RelatedP float64
	Type     string
	// Direction is "unclear" when the model gave no usable direction.
	Direction string
	// Score is the model's expected strength level in [0,3].
	Score float64
	// Strength = score/3 × P(related), nudged by entity evidence, in [0,1].
	Strength         float64
	StrengthAnswered bool
	EntityOverlap    float64
	Tier             string
}

// CombineRelate applies "type only if related" in code and computes strength.
func CombineRelate(resp Response, a, b Story, t config.Thresholds) RelateResult {
	res := RelateResult{Direction: "unclear"}
	related, ok := resp.Answer("related", TypeNoul)
	if !ok {
		return RelateResult{Direction: "unclear"}
	}
	res.Answered = true
	res.RelatedP = related.Noul
	if related.Noul < t.Relate.Accept {
		res.Type = "none"
		return res
	}
	typ, ok := resp.Answer("type", TypeChoice)
	if !ok || !validRelationType(typ.Choice) {
		// The pair looks related but cannot be typed: fail closed.
		res.Answered = false
		return RelateResult{RelatedP: related.Noul, Direction: "unclear"}
	}
	res.Type = typ.Choice
	if typ.Choice == "none" {
		return res
	}
	res.Related = true
	res.Accepted = true

	if dir, ok := resp.Answer("direction", TypeChoice); ok && validDirection(dir.Choice) {
		res.Direction = dir.Choice
	}
	if score, ok := resp.Answer("strength", TypeScore); ok {
		res.StrengthAnswered = true
		res.Score = score.Score
		s := score.Score
		if s < 0 {
			s = 0
		}
		if s > 3 {
			s = 3
		}
		res.EntityOverlap = EntityOverlap(a.Entities, b.Entities)
		// Entity evidence nudges by up to ±10% around the score×P product.
		nudge := 0.9 + 0.2*res.EntityOverlap
		res.Strength = clampFloat(s/3*related.Noul*nudge, 0, 1)
		res.Tier = StrengthTier(res.Strength)
	}
	return res
}

// StrengthTier buckets a strength into the plan's 0.3 / 0.5 / 0.75 tiers.
func StrengthTier(strength float64) string {
	switch {
	case strength >= 0.75:
		return "immediate"
	case strength >= 0.5:
		return "direct"
	case strength >= 0.3:
		return "indirect"
	default:
		return "thematic"
	}
}

// MatchResult is the combined alert decision.
type MatchResult struct {
	Answered bool
	Accepted bool
	MatchP   float64
}

// CombineMatch applies the strictness threshold.
func CombineMatch(resp Response, t config.Thresholds) MatchResult {
	a, ok := resp.Answer("match", TypeNoul)
	if !ok {
		return MatchResult{}
	}
	return MatchResult{
		Answered: true,
		Accepted: a.Noul >= t.Match.Strictness,
		MatchP:   a.Noul,
	}
}

func validCategory(s string) bool {
	for _, k := range CategoryKeys() {
		if s == k {
			return true
		}
	}
	return false
}

func validPrecision(s string) bool {
	return contains(optionKeys(PrecisionOptions()), s)
}

func validRelationType(s string) bool {
	return contains(optionKeys(RelationTypeOptions()), s)
}

func validDirection(s string) bool {
	return contains(optionKeys(DirectionOptions()), s)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
