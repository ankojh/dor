package decide

import (
	"fmt"
	"math"
	"testing"

	"github.com/ankojh/dor/config"
)

func thr() config.Thresholds { return config.DefaultThresholds() }

func noulAns(p float64) Answer {
	return Answer{Type: TypeNoul, Present: true, Noul: p, HasNoul: true}
}

// choiceAns is a choice whose selected option has probability p. jev's
// confidence field is deliberately absent: it must never be needed.
func choiceAns(choice string, p float64) Answer {
	return Answer{Type: TypeChoice, Present: true, Choice: choice, Probabilities: map[string]float64{choice: p}}
}

// choiceConc is a choice with a separate concentration (jev "confidence").
func choiceConc(choice string, p, conc float64) Answer {
	a := choiceAns(choice, p)
	a.Confidence, a.HasConfidence = conc, true
	return a
}

// choiceOnlyConc has a confidence but no probability map.
func choiceOnlyConc(choice string, conc float64) Answer {
	return Answer{Type: TypeChoice, Present: true, Choice: choice, Confidence: conc, HasConfidence: true}
}

func scoreAns(s float64) Answer {
	return Answer{Type: TypeScore, Present: true, Score: s, HasScore: true}
}

func resp(answers map[string]Answer) Response { return Response{Answers: answers} }

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestAnswerFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		a    Answer
		want bool
	}{
		{"noul ok", noulAns(0.4), true},
		{"noul zero probability is still an answer", noulAns(0), true},
		{"choice ok", choiceAns("a", 0.9), true},
		{"score ok", scoreAns(0), true},
		{"not present", Answer{Type: TypeNoul, HasNoul: true}, false},
		{"refusal arm", Answer{Type: "refusal", Present: true, Refusal: true}, false},
		{"refusal flag with usable fields", Answer{Type: TypeNoul, Present: true, Refusal: true, Noul: 1, HasNoul: true}, false},
		{"unknown type", Answer{Type: "mystery", Present: true, Noul: 1, HasNoul: true}, false},
		{"empty type", Answer{Present: true}, false},
		{"noul missing value", Answer{Type: TypeNoul, Present: true}, false},
		{"choice missing value", Answer{Type: TypeChoice, Present: true}, false},
		{"score missing value", Answer{Type: TypeScore, Present: true}, false},
	}
	for _, c := range cases {
		if got := c.a.Answered(); got != c.want {
			t.Errorf("%s: Answered = %v, want %v", c.name, got, c.want)
		}
	}
	// A usable answer of the wrong type is not an answer to this question.
	if noulAns(0.9).AnsweredAs(TypeChoice) {
		t.Error("noul answer must not satisfy a choice question")
	}
	if !noulAns(0.9).AnsweredAs(TypeNoul) {
		t.Error("noul answer satisfies a noul question")
	}
	if (Answer{Type: "refusal", Present: true, Refusal: true}).AnsweredAs("refusal") {
		t.Error("a refusal never satisfies AnsweredAs")
	}
}

func TestResponseAnswer(t *testing.T) {
	r := resp(map[string]Answer{"a": noulAns(0.5), "b": {Type: "refusal", Present: true, Refusal: true}})
	if _, ok := r.Answer("a", TypeNoul); !ok {
		t.Error("a should be answered")
	}
	if _, ok := r.Answer("a", TypeChoice); ok {
		t.Error("a is the wrong type")
	}
	if _, ok := r.Answer("b", TypeNoul); ok {
		t.Error("a refusal is not an answer")
	}
	if _, ok := r.Answer("missing", TypeNoul); ok {
		t.Error("a missing key is not an answer")
	}
}

func TestSelectedProbabilityAndConcentration(t *testing.T) {
	a := Answer{Type: TypeChoice, Present: true, Choice: "x", Confidence: 0.4, HasConfidence: true, Probabilities: map[string]float64{"x": 0.9, "y": 0.1}}
	if p, ok := a.SelectedProbability(); !ok || p != 0.9 {
		t.Errorf("choice probability = %v %v, want the selected option's 0.9", p, ok)
	}
	if a.Concentration() != 0.4 {
		t.Errorf("concentration = %v", a.Concentration())
	}
	// A confidence alone is never a probability.
	if _, ok := choiceOnlyConc("x", 0.99).SelectedProbability(); ok {
		t.Error("a choice with only confidence has no usable probability")
	}
	if _, ok := (Answer{Type: TypeChoice, Choice: "x", Probabilities: map[string]float64{"y": 1}}).SelectedProbability(); ok {
		t.Error("a probability map without the selected key is unusable")
	}
	if _, ok := (Answer{Type: TypeChoice, Probabilities: map[string]float64{"": 1}}).SelectedProbability(); ok {
		t.Error("an empty choice is unusable")
	}
	if p, ok := noulAns(0.8).SelectedProbability(); !ok || p != 0.8 {
		t.Errorf("noul = %v %v", p, ok)
	}
	if _, ok := (Answer{Type: TypeNoul}).SelectedProbability(); ok {
		t.Error("noul without a value")
	}
	s := Answer{Type: TypeScore, Present: true, Score: 1.6, HasScore: true, Probabilities: map[string]float64{"2": 0.55, "1": 0.3}}
	if p, ok := s.SelectedProbability(); !ok || p != 0.55 {
		t.Errorf("score level probability = %v %v", p, ok)
	}
	if _, ok := scoreAns(1.6).SelectedProbability(); ok {
		t.Error("score without probabilities")
	}
	if _, ok := (Answer{Type: "mystery"}).SelectedProbability(); ok {
		t.Error("unknown type")
	}
	if noulAns(0.8).Concentration() != 0 {
		t.Error("no confidence field, no concentration")
	}
	if a.Probability("missing") != 0 || a.Probability("x") != 0.9 {
		t.Error("Probability lookup wrong")
	}
}

func TestCombineClassify(t *testing.T) {
	th := thr() // accept 0.70
	cases := []struct {
		name     string
		ans      map[string]Answer
		answered bool
		accepted bool
		category string
	}{
		{"at threshold", map[string]Answer{"category": choiceAns("politics", 0.70)}, true, true, "politics"},
		{"just below", map[string]Answer{"category": choiceAns("politics", 0.699)}, true, false, "politics"},
		{"well above", map[string]Answer{"category": choiceAns("tech", 0.99)}, true, true, "tech"},
		{"invalid category", map[string]Answer{"category": choiceAns("weather", 0.99)}, false, false, ""},
		{"missing", map[string]Answer{}, false, false, ""},
		{"refusal", map[string]Answer{"category": {Type: "refusal", Present: true, Refusal: true}}, false, false, ""},
		{"wrong type", map[string]Answer{"category": noulAns(0.99)}, false, false, ""},
		{"unknown type", map[string]Answer{"category": {Type: "mystery", Present: true, Choice: "tech"}}, false, false, ""},
	}
	for _, c := range cases {
		got := CombineClassify(resp(c.ans), th)
		if got.Answered != c.answered || got.Accepted != c.accepted || got.Category != c.category {
			t.Errorf("%s: %+v", c.name, got)
		}
	}
	// The gate is the selected probability, not jev's confidence.
	got := CombineClassify(resp(map[string]Answer{"category": choiceConc("tech", 0.75, 0.30)}), th)
	if !got.Accepted || !got.HasProbability || got.Probability != 0.75 || got.Concentration != 0.30 {
		t.Errorf("P .75 / conf .30 must be accepted: %+v", got)
	}
	got = CombineClassify(resp(map[string]Answer{"category": choiceConc("tech", 0.40, 0.99)}), th)
	if got.Accepted || got.Probability != 0.40 || got.Concentration != 0.99 {
		t.Errorf("P .40 / conf .99 must not be accepted: %+v", got)
	}
	// A confidence with no probability map is answered but never accepted.
	got = CombineClassify(resp(map[string]Answer{"category": choiceOnlyConc("tech", 0.99)}), th)
	if !got.Answered || got.Accepted || got.HasProbability || got.Probability != 0 || got.Concentration != 0.99 {
		t.Errorf("confidence only: %+v", got)
	}
	th.Classify.Accept = 0.95
	if got := CombineClassify(resp(map[string]Answer{"category": choiceAns("tech", 0.9)}), th); got.Accepted {
		t.Error("0.9 < 0.95 must not be accepted")
	}
}

func plotCands(n int) []Candidate {
	out := make([]Candidate, n)
	for i := range out {
		out[i] = Candidate{ID: fmt.Sprintf("id%d", i), Name: fmt.Sprintf("N%d", i)}
	}
	return out
}

func TestCombinePlotAccepted(t *testing.T) {
	th := thr() // primary 0.60, party 0.65
	r := resp(map[string]Answer{
		"primary":   choiceAns("c1", 0.60),
		"precision": choiceAns("city", 0.9),
		"party_c0":  noulAns(0.65),
		"party_c1":  noulAns(0.64),
		// party_c2 missing
	})
	got := CombinePlot(r, plotCands(3), th)
	if !got.Answered || !got.Accepted || got.Fallback {
		t.Fatalf("result = %+v", got)
	}
	if got.PrimaryID != "id1" || got.PrimaryName != "N1" || got.PrimaryIndex != 1 || got.PrimaryP != 0.60 {
		t.Errorf("primary = %+v", got)
	}
	if got.Precision != "city" || !got.PrecisionOK {
		t.Errorf("precision = %q ok=%v", got.Precision, got.PrecisionOK)
	}
	if len(got.Parties) != 1 || got.Parties[0] != "id0" {
		t.Errorf("parties = %v (0.65 counts, 0.64 does not, missing does not)", got.Parties)
	}
	if got.PartyP["id0"] != 0.65 || got.PartyP["id1"] != 0.64 {
		t.Errorf("party probabilities = %v", got.PartyP)
	}
	if _, ok := got.PartyP["id2"]; ok {
		t.Error("a missing party answer must not appear as a probability")
	}
}

func TestCombinePlotBelowThreshold(t *testing.T) {
	r := resp(map[string]Answer{
		"primary":   choiceAns("c0", 0.59),
		"precision": choiceAns("street", 0.9),
		"party_c0":  noulAns(0.99),
	})
	got := CombinePlot(r, plotCands(2), thr())
	if !got.Answered || got.Accepted || !got.Fallback {
		t.Fatalf("a weak primary falls back to Claude: %+v", got)
	}
	if got.PrecisionOK {
		t.Error("precision only counts when the primary is accepted")
	}
	if got.Precision != "street" {
		t.Errorf("the raw precision is still reported: %q", got.Precision)
	}
	if len(got.Parties) != 0 {
		t.Errorf("parties only count when the primary is accepted: %v", got.Parties)
	}
	if got.PartyP["id0"] != 0.99 {
		t.Error("party probability is still recorded for reporting")
	}
}

func TestCombinePlotNone(t *testing.T) {
	r := resp(map[string]Answer{"primary": choiceAns("none", 0.9), "precision": choiceAns("city", 0.9), "party_c0": noulAns(0.99)})
	got := CombinePlot(r, plotCands(2), thr())
	if !got.Answered || got.Accepted || !got.Fallback || got.PrimaryIndex != -1 || got.PrimaryID != "" || got.PrimaryP != 0.9 {
		t.Errorf("none = %+v", got)
	}
	if len(got.Parties) != 0 || got.PrecisionOK {
		t.Errorf("nothing else counts for none: %+v", got)
	}
}

func TestCombinePlotFailsClosed(t *testing.T) {
	bad := map[string]map[string]Answer{
		"missing":      {},
		"refusal":      {"primary": {Type: "refusal", Present: true, Refusal: true}},
		"wrong type":   {"primary": noulAns(0.9)},
		"empty choice": {"primary": {Type: TypeChoice, Present: true}},
		"garbage":      {"primary": choiceAns("zzz", 0.9)},
		"bare c":       {"primary": choiceAns("c", 0.9)},
		"c not number": {"primary": choiceAns("cx", 0.9)},
		"out of range": {"primary": choiceAns("c9", 0.9)},
		"negative":     {"primary": choiceAns("c-1", 0.9)},
	}
	for name, ans := range bad {
		got := CombinePlot(resp(ans), plotCands(3), thr())
		if got.Answered || got.Accepted || got.PrimaryID != "" || got.PrimaryIndex != -1 {
			t.Errorf("%s: %+v", name, got)
		}
		if got.PartyP == nil {
			t.Errorf("%s: PartyP must be non-nil", name)
		}
	}
}

func TestCombinePlotCapsCandidates(t *testing.T) {
	// c8 exists in the caller's slice but was never offered to jev.
	got := CombinePlot(resp(map[string]Answer{"primary": choiceAns("c8", 0.99)}), plotCands(12), thr())
	if got.Answered {
		t.Errorf("c8 is beyond MaxCandidates: %+v", got)
	}
	got = CombinePlot(resp(map[string]Answer{"primary": choiceAns("c7", 0.99)}), plotCands(12), thr())
	if !got.Answered || got.PrimaryID != "id7" {
		t.Errorf("c7 is the last offered candidate: %+v", got)
	}
}

func TestCombinePlotMissingPrecisionFallsBack(t *testing.T) {
	got := CombinePlot(resp(map[string]Answer{"primary": choiceAns("c0", 0.9)}), plotCands(1), thr())
	if !got.Accepted || !got.Fallback || got.Precision != "" || got.PrecisionOK {
		t.Errorf("accepted place without precision needs Claude: %+v", got)
	}
	// A refused precision question behaves the same.
	r := resp(map[string]Answer{"primary": choiceAns("c0", 0.9), "precision": {Type: "refusal", Present: true, Refusal: true}})
	if got := CombinePlot(r, plotCands(1), thr()); !got.Fallback {
		t.Errorf("refused precision: %+v", got)
	}
}

func TestCombinePlotPartyTypeMismatchIgnored(t *testing.T) {
	r := resp(map[string]Answer{"primary": choiceAns("c0", 0.9), "precision": choiceAns("city", 0.9), "party_c0": choiceAns("true", 1)})
	got := CombinePlot(r, plotCands(1), thr())
	if len(got.Parties) != 0 || len(got.PartyP) != 0 {
		t.Errorf("a non-noul party answer must be ignored: %+v", got)
	}
}

func relateStories() (Story, Story) {
	return Story{Entities: []string{"Iran", "US"}}, Story{Entities: []string{"iran", "Israel"}}
}

func TestCombineRelateStrengthFormula(t *testing.T) {
	a, b := relateStories() // overlap = 1/3
	r := resp(map[string]Answer{
		"related":   noulAns(0.8),
		"type":      choiceAns("causal", 0.9),
		"direction": choiceAns("a_to_b", 0.9),
		"strength":  scoreAns(2.4),
	})
	got := CombineRelate(r, a, b, thr())
	if !got.Answered || !got.Related || !got.Accepted || got.Type != "causal" || got.Direction != "a_to_b" {
		t.Fatalf("result = %+v", got)
	}
	wantOverlap := 1.0 / 3
	want := 2.4 / 3 * 0.8 * (0.9 + 0.2*wantOverlap)
	if !near(got.EntityOverlap, wantOverlap) || !near(got.Strength, want) || !got.StrengthAnswered || got.Score != 2.4 {
		t.Errorf("overlap %v strength %v want %v", got.EntityOverlap, got.Strength, want)
	}
	if got.Tier != "direct" {
		t.Errorf("tier for %v = %s", got.Strength, got.Tier)
	}
	if got.RelatedP != 0.8 {
		t.Errorf("RelatedP = %v", got.RelatedP)
	}
}

func TestCombineRelateNudgeRange(t *testing.T) {
	th := thr()
	r := resp(map[string]Answer{"related": noulAns(0.9), "type": choiceAns("causal", 1), "strength": scoreAns(1.5)})
	noOverlap := CombineRelate(r, Story{Entities: []string{"x"}}, Story{Entities: []string{"y"}}, th)
	full := CombineRelate(r, Story{Entities: []string{"x"}}, Story{Entities: []string{"X"}}, th)
	none := CombineRelate(r, Story{}, Story{}, th)
	base := 1.5 / 3 * 0.9
	if !near(noOverlap.Strength, base*0.9) || !near(full.Strength, base*1.1) || !near(none.Strength, base*0.9) {
		t.Errorf("nudge range wrong: %v %v %v (base %v)", noOverlap.Strength, full.Strength, none.Strength, base)
	}
}

func TestCombineRelateStrengthClamped(t *testing.T) {
	th := thr()
	same := []string{"Iran"}
	r := resp(map[string]Answer{"related": noulAns(1), "type": choiceAns("causal", 1), "strength": scoreAns(3)})
	if got := CombineRelate(r, Story{Entities: same}, Story{Entities: same}, th); got.Strength != 1 || got.Tier != "immediate" {
		t.Errorf("1.0 × 1.1 must clamp to 1: %+v", got)
	}
	r = resp(map[string]Answer{"related": noulAns(1), "type": choiceAns("causal", 1), "strength": scoreAns(7)})
	if got := CombineRelate(r, Story{}, Story{}, th); !near(got.Strength, 0.9) || got.Score != 7 {
		t.Errorf("a score above 3 is clamped to 3 for strength: %+v", got)
	}
	r = resp(map[string]Answer{"related": noulAns(0.9), "type": choiceAns("causal", 1), "strength": scoreAns(-2)})
	if got := CombineRelate(r, Story{}, Story{}, th); got.Strength != 0 || got.Tier != "thematic" {
		t.Errorf("a negative score clamps to 0: %+v", got)
	}
}

func TestStrengthTiers(t *testing.T) {
	cases := map[float64]string{
		0: "thematic", 0.2999: "thematic", 0.3: "indirect", 0.4999: "indirect",
		0.5: "direct", 0.7499: "direct", 0.75: "immediate", 1: "immediate",
	}
	for s, want := range cases {
		if got := StrengthTier(s); got != want {
			t.Errorf("StrengthTier(%v) = %s, want %s", s, got, want)
		}
	}
}

func TestCombineRelateTypeOnlyIfRelated(t *testing.T) {
	th := thr() // accept 0.60
	// Below the threshold the type answer is ignored, even a confident one.
	r := resp(map[string]Answer{"related": noulAns(0.59), "type": choiceAns("causal", 1), "direction": choiceAns("a_to_b", 1), "strength": scoreAns(3)})
	got := CombineRelate(r, Story{}, Story{}, th)
	if !got.Answered || got.Related || got.Accepted || got.Type != "none" || got.Direction != "unclear" || got.StrengthAnswered || got.Strength != 0 {
		t.Errorf("not related: %+v", got)
	}
	if got.RelatedP != 0.59 {
		t.Errorf("RelatedP = %v", got.RelatedP)
	}
	// Exactly at the threshold the pair is related.
	r = resp(map[string]Answer{"related": noulAns(0.60), "type": choiceAns("thematic", 1)})
	if got := CombineRelate(r, Story{}, Story{}, th); !got.Related || got.Type != "thematic" {
		t.Errorf("at threshold: %+v", got)
	}
}

func TestCombineRelateFailsClosed(t *testing.T) {
	th := thr()
	// related missing / refused / wrong type: not answered.
	for name, ans := range map[string]map[string]Answer{
		"missing":    {},
		"refusal":    {"related": {Type: "refusal", Present: true, Refusal: true}},
		"wrong type": {"related": choiceAns("true", 1)},
	} {
		got := CombineRelate(resp(ans), Story{}, Story{}, th)
		if got.Answered || got.Related || got.Accepted || got.Direction != "unclear" {
			t.Errorf("%s: %+v", name, got)
		}
	}
	// A confidently related pair whose type is missing or invalid cannot be typed.
	for name, typ := range map[string]map[string]Answer{
		"type missing": {"related": noulAns(0.95)},
		"type refusal": {"related": noulAns(0.95), "type": {Type: "refusal", Present: true, Refusal: true}},
		"type invalid": {"related": noulAns(0.95), "type": choiceAns("friendship", 1)},
		"type noul":    {"related": noulAns(0.95), "type": noulAns(1)},
	} {
		got := CombineRelate(resp(typ), Story{}, Story{}, th)
		if got.Answered || got.Related || got.Accepted || got.Type != "" {
			t.Errorf("%s: must fail closed: %+v", name, got)
		}
		if got.RelatedP != 0.95 || got.Direction != "unclear" {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

func TestCombineRelateTypeNoneIsNotAccepted(t *testing.T) {
	r := resp(map[string]Answer{"related": noulAns(0.9), "type": choiceAns("none", 0.9), "strength": scoreAns(3), "direction": choiceAns("a_to_b", 1)})
	got := CombineRelate(r, Story{}, Story{}, thr())
	if !got.Answered || got.Related || got.Accepted || got.Type != "none" || got.StrengthAnswered || got.Direction != "unclear" {
		t.Errorf("type none: %+v", got)
	}
}

func TestCombineRelateDirectionAndStrengthDefaults(t *testing.T) {
	th := thr()
	base := map[string]Answer{"related": noulAns(0.9), "type": choiceAns("reaction", 1)}
	got := CombineRelate(resp(base), Story{}, Story{}, th)
	if !got.Accepted || got.Direction != "unclear" || got.StrengthAnswered || got.Strength != 0 || got.Tier != "" {
		t.Errorf("no direction, no strength: %+v", got)
	}
	base["direction"] = choiceAns("sideways", 1)
	if got := CombineRelate(resp(base), Story{}, Story{}, th); got.Direction != "unclear" {
		t.Errorf("an invalid direction is unclear, got %q", got.Direction)
	}
	base["direction"] = Answer{Type: "refusal", Present: true, Refusal: true}
	base["strength"] = Answer{Type: TypeScore, Present: true} // no value
	got = CombineRelate(resp(base), Story{}, Story{}, th)
	if got.Direction != "unclear" || got.StrengthAnswered {
		t.Errorf("refused direction / valueless strength: %+v", got)
	}
	if !got.Accepted {
		t.Error("the relation itself still stands without direction and strength")
	}
}

func TestCombineMatch(t *testing.T) {
	th := thr() // strictness 0.80
	cases := []struct {
		name     string
		ans      map[string]Answer
		answered bool
		accepted bool
	}{
		{"at threshold", map[string]Answer{"match": noulAns(0.80)}, true, true},
		{"below", map[string]Answer{"match": noulAns(0.79)}, true, false},
		{"zero", map[string]Answer{"match": noulAns(0)}, true, false},
		{"missing", map[string]Answer{}, false, false},
		{"refusal", map[string]Answer{"match": {Type: "refusal", Present: true, Refusal: true}}, false, false},
		{"wrong type", map[string]Answer{"match": choiceAns("true", 1)}, false, false},
	}
	for _, c := range cases {
		got := CombineMatch(resp(c.ans), th)
		if got.Answered != c.answered || got.Accepted != c.accepted {
			t.Errorf("%s: %+v", c.name, got)
		}
	}
	th.Match.Strictness = 0.95
	if got := CombineMatch(resp(map[string]Answer{"match": noulAns(0.9)}), th); got.Accepted || got.MatchP != 0.9 {
		t.Errorf("stricter threshold: %+v", got)
	}
}

func TestCombinePlotGatesOnProbabilityNotConfidence(t *testing.T) {
	th := thr() // primary 0.60
	ans := map[string]Answer{"primary": choiceConc("c1", 0.70, 0.40), "precision": choiceAns("city", 0.9)}
	got := CombinePlot(resp(ans), plotCands(3), th)
	if !got.Accepted || got.Fallback || !got.HasProbability || got.PrimaryP != 0.70 || got.Concentration != 0.40 || got.PrimaryID != "id1" {
		t.Errorf("P .70 / conf .40 with threshold .60: %+v", got)
	}
	ans["primary"] = choiceConc("c1", 0.50, 0.95)
	got = CombinePlot(resp(ans), plotCands(3), th)
	if got.Accepted || !got.Fallback || got.PrimaryP != 0.50 || got.Concentration != 0.95 {
		t.Errorf("P .50 / conf .95 must not be accepted: %+v", got)
	}
}

func TestCombinePlotConfidenceOnlyFallsBack(t *testing.T) {
	ans := map[string]Answer{"primary": choiceOnlyConc("c0", 0.99), "precision": choiceAns("city", 0.9), "party_c0": noulAns(0.99)}
	got := CombinePlot(resp(ans), plotCands(2), thr())
	if !got.Answered || got.Accepted || !got.Fallback || got.HasProbability || got.PrimaryP != 0 || got.Concentration != 0.99 {
		t.Errorf("a choice with only confidence is not accepted: %+v", got)
	}
	if got.PrimaryID != "id0" || len(got.Parties) != 0 || got.PrecisionOK {
		t.Errorf("nothing else counts: %+v", got)
	}
}

func TestCombinePlotFallbackFlag(t *testing.T) {
	th := thr()
	cands := plotCands(3)
	cases := []struct {
		name string
		ans  map[string]Answer
		want bool
	}{
		{"accepted with precision", map[string]Answer{"primary": choiceAns("c0", 0.9), "precision": choiceAns("city", 0.9)}, false},
		{"missing primary", map[string]Answer{"precision": choiceAns("city", 0.9)}, true},
		{"refused primary", map[string]Answer{"primary": {Type: "refusal", Present: true, Refusal: true}}, true},
		{"invalid primary", map[string]Answer{"primary": choiceAns("zzz", 0.9)}, true},
		{"out of range primary", map[string]Answer{"primary": choiceAns("c5", 0.9)}, true},
		{"none", map[string]Answer{"primary": choiceAns("none", 0.95)}, true},
		{"no probability", map[string]Answer{"primary": {Type: TypeChoice, Present: true, Choice: "c0"}}, true},
		{"below threshold", map[string]Answer{"primary": choiceAns("c0", 0.5), "precision": choiceAns("city", 0.9)}, true},
		{"missing precision", map[string]Answer{"primary": choiceAns("c0", 0.9)}, true},
		{"refused precision", map[string]Answer{"primary": choiceAns("c0", 0.9), "precision": {Type: "refusal", Present: true, Refusal: true}}, true},
		{"invalid precision", map[string]Answer{"primary": choiceAns("c0", 0.9), "precision": choiceAns("galaxy", 0.9)}, true},
	}
	for _, c := range cases {
		got := CombinePlot(resp(c.ans), cands, th)
		if got.Fallback != c.want {
			t.Errorf("%s: Fallback = %v, want %v (%+v)", c.name, got.Fallback, c.want, got)
		}
	}
	// An invalid precision never leaks into the result.
	got := CombinePlot(resp(cases[10].ans), cands, th)
	if got.Precision != "" || got.PrecisionOK {
		t.Errorf("invalid precision: %+v", got)
	}
}
