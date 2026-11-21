package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/llm"
)

type fakeJev func(req decide.Request) (decide.Response, error)

func (f fakeJev) Decide(_ context.Context, req decide.Request) (decide.Response, error) {
	return f(req)
}

func noulA(p float64) decide.Answer {
	return decide.Answer{Type: decide.TypeNoul, Present: true, Noul: p, HasNoul: true}
}

func choiceA(choice string, p float64) decide.Answer {
	return decide.Answer{Type: decide.TypeChoice, Present: true, Choice: choice, Probabilities: map[string]float64{choice: p}}
}

func scoreA(level int, p float64) decide.Answer {
	return decide.Answer{Type: decide.TypeScore, Present: true, Score: float64(level), HasScore: true,
		Probabilities: map[string]float64{fmt.Sprint(level): p}}
}

func refusalA() decide.Answer { return decide.Answer{Type: "refusal", Present: true, Refusal: true} }

func memDataset(nArticles int) *Dataset {
	ds := &Dataset{ArticleLabels: map[string]ArticleLabel{}, PairLabels: map[string]PairLabel{}}
	for i := 0; i < nArticles; i++ {
		id := fmt.Sprintf("a%02d", i)
		ds.Articles = append(ds.Articles, Article{
			ID: id, Title: "Title " + id, Snippet: "Snippet " + id, Source: "Src",
			Candidates: []Candidate{{ID: "country:XX", Name: "Xland", Country: "Xland"}},
		})
		ds.ArticleLabels[id] = ArticleLabel{ArticleID: id, Category: "politics", PrimaryPlace: "country:XX", Precision: "country", Labeler: "human:t", LabeledAt: "2026-11-20T00:00:00Z"}
	}
	return ds
}

func addPair(ds *Dataset, id string, label PairLabel) {
	a, b := ds.Articles[0].ID, ds.Articles[1].ID
	ds.Pairs = append(ds.Pairs, Pair{ID: id, A: a, B: b})
	label.PairID = id
	ds.PairLabels[id] = label
}

func harness(ds *Dataset, jev decide.Decider) *Harness {
	return &Harness{DS: ds, Jev: jev, JevKind: "live", Thresholds: config.DefaultThresholds(), Log: quietLog(),
		Now: func() time.Time { return time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC) }}
}

func resp(answers map[string]decide.Answer) decide.Response {
	return decide.Response{Model: "typesafe/jev-1.14-20261014", Answers: answers, Attempts: 1}
}

func TestRelateSelectionCountsGoldUnrelated(t *testing.T) {
	ds := memDataset(2)
	for i := 0; i < 10; i++ {
		addPair(ds, fmt.Sprintf("rel%02d", i), PairLabel{Related: true, Type: "causal", Direction: "unclear", Strength: 2})
		addPair(ds, fmt.Sprintf("unr%02d", i), PairLabel{Related: false, Type: "none", Direction: "unclear", Strength: 0})
	}
	// The model is certain every pair is a causal link.
	h := harness(ds, fakeJev(func(decide.Request) (decide.Response, error) {
		return resp(map[string]decide.Answer{
			"related": noulA(0.99), "type": choiceA("causal", 0.9), "direction": choiceA("unclear", 0.9), "strength": scoreA(2, 0.8),
		}), nil
	}))
	task := h.runRelateJev(context.Background())
	if task.N != 20 || task.Answered != 20 {
		t.Fatalf("n=%d answered=%d", task.N, task.Answered)
	}
	if got := task.Extra["accepted_type_accuracy"]; !near(got, 0.5) {
		t.Errorf("accepted_type_accuracy = %v, want 0.5 (gold-unrelated links are wrong)", got)
	}
	if len(task.Relations) != 20 || len(task.Binary) != 20 {
		t.Errorf("relations=%d binary=%d, want all 20 including gold-unrelated", len(task.Relations), len(task.Binary))
	}
	unrelated := 0
	for _, r := range task.Relations {
		if !r.GoldRelated {
			unrelated++
		}
	}
	if unrelated != 10 {
		t.Errorf("gold-unrelated relations = %d, want 10", unrelated)
	}
	sel := SelectRelationThreshold(task.Relations, 0.85, 1)
	if sel.Met || !near(sel.Accuracy, 0.5) {
		t.Errorf("raw selection = %+v, want accuracy 0.5 and not met", sel)
	}
	if task.Extra["related_precision"] != 0.5 || task.Extra["related_recall"] != 1 {
		t.Errorf("precision/recall = %v / %v", task.Extra["related_precision"], task.Extra["related_recall"])
	}
}

func TestRelatednessCalibrationIsSeparateFromTypeCorrectness(t *testing.T) {
	ds := memDataset(2)
	for i := 0; i < 10; i++ {
		addPair(ds, fmt.Sprintf("rel%02d", i), PairLabel{Related: true, Type: "causal", Direction: "unclear", Strength: 2})
	}
	// Gold-related, P(related) 0.9, but the model picks the wrong type every time.
	h := harness(ds, fakeJev(func(decide.Request) (decide.Response, error) {
		return resp(map[string]decide.Answer{"related": noulA(0.9), "type": choiceA("reaction", 0.9)}), nil
	}))
	task := h.runRelateJev(context.Background())
	if task.Extra["accepted_type_accuracy"] != 0 || task.Accuracy != 0 {
		t.Errorf("type accuracy = %v / %v, want 0", task.Extra["accepted_type_accuracy"], task.Accuracy)
	}
	// Event calibration is about relatedness (gold true), not type: bin 0.9 is 100% right.
	if len(task.Bins) == 0 || task.Bins[9].N != 10 || !near(task.Bins[9].Accuracy, 1) || !near(task.ECE, 0.1) {
		t.Errorf("event bins/ECE = %+v / %v", task.Bins[9], task.ECE)
	}
}

func TestMatchCalibrationAndStrictnessCurve(t *testing.T) {
	ds := memDataset(3)
	for i := 0; i < 20; i++ {
		ds.Matches = append(ds.Matches, MatchCandidate{ID: fmt.Sprintf("m%02d", i), ArticleID: "a00", Prompt: "p", Relevant: false})
	}
	h := harness(ds, fakeJev(func(decide.Request) (decide.Response, error) {
		return resp(map[string]decide.Answer{"match": noulA(0.01)}), nil
	}))
	task := h.runMatchJev(context.Background())
	if task.Accuracy != 1 || task.Answered != 20 || task.Extra["match_precision"] != 0 {
		t.Errorf("20 true negatives: accuracy %v answered %d", task.Accuracy, task.Answered)
	}
	if !near(task.ECE, 0.01) {
		t.Errorf("event ECE = %v, want ~0.01", task.ECE)
	}
	// Decision calibration at 0.80: every call is a confident "no" (confidence 0.99) and right.
	if task.DecisionThreshold != 0.80 || !near(task.DecisionECE, 0.01) {
		t.Errorf("decision threshold %v ECE %v", task.DecisionThreshold, task.DecisionECE)
	}
	if len(task.DecisionBins) == 0 || task.DecisionBins[9].N != 20 || !near(task.DecisionBins[9].Accuracy, 1) {
		t.Errorf("decision bins = %+v", task.DecisionBins)
	}
}

func TestBinaryCoverageRecomputesPerThreshold(t *testing.T) {
	obs := []BinaryObservation{
		{ID: "t1", P: 0.9, Gold: true}, {ID: "t2", P: 0.7, Gold: true}, {ID: "t3", P: 0.6, Gold: true},
		{ID: "f1", P: 0.85, Gold: false}, {ID: "f2", P: 0.55, Gold: false}, {ID: "f3", P: 0.3, Gold: false},
	}
	pts := BinaryCoverage(obs, []float64{0.5, 0.6, 0.7, 0.8, 0.9, 0.95})
	want := []struct{ tp, fp, fn int }{{3, 2, 0}, {3, 1, 0}, {2, 1, 1}, {1, 1, 2}, {1, 0, 2}, {0, 0, 3}}
	for i, w := range want {
		p := pts[i]
		if p.TP != w.tp || p.FP != w.fp || p.FN != w.fn || p.Accepted != w.tp+w.fp {
			t.Errorf("t=%.2f: %+v, want %+v", p.Threshold, p, w)
		}
		if !near(p.Precision, ratio(w.tp, w.tp+w.fp)) || !near(p.Recall, ratio(w.tp, w.tp+w.fn)) || !near(p.Coverage, float64(w.tp+w.fp)/6) {
			t.Errorf("t=%.2f: precision %v recall %v coverage %v", p.Threshold, p.Precision, p.Recall, p.Coverage)
		}
	}
	// The harness fills the same curve on the match task.
	ds := memDataset(2)
	probs := map[string]float64{}
	for i, o := range obs {
		id := fmt.Sprintf("m%d", i)
		probs["prompt-"+id] = o.P
		ds.Matches = append(ds.Matches, MatchCandidate{ID: id, ArticleID: "a00", Prompt: "prompt-" + id, Relevant: o.Gold})
	}
	h := harness(ds, fakeJev(func(req decide.Request) (decide.Response, error) {
		instr := req.Questions["match"].Instructions
		for prompt, p := range probs {
			if strings.Contains(instr, `"`+prompt+`"`) {
				return resp(map[string]decide.Answer{"match": noulA(p)}), nil
			}
		}
		return decide.Response{}, errors.New("unknown prompt")
	}))
	task := h.runMatchJev(context.Background())
	if len(task.Coverage) != len(ThresholdGrid()) {
		t.Fatalf("coverage points = %d", len(task.Coverage))
	}
	for _, c := range task.Coverage {
		if c.Threshold == 0.70 && (c.TP != 2 || c.FP != 1 || c.FN != 1) {
			t.Errorf("harness curve at 0.70 = %+v", c)
		}
	}
}

func TestPaidRefusalsAreAccountedAndPerItemCostUsesN(t *testing.T) {
	ds := memDataset(20)
	h := harness(ds, fakeJev(func(req decide.Request) (decide.Response, error) {
		answers := map[string]decide.Answer{}
		for k := range req.Questions {
			answers[k] = refusalA()
		}
		r := resp(answers)
		r.Usage = decide.Usage{InputTokens: 100, OutputTokens: 1, CostUSD: 0.01}
		r.Latency, r.LastAttemptLatency = time.Second, time.Second
		return r, nil
	}))
	task := h.runClassifyJev(context.Background())
	if task.N != 20 || task.Calls != 20 || task.Refusals != 20 || task.Unanswered != 20 || task.Answered != 0 {
		t.Errorf("counts = %+v", task)
	}
	if !near(task.CostUSD, 0.20) || task.InputTokens != 2000 || task.OutputTokens != 20 {
		t.Errorf("cost %v input %d output %d", task.CostUSD, task.InputTokens, task.OutputTokens)
	}
	if !near(task.CostPer1kUSD, 10) {
		t.Errorf("$/1k = %v, want 10 (0.20 over 20 labelled items)", task.CostPer1kUSD)
	}
	if task.LatencyP95MS < 999 || task.LatencyP95MS > 1001 || task.LastLatencyP95MS < 999 || task.LastLatencyP95MS > 1001 {
		t.Errorf("p95 = %v last %v ms, want ~1000", task.LatencyP95MS, task.LastLatencyP95MS)
	}
	if task.Accuracy != 0 || len(task.Bins) != 0 {
		t.Errorf("no answers, no accuracy or calibration: %+v", task.Accuracy)
	}
}

func TestCostPerItemDividesByLabelledNotAnswered(t *testing.T) {
	ds := memDataset(20)
	n := 0
	h := harness(ds, fakeJev(func(req decide.Request) (decide.Response, error) {
		n++
		if n%2 == 0 {
			return decide.Response{}, errors.New("boom") // an error costs nothing and answers nothing
		}
		r := resp(map[string]decide.Answer{"category": refusalA()})
		r.Usage = decide.Usage{InputTokens: 10, CostUSD: 0.01}
		return r, nil
	}))
	task := h.runClassifyJev(context.Background())
	if task.N != 20 || task.Errors != 10 || task.Refusals != 10 || task.Answered != 0 {
		t.Errorf("task = %+v", task)
	}
	if !near(task.CostPer1kUSD, 0.10/20*1000) {
		t.Errorf("$/1k = %v, want 5", task.CostPer1kUSD)
	}
}

func claudeServer(t *testing.T, body func(n int) string) (*httptest.Server, *int) {
	t.Helper()
	hits := new(int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		*hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body(*hits))
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

func newClaude(t *testing.T, url string) *llm.Client {
	t.Helper()
	models := config.DefaultModels()
	bcfg := config.DefaultBudget()
	bcfg.DailyUSD = 1e6
	for k, v := range bcfg.TokenBuckets {
		v.Capacity = 1e12
		bcfg.TokenBuckets[k] = v
	}
	c, err := llm.New(llm.Config{
		APIKey: "k", BaseURL: url, HaikuModel: models.Claude.HaikuModel, OpusModel: models.Claude.OpusModel,
		Effort: "low", Timeout: 10 * time.Second, Price: models.Claude.Price, MaxRetries: 0,
	}, budget.NewMemory(bcfg))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClaudeTypedStopsKeepUsage(t *testing.T) {
	refusal := `{"id":"m","type":"message","role":"assistant","model":"claude-haiku-5-5","content":[],"stop_reason":"refusal",
	  "stop_details":{"type":"refusal","category":"cyber","explanation":"x"},
	  "usage":{"input_tokens":100,"output_tokens":2,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`
	srv, hits := claudeServer(t, func(int) string { return refusal })
	ds := memDataset(20)
	h := harness(ds, nil)
	h.Claude, h.ClaudeKind = newClaude(t, srv.URL), "live"
	task := h.runClassifyClaude(context.Background())
	if *hits != 20 || task.N != 20 || task.Calls != 20 || task.Refusals != 20 || task.Errors != 0 || task.Answered != 0 {
		t.Fatalf("hits=%d task=%+v", *hits, task)
	}
	price := config.DefaultModels().Claude.Price["claude-haiku-5-5"]
	want := 20 * price.CostUsage(100, 2, 0, 0)
	if !near(task.CostUSD, want) || task.InputTokens != 2000 || task.OutputTokens != 40 {
		t.Errorf("a refusal is paid: cost %v (want %v) input %d output %d", task.CostUSD, want, task.InputTokens, task.OutputTokens)
	}
	if !near(task.CostPer1kUSD, want/20*1000) {
		t.Errorf("$/1k = %v", task.CostPer1kUSD)
	}
	if task.LatencyP95MS <= 0 {
		t.Error("typed stops keep their latency")
	}
}

func TestAllErrorHarnessFallsBackEverywhere(t *testing.T) {
	ds := memDataset(7)
	ds.ArticleLabels["ghost"] = ArticleLabel{ArticleID: "ghost", Category: "tech", PrimaryPlace: "none", Precision: "country", Labeler: "human:t", LabeledAt: "x"}
	h := harness(ds, decide.Unavailable{})
	classify := h.runClassifyJev(context.Background())
	plot := h.runPlotJev(context.Background())
	if classify.N != 8 || len(classify.FallbackClassify) != classify.N || plot.N != 8 || len(plot.FallbackPlot) != plot.N {
		t.Fatalf("N=%d/%d fallback records=%d/%d: every labelled id (including ghost) must be present",
			classify.N, plot.N, len(classify.FallbackClassify), len(plot.FallbackPlot))
	}
	hasGhost := false
	for _, f := range classify.FallbackClassify {
		if f.OK {
			t.Errorf("an errored item cannot be OK: %+v", f)
		}
		hasGhost = hasGhost || f.ID == "ghost"
	}
	if !hasGhost {
		t.Error("an id missing from the candidate file must stay in the denominator")
	}
	for _, f := range plot.FallbackPlot {
		if !f.Structural || f.OK {
			t.Errorf("errored plot record = %+v", f)
		}
	}
	run := RunResult{JevKind: "live", Tasks: []TaskResult{classify, plot, {Task: "relate", Backend: "jev"}}}
	sel := SelectThresholds(run, config.DefaultThresholds(), false)
	if !sel.Skipped || !sel.HasProjection || sel.FallbackRate != 1 || sel.FallbackMet {
		t.Errorf("projection = %+v, want 1.0 and not met", sel)
	}
	// The ghost label has no article to ask about: unanswered, not an error, and no call.
	if classify.Errors != 7 || classify.Calls != 7 || classify.Unanswered != 8 || classify.Answered != 0 {
		t.Errorf("classify = %+v", classify)
	}
}

func TestNoPlaceHarnessProjectsFullFallback(t *testing.T) {
	ds := memDataset(10)
	h := harness(ds, fakeJev(func(req decide.Request) (decide.Response, error) {
		answers := map[string]decide.Answer{}
		for k, q := range req.Questions {
			switch {
			case k == "category":
				answers[k] = choiceA("politics", 0.95)
			case k == "primary":
				answers[k] = choiceA("none", 0.95)
			case q.Type == decide.TypeChoice:
				answers[k] = choiceA("country", 0.9)
			default:
				answers[k] = noulA(0.9)
			}
		}
		return resp(answers), nil
	}))
	classify := h.runClassifyJev(context.Background())
	plot := h.runPlotJev(context.Background())
	run := RunResult{JevKind: "live", Tasks: []TaskResult{classify, plot, {Task: "relate", Backend: "jev"}}}
	sel := SelectThresholds(run, config.DefaultThresholds(), false)
	if !sel.Skipped || !sel.HasProjection || sel.FallbackRate != 1 || sel.FallbackMet {
		t.Errorf("all no-place: %+v", sel)
	}
	if len(plot.Observations) != 0 || plot.Extra["fallback_rate"] != 1 {
		t.Errorf("plot observations=%d fallback_rate=%v", len(plot.Observations), plot.Extra["fallback_rate"])
	}
}

func TestMissingPrecisionIsStructuralFallback(t *testing.T) {
	ds := memDataset(4)
	h := harness(ds, fakeJev(func(req decide.Request) (decide.Response, error) {
		return resp(map[string]decide.Answer{"primary": choiceA("c0", 0.95)}), nil // no precision answer
	}))
	plot := h.runPlotJev(context.Background())
	if len(plot.FallbackPlot) != 4 {
		t.Fatalf("records = %d", len(plot.FallbackPlot))
	}
	for _, f := range plot.FallbackPlot {
		if !f.Structural || !f.OK || f.P != 0.95 {
			t.Errorf("record = %+v, want structural with P kept", f)
		}
	}
	if plot.Extra["fallback_rate"] != 1 {
		t.Errorf("fallback_rate = %v", plot.Extra["fallback_rate"])
	}
}

type contrast struct {
	Name  string `json:"name"`
	Why   string `json:"why"`
	A     fixStory
	B     fixStory
	Label struct {
		Related   bool   `json:"related"`
		Type      string `json:"type"`
		Direction string `json:"direction"`
		Strength  int    `json:"strength"`
	} `json:"label"`
}

type fixStory struct {
	Title    string   `json:"title"`
	Brief    string   `json:"brief"`
	Entities []string `json:"entities"`
	Place    string   `json:"place"`
	Category string   `json:"category"`
}

func TestRelateContrastFixtures(t *testing.T) {
	raw, err := os.ReadFile("testdata/relate_contrast.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []contrast
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 5 {
		t.Fatalf("%d contrast cases, want 5", len(cases))
	}
	want := map[string]bool{"same event coverage": false, "specific shared theme": false, "broad coincidental topic": false, "indirect chain": false, "reversed causal pair": false}
	validType := map[string]bool{"causal": true, "reaction": true, "thematic": true, "none": true}
	validDir := map[string]bool{"a_to_b": true, "b_to_a": true, "unclear": true}
	th := config.DefaultThresholds()
	for _, c := range cases {
		if _, ok := want[c.Name]; !ok {
			t.Errorf("unexpected case %q", c.Name)
		}
		want[c.Name] = true
		l := c.Label
		if c.Why == "" || c.A.Title == "" || c.B.Title == "" || c.A.Brief == "" || c.B.Brief == "" {
			t.Errorf("%s: incomplete fixture", c.Name)
		}
		if !validType[l.Type] || !validDir[l.Direction] || l.Strength < 0 || l.Strength > 3 {
			t.Errorf("%s: invalid label %+v", c.Name, l)
		}
		if !l.Related && (l.Type != "none" || l.Direction != "unclear" || l.Strength != 0) {
			t.Errorf("%s: an unrelated pair must be type none / unclear / 0: %+v", c.Name, l)
		}
		if l.Related && l.Type == "none" {
			t.Errorf("%s: a related pair cannot be type none", c.Name)
		}
		if l.Type == "thematic" && l.Direction != "unclear" {
			t.Errorf("%s: a thematic link has no direction", c.Name)
		}

		// The builder carries both stories, and an answer that matches the label round-trips.
		a := decide.Story{Title: c.A.Title, Brief: c.A.Brief, Entities: c.A.Entities, Place: c.A.Place, Category: c.A.Category}
		b := decide.Story{Title: c.B.Title, Brief: c.B.Brief, Entities: c.B.Entities, Place: c.B.Place, Category: c.B.Category}
		req := decide.RelateRequest(a, b)
		state, _ := json.Marshal(req.State)
		if !strings.Contains(string(state), c.A.Title) || !strings.Contains(string(state), c.B.Title) {
			t.Errorf("%s: both stories must be in the state", c.Name)
		}
		answers := map[string]decide.Answer{"related": noulA(0.05)}
		if l.Related {
			answers = map[string]decide.Answer{
				"related": noulA(0.9), "type": choiceA(l.Type, 0.9), "direction": choiceA(l.Direction, 0.9), "strength": scoreA(l.Strength, 0.8),
			}
		}
		got := decide.CombineRelate(decide.Response{Answers: answers}, a, b, th)
		if got.Related != l.Related || (l.Related && (got.Type != l.Type || got.Direction != l.Direction)) {
			t.Errorf("%s: combined %+v does not reproduce the label %+v", c.Name, got, l)
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("missing contrast case %q", name)
		}
	}
	// reversed causal: the same type with the opposite direction.
	for _, c := range cases {
		if c.Name == "reversed causal pair" && (c.Label.Type != "causal" || c.Label.Direction != "b_to_a") {
			t.Errorf("reversed causal label = %+v", c.Label)
		}
	}
}

func TestRelateInstructionsEncodeTheContrasts(t *testing.T) {
	req := decide.RelateRequest(decide.Story{Title: "A"}, decide.Story{Title: "B"})
	related := req.Questions["related"].Instructions
	typ := req.Questions["type"].Instructions
	dir := req.Questions["direction"].Instructions
	strength := req.Questions["strength"].Instructions
	for name, want := range map[string][2]string{
		"same event excluded":             {related, "Coverage of the same event does NOT count"},
		"coincidental overlap excluded":   {related, "A broad or coincidental overlap"},
		"thematic included":               {related, "share a specific condition or theme"},
		"indirect included":               {related, "indirect chain"},
		"causal is symmetric":             {typ, "in either order"},
		"type none covers the same event": {typ, "none: the same event"},
		"direction explains a_to_b":       {dir, "a_to_b means story A's event acted on story B's"},
		"direction unclear for thematic":  {dir, "Use unclear for a thematic connection"},
		"strength 0 is theme only":        {strength, "0 = a specific shared condition or theme only"},
		"strength 1 is an indirect chain": {strength, "1 = an indirect chain"},
		"strength 3 is immediate":         {strength, "3 = an immediate, direct consequence"},
	} {
		if !strings.Contains(want[0], want[1]) {
			t.Errorf("%s: instruction lacks %q", name, want[1])
		}
	}
	// Each question carries everything it needs, because jev never sees keys.
	for key, q := range req.Questions {
		if strings.Contains(q.Instructions, "related question") || strings.Contains(q.Instructions, "previous question") || strings.Contains(q.Instructions, key+" question") {
			t.Errorf("%s: instruction refers to another question: %q", key, q.Instructions)
		}
		if !strings.Contains(q.Instructions, "distinct event") && key != "related" {
			t.Errorf("%s: instruction does not restate that the events are distinct: %q", key, q.Instructions)
		}
	}
	// The criteria no longer let a same-event pair or a broad overlap count as thematic.
	for _, o := range decide.RelationTypeOptions() {
		if o.Key == "thematic" && !strings.Contains(o.Definition, "specific") {
			t.Errorf("thematic definition is not specific: %q", o.Definition)
		}
		if o.Key == "none" && !strings.Contains(o.Definition, "same event") {
			t.Errorf("none definition = %q", o.Definition)
		}
	}
}

func TestClaudeDecisionConfidenceIsNotFlipped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		props := body["output_config"].(map[string]any)["format"].(map[string]any)["schema"].(map[string]any)["properties"].(map[string]any)
		var out map[string]any
		switch {
		case props["match"] != nil:
			out = map[string]any{"match": false, "confidence": 0.99}
		case props["related"] != nil:
			out = map[string]any{"related": false, "type": "none", "direction": "unclear", "strength": 0, "confidence": 0.99}
		default:
			t.Errorf("unexpected schema: %v", props)
			return
		}
		text, _ := json.Marshal(out)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "m", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
			"content":     []any{map[string]any{"type": "text", "text": string(text)}},
			"stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	ds := memDataset(20)
	for i := 0; i < 20; i++ {
		m := MatchCandidate{ID: fmt.Sprintf("m%02d", i), ArticleID: ds.Articles[i].ID, Prompt: "sport", Relevant: false}
		ds.Matches = append(ds.Matches, m)
		addPair(ds, fmt.Sprintf("neg%02d", i), PairLabel{Related: false, Type: "none", Direction: "unclear", Strength: 0})
	}
	h := harness(ds, nil)
	h.Claude, h.ClaudeKind = newClaude(t, srv.URL), "live"

	match := h.runMatchClaude(context.Background())
	if len(match.DecisionObservations) != 20 || !near(match.DecisionECE, 0.01) {
		t.Errorf("match decision observations=%d ECE=%v, want 20/.01", len(match.DecisionObservations), match.DecisionECE)
	}
	if len(match.Coverage) == 0 {
		t.Error("match decision observations should produce a coverage curve")
	}
	relate := h.runRelateClaude(context.Background())
	if len(relate.DecisionObservations) != 20 || !near(relate.DecisionECE, 0.01) {
		t.Errorf("relate decision observations=%d ECE=%v, want 20/.01", len(relate.DecisionObservations), relate.DecisionECE)
	}
	if len(relate.Coverage) == 0 {
		t.Error("relate decision observations should produce a coverage curve")
	}
}
