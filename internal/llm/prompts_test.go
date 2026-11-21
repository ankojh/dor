package llm

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/decide"
)

func productionJobs() map[string]Job {
	return map[string]Job{
		"enrich": EnricherJob(Article{Title: "Flood hits Jakarta", Lede: "Thousands displaced", Dateline: "JAKARTA", Source: "Outlet"}),
		"brief":  BriefJob(BriefInput{Title: "Flood", Category: "disaster", Place: "Jakarta", Sources: []Source{{Name: "A", Title: "t", Lede: "l"}}}),
		"why":    WhyJob(WhyInput{A: LinkEnd{Title: "a"}, B: LinkEnd{Title: "b"}, Type: "causal", Direction: "a_to_b", Strength: 0.8}),
	}
}

func TestProductionSystemPromptsAreCacheable(t *testing.T) {
	for name, job := range productionJobs() {
		if n := SystemPromptLength(job); n < 2048 {
			t.Errorf("%s: system prompt is %d bytes, want >= 2048", name, n)
		}
		// Conservative 4 bytes/token: still above the 512-token minimum.
		if SystemPromptLength(job)/4 < 512 {
			t.Errorf("%s: under the minimum cacheable prefix", name)
		}
		if !strings.Contains(job.System, injectGuard) {
			t.Errorf("%s: missing the prompt-injection guard", name)
		}
		if strings.Contains(job.System, job.User) {
			t.Errorf("%s: user text is inside the stable prefix", name)
		}
	}
}

func TestProductionJobTiersAndFallbacks(t *testing.T) {
	jobs := productionJobs()
	cases := []struct {
		name      string
		tier      Tier
		fallbacks bool
		priority  budget.Priority
	}{
		{"enrich", TierHaiku, false, ""}, // empty: Complete applies PriorityDefault
		{"brief", TierOpus, true, budget.PriorityBrief},
		{"why", TierOpus, true, budget.PriorityWhy},
	}
	for _, c := range cases {
		j := jobs[c.name]
		if j.Name != c.name || j.Tier != c.tier || j.Fallbacks != c.fallbacks || j.Priority != c.priority {
			t.Errorf("%s: %+v", c.name, struct {
				Name      string
				Tier      Tier
				Fallbacks bool
				Priority  budget.Priority
			}{j.Name, j.Tier, j.Fallbacks, j.Priority})
		}
		if j.MaxTokens <= 0 || j.Model != "" {
			t.Errorf("%s: max_tokens=%d model=%q (tier picks the model)", c.name, j.MaxTokens, j.Model)
		}
		// Only Opus gets server-side fallbacks.
		if j.Fallbacks && j.Tier != TierOpus {
			t.Errorf("%s: fallbacks on a non-Opus tier", c.name)
		}
	}
}

func TestProductionSchemasAreStrict(t *testing.T) {
	for name, job := range productionJobs() {
		s := job.Schema
		if s["type"] != "object" || s["additionalProperties"] != false {
			t.Errorf("%s: schema must be a closed object", name)
		}
		props := s["properties"].(map[string]any)
		required := s["required"].([]any)
		if len(required) != len(props) {
			t.Errorf("%s: every property must be required (%d vs %d)", name, len(required), len(props))
		}
		for _, r := range required {
			if _, ok := props[r.(string)]; !ok {
				t.Errorf("%s: required %v is not a property", name, r)
			}
		}
		if _, err := json.Marshal(s); err != nil {
			t.Errorf("%s: schema does not marshal: %v", name, err)
		}
	}
	enum := EnrichmentSchema()["properties"].(map[string]any)["category"].(map[string]any)["enum"].([]any)
	if len(enum) != 11 {
		t.Errorf("category enum has %d values", len(enum))
	}
}

func TestCategoriesMatchDecide(t *testing.T) {
	want := decide.CategoryOptions()
	if len(categoryDefs) != len(want) {
		t.Fatalf("%d categories vs decide's %d", len(categoryDefs), len(want))
	}
	for i, o := range want {
		if categoryDefs[i].Key != o.Key || categoryDefs[i].Definition != o.Definition {
			t.Errorf("category %d drifted: llm %+v vs decide %+v", i, categoryDefs[i], o)
		}
	}
	keys := CategoryKeys()
	for i, k := range decide.CategoryKeys() {
		if keys[i] != k {
			t.Errorf("CategoryKeys[%d] = %s, want %s", i, keys[i], k)
		}
	}
	// The enum in the schema and the prompt list the same keys.
	enum := categoryEnum()
	prompt := enricherSystem
	for i, k := range decide.CategoryKeys() {
		if enum[i] != k {
			t.Errorf("enum[%d] = %v", i, enum[i])
		}
		if !strings.Contains(prompt, "- "+k+":") {
			t.Errorf("enricher prompt does not define %s", k)
		}
	}
}

func TestEnricherUserMessage(t *testing.T) {
	job := EnricherJob(Article{Title: "  Headline  ", Lede: "The lede", Dateline: "PARIS", Source: "Src"})
	for _, want := range []string{"Headline: Headline\n", "Dateline: PARIS\n", "Source: Src\n", "Lede: The lede\n"} {
		if !strings.Contains(job.User, want) {
			t.Errorf("user message lacks %q:\n%s", want, job.User)
		}
	}
	if strings.Contains(EnricherJob(Article{Title: "t"}).User, "Dateline:") {
		t.Error("an empty dateline is omitted")
	}
}

func TestBriefUserMessageCapsSources(t *testing.T) {
	var sources []Source
	for i := 0; i < 12; i++ {
		sources = append(sources, Source{Name: "Outlet" + string(rune('A'+i)), Title: "T", Lede: "L"})
	}
	job := BriefJob(BriefInput{Title: "Event", Sources: sources})
	if strings.Contains(job.User, "[9]") || !strings.Contains(job.User, "[8]") {
		t.Errorf("at most eight sources are sent:\n%s", job.User)
	}
	if strings.Contains(job.User, "Category:") || strings.Contains(job.User, "Place:") {
		t.Error("empty category and place are omitted")
	}
}

func TestWhyUserMessage(t *testing.T) {
	job := WhyJob(WhyInput{A: LinkEnd{Title: "A", Place: "Oslo", Brief: "ab"}, B: LinkEnd{Title: "B"}, Type: "reaction", Direction: "b_to_a", Strength: 0.5})
	for _, want := range []string{"Story A:", "Place: Oslo", "Brief: ab", "Story B:", "Relation: reaction, direction b_to_a, strength 0.50."} {
		if !strings.Contains(job.User, want) {
			t.Errorf("lacks %q:\n%s", want, job.User)
		}
	}
}

func TestEstimateJobTokens(t *testing.T) {
	job := EnricherJob(Article{Title: "t"})
	in, out := EstimateJobTokens(job)
	if out != job.MaxTokens || in < int64(len(job.System)/4) {
		t.Errorf("estimate = %d/%d", in, out)
	}
}
