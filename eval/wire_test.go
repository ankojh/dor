package eval

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ankojh/dor/internal/llm"
)

var constraintWords = []string{"minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "pattern", "format"}

func findKeys(v any, keys []string, path string, out *[]string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	for k, c := range m {
		for _, bad := range keys {
			if k == bad {
				*out = append(*out, path+"."+k)
			}
		}
		if k == "properties" {
			if props, ok := c.(map[string]any); ok {
				for name, p := range props {
					findKeys(p, keys, path+".properties."+name, out)
				}
			}
			continue
		}
		findKeys(c, keys, path+"."+k, out)
	}
}

func allSevenJobs(t *testing.T) map[string]llm.Job {
	t.Helper()
	ds, err := LoadDataset("data")
	if err != nil {
		t.Fatal(err)
	}
	a := ds.Articles[0]
	for _, c := range ds.Articles {
		if len(c.Candidates) > 1 {
			a = c
			break
		}
	}
	plot, _ := claudePlotJob(a)
	return map[string]llm.Job{
		"enrich":   llm.EnricherJob(llm.Article{Title: "t", Lede: "l", Source: "s"}),
		"brief":    llm.BriefJob(llm.BriefInput{Title: "t", Sources: []llm.Source{{Name: "A", Title: "t"}}}),
		"why":      llm.WhyJob(llm.WhyInput{A: llm.LinkEnd{Title: "a"}, B: llm.LinkEnd{Title: "b"}, Type: "causal"}),
		"classify": claudeClassifyJob(a),
		"plot":     plot,
		"relate":   claudeRelateJob(ds.Articles[0], ds.Articles[1]),
		"match":    claudeMatchJob(ds.Matches[0], a),
	}
}

func TestAllSevenWireSchemasAreAPISafe(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, decodeJSON(raw))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"x","content":[{"type":"text","text":"{}"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()
	client := newClaude(t, srv.URL)

	jobs := allSevenJobs(t)
	if len(jobs) != 7 {
		t.Fatalf("jobs = %d", len(jobs))
	}
	order := []string{"enrich", "brief", "why", "classify", "plot", "relate", "match"}
	for _, name := range order {
		if _, err := client.Complete(context.Background(), jobs[name]); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if len(bodies) != 7 {
		t.Fatalf("captured %d requests", len(bodies))
	}
	sawConstraint := 0
	for i, name := range order {
		job := jobs[name]
		format := bodies[i]["output_config"].(map[string]any)["format"].(map[string]any)
		wire := format["schema"].(map[string]any)
		var found []string
		findKeys(wire, constraintWords, "", &found)
		if len(found) != 0 {
			t.Errorf("%s: the wire schema carries %v", name, found)
		}
		if bad := llm.UnsupportedSchemaKeywords(wire); len(bad) != 0 {
			t.Errorf("%s: unsupported keywords on the wire: %v", name, bad)
		}
		// Constraints in the original schema are restated in descriptions.
		var orig []string
		findKeys(job.Schema, constraintWords, "", &orig)
		if len(orig) > 0 {
			sawConstraint++
			desc := collectDescriptions(wire)
			if !strings.Contains(desc, "Minimum") && !strings.Contains(desc, "Maximum") && !strings.Contains(desc, "At most") && !strings.Contains(desc, "At least") {
				t.Errorf("%s: constraints %v are not restated in descriptions", name, orig)
			}
		}
		// The original is untouched and still enforced after decoding.
		if err := llm.ValidateValue(job.Schema, map[string]any{}); err == nil && len(job.Schema["required"].([]any)) > 0 {
			t.Errorf("%s: an empty object must violate the required fields", name)
		}
	}
	if sawConstraint == 0 {
		t.Error("none of the seven jobs carries a constraint; the test would prove nothing")
	}
}

func decodeJSON(raw []byte) map[string]any {
	var m map[string]any
	_ = jsonUnmarshal(raw, &m)
	return m
}

func collectDescriptions(v any) string {
	var b strings.Builder
	var walk func(any)
	walk = func(v any) {
		m, ok := v.(map[string]any)
		if !ok {
			return
		}
		if d, ok := m["description"].(string); ok {
			b.WriteString(d + " ")
		}
		for k, c := range m {
			if k == "properties" {
				for _, p := range c.(map[string]any) {
					walk(p)
				}
				continue
			}
			walk(c)
		}
	}
	walk(v)
	return b.String()
}

func TestEvalJobSchemasEnforceConstraintsAfterDecode(t *testing.T) {
	jobs := allSevenJobs(t)
	// Claude's self-reported confidence is bounded [0,1].
	classify := jobs["classify"]
	good := map[string]any{"category": "politics", "confidence": 0.5}
	if err := llm.ValidateValue(classify.Schema, good); err != nil {
		t.Fatalf("valid classify output rejected: %v", err)
	}
	for _, bad := range []float64{-0.1, 1.5} {
		err := llm.ValidateValue(classify.Schema, map[string]any{"category": "politics", "confidence": bad})
		var se *llm.SchemaError
		if !errors.As(err, &se) || se.Path != "confidence" {
			t.Errorf("confidence %v: err = %v, want a *llm.SchemaError at confidence", bad, err)
		}
	}
	// The plot parties list is capped at the number of candidate keys.
	plot := jobs["plot"]
	props := plot.Schema["properties"].(map[string]any)
	parties := props["parties"].(map[string]any)
	max, _ := parties["maxItems"].(int)
	if max == 0 {
		t.Fatalf("parties has no maxItems: %v", parties)
	}
	tooMany := make([]any, max+1)
	for i := range tooMany {
		tooMany[i] = "c0"
	}
	err := llm.ValidateValue(plot.Schema, map[string]any{"primary": "c0", "precision": "city", "parties": tooMany, "confidence": 0.5})
	var se *llm.SchemaError
	if !errors.As(err, &se) {
		t.Errorf("an oversized parties list must be a *llm.SchemaError, got %v", err)
	}
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

func TestAllSevenSchemasPassContract(t *testing.T) {
	for name, job := range allSevenJobs(t) {
		if err := llm.ValidateSchema(job.Schema); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
