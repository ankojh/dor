package mockapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/llm/mockapi"
)

const (
	haiku = "claude-haiku-5-5"
	opus  = "claude-opus-5-5"
)

func client(t *testing.T, opts mockapi.Options) *llm.Client {
	t.Helper()
	srv := httptest.NewServer(mockapi.NewHandler(opts))
	t.Cleanup(srv.Close)
	cfg := config.DefaultBudget()
	cfg.DailyUSD = 1e6
	for k, v := range cfg.TokenBuckets {
		v.Capacity = 1e12
		cfg.TokenBuckets[k] = v
	}
	c, err := llm.New(llm.Config{
		APIKey: "mock", BaseURL: srv.URL, HaikuModel: haiku, OpusModel: opus,
		MaxRetries: 0, Timeout: 10 * time.Second,
		Price: config.DefaultModels().Claude.Price,
	}, budget.NewMemory(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func job(system, user string, schema map[string]any) llm.Job {
	if schema == nil {
		schema = llm.BriefSchema()
	}
	return llm.Job{Name: "t", Tier: llm.TierHaiku, System: system, User: user, Schema: schema, MaxTokens: 300}
}

var longSystem = strings.Repeat("stable system text ", 200)

func noDelay() mockapi.Options {
	o := mockapi.DefaultOptions()
	o.Latency = 0
	return o
}

func TestCacheReadOnSecondIdenticalPrefix(t *testing.T) {
	c := client(t, noDelay())
	ctx := context.Background()
	first, err := c.Complete(ctx, job(longSystem, "item one", nil))
	if err != nil {
		t.Fatal(err)
	}
	if first.Usage.CacheCreationInputTokens <= 0 || first.Usage.CacheReadInputTokens != 0 {
		t.Errorf("first call: %+v, want a cache write only", first.Usage)
	}
	second, err := c.Complete(ctx, job(longSystem, "item two, different volatile text", nil))
	if err != nil {
		t.Fatal(err)
	}
	if second.Usage.CacheReadInputTokens <= 0 || second.Usage.CacheCreationInputTokens != 0 {
		t.Errorf("second call: %+v, want a cache read only", second.Usage)
	}
	if second.Usage.CacheReadInputTokens != first.Usage.CacheCreationInputTokens {
		t.Errorf("read %d != written %d", second.Usage.CacheReadInputTokens, first.Usage.CacheCreationInputTokens)
	}
	// A different prefix (system text or schema) is a miss.
	other, err := c.Complete(ctx, job(longSystem+"changed", "item", nil))
	if err != nil {
		t.Fatal(err)
	}
	if other.Usage.CacheReadInputTokens != 0 || other.Usage.CacheCreationInputTokens <= 0 {
		t.Errorf("changed system prefix: %+v", other.Usage)
	}
	other2, err := c.Complete(ctx, job(longSystem, "item", llm.WhySchema()))
	if err != nil {
		t.Fatal(err)
	}
	if other2.Usage.CacheReadInputTokens != 0 {
		t.Errorf("changed schema prefix: %+v", other2.Usage)
	}
}

func TestCacheSimulationOff(t *testing.T) {
	o := noDelay()
	o.CacheSimulation = false
	c := client(t, o)
	for i := 0; i < 2; i++ {
		res, err := c.Complete(context.Background(), job(longSystem, "x", nil))
		if err != nil {
			t.Fatal(err)
		}
		if res.Usage.CacheReadInputTokens != 0 || res.Usage.CacheCreationInputTokens != 0 {
			t.Errorf("call %d: %+v", i, res.Usage)
		}
	}
}

func TestRefusalEvery(t *testing.T) {
	o := noDelay()
	o.RefusalEvery = 2
	c := client(t, o)
	if _, err := c.Complete(context.Background(), job(longSystem, "a", nil)); err != nil {
		t.Fatalf("first call: %v", err)
	}
	res, err := c.Complete(context.Background(), job(longSystem, "b", nil))
	var se *llm.StopError
	if !errors.Is(err, llm.ErrRefusal) || !errors.As(err, &se) {
		t.Fatalf("second call err = %v", err)
	}
	if se.Category != "general_harms" || se.Explanation != "mock refusal" || res.Text != "" {
		t.Errorf("stop error = %+v result=%+v", se, res)
	}
	if _, err := c.Complete(context.Background(), job(longSystem, "c", nil)); err != nil {
		t.Fatalf("third call: %v", err)
	}
}

func TestMaxTokensEvery(t *testing.T) {
	o := noDelay()
	o.MaxTokensEvery = 1
	c := client(t, o)
	_, err := c.Complete(context.Background(), job(longSystem, "a", nil))
	if !errors.Is(err, llm.ErrMaxTokens) {
		t.Errorf("err = %v", err)
	}
}

func TestResponsesAreDeterministicPerRequest(t *testing.T) {
	c := client(t, noDelay())
	a, err := c.Complete(context.Background(), job(longSystem, "same", nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Complete(context.Background(), job(longSystem, "same", nil))
	if err != nil {
		t.Fatal(err)
	}
	if a.Text != b.Text {
		t.Errorf("same request, different answers: %s vs %s", a.Text, b.Text)
	}
	d, _ := c.Complete(context.Background(), job(longSystem, "different", nil))
	if a.Text == d.Text {
		t.Log("two different requests happened to return the same title; acceptable but unlikely")
	}
}

func TestSchemaDrivenOutputRespectsConstraints(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"cat", "n", "x", "flag", "tags", "nested", "title", "brief", "why", "free"},
		"properties": map[string]any{
			"cat":    map[string]any{"type": "string", "enum": []any{"a", "b", "c"}},
			"n":      map[string]any{"type": "integer", "minimum": 3, "maximum": 5},
			"x":      map[string]any{"type": "number", "minimum": -1.5, "maximum": -0.5},
			"flag":   map[string]any{"type": "boolean"},
			"tags":   map[string]any{"type": "array", "maxItems": 1, "items": map[string]any{"type": "string", "enum": []any{"p", "q"}}},
			"nested": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"score"}, "properties": map[string]any{"score": map[string]any{"type": "integer", "minimum": 7, "maximum": 7}}},
			"title":  map[string]any{"type": "string"},
			"brief":  map[string]any{"type": "string"},
			"why":    map[string]any{"type": "string"},
			"free":   map[string]any{"type": "string"},
		},
	}
	c := client(t, noDelay())
	seenCat := map[string]bool{}
	for i := 0; i < 60; i++ {
		res, err := c.Complete(context.Background(), job(longSystem, fmt.Sprintf("item %d", i), schema))
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Cat    string   `json:"cat"`
			N      int      `json:"n"`
			X      float64  `json:"x"`
			Flag   bool     `json:"flag"`
			Tags   []string `json:"tags"`
			Nested struct {
				Score int `json:"score"`
			} `json:"nested"`
			Title, Brief, Why, Free string
		}
		dec := json.NewDecoder(strings.NewReader(res.Text))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&out); err != nil {
			t.Fatalf("output %q does not decode: %v", res.Text, err)
		}
		if out.Cat != "a" && out.Cat != "b" && out.Cat != "c" {
			t.Errorf("enum violated: %q", out.Cat)
		}
		seenCat[out.Cat] = true
		if out.N < 3 || out.N > 5 {
			t.Errorf("integer bounds violated: %d", out.N)
		}
		if out.X < -1.5 || out.X > -0.5 {
			t.Errorf("number bounds violated: %v", out.X)
		}
		if len(out.Tags) != 1 || (out.Tags[0] != "p" && out.Tags[0] != "q") {
			t.Errorf("array/maxItems/items enum violated: %v", out.Tags)
		}
		if out.Nested.Score != 7 {
			t.Errorf("nested bounds violated: %d", out.Nested.Score)
		}
		if !strings.HasPrefix(out.Title, "Mock headline") || out.Brief == "" || out.Why == "" || out.Free != "mock free" {
			t.Errorf("string fields = %q %q %q %q", out.Title, out.Brief, out.Why, out.Free)
		}
	}
	if len(seenCat) < 2 {
		t.Errorf("enum values never vary: %v", seenCat)
	}
}

func TestProductionJobsGetValidOutput(t *testing.T) {
	c := client(t, noDelay())
	ctx := context.Background()
	res, err := c.Complete(ctx, llm.EnricherJob(llm.Article{Title: "Flood in Jakarta"}))
	if err != nil {
		t.Fatal(err)
	}
	var e llm.Enrichment
	if err := res.Decode(&e); err != nil {
		t.Fatal(err)
	}
	valid := false
	for _, k := range llm.CategoryKeys() {
		valid = valid || e.Category == k
	}
	if !valid || len(e.Entities) == 0 || len(e.Entities) > 8 {
		t.Errorf("enrichment = %+v", e)
	}
	res, err = c.Complete(ctx, llm.BriefJob(llm.BriefInput{Title: "t"}))
	if err != nil {
		t.Fatal(err)
	}
	var d llm.Draft
	if err := res.Decode(&d); err != nil || d.Title == "" || d.Brief == "" {
		t.Errorf("draft = %+v %v", d, err)
	}
	res, err = c.Complete(ctx, llm.WhyJob(llm.WhyInput{}))
	if err != nil {
		t.Fatal(err)
	}
	var w llm.Explanation
	if err := res.Decode(&w); err != nil || w.Why == "" {
		t.Errorf("why = %+v %v", w, err)
	}
	// The Opus jobs went through the fallback header path without a 400.
}

func TestHTTPSurface(t *testing.T) {
	srv := httptest.NewServer(mockapi.NewHandler(noDelay()))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/messages", "application/json", bytes.NewReader([]byte("{nope")))
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Type  string `json:"type"`
		Error struct{ Type, Message string }
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if resp.StatusCode != 400 || body.Type != "error" || body.Error.Type != "invalid_request_error" {
		t.Errorf("bad JSON: %d %+v", resp.StatusCode, body)
	}

	resp, err = http.Get(srv.URL + "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d", resp.StatusCode)
	}
	resp, err = http.Post(srv.URL+"/v1/other", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path = %d", resp.StatusCode)
	}

	// A bad schema is a 400 as well.
	resp, err = http.Post(srv.URL+"/v1/messages", "application/json",
		bytes.NewReader([]byte(`{"model":"m","max_tokens":1,"messages":[],"output_config":{"format":{"type":"json_schema","schema":"oops"}}}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("bad schema = %d", resp.StatusCode)
	}
}

func TestRawConstrainedSchemaIsA400(t *testing.T) {
	srv := httptest.NewServer(mockapi.NewHandler(noDelay()))
	defer srv.Close()
	for _, kw := range []string{`"minimum":1`, `"maximum":5`, `"maxLength":9`, `"minLength":1`, `"maxItems":2`, `"minItems":1`, `"pattern":"x"`, `"format":"date"`} {
		body := `{"model":"m","max_tokens":10,"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":"u"}],` +
			`"output_config":{"effort":"low","format":{"type":"json_schema","schema":{"type":"object","properties":{"a":{"type":"integer",` + kw + `}}}}}}`
		resp, err := http.Post(srv.URL+"/v1/messages", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Error struct{ Type, Message string }
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != 400 || out.Error.Type != "invalid_request_error" || !strings.Contains(out.Error.Message, "unsupported schema keyword") {
			t.Errorf("%s: status %d %+v, want a 400 naming the keyword", kw, resp.StatusCode, out)
		}
	}
	// The same schema without constraints is accepted.
	ok := `{"model":"m","max_tokens":10,"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":"u"}],` +
		`"output_config":{"effort":"low","format":{"type":"json_schema","schema":{"type":"object","additionalProperties":false,"required":["a"],"properties":{"a":{"type":"integer"}}}}}}`
	resp, err := http.Post(srv.URL+"/v1/messages", "application/json", bytes.NewReader([]byte(ok)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("clean schema = %d", resp.StatusCode)
	}
}

func TestOpenObjectSchemaIsA400(t *testing.T) {
	srv := httptest.NewServer(mockapi.NewHandler(mockapi.DefaultOptions()))
	defer srv.Close()
	body := `{"model":"m","max_tokens":10,"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":"u"}],` +
		`"output_config":{"effort":"low","format":{"type":"json_schema","schema":{"type":"object","additionalProperties":true,"required":["a"],"properties":{"a":{"type":"string"}}}}}}`
	resp, err := http.Post(srv.URL+"/v1/messages", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("open object schema status = %d, want 400", resp.StatusCode)
	}
}
