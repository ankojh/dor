package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
)

const (
	haiku = "claude-haiku-5-5"
	opus  = "claude-opus-5-5"
)

var testPrices = map[string]config.Price{
	haiku: {InputPerMTok: 1, OutputPerMTok: 5},
	opus:  {InputPerMTok: 15, OutputPerMTok: 75},
}

type recordCall struct {
	Res   budget.Reservation
	Usage budget.Usage
}

type fakeBudget struct {
	mu       sync.Mutex
	checks   []budget.Request
	records  []recordCall
	denyFrom int
	reason   string
	events   []string
}

func (f *fakeBudget) Check(_ context.Context, r budget.Request) (budget.Verdict, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = append(f.checks, r)
	f.events = append(f.events, "check")
	if f.denyFrom > 0 && len(f.checks) >= f.denyFrom {
		reason := f.reason
		if reason == "" {
			reason = "daily-cap"
		}
		return budget.Verdict{Allowed: false, Reason: reason}, nil
	}
	n := len(f.checks)
	return budget.Verdict{Allowed: true, Reason: "ok", Reservation: &budget.Reservation{
		ID: fmt.Sprintf("res-%d", n), API: r.API, Tokens: r.EstimatedInputTokens + r.EstimatedOutputTokens, USD: r.EstimatedUSD,
	}}, nil
}

func (f *fakeBudget) Record(_ context.Context, res budget.Reservation, u budget.Usage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, recordCall{res, u})
	f.events = append(f.events, "record")
	return nil
}

type sleeper struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *sleeper) Sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.d = append(s.d, d)
	return nil
}

// captured is one request the fake endpoint saw.
type captured struct {
	Path   string
	Header http.Header
	Raw    []byte
	Body   map[string]any
}

type fakeAPI struct {
	t    *testing.T
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []captured
	// handler returns the status and body for request n (1-based).
	handler func(n int) (int, string)
}

func newFakeAPI(t *testing.T, handler func(n int) (int, string)) *fakeAPI {
	t.Helper()
	f := &fakeAPI{t: t, handler: handler}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		f.mu.Lock()
		f.reqs = append(f.reqs, captured{Path: r.URL.Path, Header: r.Header.Clone(), Raw: raw, Body: body})
		n := len(f.reqs)
		f.mu.Unlock()
		status, resp := f.handler(n)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) hits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func (f *fakeAPI) req(i int) captured {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[i]
}

type msgOpts struct {
	stop        string
	text        string
	noContent   bool
	details     string
	in, out     int
	cacheRead   int
	cacheCreate int
	model       string
}

// message renders a realistic beta Messages response.
func message(o msgOpts) string {
	if o.model == "" {
		o.model = opus
	}
	content := `[{"type":"text","text":` + mustJSON(o.text) + `}]`
	if o.noContent {
		content = `[]`
	}
	details := ""
	if o.details != "" {
		details = `"stop_details":` + o.details + `,`
	}
	return fmt.Sprintf(`{
  "id": "msg_test_1", "type": "message", "role": "assistant", "model": %q,
  "content": %s, "stop_reason": %q, "stop_sequence": null, %s
  "usage": {
    "input_tokens": %d, "output_tokens": %d,
    "cache_creation_input_tokens": %d, "cache_read_input_tokens": %d,
    "cache_creation": {"ephemeral_5m_input_tokens": %d, "ephemeral_1h_input_tokens": 0},
    "service_tier": "standard"
  }
}`, o.model, content, o.stop, details, o.in, o.out, o.cacheCreate, o.cacheRead, o.cacheCreate)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func okMessage(text string) string {
	return message(msgOpts{stop: "end_turn", text: text, in: 2100, out: 40, cacheRead: 1800, cacheCreate: 200})
}

func newTestClient(t *testing.T, url string, b Budget, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		APIKey:     "sk-test-key",
		BaseURL:    url,
		HaikuModel: haiku,
		OpusModel:  opus,
		MaxRetries: 2,
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		Price:      testPrices,
		Jitter:     func() float64 { return 0.5 },
		Sleep:      func(context.Context, time.Duration) error { return nil },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := New(cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testJob() Job {
	return Job{
		Name:      "unit",
		Tier:      TierHaiku,
		System:    strings.Repeat("Stable instructions. ", 150),
		User:      "VOLATILE ITEM TEXT 42",
		Schema:    map[string]any{"type": "object", "additionalProperties": false, "required": []any{"ok"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}},
		MaxTokens: 256,
	}
}

func TestCompleteRequestShape(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{"ok":true}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	job := testJob()
	res, err := c.Complete(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != `{"ok":true}` || res.StopReason != "end_turn" || res.Attempts != 1 {
		t.Errorf("result = %+v", res)
	}
	var out struct{ OK bool }
	if err := res.Decode(&out); err != nil || !out.OK {
		t.Errorf("decode = %+v %v", out, err)
	}

	r := api.req(0)
	if r.Path != "/v1/messages" {
		t.Errorf("path = %q, want the Messages endpoint", r.Path)
	}
	if !strings.Contains(r.Header.Get("X-Api-Key")+r.Header.Get("Authorization"), "sk-test-key") {
		t.Errorf("the API key is not sent: %v", r.Header)
	}
	if r.Body["model"] != haiku || r.Body["max_tokens"] != float64(256) {
		t.Errorf("model/max_tokens = %v / %v", r.Body["model"], r.Body["max_tokens"])
	}

	// One stable system block carrying the cache breakpoint.
	system, ok := r.Body["system"].([]any)
	if !ok || len(system) != 1 {
		t.Fatalf("system = %v", r.Body["system"])
	}
	blk := system[0].(map[string]any)
	if blk["type"] != "text" || blk["text"] != job.System {
		t.Errorf("system block = %v", blk["type"])
	}
	cc, _ := blk["cache_control"].(map[string]any)
	if cc["type"] != "ephemeral" {
		t.Errorf("cache_control = %v, want ephemeral on the last system block", blk["cache_control"])
	}

	// Volatile text only in the user message.
	if bytes.Contains([]byte(blk["text"].(string)), []byte("VOLATILE")) {
		t.Error("volatile text leaked into the system prompt")
	}
	msgs := r.Body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages = %v", msgs)
	}
	m0 := msgs[0].(map[string]any)
	if m0["role"] != "user" || !strings.Contains(mustJSON(m0["content"]), "VOLATILE ITEM TEXT 42") {
		t.Errorf("message = %v", m0)
	}
	if strings.Count(string(r.Raw), "VOLATILE ITEM TEXT 42") != 1 {
		t.Error("the item text must appear exactly once, in messages")
	}

	// Structured output with explicit effort, and none of the forbidden knobs.
	oc := r.Body["output_config"].(map[string]any)
	if oc["effort"] != "low" {
		t.Errorf("effort = %v, want low", oc["effort"])
	}
	format := oc["format"].(map[string]any)
	if format["type"] != "json_schema" {
		t.Errorf("format type = %v", format["type"])
	}
	if mustJSON(format["schema"]) != mustJSON(job.Schema) {
		t.Errorf("schema = %v, want the job's schema", mustJSON(format["schema"]))
	}
	for _, key := range []string{"thinking", "tool_choice", "tools", "fallbacks", "temperature", "top_p", "top_k"} {
		if _, present := r.Body[key]; present {
			t.Errorf("request must not carry %q", key)
		}
	}
	if strings.Contains(string(r.Raw), "budget_tokens") {
		t.Error("request must not carry thinking.budget_tokens")
	}
	if strings.Contains(r.Header.Get("Anthropic-Beta"), "server-side-fallback") {
		t.Errorf("a Haiku job must not enable server-side fallback: %q", r.Header.Get("Anthropic-Beta"))
	}
}

func TestNoAssistantPrefill(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	for _, job := range []Job{testJob(), BriefJob(BriefInput{Title: "t"}), WhyJob(WhyInput{})} {
		if _, err := c.Complete(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < api.hits(); i++ {
		for _, m := range api.req(i).Body["messages"].([]any) {
			if m.(map[string]any)["role"] != "user" {
				t.Errorf("request %d has a non-user message (prefill): %v", i, m)
			}
		}
	}
}

func TestEffortIsAlwaysExplicit(t *testing.T) {
	cases := map[string]string{"": "low", "low": "low", "medium": "medium", "high": "high", "xhigh": "xhigh", "max": "max"}
	for cfgEffort, want := range cases {
		api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
		c := newTestClient(t, api.srv.URL, &fakeBudget{}, func(cfg *Config) { cfg.Effort = cfgEffort })
		for _, job := range []Job{testJob(), WhyJob(WhyInput{})} {
			if _, err := c.Complete(context.Background(), job); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 2; i++ {
			got := api.req(i).Body["output_config"].(map[string]any)["effort"]
			if got != want {
				t.Errorf("config %q: request %d effort = %v, want %s", cfgEffort, i, got, want)
			}
		}
	}
	if _, err := New(Config{APIKey: "k", HaikuModel: haiku, OpusModel: opus, Effort: "extreme"}, &fakeBudget{}); err == nil {
		t.Error("an invalid effort must be rejected")
	}
}

func TestFallbackHeaderAndBody(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{"title":"t","brief":"b"}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	if _, err := c.Complete(context.Background(), BriefJob(BriefInput{Title: "t"})); err != nil {
		t.Fatal(err)
	}
	r := api.req(0)
	if r.Body["model"] != opus {
		t.Errorf("model = %v", r.Body["model"])
	}
	if !strings.Contains(r.Header.Get("Anthropic-Beta"), "server-side-fallback-2026-07-01") {
		t.Errorf("anthropic-beta = %q", r.Header.Get("Anthropic-Beta"))
	}
	if r.Body["fallbacks"] != "default" {
		t.Errorf("fallbacks = %v, want \"default\"", r.Body["fallbacks"])
	}
	if _, ok := r.Body["thinking"]; ok {
		t.Error("an Opus job must not send thinking")
	}
	if effort := r.Body["output_config"].(map[string]any)["effort"]; effort != "low" {
		t.Errorf("effort on Opus = %v", effort)
	}
}

func TestFallbacksRejectedOnHaikuBeforeAnyRequest(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb := &fakeBudget{}
	c := newTestClient(t, api.srv.URL, fb, nil)
	job := testJob()
	job.Fallbacks = true
	_, err := c.Complete(context.Background(), job)
	if err == nil || !strings.Contains(err.Error(), "fallback") {
		t.Fatalf("err = %v", err)
	}
	if api.hits() != 0 || len(fb.checks) != 0 {
		t.Errorf("hits=%d checks=%d: rejection must precede the budget and the network", api.hits(), len(fb.checks))
	}
	// An explicit Model override counts too.
	job = testJob()
	job.Tier, job.Model, job.Fallbacks = TierOpus, haiku, true
	if _, err := c.Complete(context.Background(), job); err == nil {
		t.Error("fallbacks on an explicit Haiku model must be rejected")
	}
	// ... and an explicit Opus model with a Haiku tier is fine.
	job.Model = opus
	if _, err := c.Complete(context.Background(), job); err != nil {
		t.Errorf("explicit Opus with fallbacks: %v", err)
	}
}

func TestJobValidationAndModelSelection(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)

	noSchema := testJob()
	noSchema.Schema = nil
	if _, err := c.Complete(context.Background(), noSchema); err == nil {
		t.Error("a job without a schema must be rejected")
	}
	noMax := testJob()
	noMax.MaxTokens = 0
	if _, err := c.Complete(context.Background(), noMax); err == nil {
		t.Error("a job without max_tokens must be rejected")
	}
	unpriced := testJob()
	unpriced.Model = "claude-unknown-1-0"
	if _, err := c.Complete(context.Background(), unpriced); err == nil || !strings.Contains(err.Error(), "price") {
		t.Errorf("an unpriced model must be rejected: %v", err)
	}
	if api.hits() != 0 {
		t.Errorf("invalid jobs reached the network: %d", api.hits())
	}

	for tier, want := range map[Tier]string{TierHaiku: haiku, TierOpus: opus, "": haiku} {
		job := testJob()
		job.Tier = tier
		if _, err := c.Complete(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		if got := api.req(api.hits() - 1).Body["model"]; got != want {
			t.Errorf("tier %q → model %v, want %s", tier, got, want)
		}
	}
	explicit := testJob()
	explicit.Model = opus
	if _, err := c.Complete(context.Background(), explicit); err != nil {
		t.Fatal(err)
	}
	if got := api.req(api.hits() - 1).Body["model"]; got != opus {
		t.Errorf("explicit model = %v", got)
	}
}

func TestPrefixIsByteStable(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	job := BriefJob(BriefInput{Title: "Same", Category: "politics", Place: "Oslo", Sources: []Source{{Name: "A", Title: "t", Lede: "l"}}})
	for i := 0; i < 3; i++ {
		if _, err := c.Complete(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	first := api.req(0).Raw
	for i := 1; i < 3; i++ {
		if !bytes.Equal(first, api.req(i).Raw) {
			t.Fatalf("request %d differs from request 0:\n%s\n%s", i, first, api.req(i).Raw)
		}
	}

	// With a different item, everything before the user message is identical.
	job2 := job
	job2.User = "A different item"
	if _, err := c.Complete(context.Background(), job2); err != nil {
		t.Fatal(err)
	}
	a, b := api.req(0).Body, api.req(3).Body
	for _, key := range []string{"model", "system", "output_config", "max_tokens", "fallbacks"} {
		if mustJSON(a[key]) != mustJSON(b[key]) {
			t.Errorf("%s changed with the item: %s vs %s", key, mustJSON(a[key]), mustJSON(b[key]))
		}
	}
	if mustJSON(a["messages"]) == mustJSON(b["messages"]) {
		t.Error("the message must carry the volatile part")
	}
}

func TestJobSchemaIsNotMutated(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	job := testJob()
	before := mustJSON(job.Schema)
	if _, err := c.Complete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if mustJSON(job.Schema) != before {
		t.Error("Complete mutated the caller's schema")
	}
}

func TestStopReasons(t *testing.T) {
	t.Run("end_turn decodes the first text block", func(t *testing.T) {
		multi := `{"id":"m","type":"message","role":"assistant","model":"` + haiku + `","stop_reason":"end_turn",
		  "content":[{"type":"thinking","thinking":"hmm","signature":"s"},{"type":"text","text":"{\"a\":1}"},{"type":"text","text":"ignored"}],
		  "usage":{"input_tokens":1,"output_tokens":1}}`
		api := newFakeAPI(t, func(int) (int, string) { return 200, multi })
		res, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob())
		if err != nil || res.Text != `{"a":1}` {
			t.Fatalf("res=%+v err=%v", res, err)
		}
	})

	t.Run("refusal carries stop_details and never decodes content", func(t *testing.T) {
		body := message(msgOpts{stop: "refusal", text: `{"ok":true}`, in: 50, out: 3,
			details: `{"type":"refusal","category":"cyber","explanation":"Request looked like malware development."}`})
		api := newFakeAPI(t, func(int) (int, string) { return 200, body })
		fb := &fakeBudget{}
		res, err := newTestClient(t, api.srv.URL, fb, nil).Complete(context.Background(), testJob())
		var se *StopError
		if !errors.As(err, &se) || !errors.Is(err, ErrRefusal) {
			t.Fatalf("err = %v", err)
		}
		if se.Category != "cyber" || se.Explanation != "Request looked like malware development." || se.StopReason != "refusal" {
			t.Errorf("stop error = %+v", se)
		}
		if res.Refusal != "cyber" || res.Explanation == "" || res.Text != "" {
			t.Errorf("result = %+v: refusal text must not be exposed as content", res)
		}
		if !strings.Contains(err.Error(), "cyber") {
			t.Errorf("message = %q", err.Error())
		}
		// The tokens were spent, so they are recorded.
		if len(fb.records) != 1 || fb.records[0].Usage.InputTokens != 50 {
			t.Errorf("records = %+v", fb.records)
		}
		if errors.Is(err, ErrMaxTokens) {
			t.Error("refusal is not max_tokens")
		}
	})

	t.Run("refusal is not retried", func(t *testing.T) {
		api := newFakeAPI(t, func(int) (int, string) {
			return 200, message(msgOpts{stop: "refusal", noContent: true, details: `{"type":"refusal","category":"cbrn","explanation":"x"}`})
		})
		_, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob())
		if !errors.Is(err, ErrRefusal) || api.hits() != 1 {
			t.Errorf("err=%v hits=%d", err, api.hits())
		}
	})

	t.Run("max_tokens", func(t *testing.T) {
		api := newFakeAPI(t, func(int) (int, string) {
			return 200, message(msgOpts{stop: "max_tokens", text: `{"ok":tr`, in: 10, out: 256})
		})
		res, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob())
		var se *StopError
		if !errors.Is(err, ErrMaxTokens) || !errors.As(err, &se) || se.StopReason != "max_tokens" {
			t.Fatalf("err = %v", err)
		}
		if res.Text != "" {
			t.Errorf("a truncated response must not be exposed: %q", res.Text)
		}
		if errors.Is(err, ErrRefusal) {
			t.Error("max_tokens is not a refusal")
		}
	})

	t.Run("unknown stop_reason", func(t *testing.T) {
		for _, stop := range []string{"pause_turn", "tool_use", "stop_sequence", "model_context_window_exceeded"} {
			api := newFakeAPI(t, func(int) (int, string) { return 200, message(msgOpts{stop: stop, text: `{"ok":true}`}) })
			res, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob())
			var se *StopError
			if !errors.Is(err, ErrUnexpectedStop) || !errors.As(err, &se) || se.StopReason != stop {
				t.Errorf("%s: err = %v", stop, err)
			}
			if res.Text != "" {
				t.Errorf("%s: content must not be read", stop)
			}
		}
	})

	t.Run("end_turn without a text block", func(t *testing.T) {
		api := newFakeAPI(t, func(int) (int, string) { return 200, message(msgOpts{stop: "end_turn", noContent: true}) })
		res, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob())
		if err == nil || !strings.Contains(err.Error(), "no text") {
			t.Fatalf("err = %v", err)
		}
		if errors.Is(err, ErrRefusal) || errors.Is(err, ErrMaxTokens) || errors.Is(err, ErrUnexpectedStop) {
			t.Errorf("a missing block is its own error: %v", err)
		}
		if res.Text != "" {
			t.Error("no text")
		}
	})
}

func TestResultDecode(t *testing.T) {
	if err := (Result{}).Decode(&struct{}{}); err == nil {
		t.Error("empty text must error")
	}
	if err := (Result{Text: "{nope"}).Decode(&struct{}{}); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Errorf("bad json err = %v", err)
	}
	var v struct{ A int }
	if err := (Result{Text: `{"A":7}`}).Decode(&v); err != nil || v.A != 7 {
		t.Errorf("v=%+v err=%v", v, err)
	}
}

func TestUsageAndCostParsed(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{"ok":true}`) })
	fb := &fakeBudget{}
	c := newTestClient(t, api.srv.URL, fb, nil)
	res, err := c.Complete(context.Background(), testJob())
	if err != nil {
		t.Fatal(err)
	}
	u := res.Usage
	if u.InputTokens != 2100 || u.OutputTokens != 40 || u.CacheReadInputTokens != 1800 || u.CacheCreationInputTokens != 200 {
		t.Errorf("usage = %+v", u)
	}
	// Cache reads (0.1x) and writes (1.25x) are priced on top of plain input;
	// pricing follows the requested (Haiku) model.
	wantCost := testPrices[haiku].CostUsage(2100, 40, 1800, 200)
	if !near(res.CostUSD, wantCost) {
		t.Errorf("cost = %v, want %v", res.CostUSD, wantCost)
	}
	if res.Model != opus {
		t.Errorf("served model = %q", res.Model)
	}
}

func TestBudgetCheckedBeforeEveryAttemptAndUsageRecorded(t *testing.T) {
	api := newFakeAPI(t, func(n int) (int, string) {
		if n < 3 {
			return 529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`
		}
		return 200, okMessage(`{"title":"a","brief":"b"}`)
	})
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, api.srv.URL, fb, func(cfg *Config) { cfg.Sleep = sl.Sleep })
	job := BriefJob(BriefInput{Title: "t"})
	res, err := c.Complete(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Attempts != 3 || api.hits() != 3 || len(fb.checks) != 3 || len(sl.d) != 2 {
		t.Fatalf("attempts=%d hits=%d checks=%d sleeps=%d", res.Attempts, api.hits(), len(fb.checks), len(sl.d))
	}
	if got := strings.Join(fb.events, ","); got != "check,record,check,record,check,record" {
		t.Errorf("events = %s", got)
	}
	in, out := EstimateJobTokens(job)
	chk := fb.checks[0]
	if chk.API != "claude-opus" || chk.Priority != budget.PriorityBrief || chk.EstimatedInputTokens != in || chk.EstimatedOutputTokens != out || out != job.MaxTokens {
		t.Errorf("check = %+v (estimate %d/%d)", chk, in, out)
	}
	if want := testPrices[opus].Cost(in, out); !near(chk.EstimatedUSD, want) {
		t.Errorf("estimated USD = %v, want %v", chk.EstimatedUSD, want)
	}
	// Failed attempts refund the reservation with zero usage; the success books real usage.
	for i := 0; i < 2; i++ {
		if fb.records[i].Usage != (budget.Usage{}) || fb.records[i].Res.Tokens != in+out || fb.records[i].Res.API != "claude-opus" || fb.records[i].Res.USD != chk.EstimatedUSD {
			t.Errorf("refund %d = %+v", i, fb.records[i])
		}
	}
	last := fb.records[2]
	wantCost := testPrices[opus].CostUsage(2100, 40, 1800, 200)
	if last.Usage.InputTokens != 2100 || last.Usage.OutputTokens != 40 || !near(last.Usage.CostUSD, wantCost) || last.Res.Tokens != in+out {
		t.Errorf("record = %+v, want cost %v", last, wantCost)
	}
}

func TestBudgetDefaultsAndHaikuAPI(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb := &fakeBudget{}
	c := newTestClient(t, api.srv.URL, fb, nil)
	if _, err := c.Complete(context.Background(), EnricherJob(Article{Title: "t"})); err != nil {
		t.Fatal(err)
	}
	if fb.checks[0].API != "claude-haiku" || fb.checks[0].Priority != budget.PriorityDefault {
		t.Errorf("check = %+v", fb.checks[0])
	}
}

func TestBudgetDenial(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb := &fakeBudget{denyFrom: 1, reason: "shed:brief"}
	_, err := newTestClient(t, api.srv.URL, fb, nil).Complete(context.Background(), testJob())
	var be *BudgetError
	if !errors.Is(err, ErrBudget) || !errors.As(err, &be) || be.Reason != "shed:brief" || be.API != "claude-haiku" {
		t.Fatalf("err = %v", err)
	}
	if api.hits() != 0 || len(fb.records) != 0 {
		t.Errorf("hits=%d records=%d", api.hits(), len(fb.records))
	}
}

func TestBudgetDenialOnRetry(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 503, `{"type":"error","error":{"type":"api_error","message":"x"}}` })
	fb := &fakeBudget{denyFrom: 2}
	_, err := newTestClient(t, api.srv.URL, fb, func(cfg *Config) { cfg.MaxRetries = 5 }).Complete(context.Background(), testJob())
	if !errors.Is(err, ErrBudget) || api.hits() != 1 || len(fb.checks) != 2 {
		t.Errorf("err=%v hits=%d checks=%d", err, api.hits(), len(fb.checks))
	}
}

func TestRetryClassification(t *testing.T) {
	retry := []int{429, 500, 501, 502, 503, 504, 520, 524, 529, 599}
	noRetry := []int{400, 401, 402, 403, 404, 408, 413, 422}
	run := func(status int) (*fakeAPI, *fakeBudget, *sleeper, error) {
		api := newFakeAPI(t, func(int) (int, string) {
			return status, `{"type":"error","error":{"type":"x_error","message":"nope"}}`
		})
		fb, sl := &fakeBudget{}, &sleeper{}
		_, err := newTestClient(t, api.srv.URL, fb, func(cfg *Config) { cfg.Sleep = sl.Sleep }).Complete(context.Background(), testJob())
		return api, fb, sl, err
	}
	for _, status := range retry {
		api, fb, sl, err := run(status)
		var apiErr *anthropic.Error
		if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
			t.Errorf("%d: err = %v", status, err)
		}
		// MaxRetries 2 → exactly 3 server hits: the SDK must not retry underneath.
		if api.hits() != 3 || len(fb.checks) != 3 || len(fb.records) != 3 || len(sl.d) != 2 {
			t.Errorf("%d: hits=%d checks=%d records=%d sleeps=%d, want 3/3/3/2", status, api.hits(), len(fb.checks), len(fb.records), len(sl.d))
		}
	}
	for _, status := range noRetry {
		api, fb, sl, err := run(status)
		var apiErr *anthropic.Error
		if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
			t.Errorf("%d: err = %v", status, err)
		}
		if api.hits() != 1 || len(fb.checks) != 1 || len(sl.d) != 0 {
			t.Errorf("%d must not be retried: hits=%d checks=%d sleeps=%d", status, api.hits(), len(fb.checks), len(sl.d))
		}
	}
}

func TestZeroRetriesIsExactlyOneHit(t *testing.T) {
	for _, status := range []int{429, 500, 503, 520} {
		api := newFakeAPI(t, func(int) (int, string) { return status, `{"type":"error","error":{"type":"api_error","message":"x"}}` })
		_, err := newTestClient(t, api.srv.URL, &fakeBudget{}, func(cfg *Config) { cfg.MaxRetries = 0 }).Complete(context.Background(), testJob())
		if err == nil || api.hits() != 1 {
			t.Errorf("%d: err=%v hits=%d, the SDK must not add its own retries", status, err, api.hits())
		}
	}
}

func TestRetryHeaderCountIsZero(t *testing.T) {
	api := newFakeAPI(t, func(n int) (int, string) {
		if n == 1 {
			return 500, `{"type":"error","error":{"type":"api_error","message":"x"}}`
		}
		return 200, okMessage(`{}`)
	})
	if _, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), testJob()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < api.hits(); i++ {
		if got := api.req(i).Header.Get("X-Stainless-Retry-Count"); got != "0" {
			t.Errorf("request %d retry count header = %q: the SDK is retrying under us", i, got)
		}
	}
}

func TestBackoffDurations(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 502, `{"type":"error","error":{"type":"api_error","message":"x"}}` })
	run := func(jitter float64) []time.Duration {
		sl := &sleeper{}
		_, _ = newTestClient(t, api.srv.URL, &fakeBudget{}, func(cfg *Config) {
			cfg.MaxRetries = 4
			cfg.MinBackoff = 100 * time.Millisecond
			cfg.MaxBackoff = 250 * time.Millisecond
			cfg.Jitter = func() float64 { return jitter }
			cfg.Sleep = sl.Sleep
		}).Complete(context.Background(), testJob())
		return sl.d
	}
	ms := time.Millisecond
	lo, hi := run(0), run(1)
	wantLo := []time.Duration{50 * ms, 100 * ms, 125 * ms, 125 * ms}
	wantHi := []time.Duration{100 * ms, 200 * ms, 250 * ms, 250 * ms}
	if len(lo) != 4 || len(hi) != 4 {
		t.Fatalf("lo=%v hi=%v", lo, hi)
	}
	for i := range wantLo {
		if lo[i] != wantLo[i] || hi[i] != wantHi[i] {
			t.Fatalf("lo=%v hi=%v want %v / %v", lo, hi, wantLo, wantHi)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransportErrorsAreRetried(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if n.Add(1) < 3 {
			return nil, errors.New("connection reset by peer")
		}
		return &http.Response{
			StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(okMessage(`{"ok":true}`))), Request: r,
		}, nil
	})}
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, "http://example.invalid", fb, func(cfg *Config) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	res, err := c.Complete(context.Background(), testJob())
	if err != nil || res.Attempts != 3 || n.Load() != 3 || len(sl.d) != 2 || len(fb.checks) != 3 {
		t.Fatalf("res=%+v err=%v hits=%d sleeps=%v checks=%d", res, err, n.Load(), sl.d, len(fb.checks))
	}
}

func TestContextCancellationIsNotRetried(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, api.srv.URL, fb, func(cfg *Config) { cfg.Sleep = sl.Sleep })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Complete(ctx, testJob())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if api.hits() != 0 || len(fb.checks) != 1 || len(sl.d) != 0 {
		t.Errorf("hits=%d checks=%d sleeps=%d", api.hits(), len(fb.checks), len(sl.d))
	}
	if len(fb.records) != 1 || fb.records[0].Usage != (budget.Usage{}) {
		t.Errorf("the reservation must be refunded: %+v", fb.records)
	}
}

func TestCancellationDuringBackoff(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 503, `{"type":"error","error":{"type":"api_error","message":"x"}}` })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, func(cfg *Config) {
		cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	})
	_, err := c.Complete(context.Background(), testJob())
	if !errors.Is(err, context.Canceled) || api.hits() != 1 {
		t.Errorf("err=%v hits=%d", err, api.hits())
	}
}

func TestNewValidation(t *testing.T) {
	ok := Config{APIKey: "k", HaikuModel: haiku, OpusModel: opus}
	for _, key := range []string{"", "  "} {
		cfg := ok
		cfg.APIKey = key
		if _, err := New(cfg, &fakeBudget{}); !errors.Is(err, ErrNoAPIKey) {
			t.Errorf("key %q: %v", key, err)
		}
	}
	if _, err := New(ok, nil); err == nil {
		t.Error("nil budget")
	}
	noModels := ok
	noModels.OpusModel = ""
	if _, err := New(noModels, &fakeBudget{}); err == nil {
		t.Error("missing model ids")
	}
	c, err := New(ok, &fakeBudget{})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Effort != "low" || c.cfg.Timeout != 120*time.Second || c.cfg.MinBackoff != 500*time.Millisecond || c.cfg.MaxBackoff != 15*time.Second {
		t.Errorf("defaults = %+v", c.cfg)
	}
	if _, err := NewFromConfig(config.DefaultModels(), "", "", &fakeBudget{}); !errors.Is(err, ErrNoAPIKey) {
		t.Errorf("NewFromConfig without a key: %v", err)
	}
	c2, err := NewFromConfig(config.DefaultModels(), "k", "http://x", &fakeBudget{})
	if err != nil || c2.cfg.HaikuModel != haiku || c2.cfg.MaxRetries != 4 || c2.cfg.Effort != "low" {
		t.Errorf("NewFromConfig = %+v %v", c2, err)
	}
}

func TestRetryableHelper(t *testing.T) {
	if retryable(context.Canceled) || retryable(fmt.Errorf("x: %w", context.Canceled)) {
		t.Error("cancellation is final")
	}
	if !retryable(context.DeadlineExceeded) {
		t.Error("a per-attempt timeout is retryable (Complete checks the parent context)")
	}
	if !retryable(errors.New("dial tcp: refused")) {
		t.Error("transport errors retry")
	}
	if retryable(&anthropic.Error{StatusCode: 401}) || !retryable(&anthropic.Error{StatusCode: 529}) {
		t.Error("API errors follow the status table")
	}
}

func near(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-9
}

func TestCostCountsCacheTokens(t *testing.T) {
	cases := map[string]struct {
		body string
		want float64
	}{
		"no cache":     {message(msgOpts{stop: "end_turn", text: "{}", in: 1000, out: 10, model: haiku}), testPrices[haiku].Cost(1000, 10)},
		"cache read":   {message(msgOpts{stop: "end_turn", text: "{}", in: 1000, out: 10, cacheRead: 5000, model: haiku}), testPrices[haiku].CostUsage(1000, 10, 5000, 0)},
		"cache create": {message(msgOpts{stop: "end_turn", text: "{}", in: 1000, out: 10, cacheCreate: 3000, model: haiku}), testPrices[haiku].CostUsage(1000, 10, 0, 3000)},
	}
	// Cache tokens must change the cost: reads and writes are never free.
	if !(testPrices[haiku].CostUsage(1000, 10, 5000, 0) > testPrices[haiku].Cost(1000, 10)) ||
		!(testPrices[haiku].CostUsage(1000, 10, 0, 3000) > testPrices[haiku].CostUsage(1000, 10, 5000, 0)*0.5) {
		t.Fatal("test prices do not exercise the cache rates")
	}
	for name, c := range cases {
		body := c.body
		api := newFakeAPI(t, func(int) (int, string) { return 200, body })
		fb := &fakeBudget{}
		res, err := newTestClient(t, api.srv.URL, fb, nil).Complete(context.Background(), testJob())
		if err != nil {
			t.Fatal(err)
		}
		if !near(res.CostUSD, c.want) || !near(fb.records[0].Usage.CostUSD, c.want) {
			t.Errorf("%s: cost = %v (recorded %v), want %v", name, res.CostUSD, fb.records[0].Usage.CostUSD, c.want)
		}
		// Token accounting for the bucket stays on the uncached input tokens.
		if fb.records[0].Usage.InputTokens != 1000 {
			t.Errorf("%s: recorded input tokens = %d", name, fb.records[0].Usage.InputTokens)
		}
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func httpResp(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func TestLatencyIncludesRetriesAndBackoff(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		clock.advance(200 * time.Millisecond)
		if n.Add(1) < 3 {
			return httpResp(r, 529, `{"type":"error","error":{"type":"overloaded_error","message":"x"}}`), nil
		}
		return httpResp(r, 200, okMessage(`{"ok":true}`)), nil
	})}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *Config) {
		cfg.HTTPClient = hc
		cfg.Jitter = func() float64 { return 1 }
		cfg.Sleep = func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }
	})
	c.now = clock.now
	res, err := c.Complete(context.Background(), testJob())
	if err != nil {
		t.Fatal(err)
	}
	if want := 3*200*time.Millisecond + 100*time.Millisecond + 200*time.Millisecond; res.Latency != want {
		t.Errorf("total latency = %v, want %v", res.Latency, want)
	}
	if res.LastAttemptLatency != 200*time.Millisecond || res.Attempts != 3 {
		t.Errorf("last attempt = %v attempts = %d", res.LastAttemptLatency, res.Attempts)
	}
}

func TestFailedCallReturnsPartialTiming(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		clock.advance(300 * time.Millisecond)
		return httpResp(r, 503, `{"type":"error","error":{"type":"api_error","message":"x"}}`), nil
	})}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *Config) {
		cfg.HTTPClient = hc
		cfg.MaxRetries = 1
		cfg.Jitter = func() float64 { return 1 }
		cfg.Sleep = func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }
	})
	c.now = clock.now
	res, err := c.Complete(context.Background(), testJob())
	if err == nil {
		t.Fatal("expected an error")
	}
	if res.Attempts != 2 || res.LastAttemptLatency != 300*time.Millisecond || res.Latency != 700*time.Millisecond {
		t.Errorf("partial = attempts %d last %v total %v", res.Attempts, res.LastAttemptLatency, res.Latency)
	}
}

func TestTypedStopsKeepUsageCostAndLatency(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	cases := map[string]struct {
		body string
		kind error
	}{
		"refusal":    {message(msgOpts{stop: "refusal", noContent: true, in: 100, out: 2, cacheRead: 1000, model: haiku, details: `{"type":"refusal","category":"cyber","explanation":"x"}`}), ErrRefusal},
		"max_tokens": {message(msgOpts{stop: "max_tokens", text: "{", in: 100, out: 2, cacheRead: 1000, model: haiku}), ErrMaxTokens},
		"unexpected": {message(msgOpts{stop: "pause_turn", text: "{", in: 100, out: 2, cacheRead: 1000, model: haiku}), ErrUnexpectedStop},
	}
	for name, c := range cases {
		body := c.body
		hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			clock.advance(time.Second)
			return httpResp(r, 200, body), nil
		})}
		fb := &fakeBudget{}
		cl := newTestClient(t, "http://example.invalid", fb, func(cfg *Config) { cfg.HTTPClient = hc })
		cl.now = clock.now
		res, err := cl.Complete(context.Background(), testJob())
		var se *StopError
		if !errors.Is(err, c.kind) || !errors.As(err, &se) {
			t.Fatalf("%s: err = %v", name, err)
		}
		want := testPrices[haiku].CostUsage(100, 2, 1000, 0)
		if se.Usage.InputTokens != 100 || se.Usage.CacheReadInputTokens != 1000 || !near(se.CostUSD, want) || se.Latency != time.Second || se.LastAttemptLatency != time.Second {
			t.Errorf("%s: stop error = %+v, want cost %v", name, se, want)
		}
		if res.Usage != se.Usage || !near(res.CostUSD, want) || res.Latency != time.Second {
			t.Errorf("%s: result = %+v", name, res)
		}
		if len(fb.records) != 1 || !near(fb.records[0].Usage.CostUSD, want) || fb.records[0].Usage.InputTokens != 100 {
			t.Errorf("%s: the paid call must be booked: %+v", name, fb.records)
		}
	}
}

func TestPerAttemptTimeoutIsRetriedWhileParentAlive(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if n.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return httpResp(r, 200, okMessage(`{}`)), nil
	})}
	sl := &sleeper{}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *Config) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	res, err := c.Complete(context.Background(), testJob())
	if err != nil || res.Attempts != 2 || n.Load() != 2 || len(sl.d) != 1 {
		t.Fatalf("err=%v attempts=%d hits=%d sleeps=%v", err, res.Attempts, n.Load(), sl.d)
	}
}

func TestExpiredParentContextIsNotRetried(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n.Add(1)
		return nil, context.DeadlineExceeded
	})}
	sl := &sleeper{}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *Config) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := c.Complete(ctx, testJob())
	if err == nil || len(sl.d) != 0 || n.Load() > 1 {
		t.Errorf("err=%v sleeps=%v hits=%d: an expired parent is final", err, sl.d, n.Load())
	}
}

func TestDecodeEnforcesOriginalConstraints(t *testing.T) {
	schema := map[string]any{
		"type": "object", "required": []any{"n", "tags"}, "additionalProperties": false,
		"properties": map[string]any{
			"n":    map[string]any{"type": "integer", "minimum": 1, "maximum": 3},
			"tags": map[string]any{"type": "array", "maxItems": 1, "items": map[string]any{"type": "string"}},
		},
	}
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{"n":9,"tags":["a","b"]}`) })
	res, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), Job{
		Name: "c", Tier: TierHaiku, System: strings.Repeat("s ", 50), User: "u", Schema: schema, MaxTokens: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		N    int
		Tags []string
	}
	err = res.Decode(&out)
	var se *SchemaError
	if !errors.As(err, &se) {
		t.Fatalf("Decode err = %v, want a *SchemaError", err)
	}
	if res.Schema == nil {
		t.Error("the result must carry the original schema")
	}
	// The wire carried none of the constraints.
	wire := api.req(0).Raw
	for _, kw := range []string{`"minimum"`, `"maximum"`, `"maxItems"`} {
		if strings.Contains(string(wire), kw) {
			t.Errorf("wire request contains %s", kw)
		}
	}
	// A valid payload decodes.
	ok := Result{Text: `{"n":2,"tags":["a"]}`, Schema: schema}
	if err := ok.Decode(&out); err != nil || out.N != 2 {
		t.Errorf("valid decode: %v %+v", err, out)
	}
}

func TestEmptySystemPromptRejected(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	job := testJob()
	job.System = "  "
	if _, err := newTestClient(t, api.srv.URL, &fakeBudget{}, nil).Complete(context.Background(), job); err == nil || api.hits() != 0 {
		t.Errorf("err=%v hits=%d", err, api.hits())
	}
}

func TestNoReservationIsAnError(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb := &noResBudget{}
	_, err := newTestClient(t, api.srv.URL, fb, nil).Complete(context.Background(), testJob())
	if err == nil || !strings.Contains(err.Error(), "reservation") || api.hits() != 0 {
		t.Errorf("err=%v hits=%d", err, api.hits())
	}
}

type noResBudget struct{}

func (noResBudget) Check(context.Context, budget.Request) (budget.Verdict, error) {
	return budget.Verdict{Allowed: true}, nil
}
func (noResBudget) Record(context.Context, budget.Reservation, budget.Usage) error { return nil }
