package decide

import (
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

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
)

type recordCall struct {
	Res   budget.Reservation
	Usage budget.Usage
}

// fakeBudget records every Check/Record and can deny from the Nth check on.
type fakeBudget struct {
	mu         sync.Mutex
	checks     []budget.Request
	records    []recordCall
	denyFrom   int // 1-based check index from which to deny; 0 never denies
	reason     string
	checkErr   error
	recordErr  error
	events     []string
	denyAlways bool
	noRes      bool
}

func (f *fakeBudget) Check(_ context.Context, r budget.Request) (budget.Verdict, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = append(f.checks, r)
	f.events = append(f.events, "check")
	if f.checkErr != nil {
		return budget.Verdict{}, f.checkErr
	}
	if f.denyAlways || (f.denyFrom > 0 && len(f.checks) >= f.denyFrom) {
		reason := f.reason
		if reason == "" {
			reason = "daily-cap"
		}
		return budget.Verdict{Allowed: false, Reason: reason}, nil
	}
	if f.noRes {
		return budget.Verdict{Allowed: true, Reason: "ok"}, nil
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
	return f.recordErr
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

const okBody = `{
  "id": "gen-dec-1",
  "model": "typesafe/jev-1.14-20261014",
  "provider": "TypeSafe",
  "answers": {
    "category": {"type": "choice", "choice": "tech", "confidence": 0.93,
                 "probabilities": {"tech": 0.93, "other": 0.07}, "legend": {"tech": "Technology"}},
    "match": {"type": "noul", "noul": 0.42},
    "strength": {"type": "score", "score": 2.2, "probabilities": {"2": 0.8, "3": 0.2}},
    "refused": {"type": "refusal"},
    "weird": {"type": "mystery"},
    "nonoul": {"type": "noul"}
  },
  "usage": {"input_tokens": 321, "output_tokens": 12, "cost": 0.0000321,
            "input_tokens_details": {"cached_tokens": 100}}
}`

func newTestClient(t *testing.T, url string, b Budget, mutate func(*ClientConfig)) *Client {
	t.Helper()
	cfg := ClientConfig{
		BaseURL:    url,
		APIKey:     "secret-key",
		Model:      "typesafe/jev-1.14",
		MaxRetries: 2,
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		Price:      config.Price{InputPerMTok: 0.10},
		Jitter:     func() float64 { return 0.5 },
		Sleep:      func(context.Context, time.Duration) error { return nil },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func simpleReq() Request { return ClassifyRequest(Article{Title: "hello", Lede: "world"}) }

func TestDecideSuccess(t *testing.T) {
	var gotBody []byte
	var gotAuth, gotCT, gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotAuth, gotCT, gotPath, gotMethod = r.Header.Get("Authorization"), r.Header.Get("Content-Type"), r.URL.Path, r.Method
		_, _ = io.WriteString(w, okBody)
	}))
	defer srv.Close()
	fb := &fakeBudget{}
	c := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.Priority = budget.PriorityWhy })

	req := simpleReq()
	req.SessionID, req.User = "sess", "user"
	resp, err := c.Decide(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/alpha/decisions" || gotAuth != "Bearer secret-key" || gotCT != "application/json" {
		t.Errorf("request = %s %s auth=%q ct=%q", gotMethod, gotPath, gotAuth, gotCT)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatal(err)
	}
	if sent["model"] != "typesafe/jev-1.14" || sent["session_id"] != "sess" || sent["user"] != "user" || sent["questions"] == nil || sent["state"] == nil {
		t.Errorf("sent body = %s", gotBody)
	}

	if resp.ID != "gen-dec-1" || resp.Provider != "TypeSafe" || resp.Model != "typesafe/jev-1.14-20261014" {
		t.Errorf("identity = %+v", resp)
	}
	if resp.Usage.InputTokens != 321 || resp.Usage.OutputTokens != 12 || resp.Usage.CachedTokens != 100 || resp.Usage.CostUSD != 0.0000321 {
		t.Errorf("usage = %+v", resp.Usage)
	}
	if resp.Attempts != 1 || resp.Latency < 0 {
		t.Errorf("attempts=%d latency=%v", resp.Attempts, resp.Latency)
	}

	cat, ok := resp.Answer("category", TypeChoice)
	if p, _ := cat.SelectedProbability(); p != 0.93 || cat.Concentration() != 0.93 {
		t.Errorf("category probability/concentration = %v / %v", p, cat.Concentration())
	}
	if !ok || cat.Choice != "tech" || cat.Probability("other") != 0.07 || cat.Legend["tech"] != "Technology" {
		t.Errorf("category = %+v ok=%v", cat, ok)
	}
	if m, ok := resp.Answer("match", TypeNoul); !ok || m.Noul != 0.42 || m.HasConfidence {
		t.Errorf("match = %+v", m)
	}
	if s, ok := resp.Answer("strength", TypeScore); !ok || s.Score != 2.2 {
		t.Errorf("strength = %+v", s)
	} else if p, ok := s.SelectedProbability(); !ok || p != 0.8 {
		t.Errorf("strength level probability = %v %v", p, ok)
	}
	for _, k := range []string{"refused", "weird", "nonoul", "absent"} {
		if _, ok := resp.Answer(k, TypeNoul); ok {
			t.Errorf("%s must not be answered", k)
		}
	}
	if !resp.Answers["refused"].Refusal || !resp.Answers["refused"].Present {
		t.Errorf("refusal arm not parsed: %+v", resp.Answers["refused"])
	}
	if _, present := resp.Answers["absent"]; present {
		t.Error("absent key must be absent")
	}

	// Budget: one check before the call, one record after, with real usage.
	if len(fb.checks) != 1 || len(fb.records) != 1 || fb.events[0] != "check" || fb.events[1] != "record" {
		t.Fatalf("events = %v", fb.events)
	}
	chk := fb.checks[0]
	wantEst := int64(len(gotBody)/4 + 1)
	if chk.API != "jev" || chk.Priority != budget.PriorityWhy || chk.EstimatedInputTokens != wantEst || chk.EstimatedOutputTokens != 0 {
		t.Errorf("check = %+v (want estimate %d)", chk, wantEst)
	}
	if want := float64(wantEst) / 1e6 * 0.10; chk.EstimatedUSD != want {
		t.Errorf("estimated USD = %v, want %v", chk.EstimatedUSD, want)
	}
	rec := fb.records[0]
	if rec.Res.API != "jev" || rec.Res.Tokens != wantEst || rec.Res.USD != chk.EstimatedUSD || rec.Res.ID == "" || rec.Usage.CostUSD != 0.0000321 || rec.Usage.InputTokens != 321 || rec.Usage.OutputTokens != 12 {
		t.Errorf("record = %+v", rec)
	}
}

func TestDecideDefaultPriority(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	fb := &fakeBudget{}
	if _, err := newTestClient(t, srv.URL, fb, nil).Decide(context.Background(), simpleReq()); err != nil {
		t.Fatal(err)
	}
	if fb.checks[0].Priority != budget.PriorityDefault {
		t.Errorf("priority = %q", fb.checks[0].Priority)
	}
}

func TestHTTPErrorBodyParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"code":400,"message":"question key too long"}}`)
	}))
	defer srv.Close()
	_, err := newTestClient(t, srv.URL, &fakeBudget{}, nil).Decide(context.Background(), simpleReq())
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("err = %v", err)
	}
	if he.Status != 400 || he.Code != 400 || he.Message != "question key too long" || !strings.Contains(he.Body, "too long") {
		t.Errorf("http error = %+v", he)
	}
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "question key too long") {
		t.Errorf("message = %q", err.Error())
	}
}

func TestHTTPErrorWithoutJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "<html>nope</html>")
	}))
	defer srv.Close()
	_, err := newTestClient(t, srv.URL, &fakeBudget{}, nil).Decide(context.Background(), simpleReq())
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 404 || he.Code != 0 || he.Body != "<html>nope</html>" {
		t.Fatalf("err = %v / %+v", err, he)
	}
	if err.Error() != "decide: jev HTTP 404" {
		t.Errorf("message = %q", err.Error())
	}
}

func TestRetryClassification(t *testing.T) {
	retry := []int{429, 500, 501, 502, 503, 504, 520, 524, 529, 599}
	noRetry := []int{400, 401, 402, 403, 404, 408, 413, 422}
	run := func(status int) (hits int32, fb *fakeBudget, sl *sleeper, err error) {
		var n atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n.Add(1)
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":{"code":1,"message":"x"}}`)
		}))
		defer srv.Close()
		fb, sl = &fakeBudget{}, &sleeper{}
		c := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.Sleep = sl.Sleep })
		_, err = c.Decide(context.Background(), simpleReq())
		return n.Load(), fb, sl, err
	}
	for _, status := range retry {
		hits, fb, sl, err := run(status)
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != status {
			t.Errorf("%d: err = %v", status, err)
		}
		if hits != 3 || len(fb.checks) != 3 || len(fb.records) != 3 || len(sl.d) != 2 {
			t.Errorf("%d: hits=%d checks=%d records=%d sleeps=%d, want 3/3/3/2", status, hits, len(fb.checks), len(fb.records), len(sl.d))
		}
	}
	for _, status := range noRetry {
		hits, fb, sl, err := run(status)
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != status {
			t.Errorf("%d: err = %v", status, err)
		}
		if hits != 1 || len(fb.checks) != 1 || len(sl.d) != 0 {
			t.Errorf("%d must not be retried: hits=%d checks=%d sleeps=%d", status, hits, len(fb.checks), len(sl.d))
		}
	}
}

func TestRetryThenSuccessCountsAttempts(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, okBody)
	}))
	defer srv.Close()
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.Sleep = sl.Sleep })
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Attempts != 3 || n.Load() != 3 {
		t.Errorf("attempts = %d hits = %d", resp.Attempts, n.Load())
	}
	if len(fb.checks) != 3 {
		t.Errorf("the budget must be checked before every attempt, got %d", len(fb.checks))
	}
	// Two refunds (zero usage) then one real record, each reserving the same estimate.
	if len(fb.records) != 3 || fb.records[0].Usage != (budget.Usage{}) || fb.records[1].Usage != (budget.Usage{}) || fb.records[2].Usage.CostUSD == 0 {
		t.Errorf("records = %+v", fb.records)
	}
	if fb.records[0].Res.Tokens != fb.checks[0].EstimatedInputTokens {
		t.Errorf("refund reserved %d, checked %d", fb.records[0].Res.Tokens, fb.checks[0].EstimatedInputTokens)
	}
	wantEvents := "check,record,check,record,check,record"
	if got := strings.Join(fb.events, ","); got != wantEvents {
		t.Errorf("events = %s, want %s", got, wantEvents)
	}
}

func TestBackoffIsExponentialJitteredAndCapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }))
	defer srv.Close()
	run := func(jitter float64) []time.Duration {
		sl := &sleeper{}
		c := newTestClient(t, srv.URL, &fakeBudget{}, func(cfg *ClientConfig) {
			cfg.MaxRetries = 4
			cfg.MinBackoff = 100 * time.Millisecond
			cfg.MaxBackoff = 250 * time.Millisecond
			cfg.Jitter = func() float64 { return jitter }
			cfg.Sleep = sl.Sleep
		})
		if _, err := c.Decide(context.Background(), simpleReq()); err == nil {
			t.Fatal("expected an error")
		}
		return sl.d
	}
	ms := time.Millisecond
	lo := run(0) // half of the capped exponential value
	wantLo := []time.Duration{50 * ms, 100 * ms, 125 * ms, 125 * ms}
	hi := run(1) // the full value (jitter is in [0,1))
	wantHi := []time.Duration{100 * ms, 200 * ms, 250 * ms, 250 * ms}
	for i := range wantLo {
		if len(lo) != 4 || lo[i] != wantLo[i] || hi[i] != wantHi[i] {
			t.Fatalf("lo=%v hi=%v, want %v / %v", lo, hi, wantLo, wantHi)
		}
	}
	mid := run(0.5)
	if mid[0] != 75*ms || mid[1] != 150*ms {
		t.Errorf("mid = %v", mid)
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
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(okBody)), Header: http.Header{}}, nil
	})}
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, "http://example.invalid", fb, func(cfg *ClientConfig) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil || resp.Attempts != 3 || len(sl.d) != 2 {
		t.Fatalf("resp=%+v err=%v sleeps=%v", resp.Attempts, err, sl.d)
	}
}

func TestTransportErrorExhaustsRetries(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n.Add(1)
		return nil, errors.New("dial tcp: boom")
	})}
	fb := &fakeBudget{}
	c := newTestClient(t, "http://example.invalid", fb, func(cfg *ClientConfig) { cfg.HTTPClient = hc; cfg.MaxRetries = 3 })
	_, err := c.Decide(context.Background(), simpleReq())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	if n.Load() != 4 || len(fb.checks) != 4 {
		t.Errorf("attempts = %d, checks = %d, want 4", n.Load(), len(fb.checks))
	}
}

func TestZeroRetriesMeansOneAttempt(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	c := newTestClient(t, srv.URL, &fakeBudget{}, func(cfg *ClientConfig) { cfg.MaxRetries = 0 })
	if _, err := c.Decide(context.Background(), simpleReq()); err == nil || n.Load() != 1 {
		t.Errorf("err=%v hits=%d", err, n.Load())
	}
}

func TestContextCancellationIsNotRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.Sleep = sl.Sleep })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Decide(ctx, simpleReq())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if n.Load() != 0 || len(fb.checks) != 1 || len(sl.d) != 0 {
		t.Errorf("hits=%d checks=%d sleeps=%d: a cancelled context is final", n.Load(), len(fb.checks), len(sl.d))
	}
	if len(fb.records) != 1 || fb.records[0].Usage != (budget.Usage{}) {
		t.Errorf("the reservation must still be refunded: %+v", fb.records)
	}
}

func TestCancellationDuringBackoffStops(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	c := newTestClient(t, srv.URL, &fakeBudget{}, func(cfg *ClientConfig) {
		cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	})
	_, err := c.Decide(context.Background(), simpleReq())
	if !errors.Is(err, context.Canceled) || n.Load() != 1 {
		t.Errorf("err=%v hits=%d", err, n.Load())
	}
}

func TestMissingAPIKey(t *testing.T) {
	for _, key := range []string{"", "   "} {
		_, err := NewClient(ClientConfig{BaseURL: "http://x", APIKey: key, Model: "typesafe/jev-1.14"}, &fakeBudget{})
		if !errors.Is(err, ErrNoAPIKey) {
			t.Errorf("key %q: err = %v", key, err)
		}
	}
	// NewClientFromModels propagates it.
	if _, err := NewClientFromModels(config.DefaultModels(), "", "", &fakeBudget{}, nil); !errors.Is(err, ErrNoAPIKey) {
		t.Errorf("from models: %v", err)
	}
}

func TestUnavailableDecider(t *testing.T) {
	if _, err := (Unavailable{}).Decide(context.Background(), Request{}); !errors.Is(err, ErrNoAPIKey) {
		t.Errorf("default reason = %v", err)
	}
	boom := errors.New("down")
	if _, err := (Unavailable{Reason: boom}).Decide(context.Background(), Request{}); !errors.Is(err, boom) {
		t.Errorf("custom reason = %v", err)
	}
	var _ Decider = Unavailable{}
	var _ Decider = (*Client)(nil)
}

func TestNewClientRejectsUnpinnedModels(t *testing.T) {
	for _, model := range []string{"~typesafe/jev-latest", "typesafe/jev-latest", "typesafe/jev", ""} {
		_, err := NewClient(ClientConfig{BaseURL: "http://x", APIKey: "k", Model: model}, &fakeBudget{})
		if err == nil {
			t.Errorf("model %q must be rejected", model)
		}
	}
}

func TestNewClientValidation(t *testing.T) {
	ok := ClientConfig{BaseURL: "http://x", APIKey: "k", Model: "typesafe/jev-1.14"}
	if _, err := NewClient(ok, nil); err == nil {
		t.Error("a nil budget must be rejected")
	}
	noURL := ok
	noURL.BaseURL = ""
	if _, err := NewClient(noURL, &fakeBudget{}); err == nil {
		t.Error("an empty base URL must be rejected")
	}
	badURL := ok
	badURL.BaseURL = "http://exa mple.com"
	if _, err := NewClient(badURL, &fakeBudget{}); err == nil {
		t.Error("an unparsable base URL must be rejected")
	}
	c, err := NewClient(ok, &fakeBudget{})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Path != "/api/alpha/decisions" || c.cfg.Timeout != 30*time.Second || c.cfg.MinBackoff != 250*time.Millisecond ||
		c.cfg.MaxBackoff != 8*time.Second || c.cfg.Priority != budget.PriorityDefault || c.http == nil || c.sleep == nil || c.jitter == nil {
		t.Errorf("defaults not applied: %+v", c.cfg)
	}
	neg := ok
	neg.MaxRetries = -5
	if c, _ := NewClient(neg, &fakeBudget{}); c.cfg.MaxRetries != 0 {
		t.Errorf("negative retries = %d", c.cfg.MaxRetries)
	}
}

func TestNewClientFromModels(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotPath = r.URL.Path; _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	m := config.DefaultModels()
	c, err := NewClientFromModels(m, srv.URL, "key", &fakeBudget{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Model != "typesafe/jev-1.14" || c.cfg.MaxRetries != 4 || c.cfg.Timeout != 30*time.Second {
		t.Errorf("cfg = %+v", c.cfg)
	}
	if _, err := c.Decide(context.Background(), simpleReq()); err != nil || gotPath != "/api/alpha/decisions" {
		t.Errorf("err=%v path=%q", err, gotPath)
	}
	// The default base URL comes from models.yaml when none is given.
	c2, err := NewClientFromModels(m, "", "key", &fakeBudget{}, nil)
	if err != nil || c2.cfg.BaseURL != "https://openrouter.ai" {
		t.Errorf("base url = %q err=%v", c2.cfg.BaseURL, err)
	}
}

// TypeSafe direct returns no id and no usage.cost (live response captured
// 2026-11-21), so cost is priced from input tokens.
const typeSafeBody = `{"model":"jev-1.14.0","answers":{"category":{"type":"choice","choice":"politics","confidence":1.0,"probabilities":{"politics":1.0,"other":0.0}},"urgent":{"type":"noul","noul":0.89}},"usage":{"input_tokens":374,"output_tokens":72}}`

func TestNewTypeSafeClientFromModels(t *testing.T) {
	var gotPath, gotAuth, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotModel = body.Model
		_, _ = io.WriteString(w, typeSafeBody)
	}))
	defer srv.Close()
	m := config.DefaultModels()
	fb := &fakeBudget{}
	c, err := NewTypeSafeClientFromModels(m, srv.URL, "ts-key", fb, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/systemone" || gotAuth != "Bearer ts-key" || gotModel != "jev-1.14.0" {
		t.Errorf("path=%q auth=%q model=%q", gotPath, gotAuth, gotModel)
	}
	if resp.ID != "" || resp.Model != "jev-1.14.0" || resp.Answers["category"].Choice != "politics" {
		t.Errorf("resp = %+v", resp)
	}
	want := 374 * 0.042 / 1e6
	if diff := resp.Usage.CostUSD - want; diff > 1e-12 || diff < -1e-12 {
		t.Errorf("cost = %v, want %v", resp.Usage.CostUSD, want)
	}
	if len(fb.records) != 1 || fb.records[0].Usage.CostUSD != resp.Usage.CostUSD {
		t.Errorf("records = %+v", fb.records)
	}
	c2, err := NewTypeSafeClientFromModels(m, "", "ts-key", fb, nil)
	if err != nil || c2.cfg.BaseURL != "https://api.typesafe.ai" {
		t.Errorf("base url = %q err=%v", c2.cfg.BaseURL, err)
	}
}

func TestReportedCostIsNotRepriced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	c := newTestClient(t, srv.URL, &fakeBudget{}, func(cfg *ClientConfig) { cfg.CostFromPrice = true })
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil || resp.Usage.CostUSD != 0.0000321 {
		t.Errorf("cost = %v err=%v", resp.Usage.CostUSD, err)
	}
}

func TestBudgetDenial(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	fb := &fakeBudget{denyAlways: true, reason: "token-bucket"}
	_, err := newTestClient(t, srv.URL, fb, nil).Decide(context.Background(), simpleReq())
	var be *BudgetError
	if !errors.As(err, &be) || be.Reason != "token-bucket" || be.API != "jev" {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, ErrBudget) {
		t.Error("errors.Is(err, ErrBudget) must hold")
	}
	if n.Load() != 0 || len(fb.records) != 0 {
		t.Errorf("a denied call makes no request (%d) and records nothing (%d)", n.Load(), len(fb.records))
	}
}

func TestBudgetDenialOnRetryStopsRetrying(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	fb := &fakeBudget{denyFrom: 2, reason: "daily-cap"}
	_, err := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.MaxRetries = 5 }).Decide(context.Background(), simpleReq())
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("err = %v", err)
	}
	if n.Load() != 1 || len(fb.checks) != 2 {
		t.Errorf("hits=%d checks=%d: the second attempt must be denied before any request", n.Load(), len(fb.checks))
	}
}

func TestBudgetCheckErrorAndRecordError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	boom := errors.New("kv down")
	_, err := newTestClient(t, srv.URL, &fakeBudget{checkErr: boom}, nil).Decide(context.Background(), simpleReq())
	if !errors.Is(err, boom) || errors.Is(err, ErrBudget) {
		t.Errorf("check error = %v", err)
	}
	_, err = newTestClient(t, srv.URL, &fakeBudget{recordErr: boom}, nil).Decide(context.Background(), simpleReq())
	if !errors.Is(err, boom) {
		t.Errorf("record error = %v", err)
	}
}

func TestMalformedSuccessBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "{nope") }))
	defer srv.Close()
	fb := &fakeBudget{}
	_, err := newTestClient(t, srv.URL, fb, func(cfg *ClientConfig) { cfg.MaxRetries = 0 }).Decide(context.Background(), simpleReq())
	if err == nil || !strings.Contains(err.Error(), "decode response") {
		t.Errorf("err = %v", err)
	}
	if len(fb.records) != 1 || fb.records[0].Usage != (budget.Usage{}) {
		t.Errorf("a bad body is refunded: %+v", fb.records)
	}
}

func TestParseResponseAnswerShapes(t *testing.T) {
	r, err := parseResponse([]byte(okBody))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(len(r.Answers)); got != "6" {
		t.Errorf("answers = %s", got)
	}
	if a := r.Answers["nonoul"]; a.Answered() || a.HasNoul {
		t.Errorf("a noul answer without a value is not usable: %+v", a)
	}
	if a := r.Answers["weird"]; a.Answered() || a.Type != "mystery" {
		t.Errorf("unknown type: %+v", a)
	}
	if _, err := parseResponse([]byte(`{"answers": 5}`)); err == nil {
		t.Error("a structurally wrong body must error")
	}
	empty, err := parseResponse([]byte(`{}`))
	if err != nil || len(empty.Answers) != 0 {
		t.Errorf("empty body: %+v %v", empty, err)
	}
}

func TestRetryableHelpers(t *testing.T) {
	for _, s := range []int{429, 500, 501, 502, 503, 504, 505, 520, 524, 529, 599} {
		if !retryableStatus(s) {
			t.Errorf("%d should retry", s)
		}
	}
	for _, s := range []int{200, 301, 400, 401, 402, 403, 404, 408, 413, 422} {
		if retryableStatus(s) {
			t.Errorf("%d must not retry", s)
		}
	}
	if isRetryableError(context.Canceled) || isRetryableError(fmt.Errorf("x: %w", context.Canceled)) {
		t.Error("cancellation is final")
	}
	if !isRetryableError(context.DeadlineExceeded) {
		t.Error("a per-attempt timeout is retryable (the caller checks the parent context)")
	}
	if !isRetryableError(errors.New("eof")) {
		t.Error("transport errors retry")
	}
	if isRetryableError(&HTTPError{Status: 401}) || !isRetryableError(&HTTPError{Status: 529}) {
		t.Error("HTTPError follows the status table")
	}
}

func TestEstimateRequestTokens(t *testing.T) {
	small := EstimateRequestTokens(Request{State: "x", Questions: map[string]Question{"q": {Type: TypeNoul}}})
	big := EstimateRequestTokens(PlotRequest(Article{Title: strings.Repeat("a", 400)}, plotCands(8)))
	if small < 1 || big <= small {
		t.Errorf("estimates = %d, %d", small, big)
	}
	if EstimateRequestTokens(Request{State: make(chan int)}) != 1 {
		t.Error("an unencodable request estimates 1")
	}
}

func TestNoReservationIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, okBody) }))
	defer srv.Close()
	_, err := newTestClient(t, srv.URL, &fakeBudget{noRes: true}, nil).Decide(context.Background(), simpleReq())
	if err == nil || !strings.Contains(err.Error(), "reservation") {
		t.Errorf("err = %v", err)
	}
}

// fakeClock advances only when told: the round tripper charges a fixed time
// per attempt and Sleep charges the backoff.
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

func TestLatencyIncludesRetriesAndBackoff(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		clock.advance(200 * time.Millisecond)
		if n.Add(1) < 3 {
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(okBody)), Header: http.Header{}}, nil
	})}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *ClientConfig) {
		cfg.HTTPClient = hc
		cfg.Jitter = func() float64 { return 1 } // full backoff: 100ms then 200ms
		cfg.Sleep = func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }
	})
	c.now = clock.now
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil {
		t.Fatal(err)
	}
	if want := 3*200*time.Millisecond + 100*time.Millisecond + 200*time.Millisecond; resp.Latency != want {
		t.Errorf("total latency = %v, want %v (attempts + backoff)", resp.Latency, want)
	}
	if resp.LastAttemptLatency != 200*time.Millisecond {
		t.Errorf("last attempt latency = %v, want 200ms", resp.LastAttemptLatency)
	}
	if resp.Attempts != 3 {
		t.Errorf("attempts = %d", resp.Attempts)
	}
}

func TestFailedCallReturnsPartialTiming(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		clock.advance(300 * time.Millisecond)
		return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
	})}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *ClientConfig) {
		cfg.HTTPClient = hc
		cfg.MaxRetries = 1
		cfg.Jitter = func() float64 { return 1 }
		cfg.Sleep = func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }
	})
	c.now = clock.now
	resp, err := c.Decide(context.Background(), simpleReq())
	if err == nil {
		t.Fatal("expected an error")
	}
	if resp.Attempts != 2 || resp.LastAttemptLatency != 300*time.Millisecond || resp.Latency != 700*time.Millisecond {
		t.Errorf("partial = attempts %d last %v total %v", resp.Attempts, resp.LastAttemptLatency, resp.Latency)
	}
}

func TestPerAttemptTimeoutIsRetriedWhileParentAlive(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if n.Add(1) == 1 {
			return nil, context.DeadlineExceeded // what a per-attempt timeout looks like
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(okBody)), Header: http.Header{}}, nil
	})}
	fb, sl := &fakeBudget{}, &sleeper{}
	c := newTestClient(t, "http://example.invalid", fb, func(cfg *ClientConfig) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	resp, err := c.Decide(context.Background(), simpleReq())
	if err != nil || resp.Attempts != 2 || n.Load() != 2 || len(sl.d) != 1 {
		t.Fatalf("err=%v attempts=%d hits=%d sleeps=%v", err, resp.Attempts, n.Load(), sl.d)
	}
}

func TestExpiredParentContextIsNotRetried(t *testing.T) {
	var n atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n.Add(1)
		return nil, context.DeadlineExceeded
	})}
	sl := &sleeper{}
	c := newTestClient(t, "http://example.invalid", &fakeBudget{}, func(cfg *ClientConfig) { cfg.HTTPClient = hc; cfg.Sleep = sl.Sleep })
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := c.Decide(ctx, simpleReq())
	if !errors.Is(err, context.DeadlineExceeded) || len(sl.d) != 0 || n.Load() > 1 {
		t.Errorf("err=%v sleeps=%v hits=%d: an expired parent is final", err, sl.d, n.Load())
	}
}

func TestPaidRefusalsAreRecordedWithUsage(t *testing.T) {
	// A refusal answer is HTTP 200: the tokens were spent and must be booked.
	body := `{"id":"x","model":"typesafe/jev-1.14-20261014","answers":{"category":{"type":"refusal"}},
	  "usage":{"input_tokens":100,"output_tokens":1,"cost":0.01,"input_tokens_details":{"cached_tokens":0}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
	defer srv.Close()
	fb := &fakeBudget{}
	resp, err := newTestClient(t, srv.URL, fb, nil).Decide(context.Background(), simpleReq())
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Answers["category"].Refusal || resp.Usage.CostUSD != 0.01 {
		t.Errorf("resp = %+v", resp)
	}
	if len(fb.records) != 1 || fb.records[0].Usage.CostUSD != 0.01 || fb.records[0].Usage.InputTokens != 100 {
		t.Errorf("records = %+v", fb.records)
	}
}
