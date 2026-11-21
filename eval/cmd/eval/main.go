// Command eval runs Dor's model evaluation and writes eval/report.md and
// eval/report.json.
//
// Without OPENROUTER_API_KEY and ANTHROPIC_API_KEY it runs both backends as
// in-process mocks. The report's first line then says
// "MOCK BACKENDS — NOT A MEASUREMENT" and thresholds stay at their starting
// values.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/eval"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/decide"
	decidemock "github.com/ankojh/dor/internal/decide/mockapi"
	"github.com/ankojh/dor/internal/llm"
	llmmock "github.com/ankojh/dor/internal/llm/mockapi"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eval:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("eval", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		dataDir         = fs.String("data", "eval/data", "candidate and label directory")
		reportPath      = fs.String("report", "eval/report.md", "markdown report path")
		jsonPath        = fs.String("json", "eval/report.json", "JSON report path")
		configDir       = fs.String("config", "config", "config directory")
		timeout         = fs.Duration("timeout", 30*time.Minute, "overall timeout")
		forceMock       = fs.Bool("mock", false, "force mock backends even when keys are set")
		allowOverCap    = fs.Bool("allow-over-cap", false, "run a live eval whose estimate exceeds eval_usd")
		writeThresholds = fs.Bool("write-thresholds", true, "write selected thresholds to config/thresholds.yaml on a fully live run")
		maxItems        = fs.Int("max-items", 0, "limit each set to N items (0 = all; also a CLI test hook)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	models, err := config.LoadModels(*configDir + "/models.yaml")
	if err != nil {
		return err
	}
	thresholds, err := config.LoadThresholds(*configDir + "/thresholds.yaml")
	if err != nil {
		return err
	}
	budgetCfg, err := config.LoadBudget(*configDir + "/budget.yaml")
	if err != nil {
		return err
	}
	ds, err := eval.LoadDataset(*dataDir)
	if err != nil {
		return err
	}
	if *maxItems > 0 {
		ds.Limit(*maxItems)
	}
	if len(ds.ArticleLabels) == 0 || len(ds.PairLabels) == 0 {
		return fmt.Errorf("no labels in %s; seed agent labels or run the labelling CLI", *dataDir)
	}

	// TypeSafe direct is deliberately not guessed. OpenRouter is the supported
	// live path; a TypeSafe key needs an explicit compatible base URL, path and
	// request shape, and the direct /v1/systemone shape is not implemented.
	var jevKey, jevBase, jevPath string
	jevMock := *forceMock
	if !*forceMock {
		jevKey, jevBase, jevPath, err = jevCredentials()
		if err != nil {
			return err
		}
		jevMock = jevKey == ""
	}
	anthropicKey := os.Getenv("ANTHROPIC_API_KEY")
	claudeBase := os.Getenv("DOR_CLAUDE_BASE_URL")
	claudeMock := *forceMock || anthropicKey == ""
	anyLive := !jevMock || !claudeMock

	if anyLive && budgetCfg.EvalUSD <= 0 {
		return errors.New("eval_usd is 0: live evals are denied; set eval_usd in config/budget.yaml or use -mock")
	}

	// The eval has its own hard cap. The same in-memory limiter serves both
	// clients, so a mixed live/mock run cannot spend more than eval_usd.
	limiterCfg := budgetCfg
	if anyLive {
		limiterCfg.DailyUSD = budgetCfg.EvalUSD
	}
	mockLimiterCfg := budgetCfg
	mockLimiterCfg.DailyUSD = 1_000_000
	// The eval is a batch job, not production traffic: size the token buckets
	// for a whole run so a long eval does not turn into a throttle test. The
	// daily USD cap is the binding limit.
	for api, bucket := range limiterCfg.TokenBuckets {
		if bucket.Capacity < 10_000_000 {
			bucket.Capacity = 10_000_000
		}
		if bucket.RefillPerSecond < 1_000_000 {
			bucket.RefillPerSecond = 1_000_000
		}
		limiterCfg.TokenBuckets[api] = bucket
		mockLimiterCfg.TokenBuckets[api] = bucket
	}
	limiter := budget.NewMemory(limiterCfg)
	mockLimiter := budget.NewMemory(mockLimiterCfg)

	var closeFns []func()
	defer func() {
		for _, f := range closeFns {
			f()
		}
	}()

	var jev decide.Decider
	jevKind := "live"
	if jevMock {
		srv := httptest.NewServer(decidemock.NewHandler(decidemock.DefaultOptions()))
		closeFns = append(closeFns, srv.Close)
		client, err := decide.NewClient(decide.ClientConfig{
			BaseURL:    srv.URL,
			Path:       "/api/alpha/decisions",
			APIKey:     "mock",
			Model:      models.Jev.Model,
			Timeout:    models.Jev.Timeout.Std(),
			MaxRetries: 0,
			Price:      models.Jev.Price,
		}, mockLimiter)
		if err != nil {
			return err
		}
		jev = client
		jevKind = "mock"
	} else {
		if jevBase == "" {
			jevBase = models.Jev.BaseURL
		}
		if jevPath == "" {
			jevPath = models.Jev.Path
		}
		client, err := decide.NewClient(decide.ClientConfig{
			BaseURL:    jevBase,
			Path:       jevPath,
			APIKey:     jevKey,
			Model:      models.Jev.Model,
			Timeout:    models.Jev.Timeout.Std(),
			MaxRetries: models.Jev.MaxRetries,
			MinBackoff: models.Jev.MinBackoff.Std(),
			MaxBackoff: models.Jev.MaxBackoff.Std(),
			Price:      models.Jev.Price,
		}, limiter)
		if err != nil {
			return err
		}
		jev = client
	}

	var claude *llm.Client
	claudeKind := "live"
	if claudeMock {
		srv := httptest.NewServer(llmmock.NewHandler(llmmock.DefaultOptions()))
		closeFns = append(closeFns, srv.Close)
		client, err := llm.New(llm.Config{
			APIKey:     "mock",
			BaseURL:    srv.URL,
			HaikuModel: models.Claude.HaikuModel,
			OpusModel:  models.Claude.OpusModel,
			Effort:     models.Claude.Effort,
			Timeout:    models.Claude.Timeout.Std(),
			MaxRetries: 0,
			Price:      models.Claude.Price,
		}, mockLimiter)
		if err != nil {
			return err
		}
		claude = client
		claudeKind = "mock"
	} else {
		client, err := llm.NewFromConfig(models, anthropicKey, claudeBase, limiter)
		if err != nil {
			return err
		}
		claude = client
	}

	jevCost, claudeCost, calls := eval.EstimateRun(ds, models)
	liveJev, liveClaude := 0.0, 0.0
	if !jevMock {
		liveJev = jevCost
	}
	if !claudeMock {
		liveClaude = claudeCost
	}
	liveTotal := liveJev + liveClaude
	fmt.Printf("live cost estimate: %d calls, jev $%.4f, claude $%.4f, live total $%.4f (eval_usd cap $%.2f)\n",
		calls, liveJev, liveClaude, liveTotal, budgetCfg.EvalUSD)
	if anyLive && liveTotal > budgetCfg.EvalUSD && !*allowOverCap {
		return fmt.Errorf("estimated live cost $%.4f exceeds eval_usd $%.2f; pass -allow-over-cap to proceed", liveTotal, budgetCfg.EvalUSD)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	harness := &eval.Harness{
		DS: ds, Jev: jev, JevKind: jevKind, Claude: claude, ClaudeKind: claudeKind,
		Thresholds: thresholds,
	}
	run := harness.Run(ctx)
	sel := eval.SelectThresholds(run, thresholds, run.Mock())
	sel.Applied.LabelSource = eval.LabelSummary(ds.Sources)
	sel.Applied.SampleSize = len(ds.ArticleLabels) + len(ds.PairLabels) + len(ds.Matches)
	sel.Applied.Samples = config.SampleCounts{
		Articles: len(ds.ArticleLabels),
		Pairs:    len(ds.PairLabels),
		Match:    len(ds.Matches),
	}
	if !run.Mock() && !sel.Skipped && *writeThresholds {
		if err := config.SaveThresholds(*configDir+"/thresholds.yaml", sel.Applied); err != nil {
			return err
		}
		fmt.Println("wrote", *configDir+"/thresholds.yaml")
	}

	notes := []string{
		"Alert-prompt labels in match.candidates.jsonl are derived from article categories (derived: true), not independently labelled.",
	}
	if jevKind != claudeKind {
		notes = append(notes, fmt.Sprintf("Mixed run: jev %s, Claude %s. Threshold selection is skipped for mixed runs.", jevKind, claudeKind))
	}
	if jevMock {
		notes = append(notes, "No OPENROUTER_API_KEY: jev ran against the mock decisions endpoint.")
	}
	if claudeMock {
		notes = append(notes, "No ANTHROPIC_API_KEY: Claude ran against the mock Messages endpoint.")
	}
	if claudeMock && !jevMock {
		notes = append(notes, "Mock Claude returns simulated cache reads; that is plumbing evidence, not prompt-cache evidence.")
	}

	if err := eval.WriteReport(*reportPath, *jsonPath, eval.ReportData{
		Run: run, Selections: sel, DS: ds, Notes: notes,
	}); err != nil {
		return err
	}
	if run.Mock() {
		fmt.Println(eval.MockMarker)
	}
	fmt.Printf("wrote %s and %s\n", *reportPath, *jsonPath)
	for _, t := range run.Tasks {
		fmt.Printf("%-8s %-6s n=%-3d calls=%-3d answered=%-3d acc=%.3f p95=%.0fms $/1k=%.4f\n",
			t.Task, t.Backend, t.N, t.Calls, t.Answered, t.Accuracy, t.LatencyP95MS, t.CostPer1kUSD)
	}
	if !run.Mock() {
		fmt.Printf("fallback projected %.1f%% (met %t), place %s, relation %s\n",
			sel.FallbackRate*100, sel.FallbackMet, metLabel(sel.Plot.Met), metLabel(sel.Relate.Met))
	}

	var failed []string
	if !jevMock && backendFailed(run, "jev") {
		failed = append(failed, "jev")
	}
	if !claudeMock && backendFailed(run, "claude") {
		failed = append(failed, "claude")
	}
	if len(failed) > 0 {
		return fmt.Errorf("backend failed entirely with no usable answers: %v", failed)
	}
	return nil
}

// jevCredentials resolves the live jev provider. OpenRouter is the supported
// path. TypeSafe direct is not guessed: a TYPESAFE_API_KEY without an explicit
// compatible base URL, path and request shape fails before any call.
func jevCredentials() (key, base, path string, err error) {
	if k := os.Getenv("OPENROUTER_API_KEY"); k != "" {
		return k, os.Getenv("DOR_JEV_BASE_URL"), os.Getenv("DOR_JEV_PATH"), nil
	}
	if k := os.Getenv("TYPESAFE_API_KEY"); k != "" {
		base = firstEnv("DOR_JEV_BASE_URL", "TYPESAFE_BASE_URL")
		path = os.Getenv("DOR_JEV_PATH")
		shape := os.Getenv("DOR_JEV_REQUEST_SHAPE")
		if base == "" || path == "" || shape == "" {
			return "", "", "", errors.New("TYPESAFE_API_KEY needs DOR_JEV_BASE_URL, DOR_JEV_PATH and DOR_JEV_REQUEST_SHAPE; TypeSafe direct is not implemented, use OPENROUTER_API_KEY")
		}
		if shape != "openrouter-decisions" {
			return "", "", "", fmt.Errorf("unsupported DOR_JEV_REQUEST_SHAPE %q: TypeSafe direct /v1/systemone is not implemented; use OPENROUTER_API_KEY", shape)
		}
		return k, base, path, nil
	}
	return "", "", "", nil
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func backendFailed(run eval.RunResult, backend string) bool {
	seen, answered := false, false
	for _, t := range run.Tasks {
		if t.Backend != backend || t.N == 0 {
			continue
		}
		seen = true
		if t.Answered > 0 {
			answered = true
		}
	}
	return seen && !answered
}

func metLabel(met bool) string {
	if met {
		return "target met"
	}
	return "target not met"
}
