package llm

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/budget"
)

func cliJob() Job {
	return Job{
		Name:      "enrich",
		Tier:      TierHaiku,
		System:    "Classify the article.",
		User:      "Headline: Parliament passes budget",
		Schema:    testSchema(),
		MaxTokens: 400,
	}
}

func testSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"category":   map[string]any{"type": "string", "enum": []any{"politics", "other"}},
			"confidence": map[string]any{"type": "number", "minimum": 0.0, "maximum": 1.0},
		},
		"required":             []any{"category", "confidence"},
		"additionalProperties": false,
	}
}

func newTestCLI(t *testing.T, kind CLIKind, run CommandRunner) *CLIClient {
	t.Helper()
	c, err := NewCLI(CLIConfig{Kind: kind, HaikuModel: "small", OpusModel: "big", Run: run})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func argAfter(args []string, flag string) string {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}

// Captured from `claude -p --output-format json --json-schema ...` (2.1.322),
// trimmed to the fields the client reads.
const claudeOK = `{"type":"result","subtype":"success","is_error":false,"api_error_status":null,
"result":"{\"category\":\"politics\",\"confidence\":0.95}",
"structured_output":{"category":"politics","confidence":0.95},
"usage":{"input_tokens":2,"cache_creation_input_tokens":1449,"cache_read_input_tokens":0,"output_tokens":77},
"modelUsage":{"claude-haiku-5-5":{"inputTokens":1184}},"total_cost_usd":0.0004552}`

func TestClaudeCLISuccess(t *testing.T) {
	var got Command
	c := newTestCLI(t, CLIClaude, func(_ context.Context, cmd Command) ([]byte, []byte, error) {
		got = cmd
		return []byte(claudeOK), nil, nil
	})
	t.Setenv("ANTHROPIC_API_KEY", "must-not-leak")
	res, err := c.Complete(context.Background(), cliJob())
	if err != nil {
		t.Fatal(err)
	}
	if argAfter(got.Args, "--model") != "small" || argAfter(got.Args, "--system-prompt") != "Classify the article." ||
		argAfter(got.Args, "--tools") != "" || argAfter(got.Args, "--effort") != "low" {
		t.Errorf("args = %q", got.Args)
	}
	// Range constraints are moved into descriptions before the schema is sent.
	if s := argAfter(got.Args, "--json-schema"); strings.Contains(s, "minimum") || !strings.Contains(s, "politics") {
		t.Errorf("schema = %s", s)
	}
	if string(got.Stdin) != "Headline: Parliament passes budget" || got.Dir != c.dir {
		t.Errorf("stdin=%q dir=%q", got.Stdin, got.Dir)
	}
	for _, kv := range got.Env {
		if strings.HasPrefix(kv, "ANTHROPIC_API_KEY=") {
			t.Errorf("API key passed to the subscription CLI")
		}
	}
	var out struct {
		Category   string  `json:"category"`
		Confidence float64 `json:"confidence"`
	}
	if err := res.Decode(&out); err != nil || out.Category != "politics" {
		t.Errorf("decode = %+v err=%v", out, err)
	}
	if res.Model != "claude-haiku-5-5" || res.StopReason != "end_turn" || res.CostUSD != 0 ||
		res.Usage.CacheCreationInputTokens != 1449 || res.Attempts != 1 {
		t.Errorf("res = %+v", res)
	}
}

func TestClaudeCLIModelSelection(t *testing.T) {
	var model string
	c := newTestCLI(t, CLIClaude, func(_ context.Context, cmd Command) ([]byte, []byte, error) {
		model = argAfter(cmd.Args, "--model")
		return []byte(claudeOK), nil, nil
	})
	job := cliJob()
	job.Tier = TierOpus
	if _, err := c.Complete(context.Background(), job); err != nil || model != "big" {
		t.Errorf("opus tier model = %q err=%v", model, err)
	}
	// The tier mapping always wins for claude-cli: an explicit Claude id on
	// the job is never sent, so Opus-tier work cannot be redirected to Haiku.
	job.Model = "claude-explicit-1-0"
	if _, err := c.Complete(context.Background(), job); err != nil || model != "big" {
		t.Errorf("explicit model = %q err=%v, want the tier's model %q", model, err, "big")
	}
	job = cliJob()
	job.Tier = TierHaiku
	if _, err := c.Complete(context.Background(), job); err != nil || model != "small" {
		t.Errorf("haiku tier model = %q err=%v", model, err)
	}
}

func TestClaudeCLIErrors(t *testing.T) {
	cases := map[string]struct {
		stdout string
		err    error
		want   string
	}{
		"rate limited":  {stdout: `{"is_error":true,"subtype":"error_during_execution","api_error_status":429,"result":"usage limit reached"}`, err: errors.New("exit status 1"), want: "status 429"},
		"no output":     {stdout: `{"is_error":false,"result":"sorry","structured_output":null}`, want: "no structured output"},
		"not json":      {stdout: "boom", err: errors.New("exit status 1"), want: "exit status 1"},
		"garbage ok":    {stdout: "boom", want: "decode output"},
		"missing field": {stdout: `{"is_error":false,"result":""}`, want: "no structured output"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := newTestCLI(t, CLIClaude, func(context.Context, Command) ([]byte, []byte, error) {
				return []byte(tc.stdout), []byte("stderr text"), tc.err
			})
			_, err := c.Complete(context.Background(), cliJob())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// Captured from `codex exec --json` (0.158.0); the item-level error is a
// config warning Codex prints on every run.
const codexEvents = `{"type":"thread.started","thread_id":"t1"}
{"type":"item.completed","item":{"id":"item_0","type":"error","message":"Ignoring unknown features requirement"}}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"{\"category\":\"politics\",\"confidence\":0.99}"}}
{"type":"turn.completed","usage":{"input_tokens":16658,"cached_input_tokens":8000,"output_tokens":22,"reasoning_output_tokens":0}}
`

func TestCodexCLISuccess(t *testing.T) {
	var got Command
	var schema []byte
	c := newTestCLI(t, CLICodex, func(_ context.Context, cmd Command) ([]byte, []byte, error) {
		got = cmd
		schema, _ = os.ReadFile(argAfter(cmd.Args, "--output-schema"))
		out := argAfter(cmd.Args, "--output-last-message")
		if err := os.WriteFile(out, []byte(`{"category":"politics","confidence":0.99}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return []byte(codexEvents), nil, nil
	})
	job := cliJob()
	job.Model = "claude-haiku-5-5" // a Claude id on the job is never sent to Codex
	res, err := c.Complete(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got.Args[0] != "exec" || argAfter(got.Args, "--model") != "small" || argAfter(got.Args, "--sandbox") != "read-only" ||
		argAfter(got.Args, "-c") != "model_reasoning_effort=low" || got.Args[len(got.Args)-1] != "-" {
		t.Errorf("args = %q", got.Args)
	}
	if !strings.Contains(string(schema), `"additionalProperties":false`) {
		t.Errorf("schema = %s", schema)
	}
	prompt := string(got.Stdin)
	if !strings.Contains(prompt, "<instructions>\nClassify the article.\n</instructions>") || !strings.Contains(prompt, "Headline: Parliament passes budget") {
		t.Errorf("prompt = %q", prompt)
	}
	if res.Text != `{"category":"politics","confidence":0.99}` || res.Model != "small" ||
		res.Usage.InputTokens != 16658 || res.Usage.CacheReadInputTokens != 8000 {
		t.Errorf("res = %+v", res)
	}
	// The per-call directory (schema and answer files) is removed.
	if _, err := os.Stat(filepath.Dir(argAfter(got.Args, "--output-schema"))); !os.IsNotExist(err) {
		t.Errorf("call dir left behind: %v", err)
	}
}

func TestCodexCLITurnFailure(t *testing.T) {
	c := newTestCLI(t, CLICodex, func(context.Context, Command) ([]byte, []byte, error) {
		return []byte(`{"type":"turn.failed","error":{"message":"The 'x' model is not supported when using Codex with a ChatGPT account."}}` + "\n"), nil, errors.New("exit status 1")
	})
	_, err := c.Complete(context.Background(), cliJob())
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err = %v", err)
	}
	c2 := newTestCLI(t, CLICodex, func(context.Context, Command) ([]byte, []byte, error) { return nil, nil, nil })
	if _, err := c2.Complete(context.Background(), cliJob()); !errors.Is(err, ErrCLIOutput) {
		t.Errorf("empty answer err = %v", err)
	}
}

func TestCLIConcurrencyCap(t *testing.T) {
	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	c, err := NewCLI(CLIConfig{Kind: CLIClaude, HaikuModel: "s", OpusModel: "b", Concurrency: 2,
		Run: func(context.Context, Command) ([]byte, []byte, error) {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			inFlight.Add(-1)
			return []byte(claudeOK), nil, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.Complete(context.Background(), cliJob()) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if peak.Load() != 2 {
		t.Errorf("peak in flight = %d, want 2", peak.Load())
	}
}

func TestCLIValidation(t *testing.T) {
	if _, err := NewCLI(CLIConfig{Kind: "gemini-cli", HaikuModel: "a", OpusModel: "b"}); err == nil {
		t.Error("unknown kind accepted")
	}
	if _, err := NewCLI(CLIConfig{Kind: CLICodex}); err == nil {
		t.Error("empty models accepted")
	}
	if _, err := NewCLI(CLIConfig{Kind: CLIClaude, Binary: "dor-no-such-binary", HaikuModel: "a", OpusModel: "b"}); err == nil {
		t.Error("missing binary accepted")
	}
	c := newTestCLI(t, CLIClaude, func(context.Context, Command) ([]byte, []byte, error) {
		t.Error("ran an invalid job")
		return nil, nil, nil
	})
	job := cliJob()
	job.System = " "
	if _, err := c.Complete(context.Background(), job); err == nil {
		t.Error("empty system prompt accepted")
	}
	job = cliJob()
	job.Schema = map[string]any{"type": "object"}
	if _, err := c.Complete(context.Background(), job); err == nil {
		t.Error("invalid schema accepted")
	}
}

func TestCLICancelledContext(t *testing.T) {
	c := newTestCLI(t, CLIClaude, func(ctx context.Context, _ Command) ([]byte, []byte, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if _, err := c.Complete(ctx, cliJob()); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

// newBudgetTestCLI builds a CLI backend with a budget hook and a fake runner.
func newBudgetTestCLI(t *testing.T, kind CLIKind, b Budget, run CommandRunner) *CLIClient {
	t.Helper()
	c, err := NewCLI(CLIConfig{Kind: kind, HaikuModel: "small", OpusModel: "big", Budget: b, Run: run})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cliBudgetInputTokens is the input estimate Complete sends to the limiter:
// system, user and the sanitized schema.
func cliBudgetInputTokens(t *testing.T, job Job) int64 {
	t.Helper()
	schemaJSON, err := json.Marshal(SanitizeSchema(job.Schema))
	if err != nil {
		t.Fatal(err)
	}
	return estimateTokens([]byte(job.System)) + estimateTokens([]byte(job.User)) + estimateTokens(schemaJSON)
}

// successRunner returns the fixture output for kind: claudeOK for claude-cli,
// and the codex events plus a valid answer file for codex-cli.
func successRunner(t *testing.T, kind CLIKind) CommandRunner {
	t.Helper()
	if kind == CLIClaude {
		return func(context.Context, Command) ([]byte, []byte, error) {
			return []byte(claudeOK), nil, nil
		}
	}
	return func(_ context.Context, cmd Command) ([]byte, []byte, error) {
		out := argAfter(cmd.Args, "--output-last-message")
		if err := os.WriteFile(out, []byte(`{"category":"politics","confidence":0.99}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return []byte(codexEvents), nil, nil
	}
}

func TestClaudeCLIBudgetWiring(t *testing.T) {
	fb := &fakeBudget{}
	c := newBudgetTestCLI(t, CLIClaude, fb, successRunner(t, CLIClaude))
	job := cliJob()
	job.Priority = budget.PriorityWhy
	if _, err := c.Complete(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	wantInput := cliBudgetInputTokens(t, job)
	if len(fb.checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(fb.checks))
	}
	chk := fb.checks[0]
	if chk.API != "claude-cli" || chk.Priority != budget.PriorityWhy ||
		chk.EstimatedInputTokens != wantInput || chk.EstimatedOutputTokens != job.MaxTokens || chk.EstimatedUSD != 0 {
		t.Errorf("check = %+v, want api=claude-cli priority=why input=%d output=%d usd=0", chk, wantInput, job.MaxTokens)
	}

	if len(fb.records) != 1 {
		t.Fatalf("records = %d, want 1", len(fb.records))
	}
	rec := fb.records[0]
	// The subscription CLI spends quota, not dollars; tokens still count.
	if rec.Usage.CostUSD != 0 || rec.Usage.InputTokens != 2 || rec.Usage.OutputTokens != 77 {
		t.Errorf("record = %+v, want cost 0 input 2 output 77", rec.Usage)
	}
	if rec.Res.API != "claude-cli" {
		t.Errorf("reservation api = %q, want claude-cli", rec.Res.API)
	}
}

func TestCodexCLIBudgetWiring(t *testing.T) {
	fb := &fakeBudget{}
	c := newBudgetTestCLI(t, CLICodex, fb, successRunner(t, CLICodex))
	job := cliJob()
	if _, err := c.Complete(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	wantInput := cliBudgetInputTokens(t, job)
	if len(fb.checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(fb.checks))
	}
	chk := fb.checks[0]
	if chk.API != "codex-cli" || chk.EstimatedInputTokens != wantInput ||
		chk.EstimatedOutputTokens != job.MaxTokens || chk.EstimatedUSD != 0 {
		t.Errorf("check = %+v, want api=codex-cli input=%d output=%d usd=0", chk, wantInput, job.MaxTokens)
	}

	if len(fb.records) != 1 {
		t.Fatalf("records = %d, want 1", len(fb.records))
	}
	rec := fb.records[0]
	if rec.Usage.CostUSD != 0 || rec.Usage.InputTokens != 16658 || rec.Usage.OutputTokens != 22 {
		t.Errorf("record = %+v, want cost 0 input 16658 output 22", rec.Usage)
	}
	if rec.Res.API != "codex-cli" {
		t.Errorf("reservation api = %q, want codex-cli", rec.Res.API)
	}
}

func TestCLIBudgetDefaultPriority(t *testing.T) {
	for _, kind := range []CLIKind{CLIClaude, CLICodex} {
		t.Run(string(kind), func(t *testing.T) {
			fb := &fakeBudget{}
			c := newBudgetTestCLI(t, kind, fb, successRunner(t, kind))
			job := cliJob() // Priority is empty.
			if _, err := c.Complete(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if len(fb.checks) != 1 || fb.checks[0].Priority != budget.PriorityDefault {
				t.Errorf("checks = %+v, want one check with priority %q", fb.checks, budget.PriorityDefault)
			}
		})
	}
}

func TestCLIBudgetDenial(t *testing.T) {
	for _, kind := range []CLIKind{CLIClaude, CLICodex} {
		t.Run(string(kind), func(t *testing.T) {
			fb := &fakeBudget{denyFrom: 1, reason: "token-bucket"}
			ran := false
			c := newBudgetTestCLI(t, kind, fb, func(context.Context, Command) ([]byte, []byte, error) {
				ran = true
				return nil, nil, nil
			})
			_, err := c.Complete(context.Background(), cliJob())
			var be *BudgetError
			if !errors.Is(err, ErrBudget) || !errors.As(err, &be) {
				t.Fatalf("err = %v, want a *BudgetError wrapping ErrBudget", err)
			}
			if be.API != string(kind) || be.Reason != "token-bucket" {
				t.Errorf("budget error = %+v", be)
			}
			if ran {
				t.Error("the command runner ran despite the denial")
			}
			if len(fb.records) != 0 {
				t.Errorf("records = %+v, want none on denial", fb.records)
			}
		})
	}
}

func TestCLIBudgetRecordsZeroUsageOnFailure(t *testing.T) {
	fb := &fakeBudget{}
	boom := errors.New("exit status 1")
	c := newBudgetTestCLI(t, CLIClaude, fb, func(context.Context, Command) ([]byte, []byte, error) {
		return nil, []byte("stderr text"), boom
	})
	_, err := c.Complete(context.Background(), cliJob())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if len(fb.checks) != 1 {
		t.Errorf("checks = %d, want 1", len(fb.checks))
	}
	if len(fb.records) != 1 {
		t.Fatalf("records = %d, want one refund", len(fb.records))
	}
	if fb.records[0].Usage != (budget.Usage{}) {
		t.Errorf("recorded usage = %+v, want zero", fb.records[0].Usage)
	}
}

func TestClaudeCLIOAuthTokenPassthrough(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token-xyz")
	t.Setenv("ANTHROPIC_API_KEY", "sk-must-be-stripped")
	var got Command
	c := newTestCLI(t, CLIClaude, func(_ context.Context, cmd Command) ([]byte, []byte, error) {
		got = cmd
		return []byte(claudeOK), nil, nil
	})
	if _, err := c.Complete(context.Background(), cliJob()); err != nil {
		t.Fatal(err)
	}
	var oauth int
	for _, kv := range got.Env {
		if kv == "CLAUDE_CODE_OAUTH_TOKEN=oauth-token-xyz" {
			oauth++
		}
		if strings.HasPrefix(kv, "ANTHROPIC_API_KEY=") {
			t.Errorf("ANTHROPIC_API_KEY leaked to the subscription CLI")
		}
	}
	if oauth != 1 {
		t.Errorf("CLAUDE_CODE_OAUTH_TOKEN entries = %d, want exactly 1", oauth)
	}
}

func TestCLICloseRemovesWorkdir(t *testing.T) {
	c, err := NewCLI(CLIConfig{Kind: CLIClaude, HaikuModel: "small", OpusModel: "big",
		Run: func(context.Context, Command) ([]byte, []byte, error) { return nil, nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.dir); err != nil {
		t.Fatalf("workdir missing before Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(c.dir); !os.IsNotExist(err) {
		t.Errorf("workdir still present after Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}
