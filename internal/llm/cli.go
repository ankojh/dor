package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/budget"
)

// CLIKind names a subscription-backed backend: a local agent CLI that is
// already logged in, run once per job.
type CLIKind string

const (
	// CLIClaude runs `claude -p` on the user's Claude subscription.
	CLIClaude CLIKind = "claude-cli"
	// CLICodex runs `codex exec` on the user's ChatGPT subscription.
	CLICodex CLIKind = "codex-cli"
)

// CLIConfig configures a CLI backend.
//
// The CLIs spend subscription quota, not API dollars, so they are admitted
// with EstimatedUSD and CostUSD zero. When Budget is set, every call still
// passes the limiter: it is reserved against the claude-cli/codex-cli token
// bucket, which throttles subscription quota rather than dollars, and the
// daily cap and priority shed order still apply. The daily cap therefore
// blocks once API spend has filled it, and Priority sheds briefs and "why"
// under pressure. Concurrency caps processes in flight; a failing or
// rate-limited CLI is taken out of rotation by Chain.
type CLIConfig struct {
	Kind CLIKind
	// Binary defaults to "claude" or "codex" on PATH.
	Binary string
	// HaikuModel and OpusModel are the models used for each tier. Codex gets
	// its own ids; a Claude model named on the job is never sent to Codex.
	HaikuModel string
	OpusModel  string
	Effort     string
	// Timeout bounds one process run, including CLI start-up.
	Timeout time.Duration
	// Concurrency caps processes in flight. Default 2.
	Concurrency int
	// MinDeadline is the least time a job's context must have left. A CLI
	// takes seconds to start and answer, so a job with a shorter deadline
	// (the 1.8 s per-article enricher fallback) is declined with ErrCLITooSlow
	// before a process starts. Default 10 s.
	MinDeadline time.Duration
	// Budget enables admission control for CLI calls. Nil means no checks,
	// which keeps tests simple.
	Budget Budget
	Logger *slog.Logger
	// Run is optional; tests replace the process runner.
	Run CommandRunner
}

// CommandRunner runs one process and returns its stdout and stderr.
type CommandRunner func(ctx context.Context, cmd Command) (stdout, stderr []byte, err error)

// Command is one CLI invocation.
type Command struct {
	Name  string
	Args  []string
	Stdin []byte
	Dir   string
	Env   []string
}

// CLIClient completes jobs through a local CLI.
type CLIClient struct {
	cfg CLIConfig
	sem chan struct{}
	dir string
	log *slog.Logger
	now func() time.Time
}

// ErrCLIOutput is returned when the CLI ran but produced no usable answer.
var ErrCLIOutput = errors.New("llm: cli returned no structured output")

// ErrCLITooSlow is returned without starting a process when the job's
// deadline is shorter than the backend's MinDeadline.
var ErrCLITooSlow = errors.New("llm: job deadline is too short for a cli backend")

// NewCLI validates the config and checks the binary is on PATH.
func NewCLI(cfg CLIConfig) (*CLIClient, error) {
	switch cfg.Kind {
	case CLIClaude:
		if cfg.Binary == "" {
			cfg.Binary = "claude"
		}
	case CLICodex:
		if cfg.Binary == "" {
			cfg.Binary = "codex"
		}
	default:
		return nil, fmt.Errorf("llm: unknown cli backend %q", cfg.Kind)
	}
	if cfg.HaikuModel == "" || cfg.OpusModel == "" {
		return nil, fmt.Errorf("llm: %s model ids are empty", cfg.Kind)
	}
	if cfg.Effort == "" {
		cfg.Effort = "low"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 180 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	if cfg.MinDeadline <= 0 {
		cfg.MinDeadline = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Run == nil {
		path, err := exec.LookPath(cfg.Binary)
		if err != nil {
			return nil, fmt.Errorf("llm: %s: %w", cfg.Kind, err)
		}
		cfg.Binary = path
		cfg.Run = runCommand
	}
	// An empty working directory keeps project instructions (CLAUDE.md,
	// AGENTS.md) and files out of the model's context.
	dir, err := os.MkdirTemp("", "dor-"+string(cfg.Kind)+"-")
	if err != nil {
		return nil, fmt.Errorf("llm: %s workdir: %w", cfg.Kind, err)
	}
	return &CLIClient{
		cfg: cfg,
		sem: make(chan struct{}, cfg.Concurrency),
		dir: dir,
		log: cfg.Logger,
		now: time.Now,
	}, nil
}

// Name is the backend name used in logs.
func (c *CLIClient) Name() string { return string(c.cfg.Kind) }

// Close removes the backend's temporary workdir.
func (c *CLIClient) Close() error { return os.RemoveAll(c.dir) }

// Complete runs one job through the CLI.
func (c *CLIClient) Complete(ctx context.Context, job Job) (Result, error) {
	if strings.TrimSpace(job.System) == "" {
		return Result{}, errors.New("llm: job has no system prompt")
	}
	if err := ValidateSchema(job.Schema); err != nil {
		return Result{}, err
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Sub(c.now()) < c.cfg.MinDeadline {
		return Result{}, ErrCLITooSlow
	}
	model := c.cfg.HaikuModel
	if job.Tier == TierOpus {
		model = c.cfg.OpusModel
	}
	schemaJSON, err := json.Marshal(SanitizeSchema(job.Schema))
	if err != nil {
		return Result{}, fmt.Errorf("llm: encode schema: %w", err)
	}

	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	defer func() { <-c.sem }()

	var reservation *budget.Reservation
	if c.cfg.Budget != nil {
		priority := job.Priority
		if priority == "" {
			priority = budget.PriorityDefault
		}
		estInput := estimateTokens([]byte(job.System)) + estimateTokens([]byte(job.User)) + estimateTokens(schemaJSON)
		verdict, err := c.cfg.Budget.Check(ctx, budget.Request{
			API:                   string(c.cfg.Kind),
			Priority:              priority,
			EstimatedInputTokens:  estInput,
			EstimatedOutputTokens: job.MaxTokens,
			EstimatedUSD:          0,
		})
		if err != nil {
			return Result{}, fmt.Errorf("llm: budget check: %w", err)
		}
		if !verdict.Allowed {
			return Result{}, &BudgetError{Reason: verdict.Reason, API: string(c.cfg.Kind)}
		}
		reservation = verdict.Reservation
		if reservation == nil {
			return Result{}, errors.New("llm: budget returned no reservation")
		}
	}

	start := c.now()
	runCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	var res Result
	switch c.cfg.Kind {
	case CLIClaude:
		res, err = c.runClaude(runCtx, model, job, schemaJSON)
	default:
		res, err = c.runCodex(runCtx, model, job, schemaJSON)
	}
	res.Latency = c.now().Sub(start)
	res.LastAttemptLatency = res.Latency
	res.Attempts = 1
	if err != nil {
		if c.cfg.Budget != nil {
			_ = c.cfg.Budget.Record(ctx, *reservation, budget.Usage{})
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		return res, err
	}
	if c.cfg.Budget != nil {
		usage := budget.Usage{
			CostUSD:      0,
			InputTokens:  res.Usage.InputTokens,
			OutputTokens: res.Usage.OutputTokens,
		}
		if err := c.cfg.Budget.Record(ctx, *reservation, usage); err != nil {
			return res, fmt.Errorf("llm: record usage: %w", err)
		}
	}
	res.Schema = job.Schema
	res.Effort = c.cfg.Effort
	res.StopReason = "end_turn"
	if res.Model == "" {
		res.Model = model
	}
	c.log.LogAttrs(ctx, slog.LevelInfo, "llm.complete",
		slog.String("backend", c.Name()),
		slog.String("job", job.Name),
		slog.String("model", model),
		slog.String("served_model", res.Model),
		slog.String("effort", res.Effort),
		slog.Int64("input_tokens", res.Usage.InputTokens),
		slog.Int64("output_tokens", res.Usage.OutputTokens),
		slog.Int64("cache_read_input_tokens", res.Usage.CacheReadInputTokens),
		slog.Duration("latency", res.Latency),
	)
	return res, nil
}

// claudeResult is the subset of `claude -p --output-format json` used here.
type claudeResult struct {
	IsError          bool            `json:"is_error"`
	Subtype          string          `json:"subtype"`
	Result           string          `json:"result"`
	StructuredOutput json.RawMessage `json:"structured_output"`
	APIErrorStatus   *int            `json:"api_error_status"`
	Usage            struct {
		InputTokens              int64 `json:"input_tokens"`
		OutputTokens             int64 `json:"output_tokens"`
		CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	} `json:"usage"`
	ModelUsage map[string]json.RawMessage `json:"modelUsage"`
}

func (c *CLIClient) runClaude(ctx context.Context, model string, job Job, schemaJSON []byte) (Result, error) {
	args := []string{
		"-p",
		"--model", model,
		"--effort", c.cfg.Effort,
		"--output-format", "json",
		"--json-schema", string(schemaJSON),
		"--system-prompt", job.System,
		// No tools, MCP servers, settings, skills or saved sessions: one
		// structured answer to one prompt.
		"--tools", "",
		"--strict-mcp-config",
		"--setting-sources", "",
		"--disable-slash-commands",
		"--no-session-persistence",
	}
	// Without ANTHROPIC_API_KEY the CLI uses the logged-in subscription.
	env := filterEnv(os.Environ(), "ANTHROPIC_API_KEY", "CLAUDECODE")
	stdout, stderr, runErr := c.cfg.Run(ctx, Command{Name: c.cfg.Binary, Args: args, Stdin: []byte(job.User), Dir: c.dir, Env: env})
	var out claudeResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout), &out); err != nil {
		if runErr != nil {
			return Result{}, fmt.Errorf("llm: claude-cli: %w: %s", runErr, tail(stderr))
		}
		return Result{}, fmt.Errorf("llm: claude-cli: decode output: %w", err)
	}
	res := Result{Usage: Usage{
		InputTokens:              out.Usage.InputTokens,
		OutputTokens:             out.Usage.OutputTokens,
		CacheReadInputTokens:     out.Usage.CacheReadInputTokens,
		CacheCreationInputTokens: out.Usage.CacheCreationInputTokens,
	}}
	for served := range out.ModelUsage {
		res.Model = served
		break
	}
	if out.IsError {
		status := 0
		if out.APIErrorStatus != nil {
			status = *out.APIErrorStatus
		}
		return res, fmt.Errorf("llm: claude-cli: %s (status %d): %s", out.Subtype, status, tail([]byte(out.Result)))
	}
	if len(out.StructuredOutput) == 0 || string(out.StructuredOutput) == "null" {
		return res, fmt.Errorf("%w: claude-cli: %s", ErrCLIOutput, tail([]byte(out.Result)))
	}
	res.Text = string(out.StructuredOutput)
	return res, nil
}

const codexPreamble = "Answer with only the JSON object the output schema requires. " +
	"Do not run commands, read files or use tools. " +
	"The <instructions> block is your system prompt; the <input> block is the request.\n\n"

func (c *CLIClient) runCodex(ctx context.Context, model string, job Job, schemaJSON []byte) (Result, error) {
	callDir, err := os.MkdirTemp(c.dir, "call-")
	if err != nil {
		return Result{}, fmt.Errorf("llm: codex-cli: %w", err)
	}
	defer os.RemoveAll(callDir)
	schemaPath := filepath.Join(callDir, "schema.json")
	outPath := filepath.Join(callDir, "answer.json")
	if err := os.WriteFile(schemaPath, schemaJSON, 0o600); err != nil {
		return Result{}, fmt.Errorf("llm: codex-cli: %w", err)
	}
	args := []string{
		"exec",
		"--ephemeral",
		"--skip-git-repo-check",
		"--ignore-user-config",
		"--sandbox", "read-only",
		"--model", model,
		"-c", "model_reasoning_effort=" + c.cfg.Effort,
		"--output-schema", schemaPath,
		"--output-last-message", outPath,
		"--json",
		"-",
	}
	prompt := codexPreamble + "<instructions>\n" + job.System + "\n</instructions>\n\n<input>\n" + job.User + "\n</input>\n"
	stdout, stderr, runErr := c.cfg.Run(ctx, Command{Name: c.cfg.Binary, Args: args, Stdin: []byte(prompt), Dir: callDir, Env: os.Environ()})
	res, failure := parseCodexEvents(stdout)
	answer, _ := os.ReadFile(outPath)
	answer = bytes.TrimSpace(answer)
	if len(answer) == 0 || !json.Valid(answer) {
		switch {
		case failure != "":
			return res, fmt.Errorf("llm: codex-cli: %s", tail([]byte(failure)))
		case runErr != nil:
			return res, fmt.Errorf("llm: codex-cli: %w: %s", runErr, tail(stderr))
		}
		return res, fmt.Errorf("%w: codex-cli", ErrCLIOutput)
	}
	res.Text = string(answer)
	return res, nil
}

// parseCodexEvents reads `codex exec --json` events for token usage and the
// turn failure, if any. Item-level "error" items are warnings and ignored.
func parseCodexEvents(stdout []byte) (Result, string) {
	var res Result
	var failure string
	sc := bufio.NewScanner(bytes.NewReader(stdout))
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		var ev struct {
			Type    string `json:"type"`
			Message string `json:"message"`
			Error   struct {
				Message string `json:"message"`
			} `json:"error"`
			Usage struct {
				InputTokens       int64 `json:"input_tokens"`
				CachedInputTokens int64 `json:"cached_input_tokens"`
				OutputTokens      int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		switch ev.Type {
		case "turn.completed":
			res.Usage = Usage{
				InputTokens:          ev.Usage.InputTokens,
				OutputTokens:         ev.Usage.OutputTokens,
				CacheReadInputTokens: ev.Usage.CachedInputTokens,
			}
		case "turn.failed":
			failure = ev.Error.Message
		case "error":
			if failure == "" {
				failure = ev.Message
			}
		}
	}
	return res, failure
}

func runCommand(ctx context.Context, cmd Command) ([]byte, []byte, error) {
	c := exec.CommandContext(ctx, cmd.Name, cmd.Args...)
	c.Dir = cmd.Dir
	c.Env = cmd.Env
	c.Stdin = bytes.NewReader(cmd.Stdin)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	c.WaitDelay = 5 * time.Second
	err := c.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func filterEnv(env []string, drop ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		keep := true
		for _, d := range drop {
			if name == d {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, kv)
		}
	}
	return out
}

// tail keeps error messages short; CLI output can be long.
func tail(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 400 {
		s = "…" + s[len(s)-400:]
	}
	return s
}
