// Package llm is Dor's Claude client.
//
// It uses the official Go SDK (github.com/anthropics/anthropic-sdk-go/v2)
// through the beta Messages endpoint so structured outputs (output_config),
// explicit effort, and the server-side refusal fallback share one path.
//
// Rules this package enforces:
//
//   - model ids are plain strings from config (claude-haiku-5-5,
//     claude-opus-5-5);
//   - effort is always sent explicitly (low by default). Opus 5.5 cannot
//     disable thinking, so no thinking.budget_tokens and no disabled thinking;
//   - stop_reason is checked before content is read: refusal (with
//     stop_details), max_tokens and any unexpected stop are typed errors;
//   - no assistant prefill and no forced tool_choice;
//   - system prompts and schemas are byte-stable, with cache_control on the
//     last stable system block and volatile content only in the user message;
//   - retry 429/5xx/transport errors with backoff, never 400/401/403/404;
//   - the budget hook is checked before every attempt.
//
// Without ANTHROPIC_API_KEY the client is never built. Callers use mockapi
// (a realistic fake Messages endpoint) so every code path is exercised
// without a key, and reports produced that way are marked as mock.
package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
	"github.com/anthropics/anthropic-sdk-go/v2/shared/constant"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
)

// Budget is the limiter surface the client needs.
type Budget interface {
	Check(ctx context.Context, req budget.Request) (budget.Verdict, error)
	Record(ctx context.Context, res budget.Reservation, usage budget.Usage) error
}

// Config configures the Claude client.
type Config struct {
	APIKey     string
	BaseURL    string // empty uses the SDK default; set for mockapi
	HaikuModel string
	OpusModel  string
	Effort     string
	Timeout    time.Duration
	MaxRetries int
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Price      map[string]config.Price
	HTTPClient *http.Client
	Sleep      func(ctx context.Context, d time.Duration) error
	Jitter     func() float64
	Logger     *slog.Logger
}

// Client is the Claude client.
type Client struct {
	cfg    Config
	sdk    anthropic.Client
	budget Budget
	log    *slog.Logger
	sleep  func(context.Context, time.Duration) error
	jitter func() float64
	now    func() time.Time
}

// New validates the config and returns a client. A missing key is
// ErrNoAPIKey; callers must fail closed rather than call.
func New(cfg Config, b Budget) (*Client, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, ErrNoAPIKey
	}
	if b == nil {
		return nil, errors.New("llm: budget hook is required")
	}
	if cfg.HaikuModel == "" || cfg.OpusModel == "" {
		return nil, errors.New("llm: model ids are empty")
	}
	switch cfg.Effort {
	case "":
		cfg.Effort = "low"
	case "low", "medium", "high", "xhigh", "max":
	default:
		return nil, fmt.Errorf("llm: effort %q is invalid", cfg.Effort)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = 15 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepContext
	}
	if cfg.Jitter == nil {
		cfg.Jitter = rand.Float64
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	opts := []option.RequestOption{
		option.WithAPIKey(cfg.APIKey),
		// Retries are ours, so the SDK must not retry underneath us: one
		// budget check must correspond to one HTTP attempt.
		option.WithMaxRetries(0),
	}
	if cfg.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseURL))
	}
	if cfg.HTTPClient != nil {
		opts = append(opts, option.WithHTTPClient(cfg.HTTPClient))
	}
	return &Client{
		cfg:    cfg,
		sdk:    anthropic.NewClient(opts...),
		budget: b,
		log:    cfg.Logger,
		sleep:  cfg.Sleep,
		jitter: cfg.Jitter,
		now:    time.Now,
	}, nil
}

// NewFromConfig builds a client from config/models.yaml.
func NewFromConfig(m config.Models, apiKey, baseURL string, b Budget) (*Client, error) {
	return New(Config{
		APIKey:     apiKey,
		BaseURL:    baseURL,
		HaikuModel: m.Claude.HaikuModel,
		OpusModel:  m.Claude.OpusModel,
		Effort:     m.Claude.Effort,
		Timeout:    m.Claude.Timeout.Std(),
		MaxRetries: m.Claude.MaxRetries,
		MinBackoff: m.Claude.MinBackoff.Std(),
		MaxBackoff: m.Claude.MaxBackoff.Std(),
		Price:      m.Claude.Price,
	}, b)
}

// Job is one structured-output request. System is the stable cached prefix;
// User is the only volatile part. If Model is empty, Tier selects the model.
type Job struct {
	Name      string
	Tier      Tier
	Model     string
	System    string
	User      string
	Schema    map[string]any
	MaxTokens int64
	// Fallbacks enables the server-side refusal fallback. Only Opus supports
	// it; sending it on Haiku returns a 400.
	Fallbacks bool
	Priority  budget.Priority
}

// Usage is one call's token usage.
type Usage struct {
	InputTokens              int64
	OutputTokens             int64
	CacheReadInputTokens     int64
	CacheCreationInputTokens int64
}

// Result is a successful (end_turn) or typed-stop response.
type Result struct {
	Text        string
	Schema      map[string]any `json:"-"`
	StopReason  string
	Refusal     string // stop_details.category
	Explanation string // stop_details.explanation
	Model       string
	Effort      string
	Usage       Usage
	// Latency is total elapsed time for the logical call, including retries
	// and backoff. LastAttemptLatency is the final attempt only.
	Latency            time.Duration
	LastAttemptLatency time.Duration
	Attempts           int
	CostUSD            float64
}

// Decode unmarshals the structured-output JSON into dest.
func (r Result) Decode(dest any) error {
	if r.Text == "" {
		return errors.New("llm: no text content to decode")
	}
	// Check the original constraints (min/max, maxLength, enum, ...) before
	// returning the typed value. The wire schema has them stripped; this is
	// where they are enforced.
	var raw any
	if err := json.Unmarshal([]byte(r.Text), &raw); err != nil {
		return fmt.Errorf("llm: decode structured output: %w", err)
	}
	if r.Schema != nil {
		if err := ValidateValue(r.Schema, raw); err != nil {
			return err
		}
	}
	if err := json.Unmarshal([]byte(r.Text), dest); err != nil {
		return fmt.Errorf("llm: decode structured output: %w", err)
	}
	return nil
}

// Errors callers branch on.
var (
	ErrNoAPIKey       = errors.New("llm: no ANTHROPIC_API_KEY")
	ErrBudget         = errors.New("llm: budget denied")
	ErrRefusal        = errors.New("llm: model refused")
	ErrMaxTokens      = errors.New("llm: stopped at max_tokens")
	ErrUnexpectedStop = errors.New("llm: unexpected stop_reason")
)

// StopError carries the stop_reason details for refusal and max_tokens.
type StopError struct {
	Kind        error
	StopReason  string
	Category    string
	Explanation string
	// Usage and accounting travel with the error so a caller that only checks
	// the error still sees the paid call.
	Usage              Usage
	CostUSD            float64
	Latency            time.Duration
	LastAttemptLatency time.Duration
}

// Error implements error.
func (e *StopError) Error() string {
	if e.Kind == ErrRefusal {
		return fmt.Sprintf("llm: refused (%s): %s", e.Category, e.Explanation)
	}
	return fmt.Sprintf("llm: stop_reason %q", e.StopReason)
}

// Unwrap points at the sentinel.
func (e *StopError) Unwrap() error { return e.Kind }

// BudgetError is returned when the limiter denies a call.
type BudgetError struct {
	Reason string
	API    string
}

// Error implements error.
func (e *BudgetError) Error() string {
	return fmt.Sprintf("llm: budget denied for %s: %s", e.API, e.Reason)
}

// Unwrap makes errors.Is(err, ErrBudget) true.
func (e *BudgetError) Unwrap() error { return ErrBudget }

// Complete runs one job. Content is never read before stop_reason is checked.
func (c *Client) Complete(ctx context.Context, job Job) (Result, error) {
	if job.Model == "" {
		if job.Tier == TierOpus {
			job.Model = c.cfg.OpusModel
		} else {
			job.Model = c.cfg.HaikuModel
		}
	}
	if job.Fallbacks && job.Model != c.cfg.OpusModel {
		return Result{}, fmt.Errorf("llm: server-side fallback is not supported on %s", job.Model)
	}
	if strings.TrimSpace(job.System) == "" {
		return Result{}, errors.New("llm: job has no system prompt")
	}
	if len(job.Schema) == 0 {
		return Result{}, errors.New("llm: job has no schema")
	}
	if job.MaxTokens <= 0 {
		return Result{}, errors.New("llm: job has no max_tokens")
	}
	price, ok := c.cfg.Price[job.Model]
	if !ok {
		return Result{}, fmt.Errorf("llm: no price configured for %s", job.Model)
	}
	api := c.apiFor(job.Model)
	effort := c.cfg.Effort
	priority := job.Priority
	if priority == "" {
		priority = budget.PriorityDefault
	}

	params, estInput, err := c.buildParams(job, effort)
	if err != nil {
		return Result{}, err
	}
	costEstimate := price.Cost(estInput, job.MaxTokens)
	startTotal := c.now()

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		verdict, err := c.budget.Check(ctx, budget.Request{
			API:                   api,
			Priority:              priority,
			EstimatedInputTokens:  estInput,
			EstimatedOutputTokens: job.MaxTokens,
			EstimatedUSD:          costEstimate,
		})
		if err != nil {
			return Result{}, fmt.Errorf("llm: budget check: %w", err)
		}
		if !verdict.Allowed {
			return Result{}, &BudgetError{Reason: verdict.Reason, API: api}
		}
		reservation := verdict.Reservation
		if reservation == nil {
			return Result{}, errors.New("llm: budget returned no reservation")
		}

		start := c.now()
		attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		msg, callErr := c.sdk.Beta.Messages.New(attemptCtx, params)
		cancel()
		lastAttempt := c.now().Sub(start)
		if callErr != nil {
			_ = c.budget.Record(ctx, *reservation, budget.Usage{})
			lastErr = callErr
			partial := Result{Latency: c.now().Sub(startTotal), LastAttemptLatency: lastAttempt, Attempts: attempt + 1}
			if ctx.Err() != nil || !retryable(callErr) || attempt == c.cfg.MaxRetries {
				return partial, fmt.Errorf("llm: messages: %w", callErr)
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return partial, err
			}
			continue
		}

		res := Result{
			Schema:             job.Schema,
			StopReason:         string(msg.StopReason),
			Model:              string(msg.Model),
			Effort:             effort,
			Latency:            c.now().Sub(startTotal),
			LastAttemptLatency: lastAttempt,
			Attempts:           attempt + 1,
			Usage: Usage{
				InputTokens:              msg.Usage.InputTokens,
				OutputTokens:             msg.Usage.OutputTokens,
				CacheReadInputTokens:     msg.Usage.CacheReadInputTokens,
				CacheCreationInputTokens: msg.Usage.CacheCreationInputTokens,
			},
		}
		// Claude reports tokens, not money. Price cached input separately
		// (reads at 0.1x, writes at 1.25x unless config overrides).
		res.CostUSD = price.CostUsage(
			res.Usage.InputTokens,
			res.Usage.OutputTokens,
			res.Usage.CacheReadInputTokens,
			res.Usage.CacheCreationInputTokens,
		)
		usage := budget.Usage{
			CostUSD:      res.CostUSD,
			InputTokens:  res.Usage.InputTokens,
			OutputTokens: res.Usage.OutputTokens,
		}
		if err := c.budget.Record(ctx, *reservation, usage); err != nil {
			return res, fmt.Errorf("llm: record usage: %w", err)
		}
		c.log.LogAttrs(ctx, slog.LevelInfo, "llm.complete",
			slog.String("job", job.Name),
			slog.String("model", job.Model),
			slog.String("served_model", res.Model),
			slog.String("effort", res.Effort),
			slog.String("stop_reason", res.StopReason),
			slog.Int("attempts", res.Attempts),
			slog.Int64("input_tokens", res.Usage.InputTokens),
			slog.Int64("output_tokens", res.Usage.OutputTokens),
			slog.Int64("cache_read_input_tokens", res.Usage.CacheReadInputTokens),
			slog.Int64("cache_creation_input_tokens", res.Usage.CacheCreationInputTokens),
			slog.Float64("cost_usd", res.CostUSD),
			slog.Duration("latency", res.Latency),
			slog.Duration("last_attempt_latency", res.LastAttemptLatency),
		)

		// stop_reason is checked before content is read.
		switch msg.StopReason {
		case anthropic.BetaStopReasonEndTurn:
			text, err := firstText(msg)
			if err != nil {
				return res, err
			}
			res.Text = text
			return res, nil
		case anthropic.BetaStopReasonRefusal:
			res.Refusal = string(msg.StopDetails.Category)
			res.Explanation = msg.StopDetails.Explanation
			return res, &StopError{
				Kind:               ErrRefusal,
				StopReason:         res.StopReason,
				Category:           res.Refusal,
				Explanation:        res.Explanation,
				Usage:              res.Usage,
				CostUSD:            res.CostUSD,
				Latency:            res.Latency,
				LastAttemptLatency: res.LastAttemptLatency,
			}
		case anthropic.BetaStopReasonMaxTokens:
			return res, &StopError{
				Kind: ErrMaxTokens, StopReason: res.StopReason,
				Usage: res.Usage, CostUSD: res.CostUSD, Latency: res.Latency, LastAttemptLatency: res.LastAttemptLatency,
			}
		default:
			return res, &StopError{
				Kind: ErrUnexpectedStop, StopReason: res.StopReason,
				Usage: res.Usage, CostUSD: res.CostUSD, Latency: res.Latency, LastAttemptLatency: res.LastAttemptLatency,
			}
		}
	}
	return Result{}, lastErr
}

func (c *Client) buildParams(job Job, effort string) (anthropic.BetaMessageNewParams, int64, error) {
	// Estimate from the wire schema (constraints moved into descriptions), the
	// same bytes EstimateJobTokens counts.
	if err := ValidateSchema(job.Schema); err != nil {
		return anthropic.BetaMessageNewParams{}, 0, err
	}
	cleanSchema := SanitizeSchema(job.Schema)
	schemaJSON, err := json.Marshal(cleanSchema)
	if err != nil {
		return anthropic.BetaMessageNewParams{}, 0, fmt.Errorf("llm: encode schema: %w", err)
	}
	params := anthropic.BetaMessageNewParams{
		Model:     job.Model,
		MaxTokens: job.MaxTokens,
		System: []anthropic.BetaTextBlockParam{{
			Text: job.System,
			// Byte-stable system prompt with the cache breakpoint on the last
			// stable block. Volatile content stays in the user message.
			CacheControl: anthropic.BetaCacheControlEphemeralParam{Type: constant.ValueOf[constant.Ephemeral]()},
		}},
		Messages: []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(
			anthropic.BetaContentBlockParamUnion{OfText: &anthropic.BetaTextBlockParam{Text: job.User}},
		)},
		OutputConfig: anthropic.BetaOutputConfigParam{
			Effort: anthropic.BetaOutputConfigEffort(effort),
			Format: anthropic.BetaJSONOutputFormatParam{
				// The API rejects JSON-Schema keywords it does not support.
				// Constraints stay in the original job.Schema and are enforced
				// by Result.Decode.
				Schema: cleanSchema,
				Type:   constant.ValueOf[constant.JSONSchema](),
			},
		},
	}
	if job.Fallbacks {
		params.Fallbacks = anthropic.BetaFallbacksParamOfDefault()
		params.Betas = []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01}
	}
	est := estimateTokens([]byte(job.System)) + estimateTokens([]byte(job.User)) + estimateTokens(schemaJSON)
	return params, est, nil
}

func firstText(msg *anthropic.BetaMessage) (string, error) {
	for _, block := range msg.Content {
		if block.Type == "text" {
			return block.Text, nil
		}
	}
	return "", errors.New("llm: end_turn with no text content block")
}

func (c *Client) apiFor(model string) string {
	switch model {
	case c.cfg.OpusModel:
		return "claude-opus"
	default:
		return "claude-haiku"
	}
}

func retryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A per-attempt timeout is retryable while the parent is alive; Complete
	// checks the parent before scheduling the next attempt.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusTooManyRequests ||
			(apiErr.StatusCode >= 500 && apiErr.StatusCode <= 599)
	}
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// Transport errors (DNS, reset) have no API status.
	return true
}

func (c *Client) backoff(ctx context.Context, attempt int) error {
	d := c.cfg.MinBackoff << attempt
	if d > c.cfg.MaxBackoff || d <= 0 {
		d = c.cfg.MaxBackoff
	}
	d = time.Duration(float64(d) * (0.5 + 0.5*c.jitter()))
	return c.sleep(ctx, d)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func estimateTokens(b []byte) int64 {
	return int64(len(b)/4 + 1)
}
