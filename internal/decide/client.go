package decide

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
)

// Budget is the limiter surface the client needs. *budget.Manager implements
// it; tests use budget.NewMemory.
type Budget interface {
	Check(ctx context.Context, req budget.Request) (budget.Verdict, error)
	Record(ctx context.Context, res budget.Reservation, usage budget.Usage) error
}

// ClientConfig configures a jev client. Model must be pinned; floating
// aliases are rejected.
type ClientConfig struct {
	BaseURL    string
	Path       string
	APIKey     string
	Model      string
	Timeout    time.Duration
	MaxRetries int
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Price estimates spend before the call. Live cost comes from usage.cost.
	Price config.Price
	// Priority labels every call for the shed order.
	Priority budget.Priority
	// HTTPClient is optional; a timeout-bound client is created when nil.
	HTTPClient *http.Client
	// Sleep is optional; tests replace it to avoid real waits.
	Sleep func(ctx context.Context, d time.Duration) error
	// Jitter returns [0,1). Tests pin it.
	Jitter func() float64
	Logger *slog.Logger
}

// Client is the jev decisions client.
type Client struct {
	cfg    ClientConfig
	http   *http.Client
	budget Budget
	log    *slog.Logger
	sleep  func(context.Context, time.Duration) error
	jitter func() float64
	now    func() time.Time
}

// NewClient validates the config and returns a client.
func NewClient(cfg ClientConfig, b Budget) (*Client, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, ErrNoAPIKey
	}
	if err := config.ValidateModelPin(cfg.Model); err != nil {
		return nil, fmt.Errorf("decide: %w", err)
	}
	if cfg.BaseURL == "" {
		return nil, errors.New("decide: base URL is empty")
	}
	if _, err := url.Parse(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("decide: base URL %q: %w", cfg.BaseURL, err)
	}
	if cfg.Path == "" {
		cfg.Path = "/api/alpha/decisions"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 250 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = 8 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.Priority == "" {
		cfg.Priority = budget.PriorityDefault
	}
	if b == nil {
		return nil, errors.New("decide: budget hook is required")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
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
	return &Client{
		cfg:    cfg,
		http:   hc,
		budget: b,
		log:    cfg.Logger,
		sleep:  cfg.Sleep,
		jitter: cfg.Jitter,
		now:    time.Now,
	}, nil
}

// NewClientFromModels builds a client from config/models.yaml values. baseURL
// and key override the file.
func NewClientFromModels(m config.Models, baseURL, apiKey string, b Budget) (*Client, error) {
	if baseURL == "" {
		baseURL = m.Jev.BaseURL
	}
	return NewClient(ClientConfig{
		BaseURL:    baseURL,
		Path:       m.Jev.Path,
		APIKey:     apiKey,
		Model:      m.Jev.Model,
		Timeout:    m.Jev.Timeout.Std(),
		MaxRetries: m.Jev.MaxRetries,
		MinBackoff: m.Jev.MinBackoff.Std(),
		MaxBackoff: m.Jev.MaxBackoff.Std(),
		Price:      m.Jev.Price,
	}, b)
}

// wireQuestion is the JSON shape jev expects. Criteria is a map for noul and
// choice and an array for score.
type wireQuestion struct {
	Type         QuestionType `json:"type"`
	Instructions string       `json:"instructions,omitempty"`
	Criteria     any          `json:"criteria,omitempty"`
}

func (q Question) wire() wireQuestion {
	w := wireQuestion{Type: q.Type, Instructions: q.Instructions}
	switch q.Type {
	case TypeNoul, TypeChoice:
		if len(q.Criteria) > 0 {
			w.Criteria = q.Criteria
		}
	case TypeScore:
		if len(q.Levels) > 0 {
			w.Criteria = q.Levels
		}
	}
	return w
}

type wireRequest struct {
	Model     string                  `json:"model"`
	State     any                     `json:"state"`
	Questions map[string]wireQuestion `json:"questions"`
	SessionID string                  `json:"session_id,omitempty"`
	User      string                  `json:"user,omitempty"`
}

func (c *Client) buildBody(req Request) ([]byte, error) {
	qs := make(map[string]wireQuestion, len(req.Questions))
	for k, q := range req.Questions {
		qs[k] = q.wire()
	}
	return json.Marshal(wireRequest{
		Model:     c.cfg.Model,
		State:     req.State,
		Questions: qs,
		SessionID: req.SessionID,
		User:      req.User,
	})
}

// Decide makes one decisions call, retrying only 429/5xx/524/529 and
// transport errors. The budget is checked before every attempt.
func (c *Client) Decide(ctx context.Context, req Request) (Response, error) {
	body, err := c.buildBody(req)
	if err != nil {
		return Response{}, fmt.Errorf("decide: encode request: %w", err)
	}
	inputEstimate := estimateTokens(body)
	costEstimate := c.cfg.Price.Cost(inputEstimate, 0)
	endpoint := strings.TrimRight(c.cfg.BaseURL, "/") + c.cfg.Path
	startTotal := c.now()
	priority := c.cfg.Priority
	if req.Priority != "" {
		priority = req.Priority
	}

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		verdict, err := c.budget.Check(ctx, budget.Request{
			API:                   "jev",
			Priority:              priority,
			EstimatedInputTokens:  inputEstimate,
			EstimatedOutputTokens: 0,
			EstimatedUSD:          costEstimate,
		})
		if err != nil {
			return Response{}, fmt.Errorf("decide: budget check: %w", err)
		}
		if !verdict.Allowed {
			return Response{}, &BudgetError{Reason: verdict.Reason, API: "jev"}
		}
		reservation := verdict.Reservation
		if reservation == nil {
			return Response{}, errors.New("decide: budget returned no reservation")
		}

		start := c.now()
		resp, httpErr, err := c.do(ctx, endpoint, body)
		lastAttempt := c.now().Sub(start)
		if err != nil {
			_ = c.budget.Record(ctx, *reservation, budget.Usage{})
			lastErr = err
			partial := Response{Latency: c.now().Sub(startTotal), LastAttemptLatency: lastAttempt, Attempts: attempt + 1}
			if ctx.Err() != nil || !isRetryableError(err) || attempt == c.cfg.MaxRetries {
				return partial, err
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return partial, err
			}
			continue
		}
		if httpErr != nil {
			_ = c.budget.Record(ctx, *reservation, budget.Usage{})
			lastErr = httpErr
			partial := Response{Latency: c.now().Sub(startTotal), LastAttemptLatency: lastAttempt, Attempts: attempt + 1}
			if ctx.Err() != nil || !retryableStatus(httpErr.Status) || attempt == c.cfg.MaxRetries {
				return partial, httpErr
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return partial, err
			}
			continue
		}
		resp.Latency = c.now().Sub(startTotal)
		resp.LastAttemptLatency = lastAttempt
		resp.Attempts = attempt + 1
		usage := budget.Usage{
			CostUSD:      resp.Usage.CostUSD,
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
		}
		if err := c.budget.Record(ctx, *reservation, usage); err != nil {
			return resp, fmt.Errorf("decide: record usage: %w", err)
		}
		c.log.LogAttrs(ctx, slog.LevelInfo, "jev.decide",
			slog.String("model", c.cfg.Model),
			slog.String("served_model", resp.Model),
			slog.String("request_id", resp.ID),
			slog.Int("attempts", resp.Attempts),
			slog.Int64("input_tokens", resp.Usage.InputTokens),
			slog.Int64("output_tokens", resp.Usage.OutputTokens),
			slog.Int64("cached_tokens", resp.Usage.CachedTokens),
			slog.Float64("cost_usd", resp.Usage.CostUSD),
			slog.Duration("latency", resp.Latency),
			slog.Duration("last_attempt_latency", resp.LastAttemptLatency),
		)
		return resp, nil
	}
	return Response{}, lastErr
}

func (c *Client) do(ctx context.Context, endpoint string, body []byte) (Response, *HTTPError, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return Response{}, nil, fmt.Errorf("decide: build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		return Response{}, nil, fmt.Errorf("decide: jev request: %w", err)
	}
	defer httpResp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, 4<<20))
	if err != nil {
		return Response{}, nil, fmt.Errorf("decide: read response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		he := &HTTPError{Status: httpResp.StatusCode, Body: string(payload)}
		var eb struct {
			Error struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(payload, &eb) == nil {
			he.Code = eb.Error.Code
			he.Message = eb.Error.Message
		}
		return Response{}, he, nil
	}
	resp, err := parseResponse(payload)
	if err != nil {
		return Response{}, nil, err
	}
	return resp, nil, nil
}

type wireAnswer struct {
	Type          QuestionType       `json:"type"`
	Noul          *float64           `json:"noul"`
	Choice        *string            `json:"choice"`
	Score         *float64           `json:"score"`
	Confidence    *float64           `json:"confidence"`
	Probabilities map[string]float64 `json:"probabilities"`
	Legend        map[string]string  `json:"legend"`
}

type wireResponse struct {
	ID       string                `json:"id"`
	Model    string                `json:"model"`
	Provider string                `json:"provider"`
	Answers  map[string]wireAnswer `json:"answers"`
	Usage    struct {
		InputTokens        int64   `json:"input_tokens"`
		OutputTokens       int64   `json:"output_tokens"`
		Cost               float64 `json:"cost"`
		InputTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	} `json:"usage"`
}

func parseResponse(payload []byte) (Response, error) {
	var wire wireResponse
	if err := json.Unmarshal(payload, &wire); err != nil {
		return Response{}, fmt.Errorf("decide: decode response: %w", err)
	}
	out := Response{
		ID:       wire.ID,
		Model:    wire.Model,
		Provider: wire.Provider,
		Answers:  make(map[string]Answer, len(wire.Answers)),
		Usage: Usage{
			InputTokens:  wire.Usage.InputTokens,
			OutputTokens: wire.Usage.OutputTokens,
			CachedTokens: wire.Usage.InputTokensDetails.CachedTokens,
			CostUSD:      wire.Usage.Cost,
		},
	}
	for key, wa := range wire.Answers {
		a := Answer{
			Type:          wa.Type,
			Present:       true,
			Refusal:       wa.Type == "refusal",
			Probabilities: wa.Probabilities,
			Legend:        wa.Legend,
		}
		if wa.Noul != nil {
			a.Noul = *wa.Noul
			a.HasNoul = true
		}
		if wa.Choice != nil {
			a.Choice = *wa.Choice
		}
		if wa.Score != nil {
			a.Score = *wa.Score
			a.HasScore = true
		}
		if wa.Confidence != nil {
			a.Confidence = *wa.Confidence
			a.HasConfidence = true
		}
		out.Answers[key] = a
	}
	return out, nil
}

// retryableStatus lists the only statuses worth retrying. 400, 401, 402, 404
// and 413 are permanent: retrying them burns budget and cannot succeed.
func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || (status >= 500 && status <= 599)
}

func isRetryableError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A per-attempt context timeout is retryable while the parent is alive;
	// the caller checks the parent before scheduling the next attempt.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return retryableStatus(he.Status)
	}
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return true // transport errors are retryable
}

func (c *Client) backoff(ctx context.Context, attempt int) error {
	d := c.cfg.MinBackoff << attempt
	if d > c.cfg.MaxBackoff || d <= 0 {
		d = c.cfg.MaxBackoff
	}
	// Full jitter: [d/2, d).
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

// estimateTokens is a conservative byte/4 estimate used for budgets and
// cost caps before a call. It is not a tokenizer.
func estimateTokens(b []byte) int64 {
	n := int64(len(b)/4 + 1)
	if n < 1 {
		return 1
	}
	return n
}

// EstimateRequestTokens returns the conservative input-token estimate for a
// request, used by the eval cost cap before a live run.
func EstimateRequestTokens(req Request) int64 {
	qs := make(map[string]wireQuestion, len(req.Questions))
	for k, q := range req.Questions {
		qs[k] = q.wire()
	}
	b, err := json.Marshal(wireRequest{State: req.State, Questions: qs, SessionID: req.SessionID, User: req.User})
	if err != nil {
		return 1
	}
	return estimateTokens(b)
}
