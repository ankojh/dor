// Package decide is Dor's jev client and the question builders for the four
// decision jobs: classify, plot, relate and match.
//
// jev answers typed questions (noul, choice, score) with probabilities. It
// never sees question keys and answers every question independently, so all
// meaning lives in Instructions and Criteria and combinations happen here, in
// code:
//
//   - a relation's type only counts when the pair is related;
//   - strength = score/3 × P(related), nudged by entity evidence;
//   - strength tiers are 0.3 / 0.5 / 0.75;
//   - precision and parties only count when the primary place is accepted.
//
// A refusal, a missing key, a missing answer or an unknown answer type is "not
// answered" and fails closed. It is never read as a negative.
//
// The model is pinned (config/models.yaml, default typesafe/jev-1.14). The
// floating ~typesafe/jev-latest alias is rejected: thresholds are
// model-specific and must be re-tuned per pinned release.
package decide

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ankojh/dor/internal/budget"
)

// QuestionType is one of the documented jev question kinds. `box` is not used
// by Dor.
type QuestionType string

const (
	// TypeNoul asks for P(true).
	TypeNoul QuestionType = "noul"
	// TypeChoice asks for one option key.
	TypeChoice QuestionType = "choice"
	// TypeScore asks for an expected score on ordered levels.
	TypeScore QuestionType = "score"
)

// Question is one typed jev question.
type Question struct {
	Type         QuestionType
	Instructions string
	// Criteria is the "true"/"false" definition map for noul questions, and
	// option key → definition for choice questions.
	Criteria map[string]string
	// Levels are the ordered score levels, lowest first.
	Levels []string
}

// Request is one decisions call. Model, endpoint and auth come from the
// client; a Request only carries content.
type Request struct {
	State     any
	Questions map[string]Question
	SessionID string
	User      string
	// Priority labels the budget check for this request. Zero keeps the
	// client's configured default. The relation judge sets thematic so the
	// documented shed order can stop it without touching plotting or the wire.
	Priority budget.Priority
}

// Answer is one answer from a response, normalized across the answer union.
type Answer struct {
	// Type is the type the model returned. It may differ from the question's
	// type; callers must check AnsweredAs.
	Type QuestionType
	// Present is true when the key existed in the response.
	Present bool
	// Refusal is true for the refusal arm. It is not a negative answer.
	Refusal bool

	Noul          float64
	HasNoul       bool
	Choice        string
	Score         float64
	HasScore      bool
	Confidence    float64
	HasConfidence bool
	Probabilities map[string]float64
	Legend        map[string]string
}

// Answered reports whether the answer is usable at all.
func (a Answer) Answered() bool {
	if !a.Present || a.Refusal {
		return false
	}
	switch a.Type {
	case TypeNoul:
		return a.HasNoul
	case TypeChoice:
		return a.Choice != ""
	case TypeScore:
		return a.HasScore
	default:
		return false
	}
}

// AnsweredAs reports whether the answer is usable and of the expected type.
func (a Answer) AnsweredAs(t QuestionType) bool {
	return a.Answered() && a.Type == t
}

// Concentration returns jev's confidence field. It is a normalized
// concentration, NOT the probability of the selected option, and must never be
// used as a gate. See https://docs.typesafe.ai/confidence.
func (a Answer) Concentration() float64 {
	if a.HasConfidence {
		return a.Confidence
	}
	return 0
}

// SelectedProbability returns the probability of the option the model chose:
// probabilities[choice] for a choice, noul for a noul, and the level
// probability for a score. A choice without a probability map has no usable
// probability and returns ok=false; callers fail closed.
func (a Answer) SelectedProbability() (float64, bool) {
	switch a.Type {
	case TypeNoul:
		if !a.HasNoul {
			return 0, false
		}
		return a.Noul, true
	case TypeChoice:
		if a.Probabilities == nil || a.Choice == "" {
			return 0, false
		}
		p, ok := a.Probabilities[a.Choice]
		return p, ok
	case TypeScore:
		if !a.HasScore {
			return 0, false
		}
		if a.Probabilities == nil {
			return 0, false
		}
		p, ok := a.Probabilities[fmt.Sprintf("%d", int(a.Score+0.5))]
		return p, ok
	default:
		return 0, false
	}
}

// Probability returns the model's probability for an option key (choice) or
// level index (score). A missing probability is zero.
func (a Answer) Probability(key string) float64 {
	return a.Probabilities[key]
}

// Usage is what one call cost.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	CachedTokens int64
	// CostUSD is usage.cost from the API (authoritative for jev).
	CostUSD float64
}

// Response is one decisions response plus the local timing.
type Response struct {
	ID       string
	Provider string
	// Model is the served dated snapshot, e.g. typesafe/jev-1.14-20261014.
	Model   string
	Answers map[string]Answer
	Usage   Usage
	// Latency is total elapsed time for the logical call, including retries
	// and backoff. LastAttemptLatency is the final HTTP attempt only.
	Latency            time.Duration
	LastAttemptLatency time.Duration
	Attempts           int
}

// Answer returns one answer and whether it is usable as the expected type.
func (r Response) Answer(key string, want QuestionType) (Answer, bool) {
	a, ok := r.Answers[key]
	if !ok || !a.AnsweredAs(want) {
		return a, false
	}
	return a, true
}

// Decider is the interface agents and eval use. *JevClient and the mock
// backends implement it.
type Decider interface {
	Decide(ctx context.Context, req Request) (Response, error)
}

// Errors callers branch on.
var (
	// ErrNoAPIKey means the client has no key. Callers fail closed.
	ErrNoAPIKey = errors.New("decide: no jev API key")
	// ErrBudget means the budget hook refused the call.
	ErrBudget = errors.New("decide: budget denied")
	// ErrRefusal is returned by helpers that require a usable answer when the
	// model refused.
	ErrRefusal = errors.New("decide: model refused")
)

// Unavailable is a Decider that fails every call with a fixed reason. Use it
// when a key is missing so callers get "not answered" without a network call.
type Unavailable struct {
	Reason error
}

// Decide implements Decider.
func (u Unavailable) Decide(context.Context, Request) (Response, error) {
	if u.Reason == nil {
		return Response{}, ErrNoAPIKey
	}
	return Response{}, u.Reason
}

// BudgetError is returned when the limiter denies a call.
type BudgetError struct {
	Reason string
	API    string
}

// Error implements error.
func (e *BudgetError) Error() string {
	return fmt.Sprintf("decide: budget denied for %s: %s", e.API, e.Reason)
}

// Unwrap makes errors.Is(err, ErrBudget) true.
func (e *BudgetError) Unwrap() error { return ErrBudget }

// HTTPError is a non-2xx jev response.
type HTTPError struct {
	Status  int
	Code    int
	Message string
	Body    string
}

// Error implements error.
func (e *HTTPError) Error() string {
	if e.Code != 0 || e.Message != "" {
		return fmt.Sprintf("decide: jev HTTP %d: %d %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("decide: jev HTTP %d", e.Status)
}
