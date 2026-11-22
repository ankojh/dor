package config

import (
	"fmt"
	"time"
)

// TokenBucket is one API's short-lived token budget.
type TokenBucket struct {
	Capacity        float64 `yaml:"capacity"`
	RefillPerSecond float64 `yaml:"refill_per_second"`
}

// Budget is config/budget.yaml.
type Budget struct {
	Version        int     `yaml:"version"`
	DailyUSD       float64 `yaml:"daily_usd"`
	EvalUSD        float64 `yaml:"eval_usd"`
	ShedBriefAt    float64 `yaml:"shed_brief_at"`
	ShedWhyAt      float64 `yaml:"shed_why_at"`
	ShedThematicAt float64 `yaml:"shed_thematic_at"`
	// PendingTTL garbage-collects outstanding reservations older than this.
	PendingTTL   Duration               `yaml:"pending_ttl"`
	TokenBuckets map[string]TokenBucket `yaml:"token_buckets"`
}

// DefaultBudget returns conservative local defaults.
func DefaultBudget() Budget {
	return Budget{
		Version:        1,
		DailyUSD:       5.00,
		EvalUSD:        2.00,
		ShedBriefAt:    0.80,
		ShedWhyAt:      0.90,
		ShedThematicAt: 0.95,
		PendingTTL:     Duration(15 * time.Minute),
		TokenBuckets: map[string]TokenBucket{
			"jev":          {Capacity: 200_000, RefillPerSecond: 4_000},
			"claude-haiku": {Capacity: 400_000, RefillPerSecond: 8_000},
			"claude-opus":  {Capacity: 200_000, RefillPerSecond: 2_000},
			"claude-cli":   {Capacity: 400_000, RefillPerSecond: 8_000},
			"codex-cli":    {Capacity: 400_000, RefillPerSecond: 8_000},
		},
	}
}

// LoadBudget reads config/budget.yaml, filling omitted fields with defaults.
func LoadBudget(path string) (Budget, error) {
	b := DefaultBudget()
	if err := readYAML(path, &b); err != nil {
		return Budget{}, err
	}
	if err := b.Validate(); err != nil {
		return Budget{}, fmt.Errorf("config: %s: %w", path, err)
	}
	return b, nil
}

// Validate checks the budget config.
func (b *Budget) Validate() error {
	if b.DailyUSD <= 0 {
		return fmt.Errorf("daily_usd must be positive (a zero cap disables all model calls)")
	}
	if b.EvalUSD < 0 {
		return fmt.Errorf("eval_usd must not be negative")
	}
	if b.ShedBriefAt <= 0 || b.ShedBriefAt > 1 {
		return fmt.Errorf("shed_brief_at = %v, want (0,1]", b.ShedBriefAt)
	}
	if b.ShedWhyAt < b.ShedBriefAt || b.ShedWhyAt > 1 {
		return fmt.Errorf("shed_why_at = %v, want [shed_brief_at,1]", b.ShedWhyAt)
	}
	if b.ShedThematicAt < b.ShedWhyAt || b.ShedThematicAt > 1 {
		return fmt.Errorf("shed_thematic_at = %v, want [shed_why_at,1]", b.ShedThematicAt)
	}
	if b.PendingTTL.Std() <= 0 {
		return fmt.Errorf("pending_ttl must be positive")
	}
	for api, bucket := range b.TokenBuckets {
		if bucket.Capacity <= 0 || bucket.RefillPerSecond <= 0 {
			return fmt.Errorf("token bucket %q must have positive capacity and refill", api)
		}
	}
	return nil
}
