package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Price is a provisional per-million-token price used for estimates and for
// reporting Claude cost (the API returns tokens, not money). jev's own cost is
// authoritative and comes from usage.cost in the response.
type Price struct {
	InputPerMTok  float64 `yaml:"input_per_mtok"`
	OutputPerMTok float64 `yaml:"output_per_mtok"`
	// CacheReadPerMTok and CacheWritePerMTok are optional. When zero, the
	// documented Anthropic ratios apply: reads at 0.1x input, writes at 1.25x.
	CacheReadPerMTok  float64 `yaml:"cache_read_per_mtok"`
	CacheWritePerMTok float64 `yaml:"cache_write_per_mtok"`
	Provisional       bool    `yaml:"provisional"`
}

// Cost returns the estimated USD cost for one call with no cache breakdown.
func (p Price) Cost(inputTokens, outputTokens int64) float64 {
	return float64(inputTokens)/1e6*p.InputPerMTok + float64(outputTokens)/1e6*p.OutputPerMTok
}

// CostUsage prices a Claude usage record, including cached input. The cache
// rates default to the documented 0.1x (read) and 1.25x (write) multiples of
// the input rate. The result is an estimate; the API returns tokens, not money.
func (p Price) CostUsage(input, output, cacheRead, cacheWrite int64) float64 {
	readRate := p.CacheReadPerMTok
	if readRate == 0 {
		readRate = p.InputPerMTok * 0.10
	}
	writeRate := p.CacheWritePerMTok
	if writeRate == 0 {
		writeRate = p.InputPerMTok * 1.25
	}
	return float64(input)/1e6*p.InputPerMTok +
		float64(output)/1e6*p.OutputPerMTok +
		float64(cacheRead)/1e6*readRate +
		float64(cacheWrite)/1e6*writeRate
}

// JevModel is the jev endpoint and pin.
type JevModel struct {
	Model      string   `yaml:"model"`
	BaseURL    string   `yaml:"base_url"`
	Path       string   `yaml:"path"`
	Timeout    Duration `yaml:"timeout"`
	MaxRetries int      `yaml:"max_retries"`
	MinBackoff Duration `yaml:"min_backoff"`
	MaxBackoff Duration `yaml:"max_backoff"`
	Price      Price    `yaml:"price"`
}

// ClaudeModel is the Claude client configuration. Both model ids are plain
// strings so a new release needs no SDK upgrade.
type ClaudeModel struct {
	HaikuModel string           `yaml:"haiku_model"`
	OpusModel  string           `yaml:"opus_model"`
	Effort     string           `yaml:"effort"`
	Timeout    Duration         `yaml:"timeout"`
	MaxRetries int              `yaml:"max_retries"`
	MinBackoff Duration         `yaml:"min_backoff"`
	MaxBackoff Duration         `yaml:"max_backoff"`
	Price      map[string]Price `yaml:"price"`
}

// Models is config/models.yaml.
type Models struct {
	Version int         `yaml:"version"`
	Jev     JevModel    `yaml:"jev"`
	Claude  ClaudeModel `yaml:"claude"`
}

// DefaultModels returns the pinned defaults. It never returns the floating
// ~typesafe/jev-latest alias.
func DefaultModels() Models {
	return Models{
		Version: 1,
		Jev: JevModel{
			Model:      "typesafe/jev-1.14",
			BaseURL:    "https://openrouter.ai",
			Path:       "/api/alpha/decisions",
			Timeout:    Duration(30 * time.Second),
			MaxRetries: 4,
			MinBackoff: Duration(250 * time.Millisecond),
			MaxBackoff: Duration(8 * time.Second),
			Price:      Price{InputPerMTok: 0.10, Provisional: true},
		},
		Claude: ClaudeModel{
			HaikuModel: "claude-haiku-5-5",
			OpusModel:  "claude-opus-5-5",
			Effort:     "low",
			Timeout:    Duration(120 * time.Second),
			MaxRetries: 4,
			MinBackoff: Duration(500 * time.Millisecond),
			MaxBackoff: Duration(15 * time.Second),
			Price: map[string]Price{
				"claude-haiku-5-5": {InputPerMTok: 1.00, OutputPerMTok: 5.00, Provisional: true},
				"claude-opus-5-5":  {InputPerMTok: 15.00, OutputPerMTok: 75.00, Provisional: true},
			},
		},
	}
}

// LoadModels reads config/models.yaml, filling omitted fields with defaults.
func LoadModels(path string) (Models, error) {
	m := DefaultModels()
	if err := readYAML(path, &m); err != nil {
		return Models{}, err
	}
	if err := m.Validate(); err != nil {
		return Models{}, fmt.Errorf("config: %s: %w", path, err)
	}
	return m, nil
}

var pinnedVersion = regexp.MustCompile(`-\d+\.\d+$`)

// ValidateModelPin rejects an empty model, the floating "latest" alias and
// any model id without an explicit version suffix. A dated served snapshot is
// recorded per response; the request always names a pinned release.
func ValidateModelPin(model string) error {
	if strings.TrimSpace(model) == "" {
		return fmt.Errorf("model is empty")
	}
	if strings.Contains(model, "~") || strings.Contains(strings.ToLower(model), "latest") {
		return fmt.Errorf("model %q is a floating alias; pin the release (e.g. typesafe/jev-1.14)", model)
	}
	if !pinnedVersion.MatchString(model) {
		return fmt.Errorf("model %q has no version suffix (want e.g. -1.14)", model)
	}
	return nil
}

// Validate checks the model config.
func (m *Models) Validate() error {
	if err := ValidateModelPin(m.Jev.Model); err != nil {
		return fmt.Errorf("jev.model: %w", err)
	}
	if m.Jev.BaseURL == "" {
		return fmt.Errorf("jev.base_url is empty")
	}
	if m.Jev.Path == "" {
		return fmt.Errorf("jev.path is empty")
	}
	if m.Jev.Timeout <= 0 {
		return fmt.Errorf("jev.timeout must be positive")
	}
	if m.Jev.MaxRetries < 0 || m.Jev.MaxRetries > 10 {
		return fmt.Errorf("jev.max_retries = %d, want 0..10", m.Jev.MaxRetries)
	}
	if m.Jev.MinBackoff <= 0 || m.Jev.MaxBackoff < m.Jev.MinBackoff {
		return fmt.Errorf("jev backoff range is invalid")
	}
	if m.Claude.HaikuModel == "" || m.Claude.OpusModel == "" {
		return fmt.Errorf("claude model ids are empty")
	}
	switch m.Claude.Effort {
	case "low", "medium", "high", "xhigh", "max":
	default:
		return fmt.Errorf("claude.effort = %q, want low|medium|high|xhigh|max", m.Claude.Effort)
	}
	if m.Claude.Timeout <= 0 {
		return fmt.Errorf("claude.timeout must be positive")
	}
	if m.Claude.MaxRetries < 0 || m.Claude.MaxRetries > 10 {
		return fmt.Errorf("claude.max_retries = %d, want 0..10", m.Claude.MaxRetries)
	}
	if _, ok := m.Claude.Price[m.Claude.HaikuModel]; !ok {
		return fmt.Errorf("claude.price has no entry for %s", m.Claude.HaikuModel)
	}
	if _, ok := m.Claude.Price[m.Claude.OpusModel]; !ok {
		return fmt.Errorf("claude.price has no entry for %s", m.Claude.OpusModel)
	}
	return nil
}
