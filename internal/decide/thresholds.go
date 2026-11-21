package decide

import "github.com/ankojh/dor/config"

// LoadThresholds loads config/thresholds.yaml for callers that build decision
// results. It is a thin wrapper over the config loader so the decision package
// owns the threshold vocabulary it applies.
func LoadThresholds(path string) (config.Thresholds, error) {
	return config.LoadThresholds(path)
}

// DefaultThresholds returns the plan's provisional starting values.
func DefaultThresholds() config.Thresholds { return config.DefaultThresholds() }
