// Package config loads Dor's YAML configuration: decision thresholds, model
// pins and budget limits.
//
// The files live in config/ at the repository root. Loaders validate eagerly
// and return the plan's defaults for fields a file omits, so a partial file is
// safe but a wrong value is not.
package config

import (
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v4"
)

// Duration is a time.Duration that unmarshals from YAML strings such as
// "30s" or "2m".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("config: invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// Std returns the standard library duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// readYAML reads and unmarshals one file, wrapping every error with the path.
func readYAML(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, out); err != nil {
		return fmt.Errorf("config: parse %s: %w", path, err)
	}
	return nil
}
