package main

import (
	"os"
	"testing"

	dorconfig "github.com/ankojh/dor/config"
)

// clearEnv makes the named variables genuinely absent for the test while
// still restoring whatever the developer's shell had afterwards.
func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func TestClaudeCLIModelsDefaultToHaikuForBothTiers(t *testing.T) {
	clearEnv(t, "DOR_CLAUDE_CLI_MODEL", "DOR_CLAUDE_CLI_OPUS_MODEL")
	models := dorconfig.DefaultModels()
	haiku, opus := claudeCLIModels(models)
	if haiku != models.Claude.HaikuModel || opus != models.Claude.HaikuModel {
		t.Fatalf("haiku=%q opus=%q, want both %q", haiku, opus, models.Claude.HaikuModel)
	}
}

func TestClaudeCLIModelsEnvOverrides(t *testing.T) {
	models := dorconfig.DefaultModels()

	t.Run("both tiers", func(t *testing.T) {
		clearEnv(t, "DOR_CLAUDE_CLI_MODEL", "DOR_CLAUDE_CLI_OPUS_MODEL")
		t.Setenv("DOR_CLAUDE_CLI_MODEL", "claude-cli-haiku-override")
		t.Setenv("DOR_CLAUDE_CLI_OPUS_MODEL", "claude-cli-opus-override")
		haiku, opus := claudeCLIModels(models)
		if haiku != "claude-cli-haiku-override" || opus != "claude-cli-opus-override" {
			t.Errorf("haiku=%q opus=%q", haiku, opus)
		}
	})

	t.Run("opus only", func(t *testing.T) {
		clearEnv(t, "DOR_CLAUDE_CLI_MODEL", "DOR_CLAUDE_CLI_OPUS_MODEL")
		t.Setenv("DOR_CLAUDE_CLI_OPUS_MODEL", "claude-cli-opus-override")
		haiku, opus := claudeCLIModels(models)
		if haiku != models.Claude.HaikuModel {
			t.Errorf("haiku = %q, want the default %q", haiku, models.Claude.HaikuModel)
		}
		if opus != "claude-cli-opus-override" {
			t.Errorf("opus = %q", opus)
		}
	})
}
