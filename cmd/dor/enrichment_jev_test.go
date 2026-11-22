package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	dorconfig "github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
)

func jevTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func jevTestBudget() *budget.Manager {
	return budget.NewMemory(dorconfig.DefaultBudget())
}

func TestNewJevClientRefusesTypeSafeKeyOnOpenRouter(t *testing.T) {
	clearEnv(t, "DOR_JEV_API_KEY", "OPENROUTER_API_KEY", "TYPESAFE_API_KEY",
		"TYPESAFE_BASE_URL", "DOR_JEV_BASE_URL")
	const key = "ts-secret-key"
	t.Setenv("TYPESAFE_API_KEY", key)
	t.Setenv("TYPESAFE_BASE_URL", "https://openrouter.ai")
	t.Setenv("DOR_JEV_BASE_URL", "https://openrouter.ai")

	client, model, err := newJevClient(dorconfig.DefaultModels(), jevTestBudget(), jevTestLogger())
	if err == nil {
		t.Fatal("newJevClient accepted a TypeSafe key pointed at OpenRouter")
	}
	if client != nil {
		t.Fatal("newJevClient returned a client after refusing the endpoint")
	}
	if model != "" {
		t.Fatalf("model = %q, want empty on a refusal", model)
	}
	if strings.Contains(err.Error(), key) {
		t.Fatalf("error leaked the API key: %v", err)
	}
}

func TestNewJevClientTypeSafeUsesTypeSafeEndpointAndModel(t *testing.T) {
	clearEnv(t, "DOR_JEV_API_KEY", "OPENROUTER_API_KEY", "TYPESAFE_API_KEY",
		"TYPESAFE_BASE_URL", "DOR_JEV_BASE_URL")
	t.Setenv("TYPESAFE_API_KEY", "ts-secret-key")

	models := dorconfig.DefaultModels()
	client, model, err := newJevClient(models, jevTestBudget(), jevTestLogger())
	if err != nil {
		t.Fatalf("newJevClient: %v", err)
	}
	if client == nil {
		t.Fatal("newJevClient returned no client for a TypeSafe key")
	}
	if model != models.Jev.TypeSafe.Model {
		t.Fatalf("model = %q, want TypeSafe model %q", model, models.Jev.TypeSafe.Model)
	}
}

func TestNewJevClientIgnoresLegacyBaseURLForTypeSafeKey(t *testing.T) {
	clearEnv(t, "DOR_JEV_API_KEY", "OPENROUTER_API_KEY", "TYPESAFE_API_KEY",
		"TYPESAFE_BASE_URL", "DOR_JEV_BASE_URL")
	t.Setenv("TYPESAFE_API_KEY", "ts-secret-key")
	// A TypeSafe key must never follow the OpenRouter base URL. Pointing the
	// legacy variable at OpenRouter must not make the client refuse or, worse,
	// route the key there.
	t.Setenv("DOR_JEV_BASE_URL", "https://openrouter.ai")

	models := dorconfig.DefaultModels()
	client, model, err := newJevClient(models, jevTestBudget(), jevTestLogger())
	if err != nil {
		t.Fatalf("newJevClient followed DOR_JEV_BASE_URL for a TypeSafe key: %v", err)
	}
	if client == nil {
		t.Fatal("newJevClient returned no client")
	}
	if model != models.Jev.TypeSafe.Model {
		t.Fatalf("model = %q, want TypeSafe model %q", model, models.Jev.TypeSafe.Model)
	}
}

func TestNewJevClientOpenRouterKeyUsesJevModel(t *testing.T) {
	clearEnv(t, "DOR_JEV_API_KEY", "OPENROUTER_API_KEY", "TYPESAFE_API_KEY",
		"TYPESAFE_BASE_URL", "DOR_JEV_BASE_URL")
	t.Setenv("DOR_JEV_API_KEY", "or-test-key")
	t.Setenv("DOR_JEV_BASE_URL", "http://127.0.0.1:9")

	models := dorconfig.DefaultModels()
	client, model, err := newJevClient(models, jevTestBudget(), jevTestLogger())
	if err != nil {
		t.Fatalf("newJevClient: %v", err)
	}
	if client == nil {
		t.Fatal("newJevClient returned no client for an OpenRouter key")
	}
	if model != models.Jev.Model {
		t.Fatalf("model = %q, want jev model %q", model, models.Jev.Model)
	}
}
