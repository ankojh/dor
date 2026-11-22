package eval

import (
	"math"
	"testing"

	"github.com/ankojh/dor/config"
)

// TestEstimateRunUsesSelectedJevPrice proves the preflight estimate prices the
// dataset with the jev host that will actually run live. jev output tokens are
// free on both hosts, so the OpenRouter/TypeSafe ratio is exactly the input
// price ratio.
func TestEstimateRunUsesSelectedJevPrice(t *testing.T) {
	ds, err := LoadDataset("data")
	if err != nil {
		t.Fatal(err)
	}
	models := config.DefaultModels()

	openrouter, claudeOpenrouter, openrouterCalls := EstimateRun(ds, models, models.Jev.Price)
	typesafe, claudeTypesafe, typesafeCalls := EstimateRun(ds, models, models.Jev.TypeSafe.Price)

	if openrouter <= 0 || typesafe <= 0 {
		t.Fatalf("jev estimates = openrouter %v, typesafe %v; want both positive", openrouter, typesafe)
	}
	if claudeOpenrouter != claudeTypesafe || openrouterCalls != typesafeCalls {
		t.Errorf("Claude estimate/calls changed with the jev price: claude %v/%v, calls %d/%d",
			claudeOpenrouter, claudeTypesafe, openrouterCalls, typesafeCalls)
	}
	if typesafe >= openrouter {
		t.Errorf("typesafe estimate %v must be below the OpenRouter estimate %v", typesafe, openrouter)
	}

	wantRatio := models.Jev.Price.InputPerMTok / models.Jev.TypeSafe.Price.InputPerMTok
	gotRatio := openrouter / typesafe
	if math.Abs(gotRatio-wantRatio) > 1e-9*wantRatio {
		t.Errorf("estimate ratio = %v, want %v (0.10/0.042)", gotRatio, wantRatio)
	}

	// A mock jev backend passes the zero price: no jev spend is estimated, but
	// the Claude side is unchanged.
	mockJev, mockClaude, mockCalls := EstimateRun(ds, models, config.Price{})
	if mockJev != 0 {
		t.Errorf("zero jev price estimate = %v, want 0", mockJev)
	}
	if mockClaude != claudeOpenrouter || mockCalls != openrouterCalls {
		t.Errorf("mock jev estimate changed Claude/calls: %v/%d, want %v/%d",
			mockClaude, mockCalls, claudeOpenrouter, openrouterCalls)
	}
}
