package geo

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
)

// The thirty headlines are copied verbatim from dor_f1_1 on 2026-11-21.
// Expectations were labelled from the headline itself: a known place at its
// supported precision, a country fallback, or no dot. No article/model keys.
func TestLiveHeadlineRegressions(t *testing.T) {
	dir := "testdata/live"
	if full := os.Getenv("DOR_TEST_GEONAMES_DIR"); full != "" {
		dir = full
	}
	g, err := Load(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/live/headlines.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Title   string   `json:"title"`
		Allowed []string `json:"allowed"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 30 {
		t.Fatalf("got %d headlines, want 30", len(fixtures))
	}
	for _, f := range fixtures {
		t.Run(f.Title, func(t *testing.T) {
			candidates := g.Candidates("", f.Title, "", "", 1)
			if len(candidates) == 0 || candidates[0].Score < 10 {
				if len(f.Allowed) > 0 {
					t.Fatal("explicit place in headline should resolve")
				}
				return // Honest global/no-dot is acceptable for ambiguous titles.
			}
			p := candidates[0]
			for _, id := range f.Allowed {
				if p.ID == id {
					if !ValidCoordinates(p.Lat, p.Lon) {
						t.Fatalf("invalid point: %+v", p)
					}
					return
				}
			}
			t.Fatalf("unsupported headline location: %s (%s), score %.1f", p.Name, p.ID, p.Score)
		})
	}
}
