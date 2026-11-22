package agents

import (
	"context"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"testing"
)

func TestF11FinalReviewCityRecallAndAmbiguity(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	for _, tc := range []struct{ title, want string }{
		{"Paris prosecutor opens inquiry into fire", "Paris"},
		{"Berlin police arrest suspect", "Berlin"},
		{"Manila prepares for typhoon", "Manila"},
		{"Lagos increases HIV response budget", "Lagos"},
		{"Durban business confidence drops", "Durban"},
		{"Sheinbaum leaves Shenzhen after visit", "Shenzhen"},
		{"2026 Elections: Tainan candidate posts bail", "Tainan"},
		{"From Christchurch school to FBI's Most Wanted list", "Christchurch"},
		{"Former Oxford professor arrested", ""},
		{"Festival Recife opens this week", "Recife"},
		{"Reading list released", ""}, {"Nice weather expected", ""},
		{"Mobile phone sales increase", ""}, {"Paris Hilton attends gala", ""},
		{"Great Salt Lake shrinks to record low", ""},
		{"Ho Chi Minh's legacy debated", ""},
		{"Kingston upon Thames council approves plan", ""},
	} {
		t.Run(tc.title, func(t *testing.T) {
			got := p.place(context.Background(), &dorv1.Article{Title: tc.title})
			if tc.want == "" {
				if len(got.Locations) != 0 {
					t.Fatalf("unexpected locations: %+v", got.Locations)
				}
			} else if len(got.Locations) == 0 || got.Locations[0].Name != tc.want {
				t.Fatalf("want %s: %+v", tc.want, got.Locations)
			}
		})
	}
}
