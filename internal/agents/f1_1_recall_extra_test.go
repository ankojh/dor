package agents

import (
	"context"
	"testing"

	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// F1·1 handoff gap: these four populous places are absent from the committed
// recall fixture and must still resolve through the production plotter.
func TestF11ExtraLargePlaceRecall(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	for _, tc := range []struct {
		name, id, title string
	}{
		{"soweto", "953781", "Soweto announces a new policy"},
		{"vijayawada", "1253184", "Vijayawada announces a new policy"},
		{"bogor", "1648473", "Bogor announces a new policy"},
		{"semarang", "1627896", "Semarang announces a new policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := g.ByID(tc.id)
			if !ok || !geo.ValidCoordinates(want.Lat, want.Lon) {
				t.Fatalf("full cache is missing required place %s", tc.id)
			}
			placed := p.place(context.Background(), &dorv1.Article{Id: "f11-extra-" + tc.name, Title: tc.title})
			for _, loc := range placed.Locations {
				if loc.PlaceId != tc.id {
					continue
				}
				if loc.Precision != dorv1.Precision_PRECISION_CITY {
					t.Fatalf("%q should resolve at city precision, got %+v", tc.title, loc)
				}
				t.Logf("resolved %s (%s) lat=%.5f lon=%.5f precision=%s role=%s", loc.Name, loc.PlaceId, loc.Lat, loc.Lon, loc.Precision, loc.Role)
				return
			}
			t.Fatalf("%q should plot %s (%s) at city precision, got %+v", tc.title, want.Name, tc.id, placed.Locations)
		})
	}
}
