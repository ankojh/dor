package geo

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// A same-named ADM1 is only the region's own seat when the city really is that
// region's capital. Washington, D.C. is the US national capital but not
// Washington State's seat; Taipei and La Paz are their own regions' seats.
func TestSameSeatRegionOwnSeatVersusSameNamedRegion(t *testing.T) {
	dir := os.Getenv("DOR_TEST_GEONAMES_DIR")
	if dir == "" {
		t.Skip("set DOR_TEST_GEONAMES_DIR to run against the full cached GeoNames dataset")
	}
	g, err := LoadExisting(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load full cached GeoNames: %v", err)
	}
	for _, id := range []string{"4140963", "admin:US.WA", "1668341", "admin:TW.03", "3911925", "admin:BO.04"} {
		p, ok := g.ByID(id)
		if !ok || !ValidCoordinates(p.Lat, p.Lon) {
			t.Fatalf("full cache must contain a valid row %s: %+v found=%v", id, p, ok)
		}
	}
	dc, _ := g.ByID("4140963")
	washingtonState, _ := g.ByID("admin:US.WA")
	if g.sameSeatRegion(dc, washingtonState) {
		t.Fatalf("Washington, D.C. must not be treated as Washington State's own seat: dc=%+v state=%+v", dc, washingtonState)
	}
	if g.sameSeatRegion(washingtonState, dc) {
		t.Fatalf("sameSeatRegion must not be symmetric for a non-seat region: state=%+v dc=%+v", washingtonState, dc)
	}
	taipei, _ := g.ByID("1668341")
	taipeiRegion, _ := g.ByID("admin:TW.03")
	if !g.sameSeatRegion(taipei, taipeiRegion) {
		t.Fatalf("Taipei city must be treated as Taipei region's own seat: city=%+v region=%+v", taipei, taipeiRegion)
	}
	laPaz, _ := g.ByID("3911925")
	laPazRegion, _ := g.ByID("admin:BO.04")
	if !g.sameSeatRegion(laPaz, laPazRegion) {
		t.Fatalf("La Paz city must be treated as La Paz Department's own seat: city=%+v region=%+v", laPaz, laPazRegion)
	}
}
