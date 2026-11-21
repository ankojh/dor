package geo

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeGazetteer is a tiny, offline GeoNames fixture. Rows use the same
// tab-separated layout as cities15000.txt, countryInfo.txt and admin1Codes.
func writeGazetteer(t *testing.T, countries, admins, places string) *Gazetteer {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range map[string]string{
		"cities15000.txt":      places,
		"countryInfo.txt":      countries,
		"admin1CodesASCII.txt": admins,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	g, err := Load(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load fixture gazetteer: %v", err)
	}
	return g
}

func countryRow(code, iso3, name, capital string) string {
	return strings.Join([]string{code, iso3, "", "", name, capital}, "\t") + "\n"
}

func cityRow(id, name, aliases, feature, lat, lon, cc, admin, population string) string {
	fields := make([]string, 19)
	fields[0], fields[1], fields[3] = id, name, aliases
	fields[4], fields[5], fields[7] = lat, lon, feature
	fields[8], fields[10], fields[14] = cc, admin, population
	return strings.Join(fields, "\t") + "\n"
}

func fixtureGazetteer(t *testing.T) *Gazetteer {
	countries := countryRow("US", "USA", "United States", "Washington") +
		countryRow("FR", "FRA", "France", "Paris") +
		countryRow("CN", "CHN", "China", "Beijing") +
		countryRow("CH", "CHE", "Switzerland", "Bern")
	admins := "US.TX\tTexas\nUS.PA\tPennsylvania\nFR.11\tIle-de-France\nCH.GE\tGeneva\n"
	places := cityRow("100", "Paris", "", "PPLC", "48.8566", "2.3522", "FR", "11", "2161000") +
		cityRow("101", "Paris", "", "PPL", "33.6609", "-95.5555", "US", "TX", "25000") +
		cityRow("102", "Dallas", "", "PPLA", "32.7767", "-96.7970", "US", "TX", "1300000") +
		cityRow("103", "Philadelphia", "", "PPLA", "39.9526", "-75.1652", "US", "PA", "1600000") +
		cityRow("104", "Saint-Maximin-la-Sainte-Baume", "Marathon", "PPL", "43.4520", "5.8620", "FR", "93", "16000") +
		cityRow("105", "Frome", "", "PPL", "51.2300", "-2.3200", "GB", "ENG", "28000") +
		cityRow("106", "Nis", "Niš", "PPL", "43.3209", "21.8958", "RS", "SE", "260000") +
		cityRow("107", "Uniao dos Palmares", "União dos Palmares,Palmares", "PPL", "-9.1590", "-36.0220", "BR", "AL", "65000") +
		cityRow("108", "Biga", "", "PPL", "40.2281", "27.2422", "TR", "17", "90000") +
		cityRow("109", "Saint Paul", "Paul,Saint", "PPL", "44.9537", "-93.0900", "US", "MN", "300000") +
		cityRow("110", "Geneva", "", "PPLA", "46.2044", "6.1432", "CH", "GE", "200000") +
		cityRow("111", "Washington", "", "PPLC", "38.9072", "-77.0369", "US", "DC", "700000") +
		cityRow("112", "Texas", "", "ADM1", "31.0000", "-100.0000", "US", "TX", "") +
		cityRow("113", "Ile-de-France", "", "ADM1", "48.5000", "2.5000", "FR", "11", "")
	return writeGazetteer(t, countries, admins, places)
}

func TestValidCoordinatesAndPrecisionOf(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lat, lon float64
		want     bool
	}{
		{"zero zero sentinel", 0, 0, false}, {"equator", 0, 10, true}, {"prime meridian", 10, 0, true},
		{"nan latitude", math.NaN(), 1, false}, {"infinite longitude", 1, math.Inf(1), false},
		{"latitude too large", 90.01, 0, false}, {"longitude too small", 0, -180.01, false},
		{"bounds", 90, 180, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ValidCoordinates(tc.lat, tc.lon); got != tc.want {
				t.Fatalf("ValidCoordinates(%v, %v) = %v, want %v", tc.lat, tc.lon, got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		place Place
		want  Resolution
	}{
		{Place{CountryID: true, Feature: "PPLC"}, CountryResolution},
		{Place{Feature: "ADM1"}, RegionResolution},
		{Place{Feature: "PPL"}, CityResolution},
		{Place{Feature: "street"}, StreetResolution},
		{Place{Feature: "museum"}, PointResolution},
		{Place{Feature: "unknown-photon-type"}, CityResolution},
	} {
		if got := PrecisionOf(tc.place); got != tc.want {
			t.Errorf("PrecisionOf(%+v) = %v, want %v", tc.place, got, tc.want)
		}
	}
}

func TestGazetteerRegionCentresAndCanonicalLookup(t *testing.T) {
	g := fixtureGazetteer(t)
	if p, ok := g.ByID("112"); !ok || !ValidCoordinates(p.Lat, p.Lon) || PrecisionOf(p) != RegionResolution {
		t.Fatalf("ByID admin-1 did not retain valid region coordinate: %+v, %v", p, ok)
	}
	if tx, ok := g.Lookup("Texas", "US"); !ok || tx.Feature != "ADM1" || tx.Lat != 31 {
		t.Fatalf("Texas should resolve to its ADM1 coordinate: %+v, %v", tx, ok)
	}
	if p, ok := g.Lookup("Ile-de-France", "FR"); !ok || p.Feature != "ADM1" {
		t.Fatalf("Ile-de-France should resolve to ADM1: %+v, %v", p, ok)
	}
	if p, ok := g.ByID("101"); !ok || p.Name != "Paris" || p.CountryCode != "US" {
		t.Fatalf("ByID canonical lookup failed: %+v, %v", p, ok)
	}
	if _, ok := g.ByID("missing"); ok {
		t.Fatal("ByID found unknown id")
	}
}

func TestRegionWithoutPlaceDataDowngradesToCountry(t *testing.T) {
	countries := countryRow("US", "USA", "United States", "Washington")
	admins := "US.WY\tWyoming\n"
	places := cityRow("200", "Washington", "", "PPLC", "38.9", "-77.0", "US", "DC", "700000")
	g := writeGazetteer(t, countries, admins, places)
	p, ok := g.Lookup("Wyoming", "US")
	if !ok || !p.CountryID || p.ID != "country:US" || PrecisionOf(p) != CountryResolution {
		t.Fatalf("region without coordinates should downgrade to country: %+v, %v", p, ok)
	}
}

func TestRegionWithoutADM1UsesAliasDeduplicatedPopulationCentre(t *testing.T) {
	countries := countryRow("US", "USA", "United States", "Washington")
	admins := "US.TN\tTennessee\n"
	places := cityRow("301", "Smalltown", "Small Town", "PPL", "20", "20", "US", "TN", "100") +
		cityRow("302", "Bigcity", "Big City", "PPL", "40", "40", "US", "TN", "300")
	g := writeGazetteer(t, countries, admins, places)
	got, ok := g.Lookup("Tennessee", "US")
	if !ok || got.Feature != "ADM1" || !ValidCoordinates(got.Lat, got.Lon) {
		t.Fatalf("region should fall back to a population centre: %+v, %v", got, ok)
	}
	wantLat, wantLon, ok := populationCentre([]Place{
		{ID: "301", CountryCode: "US", AdminCode: "TN", Feature: "PPL", Lat: 20, Lon: 20, Population: 100},
		{ID: "301", CountryCode: "US", AdminCode: "TN", Feature: "PPL", Lat: 20, Lon: 20, Population: 100}, // alternate name
		{ID: "302", CountryCode: "US", AdminCode: "TN", Feature: "PPL", Lat: 40, Lon: 40, Population: 300},
	}, "US", "TN")
	if !ok || math.Abs(got.Lat-wantLat) > 1e-8 || math.Abs(got.Lon-wantLon) > 1e-8 {
		t.Errorf("region centre = (%f,%f), want alias-deduplicated weighted centre (%f,%f)", got.Lat, got.Lon, wantLat, wantLon)
	}
}

func TestPopulationCentreUsesUniqueCitiesAcrossDateline(t *testing.T) {
	places := []Place{
		{ID: "west", CountryCode: "XX", Feature: "PPL", Lat: 10, Lon: 179, Population: 100},
		{ID: "west", CountryCode: "XX", Feature: "PPL", Lat: 10, Lon: 179, Population: 100}, // alias duplicate
		{ID: "east", CountryCode: "XX", Feature: "PPL", Lat: 10, Lon: -179, Population: 100},
	}
	lat, lon, ok := populationCentre(places, "XX", "")
	if !ok || math.Abs(lat-10) > .01 || math.Abs(math.Abs(lon)-180) > .1 {
		t.Fatalf("dateline-safe, deduplicated centre = (%f,%f,%v)", lat, lon, ok)
	}
}

func TestAmbiguousParisAndPlaceLikeEvidence(t *testing.T) {
	g := fixtureGazetteer(t)
	for _, tc := range []struct {
		text, title, dateline, country string
		wantID                         string
	}{
		{"", "Paris reports new plan", "", "FR", "100"},
		{"", "Paris residents vote", "", "FR", "100"},
		{"", "Officials meet", "Paris, Texas", "US", "101"},
		{"", "In Paris, officials announce a plan", "Paris, France", "", "100"},
		{"", "New bridge opens", "Paris, France", "", "100"},
	} {
		cands := g.Candidates(tc.text, tc.title, tc.dateline, tc.country, 8)
		got := ""
		if len(cands) != 0 {
			got = cands[0].ID
		}
		if got != tc.wantID {
			t.Errorf("Candidates(%q,%q,%q,%q) first id = %q, want %q (all=%+v)", tc.text, tc.title, tc.dateline, tc.country, got, tc.wantID, cands)
		}
	}
	for _, tc := range []struct{ phrase, title string }{
		{"marathon", "Philly Marathon draws record crowd"},
		{"frome", "A minister calls for unity"},
		{"nis", "Officials praise the new plan"},
		{"palmares", "Union members celebrate victory"},
		{"biga", "A business group reports growth"},
		{"paul", "Paul calls for talks"},
		{"saint", "The saint was remembered at a service"},
	} {
		if got := g.Candidates("", tc.title, "", "", 8); len(got) != 0 {
			t.Errorf("stray %q in %q became candidate(s): %+v", tc.phrase, tc.title, got)
		}
	}
	for _, title := range []string{"Officials meet in Philadelphia", "New policy announced for Saint-Maximin-la-Sainte-Baume"} {
		if got := g.Candidates("", title, "", "", 8); len(got) == 0 {
			t.Errorf("genuine capitalized/locative title %q should match", title)
		}
	}
	if got := g.Candidates("", "Officials in União dos Palmares", "", "", 8); len(got) == 0 || got[0].ID != "107" {
		t.Errorf("genuine locative multiword place should match: %+v", got)
	}
}

func TestRepairLocationInvalidStrayPrecisionAndIdempotency(t *testing.T) {
	g := fixtureGazetteer(t)
	cases := []struct {
		name            string
		loc             StoredLocation
		evidence        []TextEvidence
		wantReason      string
		wantReplacement string
		wantPrecision   Resolution
	}{
		{"invalid coordinate replots from valid dateline", StoredLocation{PlaceID: "100", Lat: 0, Lon: 0, Precision: CityResolution}, []TextEvidence{{Title: "Officials meet in Paris", Dateline: "Paris, France"}}, "invalid_coordinate", "100", CityResolution},
		{"stray alias removes old hit", StoredLocation{PlaceID: "104", Lat: 43.452, Lon: 5.862, Precision: CityResolution}, []TextEvidence{{Title: "Philly Marathon draws record crowd"}}, "unsupported_match", "", 0},
		{"precision downgrade", StoredLocation{PlaceID: "100", Lat: 48.8566, Lon: 2.3522, Precision: PointResolution}, []TextEvidence{{Title: "Officials meet in Paris", Dateline: "Paris, France"}}, "precision", "100", CityResolution},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := g.RepairLocation(tc.loc, tc.evidence)
			if decision.Reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q: %+v", decision.Reason, tc.wantReason, decision)
			}
			gotID := ""
			if decision.Replacement != nil {
				gotID = decision.Replacement.ID
			}
			if gotID != tc.wantReplacement || decision.Precision != tc.wantPrecision {
				t.Fatalf("replacement/precision = %q/%v, want %q/%v", gotID, decision.Precision, tc.wantReplacement, tc.wantPrecision)
			}
			if decision.Replacement != nil {
				second := g.RepairLocation(StoredLocation{PlaceID: decision.Replacement.ID, Lat: decision.Replacement.Lat, Lon: decision.Replacement.Lon, Precision: decision.Precision}, tc.evidence)
				if second.Reason != "" {
					t.Errorf("second repair pass should be idempotent, got %+v", second)
				}
			}
		})
	}
}

func TestPhotonFiltersInvalidCoordinatesAndBuildsRequest(t *testing.T) {
	var got url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		fmt.Fprint(w, `{"features":[`+
			`{"geometry":{"coordinates":[0,0]},"properties":{"osm_type":"N","osm_id":1,"name":"Bad","type":"city","countrycode":"US"}},`+
			`{"geometry":{"coordinates":[-75.16,39.95]},"properties":{"osm_type":"W","osm_id":2,"name":"Broad Street","type":"street","countrycode":"US"}}]}`)
	}))
	defer server.Close()
	photon := NewPhoton(server.URL)
	places, err := photon.Geocode(context.Background(), "Broad Street, Philadelphia", 39.95, -75.16, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(places) != 1 || places[0].Name != "Broad Street" || places[0].Feature != "street" {
		t.Fatalf("invalid Photon coordinate was not filtered: %+v", places)
	}
	if got.Get("q") != "Broad Street, Philadelphia" || got.Get("limit") != "1" || got.Get("lat") == "" || got.Get("lon") == "" {
		t.Fatalf("Photon request query = %v", got)
	}
	if _, err := NewPhoton("").Geocode(context.Background(), "anything", 0, 0, 1); err == nil {
		t.Fatal("disabled Photon should report an error")
	}
}
