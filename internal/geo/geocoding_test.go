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
	// Wyoming is present only as a RegionAlias on the country fallback; it must
	// not manufacture a country-sized event location from capital-only data.
	candidates := g.Candidates("", "Officials gather in Wyoming", "", "", 10)
	for _, candidate := range candidates {
		if candidate.ID == "country:US" {
			t.Fatalf("unsupported RegionAlias must not auto-match through country coordinates: %+v", candidates)
		}
	}
	decision := g.RepairLocation(StoredLocation{PlaceID: "country:US", Lat: 38.9, Lon: -77, Precision: CountryResolution}, []TextEvidence{{Title: "Officials gather in Wyoming"}})
	if decision.Reason != "unsupported_match" || decision.Replacement != nil {
		t.Fatalf("unsupported downgraded RegionAlias row should be removed: %+v", decision)
	}
}

func TestCountryFallbackUsesCapitalAndDoesNotBorrowAnotherCity(t *testing.T) {
	countries := countryRow("US", "USA", "United States", "Washington") + countryRow("XX", "XXX", "Exampleland", "Missing Capital")
	admins := ""
	places := cityRow("401", "Washington", "", "PPLC", "38.9", "-77.0", "US", "DC", "700000") +
		cityRow("402", "Largeville", "", "PPL", "10", "20", "US", "CA", "9000000") +
		cityRow("403", "Other City", "", "PPL", "1", "2", "XX", "01", "50000")
	g := writeGazetteer(t, countries, admins, places)
	centroid, ok := g.CountryCentroid("US")
	if !ok || centroid.Lat != 38.9 || centroid.Lon != -77 || centroid.Name != "United States" {
		t.Fatalf("country fallback should use capital coordinates: %+v, %v", centroid, ok)
	}
	if _, ok := g.CountryCentroid("XX"); ok {
		t.Fatal("country without a gazetteer capital must not borrow another city's coordinate")
	}
}

func TestRegionFallbackUsesSeatOrMostPopulousMemberNotWeightedCentre(t *testing.T) {
	countries := countryRow("US", "USA", "United States", "Washington")
	admins := "US.TN\tTennessee\nUS.AL\tAlabama\n"
	places := cityRow("501", "Seat City", "", "PPLA", "35", "-86", "US", "TN", "10000") +
		cityRow("502", "Largest City", "", "PPL", "36", "-85", "US", "TN", "900000") +
		cityRow("503", "Alabama Town", "", "PPL", "32", "-86", "US", "AL", "300000") +
		cityRow("504", "Alpha", "", "PPL", "30", "-88", "US", "AL", "100000")
	g := writeGazetteer(t, countries, admins, places)
	seat, ok := g.Lookup("Tennessee", "US")
	if !ok || seat.Feature != "ADM1" || seat.Lat != 35 || seat.Lon != -86 {
		t.Fatalf("region with administrative seat should use seat coordinates: %+v, %v", seat, ok)
	}
	seatByID, ok := g.ByID(seat.ID)
	if !ok || seatByID.Lat != seat.Lat || seatByID.Lon != seat.Lon {
		t.Fatalf("canonical region ID has inconsistent coordinates: region=%+v byID=%+v", seat, seatByID)
	}
	otherRegion, ok := g.Lookup("Alabama", "US")
	if !ok || otherRegion.Lat != 32 || otherRegion.Lon != -86 {
		t.Fatalf("region without a seat should use its most populous in-region place, got %+v, %v", otherRegion, ok)
	}
}

func TestSameCountryAdminRegionSeatHomonymHasOneCanonicalCandidate(t *testing.T) {
	countries := countryRow("US", "USA", "United States", "Washington")
	admins := "US.DC\tDistrict of Columbia\n"
	places := cityRow("701", "Washington", "Washington DC", "PPLC", "38.9072", "-77.0369", "US", "DC", "700000")
	g := writeGazetteer(t, countries, admins, places)

	region, ok := g.ByID("admin:US.DC")
	if !ok || region.Feature != "ADM1" || region.CountryCode != "US" {
		t.Fatalf("expected canonical District of Columbia ADM1: %+v, found=%v", region, ok)
	}
	seat, ok := g.ByID("701")
	if !ok || seat.CountryCode != region.CountryCode || seat.AdminCode != region.AdminCode || region.Lat != seat.Lat || region.Lon != seat.Lon {
		t.Fatalf("fixture should model a same-country/admin region and capital seat: region=%+v seat=%+v found=%v", region, seat, ok)
	}
	candidates := g.Candidates("", "Officials meet in Washington, DC", "", "", 10)
	if len(candidates) != 1 || candidates[0].ID != "701" {
		t.Fatalf("same-country/admin seat homonym should have one canonical city interpretation, got %+v", candidates)
	}
}

func TestExplicitLookupRemainsAvailableForAmbiguousNames(t *testing.T) {
	g := fixtureGazetteer(t)
	for _, tc := range []struct {
		country, wantID string
	}{
		{"FR", "100"},
		{"France", "100"},
		{"US", "101"},
		{"United States", "101"},
	} {
		got, ok := g.Lookup("Paris", tc.country)
		if !ok || got.ID != tc.wantID {
			t.Errorf("explicit Lookup(Paris, %q) = %+v, %v; want %q", tc.country, got, ok, tc.wantID)
		}
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
		{"unresolved historical numeric ID is removed", StoredLocation{PlaceID: "99999999", Lat: 12, Lon: 34, Precision: CityResolution}, []TextEvidence{{Title: "Officials discuss unrelated policy"}}, "unsupported_match", "", 0},
		{"unresolved historical admin ID is removed", StoredLocation{PlaceID: "admin:US.WY", Lat: 43, Lon: -107, Precision: RegionResolution}, []TextEvidence{{Title: "Officials discuss unrelated policy"}}, "unsupported_match", "", 0},
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

func TestRepairUsesAcceptedLongestSpanAndCanonicalCoordinates(t *testing.T) {
	g := writeGazetteer(t,
		countryRow("US", "USA", "United States", "Washington"),
		"US.TX\tTexas\nUS.NY\tNew York\n",
		cityRow("601", "Springfield", "", "PPL", "30", "-90", "US", "TX", "100000")+
			cityRow("602", "New Springfield", "", "PPL", "40", "-75", "US", "NY", "50000")+
			cityRow("603", "Washington", "", "PPLC", "38.9", "-77", "US", "DC", "700000"),
	)
	evidence := []TextEvidence{{Title: "New Springfield officials announce a plan"}}
	loser := g.RepairLocation(StoredLocation{PlaceID: "601", Lat: 30, Lon: -90, Precision: CityResolution}, evidence)
	if loser.Reason == "" || loser.Replacement == nil || loser.Replacement.ID != "602" {
		t.Fatalf("overlapping alias loser should be replaced by accepted longest span: %+v", loser)
	}
	canonical := g.RepairLocation(StoredLocation{PlaceID: "602", Lat: 39, Lon: -76, Precision: CityResolution}, []TextEvidence{{Title: "New Springfield", Dateline: "New Springfield, New York"}})
	if canonical.Reason == "" || canonical.Replacement == nil || canonical.Replacement.ID != "602" || canonical.Replacement.Lat != 40 || canonical.Replacement.Lon != -75 {
		t.Fatalf("repair should normalize valid-but-noncanonical coordinates: %+v", canonical)
	}
}

func TestRepairPreservesValidHazardHintPoint(t *testing.T) {
	g := fixtureGazetteer(t)
	decision := g.RepairLocation(StoredLocation{PlaceID: "hint:event-7", Lat: 12.3, Lon: 45.6, Precision: PointResolution}, []TextEvidence{{Title: "Earthquake reported"}})
	if decision.Reason != "" || decision.Replacement != nil {
		t.Fatalf("valid hazard point should remain unchanged, got %+v", decision)
	}
}

func TestRepairReviewsAmbiguousCanonicalTitleButReplotsStrongConflict(t *testing.T) {
	g := writeGazetteer(t,
		countryRow("IN", "IND", "India", "New Delhi")+countryRow("PK", "PAK", "Pakistan", "Islamabad"),
		"IN.TG\tTelangana\nPK.SD\tSindh\n",
		cityRow("801", "Hyderabad", "", "PPLA", "17.3850", "78.4867", "IN", "TG", "10000000")+
			cityRow("802", "Hyderabad", "", "PPLA", "25.3960", "68.3578", "PK", "SD", "1000000"),
	)
	india, ok := g.ByID("801")
	if !ok {
		t.Fatal("fixture city Hyderabad, India is missing")
	}
	pakistan, ok := g.ByID("802")
	if !ok {
		t.Fatal("fixture city Hyderabad, Pakistan is missing")
	}
	ambiguous := g.RepairLocation(StoredLocation{PlaceID: pakistan.ID, Lat: float64(float32(pakistan.Lat)), Lon: float64(float32(pakistan.Lon)), Precision: CityResolution}, []TextEvidence{{Title: "Hyderabad Announces a New Policy"}})
	if ambiguous.Reason != "" || ambiguous.Replacement != nil || ambiguous.ReviewReason != "canonical_title_requires_disambiguation" {
		t.Fatalf("ambiguous canonical historical title should be preserved with needs-review, not deleted/replotted: %+v", ambiguous)
	}
	conflict := g.RepairLocation(StoredLocation{PlaceID: pakistan.ID, Lat: float64(float32(pakistan.Lat)), Lon: float64(float32(pakistan.Lon)), Precision: CityResolution}, []TextEvidence{{Title: "Officials meet in Hyderabad, India"}})
	if conflict.Reason != "unsupported_match" || conflict.Replacement == nil || conflict.Replacement.ID != india.ID {
		t.Fatalf("strong explicit country evidence should replot to the matching Hyderabad: %+v", conflict)
	}
}

func TestRepairRemovesUnsupportedCanonicalADM1Row(t *testing.T) {
	g := fixtureGazetteer(t)
	decision := g.RepairLocation(StoredLocation{PlaceID: "112", Lat: 31, Lon: -100, Precision: RegionResolution}, []TextEvidence{{Title: "Toyota recalls vehicles after airbag fault"}})
	if decision.Reason != "unsupported_match" || decision.Replacement != nil {
		t.Fatalf("valid-coordinate ADM1 row unsupported by article evidence must be removed: %+v", decision)
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
