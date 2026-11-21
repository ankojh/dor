package agents

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func plotFixtureGazetteer(t *testing.T) *geo.Gazetteer {
	t.Helper()
	dir := t.TempDir()
	countryRow := func(code, iso3, name, capital string) string {
		return strings.Join([]string{code, iso3, "", "", name, capital}, "\t") + "\n"
	}
	cityRow := func(id, name, feature, lat, lon, cc, admin, pop string) string {
		fields := make([]string, 19)
		fields[0], fields[1], fields[4], fields[5], fields[7] = id, name, lat, lon, feature
		fields[8], fields[10], fields[14] = cc, admin, pop
		return strings.Join(fields, "\t") + "\n"
	}
	files := map[string]string{
		"countryInfo.txt":      countryRow("US", "USA", "United States", "Washington") + countryRow("CN", "CHN", "China", "Beijing") + countryRow("CH", "CHE", "Switzerland", "Bern") + countryRow("FR", "FRA", "France", "Paris"),
		"admin1CodesASCII.txt": "US.PA\tPennsylvania\nCH.GE\tGeneva\n",
		"cities15000.txt": cityRow("phl", "Philadelphia", "PPLA", "39.9526", "-75.1652", "US", "PA", "1600000") +
			cityRow("geneva", "Geneva", "PPLA", "46.2044", "6.1432", "CH", "GE", "200000") +
			cityRow("washington", "Washington", "PPLC", "38.9072", "-77.0369", "US", "DC", "700000") +
			cityRow("beijing", "Beijing", "PPLC", "39.9042", "116.4074", "CN", "BJ", "21000000") +
			cityRow("paris", "Paris", "PPLC", "48.8566", "2.3522", "FR", "11", "2161000"),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	g, err := geo.Load(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load fixture gazetteer: %v", err)
	}
	return g
}

// plotDecider chooses the requested primary by its visible candidate label and
// marks party candidates whose names occur in their party question.
type plotDecider struct {
	primary   string
	precision string
	parties   []string
}

func (d plotDecider) Decide(_ context.Context, req decide.Request) (decide.Response, error) {
	primaryKey := ""
	for key, label := range req.Questions["primary"].Criteria {
		if key != "none" && strings.Contains(strings.ToLower(label), strings.ToLower(d.primary)) {
			primaryKey = key
			break
		}
	}
	answers := map[string]decide.Answer{
		"primary":   {Type: decide.TypeChoice, Present: true, Choice: primaryKey, Probabilities: map[string]float64{primaryKey: .99}},
		"precision": {Type: decide.TypeChoice, Present: true, Choice: d.precision},
	}
	for key, q := range req.Questions {
		if !strings.HasPrefix(key, "party_") {
			continue
		}
		party := false
		for _, name := range d.parties {
			if strings.Contains(strings.ToLower(q.Instructions), strings.ToLower(name)) {
				party = true
			}
		}
		if party {
			answers[key] = decide.Answer{Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: .99}
		} else {
			answers[key] = decide.Answer{Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: 0}
		}
	}
	return decide.Response{Answers: answers}, nil
}

func TestJevGenevaVenueAndCountryPartiesUseHonestPrecision(t *testing.T) {
	g := plotFixtureGazetteer(t)
	article := &dorv1.Article{Id: "geneva-talks", Title: "United States and China hold talks in Geneva", Lede: "Officials discuss trade policy.", Dateline: "Geneva"}
	p := &plotter{deps: Deps{Gazetteer: g, Decider: plotDecider{primary: "Geneva", precision: "point", parties: []string{"United States", "China"}}}, log: testLogger()}
	placed := p.place(context.Background(), article)
	if placed.Backend != "jev" || len(placed.Locations) < 3 {
		t.Fatalf("expected jev Geneva plus parties, got backend=%q locations=%+v", placed.Backend, placed.Locations)
	}
	venue := placed.Locations[0]
	if venue.Name != "Geneva" || venue.Role != dorv1.Role_ROLE_VENUE || venue.Precision != dorv1.Precision_PRECISION_CITY {
		t.Errorf("Geneva must be the city-precision venue despite a point answer: %+v", venue)
	}
	seen := map[string]bool{}
	for _, loc := range placed.Locations[1:] {
		if loc.Role != dorv1.Role_ROLE_PARTY {
			continue
		}
		seen[loc.Name] = true
		if loc.Precision != dorv1.Precision_PRECISION_COUNTRY {
			t.Errorf("country party %q precision = %v, want country", loc.Name, loc.Precision)
		}
	}
	if !seen["United States"] || !seen["China"] {
		t.Errorf("expected US and China country parties, got %v (all=%+v)", seen, placed.Locations)
	}
	if !placed.MultiRegion {
		t.Errorf("different-country parties should mark the story multi-region")
	}
}

func TestJevPointUsesLiveParentContextAndExtractedFeatureForPhoton(t *testing.T) {
	var query url.Values
	var contextActive bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		contextActive = r.Context().Err() == nil
		fmt.Fprint(w, `{"features":[{"geometry":{"coordinates":[-75.164,39.953]},"properties":{"osm_type":"W","osm_id":22,"name":"Broad Street","city":"Philadelphia","state":"Pennsylvania","country":"United States","countrycode":"US","type":"street"}}]}`)
	}))
	defer server.Close()
	g := plotFixtureGazetteer(t)
	article := &dorv1.Article{Id: "phl-street", Title: "Police respond on Broad Street in Philadelphia", Lede: "The incident happened near Broad Street.", Dateline: "Philadelphia"}
	p := &plotter{deps: Deps{Gazetteer: g, Photon: geo.NewPhoton(server.URL), Decider: plotDecider{primary: "Philadelphia", precision: "point"}}, log: testLogger()}
	placed := p.place(context.Background(), article)
	if !contextActive {
		t.Error("Photon request did not inherit a live parent context after the jev child context was canceled")
	}
	if !strings.Contains(query.Get("q"), "Broad Street") || !strings.Contains(query.Get("q"), "Philadelphia") {
		t.Errorf("Photon query should include the extracted feature and chosen city, got %q", query.Get("q"))
	}
	if len(placed.Locations) == 0 || placed.Locations[0].Name != "Broad Street" || placed.Locations[0].Precision != dorv1.Precision_PRECISION_STREET {
		t.Errorf("expected accepted Photon street result, got %+v", placed.Locations)
	}
}

func TestPrecisionClaimsNeverExceedResolvedLocation(t *testing.T) {
	g := plotFixtureGazetteer(t)
	city, ok := g.ByID("phl")
	if !ok {
		t.Fatal("fixture city missing")
	}
	region := geo.Place{ID: "region", Name: "Pennsylvania", Lat: 40, Lon: -77, Feature: "ADM1"}
	country := geo.Place{ID: "country:US", Name: "United States", CountryID: true, Lat: 39, Lon: -98, Feature: "PCLI"}
	for _, tc := range []struct {
		name  string
		place geo.Place
		asked dorv1.Precision
		want  dorv1.Precision
	}{
		{"city", city, dorv1.Precision_PRECISION_POINT, dorv1.Precision_PRECISION_CITY},
		{"region party", region, dorv1.Precision_PRECISION_POINT, dorv1.Precision_PRECISION_REGION},
		{"country party", country, dorv1.Precision_PRECISION_CITY, dorv1.Precision_PRECISION_COUNTRY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc := placeLocation(tc.place, tc.asked, dorv1.Role_ROLE_PARTY, 1)
			if loc == nil || loc.Precision != tc.want {
				t.Fatalf("precision cap = %+v, want %v", loc, tc.want)
			}
		})
	}
	placed := (&plotter{}).fromJev(context.Background(), &dorv1.Article{Dateline: "Philadelphia"}, []geo.Place{city, region, country}, decide.PlotResult{
		PrimaryIndex: 0,
		Precision:    "city",
		PrecisionOK:  true,
		Parties:      []string{"region", "country:US"},
		PartyP:       map[string]float64{"region": .9, "country:US": .9},
	})
	partyPrecision := map[string]dorv1.Precision{}
	for _, loc := range placed.Locations {
		if loc.Role == dorv1.Role_ROLE_PARTY {
			partyPrecision[loc.PlaceId] = loc.Precision
		}
	}
	if partyPrecision["region"] != dorv1.Precision_PRECISION_REGION || partyPrecision["country:US"] != dorv1.Precision_PRECISION_COUNTRY {
		t.Errorf("fromJev party precisions = %v, want region and country", partyPrecision)
	}
}

func TestPrecisePhotonFallbacksStayAtCityPrecision(t *testing.T) {
	g := plotFixtureGazetteer(t)
	city, _ := g.ByID("phl")
	article := &dorv1.Article{Title: "Officials meet on Broad Street"}
	for _, tc := range []struct {
		name   string
		photon *geo.Photon
	}{
		{"disabled", nil},
		{"request error", geo.NewPhoton("http://127.0.0.1:1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plotter{deps: Deps{Photon: tc.photon}}
			loc := p.geocodePrecise(context.Background(), article, city, dorv1.Precision_PRECISION_POINT, dorv1.Role_ROLE_VENUE)
			if loc.Precision != dorv1.Precision_PRECISION_CITY {
				t.Fatalf("fallback reported precision %v, want city", loc.Precision)
			}
		})
	}
	t.Run("no extracted feature", func(t *testing.T) {
		p := &plotter{deps: Deps{Photon: geo.NewPhoton("http://127.0.0.1:1")}}
		loc := p.geocodePrecise(context.Background(), &dorv1.Article{Title: "Officials gather for a meeting"}, city, dorv1.Precision_PRECISION_POINT, dorv1.Role_ROLE_VENUE)
		if loc.Precision != dorv1.Precision_PRECISION_CITY {
			t.Fatalf("no-feature fallback reported precision %v, want city", loc.Precision)
		}
	})
	t.Run("city answer avoids Photon", func(t *testing.T) {
		called := false
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
		defer server.Close()
		p := &plotter{deps: Deps{Photon: geo.NewPhoton(server.URL)}}
		loc := p.geocodePrecise(context.Background(), article, city, dorv1.Precision_PRECISION_CITY, dorv1.Role_ROLE_VENUE)
		if called || loc.Precision != dorv1.Precision_PRECISION_CITY {
			t.Fatalf("city answer should stay city precision without Photon: called=%v loc=%+v", called, loc)
		}
	})
}

func TestInvalidHintsFallBackToCountryOrGlobal(t *testing.T) {
	g := plotFixtureGazetteer(t)
	for _, tc := range []struct {
		name, country string
	}{
		{"country fallback", "US"},
		{"global fallback", "Atlantis"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
			article := &dorv1.Article{Id: "invalid-hint", Title: "An event happened", Hints: &dorv1.RawHints{Located: true, Lat: 0, Lon: 0, Country: tc.country, Place: "bad hint"}}
			placed := p.place(context.Background(), article)
			if tc.country == "US" {
				if len(placed.Locations) != 1 || placed.Locations[0].PlaceId != "country:US" || placed.Locations[0].Precision != dorv1.Precision_PRECISION_COUNTRY {
					t.Fatalf("invalid hint should fall back to valid country: %+v", placed.Locations)
				}
			} else if len(placed.Locations) != 0 || placed.Backend != "none" {
				t.Fatalf("invalid hint without a valid country should become global: %+v", placed)
			}
		})
	}
}
