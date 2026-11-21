package agents

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/llm"
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
		"countryInfo.txt":      countryRow("US", "USA", "United States", "Washington") + countryRow("CN", "CHN", "China", "Beijing") + countryRow("CH", "CHE", "Switzerland", "Bern") + countryRow("FR", "FRA", "France", ""),
		"admin1CodesASCII.txt": "US.PA\tPennsylvania\nCH.GE\tGeneva Canton\n",
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

func plotMatchingFixture(t *testing.T) *geo.Gazetteer {
	t.Helper()
	dir := t.TempDir()
	countries := []struct{ code, iso3, name, capital, admin string }{
		{"US", "USA", "United States", "Washington", "DC"}, {"GB", "GBR", "United Kingdom", "London", "ENG"},
		{"FR", "FRA", "France", "", "11"}, {"CH", "CHE", "Switzerland", "Bern", "BE"},
		{"CN", "CHN", "China", "Beijing", "BJ"}, {"CA", "CAN", "Canada", "Ottawa", "ON"},
		{"PA", "PAN", "Panama", "Panama City", "8"}, {"IL", "ISR", "Israel", "Jerusalem", "JM"},
		{"MY", "MYS", "Malaysia", "Kuala Lumpur", "14"}, {"TO", "TON", "Tonga", "Nuku'alofa", "04"},
		{"AS", "ASM", "American Samoa", "Pago Pago", "01"}, {"AT", "AUT", "Austria", "Vienna", "9"},
		{"SA", "SAU", "Saudi Arabia", "Riyadh", "01"}, {"ZA", "ZAF", "South Africa", "Pretoria", "GP"}, {"UG", "UGA", "Uganda", "Kampala", "C"},
		{"TN", "TUN", "Tunisia", "Tunis", "11"}, {"CO", "COL", "Colombia", "Bogota", "25"},
		{"AM", "ARM", "Armenia", "Yerevan", "ER"}, {"IN", "IND", "India", "New Delhi", "DL"},
		{"RS", "SRB", "Serbia", "Belgrade", "00"}, {"EG", "EGY", "Egypt", "Cairo", "C"},
		{"BR", "BRA", "Brazil", "Brasilia", "DF"}, {"JP", "JPN", "Japan", "Tokyo", "13"},
	}
	var countryData, adminData, cityData strings.Builder
	for _, c := range countries {
		countryData.WriteString(strings.Join([]string{c.code, c.iso3, "", "", c.name, c.capital}, "\t") + "\n")
		adminData.WriteString(c.code + "." + c.admin + "\t" + c.admin + "\n")
	}
	adminData.WriteString("US.TX\tTexas\nUS.NY\tNew York\nUS.FL\tFlorida\nFR.11\tIle-de-France\n")
	add := func(id, name, aliases, feature, lat, lon, code, admin, pop string) {
		fields := make([]string, 19)
		fields[0], fields[1], fields[3] = id, name, aliases
		fields[4], fields[5], fields[7] = lat, lon, feature
		fields[8], fields[10], fields[14] = code, admin, pop
		cityData.WriteString(strings.Join(fields, "\t") + "\n")
	}
	for i, c := range countries {
		if c.capital != "" {
			add(fmt.Sprintf("capital-%02d", i), c.capital, "", "PPLC", fmt.Sprintf("%d", 20+i), fmt.Sprintf("%d", 10+i), c.code, c.admin, "1000000")
		}
	}
	add("100", "Paris", "", "PPLC", "48.8566", "2.3522", "FR", "11", "2161000")
	add("101", "Paris", "", "PPL", "33.6609", "-95.5555", "US", "TX", "25000")
	// Deliberate collisions model GeoNames names/alternate names that must not
	// become an event location from capitalization or a single matching word.
	for i, name := range []string{"Drug", "Paradise", "March", "It", "Africa", "Toyota", "Michael Jackson", "Victoria Beckham", "Google", "Lula", "STAT+", "International", "Eastern Europe", "Western leaders", "Northern Lights", "Delta Air Lines", "Delta", "Goodyear", "Florida", "York"} {
		code, admin := "US", "DC"
		if name == "March" || name == "Victoria Beckham" || name == "York" {
			code, admin = "GB", "ENG"
		}
		if name == "Africa" {
			code, admin = "ZA", "GP"
		}
		if name == "Toyota" {
			code, admin = "JP", "13"
		}
		if name == "Lula" {
			code, admin = "BR", "DF"
		}
		if name == "Florida" {
			code, admin = "US", "FL"
		}
		if name == "Goodyear" {
			code, admin = "US", "AZ"
		}
		add(fmt.Sprintf("collision-%02d", i), name, "", "PPL", fmt.Sprintf("%d", 5+i), fmt.Sprintf("%d", 30+i), code, admin, "500000")
	}
	add("mah dia", "Mahdia", "", "PPL", "35.5", "11.06", "TN", "11", "80000")
	add("floridablanca", "Floridablanca", "", "PPL", "7.06", "-73.09", "CO", "25", "300000")
	add("armenia-co", "Armenia", "", "PPL", "4.53", "-75.68", "CO", "25", "300000")
	add("indjija", "Indjija", "", "PPL", "45.05", "20.08", "RS", "00", "50000")
	add("palmares", "Uniao dos Palmares", "União dos Palmares,Palmares", "PPL", "-9.15", "-36.02", "BR", "DF", "65000")
	add("new-york", "New York", "New York City,NYC", "PPLA", "40.71", "-74.00", "US", "NY", "8000000")
	add("frome", "Frome", "", "PPL", "51.23", "-2.32", "GB", "ENG", "28000")
	add("nis", "Nis", "Niš", "PPL", "43.32", "21.89", "RS", "00", "260000")
	for name, data := range map[string]string{"cities15000.txt": cityData.String(), "countryInfo.txt": countryData.String(), "admin1CodesASCII.txt": adminData.String()} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	g, err := geo.Load(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load matching fixture: %v", err)
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

func TestValidPhotonPlaceHonorsIdentityAndGeographicEvidence(t *testing.T) {
	g := plotFixtureGazetteer(t)
	p := &plotter{deps: Deps{Gazetteer: g}}
	article := &dorv1.Article{Dateline: "Philadelphia, Pennsylvania"}
	for _, tc := range []struct {
		name    string
		place   geo.Place
		article *dorv1.Article
		country string
		want    bool
	}{
		{"wrong country", geo.Place{Name: "Toronto", Lat: 43.65, Lon: -79.38, CountryCode: "CA", Country: "Canada", Feature: "PPL"}, article, "US", false},
		{"missing country identity", geo.Place{Name: "Somewhere", Lat: 39.95, Lon: -75.16, Feature: "PPL"}, article, "US", false},
		{"far from known dateline", geo.Place{Name: "Washington", Lat: 38.9072, Lon: -77.0369, CountryCode: "US", Country: "United States", Feature: "PPL"}, article, "US", false},
		{"far from valid hint anchor", geo.Place{Name: "Philadelphia", Lat: 39.9526, Lon: -75.1652, CountryCode: "US", Country: "United States", Feature: "street"}, &dorv1.Article{Hints: &dorv1.RawHints{Located: true, Lat: 46.2044, Lon: 6.1432}}, "US", false},
		{"nearby supported street", geo.Place{Name: "Broad Street", Lat: 39.953, Lon: -75.164, CountryCode: "US", Country: "United States", Feature: "street"}, article, "US", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.validPhotonPlace(tc.place, tc.country, tc.article); got != tc.want {
				t.Fatalf("validPhotonPlace(%+v, %q, %+v) = %v, want %v", tc.place, tc.country, tc.article, got, tc.want)
			}
		})
	}
	street := geo.Place{Feature: "street"}
	if got := geo.PrecisionOf(street); got != geo.StreetResolution {
		t.Errorf("accepted Photon street feature should map to street precision, got %v", got)
	}
}

func TestFromClaudeRejectsPhotonCountryMismatchAndFallsBackToCountry(t *testing.T) {
	llmServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"msg_test","type":"message","role":"assistant","model":"claude-haiku-5-5","content":[{"type":"text","text":"{\"place_name\":\"Unmapped Harbor\",\"country\":\"US\",\"entities\":[],\"category\":\"politics\"}"}],"stop_reason":"end_turn","usage":{"input_tokens":20,"output_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`)
	}))
	defer llmServer.Close()
	photonServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"features":[{"geometry":{"coordinates":[-79.3832,43.6532]},"properties":{"name":"Toronto","country":"Canada","countrycode":"CA","type":"city"}}]}`)
	}))
	defer photonServer.Close()

	client, err := llm.New(llm.Config{
		APIKey: "test-key", BaseURL: llmServer.URL,
		HaikuModel: "claude-haiku-5-5", OpusModel: "claude-opus-5-5", Effort: "low",
		MaxRetries: 0,
		Price: map[string]config.Price{
			"claude-haiku-5-5": {InputPerMTok: 1, OutputPerMTok: 5},
			"claude-opus-5-5":  {InputPerMTok: 15, OutputPerMTok: 75},
		},
		Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}, plotTestBudget{})
	if err != nil {
		t.Fatalf("create mock Claude client: %v", err)
	}
	g := plotFixtureGazetteer(t)
	p := &plotter{deps: Deps{Gazetteer: g, Photon: geo.NewPhoton(photonServer.URL), Enricher: client}, log: testLogger()}
	article := &dorv1.Article{Id: "claude-photon-mismatch", Title: "Officials discuss an agreement", Dateline: "Philadelphia, Pennsylvania"}
	placed, ok := p.fromClaude(context.Background(), article, nil, "")
	if !ok || placed == nil || len(placed.Locations) != 1 {
		t.Fatalf("Claude should fall back after rejecting the wrong-country Photon result: placed=%+v ok=%v", placed, ok)
	}
	loc := placed.Locations[0]
	if loc.PlaceId != "country:US" || loc.Precision != dorv1.Precision_PRECISION_COUNTRY {
		t.Fatalf("rejected foreign Photon point should fall back to country precision: %+v", loc)
	}
}

type plotTestBudget struct{}

func (plotTestBudget) Check(_ context.Context, req budget.Request) (budget.Verdict, error) {
	return budget.Verdict{Allowed: true, Reason: "test", Reservation: &budget.Reservation{ID: "plot-test", API: req.API}}, nil
}

func (plotTestBudget) Record(context.Context, budget.Reservation, budget.Usage) error { return nil }

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

func TestDeterministicPlottingRejectsTextCollisions(t *testing.T) {
	g := plotMatchingFixture(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	falseMatches := []struct{ title, lede, dateline string }{
		{"Residents complain about services in drug", "Officials opened an investigation.", ""},
		{"Travel troubles in paradise", "The resort is under new management.", ""},
		{"Debate continues in March", "A proposal remains under review.", ""},
		{"Coach says team will learn from it", "The club announced a change.", ""},
		{"Africa’s growth outlook improves", "Analysts expect another strong year.", ""},
		{"SA’s first female deputy president dies", "The announcement was made today.", ""},
		{"It’s a dream come true", "The singer thanked her supporters.", ""},
		{"CA officials reject the proposal", "The decision followed a lengthy debate.", ""},
		{"PA lawmakers announce a new bill", "The measure will be debated next week.", ""},
		{"IL officials confirm the report", "No additional details were provided.", ""},
		{"Zimbabwe revenue rises to US$1.239bn", "The figure was reported in annual accounts.", ""},
		{"Company reports earnings in US dollars", "Revenue increased over the prior year.", ""},
		{"The U.S.C. filing was released today", "Attorneys reviewed the document.", ""},
		{"my team is ready to compete", "The coach spoke to reporters.", ""},
		{"We need to act as soon as possible", "The announcement came on Tuesday.", ""},
		{"They travelled to the capital", "Officials declined to comment.", ""},
		{"The meeting starts at noon", "The agenda was released yesterday.", ""},
		{"Toyota recalls vehicles over airbag fault", "The recall covers several models.", ""},
		{"Michael Jackson film breaks box office records", "The biopic opened this weekend.", ""},
		{"Victoria Beckham launches a new collection", "The designer spoke at the event.", ""},
		{"Google announces a new AI model", "The company shared details online.", ""},
		{"Lula vetoes pension bill", "The president issued a statement.", ""},
		{"STAT+ reports new clinical findings", "Researchers said more study is needed.", ""},
		{"International flights face long delays", "Airports reported disruptions.", ""},
		{"Eastern Europe braces for winter", "Forecasters expect freezing conditions.", ""},
		{"Western leaders meet to discuss aid", "The talks lasted several hours.", ""},
		{"Northern Lights visible tonight", "The display may continue through morning.", ""},
		{"Delta Air Lines cancels hundreds of flights", "The airline cited a technical issue.", ""},
	}
	for i, tc := range falseMatches {
		t.Run(fmt.Sprintf("false_%02d", i), func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: fmt.Sprintf("false-%d", i), Title: tc.title, Lede: tc.lede, Dateline: tc.dateline})
			if strings.Contains(tc.title, "US$1.239bn") {
				for _, loc := range placed.Locations {
					if place, ok := g.ByID(loc.PlaceId); ok && place.CountryCode == "US" {
						t.Errorf("currency text must not produce a United States match: title=%q locations=%+v", tc.title, placed.Locations)
					}
				}
				return
			}
			if len(placed.Locations) != 0 {
				t.Errorf("ordinary text should not plot as a town: title=%q lede=%q dateline=%q locations=%+v", tc.title, tc.lede, tc.dateline, placed.Locations)
			}
		})
	}
}

func TestDeterministicPlottingUsesFullArticleEvidenceForRealPlaces(t *testing.T) {
	g := plotMatchingFixture(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	cases := []struct {
		name, title, lede, dateline, wantID, wantCountry string
		wantRole                                         dorv1.Role
	}{
		{"uganda versus mahdia", "Uganda faces Mahdia in friendly", "The match was played in Mahdia, Tunisia.", "Mahdia, Tunisia", "mah dia", "TN", dorv1.Role_ROLE_VENUE},
		{"florida substring collision", "Florida lawmakers debate policy", "The protest took place in Floridablanca, Colombia.", "Floridablanca, Colombia", "floridablanca", "CO", dorv1.Role_ROLE_VENUE},
		{"armenia city", "Armenia residents await election", "Officials met in Armenia, Colombia.", "Armenia, Colombia", "armenia-co", "CO", dorv1.Role_ROLE_VENUE},
		{"armenia country", "Armenia signs an accord with Azerbaijan", "Talks continued in Yerevan, Armenia.", "Yerevan, Armenia", "", "AM", dorv1.Role_ROLE_VENUE},
		{"india versus indjija", "India delegation visits Indjija", "The ceremony took place in Indjija, Serbia.", "Indjija, Serbia", "indjija", "RS", dorv1.Role_ROLE_VENUE},
		{"egypt versus goodyear", "Egypt reacts to Goodyear announcement", "Officials spoke in Cairo, Egypt.", "Cairo, Egypt", "", "EG", dorv1.Role_ROLE_VENUE},
		{"new york not york", "New York considers a congestion charge", "The plan affects New York City residents.", "NEW YORK", "new-york", "US", dorv1.Role_ROLE_VENUE},
		{"paris france", "Officials announce a plan in Paris", "The decision was made in Paris, France.", "Paris, France", "100", "FR", dorv1.Role_ROLE_VENUE},
		{"paris texas", "Officials announce a plan in Paris", "The decision was made in Paris, Texas.", "Paris, Texas", "101", "US", dorv1.Role_ROLE_VENUE},
		{"qualified frome", "Residents of Frome discuss new plan", "The debate was held in Frome, England.", "Frome, United Kingdom", "frome", "GB", dorv1.Role_ROLE_VENUE},
		{"qualified nis", "Niš residents welcome new rail service", "The station is in Niš, Serbia.", "Niš, Serbia", "nis", "RS", dorv1.Role_ROLE_VENUE},
		{"locative multiword alias", "Officials meet in União dos Palmares", "The meeting was held in the town.", "União dos Palmares, Brazil", "palmares", "BR", dorv1.Role_ROLE_VENUE},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: tc.name, Title: tc.title, Lede: tc.lede, Dateline: tc.dateline})
			if len(placed.Locations) == 0 {
				t.Fatalf("expected a supported place, got none (backend=%s)", placed.Backend)
			}
			loc := placed.Locations[0]
			if tc.wantID != "" && loc.PlaceId != tc.wantID {
				t.Errorf("resolved %s (%s), want canonical ID %s; all locations: %+v", loc.Name, loc.PlaceId, tc.wantID, placed.Locations)
			}
			resolved, known := g.ByID(loc.PlaceId)
			if !known || resolved.CountryCode != tc.wantCountry {
				t.Errorf("resolved country %q (known=%v), want %q: %+v", resolved.CountryCode, known, tc.wantCountry, loc)
			}
			if loc.Role != tc.wantRole {
				t.Errorf("resolved role %s, want %s: %+v", loc.Role, tc.wantRole, loc)
			}
		})
	}
}

func TestAutomaticCountryInitialismsRemainSupported(t *testing.T) {
	g := plotMatchingFixture(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	for _, tc := range []struct{ name, title, code string }{
		{"U.S.", "Officials announced a policy change in U.S.", "US"},
		{"U.K.", "Officials announced a policy change in U.K.", "GB"},
		{"U.S.A.", "Officials announced a policy change in U.S.A.", "US"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: "initialism-" + tc.name, Title: tc.title})
			for _, loc := range placed.Locations {
				if loc.Precision != dorv1.Precision_PRECISION_COUNTRY {
					continue
				}
				if place, ok := g.ByID(loc.PlaceId); ok && place.CountryCode == tc.code && place.CountryID {
					return
				}
			}
			t.Fatalf("proper country initialism should resolve as country %s: %+v", tc.code, placed.Locations)
		})
	}
}

func TestCachedGazetteerPlotterMatchingWhenConfigured(t *testing.T) {
	dir := os.Getenv("DOR_TEST_GEONAMES_DIR")
	if dir == "" {
		t.Skip("set DOR_TEST_GEONAMES_DIR to run these cases against the full cached GeoNames dataset")
	}
	for _, file := range []string{"cities15000.txt", "countryInfo.txt", "admin1CodesASCII.txt"} {
		if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
			t.Fatalf("configured GeoNames dataset is incomplete (%s): %v", file, err)
		}
	}
	g, err := geo.Load(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	for _, tc := range []struct {
		name, title, lede, dateline string
		want                        string
	}{
		{"lowercase common word", "Residents complain in drug", "Officials opened an investigation.", "", ""},
		{"lowercase paradise", "Travel troubles in paradise", "The resort is under new management.", "", ""},
		{"lowercase March", "Debate continues in March", "A proposal remains under review.", "", ""},
		{"learn from it", "Coach says team will learn from it", "The club announced a change.", "", ""},
		{"possessive country word", "Africa’s growth outlook improves", "Analysts expect another strong year.", "", ""},
		{"possessive short country code SA", "SA’s first female deputy president dies", "The announcement was made today.", "", ""},
		{"possessive short country code It", "It’s a dream come true", "The singer thanked her supporters.", "", ""},
		{"short code CA", "CA officials reject the proposal", "The decision followed a lengthy debate.", "", ""},
		{"short code PA", "PA lawmakers announce a new bill", "The measure will be debated next week.", "", ""},
		{"short code IL", "IL officials confirm the report", "No additional details were provided.", "", ""},
		{"lowercase my", "my team is ready to compete", "The coach spoke to reporters.", "", ""},
		{"lowercase to", "Officials travelled to the capital", "The delegation arrived on Tuesday.", "", ""},
		{"lowercase as", "We need to act as soon as possible", "The announcement came on Tuesday.", "", ""},
		{"lowercase at", "The meeting starts at noon", "The agenda was released yesterday.", "", ""},
		{"US dollar currency symbol", "Zimbabwe revenue rises to US$1.239bn", "The figure was reported in annual accounts.", "", "!US"},
		{"US dollars currency name", "Company reports earnings in US dollars", "Revenue increased over the prior year.", "", ""},
		{"USC dotted initialism prefix", "The U.S.C. filing was released today", "Attorneys reviewed the document.", "", ""},
		{"person name", "Michael Jackson film breaks box office records", "The biopic opened this weekend.", "", ""},
		{"person surname alias", "Victoria Beckham launches a new collection", "The designer spoke at the event.", "", ""},
		{"brand name", "Toyota recalls vehicles over airbag fault", "The recall covers several models.", "", ""},
		{"brand Google", "Google announces a new AI model", "The company shared details online.", "", ""},
		{"politician Lula", "Lula vetoes pension bill", "The president issued a statement.", "", ""},
		{"STAT plus", "STAT+ reports new clinical findings", "Researchers said more study is needed.", "", ""},
		{"international", "International flights face long delays", "Airports reported disruptions.", "", ""},
		{"Eastern Europe", "Eastern Europe braces for winter", "Forecasters expect freezing conditions.", "", ""},
		{"Western leaders", "Western leaders meet to discuss aid", "The talks lasted several hours.", "", ""},
		{"Northern Lights", "Northern Lights visible tonight", "The display may continue through morning.", "", ""},
		{"Delta Air Lines", "Delta Air Lines cancels hundreds of flights", "The airline cited a technical issue.", "", ""},
		{"new york dateline alias", "New York weighs tougher e-bike rules", "Officials announced the proposal.", "NEW YORK", "new york"},
		{"frome qualified dateline", "Frome residents discuss local plan", "The meeting was held in Frome.", "Frome, England", "frome"},
		{"nis qualified dateline", "Niš residents welcome new rail service", "The station is in Niš, Serbia.", "Niš, Serbia", "niš"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: tc.name, Title: tc.title, Lede: tc.lede, Dateline: tc.dateline})
			if tc.want == "!US" {
				for _, loc := range placed.Locations {
					if place, ok := g.ByID(loc.PlaceId); ok && place.CountryCode == "US" {
						t.Errorf("currency text must not produce a United States match: locations=%+v", placed.Locations)
					}
				}
				return
			}
			if tc.want == "" {
				if len(placed.Locations) != 0 {
					t.Errorf("false match produced location %+v", placed.Locations)
				}
				return
			}
			if len(placed.Locations) == 0 || normalizePlotName(placed.Locations[0].Name) != tc.want || placed.Locations[0].Role != dorv1.Role_ROLE_VENUE {
				t.Errorf("explicit place/dateline should resolve as venue %q: %+v", tc.want, placed)
			}
		})
	}
	for _, tc := range []struct {
		name, title, lede, dateline, country string
		nameContains                         string
	}{
		{"Uganda versus Mahdia", "Uganda faces Mahdia in friendly", "The match was played in Mahdia, Tunisia.", "Mahdia, Tunisia", "TN", "mahdia"},
		{"Florida versus Floridablanca", "Florida lawmakers debate policy", "The protest took place in Floridablanca, Colombia.", "Floridablanca, Colombia", "CO", "floridablanca"},
		{"Armenia country versus city", "Armenia signs an accord with Azerbaijan", "Talks continued in Yerevan, Armenia.", "Yerevan, Armenia", "AM", "yerevan"},
		{"Armenia city in Colombia", "Armenia residents await election", "Officials met in Armenia, Colombia.", "Armenia, Colombia", "CO", "armenia"},
		{"India versus Indjija", "India delegation visits Indjija", "The ceremony took place in Indjija, Serbia.", "Indjija, Serbia", "RS", "inđija"},
		{"Egypt versus Goodyear", "Egypt reacts to Goodyear announcement", "Officials spoke in Cairo, Egypt.", "Cairo, Egypt", "EG", "cairo"},
		{"York inside New York", "New York considers a congestion charge", "The plan affects New York City residents.", "NEW YORK", "US", "new york"},
	} {
		t.Run("geographic_"+tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: "full-cache-" + tc.name, Title: tc.title, Lede: tc.lede, Dateline: tc.dateline})
			var venue *dorv1.Location
			for _, loc := range placed.Locations {
				if loc.Role == dorv1.Role_ROLE_VENUE {
					venue = loc
					break
				}
			}
			if venue == nil {
				t.Fatalf("expected a venue for %s, got %+v", tc.name, placed.Locations)
			}
			resolved, known := g.ByID(venue.PlaceId)
			if !known || resolved.CountryCode != tc.country || !strings.Contains(strings.ToLower(venue.Name), tc.nameContains) {
				t.Errorf("%s resolved to %q/%q (known=%v), want country=%s and name containing %q; all=%+v", tc.name, venue.Name, resolved.CountryCode, known, tc.country, tc.nameContains, placed.Locations)
			}
		})
	}
	for _, tc := range []struct{ name, title, country string }{
		{"U.S.", "Officials discuss new regulations in U.S.", "US"},
		{"U.K.", "Officials discuss new regulations in U.K.", "GB"},
		{"U.S.A.", "Officials discuss new regulations in U.S.A.", "US"},
	} {
		t.Run("country_initialism_"+tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: "full-cache-initialism-" + tc.name, Title: tc.title})
			for _, loc := range placed.Locations {
				if loc.Precision == dorv1.Precision_PRECISION_COUNTRY {
					if place, ok := g.ByID(loc.PlaceId); ok && place.CountryID && place.CountryCode == tc.country {
						return
					}
				}
			}
			t.Fatalf("proper %s form should resolve as country %s: %+v", tc.name, tc.country, placed.Locations)
		})
	}
	for _, tc := range []struct {
		name, title, lede, dateline, hintCountry, wantPlace string
		wantPrecision                                       dorv1.Precision
	}{
		{"Victoria Beckham with Australia country hint", "Victoria Beckham launches a new collection", "The designer spoke at the event.", "", "AU", "country:AU", dorv1.Precision_PRECISION_COUNTRY},
		{"Toyota recall with Japan country hint", "Toyota recalls vehicles over airbag fault", "The recall covers several models.", "", "JP", "country:JP", dorv1.Precision_PRECISION_COUNTRY},
		{"qualified Victoria in Canada", "Victoria residents gather for a public meeting", "The meeting was held in Victoria, British Columbia.", "Victoria, Canada", "", "", dorv1.Precision_PRECISION_CITY},
	} {
		t.Run("hint_"+tc.name, func(t *testing.T) {
			article := &dorv1.Article{Id: "hint-" + tc.name, Title: tc.title, Lede: tc.lede, Dateline: tc.dateline}
			if tc.hintCountry != "" {
				article.Hints = &dorv1.RawHints{Country: tc.hintCountry}
			}
			placed := p.place(context.Background(), article)
			if len(placed.Locations) == 0 {
				t.Fatalf("expected country or qualified city result, got %+v", placed)
			}
			loc := placed.Locations[0]
			if tc.wantPlace != "" {
				if loc.PlaceId != tc.wantPlace || loc.Precision != tc.wantPrecision {
					t.Fatalf("country-only hint must not make a false precise match: got %+v, want %s at country precision", loc, tc.wantPlace)
				}
				return
			}
			place, ok := g.ByID(loc.PlaceId)
			if !ok || place.CountryCode != "CA" || loc.Precision != tc.wantPrecision || loc.Role != dorv1.Role_ROLE_VENUE || !strings.Contains(strings.ToLower(loc.Name), "victoria") {
				t.Fatalf("explicit Victoria, Canada evidence should retain the city venue: loc=%+v place=%+v known=%v all=%+v", loc, place, ok, placed.Locations)
			}
		})
	}
}

func normalizePlotName(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(name), " "))
}

func TestPlotterCorpusDiagnostic(t *testing.T) {
	inputPath, outputPath := os.Getenv("DOR_TEST_CORPUS_CSV"), os.Getenv("DOR_TEST_CORPUS_OUT")
	dataDir := os.Getenv("DOR_TEST_GEONAMES_DIR")
	if inputPath == "" || outputPath == "" || dataDir == "" {
		t.Skip("set DOR_TEST_CORPUS_CSV, DOR_TEST_CORPUS_OUT and DOR_TEST_GEONAMES_DIR to run the copied headline corpus")
	}
	for _, name := range []string{"cities15000.txt", "countryInfo.txt", "admin1CodesASCII.txt"} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err != nil {
			t.Fatalf("corpus GeoNames dataset is incomplete (%s): %v", name, err)
		}
	}
	g, err := geo.LoadExisting(context.Background(), dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load corpus GeoNames data: %v", err)
	}
	in, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(in).ReadAll()
	_ = in.Close()
	if err != nil {
		t.Fatalf("read corpus CSV: %v", err)
	}
	if len(records) > 0 && len(records[0]) > 0 && strings.EqualFold(strings.TrimSpace(records[0][0]), "id") {
		records = records[1:]
	}
	out, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create corpus JSONL output: %v", err)
	}
	defer out.Close()
	type placeResult struct {
		PlaceID   string          `json:"place_id"`
		Name      string          `json:"name"`
		Lat       float32         `json:"lat"`
		Lon       float32         `json:"lon"`
		Precision dorv1.Precision `json:"precision"`
		Role      dorv1.Role      `json:"role"`
	}
	type outcome struct {
		ID        string        `json:"id,omitempty"`
		Title     string        `json:"title,omitempty"`
		Lede      string        `json:"lede,omitempty"`
		Dateline  string        `json:"dateline,omitempty"`
		Backend   string        `json:"backend,omitempty"`
		Degraded  bool          `json:"degraded,omitempty"`
		Locations []placeResult `json:"locations,omitempty"`
		Summary   bool          `json:"summary,omitempty"`
		Rows      int           `json:"rows,omitempty"`
		Dominant  []frequency   `json:"dominant,omitempty"`
	}
	encoder := json.NewEncoder(out)
	counts := map[string]int{}
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	for i, row := range records {
		if len(row) < 4 {
			t.Fatalf("CSV record %d has %d columns, want id,title,lede,dateline", i+1, len(row))
		}
		article := &dorv1.Article{Id: row[0], Title: row[1], Lede: row[2], Dateline: row[3]}
		placed := p.place(context.Background(), article)
		item := outcome{ID: row[0], Title: row[1], Lede: row[2], Dateline: row[3], Backend: placed.Backend, Degraded: placed.Degraded}
		for _, loc := range placed.Locations {
			item.Locations = append(item.Locations, placeResult{loc.PlaceId, loc.Name, loc.Lat, loc.Lon, loc.Precision, loc.Role})
			counts[loc.PlaceId+"\t"+loc.Name]++
		}
		if err := encoder.Encode(item); err != nil {
			t.Fatalf("write outcome row %d: %v", i+1, err)
		}
	}
	if err := encoder.Encode(outcome{Summary: true, Rows: len(records), Dominant: dominantPlaces(counts, 30)}); err != nil {
		t.Fatalf("write dominant summary: %v", err)
	}
}

type frequency struct {
	PlaceID string `json:"place_id"`
	Name    string `json:"name"`
	Count   int    `json:"count"`
}

func dominantPlaces(counts map[string]int, limit int) []frequency {
	items := make([]frequency, 0, len(counts))
	for k, n := range counts {
		parts := strings.SplitN(k, "\t", 2)
		name := ""
		if len(parts) == 2 {
			name = parts[1]
		}
		items = append(items, frequency{PlaceID: parts[0], Name: name, Count: n})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		if items[i].PlaceID != items[j].PlaceID {
			return items[i].PlaceID < items[j].PlaceID
		}
		return items[i].Name < items[j].Name
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}
