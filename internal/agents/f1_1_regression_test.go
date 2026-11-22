package agents

import (
	"context"
	"testing"

	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func f11PlaceByID(t *testing.T, g *geo.Gazetteer, id string) geo.Place {
	t.Helper()
	p, ok := g.ByID(id)
	if !ok {
		t.Fatalf("full GeoNames cache is missing required place %q", id)
	}
	return p
}

// These are the reviewer's exact false-positive probes. Each phrase uses a
// capitalized common word as if it were a headline place; none has any other
// geographic evidence, so the honest result is no dot at all.
func TestF11CommonWordFalsePositivesHaveNoDot(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	for _, tc := range []struct{ name, title string }{
		{"state_duma", "State Duma approves a new law"},
		{"lira", "Lira falls against the dollar"},
		{"tyre", "Tyre prices rise across Europe"},
		{"queen_elizabeth", "Queen Elizabeth II remembered"},
		{"oral", "Oral arguments begin at the Supreme Court"},
		{"gold_mine", "Gold Mine Open returns this weekend"},
		{"biodiversity_alliance", "Biodiversity Alliance summit opens"},
		{"buffalo_wings", "Buffalo wings are a game-day favourite"},
		{"torrent", "Torrent stalls after the latest update"},
		{"woodland", "Woodland restoration begins along the river"},
		{"samba", "Samba schools prepare for carnival"},
		{"tamale", "Tamale recipe with peanut stew"},
		{"manta", "Manta ray glides past divers"},
		{"elk", "Elk herd crosses the highway"},
		{"orion", "Orion spacecraft launch delayed"},
		{"bend", "Bend in the road slows traffic"},
		{"providence", "Providence plays a role in the final outcome"},
		{"turbo", "Turbo engine recall affects thousands"},
		{"riverside", "Riverside homes flooded after the storm"},
		{"global_biodiversity_alliance", "Global Biodiversity Alliance summit opens"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: "f11-negative-" + tc.name, Title: tc.title})
			if len(placed.Locations) != 0 {
				t.Fatalf("%q must not produce a common-word town dot: %+v", tc.title, placed.Locations)
			}
		})
	}
}

// Ilocos Sur is a Philippine province, not the Omani town of Sur. A coarser
// Ilocos region is honest, but the unrelated Sur dot is not.
func TestF11IlocosSurDoesNotPlotSurOman(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	placed := p.place(context.Background(), &dorv1.Article{
		Id:    "f11-ilocos-sur",
		Title: "Ilocos Sur residents prepare for typhoon",
	})
	for _, loc := range placed.Locations {
		if loc.PlaceId == "286245" { // Sur, Oman
			t.Fatalf("Ilocos Sur must not plot Sur, Oman: %+v", loc)
		}
		if place, ok := g.ByID(loc.PlaceId); ok && normalizePlotName(place.Name) == "sur" {
			t.Fatalf("Ilocos Sur must not plot a Sur namesake: %+v", loc)
		}
	}
}

// A comma list names both cities. The first city must not disappear merely
// because the plotter chose a single deterministic primary.
func TestF11CommaSeparatedCitiesAreBothPlotted(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	for _, tc := range []struct {
		name, title string
		ids         []string
	}{
		{"india", "Mumbai, Delhi report record rainfall", []string{"1275339", "1273294"}},     // Mumbai, Delhi
		{"nigeria", "Lagos, Abuja report record heat", []string{"2332459", "2352778"}},        // Lagos, Abuja
		{"east_india", "Kolkata, Chennai report record heat", []string{"1275004", "1264527"}}, // Kolkata, Chennai
		{"africa_asia", "Jakarta, Nairobi report record heat", []string{"1642911", "184745"}}, // Jakarta, Nairobi
	} {
		t.Run(tc.name, func(t *testing.T) {
			placed := p.place(context.Background(), &dorv1.Article{Id: "f11-comma-" + tc.name, Title: tc.title})
			counts := map[string]int{}
			for _, loc := range placed.Locations {
				counts[loc.PlaceId]++
				if loc.Precision != dorv1.Precision_PRECISION_CITY {
					t.Fatalf("listed city %s should be at city precision, got %+v", loc.PlaceId, loc)
				}
			}
			for _, id := range tc.ids {
				if counts[id] != 1 {
					t.Fatalf("comma list must plot %s exactly once, got %d in %+v", id, counts[id], placed.Locations)
				}
			}
			if len(placed.Locations) != len(tc.ids) {
				t.Fatalf("a %d-city list must produce %d locations, got %+v", len(tc.ids), len(tc.ids), placed.Locations)
			}
		})
	}
}

// A bare NEW YORK dateline is the city edition. It must not become the New
// York State ADM1, even though the state row is also named New York.
func TestF11ShortNewYorkDatelinePlotsNewYorkCity(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	nyc := f11PlaceByID(t, g, "5128581")
	state := f11PlaceByID(t, g, "admin:US.NY")
	if state.Feature != "ADM1" {
		t.Fatalf("fixture assumption: admin:US.NY should be the New York ADM1, got %+v", state)
	}

	placed := p.place(context.Background(), &dorv1.Article{
		Id:       "f11-new-york-dateline",
		Title:    "Storm warning issued",
		Dateline: "NEW YORK",
	})
	foundCity := false
	for _, loc := range placed.Locations {
		if loc.PlaceId == state.ID {
			t.Fatalf("dateline NEW YORK must not plot New York State: %+v", loc)
		}
		if loc.PlaceId == nyc.ID {
			foundCity = true
			if loc.Precision != dorv1.Precision_PRECISION_CITY {
				t.Fatalf("New York City should be at city precision, got %+v", loc)
			}
		}
	}
	if !foundCity {
		t.Fatalf("dateline NEW YORK should plot New York City (%s), got %+v", nyc.ID, placed.Locations)
	}
}

// Large PPL cities are real population centres even when GeoNames does not
// give them an administrative-seat feature code. Their named recall must not
// depend on a capital-letter-only seat path or a million-person cutoff.
func TestF11LargePopulationPPLCityRecall(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	for _, tc := range []struct{ name, id, title string }{
		{"noida", "7279746", "Noida announces a new policy"},
		{"cirebon", "1646170", "Cirebon announces a new policy"},
		{"guntur", "1270668", "Guntur announces a new policy"},
		{"pune", "1259229", "Pune announces a new policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := f11PlaceByID(t, g, tc.id)
			if want.Feature != "PPL" || want.Population < 250000 {
				t.Fatalf("fixture assumption: %s should be a PPL >= 250k, got %+v", tc.id, want)
			}
			placed := p.place(context.Background(), &dorv1.Article{Id: "f11-ppl-" + tc.name, Title: tc.title})
			found := false
			for _, loc := range placed.Locations {
				if loc.PlaceId != tc.id {
					continue
				}
				found = true
				if loc.Precision != dorv1.Precision_PRECISION_CITY {
					t.Fatalf("%q should resolve at city precision, got %+v", tc.title, loc)
				}
			}
			if !found {
				t.Fatalf("%q should plot %s at city precision, got %+v", tc.title, want.Name, placed.Locations)
			}
		})
	}
}

// A repair pass must not remove a valid Mississippi ADM1 row merely because
// the headline says "county of Mississippi" instead of "in Mississippi".
func TestF11RepairRetainsADM1ForCountyOfRegion(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	state := f11PlaceByID(t, g, "admin:US.MS")
	if state.Feature != "ADM1" || !geo.ValidCoordinates(state.Lat, state.Lon) {
		t.Fatalf("fixture assumption: admin:US.MS should be a valid ADM1, got %+v", state)
	}
	title := "Town hall in reddest county of Mississippi"
	candidates := g.Candidates("", title, "", "", 8)
	found := false
	for _, c := range candidates {
		if c.ID == state.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("%q should admit the Mississippi ADM1, got %+v", title, candidates)
	}
	decision := g.RepairLocation(geo.StoredLocation{
		PlaceID: state.ID, Lat: float64(float32(state.Lat)), Lon: float64(float32(state.Lon)), Precision: geo.PrecisionOf(state),
	}, []geo.TextEvidence{{Title: title}})
	if decision.Reason != "" || decision.ReviewReason != "" || decision.Replacement != nil {
		t.Fatalf("valid Mississippi ADM1 row must be retained, got %+v", decision)
	}

	// The same region name in an organisation or natural-feature construction
	// is not jurisdiction evidence and must not become the state dot.
	for _, negative := range []string{
		"University of Mississippi announces a new plan",
		"Mississippi River flooding continues",
	} {
		for _, c := range g.Candidates("", negative, "", "", 8) {
			if c.ID == state.ID {
				t.Fatalf("%q must not admit the Mississippi ADM1, got %+v", negative, c)
			}
		}
	}
}

// The exact handoff phrase "headquarters, Abidjan, Côte d’Ivoire" must resolve
// Abidjan at city precision, not drop it as a proper-name compound or fall
// back to an unrecognised country token.
func TestF11HeadquartersAbidjanCoteDIvoire(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}
	abidjan := f11PlaceByID(t, g, "2293538")
	if abidjan.Feature != "PPLA" || !geo.ValidCoordinates(abidjan.Lat, abidjan.Lon) {
		t.Fatalf("fixture assumption: Abidjan should be a valid PPLA, got %+v", abidjan)
	}
	placed := p.place(context.Background(), &dorv1.Article{
		Id:    "f11-headquarters-abidjan",
		Title: "Development bank opens a new office",
		Lede:  "The bank's headquarters, Abidjan, Côte d’Ivoire, hosted the meeting.",
	})
	for _, loc := range placed.Locations {
		if loc.PlaceId == abidjan.ID {
			if loc.Precision != dorv1.Precision_PRECISION_CITY {
				t.Fatalf("Abidjan should be at city precision, got %+v", loc)
			}
			return
		}
	}
	t.Fatalf("'headquarters, Abidjan, Côte d’Ivoire' should plot Abidjan (%s), got %+v", abidjan.ID, placed.Locations)
}

// Repeating a place across title, lede and dateline must produce one location,
// not one per mention.
func TestF11RepeatedMentionsProduceOneLocation(t *testing.T) {
	g := loadFullCacheForPlotTests(t)
	p := &plotter{deps: Deps{Gazetteer: g}, log: testLogger()}

	boston := f11PlaceByID(t, g, "4930956")
	placed := p.place(context.Background(), &dorv1.Article{
		Id:       "f11-dedupe",
		Title:    "Boston officials spoke",
		Lede:     "Boston police later arrived at the scene.",
		Dateline: "Boston",
	})
	count := 0
	for _, loc := range placed.Locations {
		if loc.PlaceId == boston.ID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("repeated Boston mentions must yield one location, got %d in %+v", count, placed.Locations)
	}
	if len(placed.Locations) != 1 {
		t.Fatalf("the repeated place should be the only resolved location, got %+v", placed.Locations)
	}
}
