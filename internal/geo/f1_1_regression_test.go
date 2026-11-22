package geo

import "testing"

// f11Fixture models the exact F1·1 ambiguity classes with a small, offline
// dataset so the regressions run in CI without the full GeoNames cache.
func f11Fixture(t *testing.T) *Gazetteer {
	t.Helper()
	countries := countryRow("US", "USA", "United States", "Washington") +
		countryRow("GB", "GBR", "United Kingdom", "London") +
		countryRow("SY", "SYR", "Syria", "Damascus") +
		countryRow("UG", "UGA", "Uganda", "Kampala") +
		countryRow("LB", "LBN", "Lebanon", "Beirut") +
		countryRow("KZ", "KAZ", "Kazakhstan", "Astana") +
		countryRow("JP", "JPN", "Japan", "Tokyo") +
		countryRow("IN", "IND", "India", "New Delhi") +
		countryRow("ES", "ESP", "Spain", "Madrid") +
		countryRow("AO", "AGO", "Angola", "Luanda") +
		countryRow("GH", "GHA", "Ghana", "Accra") +
		countryRow("EC", "ECU", "Ecuador", "Quito") +
		countryRow("PH", "PHL", "Philippines", "Manila") +
		countryRow("OM", "OMN", "Oman", "Muscat") +
		countryRow("NG", "NGA", "Nigeria", "Abuja") +
		countryRow("KE", "KEN", "Kenya", "Nairobi") +
		countryRow("ID", "IDN", "Indonesia", "Jakarta") +
		countryRow("CI", "CIV", "Ivory Coast", "Yamoussoukro") +
		countryRow("DE", "DEU", "Germany", "Berlin") +
		countryRow("FR", "FRA", "France", "Paris") +
		countryRow("PL", "POL", "Poland", "Warsaw")
	admins := "US.NJ\tNew Jersey\nUS.OH\tOhio\nUS.NY\tNew York\nUS.MA\tMassachusetts\nUS.MS\tMississippi\n" +
		"US.CA\tCalifornia\nUS.OR\tOregon\nUS.RI\tRhode Island\nUS.CO\tColorado\n" +
		"IN.MH\tMaharashtra\nIN.DL\tDelhi\nDE.BE\tBerlin\nFR.11\tIle-de-France\nPH.01\tIlocos\nOM.04\tMuscat\n" +
		"CI.93\tAbidjan Autonomous District\n"
	places := cityRow("duma", "Duma", "", "PPLA2", "33.5718", "36.4027", "SY", "01", "111864") +
		cityRow("lira", "Lira", "", "PPLA2", "2.2499", "32.8999", "UG", "N", "119323") +
		cityRow("tyre", "Tyre", "", "PPLA2", "33.2733", "35.1939", "LB", "06", "135204") +
		cityRow("elizabeth", "Elizabeth", "", "PPLA2", "40.6640", "-74.2107", "US", "NJ", "129007") +
		cityRow("oral", "Oral", "", "PPLA", "51.2460", "51.4256", "KZ", "07", "330000") +
		cityRow("mine", "Mine", "", "PPLA2", "34.1868", "131.2162", "JP", "45", "23247") +
		cityRow("alliance", "Alliance", "", "PPL", "40.9153", "-81.1059", "US", "OH", "22055") +
		cityRow("mumbai", "Mumbai", "", "PPLA", "19.0728", "72.8826", "IN", "MH", "12691836") +
		cityRow("delhi", "Delhi", "", "PPLA", "28.6520", "77.2315", "IN", "DL", "11034555") +
		cityRow("nyc", "New York City", "New York,NYC", "PPL", "40.7143", "-74.0060", "US", "NY", "8804190") +
		cityRow("boston-us", "Boston", "", "PPLA", "42.3601", "-71.0589", "US", "MA", "653833") +
		cityRow("boston-gb", "Boston", "", "PPL", "52.9744", "-0.0264", "GB", "ENG", "45339") +
		cityRow("jackson-ms", "Jackson", "", "PPLA", "32.2988", "-90.1848", "US", "MS", "153701") +
		cityRow("buffalo", "Buffalo", "", "PPLA2", "42.8864", "-78.8784", "US", "NY", "258071") +
		cityRow("torrent", "Torrent", "", "PPLA3", "39.4333", "-0.4667", "ES", "VC", "78543") +
		cityRow("woodland", "Woodland", "", "PPLA2", "38.6785", "-121.7733", "US", "CA", "58567") +
		cityRow("samba", "Samba", "", "PPLA3", "-8.8333", "13.2333", "AO", "LU", "364986") +
		cityRow("tamale", "Tamale", "", "PPLA", "9.4008", "-0.8393", "GH", "N", "464316") +
		cityRow("manta", "Manta", "", "PPLA2", "-0.9677", "-80.7089", "EC", "M", "264281") +
		cityRow("elk", "Elk", "", "PPL", "53.6167", "22.3667", "PL", "W", "10000") +
		cityRow("orion", "Orion", "", "PPLA3", "14.6206", "120.5817", "PH", "03", "63044") +
		cityRow("bend", "Bend", "", "PPLA2", "44.0582", "-121.3153", "US", "OR", "87014") +
		cityRow("providence", "Providence", "", "PPLA", "41.8240", "-71.4128", "US", "RI", "190934") +
		cityRow("turbo", "Turbo", "", "PPLA2", "8.0928", "-76.7281", "CO", "AN", "50508") +
		cityRow("riverside", "Riverside", "", "PPLA2", "33.9533", "-117.3962", "US", "CA", "317261") +
		cityRow("san-fernando", "San Fernando", "", "PPLA", "16.6159", "120.3166", "PH", "01", "100000") +
		cityRow("sur", "Sur", "", "PPLA", "22.5667", "59.5289", "OM", "04", "71152") +
		cityRow("chennai", "Chennai", "", "PPLA", "13.0827", "80.2707", "IN", "TN", "4681087") +
		cityRow("kolkata", "Kolkata", "", "PPLA", "22.5726", "88.3639", "IN", "WB", "4631392") +
		cityRow("lagos", "Lagos", "", "PPLA2", "6.4541", "3.3947", "NG", "LA", "15388000") +
		cityRow("abuja", "Abuja", "", "PPLC", "9.0579", "7.4951", "NG", "FC", "2690000") +
		cityRow("nairobi", "Nairobi", "", "PPLC", "-1.2921", "36.8219", "KE", "30", "4397073") +
		cityRow("jakarta", "Jakarta", "", "PPLC", "-6.2146", "106.8451", "ID", "04", "8540121") +
		cityRow("abidjan", "Abidjan", "", "PPLA", "5.3600", "-4.0083", "CI", "93", "6321017")
	return writeGazetteer(t, countries, admins, places)
}

// A capitalized common word, given name or proper noun is not, by itself,
// evidence of a town with that name. Explicit geography still admits it.
func TestF11CommonWordNamesNeedGeographicEvidence(t *testing.T) {
	g := f11Fixture(t)
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
			if candidates := g.Candidates("", tc.title, "", "", 8); len(candidates) != 0 {
				t.Fatalf("%q must not turn a common word into a place dot: %+v", tc.title, candidates)
			}
		})
	}

	qualified := g.Candidates("", "Officials meet in Tyre, Lebanon", "", "", 8)
	if len(qualified) == 0 || qualified[0].ID != "tyre" {
		t.Fatalf("an explicit country qualifier must still admit Tyre: %+v", qualified)
	}

	// "Ilocos Sur" is a Philippine province, not the Omani town of Sur. The
	// coarser Ilocos region is honest; the unrelated Sur dot is not.
	for _, c := range g.Candidates("", "Ilocos Sur residents prepare for typhoon", "", "", 8) {
		if c.ID == "sur" || normalize(c.Name) == "sur" {
			t.Fatalf("Ilocos Sur must not plot Sur, Oman: %+v", c)
		}
	}
}

// A comma-separated list names two places. The separator must end a place
// span, not hide or merge the following city, and every admitted city must
// survive candidate selection.
func TestF11CommaSeparatedCitiesKeepEachCity(t *testing.T) {
	g := f11Fixture(t)
	for _, tc := range []struct {
		name, title string
		want        []string
	}{
		{"india", "Mumbai, Delhi report record rainfall", []string{"mumbai", "delhi"}},
		{"nigeria", "Lagos, Abuja report record heat", []string{"lagos", "abuja"}},
		{"east_india", "Kolkata, Chennai report record heat", []string{"kolkata", "chennai"}},
		{"africa_asia", "Jakarta, Nairobi report record heat", []string{"jakarta", "nairobi"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidates := g.Candidates("", tc.title, "", "", 8)
			byID := map[string]Place{}
			for _, c := range candidates {
				byID[c.ID] = c
			}
			for _, want := range tc.want {
				p, ok := byID[want]
				if !ok {
					t.Fatalf("comma-separated city %q was dropped from %+v", want, candidates)
				}
				if PrecisionOf(p) != CityResolution {
					t.Fatalf("city %q should stay at city precision, got %+v", want, p)
				}
			}
			if len(candidates) != len(tc.want) {
				t.Fatalf("a %d-city list must produce %d candidates, got %+v", len(tc.want), len(tc.want), candidates)
			}
		})
	}
}

// A short dateline is the city edition, not the same-named state/province.
func TestF11ShortNewYorkDatelinePrefersCity(t *testing.T) {
	g := f11Fixture(t)
	candidates := g.Candidates("", "Storm warning issued", "NEW YORK", "", 8)
	if len(candidates) != 1 || candidates[0].ID != "nyc" {
		t.Fatalf("dateline NEW YORK must resolve New York City, got %+v", candidates)
	}
	if candidates[0].Feature == "ADM1" {
		t.Fatalf("dateline NEW YORK must not resolve the New York ADM1: %+v", candidates[0])
	}
}

// "county of Mississippi" names the containing first-level region even though
// the headline does not repeat the region immediately after a locative
// preposition. The same words in an organisation name do not qualify.
func TestF11CountyOfRegionEvidence(t *testing.T) {
	g := f11Fixture(t)

	candidates := g.Candidates("", "Town hall in reddest county of Mississippi", "", "", 8)
	found := false
	for _, c := range candidates {
		if c.ID == "admin:US.MS" {
			found = true
			if PrecisionOf(c) != RegionResolution {
				t.Fatalf("Mississippi should stay at region precision, got %+v", c)
			}
		}
	}
	if !found {
		t.Fatalf("'county of Mississippi' should admit the Mississippi ADM1, got %+v", candidates)
	}

	// "of Mississippi" alone is not jurisdiction evidence, and a university
	// name is an organisation rather than a county/district container.
	for _, title := range []string{"University of Mississippi announces a new plan", "Mississippi River flooding continues"} {
		for _, c := range g.Candidates("", title, "", "", 8) {
			if c.ID == "admin:US.MS" {
				t.Fatalf("%q must not turn an organisation or natural feature into the state ADM1: %+v", title, candidates)
			}
		}
	}
}

// The handoff phrase "headquarters, Abidjan, Côte d’Ivoire" must resolve
// Abidjan even though the following country uses an unrecognised local name
// and the preceding comma-separated noun is not a locative preposition.
func TestF11HeadquartersAbidjanCoteDIvoire(t *testing.T) {
	g := f11Fixture(t)
	candidates := g.Candidates(
		"The bank's headquarters, Abidjan, Côte d’Ivoire, hosted the meeting.",
		"Development bank opens a new office", "", "", 8)
	found := false
	for _, c := range candidates {
		if c.ID == "abidjan" {
			found = true
			if PrecisionOf(c) != CityResolution {
				t.Fatalf("Abidjan should be at city precision, got %+v", c)
			}
		}
	}
	if !found {
		t.Fatalf("'headquarters, Abidjan, Côte d’Ivoire' should resolve Abidjan, got %+v", candidates)
	}
}

// Repeated mentions, aliases and a matching dateline are one place, not one
// vote per occurrence.
func TestF11RepeatedPlaceMentionsAreDeduplicated(t *testing.T) {
	g := f11Fixture(t)
	candidates := g.Candidates("Boston police arrived after Boston officials spoke.", "Boston update", "Boston", "", 8)
	count := 0
	for _, c := range candidates {
		if c.ID == "boston-us" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("repeated Boston mentions must yield exactly one candidate, got %d in %+v", count, candidates)
	}
	if len(candidates) != 1 {
		t.Fatalf("only the canonical Boston interpretation should remain, got %+v", candidates)
	}
}
