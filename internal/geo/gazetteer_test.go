package geo

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func writeGeoFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// geoNamesFixture writes the three GeoNames files in their real column order.
const (
	fixtureCountries = "# comment\n" +
		"GR\tGRC\t300\tGR\tGreece\tAthens\n" +
		"FR\tFRA\t250\tFR\tFrance\tParis\n" +
		"US\tUSA\t840\tUS\tUnited States\tWashington\n"
	fixtureAdmin1 = "GR.I5\tAttica\n" + "FR.IDF\tIle-de-France\n"
	fixtureCities = "1\tAthens\tAthens\tAthina\t37.98\t23.72\tP\tPPLC\tGR\t\tI5\t\t\t\t3000000\t\t\t\n" +
		"2\tParis\tParis\t\t48.86\t2.35\tP\tPPLC\tFR\t\tIDF\t\t\t\t2100000\t\t\t\n" +
		"3\tSpringfield\tSpringfield\t\t39.80\t-89.64\tP\tPPLA2\tUS\t\tIL\t\t\t\t115000\t\t\t\n" +
		"4\tMobile\tMobile\t\t30.69\t-88.04\tP\tPPLA2\tUS\t\tAL\t\t\t\t187000\t\t\t\n"
)

// geoIndexFile is one small GeoNames dataset shared by the index tests.
type geoIndexFile struct {
	countries map[string]rawCountry
	admin1    map[string]string
	places    []Place
}

func loadGeoFixture(t *testing.T) geoIndexFile {
	t.Helper()
	countriesPath := writeGeoFile(t, "countryInfo.txt", fixtureCountries)
	adminPath := writeGeoFile(t, "admin1CodesASCII.txt", fixtureAdmin1)
	citiesPath := writeGeoFile(t, "cities15000.txt", fixtureCities)

	countries, err := readCountries(countriesPath)
	if err != nil {
		t.Fatalf("readCountries: %v", err)
	}
	admin1, err := readAdmin1(adminPath)
	if err != nil {
		t.Fatalf("readAdmin1: %v", err)
	}
	places, err := readCities(citiesPath, countries, admin1)
	if err != nil {
		t.Fatalf("readCities: %v", err)
	}
	return geoIndexFile{countries: countries, admin1: admin1, places: places}
}

// buildFixtureIndex mirrors Load's indexing without touching the network.
func buildFixtureIndex(t *testing.T) *Gazetteer {
	t.Helper()
	f := loadGeoFixture(t)
	g := NewEmpty()
	g.loaded = true
	g.skip = ambiguousNames()
	g.countries = make(map[string]Country, len(f.countries))
	g.byCountry = make(map[string][]Place, len(f.countries))
	for _, p := range f.places {
		g.add(p)
	}
	for code, c := range f.countries {
		countryPlace := Place{
			ID:          "country:" + code,
			Name:        c.Name,
			Normalized:  normalize(c.Name),
			Country:     c.Name,
			CountryCode: code,
			CountryID:   true,
			Feature:     "PCLI",
		}
		if capPlace, ok := g.capitalPlace(code, c.Capital); ok {
			countryPlace.Lat, countryPlace.Lon = capPlace.Lat, capPlace.Lon
		}
		g.countries[code] = Country{Code: code, ISO3: c.ISO3, Name: c.Name, Capital: c.Capital, Place: countryPlace}
		g.add(countryPlace)
	}
	for code, c := range f.countries {
		cp := g.countries[code].Place
		g.addAlias(c.Name, cp)
		cp.ID = "country:" + code
		g.byCountry[code] = append(g.byCountry[code], cp)
	}
	for key, name := range f.admin1 {
		parts := splitAdminKey(key)
		if len(parts) != 2 {
			continue
		}
		if cp, ok := g.countries[parts[0]]; ok {
			g.addAlias(name, Place{
				ID:          "admin:" + key,
				Name:        name,
				Normalized:  normalize(name),
				Admin:       name,
				Country:     cp.Name,
				CountryCode: parts[0],
				Feature:     "ADM1",
			})
		}
	}
	g.buildSearchIndex()
	return g
}

func splitAdminKey(key string) []string {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return []string{key}
}

func TestReadGeoNamesFiles(t *testing.T) {
	f := loadGeoFixture(t)
	if got := f.countries["GR"]; got.Name != "Greece" || got.Capital != "Athens" || got.ISO3 != "GRC" {
		t.Fatalf("Greece = %+v", got)
	}
	if got := f.admin1["GR.I5"]; got != "Attica" {
		t.Fatalf("admin1 GR.I5 = %q", got)
	}
	byID := map[string]Place{}
	for _, p := range f.places {
		byID[p.ID+"|"+p.Normalized] = p
	}
	athens := byID["1|athens"]
	if athens.Country != "Greece" || athens.CountryCode != "GR" || athens.Admin != "Attica" || athens.Population != 3000000 {
		t.Fatalf("Athens = %+v", athens)
	}
	if athens.Lat != 37.98 || athens.Lon != 23.72 || athens.Feature != "PPLC" {
		t.Fatalf("Athens coordinates/feature = %+v", athens)
	}
	if _, ok := byID["1|athina"]; !ok {
		t.Fatal("alternate name Athina was not indexed as a place alias")
	}
}

func TestSearchRanksExactAndBoundsLimit(t *testing.T) {
	g := buildFixtureIndex(t)

	got := g.Search("athens", 8)
	if len(got) == 0 || got[0].ID != "1" {
		t.Fatalf("Search(athens) = %+v, want Athens first", got)
	}
	if got := g.Search("ath", 1); len(got) != 1 {
		t.Fatalf("Search with limit 1 returned %d rows", len(got))
	}
	if got := g.Search("paris", 8); len(got) == 0 || got[0].ID != "2" {
		t.Fatalf("Search(paris) = %+v, want Paris first", got)
	}
	if got := g.Search("zzzznotaplace", 8); len(got) != 0 {
		t.Fatalf("unmatched search returned %+v", got)
	}
	if got := g.Search("", 3); len(got) > 3 {
		t.Fatalf("empty search ignored limit: %d rows", len(got))
	}
	if got := g.Search("athens", 0); len(got) > 8 {
		t.Fatalf("zero limit did not apply the default bound: %d rows", len(got))
	}
}

func TestSearchSuppressesAmbiguousNames(t *testing.T) {
	g := buildFixtureIndex(t)
	for _, got := range g.Search("mobile", 8) {
		if got.Normalized == "mobile" {
			t.Fatalf("ambiguous name was not suppressed: %+v", got)
		}
	}
	if _, ok := g.Lookup("Mobile", ""); ok {
		t.Fatal("Lookup found an ambiguous name that add() should have skipped")
	}
}

func TestLookupExactAndAlias(t *testing.T) {
	g := buildFixtureIndex(t)

	if got, ok := g.Lookup("Athens", "Greece"); !ok || got.ID != "1" {
		t.Fatalf("Lookup(Athens, Greece) = %+v, %v", got, ok)
	}
	if got, ok := g.Lookup("Athina", "GR"); !ok || got.ID != "1" {
		t.Fatalf("Lookup alias Athina = %+v, %v", got, ok)
	}
	if _, ok := g.Lookup("Athens", "FR"); ok {
		t.Fatal("Lookup returned a Greek city for a French country hint")
	}
	if _, ok := g.Lookup("", ""); ok {
		t.Fatal("Lookup accepted an empty name")
	}
	if _, ok := g.Lookup("Atlantis", ""); ok {
		t.Fatal("Lookup found a name that is not indexed")
	}
}

func TestCandidatesRankLimitAndCountryHint(t *testing.T) {
	g := buildFixtureIndex(t)

	got := g.Candidates("Flooding near Athens and Paris", "Athens floods", "", "", 8)
	if len(got) == 0 {
		t.Fatal("Candidates found nothing")
	}
	if got[0].ID != "1" {
		t.Fatalf("Candidates ranked %+v first, want Athens (title prior + population)", got[0])
	}
	if got := g.Candidates("Athens and Paris", "", "", "FR", 8); len(got) != 1 || got[0].ID != "2" {
		t.Fatalf("country-hinted Candidates = %+v, want only Paris", got)
	}
	if got := g.Candidates("Athens and Paris", "", "", "France", 1); len(got) != 1 {
		t.Fatalf("Candidates ignored max: %+v", got)
	}
	if got := g.Candidates("nothing capitalized here", "", "", "", 8); len(got) != 0 {
		t.Fatalf("Candidates matched lowercase prose: %+v", got)
	}
	if got := g.Candidates("Athens", "", "", "", 8); len(got) == 0 {
		t.Fatal("Candidates missed an exact name")
	}
}

func TestFallbackIsBounded(t *testing.T) {
	g := buildFixtureIndex(t)

	got := g.Fallback(3)
	if len(got) == 0 || len(got) > 3 {
		t.Fatalf("Fallback(3) returned %d rows", len(got))
	}
	for _, p := range got {
		if p.ID == "" {
			t.Fatalf("fallback row without an ID: %+v", p)
		}
	}
	if got := g.Fallback(0); len(got) == 0 || len(got) > 8 {
		t.Fatalf("Fallback(0) returned %d rows", len(got))
	}
}

func TestCountryLookups(t *testing.T) {
	g := buildFixtureIndex(t)

	c, ok := g.Country("GR")
	if !ok || c.Name != "Greece" || c.Capital != "Athens" || c.ISO3 != "GRC" {
		t.Fatalf("Country(GR) = %+v, %v", c, ok)
	}
	if c, ok := g.Country("greece"); !ok || c.Code != "GR" {
		t.Fatalf("Country(greece) = %+v, %v", c, ok)
	}
	if _, ok := g.Country("XX"); ok {
		t.Fatal("Country accepted an unknown code")
	}

	for input, want := range map[string]string{
		"GR": "GR", "grc": "GR", "Greece": "GR", "FR": "FR", "france": "FR",
	} {
		if got := g.CountryCode(input); got != want {
			t.Errorf("CountryCode(%q) = %q, want %q", input, got, want)
		}
	}
	if got := g.CountryCode("nowhere"); got != "" {
		t.Errorf("CountryCode(nowhere) = %q", got)
	}

	centroid, ok := g.CountryCentroid("Greece")
	if !ok || centroid.ID != "country:GR" || centroid.Lat != 37.98 || centroid.Lon != 23.72 {
		t.Fatalf("CountryCentroid(Greece) = %+v, %v", centroid, ok)
	}
	if _, ok := g.CountryCentroid("XX"); ok {
		t.Fatal("CountryCentroid accepted an unknown country")
	}
}

func TestEmptyGazetteerIsSafe(t *testing.T) {
	g := NewEmpty()
	if g.Loaded() {
		t.Fatal("NewEmpty reports loaded")
	}
	if got := g.Search("athens", 8); got != nil {
		t.Fatalf("empty Search = %+v", got)
	}
	if got := g.Candidates("Athens", "", "", "", 8); got != nil {
		t.Fatalf("empty Candidates = %+v", got)
	}
	if got := g.Fallback(8); got != nil {
		t.Fatalf("empty Fallback = %+v", got)
	}
	if _, ok := g.Lookup("Athens", ""); ok {
		t.Fatal("empty Lookup found a place")
	}
}

func TestCellBuckets(t *testing.T) {
	if got, want := Cell(37.98, 23.72), "37.9_23.7"; got != want {
		t.Fatalf("Cell(37.98, 23.72) = %q, want %q", got, want)
	}
	if got, want := Cell(-0.05, 10), "-0.1_10.0"; got != want {
		t.Fatalf("Cell(-0.05, 10) = %q, want %q", got, want)
	}
}

func TestDistanceKM(t *testing.T) {
	if got := DistanceKM(37.98, 23.72, 37.98, 23.72); got != 0 {
		t.Fatalf("same point distance = %v", got)
	}
	// One degree of latitude is about 111.19 km.
	if got := DistanceKM(0, 0, 1, 0); math.Abs(got-111.19) > 0.5 {
		t.Fatalf("one degree latitude = %v km, want ~111.19", got)
	}
	athensToParis := DistanceKM(37.98, 23.72, 48.86, 2.35)
	if athensToParis < 2000 || athensToParis > 2500 {
		t.Fatalf("Athens-Paris = %v km, want ~2100", athensToParis)
	}
}

func TestCosine(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float64
	}{
		{name: "identical", a: []float32{1, 2, 3}, b: []float32{1, 2, 3}, want: 1},
		{name: "orthogonal", a: []float32{1, 0}, b: []float32{0, 1}, want: 0},
		{name: "opposite", a: []float32{1, 0}, b: []float32{-1, 0}, want: -1},
		{name: "zero vector", a: []float32{0, 0}, b: []float32{1, 1}, want: 0},
		{name: "empty", a: nil, b: nil, want: 0},
		{name: "wrong lengths", a: []float32{1, 2}, b: []float32{1, 2, 3}, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Cosine(tc.a, tc.b); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("Cosine = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNormalizeAndTokenize(t *testing.T) {
	tests := map[string]string{
		"  New-York, NY ": "new york ny",
		"St. John's":      "st john s",
		"Café\tDéjà":      "café déjà",
		"   ":             "",
	}
	for input, want := range tests {
		if got := normalize(input); got != want {
			t.Errorf("normalize(%q) = %q, want %q", input, got, want)
		}
	}

	tokens := tokenize("Athens, Greece — 2024")
	if len(tokens) != 3 {
		t.Fatalf("tokenize returned %d tokens: %+v", len(tokens), tokens)
	}
	if tokens[0].norm != "athens" || tokens[0].raw != "Athens" {
		t.Errorf("first token = %+v", tokens[0])
	}
	if tokens[2].norm != "2024" {
		t.Errorf("digit token = %+v", tokens[2])
	}
	if got := tokenize("   "); len(got) != 0 {
		t.Fatalf("blank tokenize = %+v", got)
	}
}

func TestCapitalized(t *testing.T) {
	tests := map[string]bool{
		"Athens": true,
		"athens": false,
		"123":    false,
		"—X":     true,
		"":       false,
	}
	for input, want := range tests {
		if got := capitalized(input); got != want {
			t.Errorf("capitalized(%q) = %v, want %v", input, got, want)
		}
	}
}
