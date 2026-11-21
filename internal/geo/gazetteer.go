// Package geo holds the production gazetteer, Photon fallback and the small
// spatial helpers used by the plotter and clusterer.
//
// The gazetteer is loaded from GeoNames files cached under DOR_GEONAMES_DIR.
// Missing files are downloaded once; a failed download is not fatal: callers
// get an error and the agents fall back to dateline, country and hazard hints.
package geo

import (
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	citiesURL  = "https://download.geonames.org/export/dump/cities15000.zip"
	countryURL = "https://download.geonames.org/export/dump/countryInfo.txt"
	admin1URL  = "https://download.geonames.org/export/dump/admin1CodesASCII.txt"
	// DefaultDir is the repository-local GeoNames cache.
	DefaultDir = "./.dor/geonames"
)

// Place is one gazetteer entry. Score is populated by Candidates and is not
// persisted.
type Place struct {
	ID          string
	Name        string
	Normalized  string
	Admin       string
	Country     string
	CountryCode string
	Lat         float64
	Lon         float64
	Population  int64
	Feature     string
	CountryID   bool
	Score       float64
}

// Country is the country index entry used for country-centroid fallbacks.
type Country struct {
	Code    string
	Name    string
	Capital string
	Place   Place
}

// Gazetteer is an in-memory name index. It is safe for concurrent reads after
// Load returns.
type Gazetteer struct {
	byName      map[string][]Place
	skip        map[string]struct{}
	countries   map[string]Country
	byCountry   map[string][]Place
	loaded      bool
	loadedNames int
}

// Load downloads (once) and indexes the GeoNames files under dir. The context
// bounds both the downloads and the file reads.
func Load(ctx context.Context, dir string, log *slog.Logger) (*Gazetteer, error) {
	if dir == "" {
		dir = DefaultDir
	}
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("geo: create cache %s: %w", dir, err)
	}
	citiesPath, err := ensureCities(ctx, dir)
	if err != nil {
		return nil, err
	}
	countryPath, err := ensureFile(ctx, dir, "countryInfo.txt", countryURL)
	if err != nil {
		return nil, err
	}
	admin1Path, err := ensureFile(ctx, dir, "admin1CodesASCII.txt", admin1URL)
	if err != nil {
		return nil, err
	}

	countries, err := readCountries(countryPath)
	if err != nil {
		return nil, err
	}
	admin1, err := readAdmin1(admin1Path)
	if err != nil {
		return nil, err
	}
	places, err := readCities(citiesPath, countries, admin1)
	if err != nil {
		return nil, err
	}
	g := &Gazetteer{
		byName:    make(map[string][]Place, len(places)*2),
		skip:      ambiguousNames(),
		countries: make(map[string]Country, len(countries)),
		byCountry: make(map[string][]Place, len(countries)),
		loaded:    true,
	}
	for _, p := range places {
		g.add(p)
	}
	for code, c := range countries {
		countryPlace := Place{
			ID:          "country:" + code,
			Name:        c.Name,
			Normalized:  normalize(c.Name),
			Country:     c.Name,
			CountryCode: code,
			CountryID:   true,
			Feature:     "PCLI",
		}
		// The capital city is the country centroid fallback. A countryInfo
		// capital is a name, not a coordinate; resolve it from the city list.
		if capPlace, ok := g.capitalPlace(code, c.Capital); ok {
			countryPlace.Lat, countryPlace.Lon = capPlace.Lat, capPlace.Lon
		}
		g.countries[code] = Country{Code: code, Name: c.Name, Capital: c.Capital, Place: countryPlace}
		g.add(countryPlace)
	}
	// Add country and admin names as aliases after cities exist, so a country
	// candidate can carry its capital coordinates.
	for code, c := range countries {
		cp := g.countries[code].Place
		g.addAlias(c.Name, cp)
		cp.ID = "country:" + code
		g.byCountry[code] = append(g.byCountry[code], cp)
	}
	for key, name := range admin1 {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			continue
		}
		code := parts[0]
		if cp, ok := g.countries[code]; ok {
			alias := Place{
				ID:          "admin:" + key,
				Name:        name,
				Normalized:  normalize(name),
				Admin:       name,
				Country:     cp.Name,
				CountryCode: code,
				Feature:     "ADM1",
			}
			g.addAlias(name, alias)
		}
	}
	g.loadedNames = len(g.byName)
	log.Info("gazetteer loaded", "places", len(places), "names", g.loadedNames, "countries", len(g.countries), "dir", dir)
	return g, nil
}

// NewEmpty is a safe no-data gazetteer. Every lookup returns no result; the
// plotter then uses dateline/country/hazard hints and marks the fallback.
func NewEmpty() *Gazetteer {
	return &Gazetteer{
		byName:    map[string][]Place{},
		skip:      map[string]struct{}{},
		countries: map[string]Country{},
		byCountry: map[string][]Place{},
	}
}

// Loaded reports whether GeoNames data was indexed.
func (g *Gazetteer) Loaded() bool { return g != nil && g.loaded }

func (g *Gazetteer) add(p Place) {
	if g == nil || len(p.Normalized) < 3 || p.Normalized == "" {
		return
	}
	if _, bad := g.skip[p.Normalized]; bad {
		return
	}
	g.byName[p.Normalized] = append(g.byName[p.Normalized], p)
	if p.CountryCode != "" && !p.CountryID {
		g.byCountry[p.CountryCode] = append(g.byCountry[p.CountryCode], p)
	}
}

func (g *Gazetteer) addAlias(name string, p Place) {
	if g == nil {
		return
	}
	n := normalize(name)
	if len(n) < 3 {
		return
	}
	p.Normalized = n
	if _, bad := g.skip[n]; bad {
		return
	}
	g.byName[n] = append(g.byName[n], p)
}

func (g *Gazetteer) capitalPlace(code, capital string) (Place, bool) {
	want := normalize(capital)
	best := Place{}
	found := false
	for _, p := range g.byCountry[code] {
		if p.CountryID {
			continue
		}
		if p.Feature == "PPLC" || (want != "" && p.Normalized == want) {
			if !found || p.Population > best.Population {
				best, found = p, true
			}
		}
	}
	return best, found
}

// Candidates returns up to max places mentioned in title/lede/dateline,
// ranked with the title and dateline as priors. countryHint is an ISO code or
// English country name; when it matches candidates, non-matching candidates
// are dropped so "Paris" resolves to Paris, France.
func (g *Gazetteer) Candidates(text, title, dateline, countryHint string, max int) []Place {
	if g == nil || !g.loaded || len(g.byName) == 0 {
		return nil
	}
	if max <= 0 {
		max = 8
	}
	text = strings.TrimSpace(text + " " + title + " " + dateline)
	words := tokenize(text)
	if len(words) == 0 {
		return nil
	}
	titleSet := wordSet(tokenize(title))
	dateSet := wordSet(tokenize(dateline))
	hintCode := g.CountryCode(countryHint)
	hintName := normalize(countryHint)

	scores := make(map[string]float64)
	found := make(map[string]Place)
	for n := 3; n >= 1; n-- {
		for i := 0; i+n <= len(words); i++ {
			if !capitalized(words[i].raw) {
				continue
			}
			parts := make([]string, n)
			for k := 0; k < n; k++ {
				parts[k] = words[i+k].norm
			}
			phrase := strings.Join(parts, " ")
			list, ok := g.byName[phrase]
			if !ok {
				continue
			}
			for _, p := range list {
				score := math.Log10(float64(p.Population) + 10)
				if _, ok := titleSet[phrase]; ok {
					score += 6
				}
				if _, ok := dateSet[phrase]; ok {
					score += 8
				}
				score += float64(n) * 2
				if p.CountryID {
					score -= 2
				}
				if hintCode != "" && p.CountryCode == hintCode {
					score += 7
				} else if hintName != "" && normalize(p.Country) == hintName {
					score += 6
				}
				if prev, ok := scores[p.ID]; !ok || score > prev {
					scores[p.ID] = score
					p.Score = score
					found[p.ID] = p
				}
			}
		}
	}
	if len(found) == 0 {
		return nil
	}

	list := make([]Place, 0, len(found))
	for _, p := range found {
		list = append(list, p)
	}
	if hintCode != "" {
		scoped := make([]Place, 0, len(list))
		for _, p := range list {
			if p.CountryCode == hintCode || (p.CountryID && p.CountryCode == hintCode) {
				scoped = append(scoped, p)
			}
		}
		if len(scoped) > 0 {
			list = scoped
		}
	}
	// One candidate per (name, country); a country candidate is redundant when
	// a city with the same name is present.
	byKey := make(map[string]Place, len(list))
	for _, p := range list {
		key := p.Normalized + "|" + p.CountryCode
		if prev, ok := byKey[key]; !ok || p.Score > prev.Score || (p.Score == prev.Score && !p.CountryID) {
			byKey[key] = p
		}
	}
	list = list[:0]
	for _, p := range byKey {
		list = append(list, p)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].CountryID != list[j].CountryID && list[i].Score == list[j].Score {
			return !list[i].CountryID
		}
		if list[i].Score != list[j].Score {
			return list[i].Score > list[j].Score
		}
		return list[i].Population > list[j].Population
	})
	out := list
	if len(out) > max {
		out = out[:max]
	}
	return out
}

// Lookup resolves a name from Claude/Photon to the best gazetteer entry.
func (g *Gazetteer) Lookup(name, country string) (Place, bool) {
	if g == nil || !g.loaded {
		return Place{}, false
	}
	n := normalize(name)
	if n == "" {
		return Place{}, false
	}
	list := g.byName[n]
	if len(list) == 0 {
		return Place{}, false
	}
	code := g.CountryCode(country)
	countryName := normalize(country)
	best := Place{}
	found := false
	for _, p := range list {
		if code != "" && p.CountryCode != code {
			continue
		}
		if code == "" && countryName != "" && normalize(p.Country) != countryName {
			continue
		}
		if !found || p.Population > best.Population || (p.CountryID == false && best.CountryID) {
			best, found = p, true
		}
	}
	if !found {
		return Place{}, false
	}
	return best, true
}

// Country resolves an ISO code or English country name.
func (g *Gazetteer) Country(codeOrName string) (Country, bool) {
	if g == nil {
		return Country{}, false
	}
	key := strings.ToUpper(strings.TrimSpace(codeOrName))
	if c, ok := g.countries[key]; ok {
		return c, true
	}
	want := normalize(codeOrName)
	for _, c := range g.countries {
		if normalize(c.Name) == want {
			return c, true
		}
	}
	return Country{}, false
}

// CountryCode returns an ISO 3166-1 alpha-2 code for a code or name.
func (g *Gazetteer) CountryCode(codeOrName string) string {
	if g == nil {
		return ""
	}
	if len(strings.TrimSpace(codeOrName)) == 2 {
		code := strings.ToUpper(strings.TrimSpace(codeOrName))
		if _, ok := g.countries[code]; ok {
			return code
		}
	}
	if c, ok := g.Country(codeOrName); ok {
		return c.Code
	}
	return ""
}

// CountryCentroid returns the country-place fallback.
func (g *Gazetteer) CountryCentroid(codeOrName string) (Place, bool) {
	c, ok := g.Country(codeOrName)
	if !ok {
		return Place{}, false
	}
	return c.Place, true
}

// Cell is a coarse grid key used only for in-memory candidate filtering. It
// is deliberately not a geohash: H3 resolution is a frontend concern.
func Cell(lat, lon float64) string {
	return fmt.Sprintf("%.1f_%.1f", math.Floor(lat*10)/10, math.Floor(lon*10)/10)
}

// DistanceKM is the haversine distance.
func DistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	const rad = math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 6371 * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// Cosine is the cosine similarity of two vectors; a zero vector or unequal
// lengths return 0.
func Cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// --- loading -----------------------------------------------------------------

type rawCountry struct {
	Code    string
	Name    string
	Capital string
}

func ensureCities(ctx context.Context, dir string) (string, error) {
	out := filepath.Join(dir, "cities15000.txt")
	if _, err := os.Stat(out); err == nil {
		return out, nil
	}
	zipPath := filepath.Join(dir, "cities15000.zip")
	if err := download(ctx, zipPath, citiesURL); err != nil {
		return "", err
	}
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("geo: open cities archive: %w", err)
	}
	defer zr.Close()
	for _, f := range zr.File {
		if f.Name != "cities15000.txt" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return "", err
		}
		tmp := out + ".tmp"
		dst, err := os.Create(tmp)
		if err != nil {
			rc.Close()
			return "", err
		}
		if _, err := io.Copy(dst, rc); err != nil {
			dst.Close()
			rc.Close()
			return "", err
		}
		dst.Close()
		rc.Close()
		if err := os.Rename(tmp, out); err != nil {
			return "", err
		}
		return out, nil
	}
	return "", fmt.Errorf("geo: cities15000.txt missing from archive")
}

func ensureFile(ctx context.Context, dir, name, url string) (string, error) {
	out := filepath.Join(dir, name)
	if _, err := os.Stat(out); err == nil {
		return out, nil
	}
	if err := download(ctx, out, url); err != nil {
		return "", err
	}
	return out, nil
}

func download(ctx context.Context, path, url string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "dor-geo/0.1 (+https://github.com/ankojh/dor)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("geo: download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("geo: download %s: HTTP %d", url, resp.StatusCode)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readCountries(path string) (map[string]rawCountry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make(map[string]rawCountry)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 5 {
			continue
		}
		out[fields[0]] = rawCountry{Code: fields[0], Name: fields[4], Capital: fields[5]}
	}
	return out, sc.Err()
}

func readAdmin1(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make(map[string]string)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 2 {
			continue
		}
		out[fields[0]] = fields[1]
	}
	return out, sc.Err()
}

func readCities(path string, countries map[string]rawCountry, admin1 map[string]string) ([]Place, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Place
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 15 {
			continue
		}
		lat, _ := strconv.ParseFloat(fields[4], 64)
		lon, _ := strconv.ParseFloat(fields[5], 64)
		pop, _ := strconv.ParseInt(fields[14], 10, 64)
		cc := fields[8]
		adm := admin1[cc+"."+fields[10]]
		if adm == "" {
			adm = fields[10]
		}
		name := fields[1]
		p := Place{
			ID:          fields[0],
			Name:        name,
			Normalized:  normalize(name),
			Admin:       adm,
			Country:     countries[cc].Name,
			CountryCode: cc,
			Lat:         lat,
			Lon:         lon,
			Population:  pop,
			Feature:     fields[7],
		}
		out = append(out, p)
		// Alternate names are free recall for transliterations. Cap the list
		// per place so the index stays small.
		if fields[3] != "" {
			aliases := strings.Split(fields[3], ",")
			for i, alias := range aliases {
				if i >= 12 {
					break
				}
				if len(normalize(alias)) >= 3 {
					out = append(out, Place{
						ID: p.ID, Name: p.Name, Normalized: normalize(alias),
						Admin: p.Admin, Country: p.Country, CountryCode: p.CountryCode,
						Lat: p.Lat, Lon: p.Lon, Population: p.Population, Feature: p.Feature,
					})
				}
			}
		}
	}
	return out, sc.Err()
}

func normalize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return r
		}
		return ' '
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

type token struct {
	norm string
	raw  string
}

func tokenize(s string) []token {
	var out []token
	var cur []rune
	flush := func() {
		if len(cur) == 0 {
			return
		}
		raw := string(cur)
		if n := normalize(raw); n != "" {
			out = append(out, token{norm: n, raw: raw})
		}
		cur = cur[:0]
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '\'' || r == '’' {
			cur = append(cur, r)
			continue
		}
		flush()
	}
	flush()
	return out
}

func wordSet(words []token) map[string]struct{} {
	out := make(map[string]struct{}, len(words))
	for _, w := range words {
		out[w.norm] = struct{}{}
	}
	return out
}

func capitalized(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) {
			return unicode.IsUpper(r)
		}
		if unicode.IsDigit(r) {
			return false
		}
	}
	return false
}

func ambiguousNames() map[string]struct{} {
	names := []string{
		"mobile", "nice", "reading", "bath", "split", "orange", "industry",
		"commerce", "general", "independence", "liberty", "union", "salem",
		"decatur", "north", "south", "east", "west", "central", "national",
		"federal", "government", "president", "minister", "court", "bank",
		"market", "school", "university", "airport", "station", "park", "lake",
		"river", "hill", "beach", "port", "new", "hope", "victory", "trinity",
	}
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	// These are real places but also very common words; they stay in the index.
	for _, n := range []string{"paris", "london", "berlin", "rome", "athens", "victoria"} {
		delete(out, n)
	}
	return out
}
