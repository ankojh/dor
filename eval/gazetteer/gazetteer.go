// Package gazetteer is the eval's minimal place matcher: GeoNames cities15000
// plus countries. It exists only so the plot evaluation has candidate lists
// without waiting for M2·1. It is not the production gazetteer, does not do
// entity recognition and does not resolve ambiguity well; M2·1 replaces it.
package gazetteer

import (
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"io"
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
	defaultDir = "eval/data/.cache/geonames"
)

// Place is one gazetteer entry.
type Place struct {
	ID         string
	Name       string
	Normalized string
	Admin      string
	Country    string
	Population int64
	CountryID  bool
}

// Gazetteer is the loaded index.
type Gazetteer struct {
	byName map[string][]Place
	// ambiguous names are common English words that would match too often.
	skip map[string]struct{}
}

// Load downloads (once) and indexes the GeoNames files under dir.
func Load(ctx context.Context, dir string) (*Gazetteer, error) {
	if dir == "" {
		dir = defaultDir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
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
	g := &Gazetteer{byName: make(map[string][]Place), skip: ambiguousNames()}
	for _, p := range places {
		if len(p.Normalized) < 3 {
			continue
		}
		if _, bad := g.skip[p.Normalized]; bad {
			continue
		}
		g.byName[p.Normalized] = append(g.byName[p.Normalized], p)
	}
	for code, name := range countries {
		n := normalize(name)
		if len(n) < 3 {
			continue
		}
		g.byName[n] = append(g.byName[n], Place{
			ID: "country:" + code, Name: name, Normalized: n, Country: name, CountryID: true,
		})
	}
	return g, nil
}

// Candidates returns up to max places mentioned in text, ranked by a match in
// the title first, then by population. A name only counts when it is
// capitalised in the text: GeoNames alternate names include ordinary English
// words ("More", "Than"), and lower-case collisions are the main source of
// false candidates for this small matcher.
func (g *Gazetteer) Candidates(text, title string, max int) []Place {
	if max <= 0 {
		max = 8
	}
	words := tokenize(text)
	titleWords := tokenize(title)
	titleSet := make(map[string]struct{}, len(titleWords))
	for _, w := range titleWords {
		titleSet[w.norm] = struct{}{}
	}
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
				if _, inTitle := titleSet[phrase]; inTitle {
					score += 6
				}
				// A longer phrase is stronger evidence than a single word.
				score += float64(n) * 2
				if p.CountryID {
					score -= 2
				}
				if score > scores[p.ID] {
					scores[p.ID] = score
					found[p.ID] = p
				}
			}
		}
	}

	type scored struct {
		p Place
		s float64
	}
	byNameCountry := make(map[string]scored)
	for id, p := range found {
		key := p.Normalized + "|" + p.Country
		prev, ok := byNameCountry[key]
		if !ok || scores[id] > prev.s || (p.CountryID == false && prev.p.CountryID) {
			byNameCountry[key] = scored{p, scores[id]}
		}
	}
	list := make([]scored, 0, len(byNameCountry))
	for _, e := range byNameCountry {
		list = append(list, e)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].p.CountryID != list[j].p.CountryID && list[i].s == list[j].s {
			return !list[i].p.CountryID
		}
		if list[i].s != list[j].s {
			return list[i].s > list[j].s
		}
		return list[i].p.Population > list[j].p.Population
	})
	// A country candidate is redundant when a selected city carries the same
	// name; drop it so the model sees one "Singapore".
	var out []Place
	names := make(map[string]struct{})
	for _, e := range list {
		if e.p.CountryID {
			if _, dup := names[e.p.Normalized]; dup {
				continue
			}
		}
		names[e.p.Normalized] = struct{}{}
		out = append(out, e.p)
		if len(out) >= max {
			break
		}
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
		return "", err
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
	return "", fmt.Errorf("gazetteer: cities15000.txt missing from archive")
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
	req.Header.Set("User-Agent", "dor-eval-gazetteer/0.1 (+https://github.com/ankojh/dor)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("gazetteer: download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gazetteer: download %s: HTTP %d", url, resp.StatusCode)
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

func readCountries(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make(map[string]string)
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
		out[fields[0]] = fields[4]
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

func readCities(path string, countries, admin1 map[string]string) ([]Place, error) {
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
		pop, _ := strconv.ParseInt(fields[14], 10, 64)
		cc := fields[8]
		adm := admin1[cc+"."+fields[10]]
		if adm == "" {
			adm = fields[10]
		}
		name := fields[1]
		out = append(out, Place{
			ID: fields[0], Name: name, Normalized: normalize(name),
			Admin: adm, Country: countries[cc], Population: pop,
		})
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
		out = append(out, token{norm: normalize(raw), raw: raw})
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
	// Drop empty norms produced by punctuation-only tokens.
	filtered := out[:0]
	for _, t := range out {
		if t.norm != "" {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func ambiguousNames() map[string]struct{} {
	names := []string{
		"mobile", "nice", "reading", "bath", "split", "orange", "industry",
		"commerce", "general", "independence", "liberty", "union", "victoria",
		"salem", "decatur", "athens", "rome", "paris", "london", "berlin",
		"north", "south", "east", "west", "central", "national", "federal",
		"government", "president", "minister", "court", "bank", "market",
		"school", "university", "airport", "station", "park", "lake", "river",
		"hill", "beach", "port", "new", "hope", "victory", "trinity",
	}
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		// Keep capital cities: they are usually meant literally in news. The
		// skip list is for English-word collisions only.
		switch n {
		case "paris", "london", "berlin", "rome", "athens", "victoria":
			continue
		}
		out[n] = struct{}{}
	}
	return out
}
