package fetch

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// GDELTItem is one GDELT 2.0 document (GKG) or event (export) row.
type GDELTItem struct {
	RecordID  string
	URL       string
	Domain    string
	Title     string // GKG Extras <PAGE_TITLE>, when present
	Published time.Time
	Themes    []string
	Place     string
	// Country is GDELT's FIPS 10-4 location code. It is not ISO and is
	// deliberately not copied into RawHints.country (BG is Bangladesh in FIPS
	// but Bulgaria in ISO). Use Place, which carries the full name.
	Country   string
	Lat       float64
	Lon       float64
	Located   bool
	Precision PlacePrecision
	// Actors and EventCode are set by the export parser only.
	Actor1    string
	Actor2    string
	EventCode string
}

// ParseGDELTGKG decodes a GDELT 2.0 GKG CSV file. GKG is document-centric: it
// carries the article URL, its domain, its themes and its extracted locations,
// which is exactly what a raw article needs. The full text is never present in
// a GKG file, so nothing beyond metadata can leak into Dor.
func ParseGDELTGKG(r io.Reader) ([]GDELTItem, error) {
	rows, err := readTSV(r)
	if err != nil {
		return nil, err
	}
	out := make([]GDELTItem, 0, len(rows))
	for _, row := range rows {
		if len(row) < 11 {
			continue
		}
		item := GDELTItem{
			RecordID: strings.TrimSpace(row[0]),
			Domain:   strings.ToLower(strings.TrimSpace(row[3])),
			URL:      strings.TrimSpace(row[4]),
			Title:    gdeltPageTitle(gdeltField(row, 26)),
			Themes:   parseGDELTThemes(firstNonEmpty(row[8], row[7])),
		}
		if item.URL == "" {
			continue
		}
		if item.Domain == "" {
			item.Domain = domainOf(item.URL)
		}
		item.Published = parseGDELTDate(row[1])
		if lat, lon, place, country, precision, ok := bestGDELTLocation(row[10]); ok {
			item.Lat, item.Lon = lat, lon
			item.Place, item.Country, item.Precision = place, country, precision
			item.Located = true
		}
		out = append(out, item)
	}
	return out, nil
}

// ParseGDELTExport decodes a GDELT 2.0 export CSV file (61 columns). It is the
// event-centric alternative to GKG: it has action coordinates and CAMEO event
// codes but no themes, so the agent derives themes from the event roots.
func ParseGDELTExport(r io.Reader) ([]GDELTItem, error) {
	rows, err := readTSV(r)
	if err != nil {
		return nil, err
	}
	out := make([]GDELTItem, 0, len(rows))
	for _, row := range rows {
		if len(row) < 61 {
			continue
		}
		sourceURL := strings.TrimSpace(row[60])
		if sourceURL == "" {
			continue
		}
		item := GDELTItem{
			RecordID:  strings.TrimSpace(row[0]),
			URL:       sourceURL,
			Domain:    domainOf(sourceURL),
			Published: parseGDELTDate(firstNonEmpty(row[59], row[1])),
			Actor1:    titleASCII(strings.TrimSpace(row[6])),
			Actor2:    titleASCII(strings.TrimSpace(row[16])),
			EventCode: strings.TrimSpace(row[26]),
			Country:   strings.TrimSpace(row[53]),
			Place:     strings.TrimSpace(row[52]),
		}
		item.Themes = gdeltEventThemes(strings.TrimSpace(row[28]), item.EventCode)
		if geoType := strings.TrimSpace(row[51]); geoType != "" && geoType != "0" {
			lat, latErr := strconv.ParseFloat(strings.TrimSpace(row[56]), 64)
			lon, lonErr := strconv.ParseFloat(strings.TrimSpace(row[57]), 64)
			if latErr == nil && lonErr == nil && validCoords(lat, lon) {
				item.Lat, item.Lon = lat, lon
				item.Located = true
				item.Precision = gdeltPrecision(geoType)
			}
		}
		out = append(out, item)
	}
	return out, nil
}

func readTSV(r io.Reader) ([][]string, error) {
	reader := csv.NewReader(r)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.ReuseRecord = false
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("fetch: parse gdelt tsv: %w", err)
	}
	return rows, nil
}

// gdeltField is a bounds-checked column read: GKG rows have varied widths in
// the wild and a short row must not panic the parser.
func gdeltField(row []string, i int) string {
	if i < 0 || i >= len(row) {
		return ""
	}
	return row[i]
}

func parseGDELTThemes(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	seen := make(map[string]bool)
	var themes []string
	for _, part := range strings.Split(raw, ";") {
		theme := strings.TrimSpace(strings.SplitN(part, ",", 2)[0])
		if theme == "" || seen[theme] {
			continue
		}
		seen[theme] = true
		themes = append(themes, theme)
		if len(themes) == 32 {
			break
		}
	}
	return themes
}

// gdeltEventThemes maps a CAMEO event root to the same semantic themes the GKG
// path uses, so one relevant-theme allowlist works for both file families.
// Roots 18/19/20 (assault, fight, mass violence) and 13/15/16/17 (threaten,
// force posture, coercion) are conflict; 14 is protest. Other roots keep the
// raw CAMEO code and are filtered out by the allowlist.
func gdeltEventThemes(root, code string) []string {
	var themes []string
	switch root {
	case "14":
		themes = append(themes, "PROTEST")
	case "13", "15", "16", "17":
		themes = append(themes, "MILITARY")
	case "18", "19", "20":
		themes = append(themes, "ARMEDCONFLICT")
	}
	if root != "" {
		themes = append(themes, "CAMEO_ROOT_"+root)
	}
	if code != "" {
		themes = append(themes, "CAMEO_"+code)
	}
	return themes
}

// bestGDELTLocation picks the most precise location in a V2Locations field.
// Entries look like "4#Delhi, Delhi, India#IN#IN07#28.6667#77.2167#-2094230".
func bestGDELTLocation(raw string) (lat, lon float64, place, country string, precision PlacePrecision, ok bool) {
	best := PlacePrecision(PrecisionUnknown)
	for _, part := range strings.Split(raw, ";") {
		fields := strings.Split(part, "#")
		if len(fields) < 7 {
			continue
		}
		la, err1 := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
		lo, err2 := strconv.ParseFloat(strings.TrimSpace(fields[6]), 64)
		if err1 != nil || err2 != nil || !validCoords(la, lo) {
			continue
		}
		p := gdeltPrecision(fields[0])
		name := strings.TrimSpace(fields[1])
		// Most precise wins. GDELT lists locations in salience order, so the
		// first entry at that precision is the deterministic, meaningful pick.
		if p <= best {
			continue
		}
		best = p
		lat, lon = la, lo
		place = name
		country = strings.TrimSpace(fields[2])
	}
	return lat, lon, place, country, best, best != PrecisionUnknown
}

// gdeltPageTitle extracts the publisher's page title from the GKG Extras
// column, which carries XML-like key/value pairs.
func gdeltPageTitle(extras string) string {
	const open, close = "<PAGE_TITLE>", "</PAGE_TITLE>"
	start := strings.Index(extras, open)
	if start < 0 {
		return ""
	}
	rest := extras[start+len(open):]
	end := strings.Index(rest, close)
	if end < 0 {
		return ""
	}
	return cleanText(rest[:end], 300)
}

func gdeltPrecision(geoType string) PlacePrecision {
	switch strings.TrimSpace(geoType) {
	case "1":
		return PrecisionCountry
	case "2", "5":
		return PrecisionRegion
	case "3", "4":
		return PrecisionCity
	default:
		return PrecisionUnknown
	}
}

func parseGDELTDate(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{"20060102150405", "20060102", "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// GDELTLastFile reads lastupdate.txt and returns the newest file URL of the
// configured family ("gkg" or "export"). Each line is "<size> <md5> <url>".
func GDELTLastFile(body []byte, family string) (string, error) {
	suffix := ".gkg.csv.zip"
	if strings.EqualFold(family, "export") {
		suffix = ".export.csv.zip"
	}
	best := ""
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		u := fields[len(fields)-1]
		if !strings.HasPrefix(u, "https://") && !strings.HasPrefix(u, "http://") {
			continue
		}
		if strings.HasSuffix(strings.ToLower(u), suffix) {
			best = u
		}
	}
	if best == "" {
		return "", fmt.Errorf("fetch: lastupdate has no %s file", suffix)
	}
	// The index is served over http but supports https; stay on https.
	if strings.HasPrefix(best, "http://") {
		best = "https://" + strings.TrimPrefix(best, "http://")
	}
	return best, nil
}

// UnzipCSV returns the first CSV entry of a zip, bounded in size.
func UnzipCSV(body []byte, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 128 << 20
	}
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("fetch: open zip: %w", err)
	}
	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("fetch: open %s: %w", f.Name, err)
		}
		defer func() { _ = rc.Close() }()
		data, err := io.ReadAll(io.LimitReader(rc, maxBytes+1))
		if err != nil {
			return nil, fmt.Errorf("fetch: read %s: %w", f.Name, err)
		}
		if int64(len(data)) > maxBytes {
			return nil, fmt.Errorf("fetch: %s exceeds %d bytes", f.Name, maxBytes)
		}
		return data, nil
	}
	return nil, errors.New("fetch: zip has no csv entry")
}

func domainOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// titleASCII upper-cases the first letter of an ASCII actor name. GDELT actor
// names are uppercase ASCII; this only makes them readable.
func titleASCII(s string) string {
	lower := strings.ToLower(s)
	if lower == "" {
		return ""
	}
	r := []rune(lower)
	r[0] = []rune(strings.ToUpper(string(r[0])))[0]
	return string(r)
}

func validCoords(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)
}

// DomainAllowed reports whether domain is in allow, matching exact hosts and
// subdomains ("bbc.co.uk" allows "www.bbc.co.uk").
func DomainAllowed(domain string, allow []string) bool {
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		return false
	}
	for _, a := range allow {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "" {
			continue
		}
		if domain == a || strings.HasSuffix(domain, "."+a) {
			return true
		}
	}
	return false
}
