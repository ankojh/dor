package geo

import (
	"math"
	"strings"
)

// ValidCoordinates excludes the missing-coordinate sentinel as well as values
// that cannot be drawn. A point on the equator or prime meridian is valid.
func ValidCoordinates(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) &&
		!math.IsInf(lat, 0) && !math.IsInf(lon, 0) &&
		lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

// Resolution orders the maximum precision supported by a resolved coordinate.
type Resolution int

const (
	CountryResolution Resolution = iota
	RegionResolution
	CityResolution
	StreetResolution
	PointResolution
)

// PrecisionOf describes the coordinate, independently of model confidence.
// Unknown Photon feature types conservatively support city precision only.
func PrecisionOf(p Place) Resolution {
	if p.CountryID {
		return CountryResolution
	}
	switch strings.ToLower(p.Feature) {
	case "pcli", "country":
		return CountryResolution
	case "adm1", "adm2", "state", "county", "region":
		return RegionResolution
	case "street":
		return StreetResolution
	case "house", "building", "landmark", "attraction", "museum", "monument":
		return PointResolution
	default:
		return CityResolution
	}
}

// ByID resolves a canonical GeoNames, country or admin identifier.
func (g *Gazetteer) ByID(id string) (Place, bool) {
	if g == nil {
		return Place{}, false
	}
	p, ok := g.byID[id]
	return p, ok && ValidCoordinates(p.Lat, p.Lon)
}

// populationCentre uses unit vectors so regions across the date line do not
// average to Greenwich. Alternate names never count as additional population.
func populationCentre(places []Place, country, admin string) (float64, float64, bool) {
	seen := make(map[string]bool)
	var x, y, z float64
	for _, p := range places {
		if p.CountryCode != country || admin != "" && p.AdminCode != admin ||
			!strings.HasPrefix(p.Feature, "PPL") || p.Population <= 0 || seen[p.ID] || !ValidCoordinates(p.Lat, p.Lon) {
			continue
		}
		seen[p.ID] = true
		lat, lon, weight := p.Lat*math.Pi/180, p.Lon*math.Pi/180, float64(p.Population)
		x += weight * math.Cos(lat) * math.Cos(lon)
		y += weight * math.Cos(lat) * math.Sin(lon)
		z += weight * math.Sin(lat)
	}
	if math.Sqrt(x*x+y*y+z*z) < 1e-9 {
		return 0, 0, false
	}
	lat, lon := math.Atan2(z, math.Hypot(x, y))*180/math.Pi, math.Atan2(y, x)*180/math.Pi
	return lat, lon, ValidCoordinates(lat, lon)
}

func regionCentre(places []Place, country, admin string) (float64, float64, bool) {
	for _, p := range places {
		if p.CountryCode == country && p.AdminCode == admin && p.Feature == "ADM1" && ValidCoordinates(p.Lat, p.Lon) {
			return p.Lat, p.Lon, true
		}
	}
	return populationCentre(places, country, admin)
}

func containsPhrase(text, phrase string) bool {
	return phrase != "" && strings.Contains(" "+normalize(text)+" ", " "+phrase+" ")
}

// mentionEvidence deliberately treats automatic headline matching more
// strictly than an explicit Lookup or Search. The stoplist covers common words,
// personal names and short/transliterated aliases observed in the live review.
func (g *Gazetteer) mentionEvidence(p Place, phrase string, words []token, start, count int, dateline string) float64 {
	if !ValidCoordinates(p.Lat, p.Lon) {
		return 0
	}
	datelineMatch := containsPhrase(dateline, phrase)
	locative := start > 0 && strings.Contains(" in at near from across around through within outside into ", " "+words[start-1].norm+" ")
	if end := start + count; end < len(words) && (words[end].norm == "based" || words[end].norm == "s") {
		locative = true
	}
	_, ambiguous := g.skip[phrase]
	if ambiguous {
		// Even "from Paul" can name a person. Require an independent geographic
		// qualifier for ambiguous single words, unless explicitly a dateline.
		qualified := p.Country != "" && containsPhrase(tokensText(words), normalize(p.Country)) ||
			p.Admin != "" && normalize(p.Admin) != phrase && containsPhrase(tokensText(words), normalize(p.Admin))
		if !datelineMatch && !(locative && qualified) {
			return 0
		}
	}
	// GeoNames aliases include IATA airport codes (BTS, INI) and ISO
	// codes that collide with ordinary news abbreviations (MP, CM).
	// Keep them for explicit lookup; automatic prose needs more evidence.
	if !p.CountryID && !datelineMatch && len(phrase) <= 3 && phrase != normalize(p.Name) {
		return 0
	}
	if !datelineMatch && !locative && len(phrase) <= 3 && phrase != normalize(p.Name) {
		if !p.CountryID || !strings.Contains(" us usa uk u s ", " "+phrase+" ") {
			return 0
		}
	}
	if datelineMatch {
		return 20
	}
	if locative {
		return 12
	}
	if !capitalized(words[start].raw) {
		return 0
	}
	if p.CountryID {
		if count == 1 && len(phrase) == 2 && words[start].raw != strings.ToUpper(words[start].raw) {
			return 0
		}
		return 2
	}
	if count > 1 {
		for _, w := range words[start : start+count] {
			if !capitalized(w.raw) && !strings.Contains(" de la le del dos da of the and ", " "+w.norm+" ") {
				return 0
			}
		}
		return 6
	}
	if p.Feature == "ADM1" || p.Population >= 50000 {
		return 2
	}
	return 0
}

func tokensText(words []token) string {
	parts := make([]string, len(words))
	for i, w := range words {
		parts[i] = w.norm
	}
	return strings.Join(parts, " ")
}

// SupportsPlace applies exactly the automatic-candidate evidence rules to an
// existing location, including its aliases. Used by the one-off data repair.
func (g *Gazetteer) SupportsPlace(id, text, title, dateline string) bool {
	p, ok := g.ByID(id)
	if !ok {
		return false
	}
	for _, field := range []string{title, dateline, text} {
		words := tokenize(field)
		for _, alias := range g.aliases[id] {
			parts := strings.Fields(alias)
			for i := 0; i+len(parts) <= len(words); i++ {
				match := true
				for k, part := range parts {
					if words[i+k].norm != part {
						match = false
						break
					}
				}
				if match && g.mentionEvidence(p, alias, words, i, len(parts), dateline) > 0 {
					return true
				}
			}
		}
	}
	return false
}
