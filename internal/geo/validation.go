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

// regionCentre always selects an actual supported in-region coordinate.
// Administrative seats precede the largest populated place; aliases never
// affect the selection. GeoNames ADM1 coordinates remain preferred.
func regionCentre(places []Place, country, admin string) (float64, float64, bool) {
	var best Place
	rank := func(p Place) int {
		if p.Feature == "ADM1" {
			return 3
		}
		if p.Feature == "PPLA" {
			return 2
		}
		if strings.HasPrefix(p.Feature, "PPL") {
			return 1
		}
		return 0
	}
	for _, p := range places {
		if p.CountryCode != country || p.AdminCode != admin || !ValidCoordinates(p.Lat, p.Lon) || rank(p) == 0 {
			continue
		}
		if best.ID == "" || rank(p) > rank(best) || rank(p) == rank(best) && (p.Population > best.Population || p.Population == best.Population && p.ID < best.ID) {
			best = p
		}
	}
	return best.Lat, best.Lon, best.ID != ""
}

func containsPhrase(text, phrase string) bool {
	return phrase != "" && strings.Contains(" "+normalize(text)+" ", " "+phrase+" ")
}

// SupportsPlace uses the admitted spans after collision and containment
// resolution, exactly as production candidate selection does.
func (g *Gazetteer) SupportsPlace(id, text, title, dateline string) bool {
	for _, p := range g.matchedPlaces(text, title, dateline, "") {
		if p.ID == id {
			return true
		}
	}
	return false
}

func tokensText(words []token) string {
	parts := make([]string, len(words))
	for i, w := range words {
		parts[i] = w.norm
	}
	return strings.Join(parts, " ")
}
