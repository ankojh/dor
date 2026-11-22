package geo

import (
	"sort"
	"strconv"
	"strings"
)

// TextEvidence is a stored article's actual geographic evidence; publisher
// countries are intentionally not reconstructed as event locations.
type TextEvidence struct{ Title, Lede, Dateline string }
type StoredLocation struct {
	PlaceID   string
	Lat, Lon  float64
	Precision Resolution
}

// A nil replacement removes only the dot, preserving the story/article.
type RepairDecision struct {
	Reason       string
	Replacement  *Place
	Precision    Resolution
	ReviewReason string
}

func (g *Gazetteer) RepairLocation(loc StoredLocation, evidence []TextEvidence) RepairDecision {
	p, known := g.ByID(loc.PlaceID)
	var admitted []Place
	byID := map[string]Place{}
	for _, e := range evidence {
		for _, candidate := range g.matchedPlaces(e.Lede, e.Title, e.Dateline, "") {
			if old, ok := byID[candidate.ID]; !ok || betterPlace(candidate, old) {
				byID[candidate.ID] = candidate
			}
		}
	}
	for _, candidate := range byID {
		admitted = append(admitted, candidate)
	}
	sort.Slice(admitted, func(i, j int) bool { return betterPlace(admitted[i], admitted[j]) })
	best, _ := DeterministicPrimary(admitted)
	_, supported := byID[loc.PlaceID]
	reviewReason := ""
	if known && !supported {
		for _, other := range admitted {
			if expandedPlaceName(other.Name) == expandedPlaceName(p.Name) && (sameJurisdiction(p, other) || coincidentCountryCity(p, other)) {
				supported = true
				break
			}
		}
	}
	if known && !supported {
		if g.historicalTitleSupport(p, evidence, admitted) {
			supported = true
			reviewReason = "canonical_title_requires_disambiguation"
		}
	}
	valid := ValidCoordinates(loc.Lat, loc.Lon)
	reason := ""
	switch {
	case !valid:
		reason = "invalid_coordinate"
	case known && !supported:
		reason = "unsupported_match"
	case !known && indexedPlaceID(loc.PlaceID):
		reason = "unsupported_match"
	case known && loc.Precision > PrecisionOf(p):
		reason = "precision"
	case known && (loc.Lat != float64(float32(p.Lat)) || loc.Lon != float64(float32(p.Lon))):
		reason = "coordinate_refresh"
	case !known && strings.HasPrefix(loc.PlaceID, "photon:") && loc.Precision > CityResolution:
		return RepairDecision{Reason: "precision", Replacement: &Place{ID: loc.PlaceID, Lat: loc.Lat, Lon: loc.Lon}, Precision: CityResolution}
	default:
		return RepairDecision{ReviewReason: reviewReason}
	}
	if known && supported {
		p.Lat, p.Lon = float64(float32(p.Lat)), float64(float32(p.Lon))
		return RepairDecision{Reason: reason, Replacement: &p, Precision: min(loc.Precision, PrecisionOf(p)), ReviewReason: reviewReason}
	}
	if best.ID != "" {
		best.Lat, best.Lon = float64(float32(best.Lat)), float64(float32(best.Lon))
		return RepairDecision{Reason: reason, Replacement: &best, Precision: PrecisionOf(best)}
	}
	return RepairDecision{Reason: reason}
}

func coincidentCountryCity(a, b Place) bool {
	return a.CountryCode == b.CountryCode && (a.CountryID || b.CountryID) && DistanceKM(a.Lat, a.Lon, b.Lat, b.Lon) < 1
}

// Abstention by a stricter new-point heuristic is not evidence that a valid
// canonical historical point is false. Keep an exact title witness unless a
// stronger admitted interpretation contradicts it; surface the uncertainty in
// the audit instead of encoding an unsupported deletion in the bundle.
func (g *Gazetteer) historicalTitleSupport(p Place, evidence []TextEvidence, admitted []Place) bool {
	witnessed := false
	strongest := 0
	for _, e := range evidence {
		for field, text := range []string{e.Dateline, e.Title, e.Lede} {
			words := tokenize(text)
			for n := min(8, len(words)); n > 0; n-- {
				for start := 0; start+n <= len(words); start++ {
					if !validPlaceSpan(words, start, n) {
						continue
					}
					phrase := tokensText(words[start : start+n])
					if expandedPlaceName(phrase) != expandedPlaceName(p.Name) || g.coveredByLongerPlace(words, start, n) {
						continue
					}
					quality := g.geographicEvidence(p, phrase, words, start, n, field == 0, "")
					if quality > 0 {
						strongest = max(strongest, quality+boolScore(field == 1, 2))
					}
					if field == 1 && g.geographicEvidenceFor(p, phrase, words, start, n, false, "", true) > 0 {
						witnessed = true
					}
				}
			}
		}
	}
	if !witnessed {
		return false
	}
	for _, other := range admitted {
		if expandedPlaceName(other.Name) == expandedPlaceName(p.Name) && !sameJurisdiction(p, other) && int(other.Score) >= 60 && int(other.Score) > strongest {
			return false
		}
	}
	return true
}

func (g *Gazetteer) coveredByLongerPlace(words []token, start, count int) bool {
	for length := count + 1; length <= min(8, len(words)); length++ {
		for left := max(0, start+count-length); left <= start && left+length <= len(words); left++ {
			if !validPlaceSpan(words, left, length) {
				continue
			}
			phrase := tokensText(words[left : left+length])
			for _, p := range g.byName[phrase] {
				if g.geographicEvidence(p, phrase, words, left, length, false, "") > 0 {
					return true
				}
			}
		}
	}
	return false
}

func indexedPlaceID(id string) bool {
	if strings.HasPrefix(id, "country:") || strings.HasPrefix(id, "admin:") {
		return true
	}
	_, err := strconv.ParseUint(id, 10, 64)
	return err == nil && id != ""
}
