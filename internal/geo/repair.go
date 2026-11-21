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
	Reason      string
	Replacement *Place
	Precision   Resolution
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
		return RepairDecision{"precision", &Place{ID: loc.PlaceID, Lat: loc.Lat, Lon: loc.Lon}, CityResolution}
	default:
		return RepairDecision{}
	}
	if known && supported {
		p.Lat, p.Lon = float64(float32(p.Lat)), float64(float32(p.Lon))
		return RepairDecision{reason, &p, min(loc.Precision, PrecisionOf(p))}
	}
	if best.ID != "" {
		best.Lat, best.Lon = float64(float32(best.Lat)), float64(float32(best.Lon))
		return RepairDecision{reason, &best, PrecisionOf(best)}
	}
	return RepairDecision{Reason: reason}
}

func indexedPlaceID(id string) bool {
	if strings.HasPrefix(id, "country:") || strings.HasPrefix(id, "admin:") {
		return true
	}
	_, err := strconv.ParseUint(id, 10, 64)
	return err == nil && id != ""
}
