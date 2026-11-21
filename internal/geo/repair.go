package geo

import "strings"

// TextEvidence is a stored article's geographic evidence. Source-country hints
// are intentionally absent: they describe the publisher, not the event.
type TextEvidence struct {
	Title    string
	Lede     string
	Dateline string
}

// StoredLocation is the portion of a persisted location used by the repair.
type StoredLocation struct {
	PlaceID   string
	Lat       float64
	Lon       float64
	Precision Resolution
}

// RepairDecision is empty for an unchanged row. A nil replacement removes the
// dot; callers keep the story/article itself available in the wire.
type RepairDecision struct {
	Reason      string
	Replacement *Place
	Precision   Resolution
}

// RepairLocation checks old data using the same evidence and precision rules
// as new plots. It never guesses the original country from a rejected town.
func (g *Gazetteer) RepairLocation(loc StoredLocation, evidence []TextEvidence) RepairDecision {
	p, known := g.ByID(loc.PlaceID)
	valid := ValidCoordinates(loc.Lat, loc.Lon)
	supported := false
	if known {
		for _, e := range evidence {
			if g.SupportsPlace(p.ID, e.Lede, e.Title, e.Dateline) {
				supported = true
				break
			}
		}
	}
	reason := ""
	switch {
	case !valid:
		reason = "invalid_coordinate"
	case known && !p.CountryID && !supported:
		reason = "unsupported_match"
	case known && loc.Precision > PrecisionOf(p):
		return RepairDecision{Reason: "precision", Replacement: &p, Precision: PrecisionOf(p)}
	case !known && strings.HasPrefix(loc.PlaceID, "photon:") && loc.Precision > CityResolution:
		// Old Photon rows have no stored feature type. The coordinate cannot
		// retrospectively prove street/point precision.
		return RepairDecision{Reason: "precision", Replacement: &Place{ID: loc.PlaceID, Lat: loc.Lat, Lon: loc.Lon}, Precision: CityResolution}
	default:
		return RepairDecision{}
	}
	if known && (supported || p.CountryID) {
		return RepairDecision{Reason: reason, Replacement: &p, Precision: min(loc.Precision, PrecisionOf(p))}
	}
	var best Place
	for _, e := range evidence {
		candidates := g.Candidates(e.Lede, e.Title, e.Dateline, "", 1)
		if len(candidates) > 0 && candidates[0].Score >= 10 && (best.ID == "" || candidates[0].Score > best.Score) {
			best = candidates[0]
		}
	}
	if best.ID != "" {
		return RepairDecision{Reason: reason, Replacement: &best, Precision: PrecisionOf(best)}
	}
	return RepairDecision{Reason: reason}
}
