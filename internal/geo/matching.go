package geo

import (
	"math"
	"sort"
	"strings"
)

type mention struct {
	place      Place
	start, end int
	quality    int
}

// matchedPlaces admits geographic spans, not arbitrary aliases anywhere in the
// text. A longer admitted span hides its contained words; an exact jurisdiction
// name hides a colliding city/alternate name unless independently qualified.
func (g *Gazetteer) matchedPlaces(text, title, dateline, countryHint string) []Place {
	if g == nil || !g.loaded {
		return nil
	}
	hint := g.CountryCode(countryHint)
	found := map[string]Place{}
	for fieldIndex, field := range []string{dateline, title, text} {
		words := tokenize(field)
		var spans []mention
		for n := 8; n > 0; n-- {
			for i := 0; i+n <= len(words); i++ {
				separated := false
				for k := i + 1; k < i+n; k++ {
					if strings.ContainsAny(words[k].sep, "\n.;:!?") {
						separated = true
						break
					}
				}
				if separated {
					continue
				}
				phrase := tokensText(words[i : i+n])
				var atSpan []mention
				for _, p := range g.byName[phrase] {
					quality := g.geographicEvidence(p, phrase, words, i, n, fieldIndex == 0, hint)
					if quality == 0 {
						continue
					}
					p.Normalized = phrase
					p.Score = float64(quality) + math.Log10(float64(p.Population)+10)/10
					if fieldIndex == 1 {
						p.Score += 2
					}
					atSpan = append(atSpan, mention{p, i, i + n, quality})
				}
				if len(atSpan) == 0 {
					continue
				}
				sort.Slice(atSpan, func(i, j int) bool { return betterPlace(atSpan[i].place, atSpan[j].place) })
				// A span has one supported deterministic interpretation. Keeping a
				// colliding loser here would make the repair retain its old dot.
				m := atSpan[0]
				contained := false
				for _, longer := range spans {
					if longer.start <= m.start && longer.end >= m.end && longer.end-longer.start > n {
						contained = true
						break
					}
				}
				if !contained {
					spans = append(spans, m)
				}
			}
		}
		for _, m := range spans {
			if old, ok := found[m.place.ID]; !ok || betterPlace(m.place, old) {
				found[m.place.ID] = m.place
			}
		}
	}
	out := make([]Place, 0, len(found))
	for _, p := range found {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return betterPlace(out[i], out[j]) })
	return out
}

func betterPlace(a, b Place) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Population != b.Population {
		return a.Population > b.Population
	}
	return a.ID < b.ID
}

// Qualifiers must be outside the matched name; a city must never receive a
// country/admin bonus by matching its own name (Armenia, India, Florida, ...).
func independentQualifier(p Place, words []token, start, count int, dateline bool) bool {
	name := normalize(p.Name)
	adjacent := func(q string) bool {
		q = normalize(q)
		if q == "" || q == name {
			return false
		}
		parts := strings.Fields(q)
		matches := func(at int) bool {
			if at < 0 || at+len(parts) > len(words) {
				return false
			}
			for k, part := range parts {
				if words[at+k].norm != part {
					return false
				}
			}
			return true
		}
		end := start + count
		if matches(end) && (dateline || strings.ContainsAny(words[end].sep, ",(")) {
			return true
		}
		if matches(start-len(parts)) && (dateline || strings.ContainsAny(words[start].sep, ",:(")) {
			return true
		}
		return end < len(words) && words[end].norm == "in" && matches(end+1)
	}
	return adjacent(p.Country) || adjacent(p.Admin)
}

func (g *Gazetteer) geographicEvidence(p Place, phrase string, words []token, start, count int, dateline bool, hint string) int {
	if !ValidCoordinates(p.Lat, p.Lon) || !capitalized(words[start].raw) {
		return 0
	}
	canonicalName := p.Name
	if p.RegionAlias != "" {
		canonicalName = p.RegionAlias
	}
	canonical := phrase == normalize(canonicalName)
	countryName := p.CountryID && p.RegionAlias == "" && canonical
	// ISO/IATA aliases describe explicit lookup vocabulary, not ordinary prose.
	if p.CountryID && !canonical && p.RegionAlias == "" {
		if !strings.Contains(" us usa uk u s u k ", " "+phrase+" ") ||
			p.CountryCode != "US" && p.CountryCode != "GB" {
			return 0
		}
		for _, w := range words[start : start+count] {
			if w.raw != strings.ToUpper(w.raw) {
				return 0
			}
		}
	}
	if !p.CountryID && !canonical {
		name := normalize(p.Name)
		// Accept faithful longer/shorter forms (NEW YORK / New York City),
		// not unrelated historical aliases, brands or airport codes.
		if count == 1 || !(strings.HasPrefix(name, phrase+" ") || strings.HasPrefix(phrase, name+" ")) {
			return 0
		}
	}
	for _, w := range words[start : start+count] {
		if !capitalized(w.raw) && !strings.Contains(" de la le del dos da of the and ", " "+w.norm+" ") {
			return 0
		}
	}
	qualified := independentQualifier(p, words, start, count, dateline) || hint != "" && p.CountryCode == hint
	if !p.CountryID {
		if _, countryCollision := g.Country(p.Name); countryCollision && !qualified {
			return 0
		}
	}
	_, ambiguous := g.skip[phrase]
	// Possessive apostrophe-s is grammar, not a location pattern. Only an
	// actual preposition or the explicit '-based' construction is locative.
	locative := start > 0 && strings.Contains(" in at near across around within outside into ", " "+words[start-1].norm+" ")
	if end := start + count; end < len(words) && words[end].norm == "based" {
		locative = true
	}
	if ambiguous && !qualified {
		return 0
	}
	if dateline {
		if !canonical && count < 2 && !p.CountryID {
			return 0
		}
		return 70 + boolScore(qualified, 20) + jurisdictionScore(p)
	}
	if countryName {
		return 35
	}
	if p.CountryID {
		return 34
	}
	if qualified {
		return 60 + jurisdictionScore(p)
	}
	if ambiguous {
		return 0
	}
	if canonical && p.Feature == "ADM1" {
		return 32
	}
	if locative {
		return 40
	}
	// A full multiword name is more specific than a capitalized surname or
	// brand. Single-token canonical cities still require geographic context.
	if count > 1 {
		return 30
	}
	return 0
}

func boolScore(b bool, n int) int {
	if b {
		return n
	}
	return 0
}
func jurisdictionScore(p Place) int {
	if p.CountryID {
		return 3
	}
	return 0
}

// DeterministicPrimary declines equally supported countries rather than using
// ID ordering to invent an event location in a multi-country headline.
func DeterministicPrimary(places []Place) (Place, bool) {
	if len(places) == 0 {
		return Place{}, false
	}
	best := places[0]
	if PrecisionOf(best) == CountryResolution {
		for _, p := range places[1:] {
			if p.Score == best.Score && PrecisionOf(p) == CountryResolution && p.CountryCode != best.CountryCode {
				return Place{}, false
			}
		}
	}
	return best, true
}
