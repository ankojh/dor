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
					separator := words[k].sep
					// GeoNames uses punctuated abbreviations and initials in
					// canonical names (St. Louis, José C. Paz, U.S.). A full
					// sentence boundary still cannot join an ordinary phrase.
					if abbreviationToken(words[k-1].norm) && strings.Count(separator, ".") == 1 {
						separator = strings.ReplaceAll(separator, ".", "")
					}
					if strings.ContainsAny(separator, "\n.;:!?") {
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
	// An uppercase administrative code is an explicit qualifier, including
	// conventional unpunctuated forms such as Washington DC or Paris TX.
	end := start + count
	adminCode := false
	if end < len(words) && len(p.AdminCode) == 2 && p.AdminCode == strings.ToUpper(p.AdminCode) &&
		p.AdminCode[0] >= 'A' && p.AdminCode[0] <= 'Z' {
		adminCode = words[end].raw == p.AdminCode
		if end+1 < len(words) && strings.Contains(words[end+1].sep, ".") && !strings.ContainsAny(words[end+1].sep, "\n;:!?") {
			adminCode = adminCode || words[end].raw+words[end+1].raw == p.AdminCode
		}
	}
	return adjacent(p.Country) || adjacent(p.Admin) || adminCode
}

func (g *Gazetteer) geographicEvidence(p Place, phrase string, words []token, start, count int, dateline bool, hint string) int {
	// No-data regions may remain explicit lookup aliases, but cannot turn a
	// bare regional name into an automatic dot at another country's capital.
	if p.RegionAlias != "" || !ValidCoordinates(p.Lat, p.Lon) || !capitalized(words[start].raw) {
		return 0
	}
	if end := start + count; end < len(words) && !strings.ContainsAny(words[end].sep, "\n.;:!?") && incompatibleFeatureSuffix(p, words[end].norm) {
		// A populated-place coordinate does not establish the position of a
		// separately named river/valley, or an administrative jurisdiction.
		return 0
	}
	canonical := expandedPlaceName(phrase) == expandedPlaceName(p.Name)
	countryName := p.CountryID && canonical
	// ISO/IATA aliases describe explicit lookup vocabulary, not ordinary prose.
	if p.CountryID && !canonical {
		if !strings.Contains(" us usa uk u s u k u s a ", " "+phrase+" ") ||
			p.CountryCode != "US" && p.CountryCode != "GB" {
			return 0
		}
		for _, w := range words[start : start+count] {
			if w.raw != strings.ToUpper(w.raw) {
				return 0
			}
		}
		if end := start + count; end < len(words) {
			// US$ and US dollars are a currency, not event geography. Also
			// reject a contained U.S. match inside a longer dotted acronym.
			next := words[end]
			if strings.ContainsAny(next.sep, "$£€") || next.norm == "dollar" || next.norm == "dollars" ||
				len(next.raw) == 1 && next.raw == strings.ToUpper(next.raw) && strings.Contains(next.sep, ".") {
				return 0
			}
		}
	}
	if !p.CountryID && !canonical {
		longForm := expandedPlaceName(phrase)
		if longForm == expandedPlaceName(p.Admin) || longForm == expandedPlaceName(p.Country) {
			// A city's alternate name can duplicate its enclosing region
			// (Rio Grande / Rio Grande do Sul). It is evidence for that
			// jurisdiction, never an independently supported city coordinate.
			return 0
		}
		name := expandedPlaceName(p.Name)
		// Accept faithful longer/shorter forms (NEW YORK / New York City),
		// not unrelated historical aliases, brands or airport codes.
		if count == 1 || !(strings.HasPrefix(name, longForm+" ") || strings.HasPrefix(longForm, name+" ")) {
			return 0
		}
	}
	for _, w := range words[start : start+count] {
		nameSuffix := w.norm == "s" && strings.ContainsAny(w.sep, "'’")
		if !capitalized(w.raw) && !nameSuffix && !strings.Contains(" de la le del dos do da of the and upon ", " "+w.norm+" ") {
			return 0
		}
	}
	textQualified := independentQualifier(p, words, start, count, dateline)
	hintQualified := hint != "" && p.CountryCode == hint
	qualified := textQualified || hintQualified
	if !p.CountryID {
		if _, countryCollision := g.Country(p.Name); countryCollision && !qualified {
			return 0
		}
	}
	_, ambiguous := g.skip[phrase]
	// Possessive apostrophe-s is grammar, not a location pattern. Only an
	// actual preposition or the explicit '-based' construction is locative.
	locative := start > 0 && !strings.ContainsAny(words[start].sep, "\n.;:!?") &&
		strings.Contains(" in at near across around within outside into ", " "+words[start-1].norm+" ")
	if end := start + count; end < len(words) && words[end].norm == "based" && !strings.ContainsAny(words[end].sep, "\n.;:!?") {
		locative = true
	}
	explicitCountry := countryName && g.explicitCountryEvidence(p, words, start, count, dateline, hint)
	if countryName && g.foreignRegionCollision(p) && !explicitCountry {
		// A locative such as 'living in Georgia' does not distinguish the
		// country from the US state. Keep both possibilities unlocated unless
		// independent evidence identifies the country or an actual venue.
		return 0
	}
	_, contextualName := contextNames[phrase]
	if contextualName && !p.CountryID && !qualified && g.foreignCanonicalCollision(p) {
		// 'in Victoria' is geographic syntax, but still cannot distinguish
		// the Australian region from several cities in other countries.
		return 0
	}
	// Shared personal names and country/commodity names need actual text
	// evidence. A publisher's country alone cannot locate a person or brand.
	if contextualName && !(textQualified || dateline || locative || explicitCountry || countryName && disasterImpact(words, start)) {
		return 0
	}
	if ambiguous && !(textQualified || hintQualified && (dateline || locative)) {
		return 0
	}
	if canonical && p.Feature == "ADM1" && count == 1 && !g.uniqueRegionName(p) &&
		!(textQualified || dateline || locative) {
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
	if locative {
		return 40
	}
	if canonical && p.Feature == "ADM1" && count == 1 {
		return 32
	}
	// A full multiword name is more specific than a capitalized surname or
	// brand. Single-token canonical cities still require geographic context.
	if count > 1 {
		return 30
	}
	return 0
}

func incompatibleFeatureSuffix(p Place, next string) bool {
	natural := strings.Contains(" river lake valley mountain range ", " "+next+" ")
	admin := strings.Contains(" province state county district ", " "+next+" ")
	if p.CountryID {
		return false // A country can honestly represent a coarser jurisdiction.
	}
	if p.Feature == "ADM1" {
		return natural
	}
	return natural || admin
}

// Prefix abbreviations are faithful canonical forms, unlike arbitrary
// alternate names. Both forms are indexed regardless of GeoNames alias order.
func expandedPlaceName(s string) string {
	parts := strings.Fields(normalize(s))
	if len(parts) > 1 {
		switch parts[0] {
		case "st":
			parts[0] = "saint"
		case "ste":
			parts[0] = "sainte"
		case "ft":
			parts[0] = "fort"
		case "mt":
			parts[0] = "mount"
		}
	}
	return strings.Join(parts, " ")
}

func abbreviatedPlaceName(s string) string {
	parts := strings.Fields(expandedPlaceName(s))
	if len(parts) > 1 {
		switch parts[0] {
		case "saint":
			parts[0] = "st"
		case "sainte":
			parts[0] = "ste"
		case "fort":
			parts[0] = "ft"
		case "mount":
			parts[0] = "mt"
		}
	}
	return strings.Join(parts, " ")
}

func abbreviationToken(s string) bool {
	return s == "st" || s == "ste" || s == "ft" || s == "mt" || len(s) == 1
}

// These canonical geographic names are also common personal names or words.
// Unlike the lexical stoplist, a genuine locative/dateline can admit them.
var contextNames = map[string]struct{}{
	"victoria": {}, "jordan": {}, "chad": {}, "israel": {}, "turkey": {},
	"reunion": {}, "georgia": {}, "lori": {}, "mila": {}, "diana": {},
	"nan": {}, "mara": {}, "carolina": {}, "alberta": {}, "brittany": {},
	"miranda": {}, "uri": {}, "andreas": {}, "douglas": {}, "patrick": {},
	"constantine": {}, "sofia": {}, "sal": {}, "paola": {}, "indiana": {},
}

// An impact verb alone can describe a person. Require a nearby disaster noun
// before accepting that construction as country geography (Floods hit Jordan).
func disasterImpact(words []token, start int) bool {
	if start < 2 || !strings.Contains(" hit hits struck strikes ", " "+words[start-1].norm+" ") {
		return false
	}
	for i := start; i >= max(0, start-4); i-- {
		if i < start-1 && strings.Contains(" flood floods flooding earthquake earthquakes storm storms hurricane hurricanes cyclone cyclones wildfire wildfires bushfire bushfires tornado tornadoes tsunami drought ", " "+words[i].norm+" ") {
			return true
		}
		if strings.ContainsAny(words[i].sep, "\n.;:!?") {
			return false
		}
	}
	return false
}

func (g *Gazetteer) foreignCanonicalCollision(p Place) bool {
	for _, other := range g.byName[normalize(p.Name)] {
		if other.RegionAlias == "" && other.CountryCode != p.CountryCode && normalize(other.Name) == normalize(p.Name) {
			return true
		}
	}
	return false
}

func (g *Gazetteer) foreignRegionCollision(p Place) bool {
	for _, other := range g.byName[normalize(p.Name)] {
		if other.RegionAlias == "" && other.Feature == "ADM1" && other.CountryCode != p.CountryCode && normalize(other.Name) == normalize(p.Name) {
			return true
		}
	}
	return false
}

func (g *Gazetteer) explicitCountryEvidence(p Place, words []token, start, count int, dateline bool, hint string) bool {
	if hint != "" && hint == p.CountryCode {
		return true
	}
	if start > 0 && words[start-1].norm == "country" ||
		start > 1 && words[start-1].norm == "of" && words[start-2].norm == "country" ||
		start+count < len(words) && words[start+count].norm == "country" {
		return true
	}
	country, ok := g.countries[p.CountryCode]
	capital := normalize(country.Capital)
	return ok && dateline && capital != "" && strings.Contains(" "+tokensText(words)+" ", " "+capital+" ")
}

// The capitalization-only ADM1 shortcut is limited to canonical names with
// one geographic meaning. Cities, countries and other regions with the same
// name require a dateline, locative or independently stated qualifier.
func (g *Gazetteer) uniqueRegionName(p Place) bool {
	for _, other := range g.byName[normalize(p.Name)] {
		if other.RegionAlias != "" || normalize(other.Name) != normalize(p.Name) || other.ID == p.ID {
			continue
		}
		if other.Feature == "ADM1" && other.CountryCode == p.CountryCode && other.AdminCode == p.AdminCode {
			continue
		}
		return false
	}
	return true
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
