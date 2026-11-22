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
	qualified  bool
}

// matchedPlaces admits geographic spans, not arbitrary aliases anywhere in the
// text. A longer admitted span hides its contained words; an exact jurisdiction
// name hides a colliding city/alternate name unless independently qualified.
func (g *Gazetteer) matchedPlaces(text, title, dateline, countryHint string) []Place {
	if g == nil || !g.loaded {
		return nil
	}
	hint := g.CountryCode(countryHint)
	sport := sportingText(title + " " + text)
	found := map[string]Place{}
	for fieldIndex, field := range []string{dateline, title, text} {
		words := tokenize(field)
		var spans []mention
		for n := 8; n > 0; n-- {
			for i := 0; i+n <= len(words); i++ {
				if !validPlaceSpan(words, i, n) {
					continue
				}
				phrase := tokensText(words[i : i+n])
				var atSpan []mention
				for _, p := range g.byName[phrase] {
					quality := g.geographicEvidence(p, phrase, words, i, n, fieldIndex == 0, hint)
					if sport && !p.CountryID && fieldIndex != 0 && !independentQualifier(p, words, i, n, false) && !locativeEvidence(words, i, n) && !geographicHead(words, i, n) {
						quality = 0
					}
					if quality == 0 {
						continue
					}
					p.Normalized = phrase
					p.Score = float64(quality) + math.Log10(float64(g.majorPopulation(p))+10)/10
					if fieldIndex == 1 {
						p.Score += 2
					}
					qualified := independentQualifier(p, words, i, n, fieldIndex == 0) || (hint != "" && p.CountryCode == hint)
					atSpan = append(atSpan, mention{p, i, i + n, quality, qualified})
				}
				if len(atSpan) == 0 {
					continue
				}
				sort.Slice(atSpan, func(i, j int) bool { return g.betterPlace(atSpan[i].place, atSpan[j].place) })
				// A span has one supported deterministic interpretation. Keeping a
				// colliding loser here would make the repair retain its old dot.
				m := atSpan[0]
				// A region's own city is the more precise answer: the same seat, the
				// national capital, or any city inside that region.
				for _, other := range atSpan[1:] {
					if m.place.Feature != "ADM1" {
						break
					}
					if g.sameSeatRegion(other.place, m.place) || cityInRegion(other.place, m.place) ||
						(!m.qualified && g.nationalCapital(other.place)) {
						m = other
						break
					}
				}
				if n == 1 {
					// A first-level region dominates a same-named foreign town. Without
					// this, "in Florida" or "in Ontario" loses the state/province to a
					// small namesake. An explicitly qualified town keeps its own meaning.
					for _, other := range atSpan[1:] {
						if other.place.CountryCode == m.place.CountryCode {
							continue
						}
						if !m.qualified && g.regionDominates(other.place, m.place) {
							m = other
							break
						}
					}
				}
				for _, other := range atSpan[1:] {
					if n > 1 && other.quality == m.quality && other.place.CountryCode != m.place.CountryCode && !g.dominatesHomonym(m.place, other.place, phrase) {
						// Population cannot choose an unresolved cross-country
						// multiword homonym. Block contained fragments too.
						m.place = Place{}
						break
					}
				}
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
			if m.place.ID == "" {
				continue
			}
			if old, ok := found[m.place.ID]; !ok || g.betterPlace(m.place, old) {
				found[m.place.ID] = m.place
			}
		}
	}
	out := make([]Place, 0, len(found))
	for _, p := range found {
		out = append(out, p)
	}
	// If the title names both a region and a city inside it, the city is the
	// precise event location (St. Louis, Missouri; Atlanta, Georgia). Keep the
	// region when no city inside it was admitted (Bihar Bridge).
	if len(out) > 1 {
		filtered := out[:0]
		for i, p := range out {
			if p.Feature == "ADM1" {
				replaced := false
				for j, other := range out {
					if i != j && cityInRegion(other, p) {
						replaced = true
						break
					}
				}
				if replaced {
					continue
				}
			}
			filtered = append(filtered, p)
		}
		out = filtered
	}
	sort.Slice(out, func(i, j int) bool { return g.betterPlace(out[i], out[j]) })
	return out
}

func sportingText(text string) bool {
	words := " " + tokensText(tokenize(text)) + " "
	for _, marker := range []string{"football", "soccer", "futbol", "cricket", "rugby", "baseball", "basketball", "tennis", "goalkeeper", "midfielder", "striker", "bundesliga", "uefa", "fifa", "nfl", "nba", "nhl", "epl", "wsl", "mlb", "ipl", "atp", "wta", "premier league", "champions league", "liga inggris", "la liga"} {
		if strings.Contains(words, " "+marker+" ") {
			return true
		}
	}
	return false
}
func locativeEvidence(words []token, start, count int) bool {
	if start > 0 && !strings.ContainsAny(words[start].sep, "\n.;:!?") && strings.Contains(" in at near across around within outside into ", " "+words[start-1].norm+" ") {
		return true
	}
	end := start + count
	return end < len(words) && words[end].norm == "based" && !strings.ContainsAny(words[end].sep, "\n.;:!?")
}

func validPlaceSpan(words []token, start, count int) bool {
	for k := start + 1; k < start+count; k++ {
		separator := words[k].sep
		if abbreviationToken(words[k-1].norm) && strings.Count(separator, ".") == 1 {
			separator = strings.ReplaceAll(separator, ".", "")
		}
		if strings.ContainsAny(separator, "\n.;:!?") {
			return false
		}
	}
	return true
}

func (g *Gazetteer) betterPlace(a, b Place) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	ap, bp := g.majorPopulation(a), g.majorPopulation(b)
	if ap != bp {
		return ap > bp
	}
	return a.ID < b.ID
}

// A first-level region dominates a same-named foreign town when its populated
// area is at least five times larger. This keeps "in Florida/Ontario/Delaware/
// Oregon" on the state/province while still allowing an explicitly qualified
// foreign town.
func (g *Gazetteer) regionDominates(region, other Place) bool {
	if region.Feature != "ADM1" || other.Feature == "ADM1" {
		return false
	}
	population, otherPopulation := g.majorPopulation(region), g.majorPopulation(other)
	return population >= 50000 && otherPopulation > 0 && population >= 5*otherPopulation
}

// capitalLetterOnlyReject narrows the bare single-word city path. It is used
// only for non-capital seats that are accepted on capitalization plus
// population. Proper-name compounds (Buffalo Bills, Orlando Bloom), acronyms
// in mixed-case prose (NEMA), a longer unknown name's connector (Agusan del
// Sur) and common-word/proper-name heads do not become city dots.
func capitalLetterOnlyReject(words []token, start, count int, p Place, population int64) bool {
	if count != 1 {
		return false
	}
	raw := words[start].raw
	if len(raw) >= 2 && len(raw) <= 4 && raw == strings.ToUpper(raw) && mixedCaseText(words) {
		return true
	}
	if start > 0 && !strings.ContainsAny(words[start].sep, "\n.;:!?") {
		previous := words[start-1]
		if population < 200000 && strings.Contains(" del de da das do dos van von der den ", " "+previous.norm+" ") {
			return true
		}
		if previous.norm == "s" && strings.ContainsAny(previous.sep, "'’") {
			return true
		}
	}
	end := start + count
	if end >= len(words) || strings.ContainsAny(words[end].sep, "\n.;:!?") {
		return false
	}
	next := words[end]
	if !capitalized(next.raw) {
		// A lowercase continuation is ordinary prose ("Gaza doctor", "Durban
		// fire chief"); common-word/name ambiguity is handled by the lexicons.
		return false
	}
	if next.norm == normalize(p.Country) || next.norm == normalize(p.Admin) || adminTypeNoun(next.norm) ||
		jurisdictionNoun(next.norm) || actorHead(next.norm) || geographicHeadWord(next.norm) || civicHead(next.norm) ||
		allCapsAcronym(next.raw) {
		return false
	}
	return true
}

func allCapsAcronym(raw string) bool {
	if len(raw) < 2 || len(raw) > 5 || raw != strings.ToUpper(raw) {
		return false
	}
	for _, r := range raw {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func properCompoundHead(words []token, start, count int, p Place) bool {
	end := start + count
	if end >= len(words) || strings.ContainsAny(words[end].sep, "\n.;:!?") || !capitalized(words[end].raw) {
		return false
	}
	next := words[end].norm
	if next == normalize(p.Country) || next == normalize(p.Admin) || adminTypeNoun(next) || jurisdictionNoun(next) {
		return false
	}
	return !(actorHead(next) || geographicHeadWord(next) || civicHead(next))
}

func cityInRegion(city, region Place) bool {
	return city.CountryCode != "" && city.CountryCode == region.CountryCode && city.Feature != "ADM1" &&
		city.AdminCode != "" && city.AdminCode == region.AdminCode
}

func (g *Gazetteer) nationalCapital(p Place) bool {
	country, ok := g.countries[p.CountryCode]
	if !ok || country.Capital == "" || p.Feature != "PPLC" && p.Feature != "PPLG" {
		return false
	}
	return normalize(country.Capital) == normalize(p.Name)
}

func (g *Gazetteer) dominatesCity(region Place) bool {
	for _, other := range g.byName[normalize(region.Name)] {
		if other.ID == region.ID || other.Feature == "ADM1" || other.RegionAlias != "" {
			continue
		}
		if sameJurisdiction(region, other) || g.sameSeatRegion(region, other) || g.sameSeatRegion(other, region) {
			continue
		}
		if g.regionDominates(region, other) {
			return true
		}
	}
	return false
}

func mixedCaseText(words []token) bool {
	for _, w := range words {
		if w.raw != strings.ToUpper(w.raw) {
			return true
		}
	}
	return false
}

func civicHead(word string) bool {
	return strings.Contains(" health ministry department agency airport traffic weather temperature climate hospital hospitals school schools road roads metro transit election elections voters candidates court courts high corporation international national global world ", " "+word+" ")
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
		if p.Feature == "ADM1" && end < len(words) && adminTypeNoun(words[end].norm) {
			end++
		}
		if matches(end) && (dateline || strings.ContainsAny(words[end].sep, ",(")) {
			return true
		}
		if start > len(parts) && words[start-1].norm == "s" && strings.ContainsAny(words[start-1].sep, "'’") && matches(start-len(parts)-1) {
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
	return g.geographicEvidenceFor(p, phrase, words, start, count, dateline, hint, false)
}

func (g *Gazetteer) geographicEvidenceFor(p Place, phrase string, words []token, start, count int, dateline bool, hint string, historical bool) int {
	// No-data regions may remain explicit lookup aliases, but cannot turn a
	// bare regional name into an automatic dot at another country's capital.
	if p.RegionAlias != "" || !ValidCoordinates(p.Lat, p.Lon) || !capitalized(words[start].raw) {
		return 0
	}
	if end := start + count; end < len(words) && !strings.ContainsAny(words[end].sep, "\n.;:!?") && incompatibleFeatureSuffix(p, words[end].norm) &&
		!(p.Feature == "ADM1" && (g.majorPlaceSupported(p) || g.majorPopulation(p) >= 50000)) &&
		!(historical && (strings.HasPrefix(p.Feature, "PPLA") && containerSuffix(words[end].norm) ||
			words[end].norm == "park" && p.Population >= 50000 ||
			words[end].norm == "bridge" && g.majorPlaceSupported(p) && !givenNames[phrase] && !familyNames[phrase] ||
			(g.majorPlaceSupported(p) || seatLike(p)) && (containerSuffix(words[end].norm) || routeContextNearby(words, end)))) {
		// A populated-place coordinate does not establish the position of a
		// separately named river/valley, or an administrative jurisdiction.
		return 0
	}
	canonical := expandedPlaceName(phrase) == expandedPlaceName(p.Name) || p.CountryID && p.CountryCode == "PS" && phrase == "palestine"
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
	for i, w := range words[start : start+count] {
		nameSuffix := w.norm == "s" && strings.ContainsAny(w.sep, "'’")
		// News style lowercases a trailing generic jurisdiction noun even when
		// it is part of the canonical ADM1 name ("Naryn region"). The leading
		// token still has to be capitalized.
		trailingJurisdiction := canonical && p.Feature == "ADM1" && i == count-1 && jurisdictionNoun(w.norm)
		if !capitalized(w.raw) && !nameSuffix && !trailingJurisdiction && !strings.Contains(" de la le del dos do da of the and upon ", " "+w.norm+" ") {
			return 0
		}
	}
	textQualified := independentQualifier(p, words, start, count, dateline)
	organization := !p.CountryID && organizationUse(words, start, count)
	if organization && textQualified && normalize(p.Country) != "" && strings.HasPrefix(tokensText(words[start+count:])+" ", normalize(p.Country)+" ") {
		// United is part of the adjacent country qualifier (United Kingdom/
		// States), not a club suffix. Other organization evidence still rejects.
		organization = false
	}
	// A hyphenated route/compound ("Hassan-Sakleshpur section") is not a
	// personal name even when the first token is also a given name.
	routeCompound := historical && routeContextNearby(words, start+count)
	if (personUse(words, start, count) && !textQualified && !routeCompound) || organization {
		return 0
	}
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
	locative := locativeEvidence(words, start, count)
	explicitCountry := countryName && g.explicitCountryEvidence(p, words, start, count, dateline, hint)
	if countryName && g.foreignRegionCollision(p) && !explicitCountry {
		// A locative such as 'living in Georgia' does not distinguish the
		// country from the US state. Keep both possibilities unlocated unless
		// independent evidence identifies the country or an actual venue.
		return 0
	}
	if countryName && !dateline && !explicitCountry && countryNonGeographicUse(p, words, start, count) {
		return 0
	}
	_, contextualName := contextNames[phrase]
	contextualName = contextualName || givenNames[phrase] || familyNames[phrase]
	geographicHead := geographicHead(words, start, count)
	adminLabel := p.Feature == "ADM1" && start+count < len(words) && adminTypeNoun(words[start+count].norm)
	typedRegion := adminLabel && g.uniqueAdministrativeName(p)
	if contextualName && !historical && !p.CountryID && !qualified && !typedRegion && (p.Feature == "ADM1" || !g.majorPlaceSupported(p)) && g.foreignCanonicalCollision(p) {
		// 'in Victoria' is geographic syntax, but still cannot distinguish
		// the Australian region from several cities in other countries.
		return 0
	}
	// Shared personal names and country/commodity names need actual text
	// evidence. A publisher's country alone cannot locate a person or brand.
	if contextualName && !historical && !p.CountryID && !(textQualified || dateline || locative || typedRegion || geographicHead && g.majorPlaceSupported(p) || explicitCountry || countryName && disasterImpact(words, start)) {
		return 0
	}
	if ambiguous && !(textQualified || hintQualified && (dateline || locative)) {
		return 0
	}
	major := canonical && count == 1 && g.majorPopulation(p) >= 50000 && (historical || (seatLike(p) && g.majorPlaceSupported(p)))
	if historical && canonical {
		return 31
	}
	if canonical && p.Feature == "ADM1" && count == 1 && !g.uniqueRegionName(p) && !major &&
		!(textQualified || dateline || locative || typedRegion || geographicHead && !contextualName && g.dominatesCity(p)) {
		return 0
	}
	if dateline {
		if !canonical && count < 2 && !p.CountryID {
			return 0
		}
		return 70 + boolScore(textQualified, 30) + boolScore(hintQualified, 10) + jurisdictionScore(p)
	}
	if countryName {
		return 35
	}
	if p.CountryID {
		return 34
	}
	if textQualified {
		if !historical && p.Feature != "ADM1" && p.Feature != "PPLC" && p.Feature != "PPLG" && properCompoundHead(words, start, count, p) {
			return 0
		}
		return 65 + jurisdictionScore(p)
	}
	if hintQualified {
		return 55 + jurisdictionScore(p)
	}
	if ambiguous {
		return 0
	}
	if geographicHead {
		return 40
	}
	if locative {
		return 40
	}
	if major && p.Feature != "ADM1" {
		if !historical && !textQualified && !dateline && !locative && p.Feature != "PPLC" && p.Feature != "PPLG" && capitalLetterOnlyReject(words, start, count, p, g.majorPopulation(p)) {
			return 0
		}
		return 33
	}
	if canonical && p.Feature == "ADM1" && count == 1 {
		return 32
	}
	// A full multiword name is more specific than a capitalized surname or
	// brand. Major canonical cities also have population/uniqueness evidence.
	if count > 1 {
		return 30
	}
	return 0
}

// Curated common given/family names: they are lexical ambiguity classes, not
// headline phrases. Adjacent personal-name structure must not become a city.
var givenNames = nameVocabulary(`aaron adam austin sergio nigel adrian ahmed ahmad ali amran andi anwar ariel alan alex alexander alfred alice amanda amy andrew andi anna anne anthony arthur barbara ben benjamin bernard bill bob brian carl carlos charles charlotte cheryl christopher daniel david dennis denzel diana diane darwin donna donald dorothy edward ella emily eric eugene evan francis frank florence fred frederick george gerald harold harry hassan hasan hussein hussain helen henry hugh irene irving isaac jack jacob james jane jason jean jeff jeffrey jennifer jeremy jerry jim joan joe john jon jonathan jose joseph joshua joyce judith julie justin karen kathleen kathryn kelly kenneth kevin laura lawrence leo leonard leslie lewis linda lois louis louise luis lynn marco margaret maria marie marilyn mark martha martin mary matthew michael nancy natalie nicholas nicole norman oliver oscar patrick paul paula peter philip pria priya raymond rebecca richard robert roger ronald rose roy ruth sam samuel santiago sandra sarah scott sharon shawn shirley simon stephen steven susan thomas timothy tom valerie victoria vincent virginia vladimir walter wayne william yahaya`)
var familyNames = nameVocabulary(`smith johnson williams brown jones garcia miller davis rodriguez martinez hernandez lopez gonzalez wilson anderson thomas taylor moore jackson martin lee perez thompson white harris sanchez clark ramirez lewis robinson walker young allen king wright scott torres nguyen hill flores green adams nelson baker hall rivera campbell mitchell carter roberts gomez phillips evans turner diaz parker cruz edwards collins reyes stewart morris morales murphy cook rogers ortiz morgan cooper peterson reed kelly howard kim ward watson brooks wood bennett gray hughes price myers ross foster perry russell bell butler barnes fisher jordan alexander hamilton graham west hayes ford marshall kennedy clinton lincoln washington bolton rubio shevchenko massa farage holmes sweeney bloom warren bello langley lima hilton beckham giuffre berry hollen orden gogh adesanya`)

func nameVocabulary(words string) map[string]bool {
	out := map[string]bool{}
	for _, word := range strings.Fields(words) {
		out[word] = true
	}
	return out
}
func personUse(words []token, start, count int) bool {
	if start > 0 && !strings.ContainsAny(words[start].sep, "\n.;:!?") && capitalized(words[start-1].raw) &&
		(givenNames[words[start-1].norm] || familyNames[words[start-1].norm] && (givenNames[words[start].norm] || familyNames[words[start].norm]) || strings.Contains(" mr mrs ms dr senator governor president professor coach pope ", " "+words[start-1].norm+" ")) {
		return true
	}
	end := start + count
	if end >= len(words) || strings.ContainsAny(words[end].sep, "\n.;:!?,():") || !capitalized(words[end].raw) {
		return false
	}
	if familyNames[words[end].norm] {
		return true
	}
	return count == 1 && givenNames[words[start].norm] && !geographicHead(words, start, count) && !actorHead(words[end].norm)
}
func actorHead(word string) bool {
	return strings.Contains(" flags begins starts businesswomen wholesaler government parliament economy opposition president minister officials forces troops army police military authorities leaders delegation elections election court courts policy announces plans says warns agrees rejects attacks strikes opens sends reports demands calls seeks faces approves expands allows bans launches backs talks votes will has is are can may reopens rolls extends tightens hosts closes debates adopts receives gets makes sees proposes unveils residents population council civic road session schools businesses hospital health and but or was had ", " "+word+" ")
}

func geographicHead(words []token, start, count int) bool {
	end := start + count
	if end < len(words) && words[end].norm == "s" && strings.ContainsAny(words[end].sep, "'’") {
		end++
	}
	return end < len(words) && !strings.ContainsAny(words[end].sep, "\n.;:!?") && geographicHeadWord(words[end].norm)
}

func geographicHeadWord(word string) bool {
	return strings.Contains(" mayor mayoral governor residents population economy municipal municipality council cbd streets hospitals schools province region state prefecture marathon olympics olympic open summit conference expo candidates voters election ", " "+word+" ")
}

func adminTypeNoun(word string) bool {
	return strings.Contains(" region state province prefecture ", " "+word+" ")
}

// Generic jurisdiction tails can be lowercase in running news text while the
// canonical ADM1 name carries them ("Naryn region" for "Naryn Region").
func jurisdictionNoun(word string) bool {
	return strings.Contains(" region state province prefecture district county municipality department canton constituency ", " "+word+" ")
}

// containerSuffix names an enclosing area rather than a conflicting feature:
// a city named in "X region"/"X valley" is still a plausible coarse witness.
func containerSuffix(word string) bool {
	return strings.Contains(" region province district county state prefecture municipality department canton constituency valley area metro metropolitan greater ", " "+word+" ")
}

// routeContextNearby detects a road/structure compound around a matched name
// ("Kabale–Lake Bunyonyi Road", "Hassan-Sakleshpur section"). It is only used
// in historical retention, never to admit a new point.
func routeContextNearby(words []token, end int) bool {
	for i := end; i < min(len(words), end+5); i++ {
		if strings.ContainsAny(words[i].sep, "\n.;:!?") {
			break
		}
		if strings.Contains(" road roads highway freeway section corridor route street avenue boulevard bridge park airport port ", " "+words[i].norm+" ") {
			return true
		}
	}
	return false
}

func seatLike(p Place) bool {
	return p.Feature == "PPLC" || p.Feature == "PPLG" || strings.HasPrefix(p.Feature, "PPLA")
}

func sameJurisdiction(a, b Place) bool {
	return a.CountryCode == b.CountryCode && a.AdminCode != "" && a.AdminCode == b.AdminCode
}

// A region named after its own capital/administrative seat is one jurisdiction,
// even when GeoNames gives the city and the ADM1 row different codes (for
// example Taipei city 1668341 and admin:TW.03). Counting that seat as a foreign
// homonym is what removed valid capital dots such as Taipei.
func (g *Gazetteer) sameSeatRegion(a, b Place) bool {
	if a.CountryCode == "" || a.CountryCode != b.CountryCode || b.Feature != "ADM1" || !seatLike(a) {
		return false
	}
	if a.AdminCode != "" && a.AdminCode == b.AdminCode {
		return true
	}
	if normalize(a.Admin) == normalize(b.Name) {
		return true
	}
	if expandedPlaceName(a.Name) != expandedPlaceName(b.Name) {
		return false
	}
	// A national capital may stand in for the same-named region when it is not
	// dwarfed by that region (this keeps Washington DC from equating to
	// Washington State, while keeping Taipei/La Paz as their own seats).
	if !strings.HasPrefix(a.Feature, "PPLA") {
		if a.Feature == "PPLC" {
			regionPopulation := g.majorPopulation(b)
			// A same-named region with no indexed urban population cannot
			// contradict the national capital's identity as its seat
			// (Taipei/TW.03).
			return a.Population > 0 && (regionPopulation == 0 || a.Population >= regionPopulation)
		}
		return false
	}
	return false
}

func (g *Gazetteer) majorPopulation(p Place) int64 {
	if p.Feature == "ADM1" {
		return g.regionPopulation[p.CountryCode+"."+p.AdminCode]
	}
	return p.Population
}

// Capitalization can support an unambiguous major canonical place. Where a
// foreign homonym exists, only a capital/administrative seat that dominates it
// by at least five to one is admitted; population alone never rescues aliases.
func (g *Gazetteer) majorPlaceSupported(p Place) bool {
	population := g.majorPopulation(p)
	if population < 50000 {
		return false
	}
	largestOther := int64(0)
	conflict := false
	for _, other := range g.byName[normalize(p.Name)] {
		if other.RegionAlias != "" || expandedPlaceName(other.Name) != expandedPlaceName(p.Name) || other.ID == p.ID ||
			sameJurisdiction(p, other) || g.sameSeatRegion(p, other) || g.sameSeatRegion(other, p) {
			continue
		}
		if other.Feature == "ADM1" && g.nationalCapital(p) && other.AdminCode != p.AdminCode {
			// A national capital and a same-named first-level region are different
			// jurisdictions (Buenos Aires city/province, Taipei, Washington DC/WA).
			continue
		}
		conflict = true
		otherPopulation := g.majorPopulation(other)
		if otherPopulation == 0 {
			return false
		}
		largestOther = max(largestOther, otherPopulation)
	}
	if !conflict {
		return seatLike(p)
	}
	return (p.Feature == "ADM1" || seatLike(p)) && population >= 5*largestOther
}

// A multiword homonym resolves only to the exact canonical name of a capital
// or administrative seat that dominates the foreign candidate five to one, as
// majorPlaceSupported requires for single words. Aliases never qualify.
func (g *Gazetteer) dominatesHomonym(p, other Place, phrase string) bool {
	if p.RegionAlias != "" || expandedPlaceName(p.Name) != expandedPlaceName(phrase) {
		return false
	}
	if p.Feature != "ADM1" && !seatLike(p) {
		return false
	}
	population, otherPopulation := g.majorPopulation(p), g.majorPopulation(other)
	return population >= 50000 && otherPopulation > 0 && population >= 5*otherPopulation
}

// Named organizations are not literal city evidence. A following City Council
// is geographic administration; City/United/FC in club names are not.
func organizationUse(words []token, start, count int) bool {
	if start > 0 && strings.Contains(" real atletico bayern borussia deportivo sporting inter dinamo dynamo lokomotiv pss persebaya persib psm fc cf afc fk nk ", " "+words[start-1].norm+" ") {
		return true
	}
	end := start + count
	if end >= len(words) || strings.ContainsAny(words[end].sep, "\n.;:!?") {
		return false
	}
	if end+1 < len(words) && (words[end].norm == "st" || words[end].norm == "saint") && words[end+1].norm == "germain" {
		return true
	}
	if capitalized(words[end].raw) && strings.Contains(" post times herald gazette chronicle tribune journal news ", " "+words[end].norm+" ") && (end+1 >= len(words) || words[end+1].norm != "office") {
		return true
	}
	if words[end].norm == "city" {
		return end+1 >= len(words) || !strings.Contains(" council government hall police administration officials ", " "+words[end+1].norm+" ")
	}
	if strings.Contains(" united pirates fc cf capitals hotspur rovers wanderers bucks lakers dodgers rams yankees mets celtics ", " "+words[end].norm+" ") {
		return true
	}
	if strings.Contains(" sign signs signed sack sacks sacked beat beats defeat defeats draw draws win wins lose loses appoint appoints ", " "+words[end].norm+" ") {
		sport := false
		for i := end + 1; i < min(len(words), end+8); i++ {
			if strings.ContainsAny(words[i].sep, "\n.;:!?") {
				break
			}
			if words[i].norm == "bid" || words[i].norm == "host" {
				return false
			}
			if strings.Contains(" manager midfielder striker forward player players coach goal match club team football league cup defender captain winger ", " "+words[i].norm+" ") {
				sport = true
			}
		}
		return sport
	}
	return false
}

// Countries can be actors and possessives without a locative. Ambiguous
// personal names still reject an adjacent proper surname; ordinary political
// and economic heads are admitted even in title case. Commodity senses have
// separate lexical cues instead of blocking all uses of Turkey/Reunion.
func countryNonGeographicUse(p Place, words []token, start, count int) bool {
	name := normalize(p.Name)
	end := start + count
	if end >= len(words) || strings.ContainsAny(words[end].sep, "\n.;:!?") {
		return false
	}
	next := words[end]
	if next.norm == "s" && strings.ContainsAny(next.sep, "'’") {
		return false
	}
	if name == "turkey" && strings.Contains(" price prices meat poultry roast roasting breast breasts sandwich sandwiches ", " "+next.norm+" ") {
		return true
	}
	if name == "reunion" && strings.Contains(" tour tours concert concerts band album dates ", " "+next.norm+" ") {
		return true
	}
	if !strings.Contains(" israel chad jordan ", " "+name+" ") || !capitalized(next.raw) {
		return false
	}
	return !actorHead(next.norm)
}

func incompatibleFeatureSuffix(p Place, next string) bool {
	natural := strings.Contains(" river lake valley mountain range forest park island islands reservoir canal harbour harbor street avenue road lane boulevard drive bridge overpass interchange highway freeway tunnel ", " "+next+" ")
	admin := strings.Contains(" province state county district region prefecture constituency canton municipality department ", " "+next+" ")
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
	"constantine": {}, "sofia": {}, "sal": {}, "paola": {}, "indiana": {}, "washington": {},
	"bournemouth": {}, "dortmund": {}, "southampton": {}, "liverpool": {}, "fulham": {}, "tottenham": {}, "ipswich": {}, "sunderland": {}, "stellenbosch": {}, "koln": {},
	"patra": {}, "arak": {}, "rahat": {}, "fullerton": {}, "phoenix": {}, "chelsea": {},
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

func (g *Gazetteer) uniqueAdministrativeName(p Place) bool {
	for _, other := range g.byName[normalize(p.Name)] {
		if other.Feature == "ADM1" && other.RegionAlias == "" && expandedPlaceName(other.Name) == expandedPlaceName(p.Name) && !sameJurisdiction(p, other) {
			return false
		}
	}
	return true
}

// The capitalization-only ADM1 shortcut is limited to canonical names with
// one geographic meaning. Cities, countries and other regions with the same
// name require a dateline, locative or independently stated qualifier.
func (g *Gazetteer) uniqueRegionName(p Place) bool {
	for _, other := range g.byName[normalize(p.Name)] {
		if other.RegionAlias != "" || normalize(other.Name) != normalize(p.Name) || other.ID == p.ID {
			continue
		}
		if sameJurisdiction(p, other) || g.sameSeatRegion(p, other) || g.sameSeatRegion(other, p) {
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
