package gateway

import (
	"math"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

const (
	// maxClientPrompts bounds one connection's prompt-filter set. The UI
	// normally has one or two; the cap keeps a script from making the matcher
	// evaluate an unbounded list.
	maxClientPrompts = 8
	// maxPromptRunes bounds one prompt. Longer prompts are truncated rather
	// than rejected so a paste does not fail silently in the UI.
	maxPromptRunes = 300
)

// boundPrompts trims, deduplicates and bounds a client's prompt list. It is
// the single admission gate for prompt text entering the gateway and matcher.
func boundPrompts(prompts []string) []string {
	out := make([]string, 0, min(len(prompts), maxClientPrompts))
	seen := make(map[string]bool, len(out))
	for _, prompt := range prompts {
		prompt = strings.TrimSpace(prompt)
		if prompt == "" || seen[prompt] {
			continue
		}
		if utf8.RuneCountInString(prompt) > maxPromptRunes {
			runes := []rune(prompt)
			prompt = strings.TrimSpace(string(runes[:maxPromptRunes]))
		}
		if prompt == "" || seen[prompt] {
			continue
		}
		seen[prompt] = true
		out = append(out, prompt)
		if len(out) >= maxClientPrompts {
			break
		}
	}
	return out
}

// wireHasPrivate reports whether a wire row carries per-client prompt data.
func wireHasPrivate(row *dorv1.WireRow) bool {
	return row != nil && (len(row.PromptScores) > 0 || len(row.ApproxPrompts) > 0)
}

// batchHasPrivateWire reports whether any delta in a batch needs per-client
// prompt projection, even for a client with no filters of its own.
func batchHasPrivateWire(batch *dorv1.DeltaBatch) bool {
	if batch == nil {
		return false
	}
	for _, delta := range batch.Deltas {
		if wireHasPrivate(delta.GetWire()) {
			return true
		}
	}
	return false
}

// batchHasAlert reports whether a batch carries a saved-alert delta. Alert
// hits include the alert prompt and are private to the owning session.
func batchHasAlert(batch *dorv1.DeltaBatch) bool {
	if batch == nil {
		return false
	}
	for _, delta := range batch.Deltas {
		if delta.GetAlert() != nil {
			return true
		}
	}
	return false
}

// snapshotHasPrivateWire reports whether a snapshot's recent wire carries
// per-client prompt data.
func snapshotHasPrivateWire(snap *dorv1.Snapshot) bool {
	if snap == nil {
		return false
	}
	for _, row := range snap.RecentWire {
		if wireHasPrivate(row) {
			return true
		}
	}
	return false
}

// filterState is a client's live filter view, captured once per broadcast so a
// SetFilters/SetPrompts racing the fan-out is applied atomically for that
// message.
type filterState struct {
	filters *dorv1.SetFilters
	prompts map[string]bool
}

// active reports whether the client needs a private delta encoding.
func (f filterState) active() bool {
	return len(f.prompts) > 0 || hasWireConstraints(f.filters)
}

func hasWireConstraints(f *dorv1.SetFilters) bool {
	if f == nil {
		return false
	}
	return len(f.Categories) > 0 || len(f.Regions) > 0 || f.MinSources > 0 ||
		len(f.Lang) > 0 || len(f.Sources) > 0 || f.RadiusKm > 0
}

// snapshot copies the client's current filters under its own lock.
func (c *client) filterState() filterState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := filterState{filters: c.filters}
	if p := c.prompts; p != nil && len(p.Prompts) > 0 {
		st.prompts = make(map[string]bool, len(p.Prompts))
		for _, prompt := range p.Prompts {
			if prompt = strings.TrimSpace(prompt); prompt != "" {
				st.prompts[prompt] = true
			}
		}
	}
	return st
}

// filterWire applies server-side filters and prompt projection to one wire
// row. The returned row is always a copy so the replay ring is never mutated.
func filterWire(row *dorv1.WireRow, f filterState, stories map[string]*dorv1.Story) (*dorv1.WireRow, bool) {
	if row == nil {
		return nil, false
	}
	if !matchesFilters(row, f.filters, stories) {
		return nil, false
	}
	out := proto.Clone(row).(*dorv1.WireRow)
	if len(f.prompts) == 0 {
		out.PromptScores = nil
		out.ApproxPrompts = nil
		return out, true
	}
	if len(out.PromptScores) > 0 {
		scores := make(map[string]float32, len(out.PromptScores))
		for prompt, score := range out.PromptScores {
			if f.prompts[prompt] {
				scores[prompt] = score
			}
		}
		out.PromptScores = scores
	}
	if len(out.ApproxPrompts) > 0 {
		approx := make([]string, 0, len(out.ApproxPrompts))
		for _, prompt := range out.ApproxPrompts {
			if f.prompts[prompt] {
				approx = append(approx, prompt)
			}
		}
		out.ApproxPrompts = approx
	}
	return out, true
}

// matchesFilters is the server-side AND of category, region, source, language,
// minimum sources and radius filters.
func matchesFilters(row *dorv1.WireRow, f *dorv1.SetFilters, stories map[string]*dorv1.Story) bool {
	if f == nil {
		return true
	}
	if len(f.Categories) > 0 {
		found := false
		for _, category := range f.Categories {
			if category == row.Category {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(f.Sources) > 0 {
		found := false
		for _, source := range f.Sources {
			if strings.EqualFold(strings.TrimSpace(source), row.Source) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(f.Lang) > 0 {
		found := false
		for _, lang := range f.Lang {
			if strings.EqualFold(strings.TrimSpace(lang), row.Lang) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(f.Regions) > 0 && !matchesRegion(row, f.Regions, stories) {
		return false
	}
	if f.MinSources > 0 {
		if story := stories[row.StoryId]; story != nil && story.SourceCount < f.MinSources {
			return false
		}
	}
	if f.RadiusKm > 0 {
		if !row.Located || invalidCoords(row.Lat, row.Lon) ||
			!withinRadius(f.Lat, f.Lon, row.Lat, row.Lon, f.RadiusKm) {
			return false
		}
	}
	return true
}

func invalidCoords(lat, lon float64) bool {
	return lat < -90 || lat > 90 || lon < -180 || lon > 180
}

func matchesRegion(row *dorv1.WireRow, regions []string, stories map[string]*dorv1.Story) bool {
	candidates := []string{row.Country, row.Place}
	if story := stories[row.StoryId]; story != nil {
		for _, loc := range story.Locations {
			candidates = append(candidates, loc.Name)
		}
	}
	for _, region := range regions {
		region = strings.ToLower(strings.TrimSpace(region))
		if region == "" {
			continue
		}
		// A two-letter region is an ISO country code; substring matching
		// would make "US" match "Russia".
		if len(region) == 2 {
			if strings.EqualFold(strings.TrimSpace(row.Country), region) {
				return true
			}
			continue
		}
		for _, candidate := range candidates {
			if candidate != "" && strings.Contains(strings.ToLower(candidate), region) {
				return true
			}
		}
	}
	return false
}

// withinRadius is the haversine distance between the client's filter point and
// the row's place.
func withinRadius(lat, lon, rowLat, rowLon, radiusKm float64) bool {
	const earthKm = 6371.0088
	lat1 := lat * math.Pi / 180
	lat2 := rowLat * math.Pi / 180
	dLat := (rowLat - lat) * math.Pi / 180
	dLon := (rowLon - lon) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthKm*2*math.Atan2(math.Sqrt(a), math.Sqrt(1-a)) <= radiusKm
}

// filterDeltaBatch returns a copy with wire rows that do not match removed,
// prompt scores projected to the client's prompts, and alert deltas kept only
// when the client's session owns the alert. Non-wire/non-alert deltas always
// pass.
//
// A filtered delta is replaced by a seq-only no-op rather than dropped. The
// gateway's sequence is global, so dropping a private alert or a non-matching
// wire row would leave a hole that a client cannot distinguish from lost
// transport frames. The no-op advances the client's contiguous seq without
// exposing the filtered body, keeping privacy and gap detection intact.
func filterDeltaBatch(batch *dorv1.DeltaBatch, f filterState, stories map[string]*dorv1.Story, sessionID string, alertOwner func(string) string) *dorv1.DeltaBatch {
	if batch == nil || len(batch.Deltas) == 0 {
		return nil
	}
	out := make([]*dorv1.Delta, 0, len(batch.Deltas))
	for _, delta := range batch.Deltas {
		if delta == nil {
			continue
		}
		if hit := delta.GetAlert(); hit != nil {
			// Fail closed: an unknown owner, an unowned legacy alert, or a
			// missing resolver delivers the hit to nobody.
			if sessionID != "" && alertOwner != nil {
				if owner := alertOwner(hit.AlertId); owner != "" && owner == sessionID {
					out = append(out, delta)
					continue
				}
			}
			out = append(out, &dorv1.Delta{Seq: delta.Seq})
			continue
		}
		wire := delta.GetWire()
		if wire == nil {
			out = append(out, delta)
			continue
		}
		filtered, ok := filterWire(wire, f, stories)
		if !ok {
			out = append(out, &dorv1.Delta{Seq: delta.Seq})
			continue
		}
		out = append(out, &dorv1.Delta{
			Seq:  delta.Seq,
			Body: &dorv1.Delta_Wire{Wire: filtered},
		})
	}
	return &dorv1.DeltaBatch{Deltas: out}
}

// filterSnapshot returns a copy of the snapshot whose recent wire is filtered
// for this client and stripped of every other client's prompt data. It is nil
// when there is no recent wire to project.
func filterSnapshot(snap *dorv1.Snapshot, f filterState, stories map[string]*dorv1.Story) *dorv1.Snapshot {
	if snap == nil || len(snap.RecentWire) == 0 {
		return nil
	}
	out := proto.Clone(snap).(*dorv1.Snapshot)
	out.RecentWire = out.RecentWire[:0]
	for _, row := range snap.RecentWire {
		if filtered, ok := filterWire(row, f, stories); ok {
			out.RecentWire = append(out.RecentWire, filtered)
		}
	}
	return out
}
