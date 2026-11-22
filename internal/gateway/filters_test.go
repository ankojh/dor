package gateway

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func filterWireRow() *dorv1.WireRow {
	return &dorv1.WireRow{
		ArticleId:     "article-1",
		StoryId:       "story-1",
		Title:         "Wildfire near Athens",
		Source:        "Example Wire",
		Place:         "Athens",
		Country:       "GR",
		Lang:          "en",
		Category:      dorv1.Category_CATEGORY_DISASTER,
		Located:       true,
		Lat:           37.98,
		Lon:           23.72,
		PromptScores:  map[string]float32{"wildfire": 0.9, "unrelated": 0.2},
		ApproxPrompts: []string{"wildfire", "unrelated"},
	}
}

func TestMatchesFilters(t *testing.T) {
	stories := map[string]*dorv1.Story{
		"story-1": {
			Id:          "story-1",
			SourceCount: 3,
			Locations:   []*dorv1.Location{{Name: "Attica"}, {Name: "Greece"}},
		},
	}
	row := filterWireRow()

	tests := []struct {
		name string
		row  *dorv1.WireRow
		f    *dorv1.SetFilters
		want bool
	}{
		{name: "no filters accepts", row: row, f: nil, want: true},
		{name: "empty filters accept", row: row, f: &dorv1.SetFilters{}, want: true},
		{name: "category accepted", row: row, f: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}}, want: true},
		{name: "category rejected", row: row, f: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_SPORTS}}, want: false},
		{name: "source accepted case and space insensitive", row: row, f: &dorv1.SetFilters{Sources: []string{"  example wire "}}, want: true},
		{name: "source rejected", row: row, f: &dorv1.SetFilters{Sources: []string{"other"}}, want: false},
		{name: "language accepted", row: row, f: &dorv1.SetFilters{Lang: []string{"EN"}}, want: true},
		{name: "language rejected", row: row, f: &dorv1.SetFilters{Lang: []string{"fr"}}, want: false},
		{name: "iso region accepted", row: row, f: &dorv1.SetFilters{Regions: []string{"gr"}}, want: true},
		{name: "iso region does not substring match", row: row, f: &dorv1.SetFilters{Regions: []string{"us"}}, want: false},
		{name: "region matches row place", row: row, f: &dorv1.SetFilters{Regions: []string{"athens"}}, want: true},
		{name: "region matches story location", row: row, f: &dorv1.SetFilters{Regions: []string{"attica"}}, want: true},
		{name: "region rejects unrelated", row: row, f: &dorv1.SetFilters{Regions: []string{"berlin"}}, want: false},
		{name: "min sources accepted at equality", row: row, f: &dorv1.SetFilters{MinSources: 3}, want: true},
		{name: "min sources rejected", row: row, f: &dorv1.SetFilters{MinSources: 4}, want: false},
		{name: "min sources passes when story unknown", row: &dorv1.WireRow{StoryId: "missing"}, f: &dorv1.SetFilters{MinSources: 10}, want: true},
		{name: "radius accepted", row: row, f: &dorv1.SetFilters{Lat: 37.98, Lon: 23.72, RadiusKm: 5}, want: true},
		{name: "radius rejected", row: row, f: &dorv1.SetFilters{Lat: 38.98, Lon: 23.72, RadiusKm: 5}, want: false},
		{name: "radius rejects unlocated row", row: &dorv1.WireRow{Located: false, Lat: 37.98, Lon: 23.72}, f: &dorv1.SetFilters{Lat: 37.98, Lon: 23.72, RadiusKm: 100}, want: false},
		{name: "radius rejects invalid coordinates", row: &dorv1.WireRow{Located: true, Lat: 200, Lon: 23.72}, f: &dorv1.SetFilters{Lat: 37.98, Lon: 23.72, RadiusKm: 10000}, want: false},
		{name: "all constraints together", row: row, f: &dorv1.SetFilters{
			Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER},
			Sources:    []string{"Example Wire"},
			Lang:       []string{"en"},
			Regions:    []string{"GR"},
			MinSources: 2,
			Lat:        37.98,
			Lon:        23.72,
			RadiusKm:   10,
		}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesFilters(tc.row, tc.f, stories); got != tc.want {
				t.Fatalf("matchesFilters = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterWireProjectsPromptsAndCopies(t *testing.T) {
	row := filterWireRow()

	t.Run("no prompts strips private fields", func(t *testing.T) {
		out, ok := filterWire(row, filterState{}, nil)
		if !ok {
			t.Fatal("row rejected without filters")
		}
		if len(out.PromptScores) != 0 || len(out.ApproxPrompts) != 0 {
			t.Fatalf("private prompt data survived projection: %+v", out)
		}
		if out.Title != row.Title || out.ArticleId != row.ArticleId {
			t.Fatalf("public fields changed: %+v", out)
		}
		if out == row {
			t.Fatal("filterWire returned the original row, want a copy")
		}
	})

	t.Run("prompts are intersected", func(t *testing.T) {
		out, ok := filterWire(row, filterState{prompts: map[string]bool{"wildfire": true}}, nil)
		if !ok {
			t.Fatal("row rejected")
		}
		if len(out.PromptScores) != 1 || out.PromptScores["wildfire"] != 0.9 {
			t.Fatalf("prompt scores = %+v, want only wildfire", out.PromptScores)
		}
		if len(out.ApproxPrompts) != 1 || out.ApproxPrompts[0] != "wildfire" {
			t.Fatalf("approx prompts = %v, want only wildfire", out.ApproxPrompts)
		}
	})

	t.Run("filtered-out row returns nil", func(t *testing.T) {
		f := filterState{filters: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_SPORTS}}}
		if out, ok := filterWire(row, f, nil); ok || out != nil {
			t.Fatalf("filterWire = %+v, %v, want nil, false", out, ok)
		}
	})

	t.Run("nil row returns nil", func(t *testing.T) {
		if out, ok := filterWire(nil, filterState{}, nil); ok || out != nil {
			t.Fatalf("filterWire(nil) = %+v, %v", out, ok)
		}
	})

	if len(row.PromptScores) != 2 || len(row.ApproxPrompts) != 2 {
		t.Fatal("filterWire mutated the replay row")
	}
}

// A filtered delta is replaced by a seq-only no-op, never dropped: the global
// sequence must stay contiguous for every client. These tests pin the exact
// body/no-op split and that no private text survives in the encoded bytes.
func TestFilterDeltaBatchKeepsMatchingWireAndOwnedAlert(t *testing.T) {
	stories := map[string]*dorv1.Story{
		"story-1": {Id: "story-1", SourceCount: 3},
	}
	f := filterState{
		filters: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}},
		prompts: map[string]bool{"wildfire": true},
	}
	batch := &dorv1.DeltaBatch{Deltas: []*dorv1.Delta{
		{Seq: 1, Body: &dorv1.Delta_Wire{Wire: filterWireRow()}},
		{Seq: 2, Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{StoryId: "story-1", Category: dorv1.Category_CATEGORY_SPORTS}}},
		{Seq: 3, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "story-1"}}},
		{Seq: 4, Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{AlertId: "mine", StoryId: "story-1", Prompt: "wildfire"}}},
		{Seq: 5, Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{AlertId: "theirs", StoryId: "story-1", Prompt: "other"}}},
	}}
	owners := map[string]string{"mine": "session-a", "theirs": "session-b"}

	out := filterDeltaBatch(batch, f, stories, "session-a", func(id string) string { return owners[id] })
	if out == nil {
		t.Fatal("batch with matching rows filtered to nil")
	}
	if len(out.Deltas) != 5 {
		t.Fatalf("kept %d deltas, want one slot per input delta", len(out.Deltas))
	}
	for i, want := range []uint64{1, 2, 3, 4, 5} {
		if out.Deltas[i].Seq != want {
			t.Fatalf("delta %d seq = %d, want %d (sequences must stay in order)", i, out.Deltas[i].Seq, want)
		}
	}

	wire := out.Deltas[0].GetWire()
	if wire == nil || wire.ArticleId != "article-1" {
		t.Fatalf("first delta = %+v, want the matching wire body", out.Deltas[0])
	}
	if len(wire.PromptScores) != 1 {
		t.Fatalf("kept wire was not prompt-projected: %+v", wire)
	}

	// The non-matching wire keeps its seq but none of its body.
	if out.Deltas[1].Body != nil || out.Deltas[1].GetWire() != nil {
		t.Fatalf("non-matching wire leaked its body: %+v", out.Deltas[1])
	}
	if out.Deltas[2].GetStoryUpsert() == nil {
		t.Fatalf("non-wire delta was not passed through: %+v", out.Deltas[2])
	}
	if hit := out.Deltas[3].GetAlert(); hit == nil || hit.AlertId != "mine" {
		t.Fatalf("alert delta = %+v, want only the owned alert body", out.Deltas[3])
	}
	// Another session's alert keeps its seq as a no-op.
	if out.Deltas[4].Body != nil || out.Deltas[4].GetAlert() != nil {
		t.Fatalf("non-owned alert leaked its body: %+v", out.Deltas[4])
	}

	if len(batch.Deltas) != 5 {
		t.Fatalf("filterDeltaBatch mutated the input batch: %d deltas", len(batch.Deltas))
	}
	if batch.Deltas[1].GetWire() == nil || batch.Deltas[4].GetAlert() == nil {
		t.Fatal("filterDeltaBatch stripped bodies from the input batch")
	}
}

func TestFilterDeltaBatchAlertPrivacyFailsClosed(t *testing.T) {
	alert := func(id string) *dorv1.Delta {
		return &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_Alert{Alert: &dorv1.AlertHit{AlertId: id}}}
	}
	tests := []struct {
		name      string
		sessionID string
		owner     func(string) string
		wantBody  bool
	}{
		{name: "owned alert delivered", sessionID: "s1", owner: func(string) string { return "s1" }, wantBody: true},
		{name: "other session no-op", sessionID: "s2", owner: func(string) string { return "s1" }},
		{name: "unknown owner no-op", sessionID: "s1", owner: func(string) string { return "" }},
		{name: "missing resolver no-op", sessionID: "s1", owner: nil},
		{name: "missing session no-op", sessionID: "", owner: func(string) string { return "s1" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := filterDeltaBatch(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{alert("secret-alert")}}, filterState{}, nil, tc.sessionID, tc.owner)
			if out == nil || len(out.Deltas) != 1 {
				t.Fatalf("output = %+v, want one accounting delta", out)
			}
			delta := out.Deltas[0]
			if delta.Seq != 1 {
				t.Fatalf("delta seq = %d, want 1", delta.Seq)
			}
			delivered := delta.GetAlert() != nil
			if delivered != tc.wantBody {
				t.Fatalf("alert delivered = %v, want %v", delivered, tc.wantBody)
			}
			if tc.wantBody {
				return
			}
			if delta.Body != nil {
				t.Fatalf("suppressed alert kept a body: %+v", delta)
			}
			raw, err := proto.Marshal(delta)
			if err != nil {
				t.Fatalf("marshal no-op: %v", err)
			}
			if bytes.Contains(raw, []byte("secret-alert")) {
				t.Fatalf("no-op leaked the private alert id: %s", raw)
			}
		})
	}
}

func TestFilterDeltaBatchEmptyInputs(t *testing.T) {
	if out := filterDeltaBatch(nil, filterState{}, nil, "", nil); out != nil {
		t.Fatalf("nil batch = %+v, want nil", out)
	}
	if out := filterDeltaBatch(&dorv1.DeltaBatch{}, filterState{}, nil, "", nil); out != nil {
		t.Fatalf("empty batch = %+v, want nil", out)
	}
	noMatch := &dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq:  1,
		Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{Category: dorv1.Category_CATEGORY_SPORTS}},
	}}}
	f := filterState{filters: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}}}
	out := filterDeltaBatch(noMatch, f, nil, "", nil)
	if out == nil || len(out.Deltas) != 1 {
		t.Fatalf("all-filtered batch = %+v, want one seq-only no-op", out)
	}
	if out.Deltas[0].Seq != 1 || out.Deltas[0].Body != nil {
		t.Fatalf("all-filtered delta = %+v, want seq-only no-op seq 1", out.Deltas[0])
	}
}

func TestFilterDeltaBatchFilteredWireNoOpDoesNotLeak(t *testing.T) {
	batch := &dorv1.DeltaBatch{Deltas: []*dorv1.Delta{{
		Seq: 11,
		Body: &dorv1.Delta_Wire{Wire: &dorv1.WireRow{
			ArticleId:     "private-article",
			StoryId:       "story-1",
			Title:         "private title",
			Category:      dorv1.Category_CATEGORY_SPORTS,
			PromptScores:  map[string]float32{"secret-prompt": 0.9},
			ApproxPrompts: []string{"secret-prompt"},
		}},
	}}}
	f := filterState{
		filters: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}},
		prompts: map[string]bool{"secret-prompt": true},
	}

	out := filterDeltaBatch(batch, f, nil, "session-a", nil)
	if out == nil || len(out.Deltas) != 1 {
		t.Fatalf("output = %+v, want one seq-only no-op", out)
	}
	delta := out.Deltas[0]
	if delta.Seq != 11 || delta.Body != nil {
		t.Fatalf("filtered wire delta = %+v, want seq-only no-op seq 11", delta)
	}
	raw, err := proto.Marshal(out)
	if err != nil {
		t.Fatalf("marshal filtered batch: %v", err)
	}
	for _, secret := range []string{"private-article", "private title", "secret-prompt"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("filtered wire no-op leaked %q: %s", secret, raw)
		}
	}
}

func TestFilterSnapshotFiltersAndCopiesRecentWire(t *testing.T) {
	snap := &dorv1.Snapshot{
		Seq: 9,
		RecentWire: []*dorv1.WireRow{
			filterWireRow(),
			{ArticleId: "sports", Category: dorv1.Category_CATEGORY_SPORTS},
		},
	}
	f := filterState{
		filters: &dorv1.SetFilters{Categories: []dorv1.Category{dorv1.Category_CATEGORY_DISASTER}},
		prompts: map[string]bool{"wildfire": true},
	}
	out := filterSnapshot(snap, f, nil)
	if out == nil {
		t.Fatal("filterSnapshot returned nil for a non-empty snapshot")
	}
	if out.Seq != 9 {
		t.Fatalf("snapshot seq = %d, want 9", out.Seq)
	}
	if len(out.RecentWire) != 1 || out.RecentWire[0].ArticleId != "article-1" {
		t.Fatalf("recent wire = %+v, want only the matching row", out.RecentWire)
	}
	if len(out.RecentWire[0].PromptScores) != 1 {
		t.Fatalf("snapshot wire not prompt-projected: %+v", out.RecentWire[0])
	}
	if len(snap.RecentWire) != 2 || len(snap.RecentWire[0].PromptScores) != 2 {
		t.Fatal("filterSnapshot mutated the input snapshot")
	}
}

func TestFilterSnapshotNilWithoutRecentWire(t *testing.T) {
	if out := filterSnapshot(nil, filterState{}, nil); out != nil {
		t.Fatalf("nil snapshot = %+v", out)
	}
	if out := filterSnapshot(&dorv1.Snapshot{Seq: 1}, filterState{}, nil); out != nil {
		t.Fatalf("snapshot without wire = %+v", out)
	}
}

func TestWithinRadius(t *testing.T) {
	tests := []struct {
		name                     string
		lat, lon, rowLat, rowLon float64
		radius                   float64
		want                     bool
	}{
		{name: "same point", lat: 37.98, lon: 23.72, rowLat: 37.98, rowLon: 23.72, radius: 1, want: true},
		{name: "nearby within radius", lat: 37.98, lon: 23.72, rowLat: 37.99, rowLon: 23.73, radius: 5, want: true},
		{name: "far outside radius", lat: 37.98, lon: 23.72, rowLat: 40.71, rowLon: -74.0, radius: 100, want: false},
		{name: "zero radius matches only the same point", lat: 37.98, lon: 23.72, rowLat: 38.2, rowLon: 23.72, radius: 0, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := withinRadius(tc.lat, tc.lon, tc.rowLat, tc.rowLon, tc.radius); got != tc.want {
				t.Fatalf("withinRadius = %v, want %v", got, tc.want)
			}
		})
	}
}
