package geo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestLocationRepairBundleRunIDDeterministicAndCanonical(t *testing.T) {
	b := validRepairBundle()
	first, err := LocationRepairBundleRunID(b)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LocationRepairBundleRunID(b)
	if err != nil || first != second {
		t.Fatalf("same bundle should have stable identity: %q, %q, err=%v", first, second, err)
	}
	b.RunID = first
	if err := ValidateLocationRepairBundle(b); err != nil {
		t.Fatalf("valid bundle rejected: %v", err)
	}
	changed := b
	changed.Articles = append([]ArticleLocationPatch(nil), b.Articles...)
	changed.Articles[0].EventCandidate.Place = "Boston"
	changedID, err := LocationRepairBundleRunID(changed)
	if err != nil || changedID == first {
		t.Fatalf("run identity must bind event candidate data: original=%q changed=%q err=%v", first, changedID, err)
	}
	if b.RunID != first || b.Articles[0].EventCandidate.Place != "Paris" {
		t.Fatal("identity computation mutated its input bundle")
	}
}

func TestValidateLocationRepairBundleRejectsMalformedProposals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*LocationRepairBundle)
	}{
		{"bad dataset digest", func(b *LocationRepairBundle) { b.DatasetSHA256 = "not-a-digest" }},
		{"unsupported version", func(b *LocationRepairBundle) { b.Version = 2 }},
		{"wrong revision binding", func(b *LocationRepairBundle) { b.RevisionBinding = "invented" }},
		{"unsorted story IDs", func(b *LocationRepairBundle) { b.Stories = append(b.Stories, b.Stories[0]) }},
		{"unsorted vote IDs", func(b *LocationRepairBundle) {
			b.Stories[0].Locations = append(b.Stories[0].Locations, LocationVote{PlaceID: "aaa", Lat: 1, Lon: 1})
		}},
		{"invalid coordinate", func(b *LocationRepairBundle) { b.Stories[0].Locations[0].Lat = 91 }},
		{"invalid confidence", func(b *LocationRepairBundle) {
			b.Articles[0].Locations = []LocationVote{{PlaceID: "paris", Lat: 48.8566, Lon: 2.3522, Confidence: 1.1}}
			b.Articles[0].Unlocated = false
		}},
		{"missing hot evidence requirement", func(b *LocationRepairBundle) { b.Articles[0].NeedsHotEvidence = false }},
		{"nil arrays", func(b *LocationRepairBundle) { b.Articles = nil }},
		{"bad primary", func(b *LocationRepairBundle) { b.Stories[0].PrimaryPlaceID = "other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := validRepairBundle()
			tt.mutate(&b)
			signRepairBundle(t, &b)
			if err := ValidateLocationRepairBundle(b); err == nil {
				t.Fatalf("malformed proposal accepted: %+v", b)
			}
		})
	}
}

func TestBundleSchemaDoesNotClaimFinalWireOrHotStatePermission(t *testing.T) {
	b := validRepairBundle()
	signRepairBundle(t, &b)
	encoded, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if strings.Contains(text, `"wire"`) || strings.Contains(text, `"final_wire"`) || strings.Contains(text, `"serving_permission"`) {
		t.Fatalf("DB proposal must not claim resolved final wire or serving permission: %s", text)
	}
	if err := ValidateLocationRepairBundle(b); err != nil {
		t.Fatalf("well-formed DB-only proposal rejected: %v", err)
	}
}

func validRepairBundle() LocationRepairBundle {
	hash := func(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
	return LocationRepairBundle{
		Version: 1, DatasetSHA256: hash("geonames"), RevisionBinding: RepairRevisionBinding,
		Stories:  []StoryLocationPatch{{ID: "story-1", DBBeforeHash: hash("story before"), Locations: []LocationVote{{PlaceID: "paris", Name: "Paris", Lat: 48.8566, Lon: 2.3522, Precision: 2, Role: 0, Weight: 2.4}}, Primary: 0, PrimaryPlaceID: "paris"}},
		Articles: []ArticleLocationPatch{{ID: "article-1", StoryID: "story-1", DBBeforeHash: hash("article before"), Locations: []LocationVote{}, Unlocated: true, EventCandidate: EffectiveWireLocation{Place: "Paris", Lat: 48.8566, Lon: 2.3522, Located: true}, NeedsHotEvidence: true}},
	}
}

func signRepairBundle(t *testing.T, b *LocationRepairBundle) {
	t.Helper()
	id, err := LocationRepairBundleRunID(*b)
	if err != nil {
		t.Fatal(err)
	}
	b.RunID = id
}
