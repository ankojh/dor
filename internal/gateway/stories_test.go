package gateway

import (
	"math"
	"testing"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestDedupeLocationJSONCollapsesBerlinRows(t *testing.T) {
	in := []locationJSON{
		{PlaceID: "gaz-berlin", Name: "Berlin, Berlin, Germany", Lat: 52.5201, Lon: 13.4041,
			Precision: "city", Role: "party", Weight: 1},
		{PlaceID: "photon-berlin", Name: "Berlin", Lat: 52.5189, Lon: 13.4049,
			Precision: "street", Role: "affected", Weight: 2.5},
		{PlaceID: "wikidata-berlin", Name: "berlin", Lat: 52.52, Lon: 13.405,
			Precision: "point", Role: "venue", Weight: 0.25},
		{PlaceID: "gaz-paris", Name: "Paris, France", Lat: 48.8566, Lon: 2.3522,
			Precision: "city", Role: "party", Weight: 1},
	}

	out := dedupeLocationJSON(in)

	if len(out) != 2 {
		t.Fatalf("len(locations) = %d, want 2 (three Berlin rows collapse)", len(out))
	}
	berlin := out[0]
	if berlin.PlaceID != "gaz-berlin" {
		t.Errorf("primary PlaceID = %q, want the first row %q", berlin.PlaceID, "gaz-berlin")
	}
	if berlin.Name != "Berlin, Berlin, Germany" {
		t.Errorf("primary name = %q, want the first row's name", berlin.Name)
	}
	if !near(berlin.Lat, 52.5201) || !near(berlin.Lon, 13.4041) {
		t.Errorf("primary coords = (%v, %v), want the first row's coords", berlin.Lat, berlin.Lon)
	}
	if berlin.Role != "venue" {
		t.Errorf("role = %q, want venue (the most decisive)", berlin.Role)
	}
	if berlin.Precision != "point" {
		t.Errorf("precision = %q, want point (the most precise)", berlin.Precision)
	}
	if want := float32(3.75); berlin.Weight != want {
		t.Errorf("weight = %v, want %v", berlin.Weight, want)
	}
	if out[1].PlaceID != "gaz-paris" {
		t.Errorf("second location = %q, want gaz-paris", out[1].PlaceID)
	}

	// The caller's rows are not mutated.
	if in[0].Role != "party" || in[0].Weight != 1 {
		t.Errorf("input row mutated: role = %q weight = %v", in[0].Role, in[0].Weight)
	}
}

func TestDedupeLocationJSONCapsAtStoryLimit(t *testing.T) {
	in := make([]locationJSON, 0, 13)
	for i := 0; i < 13; i++ {
		in = append(in, locationJSON{
			PlaceID: "p" + string(rune('a'+i)),
			Name:    "Place " + string(rune('A'+i)),
			Lat:     float64(i), Lon: float64(i),
			Precision: "city", Role: "party", Weight: 1,
		})
	}

	out := dedupeLocationJSON(in)

	if len(out) != storyLocationLimit {
		t.Fatalf("len(locations) = %d, want %d", len(out), storyLocationLimit)
	}
	if out[0].PlaceID != "pa" {
		t.Errorf("first location = %q, want pa", out[0].PlaceID)
	}
}

func protoLocation(placeID, name string, lat, lon float32, precision dorv1.Precision, role dorv1.Role, weight float32) *dorv1.Location {
	return &dorv1.Location{
		PlaceId: placeID, Name: name, Lat: lat, Lon: lon,
		Precision: precision, Role: role, Weight: weight,
	}
}

func TestStoryFromProtoRemapsPrimaryAfterDedupe(t *testing.T) {
	story := &dorv1.Story{
		Id:       "s1",
		Title:    "Berlin",
		Category: dorv1.Category_CATEGORY_POLITICS,
		Primary:  1,
		Locations: []*dorv1.Location{
			protoLocation("dup-a", "Berlin, Germany", 52.5201, 13.4041,
				dorv1.Precision_PRECISION_CITY, dorv1.Role_ROLE_PARTY, 1),
			protoLocation("dup-b", "Berlin", 52.5189, 13.4049,
				dorv1.Precision_PRECISION_POINT, dorv1.Role_ROLE_VENUE, 2),
			protoLocation("paris", "Paris, France", 48.8566, 2.3522,
				dorv1.Precision_PRECISION_CITY, dorv1.Role_ROLE_PARTY, 1),
		},
	}

	out := storyFromProto(story, "", false, "none")

	if len(out.Locations) != 2 {
		t.Fatalf("len(locations) = %d, want 2", len(out.Locations))
	}
	if out.Primary != 0 {
		t.Fatalf("primary = %d, want 0 (the surviving Berlin row)", out.Primary)
	}
	if out.Locations[0].PlaceID != "dup-a" {
		t.Errorf("locations[0] = %q, want dup-a", out.Locations[0].PlaceID)
	}
	if out.Locations[0].Role != "venue" {
		t.Errorf("surviving Berlin role = %q, want venue", out.Locations[0].Role)
	}
	if out.Locations[1].PlaceID != "paris" {
		t.Errorf("locations[1] = %q, want paris", out.Locations[1].PlaceID)
	}

	// A unique later primary still moves to its deduped index.
	story.Primary = 2
	out = storyFromProto(story, "", false, "none")
	if out.Primary != 1 {
		t.Errorf("primary = %d, want 1 (Paris after dedupe)", out.Primary)
	}
}
