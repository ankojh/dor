package agents

import (
	"fmt"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

func testLocation(placeID, name string, lat, lon float32, role dorv1.Role, precision dorv1.Precision, weight float32) *dorv1.Location {
	return &dorv1.Location{
		PlaceId: placeID, Name: name, Lat: lat, Lon: lon,
		Role: role, Precision: precision, Weight: weight,
	}
}

func TestCapLocationsDedupesByPlaceAndMergesEvidence(t *testing.T) {
	in := []*dorv1.Location{
		testLocation("gaz-berlin", "Berlin, Berlin, Germany", 52.5201, 13.4041,
			dorv1.Role_ROLE_PARTY, dorv1.Precision_PRECISION_CITY, 1),
		testLocation("photon-berlin", "Berlin", 52.5189, 13.4049,
			dorv1.Role_ROLE_AFFECTED, dorv1.Precision_PRECISION_STREET, 2.5),
		testLocation("photon-berlin-2", "berlin, Germany", 52.52, 13.4045,
			dorv1.Role_ROLE_ORIGIN, dorv1.Precision_PRECISION_REGION, 0.5),
		testLocation("wikidata-berlin", "Berlin", 52.52, 13.405,
			dorv1.Role_ROLE_VENUE, dorv1.Precision_PRECISION_POINT, 0.25),
		testLocation("gaz-paris", "Paris, France", 48.8566, 2.3522,
			dorv1.Role_ROLE_PARTY, dorv1.Precision_PRECISION_CITY, 1),
	}

	out := capLocations(in)

	if len(out) != 2 {
		t.Fatalf("len(locations) = %d, want 2 (duplicates collapsed)", len(out))
	}
	berlin := out[0]
	if berlin.PlaceId != "wikidata-berlin" {
		t.Errorf("primary PlaceId = %q, want the point row %q", berlin.PlaceId, "wikidata-berlin")
	}
	if berlin.Name != "Berlin, Berlin, Germany" {
		t.Errorf("primary name = %q, want the first row's name", berlin.Name)
	}
	if berlin.Lat != 52.52 || berlin.Lon != 13.405 {
		t.Errorf("primary coords = (%v, %v), want the point row's coords", berlin.Lat, berlin.Lon)
	}
	if berlin.Role != dorv1.Role_ROLE_VENUE {
		t.Errorf("role = %v, want venue (the most decisive role)", berlin.Role)
	}
	if berlin.Precision != dorv1.Precision_PRECISION_POINT {
		t.Errorf("precision = %v, want point (the most precise)", berlin.Precision)
	}
	if want := float32(4.25); berlin.Weight != want {
		t.Errorf("weight = %v, want %v", berlin.Weight, want)
	}
	if out[1].PlaceId != "gaz-paris" {
		t.Errorf("second location = %q, want gaz-paris", out[1].PlaceId)
	}

	// The input rows are evidence owned by the caller; dedupe must clone the
	// surviving row instead of mutating the caller's proto.
	if in[0].Role != dorv1.Role_ROLE_PARTY || in[0].Weight != 1 {
		t.Errorf("input row mutated: role = %v weight = %v", in[0].Role, in[0].Weight)
	}
}

func TestCapLocationsRoleOrder(t *testing.T) {
	order := []dorv1.Role{
		dorv1.Role_ROLE_AFFECTED,
		dorv1.Role_ROLE_PARTY,
		dorv1.Role_ROLE_ORIGIN,
		dorv1.Role_ROLE_VENUE,
	}
	// Each pair of roles keeps the more decisive one, regardless of order.
	for i, better := range order {
		if i == 0 {
			continue // the weakest role, nothing beats it
		}
		weaker := order[i-1]
		in := []*dorv1.Location{
			testLocation("p1", "Place", 10, 20, weaker, dorv1.Precision_PRECISION_CITY, 1),
			testLocation("p2", "Place", 10, 20, better, dorv1.Precision_PRECISION_CITY, 1),
		}
		out := capLocations(in)
		if len(out) != 1 {
			t.Fatalf("%v vs %v: len = %d, want 1", weaker, better, len(out))
		}
		if out[0].Role != better {
			t.Errorf("%v vs %v: kept %v, want %v", weaker, better, out[0].Role, better)
		}
	}

	// Also test the reverse input order: the first row keeps its position but
	// adopts the better role from the later duplicate.
	in := []*dorv1.Location{
		testLocation("p1", "Place", 10, 20, dorv1.Role_ROLE_AFFECTED, dorv1.Precision_PRECISION_CITY, 1),
		testLocation("p2", "Place", 10, 20, dorv1.Role_ROLE_VENUE, dorv1.Precision_PRECISION_CITY, 1),
	}
	out := capLocations(in)
	if len(out) != 1 || out[0].PlaceId != "p1" || out[0].Role != dorv1.Role_ROLE_VENUE {
		t.Fatalf("reverse order: got %d rows, first = %+v", len(out), out[0])
	}
}

func TestCapLocationsKeepsFirstAndCapsAtTwelve(t *testing.T) {
	in := make([]*dorv1.Location, 0, 13)
	in = append(in, testLocation("primary", "Primary", 30, 40,
		dorv1.Role_ROLE_PARTY, dorv1.Precision_PRECISION_CITY, 1))
	for i := 1; i < 13; i++ {
		in = append(in, testLocation(
			fmt.Sprintf("p%d", i),
			fmt.Sprintf("Place %d", i),
			float32(i)+0.5,
			float32(i)+0.5,
			dorv1.Role_ROLE_PARTY,
			dorv1.Precision_PRECISION_CITY,
			1,
		))
	}

	out := capLocations(in)

	if len(out) != maxStoryLocations {
		t.Fatalf("len(locations) = %d, want %d", len(out), maxStoryLocations)
	}
	if out[0].PlaceId != "primary" {
		t.Errorf("first location = %q, want the primary", out[0].PlaceId)
	}
	if out[len(out)-1].PlaceId != "p11" {
		t.Errorf("last location = %q, want p11", out[len(out)-1].PlaceId)
	}

	if got := capLocations(nil); got != nil {
		t.Errorf("capLocations(nil) = %v, want nil", got)
	}
}

func TestBuildStoryRemapsPrimaryAfterDedupe(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	article := &dorv1.Article{Id: "a1", SourceId: "wire", Title: "Berlin story"}
	place := &dorv1.PlacedArticle{
		Primary: 1,
		Locations: []*dorv1.Location{
			testLocation("dup-a", "Berlin, Germany", 52.5201, 13.4041,
				dorv1.Role_ROLE_PARTY, dorv1.Precision_PRECISION_CITY, 1),
			testLocation("dup-b", "Berlin", 52.5189, 13.4049,
				dorv1.Role_ROLE_VENUE, dorv1.Precision_PRECISION_POINT, 2),
			testLocation("paris", "Paris, France", 48.8566, 2.3522,
				dorv1.Role_ROLE_PARTY, dorv1.Precision_PRECISION_CITY, 1),
		},
	}

	var c clusterer
	st := c.buildStory(article, place, dorv1.Category_CATEGORY_POLITICS, nil, nil, now)
	if st == nil || st.Story == nil {
		t.Fatal("buildStory returned no story")
	}
	locations := st.Story.GetLocations()
	if len(locations) != 2 {
		t.Fatalf("len(locations) = %d, want 2", len(locations))
	}
	if st.Story.GetPrimary() != 0 {
		t.Fatalf("primary = %d, want 0 (the surviving Berlin row)", st.Story.GetPrimary())
	}
	if locations[0].GetPlaceId() != "dup-b" {
		t.Errorf("locations[0] = %q, want dup-b (point row)", locations[0].GetPlaceId())
	}
	if locations[1].GetPlaceId() != "paris" {
		t.Errorf("locations[1] = %q, want paris", locations[1].GetPlaceId())
	}
	if locations[0].GetRole() != dorv1.Role_ROLE_VENUE {
		t.Errorf("surviving Berlin role = %v, want venue", locations[0].GetRole())
	}

	// A primary that is itself unique still moves to its deduped index.
	place.Primary = 2
	st = c.buildStory(article, place, dorv1.Category_CATEGORY_POLITICS, nil, nil, now)
	if st.Story.GetPrimary() != 1 {
		t.Errorf("primary = %d, want 1 (Paris after dedupe)", st.Story.GetPrimary())
	}
}
