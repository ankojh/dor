package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/geo"
)

// prepareRepairDB checks the dedicated DOR_TEST_REPAIR_DSN database name before
// replacing its tables and applies the country migration to the scratch schema.
func TestF11MinorRetainedCountryRepair(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)
	cases := []struct {
		id, place, title, existing, want string
		explicit                         bool
	}{
		{"city", "paris", "Officials meet in Paris", "", "FR", false},
		{"existing", "paris", "Officials meet in Paris", "US", "US", false},
		{"keep", "paris", "Officials meet in Boston", "", "FR", true},
		{"region", "admin:US.MA", "Officials meet in Massachusetts", "", "US", true},
		{"country", "country:FR", "Officials meet in France", "", "FR", true},
		{"unknown", "photon:unknown", "Unlocated report", "", "", false},
	}
	allow := map[string]bool{}
	for _, tc := range cases {
		lat, lon, precision, name := 12.0, 13.0, int16(geo.CityResolution), "Unknown"
		if p, ok := g.ByID(tc.place); ok {
			lat, lon, precision, name = float64(float32(p.Lat)), float64(float32(p.Lon)), int16(geo.PrecisionOf(p)), p.Name
		}
		if _, err := conn.Exec(ctx, `INSERT INTO public.stories(id,title,primary_loc) VALUES ($1,$2,$3)`, "minor-"+tc.id, tc.title, tc.place); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO public.articles(id,story_id,title) VALUES ($1,$2,$3)`, "minor-a-"+tc.id, "minor-"+tc.id, tc.title); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO public.story_locations(story_id,place_id,name,lat,lon,precision,role,weight,country) VALUES ($1,$2,$3,$4,$5,$6,0,.625,$7)`, "minor-"+tc.id, tc.place, name, lat, lon, precision, tc.existing); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO public.article_locations(article_id,place_id,name,lat,lon,precision,role,conf,country) VALUES ($1,$2,$3,$4,$5,$6,0,.75,$7)`, "minor-a-"+tc.id, tc.place, name, lat, lon, precision, tc.existing); err != nil {
			t.Fatal(err)
		}
		if tc.explicit {
			for _, entry := range []string{"story_locations|minor-" + tc.id + "|" + tc.place, "article_locations|minor-a-" + tc.id + "|" + tc.place} {
				allow[entry] = true
				allow[entry+"|keep"] = true
			}
		}
	}
	before := make(map[string]geo.LocationVote)
	for _, tc := range cases {
		var v geo.LocationVote
		err := conn.QueryRow(ctx, `SELECT place_id,name,country,lat,lon,precision,role,weight FROM public.story_locations WHERE story_id=$1`, "minor-"+tc.id).Scan(&v.PlaceID, &v.Name, &v.Country, &v.Lat, &v.Lon, &v.Precision, &v.Role, &v.Weight)
		if err != nil {
			t.Fatal(err)
		}
		before[tc.id] = v
	}
	proposal, err := repair(ctx, conn, g, false, allow)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := repair(ctx, conn, g, true, allow)
	if err != nil {
		t.Fatal(err)
	}
	if !applied.Committed {
		t.Error("repair did not commit")
	}
	if !reflect.DeepEqual(proposal.Bundle, applied.Bundle) {
		t.Error("proposal and applied bundles differ")
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			for _, table := range []struct{ table, col, owner, weight string }{{"story_locations", "story_id", "minor-" + tc.id, "weight"}, {"article_locations", "article_id", "minor-a-" + tc.id, "conf"}} {
				var id, name, country string
				var lat, lon float64
				var precision, role int16
				var weight float32
				err := conn.QueryRow(ctx, `SELECT place_id,name,country,lat,lon,precision,role,`+table.weight+` FROM public.`+table.table+` WHERE `+table.col+`=$1`, table.owner).Scan(&id, &name, &country, &lat, &lon, &precision, &role, &weight)
				old := before[tc.id]
				expectedWeight := float32(.625)
				if table.table == "article_locations" {
					expectedWeight = .75
				}
				if err != nil || id != old.PlaceID || name != old.Name || country != tc.want || lat != old.Lat || lon != old.Lon || precision != old.Precision || role != old.Role || weight != expectedWeight {
					t.Errorf("%s: got %q %q %q (%v,%v) precision=%d role=%d weight=%v err=%v; before=%+v country want %q", table.table, id, name, country, lat, lon, precision, role, weight, err, old, tc.want)
				}
			}
			var primary string
			if err := conn.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id=$1`, "minor-"+tc.id).Scan(&primary); err != nil || primary != tc.place {
				t.Errorf("primary=%q err=%v", primary, err)
			}
			if tc.existing == "" && tc.want != "" {
				foundStory, foundArticle := false, false
				for _, p := range proposal.Bundle.Stories {
					if p.ID == "minor-"+tc.id {
						for _, v := range p.Locations {
							if v.PlaceID == tc.place && v.Country == tc.want {
								foundStory = true
							}
						}
					}
				}
				for _, p := range proposal.Bundle.Articles {
					if p.ID == "minor-a-"+tc.id {
						for _, v := range p.Locations {
							if v.PlaceID == tc.place && v.Country == tc.want {
								foundArticle = true
							}
						}
					}
				}
				if !foundStory || !foundArticle {
					t.Errorf("country missing from proposal: story=%v article=%v", foundStory, foundArticle)
				}
			}
		})
	}
	second, err := repair(ctx, conn, g, false, allow)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range second.Changes {
		for _, tc := range cases {
			if c.Owner == "minor-"+tc.id || c.Owner == "minor-a-"+tc.id {
				t.Errorf("second audit changed %s: %+v", tc.id, c)
			}
		}
	}
	if locationHash([]geo.LocationVote{{PlaceID: "paris", Country: "FR"}}, "paris") == locationHash([]geo.LocationVote{{PlaceID: "paris", Country: ""}}, "paris") {
		t.Error("country-only change must affect before hash")
	}
}

func TestF11MinorCollisionProposalPreservesCountryAndWeight(t *testing.T) {
	for _, table := range []string{"story_locations", "article_locations"} {
		t.Run(table, func(t *testing.T) {
			existing := location{Table: table, Owner: "owner", PlaceID: "paris", Name: "Paris", Lat: 48.8566, Lon: 2.3522, Precision: 2, Role: 0, Weight: .4}
			old := location{Table: table, Owner: "owner", PlaceID: "old", Name: "Unknown", Lat: 1, Lon: 2, Precision: 2, Role: 0, Weight: .9}
			replacement := geo.Place{ID: "paris", Name: "Paris", CountryCode: "FR", Lat: 48.8566, Lon: 2.3522}
			rows := plannedRows([]location{existing, old}, []change{{row: old, decision: geo.RepairDecision{Replacement: &replacement, Precision: geo.CityResolution}}})[ownerKey{table, "owner"}]
			if len(rows) != 1 {
				t.Fatalf("collision must yield one row: %+v", rows)
			}
			v := rows[0]
			weight := v.Weight
			if table == "article_locations" {
				weight = v.Confidence
			}
			if v.PlaceID != "paris" || v.Country != "FR" || v.Role != 0 || weight != .9 || v.Lat != existing.Lat || v.Lon != existing.Lon || v.Precision != existing.Precision {
				t.Errorf("collision must retain coordinates and stronger weight while filling country: %+v", v)
			}
		})
	}
}
