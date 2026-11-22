package main

import (
	"context"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/geo"
)

func TestFinalReviewCountryInMigratedRepair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, repairTestDSN(t))
	defer conn.Close(ctx)
	statements := []string{
		`INSERT INTO public.stories(id,title,primary_loc) VALUES ('country-story','Officials meet in Paris','old')`,
		`INSERT INTO public.articles(id,story_id,title) VALUES ('country-article','country-story','Officials meet in Paris')`,
		`INSERT INTO public.story_locations(story_id,place_id,name,lat,lon,precision,role,weight,country) VALUES ('country-story','old','Unknown',0,0,2,0,.7,'US')`,
		`INSERT INTO public.article_locations(article_id,place_id,name,lat,lon,precision,role,conf,country) VALUES ('country-article','old','Unknown',0,0,2,0,.8,'US')`,
	}
	for _, s := range statements {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	report, err := repair(ctx, conn, g, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct{ table, column, owner string }{{"story_locations", "story_id", "country-story"}, {"article_locations", "article_id", "country-article"}} {
		var country, id string
		err := conn.QueryRow(ctx, `SELECT country,place_id FROM public.`+v.table+` WHERE `+v.column+`=$1`, v.owner).Scan(&country, &id)
		if err != nil || id != "paris" || country != "FR" {
			t.Fatalf("%s: id=%s country=%s err=%v", v.table, id, country, err)
		}
	}
	story := f11StoryPatchByID(t, report.Bundle.Stories, "country-story")
	if len(story.Locations) != 1 || story.Locations[0].Country != "FR" {
		t.Fatalf("bundle country: %+v", story.Locations)
	}
	found := false
	for _, a := range report.Bundle.Articles {
		if a.ID == "country-article" {
			found = true
			if len(a.Locations) != 1 || a.Locations[0].Country != "FR" {
				t.Fatalf("article bundle country: %+v", a.Locations)
			}
		}
	}
	if !found {
		t.Fatal("article patch missing")
	}
	if locationHash([]geo.LocationVote{{PlaceID: "x", Country: "FR"}}, "") == locationHash([]geo.LocationVote{{PlaceID: "x", Country: "US"}}, "") {
		t.Fatal("before hash ignores country")
	}
}

func TestFinalReviewExplicitRegionCapKeepsCountry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, repairTestDSN(t))
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `INSERT INTO public.stories(id,title,primary_loc) VALUES ('region-story','Officials meet in Paris','paris')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO public.story_locations(story_id,place_id,name,lat,lon,precision,role,weight,country) VALUES ('region-story','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.7,'FR')`); err != nil {
		t.Fatal(err)
	}
	allow := map[string]bool{"story_locations|region-story|paris": true, "story_locations|region-story|paris|region": true}
	r, err := repair(ctx, conn, g, true, allow)
	if err != nil {
		t.Fatal(err)
	}
	var precision int16
	var country string
	if err := conn.QueryRow(ctx, `SELECT precision,country FROM public.story_locations WHERE story_id='region-story'`).Scan(&precision, &country); err != nil || precision != int16(geo.RegionResolution) || country != "FR" {
		t.Fatalf("region cap must preserve existing country: precision=%d country=%q err=%v", precision, country, err)
	}
	v := f11StoryPatchByID(t, r.Bundle.Stories, "region-story").Locations[0]
	if v.Precision != precision || v.Country != country {
		t.Fatalf("bundle differs from SQL: %+v", v)
	}
}

func TestFinalReviewExplicitKeepRejectsUnrelatedReplot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, repairTestDSN(t))
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `INSERT INTO public.stories(id,title,primary_loc) VALUES ('keep-story','Officials meet in Boston','paris')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO public.story_locations(story_id,place_id,name,lat,lon,precision,role,weight,country) VALUES ('keep-story','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.7,'FR')`); err != nil {
		t.Fatal(err)
	}
	allow := map[string]bool{"story_locations|keep-story|paris": true, "story_locations|keep-story|paris|keep": true}
	if _, err := repair(ctx, conn, g, true, allow); err != nil {
		t.Fatal(err)
	}
	var id, country string
	if err := conn.QueryRow(ctx, `SELECT place_id,country FROM public.story_locations WHERE story_id='keep-story'`).Scan(&id, &country); err != nil || id != "paris" || country != "FR" {
		t.Fatalf("reviewed original not retained: %q %q %v", id, country, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE public.story_locations SET lat=0,lon=0 WHERE story_id='keep-story'`); err != nil {
		t.Fatal(err)
	}
	if _, err := repair(ctx, conn, g, false, allow); err == nil {
		t.Fatal("invalid coordinate must refuse explicit keep")
	}
}

func TestFinalReviewContainerPrecision(t *testing.T) {
	for _, tc := range []struct {
		name, title string
		want        bool
	}{{"Kyiv", "Attack damages Kyiv region apartments", true}, {"Oyo", "Oyo State announces plan", true}, {"Mastung", "Mastung district clinic opens", true}, {"Kyiv", "Kyiv city opens clinic", false}} {
		r := location{Name: tc.name, Evidence: []geo.TextEvidence{{Title: tc.title}}}
		p, ok := titleContainerPrecision(r)
		if ok != tc.want || ok && p != geo.RegionResolution {
			t.Errorf("%s => %v,%v", tc.title, p, ok)
		}
	}
}
