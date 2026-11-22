package main

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/geo"
)

func f11StoryPatchByID(t *testing.T, stories []geo.StoryLocationPatch, id string) geo.StoryLocationPatch {
	t.Helper()
	for _, story := range stories {
		if story.ID == id {
			return story
		}
	}
	t.Fatalf("story patch %q not found in %+v", id, stories)
	return geo.StoryLocationPatch{}
}

// An allowlisted review row is kept, not deleted, but keeping it must not skip
// the precision/coordinate refresh that the same row needs.
func TestF11RepairAllowlistedReviewStillDowngradesPrecision(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)

	statements := []string{
		`INSERT INTO public.stories(id,title,primary_loc) VALUES ('story-review-keep','Hyderabad Announces a New Policy','hyderabad-pk')`,
		`INSERT INTO public.articles(id,story_id,title,lede,dateline) VALUES ('article-review-keep','story-review-keep','Hyderabad Announces a New Policy','','')`,
		// Stored as if an older precise geocoder had claimed street precision;
		// the coordinates are valid but not the canonical gazetteer point.
		`INSERT INTO public.story_locations VALUES ('story-review-keep','hyderabad-pk','Hyderabad',25.3964,68.3578,3,0,.6)`,
		`INSERT INTO public.article_locations VALUES ('article-review-keep','hyderabad-pk','Hyderabad',25.3964,68.3578,3,0,.6)`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed allowlisted review fixture: %v", err)
		}
	}
	allowKeep := map[string]bool{
		"story_locations|story-review-keep|hyderabad-pk":     true,
		"article_locations|article-review-keep|hyderabad-pk": true,
	}

	committed, err := repair(ctx, conn, g, true, allowKeep)
	if err != nil {
		t.Fatalf("apply allowlisted review repair: %v", err)
	}
	if !committed.Committed {
		t.Fatalf("allowlisted repair must commit: %+v", committed)
	}
	if len(committed.ReviewRemovals) != 0 || len(committed.Reviews) != 2 {
		t.Fatalf("allowlisted rows must be kept, not proposed for removal: reviews=%+v removals=%+v", committed.Reviews, committed.ReviewRemovals)
	}
	preciseChanges := 0
	for _, change := range committed.Changes {
		if change.Owner != "story-review-keep" && change.Owner != "article-review-keep" {
			continue
		}
		if change.Replacement == nil || change.Precision != geo.CityResolution {
			t.Fatalf("kept review row must still be downgraded to its supported precision: %+v", change)
		}
		preciseChanges++
	}
	if preciseChanges != 2 {
		t.Fatalf("both kept review rows need a precision/coordinate refresh, got %d changes in %+v", preciseChanges, committed.Changes)
	}

	for _, tc := range []struct{ table, ownerCol, owner string }{
		{"story_locations", "story_id", "story-review-keep"},
		{"article_locations", "article_id", "article-review-keep"},
	} {
		var count int
		var lat, lon float64
		var precision int16
		query := `SELECT count(*),max(lat),max(lon),max(precision) FROM public.` + tc.table +
			` WHERE ` + tc.ownerCol + `=$1 AND place_id='hyderabad-pk' AND role=0`
		if err := conn.QueryRow(ctx, query, tc.owner).Scan(&count, &lat, &lon, &precision); err != nil {
			t.Fatalf("read kept %s row: %v", tc.table, err)
		}
		if count != 1 || precision != int16(geo.CityResolution) ||
			math.Abs(lat-25.395999908447266) > 1e-5 || math.Abs(lon-68.35780334472656) > 1e-5 {
			t.Fatalf("%s allowlisted row must remain one canonical city point: count=%d lat=%v lon=%v precision=%d",
				tc.table, count, lat, lon, precision)
		}
	}
	var primary string
	if err := conn.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id='story-review-keep'`).Scan(&primary); err != nil || primary != "hyderabad-pk" {
		t.Fatalf("keeping the review row must preserve its primary: primary=%q err=%v", primary, err)
	}

	second, err := repair(ctx, conn, g, true, allowKeep)
	if err != nil {
		t.Fatalf("second allowlisted repair: %v", err)
	}
	for _, change := range second.Changes {
		if change.Owner == "story-review-keep" || change.Owner == "article-review-keep" {
			t.Fatalf("second allowlisted repair must be idempotent, got %+v", change)
		}
	}
}

// When a repaired row collides with an existing row for the same place and
// role, the stronger weight/confidence must survive in both the proposal and
// the committed SQL, not just in one of them.
func TestF11RepairSamePlaceCollisionKeepsLargerWeight(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)

	statements := []string{
		`INSERT INTO public.stories(id,title,primary_loc) VALUES ('story-weight','Officials meet in Paris','paris')`,
		`INSERT INTO public.articles(id,story_id,title,lede,dateline) VALUES ('article-weight','story-weight','Officials meet in Paris','','')`,
		`INSERT INTO public.story_locations VALUES ('story-weight','old','Unknown',0,0,2,0,.9)`,
		`INSERT INTO public.story_locations VALUES ('story-weight','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.5)`,
		`INSERT INTO public.article_locations VALUES ('article-weight','old','Unknown',0,0,2,0,.8)`,
		`INSERT INTO public.article_locations VALUES ('article-weight','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.4)`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed same-place collision fixture: %v", err)
		}
	}

	proposed, err := repair(ctx, conn, g, false, nil)
	if err != nil {
		t.Fatalf("dry-run same-place collision repair: %v", err)
	}
	if proposed.Committed {
		t.Fatalf("dry run must not claim commit: %+v", proposed)
	}
	proposedStory := f11StoryPatchByID(t, proposed.Bundle.Stories, "story-weight")
	proposedVote, ok := voteByID(proposedStory.Locations, "paris")
	if !ok || proposedVote.Weight != .9 || len(proposedStory.Locations) != 1 || proposedStory.PrimaryPlaceID != "paris" {
		t.Fatalf("proposal must keep the larger story weight .9 and one Paris row: %+v", proposedStory)
	}
	proposedArticle := articlePatchByID(t, proposed.Bundle.Articles, "article-weight")
	proposedArticleVote, ok := voteByID(proposedArticle.Locations, "paris")
	if !ok || proposedArticleVote.Confidence != .8 || len(proposedArticle.Locations) != 1 {
		t.Fatalf("proposal must keep the larger article confidence .8 and one Paris row: %+v", proposedArticle)
	}

	committed, err := repair(ctx, conn, g, true, nil)
	if err != nil {
		t.Fatalf("apply same-place collision repair: %v", err)
	}
	if !committed.Committed {
		t.Fatalf("same-place collision repair must commit: %+v", committed)
	}
	committedStory := f11StoryPatchByID(t, committed.Bundle.Stories, "story-weight")
	committedVote, ok := voteByID(committedStory.Locations, "paris")
	if !ok || committedVote.Weight != .9 {
		t.Fatalf("committed bundle must match the proposal weight .9: %+v", committedStory)
	}
	committedArticle := articlePatchByID(t, committed.Bundle.Articles, "article-weight")
	committedArticleVote, ok := voteByID(committedArticle.Locations, "paris")
	if !ok || committedArticleVote.Confidence != .8 {
		t.Fatalf("committed bundle must match the proposal confidence .8: %+v", committedArticle)
	}

	var count int
	var weight float32
	if err := conn.QueryRow(ctx, `SELECT count(*),max(weight) FROM public.story_locations WHERE story_id='story-weight' AND place_id='paris' AND role=0`).Scan(&count, &weight); err != nil || count != 1 || weight != .9 {
		t.Fatalf("applied story collision must keep one Paris row at weight .9: count=%d weight=%v err=%v", count, weight, err)
	}
	var conf float32
	if err := conn.QueryRow(ctx, `SELECT count(*),max(conf) FROM public.article_locations WHERE article_id='article-weight' AND place_id='paris' AND role=0`).Scan(&count, &conf); err != nil || count != 1 || conf != .8 {
		t.Fatalf("applied article collision must keep one Paris row at conf .8: count=%d conf=%v err=%v", count, conf, err)
	}
	var primary string
	if err := conn.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id='story-weight'`).Scan(&primary); err != nil || primary != "paris" {
		t.Fatalf("same-place collision must select Paris as primary: primary=%q err=%v", primary, err)
	}

	second, err := repair(ctx, conn, g, false, nil)
	if err != nil {
		t.Fatalf("second same-place collision dry run: %v", err)
	}
	for _, change := range second.Changes {
		if change.Owner == "story-weight" || change.Owner == "article-weight" {
			t.Fatalf("second same-place collision repair must be idempotent, got %+v", change)
		}
	}
}
