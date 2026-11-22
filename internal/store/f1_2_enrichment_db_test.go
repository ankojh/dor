package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// These tests exercise the real-database paths introduced for F1·2. They share
// openTestStore and skip unless DOR_TEST_DATABASE_URL points at a scratch DB.

func TestF12ArticleEnrichmentPersists(t *testing.T) {
	st := openTestStore(t)
	prefix := fmt.Sprintf("f1-2-enrich-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	now := time.Now().UTC()
	articleID := prefix + "-a"
	if err := st.UpsertArticles(ctx, []ArticleRecord{{
		ID: articleID, SourceID: "wire", URL: "https://example.test/enrich",
		Title: "Quake", Lede: "A quake", PublishedAt: now, CreatedAt: now, Lang: "en",
	}}); err != nil {
		t.Fatalf("insert article: %v", err)
	}

	missing, err := st.UpsertArticleEnrichment(ctx, []ArticleEnrichmentRecord{{
		ID: articleID, Category: 6, CategoryConfidence: 0.6, Lang: "fr",
		Locations: []LocationRecord{{
			PlaceID: "place-f12", Name: "Paris", Lat: 48.85, Lon: 2.35,
			Precision: 2, Role: 1, Confidence: 0.8, Country: "FR",
		}},
	}})
	if err != nil {
		t.Fatalf("enrich article: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}

	var category int16
	var confidence float32
	var lang string
	if err := st.Pool().QueryRow(ctx,
		`SELECT category, cat_conf, lang FROM articles WHERE id=$1`, articleID).
		Scan(&category, &confidence, &lang); err != nil {
		t.Fatalf("read enriched article: %v", err)
	}
	if category != 6 || confidence <= 0 || lang != "fr" {
		t.Fatalf("article enrichment = (%d, %v, %q), want (6, >0, fr)", category, confidence, lang)
	}

	var country string
	var conf float32
	if err := st.Pool().QueryRow(ctx,
		`SELECT country, conf FROM article_locations WHERE article_id=$1 AND place_id='place-f12' AND role=1`,
		articleID).Scan(&country, &conf); err != nil {
		t.Fatalf("read article location: %v", err)
	}
	if country != "FR" || conf <= 0 {
		t.Fatalf("article location = (%q, %v), want (FR, >0)", country, conf)
	}

	// The enrichment never inserts an article row; a queued enrichment for an
	// article that has not landed is reported for retry.
	unknown := prefix + "-missing"
	missing, err = st.UpsertArticleEnrichment(ctx, []ArticleEnrichmentRecord{{ID: unknown, Category: 1, CategoryConfidence: 0.5}})
	if err != nil {
		t.Fatalf("enrich missing article: %v", err)
	}
	if len(missing) != 1 || missing[0] != unknown {
		t.Fatalf("missing = %v, want [%s]", missing, unknown)
	}
	if _, _, exists := articleStoryID(t, st, unknown); exists {
		t.Fatal("enrichment inserted an article row")
	}
}

func TestF12EditorTitleAndBriefProvenanceSurviveClusterUpsert(t *testing.T) {
	st := openTestStore(t)
	prefix := fmt.Sprintf("f1-2-title-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	storyID := prefix + "-s"
	articleID := prefix + "-a"
	now := time.Now().UTC()
	if err := st.UpsertArticles(ctx, []ArticleRecord{{
		ID: articleID, SourceID: "wire", URL: "https://example.test/title",
		Title: "Source headline", Lede: "First lede text", PublishedAt: now, CreatedAt: now, Lang: "en",
	}}); err != nil {
		t.Fatalf("insert article: %v", err)
	}
	base := StoryRecord{
		ID: storyID, Title: "Source headline", TitleSource: "source",
		Brief: "", BriefSource: "none", Category: 6, CategoryBackend: "rule",
		CategoryConfidence: 0.6, SourceCount: 1, Heat: 0.5, Degraded: true,
		PlaceBackend: "gazetteer", PlaceDegraded: true, PlaceFallback: true,
		FirstSeen: now, UpdatedAt: now, ExpiresAt: now.Add(48 * time.Hour),
		ArticleIDs: []string{articleID},
	}
	if _, err := st.UpsertStories(ctx, []StoryRecord{base}); err != nil {
		t.Fatalf("upsert story: %v", err)
	}

	if err := st.UpsertStoryTexts(ctx, []StoryTextRecord{{
		ID: storyID, Title: "Neutral editor title", TitleSource: "editor",
		Brief: "A model-written brief.", BriefSource: "generated",
	}}); err != nil {
		t.Fatalf("write editor text: %v", err)
	}

	// A later cluster update must not revert the editor title or the generated
	// brief, and must keep the new provenance columns.
	again := base
	again.Title = "Source headline again"
	if _, err := st.UpsertStories(ctx, []StoryRecord{again}); err != nil {
		t.Fatalf("re-upsert story: %v", err)
	}

	var title, titleSource, brief, briefSource, categoryBackend, placeBackend string
	var confidence float32
	var degraded, placeDegraded bool
	if err := st.Pool().QueryRow(ctx, `
SELECT title, title_source, brief, brief_source, category_backend, cat_conf,
       degraded, place_backend, place_degraded
FROM stories WHERE id=$1`, storyID).Scan(
		&title, &titleSource, &brief, &briefSource, &categoryBackend, &confidence,
		&degraded, &placeBackend, &placeDegraded,
	); err != nil {
		t.Fatalf("read story: %v", err)
	}
	if title != "Neutral editor title" || titleSource != "editor" {
		t.Fatalf("title = (%q, %q), want editor title preserved", title, titleSource)
	}
	if brief != "A model-written brief." || briefSource != "generated" {
		t.Fatalf("brief = (%q, %q), want generated brief preserved", brief, briefSource)
	}
	if categoryBackend != "rule" || confidence <= 0 || !degraded || placeBackend != "gazetteer" || !placeDegraded {
		t.Fatalf("provenance = (%q, %v, %v, %q, %v)", categoryBackend, confidence, degraded, placeBackend, placeDegraded)
	}
}
