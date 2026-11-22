package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankojh/dor/migrations"
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

// C-2: migration 0007 must re-label legacy lede-derived briefs even after 0006
// already marked everything generated, and must leave model-backed briefs
// alone. The migration is applied by openTestStore, so this seeds fresh legacy
// rows and re-runs its SQL to exercise the repair statements.
func TestF12LegacyBriefMigration0007(t *testing.T) {
	st := openTestStore(t)
	prefix := fmt.Sprintf("f1-2-brief-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	now := time.Now().UTC()
	ledeA := "Israeli settlers attack Palestinian homes in Hebron, Ramallah Israeli settlers attacked homes before dawn while residents slept."
	ledeB := "On 11/21/2026 9:42:10 AM, an earthquake occurred in Indonesia potentially affecting 50 thousand people in the region."
	for i, lede := range []string{ledeA, ledeB} {
		id := fmt.Sprintf("%s-a%d", prefix, i+1)
		if err := st.UpsertArticles(ctx, []ArticleRecord{{
			ID: id, SourceID: "wire", URL: "https://example.test/" + id,
			Title: "Article " + id, Lede: lede, PublishedAt: now, CreatedAt: now, Lang: "en",
		}}); err != nil {
			t.Fatalf("insert article %d: %v", i, err)
		}
	}

	type briefCase struct {
		id, brief, source, model, want string
	}
	cases := []briefCase{
		{id: prefix + "-exact", brief: ledeB, source: "generated", model: "", want: "source_snippet"},
		{id: prefix + "-prefix", brief: ledeA[:60] + " and this tail differs", source: "generated", model: "", want: "source_snippet"},
		{id: prefix + "-unmatched", brief: "A feed summary that is not stored as an article lede anywhere.", source: "generated", model: "", want: "source_snippet"},
		{id: prefix + "-model", brief: "A genuinely model-written brief.", source: "generated", model: "claude-opus", want: "generated"},
		{id: prefix + "-empty", brief: "", source: "generated", model: "", want: "none"},
	}
	for _, tc := range cases {
		if _, err := st.Pool().Exec(ctx, `
INSERT INTO stories (id, title, brief, brief_source, brief_model, category, source_count, heat,
                     first_seen, updated_at, expires_at)
VALUES ($1,$2,$3,$4,$5,1,1,0.5,$6,$6,$7)`,
			tc.id, "Story "+tc.id, tc.brief, tc.source, tc.model, now, now.Add(48*time.Hour)); err != nil {
			t.Fatalf("insert story %s: %v", tc.id, err)
		}
	}

	sql, err := migrations.FS.ReadFile("0007_legacy_brief_provenance.up.sql")
	if err != nil {
		t.Fatalf("read migration 0007: %v", err)
	}
	if _, err := st.Pool().Exec(ctx, string(sql)); err != nil {
		t.Fatalf("apply migration 0007 repair: %v", err)
	}
	for _, tc := range cases {
		var got string
		if err := st.Pool().QueryRow(ctx, `SELECT brief_source FROM stories WHERE id=$1`, tc.id).Scan(&got); err != nil {
			t.Fatalf("read brief_source %s: %v", tc.id, err)
		}
		if got != tc.want {
			t.Errorf("%s brief_source = %q, want %q", tc.id, got, tc.want)
		}
	}
	// Idempotent on a second run.
	if _, err := st.Pool().Exec(ctx, string(sql)); err != nil {
		t.Fatalf("re-apply migration 0007: %v", err)
	}
	var got string
	if err := st.Pool().QueryRow(ctx, `SELECT brief_source FROM stories WHERE id=$1`, prefix+"-model").Scan(&got); err != nil || got != "generated" {
		t.Fatalf("model brief after second run = %q, %v; want generated", got, err)
	}
}

// C-4: a missing article row must stop retrying after the bound.
func TestF12EnrichmentRetryBound(t *testing.T) {
	st := openTestStore(t)
	opts := DefaultWriterOptions()
	opts.FlushInterval = 20 * time.Millisecond
	w, err := NewWriter(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	}()
	if !w.EnqueueArticleEnrichment(ArticleEnrichmentRecord{
		ID: "f1-2-never-exists", Category: 6, CategoryConfidence: 0.6, Lang: "en",
	}) {
		t.Fatal("enqueue refused")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().DroppedBatch > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("enrichment for missing article was not dropped after the bound: %+v", w.Stats())
}

// C-11: story assignment may fill a zero article category, but must not
// overwrite an enrichment category/cat_conf.
func TestF12StoryAssignmentPreservesArticleCategory(t *testing.T) {
	st := openTestStore(t)
	prefix := fmt.Sprintf("f1-2-assign-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	now := time.Now().UTC()

	richID, zeroID := prefix+"-rich", prefix+"-zero"
	if err := st.UpsertArticles(ctx, []ArticleRecord{
		{ID: richID, SourceID: "wire", URL: "https://example.test/rich", Title: "Rich", Lede: "Rich", PublishedAt: now, CreatedAt: now, Lang: "en"},
		{ID: zeroID, SourceID: "wire", URL: "https://example.test/zero", Title: "Zero", Lede: "Zero", PublishedAt: now, CreatedAt: now, Lang: "en"},
	}); err != nil {
		t.Fatalf("insert articles: %v", err)
	}
	if _, err := st.UpsertArticleEnrichment(ctx, []ArticleEnrichmentRecord{{
		ID: richID, Category: 6, CategoryConfidence: 0.6, Lang: "fr",
	}}); err != nil {
		t.Fatalf("enrich rich article: %v", err)
	}
	for _, articleID := range []string{richID, zeroID} {
		if _, err := st.UpsertStories(ctx, []StoryRecord{{
			ID: prefix + "-story-" + articleID, Title: "Story", Category: 2, SourceCount: 1,
			Heat: 0.5, FirstSeen: now, UpdatedAt: now, ExpiresAt: now.Add(48 * time.Hour),
			ArticleIDs: []string{articleID},
		}}); err != nil {
			t.Fatalf("upsert story for %s: %v", articleID, err)
		}
	}
	var richCat int16
	var richConf float32
	if err := st.Pool().QueryRow(ctx, `SELECT category, cat_conf FROM articles WHERE id=$1`, richID).Scan(&richCat, &richConf); err != nil {
		t.Fatalf("read rich article: %v", err)
	}
	if richCat != 6 || richConf <= 0 {
		t.Fatalf("enriched article = (%d, %v), want (6, >0) after story assignment", richCat, richConf)
	}
	var zeroCat int16
	if err := st.Pool().QueryRow(ctx, `SELECT category FROM articles WHERE id=$1`, zeroID).Scan(&zeroCat); err != nil {
		t.Fatalf("read zero article: %v", err)
	}
	if zeroCat != 2 {
		t.Fatalf("unclassified article category = %d, want the story category 2", zeroCat)
	}
}
