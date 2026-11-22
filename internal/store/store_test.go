package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseEnv = "DOR_TEST_DATABASE_URL"

// testStore creates an isolated schema inside the scratch database named by
// DOR_TEST_DATABASE_URL. The test pool gets that schema first on search_path,
// so every unqualified migration and query stays private to the test. The
// schema is dropped with CASCADE when the test ends.
func testStore(t *testing.T) *Store {
	t.Helper()
	url := strings.TrimSpace(os.Getenv(testDatabaseEnv))
	if url == "" {
		t.Skipf("set %s to a scratch database to run the store integration tests", testDatabaseEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pc, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse %s: %v", testDatabaseEnv, err)
	}
	if pc.ConnConfig.Database == "dor" {
		t.Fatalf("refusing to run store tests against the live dor database (%s)", testDatabaseEnv)
	}

	control, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect control pool: %v", err)
	}

	schema := testSchemaName(t)
	quoted := quoteTestSchema(t, schema)
	if _, err := control.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = control.Close(ctx)
		t.Fatalf("create schema %s: %v", schema, err)
	}

	pc.ConnConfig.RuntimeParams["search_path"] = quoted + ",public"
	pc.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		_, _ = control.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		_ = control.Close(ctx)
		t.Fatalf("open test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		_, _ = control.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		_ = control.Close(ctx)
		t.Fatalf("ping test pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := control.Exec(dropCtx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop test schema %s: %v", schema, err)
		}
		if err := control.Close(dropCtx); err != nil {
			t.Errorf("close control connection: %v", err)
		}
	})
	return &Store{pool: pool, cfg: Config{URL: url}}
}

func testSchemaName(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("dor_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	if !strings.HasPrefix(name, "dor_test_") {
		t.Fatalf("refusing non-test schema name %q", name)
	}
	return name
}

// quoteTestSchema validates that the identifier is a schema this package owns
// before quoting it. Nothing user-provided reaches the DDL.
func quoteTestSchema(t *testing.T, schema string) string {
	t.Helper()
	if !validTestSchema(schema) {
		t.Fatalf("refusing to touch non-test schema %q", schema)
	}
	return pgx.Identifier{schema}.Sanitize()
}

func validTestSchema(schema string) bool {
	if !strings.HasPrefix(schema, "dor_test_") || len(schema) > 63 {
		return false
	}
	for _, r := range schema {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func scalarInt(t *testing.T, s *Store, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func TestOpenRejectsEmptyURL(t *testing.T) {
	if _, err := Open(context.Background(), Config{}); err == nil {
		t.Fatal("Open accepted an empty database URL")
	}
}

func TestMigrateUpAndDown(t *testing.T) {
	s := testStore(t)
	ctx := testContext(t)

	ups, _, err := loadMigrations()
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(ups) == 0 {
		t.Fatal("no embedded up migrations")
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	applied, err := s.Applied(ctx)
	if err != nil {
		t.Fatalf("Applied: %v", err)
	}
	if len(applied) != len(ups) {
		t.Fatalf("Applied has %d versions, want %d: %v", len(applied), len(ups), applied)
	}
	for i, m := range ups {
		if applied[i] != m.version {
			t.Errorf("applied[%d] = %q, want %q", i, applied[i], m.version)
		}
	}

	// Re-running the migration set is a no-op, not an error.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	again, err := s.Applied(ctx)
	if err != nil {
		t.Fatalf("Applied after second Migrate: %v", err)
	}
	if len(again) != len(applied) {
		t.Fatalf("second Migrate changed applied versions: %v -> %v", applied, again)
	}

	coreTables := []string{
		"alerts", "alert_hits", "article_locations", "articles", "links",
		"push_subscriptions", "sources", "stories", "story_locations",
	}
	tables := tableNames(t, s, ctx)
	for _, want := range coreTables {
		if !tables[want] {
			t.Errorf("table %s missing after Migrate", want)
		}
	}

	if err := s.MigrateDown(ctx, 0); err != nil {
		t.Fatalf("MigrateDown: %v", err)
	}
	applied, err = s.Applied(ctx)
	if err != nil {
		t.Fatalf("Applied after down: %v", err)
	}
	if len(applied) != 0 {
		t.Fatalf("Applied after down = %v, want empty", applied)
	}
	tables = tableNames(t, s, ctx)
	for _, gone := range coreTables {
		if tables[gone] {
			t.Errorf("table %s still exists after MigrateDown", gone)
		}
	}

	// The full up set is re-appliable after a rollback.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after down: %v", err)
	}
	applied, err = s.Applied(ctx)
	if err != nil {
		t.Fatalf("Applied after re-migrate: %v", err)
	}
	if len(applied) != len(ups) {
		t.Fatalf("Applied after re-migrate = %d versions, want %d", len(applied), len(ups))
	}
	tables = tableNames(t, s, ctx)
	for _, want := range coreTables {
		if !tables[want] {
			t.Errorf("table %s missing after re-migrate", want)
		}
	}
}

func tableNames(t *testing.T, s *Store, ctx context.Context) map[string]bool {
	t.Helper()
	rows, err := s.pool.Query(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	return out
}

func TestUpsertArticlesStoriesSourcesAreIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := testContext(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	article := ArticleRecord{
		ID:                 "article-1",
		SourceID:           "source-1",
		URL:                "https://example.test/article-1",
		Title:              "First title",
		Lede:               "A lede",
		PublishedAt:        created,
		CreatedAt:          created,
		Lang:               "en",
		Dateline:           "ATHENS",
		Category:           6,
		CategoryConfidence: 0.9,
		Locations: []LocationRecord{{
			PlaceID: "place-1", Name: "Athens", Lat: 37.98, Lon: 23.72,
			Precision: 2, Role: 1, Confidence: 0.8, Weight: 1,
		}},
	}
	if err := s.UpsertArticles(ctx, []ArticleRecord{article}); err != nil {
		t.Fatalf("first article upsert: %v", err)
	}
	if err := s.UpsertArticles(ctx, []ArticleRecord{article}); err != nil {
		t.Fatalf("second article upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM articles WHERE id = $1`, article.ID); got != 1 {
		t.Fatalf("article rows = %d, want 1", got)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM article_locations WHERE article_id = $1`, article.ID); got != 1 {
		t.Fatalf("article location rows = %d, want 1", got)
	}

	// A second upsert of the same key updates the row instead of duplicating.
	updated := article
	updated.Title = "Updated title"
	updated.SourceID = "source-2"
	updated.Locations = []LocationRecord{{
		PlaceID: "place-1", Name: "Attica", Lat: 38.0, Lon: 23.8,
		Precision: 1, Role: 1, Confidence: 0.95, Weight: 1,
	}}
	if err := s.UpsertArticles(ctx, []ArticleRecord{updated}); err != nil {
		t.Fatalf("article update upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM articles WHERE id = $1`, article.ID); got != 1 {
		t.Fatalf("article rows after update = %d, want 1", got)
	}
	var title, sourceID string
	if err := s.pool.QueryRow(ctx,
		`SELECT title, source_id FROM articles WHERE id = $1`, article.ID).Scan(&title, &sourceID); err != nil {
		t.Fatalf("read updated article: %v", err)
	}
	if title != "Updated title" || sourceID != "source-2" {
		t.Fatalf("article = title %q source %q, want the updated values", title, sourceID)
	}
	var locName string
	if err := s.pool.QueryRow(ctx,
		`SELECT name FROM article_locations WHERE article_id = $1 AND place_id = $2 AND role = $3`,
		article.ID, "place-1", 1).Scan(&locName); err != nil {
		t.Fatalf("read updated article location: %v", err)
	}
	if locName != "Attica" {
		t.Fatalf("article location name = %q, want Attica", locName)
	}

	centroid := make([]float32, 384)
	centroid[0], centroid[1], centroid[2] = 0.1, 0.2, 0.3
	story := StoryRecord{
		ID:          "story-1",
		Title:       "Story title",
		Brief:       "A brief",
		Category:    6,
		PrimaryLoc:  "place-1",
		SourceCount: 2,
		Heat:        0.5,
		Centroid:    centroid,
		Entities:    []string{"Athens"},
		FirstSeen:   created,
		UpdatedAt:   created.Add(time.Minute),
		ExpiresAt:   created.Add(48 * time.Hour),
		Locations: []LocationRecord{{
			PlaceID: "place-1", Name: "Athens", Lat: 37.98, Lon: 23.72,
			Precision: 2, Role: 1, Weight: 1,
		}},
		ArticleIDs: []string{article.ID},
	}
	if _, err := s.UpsertStories(ctx, []StoryRecord{story}); err != nil {
		t.Fatalf("first story upsert: %v", err)
	}
	if _, err := s.UpsertStories(ctx, []StoryRecord{story}); err != nil {
		t.Fatalf("second story upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM stories WHERE id = $1`, story.ID); got != 1 {
		t.Fatalf("story rows = %d, want 1", got)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM story_locations WHERE story_id = $1`, story.ID); got != 1 {
		t.Fatalf("story location rows = %d, want 1", got)
	}

	// The story upsert stamps its article and category.
	var storyID string
	var category int16
	if err := s.pool.QueryRow(ctx,
		`SELECT story_id, category FROM articles WHERE id = $1`, article.ID).Scan(&storyID, &category); err != nil {
		t.Fatalf("read stamped article: %v", err)
	}
	if storyID != story.ID {
		t.Fatalf("article story_id = %q, want %q", storyID, story.ID)
	}
	if category != 6 {
		t.Fatalf("article category = %d, want the story category 6", category)
	}

	storyUpdate := story
	storyUpdate.Title = "Story title updated"
	storyUpdate.SourceCount = 5
	if _, err := s.UpsertStories(ctx, []StoryRecord{storyUpdate}); err != nil {
		t.Fatalf("story update upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM stories WHERE id = $1`, story.ID); got != 1 {
		t.Fatalf("story rows after update = %d, want 1", got)
	}
	var storyTitle string
	var sourceCount int
	if err := s.pool.QueryRow(ctx,
		`SELECT title, source_count FROM stories WHERE id = $1`, story.ID).Scan(&storyTitle, &sourceCount); err != nil {
		t.Fatalf("read updated story: %v", err)
	}
	if storyTitle != "Story title updated" || sourceCount != 5 {
		t.Fatalf("story = %q/%d, want the updated values", storyTitle, sourceCount)
	}

	source := SourceRecord{
		ID:        "source-1",
		Name:      "Example Source",
		Kind:      "rss",
		URL:       "https://example.test/feed",
		Country:   "GR",
		Region:    "Attica",
		Lang:      "en",
		IntervalS: 300,
		Enabled:   true,
		TermsOK:   true,
		TermsNote: "reviewed",
		Hazard:    "wildfire",
		Notes:     "seeded",
		Health:    []byte(`{"ok":true}`),
	}
	if err := s.UpsertSources(ctx, []SourceRecord{source}); err != nil {
		t.Fatalf("first source upsert: %v", err)
	}
	if err := s.UpsertSources(ctx, []SourceRecord{source}); err != nil {
		t.Fatalf("second source upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM sources WHERE id = $1`, source.ID); got != 1 {
		t.Fatalf("source rows = %d, want 1", got)
	}

	sourceUpdate := source
	sourceUpdate.Name = "Renamed Source"
	sourceUpdate.Enabled = false
	sourceUpdate.Hazard = "flood"
	if err := s.UpsertSources(ctx, []SourceRecord{sourceUpdate}); err != nil {
		t.Fatalf("source update upsert: %v", err)
	}
	if got := scalarInt(t, s, ctx, `SELECT count(*) FROM sources WHERE id = $1`, source.ID); got != 1 {
		t.Fatalf("source rows after update = %d, want 1", got)
	}
	var name, hazard string
	var enabled bool
	if err := s.pool.QueryRow(ctx,
		`SELECT name, hazard, enabled FROM sources WHERE id = $1`, source.ID).Scan(&name, &hazard, &enabled); err != nil {
		t.Fatalf("read updated source: %v", err)
	}
	if name != "Renamed Source" || hazard != "flood" || enabled {
		t.Fatalf("source = %q/%q/%v, want the updated values", name, hazard, enabled)
	}
}

func TestFormatVector(t *testing.T) {
	tests := []struct {
		name string
		in   []float32
		want string
	}{
		{name: "empty", in: nil, want: ""},
		{name: "one", in: []float32{1}, want: "[1]"},
		{name: "two", in: []float32{0.5, -0.25}, want: "[0.5,-0.25]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatVector(tc.in); got != tc.want {
				t.Fatalf("formatVector(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestMigrationVersion(t *testing.T) {
	tests := map[string]string{
		"0001_init.up.sql":     "0001",
		"0002_alerts.down.sql": "0002",
		"nounderscore.sql":     "nounderscore.sql",
		"_leading.sql":         "_leading.sql",
	}
	for input, want := range tests {
		if got := migrationVersion(input); got != want {
			t.Errorf("migrationVersion(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestQuoteTestSchemaRejectsUnsafeNames(t *testing.T) {
	for _, unsafe := range []string{"public", "dor_test_x; DROP TABLE articles", "dor_test_x\"", "dor_test_Ü", ""} {
		if validTestSchema(unsafe) {
			t.Errorf("validTestSchema accepted unsafe name %q", unsafe)
		}
	}
	if !validTestSchema("dor_test_1_2") {
		t.Fatal("validTestSchema rejected a well-formed test schema")
	}
}
