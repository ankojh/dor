// Package store owns the durable Postgres store: a pgx pool, migrations and
// the asynchronous batch writer used by the pipeline agents.
//
// The hot path never waits on the database. Agents enqueue records into Writer,
// which batches and upserts them off the message handler. The store is
// optional: cmd/dor starts without it and logs the degradation.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// liveDatabase is the developer's real local Dor database. Test binaries
	// must never connect to or migrate it by inheriting a default DSN.
	liveDatabase = "dor"
	// AllowTestLiveDBEnv is the explicit opt-in for a test that genuinely needs
	// the live database. Normal tests must use a scratch database instead.
	AllowTestLiveDBEnv = "DOR_ALLOW_TEST_LIVE_DB"
)

// isTestBinary reports whether this process was started by `go test`, including
// the re-exec form where package main calls its own main() from a *.test binary.
func isTestBinary() bool {
	base := filepath.Base(os.Args[0])
	return strings.HasSuffix(base, ".test") || strings.Contains(base, ".test.") ||
		os.Getenv("DOR_TEST_AS_MAIN") == "1"
}

// Config is the Postgres connection and pool configuration.
type Config struct {
	URL            string
	MaxConns       int32
	ConnectTimeout time.Duration
}

// Store is the pgx pool plus the migration and upsert helpers.
type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

// Open parses the URL, opens a pool and verifies it with a ping.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.URL == "" {
		return nil, errors.New("store: database URL is empty")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("store: parse database URL: %w", err)
	}
	// An omitted database path is resolved by Postgres to the user name. Treat
	// that as the same live database instead of waiting for the server default.
	dbName := pc.ConnConfig.Database
	if dbName == "" {
		dbName = pc.ConnConfig.User
	}
	if isTestBinary() && dbName == liveDatabase && os.Getenv(AllowTestLiveDBEnv) != "1" {
		return nil, fmt.Errorf(
			"store: refusing live database %q from a test binary; use a scratch database or set %s=1",
			liveDatabase, AllowTestLiveDBEnv,
		)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool, cfg: cfg}, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for agents that need a custom query. Keep
// this for the gateway and later story/alert agents.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// EnsurePartition makes sure the articles partition for day exists. day is
// interpreted in UTC by the SQL function.
func (s *Store) EnsurePartition(ctx context.Context, day time.Time) error {
	_, err := s.pool.Exec(ctx, `SELECT ensure_article_partition($1::date)`, day.UTC().Format("2006-01-02"))
	if err != nil {
		return fmt.Errorf("store: ensure article partition %s: %w", day.UTC().Format("2006-01-02"), err)
	}
	return nil
}

// UpsertArticles writes a batch in one transaction. It is idempotent by the
// (id, created_at) primary key and article locations by (article_id, place_id,
// role).
func (s *Store) UpsertArticles(ctx context.Context, records []ArticleRecord) error {
	if len(records) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for i := range records {
		if records[i].PublishedAt.IsZero() {
			if !records[i].CreatedAt.IsZero() {
				records[i].PublishedAt = records[i].CreatedAt
			} else {
				records[i].PublishedAt = now
			}
		}
		if records[i].CreatedAt.IsZero() {
			records[i].CreatedAt = records[i].PublishedAt
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin articles batch: %w", err)
	}
	defer tx.Rollback(ctx)

	days := make(map[string]bool, len(records))
	for _, r := range records {
		days[r.CreatedAt.UTC().Format("2006-01-02")] = true
	}
	for day := range days {
		if _, err := tx.Exec(ctx, `SELECT ensure_article_partition($1::date)`, day); err != nil {
			return fmt.Errorf("store: ensure partition %s: %w", day, err)
		}
	}

	batch := &pgx.Batch{}
	const articleSQL = `
INSERT INTO articles
    (id, source_id, url, title, lede, published_at, lang, dateline, category, cat_conf, story_id, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12)
ON CONFLICT (id, created_at) DO UPDATE SET
    source_id = EXCLUDED.source_id,
    url = EXCLUDED.url,
    title = EXCLUDED.title,
    lede = EXCLUDED.lede,
    published_at = EXCLUDED.published_at,
    lang = CASE WHEN EXCLUDED.lang <> '' THEN EXCLUDED.lang ELSE articles.lang END,
    dateline = EXCLUDED.dateline,
    -- A later assignment-only upsert (or a re-delivered clean article) must
    -- not reset enrichment that another stage already wrote.
    category = CASE WHEN EXCLUDED.category <> 0 THEN EXCLUDED.category ELSE articles.category END,
    cat_conf = CASE WHEN EXCLUDED.category <> 0 THEN EXCLUDED.cat_conf ELSE articles.cat_conf END,
    story_id = COALESCE(EXCLUDED.story_id, articles.story_id)`
	const locationSQL = `
INSERT INTO article_locations (article_id, place_id, name, lat, lon, precision, role, conf, country)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (article_id, place_id, role) DO UPDATE SET
    name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    precision = EXCLUDED.precision, conf = EXCLUDED.conf,
    country = EXCLUDED.country`
	for _, r := range records {
		batch.Queue(articleSQL, r.ID, r.SourceID, r.URL, r.Title, r.Lede, r.PublishedAt,
			r.Lang, r.Dateline, r.Category, r.CategoryConfidence, r.StoryID, r.CreatedAt)
		for _, loc := range r.Locations {
			batch.Queue(locationSQL, r.ID, loc.PlaceID, loc.Name, loc.Lat, loc.Lon,
				loc.Precision, loc.Role, loc.Confidence, loc.Country)
		}
	}
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return fmt.Errorf("store: articles batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit articles batch: %w", err)
	}
	return nil
}

// UpsertSources seeds or updates the reviewed source list.
func (s *Store) UpsertSources(ctx context.Context, records []SourceRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin sources batch: %w", err)
	}
	defer tx.Rollback(ctx)
	batch := &pgx.Batch{}
	const sql = `
INSERT INTO sources (id, name, kind, url, country, region, lang, interval_s, enabled, terms_ok, terms_note, hazard, notes, health_json)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,COALESCE($14,'{}'::jsonb))
ON CONFLICT (id) DO UPDATE SET
    name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE sources.name END,
    kind = EXCLUDED.kind, url = EXCLUDED.url, country = EXCLUDED.country,
    region = EXCLUDED.region, lang = EXCLUDED.lang, interval_s = EXCLUDED.interval_s,
    enabled = EXCLUDED.enabled, terms_ok = EXCLUDED.terms_ok,
    terms_note = EXCLUDED.terms_note, hazard = EXCLUDED.hazard,
    notes = EXCLUDED.notes, updated_at = now()`
	for _, r := range records {
		var health any
		if len(r.Health) > 0 {
			health = string(r.Health)
		}
		batch.Queue(sql, r.ID, r.Name, r.Kind, r.URL, r.Country, r.Region, r.Lang, r.IntervalS,
			r.Enabled, r.TermsOK, r.TermsNote, r.Hazard, r.Notes, health)
	}
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return fmt.Errorf("store: sources batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit sources: %w", err)
	}
	return nil
}

// UpsertStories writes story metadata and replaces its location votes in one
// transaction. It also stamps story_id on every article in ArticleIDs.
//
// The article rows and the story may be flushed in either order: a story that
// arrives before its article rows returns article id → story id for those
// missing rows, so the caller can retry the assignment after the article
// insert. The story row and its locations are committed either way; a missing
// assignment is not an error.
func (s *Store) UpsertStories(ctx context.Context, records []StoryRecord) (map[string]string, error) {
	if len(records) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin stories batch: %w", err)
	}
	defer tx.Rollback(ctx)
	missing := make(map[string]string)
	const storySQL = `
INSERT INTO stories (id, title, title_source, brief, brief_source, category,
                     category_backend, cat_conf, primary_loc, source_count, heat,
                     degraded, fallback, place_backend, place_degraded, place_fallback,
                     centroid, entities, first_seen, updated_at, expires_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
        NULLIF($17,'')::vector,$18,$19,$20,$21)
ON CONFLICT (id) DO UPDATE SET
    title = CASE
        WHEN stories.title_source = 'editor' AND EXCLUDED.title_source <> 'editor'
            THEN stories.title
        ELSE EXCLUDED.title END,
    title_source = CASE
        WHEN stories.title_source = 'editor' AND EXCLUDED.title_source <> 'editor'
            THEN stories.title_source
        ELSE EXCLUDED.title_source END,
    brief = CASE
        WHEN stories.brief_source = 'generated' AND stories.brief <> ''
            THEN stories.brief
        ELSE EXCLUDED.brief END,
    brief_source = CASE
        WHEN stories.brief_source = 'generated' AND stories.brief <> ''
            THEN stories.brief_source
        ELSE EXCLUDED.brief_source END,
    category = EXCLUDED.category,
    category_backend = EXCLUDED.category_backend,
    cat_conf = EXCLUDED.cat_conf,
    primary_loc = EXCLUDED.primary_loc,
    source_count = EXCLUDED.source_count,
    heat = EXCLUDED.heat,
    degraded = EXCLUDED.degraded,
    fallback = EXCLUDED.fallback,
    place_backend = EXCLUDED.place_backend,
    place_degraded = EXCLUDED.place_degraded,
    place_fallback = EXCLUDED.place_fallback,
    centroid = COALESCE(EXCLUDED.centroid, stories.centroid),
    entities = EXCLUDED.entities,
    first_seen = LEAST(stories.first_seen, EXCLUDED.first_seen),
    updated_at = EXCLUDED.updated_at,
    expires_at = EXCLUDED.expires_at`
	for _, r := range records {
		if r.Title == "" {
			r.Title = "Untitled"
		}
		if r.TitleSource == "" {
			r.TitleSource = "source"
		}
		if r.BriefSource == "" {
			r.BriefSource = "none"
		}
		if r.SourceCount <= 0 {
			r.SourceCount = 1
		}
		if r.FirstSeen.IsZero() {
			r.FirstSeen = now
		}
		if r.UpdatedAt.IsZero() {
			r.UpdatedAt = now
		}
		if r.ExpiresAt.IsZero() {
			r.ExpiresAt = r.UpdatedAt.Add(48 * time.Hour)
		}
		if r.Heat < 0 {
			r.Heat = 0
		}
		if r.Heat > 1 {
			r.Heat = 1
		}
		entities := r.Entities
		if entities == nil {
			entities = []string{}
		}
		if _, err := tx.Exec(ctx, storySQL,
			r.ID, r.Title, r.TitleSource, r.Brief, r.BriefSource, r.Category,
			r.CategoryBackend, r.CategoryConfidence, r.PrimaryLoc, r.SourceCount, r.Heat,
			r.Degraded, r.Fallback, r.PlaceBackend, r.PlaceDegraded, r.PlaceFallback,
			formatVector(r.Centroid), entities, r.FirstSeen, r.UpdatedAt, r.ExpiresAt,
		); err != nil {
			return nil, fmt.Errorf("store: upsert story %s: %w", r.ID, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM story_locations WHERE story_id = $1`, r.ID); err != nil {
			return nil, fmt.Errorf("store: clear story locations %s: %w", r.ID, err)
		}
		for _, loc := range r.Locations {
			if _, err := tx.Exec(ctx, `
INSERT INTO story_locations (story_id, place_id, name, lat, lon, precision, role, weight, country)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
				r.ID, loc.PlaceID, loc.Name, loc.Lat, loc.Lon, loc.Precision, loc.Role, loc.Weight, loc.Country,
			); err != nil {
				return nil, fmt.Errorf("store: story location %s/%s: %w", r.ID, loc.PlaceID, err)
			}
		}
		if len(r.ArticleIDs) > 0 {
			assigned := make(map[string]bool, len(r.ArticleIDs))
			rows, err := tx.Query(ctx, `
UPDATE articles SET story_id = $2,
    category = CASE WHEN $3::smallint <> 0 THEN $3::smallint ELSE category END
WHERE id = ANY($1)
RETURNING id`, r.ArticleIDs, r.ID, r.Category)
			if err != nil {
				return nil, fmt.Errorf("store: assign story %s: %w", r.ID, err)
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return nil, fmt.Errorf("store: assign story %s scan: %w", r.ID, err)
				}
				assigned[id] = true
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: assign story %s: %w", r.ID, err)
			}
			rows.Close()
			for _, id := range r.ArticleIDs {
				if !assigned[id] {
					missing[id] = r.ID
				}
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit stories batch: %w", err)
	}
	return missing, nil
}

// AssignStory retries a story assignment that raced ahead of the article row.
// It is idempotent and returns whether an article row existed.
func (s *Store) AssignStory(ctx context.Context, articleID, storyID string, category int16) (bool, error) {
	if articleID == "" || storyID == "" {
		return false, nil
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE articles SET story_id = $2,
    category = CASE WHEN $3::smallint <> 0 THEN $3::smallint ELSE category END
WHERE id = $1`, articleID, storyID, category)
	if err != nil {
		return false, fmt.Errorf("store: assign article %s: %w", articleID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// UpsertArticleEnrichment applies the classification, confidence, detected
// language and place votes a joined article contributed. It never changes
// story_id and never inserts an article row: assignment belongs to the story
// record. IDs whose article row has not been flushed yet are returned as
// missing so the writer can retry without blocking other records.
func (s *Store) UpsertArticleEnrichment(ctx context.Context, records []ArticleEnrichmentRecord) ([]string, error) {
	if len(records) == 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin enrichment batch: %w", err)
	}
	defer tx.Rollback(ctx)
	const articleSQL = `
UPDATE articles
SET category = CASE WHEN $2::smallint <> 0 OR $3::real > 0 THEN $2::smallint ELSE category END,
    cat_conf = CASE WHEN $2::smallint <> 0 OR $3::real > 0 THEN $3::real ELSE cat_conf END,
    lang = CASE WHEN $4 <> '' THEN $4 ELSE lang END
WHERE id = $1`
	const locationSQL = `
INSERT INTO article_locations (article_id, place_id, name, lat, lon, precision, role, conf, country)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (article_id, place_id, role) DO UPDATE SET
    name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    precision = EXCLUDED.precision, conf = EXCLUDED.conf,
    country = EXCLUDED.country`
	var missing []string
	for _, r := range records {
		if r.ID == "" {
			continue
		}
		tag, err := tx.Exec(ctx, articleSQL, r.ID, r.Category, r.CategoryConfidence, r.Lang)
		if err != nil {
			return nil, fmt.Errorf("store: enrich article %s: %w", r.ID, err)
		}
		if tag.RowsAffected() == 0 {
			missing = append(missing, r.ID)
			continue
		}
		for _, loc := range r.Locations {
			if _, err := tx.Exec(ctx, locationSQL, r.ID, loc.PlaceID, loc.Name, loc.Lat,
				loc.Lon, loc.Precision, loc.Role, loc.Confidence, loc.Country); err != nil {
				return nil, fmt.Errorf("store: enrich article location %s/%s: %w", r.ID, loc.PlaceID, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit enrichment batch: %w", err)
	}
	return missing, nil
}

func formatVector(v []float32) string {
	if len(v) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// FindStoryCandidates is the pgvector ANN shortlist used by the clusterer.
// It returns live story ids ordered by cosine distance; the caller still
// applies time/place/entity guards. An empty vector returns no rows.
//
// The query is bounded even when the caller forgets a deadline: the clusterer
// is the single story writer and must not stall behind Postgres. Callers that
// need a tighter bound supply their own context deadline.
func (s *Store) FindStoryCandidates(ctx context.Context, vector []float32, limit int) ([]string, error) {
	if len(vector) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 64 {
		limit = 32
	}
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
SELECT id
FROM stories
WHERE expires_at > now() AND centroid IS NOT NULL
ORDER BY centroid <=> $1::vector
LIMIT $2`, formatVector(vector), limit)
	if err != nil {
		return nil, fmt.Errorf("store: story candidates: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: story candidate scan: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
