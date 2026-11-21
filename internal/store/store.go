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
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
    lang = EXCLUDED.lang,
    dateline = EXCLUDED.dateline,
    category = EXCLUDED.category,
    cat_conf = EXCLUDED.cat_conf,
    story_id = COALESCE(EXCLUDED.story_id, articles.story_id)`
	const locationSQL = `
INSERT INTO article_locations (article_id, place_id, name, lat, lon, precision, role, conf)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (article_id, place_id, role) DO UPDATE SET
    name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    precision = EXCLUDED.precision, conf = EXCLUDED.conf`
	for _, r := range records {
		batch.Queue(articleSQL, r.ID, r.SourceID, r.URL, r.Title, r.Lede, r.PublishedAt,
			r.Lang, r.Dateline, r.Category, r.CategoryConfidence, r.StoryID, r.CreatedAt)
		for _, loc := range r.Locations {
			batch.Queue(locationSQL, r.ID, loc.PlaceID, loc.Name, loc.Lat, loc.Lon,
				loc.Precision, loc.Role, loc.Confidence)
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
func (s *Store) UpsertStories(ctx context.Context, records []StoryRecord) error {
	if len(records) == 0 {
		return nil
	}
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin stories batch: %w", err)
	}
	defer tx.Rollback(ctx)
	const storySQL = `
INSERT INTO stories (id, title, brief, category, primary_loc, source_count, heat,
                     centroid, entities, first_seen, updated_at, expires_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,'')::vector,$9,$10,$11,$12)
ON CONFLICT (id) DO UPDATE SET
    title = EXCLUDED.title,
    brief = CASE WHEN stories.brief = '' THEN EXCLUDED.brief ELSE stories.brief END,
    category = EXCLUDED.category,
    primary_loc = EXCLUDED.primary_loc,
    source_count = EXCLUDED.source_count,
    heat = EXCLUDED.heat,
    centroid = COALESCE(EXCLUDED.centroid, stories.centroid),
    entities = EXCLUDED.entities,
    first_seen = LEAST(stories.first_seen, EXCLUDED.first_seen),
    updated_at = EXCLUDED.updated_at,
    expires_at = EXCLUDED.expires_at`
	for _, r := range records {
		if r.Title == "" {
			r.Title = "Untitled"
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
			r.ID, r.Title, r.Brief, r.Category, r.PrimaryLoc, r.SourceCount, r.Heat,
			formatVector(r.Centroid), entities, r.FirstSeen, r.UpdatedAt, r.ExpiresAt,
		); err != nil {
			return fmt.Errorf("store: upsert story %s: %w", r.ID, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM story_locations WHERE story_id = $1`, r.ID); err != nil {
			return fmt.Errorf("store: clear story locations %s: %w", r.ID, err)
		}
		for _, loc := range r.Locations {
			if _, err := tx.Exec(ctx, `
INSERT INTO story_locations (story_id, place_id, name, lat, lon, precision, role, weight)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				r.ID, loc.PlaceID, loc.Name, loc.Lat, loc.Lon, loc.Precision, loc.Role, loc.Weight,
			); err != nil {
				return fmt.Errorf("store: story location %s/%s: %w", r.ID, loc.PlaceID, err)
			}
		}
		if len(r.ArticleIDs) > 0 {
			if _, err := tx.Exec(ctx, `
UPDATE articles SET story_id = $2,
    category = CASE WHEN $3::smallint <> 0 THEN $3::smallint ELSE category END
WHERE id = ANY($1)`, r.ArticleIDs, r.ID, r.Category); err != nil {
				return fmt.Errorf("store: assign story %s: %w", r.ID, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit stories batch: %w", err)
	}
	return nil
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
func (s *Store) FindStoryCandidates(ctx context.Context, vector []float32, limit int) ([]string, error) {
	if len(vector) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 64 {
		limit = 32
	}
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
