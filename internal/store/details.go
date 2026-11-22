package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrStoryNotFound is returned when a requested story is absent from both the
// KV hot path and the durable history.
var ErrStoryNotFound = errors.New("store: story not found")

// StoryArticle is one source article for the story panel. PublishedAt is
// stored as time.Time and serialized by the gateway.
type StoryArticle struct {
	ID          string
	SourceID    string
	Title       string
	URL         string
	PublishedAt time.Time
	Lang        string
}

// SourceInfo is the panel-facing source row (never the article-body path).
type SourceInfo struct {
	ID           string
	Name         string
	URL          string
	Enabled      bool
	Health       string
	ArticleCount int
}

// StoryDetail is the durable fallback for GET /api/stories/{id}.
type StoryDetail struct {
	ID                 string
	Title              string
	TitleSource        string
	Brief              string
	BriefSource        string
	Category           int16
	CategoryBackend    string
	CategoryConfidence float32
	Degraded           bool
	Fallback           bool
	PlaceBackend       string
	PlaceDegraded      bool
	PlaceFallback      bool
	SourceCount        int
	Heat               float64
	Primary            uint32
	PrimaryLoc         string
	FirstSeen          time.Time
	UpdatedAt          time.Time
	ExpiresAt          time.Time
	Locations          []LocationRecord
	Articles           []StoryArticle
	Sources            []SourceInfo
	ArticlesTruncated  bool
}

// StorySummary is one search/history row.
type StorySummary struct {
	ID           string
	Title        string
	Category     int16
	SourceCount  int
	Heat         float64
	FirstSeen    time.Time
	UpdatedAt    time.Time
	PrimaryPlace string
}

// GetStoryDetail loads a story, its location votes, bounded source articles
// and source labels from Postgres. It is the history/fallback path; current
// stories are served from live_state first.
func (s *Store) GetStoryDetail(ctx context.Context, id string, limit int) (StoryDetail, error) {
	if strings.TrimSpace(id) == "" {
		return StoryDetail{}, ErrStoryNotFound
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	var (
		d StoryDetail
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, title, title_source, brief, brief_source, category, category_backend, cat_conf,
       degraded, fallback, place_backend, place_degraded, place_fallback,
       source_count, heat, primary_loc, first_seen, updated_at, expires_at
FROM stories WHERE id=$1`, id).Scan(
		&d.ID, &d.Title, &d.TitleSource, &d.Brief, &d.BriefSource, &d.Category,
		&d.CategoryBackend, &d.CategoryConfidence, &d.Degraded, &d.Fallback,
		&d.PlaceBackend, &d.PlaceDegraded, &d.PlaceFallback,
		&d.SourceCount, &d.Heat, &d.PrimaryLoc, &d.FirstSeen, &d.UpdatedAt, &d.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return StoryDetail{}, ErrStoryNotFound
	}
	if err != nil {
		return StoryDetail{}, fmt.Errorf("store: story detail %s: %w", id, err)
	}
	rows, err := s.pool.Query(ctx, `
SELECT place_id, name, lat, lon, precision, role, weight, country
FROM story_locations WHERE story_id=$1
ORDER BY role, weight DESC`, id)
	if err != nil {
		return StoryDetail{}, fmt.Errorf("store: story locations %s: %w", id, err)
	}
	for rows.Next() {
		var l LocationRecord
		if err := rows.Scan(&l.PlaceID, &l.Name, &l.Lat, &l.Lon, &l.Precision, &l.Role, &l.Weight, &l.Country); err != nil {
			rows.Close()
			return StoryDetail{}, err
		}
		d.Locations = append(d.Locations, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return StoryDetail{}, err
	}
	for i, l := range d.Locations {
		if l.PlaceID == d.PrimaryLoc {
			d.Primary = uint32(i)
			break
		}
	}
	articleRows, err := s.pool.Query(ctx, `
SELECT id, source_id, title, url, published_at, lang
FROM articles a WHERE story_id=$1
  AND NOT EXISTS (SELECT 1 FROM wire_time_suppression s WHERE s.article_id = a.id)
ORDER BY published_at DESC LIMIT $2`, id, limit+1)
	if err != nil {
		return StoryDetail{}, fmt.Errorf("store: story articles %s: %w", id, err)
	}
	for articleRows.Next() {
		var a StoryArticle
		if err := articleRows.Scan(&a.ID, &a.SourceID, &a.Title, &a.URL, &a.PublishedAt, &a.Lang); err != nil {
			articleRows.Close()
			return StoryDetail{}, err
		}
		d.Articles = append(d.Articles, a)
	}
	articleRows.Close()
	if err := articleRows.Err(); err != nil {
		return StoryDetail{}, err
	}
	if len(d.Articles) > limit {
		d.Articles = d.Articles[:limit]
		d.ArticlesTruncated = true
	}
	sourceIDs := make([]string, 0, len(d.Articles))
	seen := make(map[string]bool)
	for _, a := range d.Articles {
		if a.SourceID != "" && !seen[a.SourceID] {
			seen[a.SourceID] = true
			sourceIDs = append(sourceIDs, a.SourceID)
		}
	}
	sources, err := s.GetSources(ctx, sourceIDs)
	if err != nil {
		return StoryDetail{}, err
	}
	sourceMap := make(map[string]SourceInfo, len(sources))
	for _, src := range sources {
		sourceMap[src.ID] = src
	}
	for _, article := range d.Articles {
		info, ok := sourceMap[article.SourceID]
		if !ok {
			info = SourceInfo{ID: article.SourceID, Health: "unknown"}
		}
		info.ArticleCount++
		sourceMap[article.SourceID] = info
	}
	for _, id := range sourceIDs {
		info := sourceMap[id]
		info.ID = id
		d.Sources = append(d.Sources, info)
	}
	return d, nil
}

// GetSources loads durable source labels and health state. Unknown ids are
// returned with an empty name; the gateway derives a display fallback.
func (s *Store) GetSources(ctx context.Context, ids []string) ([]SourceInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
SELECT id, name, url, enabled, health_json
FROM sources WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("store: sources: %w", err)
	}
	defer rows.Close()
	var out []SourceInfo
	for rows.Next() {
		var (
			info    SourceInfo
			health  []byte
			enabled bool
		)
		if err := rows.Scan(&info.ID, &info.Name, &info.URL, &enabled, &health); err != nil {
			return nil, err
		}
		info.Enabled = enabled
		info.Health = healthState(enabled, health)
		out = append(out, info)
	}
	return out, rows.Err()
}

// GetArticlesByIDs is the KV-miss fallback for current stories. It returns
// only the bounded article fields the panel needs.
func (s *Store) GetArticlesByIDs(ctx context.Context, ids []string) ([]StoryArticle, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
SELECT id, source_id, title, url, published_at, lang
FROM articles a WHERE id = ANY($1)
  AND NOT EXISTS (SELECT 1 FROM wire_time_suppression s WHERE s.article_id = a.id)`, ids)
	if err != nil {
		return nil, fmt.Errorf("store: articles: %w", err)
	}
	defer rows.Close()
	var out []StoryArticle
	for rows.Next() {
		var a StoryArticle
		if err := rows.Scan(&a.ID, &a.SourceID, &a.Title, &a.URL, &a.PublishedAt, &a.Lang); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SearchStories backs GET /api/stories?query=&limit=. It searches the durable
// history, including expired stories, and never blocks the live KV path.
func (s *Store) SearchStories(ctx context.Context, query string, limit int) ([]StorySummary, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	query = strings.TrimSpace(query)
	rows, err := s.pool.Query(ctx, `
SELECT s.id, s.title, s.category, s.source_count, s.heat, s.first_seen, s.updated_at,
       COALESCE((SELECT name FROM story_locations sl WHERE sl.story_id=s.id
                 ORDER BY (sl.role=0) DESC, sl.weight DESC LIMIT 1), '')
FROM stories s
WHERE ($1='' OR s.title ILIKE '%' || $1 || '%')
ORDER BY s.updated_at DESC LIMIT $2`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("store: search stories: %w", err)
	}
	defer rows.Close()
	var out []StorySummary
	for rows.Next() {
		var s StorySummary
		if err := rows.Scan(&s.ID, &s.Title, &s.Category, &s.SourceCount, &s.Heat,
			&s.FirstSeen, &s.UpdatedAt, &s.PrimaryPlace); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func healthState(enabled bool, raw []byte) string {
	if !enabled {
		return "disabled"
	}
	if len(raw) == 0 {
		return "unknown"
	}
	var h struct {
		State string `json:"state"`
	}
	if json.Unmarshal(raw, &h) != nil || h.State == "" {
		return "unknown"
	}
	switch h.State {
	case "ok":
		return "ok"
	case "backoff", "quarantined", "error", "stale":
		return "error"
	default:
		return "unknown"
	}
}
