package store

import (
	"context"
	"fmt"
	"time"
)

// UpsertLinks mirrors relation upserts and removals. A Remove record deletes
// the pair; ordinary upserts preserve an existing editor "why" text.
func (s *Store) UpsertLinks(ctx context.Context, records []LinkRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin links batch: %w", err)
	}
	defer tx.Rollback(ctx)
	const upsert = `
INSERT INTO links (a, b, type, direction, confidence, strength, why, updated_at, decays_at, unverified)
VALUES ($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9)
ON CONFLICT (a, b) DO UPDATE SET
    type = EXCLUDED.type,
    direction = EXCLUDED.direction,
    confidence = EXCLUDED.confidence,
    strength = EXCLUDED.strength,
    updated_at = EXCLUDED.updated_at,
    decays_at = EXCLUDED.decays_at,
    unverified = EXCLUDED.unverified`
	for _, r := range records {
		if r.A == "" || r.B == "" || r.A == r.B {
			continue
		}
		if r.Remove {
			if _, err := tx.Exec(ctx, `DELETE FROM links WHERE a=$1 AND b=$2`, r.A, r.B); err != nil {
				return fmt.Errorf("store: remove link %s/%s: %w", r.A, r.B, err)
			}
			continue
		}
		if r.UpdatedAt.IsZero() {
			r.UpdatedAt = time.Now().UTC()
		}
		if r.DecaysAt.IsZero() {
			r.DecaysAt = r.UpdatedAt.Add(48 * time.Hour)
		}
		if _, err := tx.Exec(ctx, upsert, r.A, r.B, r.Type, r.Direction, r.Confidence,
			r.Strength, r.UpdatedAt, r.DecaysAt, r.Unverified); err != nil {
			return fmt.Errorf("store: upsert link %s/%s: %w", r.A, r.B, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit links: %w", err)
	}
	return nil
}

// UpsertStoryTexts mirrors the story brief cache. Empty text leaves an
// existing editor brief untouched. A non-empty editor title is applied with
// title_source='editor' so a later cluster upsert cannot revert it.
func (s *Store) UpsertStoryTexts(ctx context.Context, records []StoryTextRecord) error {
	if len(records) == 0 {
		return nil
	}
	const sql = `
UPDATE stories
SET brief = CASE
        WHEN $2 <> '' THEN $2
        ELSE brief END,
    brief_source = CASE
        WHEN $2 <> '' THEN CASE WHEN $3 <> '' THEN $3 ELSE 'generated' END
        ELSE brief_source END,
    brief_model = CASE
        WHEN $2 <> '' AND $6 <> '' THEN $6
        ELSE brief_model END,
    title = CASE
        WHEN $4 <> '' AND $5 = 'editor' THEN $4
        ELSE title END,
    title_source = CASE
        WHEN $4 <> '' AND $5 = 'editor' THEN 'editor'
        ELSE title_source END
WHERE id = $1`
	for _, r := range records {
		if r.ID == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, sql, r.ID, r.Brief, r.BriefSource, r.Title, r.TitleSource, r.Model); err != nil {
			return fmt.Errorf("store: story text %s: %w", r.ID, err)
		}
	}
	return nil
}

// UpsertLinkTexts mirrors the "why" cache. A missing durable link is a no-op:
// the link upsert and its text are queued independently, and the next text
// generation will retry naturally.
func (s *Store) UpsertLinkTexts(ctx context.Context, records []LinkTextRecord) error {
	if len(records) == 0 {
		return nil
	}
	const sql = `UPDATE links SET why = CASE WHEN $3 <> '' THEN $3 ELSE why END WHERE a=$1 AND b=$2`
	for _, r := range records {
		if r.A == "" || r.B == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, sql, r.A, r.B, r.Why); err != nil {
			return fmt.Errorf("store: link text %s/%s: %w", r.A, r.B, err)
		}
	}
	return nil
}
