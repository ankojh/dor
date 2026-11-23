package alert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Postgres mirrors alert configuration and every escalation generation.

func (s *Store) listDB(ctx context.Context) ([]Alert, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT id, owner, prompt, min_sources, region, strictness, channels, enabled,
		       created_at, updated_at, ntfy_topic, daily_digest, digest_hour_utc
		FROM alerts ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Alert
	for rows.Next() {
		var a Alert
		var channels []byte
		if err := rows.Scan(&a.ID, &a.Owner, &a.Prompt, &a.MinSources, &a.Region, &a.Strictness,
			&channels, &a.Enabled, &a.CreatedAt, &a.UpdatedAt, &a.NtfyTopic, &a.DailyDigest, &a.DigestHourUTC); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(channels, &a.Channels)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) getDB(ctx context.Context, id string) (Alert, error) {
	var a Alert
	var channels []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT id, owner, prompt, min_sources, region, strictness, channels, enabled,
		       created_at, updated_at, ntfy_topic, daily_digest, digest_hour_utc
		FROM alerts WHERE id=$1`, id).
		Scan(&a.ID, &a.Owner, &a.Prompt, &a.MinSources, &a.Region, &a.Strictness,
			&channels, &a.Enabled, &a.CreatedAt, &a.UpdatedAt, &a.NtfyTopic, &a.DailyDigest, &a.DigestHourUTC)
	if err != nil {
		return Alert{}, err
	}
	_ = json.Unmarshal(channels, &a.Channels)
	return a, nil
}

func (s *Store) insertDB(ctx context.Context, a Alert) error {
	channels, _ := json.Marshal(a.Channels)
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO alerts (id, owner, prompt, min_sources, region, strictness, channels, enabled, created_at, updated_at, ntfy_topic, daily_digest, digest_hour_utc)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO NOTHING`,
		a.ID, a.Owner, a.Prompt, a.MinSources, a.Region, a.Strictness, string(channels), a.Enabled, a.CreatedAt, a.UpdatedAt, a.NtfyTopic, a.DailyDigest, a.DigestHourUTC)
	return err
}

func (s *Store) upsertDB(ctx context.Context, a Alert) error {
	channels, _ := json.Marshal(a.Channels)
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO alerts (id, owner, prompt, min_sources, region, strictness, channels, enabled, created_at, updated_at, ntfy_topic, daily_digest, digest_hour_utc)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO UPDATE SET
			owner=EXCLUDED.owner, prompt=EXCLUDED.prompt, min_sources=EXCLUDED.min_sources, region=EXCLUDED.region,
			strictness=EXCLUDED.strictness, channels=EXCLUDED.channels, enabled=EXCLUDED.enabled,
			updated_at=EXCLUDED.updated_at, ntfy_topic=EXCLUDED.ntfy_topic,
 daily_digest=EXCLUDED.daily_digest, digest_hour_utc=EXCLUDED.digest_hour_utc`,
		a.ID, a.Owner, a.Prompt, a.MinSources, a.Region, a.Strictness, string(channels), a.Enabled, a.CreatedAt, a.UpdatedAt, a.NtfyTopic, a.DailyDigest, a.DigestHourUTC)
	return err
}

func (s *Store) deleteDB(ctx context.Context, id string) error {
	_, err := s.db.Pool().Exec(ctx, `DELETE FROM alerts WHERE id=$1`, id)
	return err
}

func (s *Store) insertHitDB(ctx context.Context, h Hit) error {
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO alert_hits (alert_id, story_id, level, fired_at, title, source_count, generation, approx, base_sources)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`,
		h.AlertID, h.StoryID, h.Level, h.FiredAt, h.Title, h.SourceCount, h.Generation, h.Approx, h.BaseSources)
	return err
}

func (s *Store) hitStateDB(ctx context.Context, alertID, storyID string) (Hit, bool, error) {
	var h Hit
	err := s.db.Pool().QueryRow(ctx, `
		SELECT alert_id, story_id, level, fired_at, title, source_count, generation, approx, base_sources
		FROM alert_hits WHERE alert_id=$1 AND story_id=$2
		ORDER BY generation DESC LIMIT 1`, alertID, storyID).
		Scan(&h.AlertID, &h.StoryID, &h.Level, &h.FiredAt, &h.Title, &h.SourceCount, &h.Generation, &h.Approx, &h.BaseSources)
	if errors.Is(err, pgx.ErrNoRows) {
		return Hit{}, false, nil
	}
	if err != nil {
		return Hit{}, false, err
	}
	return h, true, nil
}

func (s *Store) listHitsDB(ctx context.Context, alertID string) ([]Hit, error) {
	q := `SELECT h.alert_id, h.story_id, h.level, h.fired_at, h.title, h.source_count, h.generation, h.approx, h.base_sources, a.prompt
	      FROM alert_hits h JOIN alerts a ON a.id = h.alert_id`
	var args []any
	if alertID != "" {
		q += ` WHERE h.alert_id=$1`
		args = append(args, alertID)
	}
	q += ` ORDER BY h.fired_at DESC LIMIT 500`
	rows, err := s.db.Pool().Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Hit
	for rows.Next() {
		var h Hit
		if err := rows.Scan(&h.AlertID, &h.StoryID, &h.Level, &h.FiredAt, &h.Title, &h.SourceCount, &h.Generation, &h.Approx, &h.BaseSources, &h.Prompt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) muteDB(ctx context.Context, alertID, storyID string) error {
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO alert_mutes (alert_id, story_id, created_at) VALUES ($1,$2,$3)
		ON CONFLICT DO NOTHING`, alertID, storyID, time.Now().UTC())
	return err
}

func (s *Store) mutedDB(ctx context.Context, alertID, storyID string) bool {
	var n int
	err := s.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM alert_mutes WHERE alert_id=$1 AND story_id=$2`, alertID, storyID).Scan(&n)
	return err == nil && n > 0
}

func (s *Store) savePushDB(ctx context.Context, rec PushRecord) error {
	payload, err := json.Marshal(map[string]string{"blob": base64.StdEncoding.EncodeToString(rec.Blob)})
	if err != nil {
		return err
	}
	// Owner is immutable once bound. A conflicting id owned by someone else is
	// a hard failure, never an update and never a silent no-op.
	for attempt := 0; attempt < 3; attempt++ {
		tag, err := s.db.Pool().Exec(ctx, `
			INSERT INTO push_subscriptions (id, owner, endpoint, keys, created_at)
			VALUES ($1, $2, 'encrypted', $3, $4)
			ON CONFLICT (id) DO NOTHING`,
			rec.ID, rec.Owner, string(payload), rec.CreatedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		var actual string
		err = s.db.Pool().QueryRow(ctx, `SELECT owner FROM push_subscriptions WHERE id=$1`, rec.ID).Scan(&actual)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if actual != rec.Owner {
			return ErrPushOwnerMismatch
		}
		_, err = s.db.Pool().Exec(ctx, `
			UPDATE push_subscriptions SET keys=$2, created_at=$3
			WHERE id=$1 AND owner=$4`, rec.ID, string(payload), rec.CreatedAt, rec.Owner)
		return err
	}
	return errors.New("alert: push subscription mirror contention")
}

func (s *Store) deletePushDB(ctx context.Context, owner, id string) error {
	var deleted string
	err := s.db.Pool().QueryRow(ctx,
		`DELETE FROM push_subscriptions WHERE id=$1 AND owner=$2 RETURNING owner`, id, owner).Scan(&deleted)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// No row matched this owner. Distinguish an absent row (idempotent delete)
	// from a row bound to a different owner (fail closed, never delete it).
	var actual string
	err = s.db.Pool().QueryRow(ctx, `SELECT owner FROM push_subscriptions WHERE id=$1`, id).Scan(&actual)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if actual != owner {
		return ErrPushOwnerMismatch
	}
	return nil
}

func (s *Store) pushSubscriptionsDB(ctx context.Context, owner string) ([]PushRecord, error) {
	rows, err := s.db.Pool().Query(ctx, `SELECT id, owner, keys, created_at FROM push_subscriptions WHERE owner=$1`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PushRecord
	for rows.Next() {
		var rec PushRecord
		var payload []byte
		if err := rows.Scan(&rec.ID, &rec.Owner, &payload, &rec.CreatedAt); err != nil {
			return nil, err
		}
		var wrapped map[string]string
		if json.Unmarshal(payload, &wrapped) != nil {
			continue
		}
		if rec.Blob, err = base64.StdEncoding.DecodeString(wrapped["blob"]); err != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}
