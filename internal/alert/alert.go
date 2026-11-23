// Package alert owns prompt alerts: CRUD, hit state, mutes, rate limiting and
// encrypted push subscriptions. Hot state is mirrored in the live_state KV
// bucket for the matcher; Postgres is the durable copy when available.
package alert

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/store"
)

// ErrNotFound is returned when an alert id does not exist.
var ErrNotFound = errors.New("alert: not found")

// Push subscription owner errors. Subscription writes and deletes require a
// verified nonempty anonymous session owner; unowned legacy rows are readable
// by nobody and are never deleted on another owner's behalf.
var (
	ErrPushOwnerRequired = errors.New("alert: push subscription owner is required")
	ErrPushOwnerMismatch = errors.New("alert: push subscription owner mismatch")
)

// Alert is one saved prompt alert.
type Alert struct {
	ID string `json:"id"`
	// Owner is the anonymous session that created the alert. An empty owner
	// is legacy/unowned and is visible to no REST or WebSocket client.
	Owner         string    `json:"owner,omitempty"`
	Prompt        string    `json:"prompt"`
	MinSources    int       `json:"min_sources"`
	Region        string    `json:"region"`
	Strictness    float64   `json:"strictness"`
	Channels      []string  `json:"channels"`
	Enabled       bool      `json:"enabled"`
	DailyDigest   bool      `json:"daily_digest"`
	DigestHourUTC int       `json:"digest_hour_utc"`
	NtfyTopic     string    `json:"ntfy_topic,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Hit is one alert firing for a story. Level is "new" or "escalated".
type Hit struct {
	AlertID     string    `json:"alert_id"`
	StoryID     string    `json:"story_id"`
	Level       string    `json:"level"`
	FiredAt     time.Time `json:"fired_at"`
	Title       string    `json:"title"`
	Prompt      string    `json:"prompt"`
	SourceCount int       `json:"source_count,omitempty"`
	BaseSources int       `json:"base_sources"`
	Generation  uint64    `json:"generation"`
	Approx      bool      `json:"approx"`
}

// Patch is a partial update from PATCH /api/alerts/{id}.
type Patch struct {
	Prompt        *string   `json:"prompt"`
	MinSources    *int      `json:"min_sources"`
	Region        *string   `json:"region"`
	Strictness    *float64  `json:"strictness"`
	Channels      *[]string `json:"channels"`
	Enabled       *bool     `json:"enabled"`
	DailyDigest   *bool     `json:"daily_digest"`
	DigestHourUTC *int      `json:"digest_hour_utc"`
}

// PushRecord is one encrypted Web Push subscription blob. Owner is the
// verified anonymous session that registered it; an empty owner is legacy and
// receives no delivery and no cross-owner mutation.
type PushRecord struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Blob      []byte    `json:"blob"`
	CreatedAt time.Time `json:"created_at"`
}

// Store persists alerts. KV is always present; Postgres is optional.
type Store struct {
	kv       jetstream.KeyValue
	delivery jetstream.KeyValue
	db       *store.Store
	log      *slog.Logger
	now      func() time.Time
}

// New opens the store and reloads durable alerts into KV when Postgres is
// available, so the matcher sees them after a KV expiry.
func New(ctx context.Context, b *bus.Bus, db *store.Store, log *slog.Logger) (*Store, error) {
	if b == nil {
		return nil, errors.New("alert: bus is required")
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return nil, fmt.Errorf("alert: live_state: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	delivery, err := b.KV(ctx, bus.KVBucketAlertDelivery)
	if err != nil {
		return nil, fmt.Errorf("alert: delivery bucket: %w", err)
	}
	s := &Store{kv: kv, delivery: delivery, db: db, log: log, now: time.Now}
	if db != nil {
		s.reload(ctx)
	}
	return s, nil
}

func (s *Store) reload(ctx context.Context) {
	alerts, err := s.listDB(ctx)
	if err != nil {
		s.log.Warn("alert: reload from postgres failed", "err", err)
		return
	}
	for _, a := range alerts {
		s.putKV(ctx, alertKey(a.ID), a)
	}
}

func (s *Store) putKV(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	if _, err := s.kv.Put(opCtx, key, data); err != nil {
		return err
	}
	return nil
}

func (s *Store) getKV(ctx context.Context, key string, v any) bool {
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	e, err := s.kv.Get(opCtx, key)
	if err != nil {
		return false
	}
	return json.Unmarshal(e.Value(), v) == nil
}

func (s *Store) keys(ctx context.Context, prefix string) []string {
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	keys, err := s.kv.Keys(opCtx)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Create validates and stores a new alert.
func (s *Store) Create(ctx context.Context, a Alert) (Alert, error) {
	if a.DigestHourUTC < 0 || a.DigestHourUTC > 23 {
		return Alert{}, errors.New("alert: digest_hour_utc must be 0–23")
	}
	a.Prompt = strings.TrimSpace(a.Prompt)
	if a.Prompt == "" {
		return Alert{}, errors.New("alert: prompt is required")
	}
	if a.ID == "" {
		a.ID = "alt_" + randomHex(8)
	}
	if a.MinSources <= 0 {
		a.MinSources = 1
	}
	if a.Strictness <= 0 || a.Strictness > 1 {
		a.Strictness = 0.80
	}
	if len(a.Channels) == 0 {
		a.Channels = []string{"inapp"}
	}
	a.Channels = normalizeChannels(a.Channels)
	if a.NtfyTopic == "" && contains(a.Channels, "ntfy") {
		a.NtfyTopic = "dor-" + randomHex(10)
	}
	now := s.now().UTC()
	a.CreatedAt = now
	a.UpdatedAt = now
	if err := s.putKV(ctx, alertKey(a.ID), a); err != nil {
		return Alert{}, err
	}
	if s.db != nil {
		if err := s.insertDB(ctx, a); err != nil {
			s.log.Warn("alert: postgres insert failed", "id", a.ID, "err", err)
		}
	}
	return a, nil
}

// Update applies a partial update.
func (s *Store) Update(ctx context.Context, id string, p Patch) (Alert, error) {
	a, ok := s.Get(ctx, id)
	if !ok {
		return Alert{}, ErrNotFound
	}
	if p.Prompt != nil {
		if strings.TrimSpace(*p.Prompt) == "" {
			return Alert{}, errors.New("alert: prompt is required")
		}
		a.Prompt = strings.TrimSpace(*p.Prompt)
	}
	if p.MinSources != nil {
		a.MinSources = *p.MinSources
		if a.MinSources <= 0 {
			a.MinSources = 1
		}
	}
	if p.Region != nil {
		a.Region = strings.TrimSpace(*p.Region)
	}
	if p.Strictness != nil {
		a.Strictness = *p.Strictness
		if a.Strictness <= 0 || a.Strictness > 1 {
			a.Strictness = 0.80
		}
	}
	if p.Channels != nil {
		a.Channels = normalizeChannels(*p.Channels)
		if len(a.Channels) == 0 {
			a.Channels = []string{"inapp"}
		}
	}
	if p.DailyDigest != nil {
		a.DailyDigest = *p.DailyDigest
	}
	if p.DigestHourUTC != nil {
		if *p.DigestHourUTC < 0 || *p.DigestHourUTC > 23 {
			return Alert{}, errors.New("alert: digest_hour_utc must be 0–23")
		}
		a.DigestHourUTC = *p.DigestHourUTC
	}
	if p.Enabled != nil {
		a.Enabled = *p.Enabled
	}
	if contains(a.Channels, "ntfy") && a.NtfyTopic == "" {
		a.NtfyTopic = "dor-" + randomHex(10)
	}
	a.UpdatedAt = s.now().UTC()
	if err := s.putKV(ctx, alertKey(a.ID), a); err != nil {
		return Alert{}, err
	}
	if s.db != nil {
		if err := s.upsertDB(ctx, a); err != nil {
			s.log.Warn("alert: postgres update failed", "id", a.ID, "err", err)
		}
	}
	return a, nil
}

// Delete removes an alert.
func (s *Store) Delete(ctx context.Context, id string) error {
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	_ = s.kv.Delete(opCtx, alertKey(id))
	if s.db != nil {
		_ = s.deleteDB(ctx, id)
	}
	return nil
}

// Get loads one alert.
func (s *Store) Get(ctx context.Context, id string) (Alert, bool) {
	a, ok, _ := s.Lookup(ctx, id)
	return a, ok
}

// Lookup distinguishes a deleted alert from an unavailable configuration store.
func (s *Store) Lookup(ctx context.Context, id string) (Alert, bool, error) {
	var a Alert
	e, err := s.kv.Get(ctx, alertKey(id))
	if err == nil {
		err = json.Unmarshal(e.Value(), &a)
		return a, err == nil, err
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return Alert{}, false, err
	}
	if s.db != nil {
		a, err = s.getDB(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return Alert{}, false, nil
		}
		if err != nil {
			return Alert{}, false, err
		}
		_ = s.putKV(ctx, alertKey(id), a)
		return a, true, nil
	}
	return Alert{}, false, nil
}

// List returns all alerts, sorted by creation time.
func (s *Store) List(ctx context.Context) []Alert {
	if s.db != nil {
		if alerts, err := s.listDB(ctx); err == nil {
			for _, a := range alerts {
				_ = s.putKV(ctx, alertKey(a.ID), a)
			}
			return alerts
		}
	}
	var out []Alert
	for _, k := range s.keys(ctx, "alert.") {
		var a Alert
		if s.getKV(ctx, k, &a) {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// ListOwned returns one anonymous session's alerts. An empty owner owns
// nothing, so legacy rows without an owner stay private.
func (s *Store) ListOwned(ctx context.Context, owner string) []Alert {
	if owner == "" {
		return nil
	}
	all := s.List(ctx)
	out := make([]Alert, 0, len(all))
	for _, a := range all {
		if a.Owner == owner {
			out = append(out, a)
		}
	}
	return out
}

// Identity is stable across redelivery and changes at every source-doubling generation.
func (h Hit) Identity() string {
	return identity(h.AlertID, h.StoryID, fmt.Sprint(h.Generation))
}

func identity(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// RecordHit retains every generation and advances the pair's state monotonically.
func (s *Store) RecordHit(ctx context.Context, h Hit) error {
	h.FiredAt = h.FiredAt.UTC()
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if _, err = s.delivery.Put(ctx, "hit."+h.Identity(), data); err != nil {
		return err
	}
	key := "hitstate." + identity(h.AlertID, h.StoryID)
	for {
		e, err := s.delivery.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			_, err = s.delivery.Create(ctx, key, data)
		} else if err == nil {
			var previous Hit
			if err = json.Unmarshal(e.Value(), &previous); err != nil {
				return err
			}
			if previous.Generation >= h.Generation {
				break
			}
			_, err = s.delivery.Update(ctx, key, data, e.Revision())
		}
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if s.db != nil {
		if err := s.insertHitDB(ctx, h); err != nil {
			s.log.Warn("alert: hit mirror failed; generation retained in durable KV", "alert", h.AlertID, "generation", h.Generation, "err", err)
		}
	}
	return nil
}

// HitState returns the latest generation, including after process/KV hot-cache expiry.
func (s *Store) HitState(ctx context.Context, alertID, storyID string) (Hit, bool, error) {
	var h Hit
	e, err := s.delivery.Get(ctx, "hitstate."+identity(alertID, storyID))
	if err == nil {
		if err = json.Unmarshal(e.Value(), &h); err != nil {
			return Hit{}, false, err
		}
		return h, true, nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return Hit{}, false, err
	}
	// Migration 0004 knows the original source count, while old hot state may
	// contain later escalations that the former DB primary key discarded.
	var durable Hit
	var durableOK bool
	if s.db != nil {
		durable, durableOK, err = s.hitStateDB(ctx, alertID, storyID)
		if err != nil {
			return Hit{}, false, err
		}
	}
	var legacy Hit
	legacyOK := false
	e, err = s.kv.Get(ctx, hitKey(alertID, storyID))
	if err == nil {
		if err = json.Unmarshal(e.Value(), &legacy); err != nil {
			return Hit{}, false, err
		}
		legacyOK = true
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return Hit{}, false, err
	}
	recovered, ok := reconcileLegacyHit(durable, durableOK, legacy, legacyOK)
	if !ok {
		return Hit{}, false, nil
	}
	if err = s.RecordHit(ctx, recovered); err != nil {
		return Hit{}, false, err
	}
	return recovered, true, nil
}

// reconcileLegacyHit retains the DB baseline and the newest observed source
// threshold. The old level-only DB key kept the first escalation; hot state
// kept the latest. Neither may roll the other one backwards on upgrade.
func reconcileLegacyHit(durable Hit, durableOK bool, legacy Hit, legacyOK bool) (Hit, bool) {
	if !durableOK {
		if !legacyOK {
			return Hit{}, false
		}
		if legacy.Level == "escalated" && legacy.Generation == 0 {
			legacy.Generation = 1
		}
		return legacy, true
	}
	base := durable.BaseSources
	if base <= 0 {
		base = max(1, durable.SourceCount>>min(durable.Generation, 32))
	}
	recovered := durable
	if legacyOK && legacy.SourceCount >= durable.SourceCount {
		recovered = legacy
	}
	recovered.BaseSources = base
	recovered.Generation = max(durable.Generation, recovered.Generation)
	if legacyOK {
		recovered.Generation = max(recovered.Generation, legacy.Generation)
	}
	// Old DB rows also need normalization after their hot KV entry expired.
	for generation := uint64(1); generation <= 32 && uint64(base)<<generation <= uint64(max(0, recovered.SourceCount)); generation++ {
		recovered.Generation = max(recovered.Generation, generation)
	}
	if recovered.Generation > 0 {
		recovered.Level = "escalated"
	}
	return recovered, true
}

// Hits returns all retained generations, newest first.
func (s *Store) Hits(ctx context.Context, alertID string) []Hit {
	seen := make(map[string]bool)
	var out []Hit
	if s.db != nil {
		if hits, err := s.listHitsDB(ctx, alertID); err == nil {
			for _, h := range hits {
				seen[h.Identity()] = true
				out = append(out, h)
			}
		}
	}
	_ = s.walkDelivery(ctx, "hit.>", func(e jetstream.KeyValueEntry) error {
		var h Hit
		if json.Unmarshal(e.Value(), &h) == nil && (alertID == "" || h.AlertID == alertID) && !seen[h.Identity()] {
			out = append(out, h)
			seen[h.Identity()] = true
			if s.db != nil {
				_ = s.insertHitDB(ctx, h)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].FiredAt.After(out[j].FiredAt) })
	return out
}

// Mute hides one story from one alert's future firings.
func (s *Store) Mute(ctx context.Context, alertID, storyID string) {
	_ = s.putKV(ctx, muteKey(alertID, storyID), map[string]bool{"muted": true})
	if s.db != nil {
		_ = s.muteDB(ctx, alertID, storyID)
	}
}

// IsMuted reports whether a pair is muted.
func (s *Store) IsMuted(ctx context.Context, alertID, storyID string) bool {
	if s.db != nil && s.mutedDB(ctx, alertID, storyID) {
		return true
	}
	var v map[string]bool
	return s.getKV(ctx, muteKey(alertID, storyID), &v) && v["muted"]
}

// SavePush stores an encrypted subscription blob bound to its verified
// anonymous session owner. Postgres keeps only the ciphertext; endpoint and
// keys are never written in plaintext. An empty owner is rejected so a legacy
// or caller-supplied identity cannot be persisted as if it were verified.
func (s *Store) SavePush(ctx context.Context, owner, id string, blob []byte) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return ErrPushOwnerRequired
	}
	rec := PushRecord{ID: id, Owner: owner, Blob: blob, CreatedAt: s.now().UTC()}
	rev, created, err := s.savePushKV(ctx, rec)
	if err != nil {
		return err
	}
	if s.db != nil {
		if err := s.savePushDB(ctx, rec); err != nil {
			if errors.Is(err, ErrPushOwnerMismatch) {
				// The DB mirror already binds this id to another owner. Remove
				// only the KV row this call created so the mismatch cannot be
				// served from KV after restart.
				if created {
					opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
					if derr := s.kv.Delete(opCtx, "pushsub."+id, jetstream.LastRevision(rev)); derr != nil && !errors.Is(derr, jetstream.ErrKeyNotFound) {
						s.log.Warn("alert: push subscription rollback failed", "id", id, "err", derr)
					}
					cancel()
				}
				return err
			}
			// KV is authoritative: an ordinary mirror outage keeps the
			// successful registration and is best-effort, as before.
			s.log.Warn("alert: push subscription mirror failed", "id", id, "err", err)
		}
	}
	return nil
}

// savePushKV creates or updates the encrypted record only while its owner
// binding stays unchanged. It returns the revision used and whether this call
// created the key.
func (s *Store) savePushKV(ctx context.Context, rec PushRecord) (uint64, bool, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, false, err
	}
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	key := "pushsub." + rec.ID
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := s.kv.Get(opCtx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			rev, cerr := s.kv.Create(opCtx, key, data)
			if cerr == nil {
				return rev, true, nil
			}
			if errors.Is(cerr, jetstream.ErrKeyExists) {
				continue
			}
			return 0, false, cerr
		}
		if err != nil {
			return 0, false, err
		}
		var existing PushRecord
		if err := json.Unmarshal(entry.Value(), &existing); err != nil {
			return 0, false, err
		}
		if existing.Owner != rec.Owner {
			return 0, false, ErrPushOwnerMismatch
		}
		if _, err := s.kv.Update(opCtx, key, data, entry.Revision()); err == nil {
			return entry.Revision(), false, nil
		} else if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return 0, false, err
		}
	}
	return 0, false, errors.New("alert: push subscription contention")
}

// DeletePush removes one subscription after a 404/410 response or a corrupt
// blob. The caller must name the verified owner; a foreign or unowned record
// is never removed. The DB mirror is deleted before KV so a mirror failure
// cannot leave a deleted KV entry resurrectable from DB fallback.
func (s *Store) DeletePush(ctx context.Context, owner, id string) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return ErrPushOwnerRequired
	}
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	key := "pushsub." + id
	rev := uint64(0)
	hasKV := false
	entry, err := s.kv.Get(opCtx, key)
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
	case err != nil:
		return err
	default:
		var rec PushRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return err
		}
		if rec.Owner != owner {
			return ErrPushOwnerMismatch
		}
		rev, hasKV = entry.Revision(), true
	}
	if s.db != nil {
		if err := s.deletePushDB(ctx, owner, id); err != nil {
			return err
		}
	}
	if !hasKV {
		return nil
	}
	if err := s.kv.Delete(opCtx, key, jetstream.LastRevision(rev)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	return nil
}

// PushDelivered and MarkPushDelivered retain one receipt per generation/subscription.
func (s *Store) PushDelivered(ctx context.Context, h Hit, subID string) (bool, error) {
	done, err := s.ChannelDelivered(ctx, h.Identity(), "push."+subID)
	if err != nil || done {
		return done, err
	}
	// Old receipts only identified new/first-escalation. They cannot suppress
	// later generations, and cannot be interpreted as digest delivery receipts.
	legacy := (h.Level == "new" && h.Generation == 0) || (h.Level == "escalated" && h.Generation == 1)
	if !legacy {
		return false, nil
	}
	e, err := s.kv.Get(ctx, "pushdelivered."+h.AlertID+"."+h.StoryID+"."+h.Level)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var receipts map[string]bool
	if err = json.Unmarshal(e.Value(), &receipts); err != nil {
		return false, err
	}
	if !receipts[subID] {
		return false, nil
	}
	if err = s.MarkPushDelivered(ctx, h, subID); err != nil {
		return false, err
	}
	return true, nil
}
func (s *Store) MarkPushDelivered(ctx context.Context, h Hit, subID string) error {
	return s.MarkChannelDelivered(ctx, h.Identity(), "push."+subID)
}

// PushSubscriptions returns only the owner's encrypted subscriptions, KV first
// and the durable mirror when KV expired. An empty owner yields nothing; an
// unowned legacy record is never returned to any caller.
func (s *Store) PushSubscriptions(ctx context.Context, owner string) ([]PushRecord, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, nil
	}
	var out []PushRecord
	seen := make(map[string]bool)
	w, err := s.kv.Watch(ctx, "pushsub.>", jetstream.IgnoreDeletes())
	if err != nil {
		return nil, err
	}
	defer w.Stop()
scan:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				return nil, errors.New("alert: subscription scan interrupted")
			}
			if e == nil {
				break scan
			}
			var p PushRecord
			if err = json.Unmarshal(e.Value(), &p); err != nil {
				return nil, err
			}
			seen[p.ID] = true
			if p.Owner == owner {
				out = append(out, p)
			}
		}
	}
	if s.db != nil {
		records, err := s.pushSubscriptionsDB(ctx, owner)
		if err != nil {
			return nil, err
		}
		for _, p := range records {
			if !seen[p.ID] && p.Owner == owner {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

func normalizeChannels(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, c := range in {
		c = strings.ToLower(strings.TrimSpace(c))
		switch c {
		case "inapp", "push", "ntfy":
		default:
			continue
		}
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func alertKey(id string) string  { return "alert." + id }
func hitKey(a, s string) string  { return "alerthit." + a + "." + s }
func muteKey(a, s string) string { return "alertmute." + a + "." + s }
