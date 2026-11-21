// Package alert owns prompt alerts: CRUD, hit state, mutes, rate limiting and
// encrypted push subscriptions. Hot state is mirrored in the live_state KV
// bucket for the matcher; Postgres is the durable copy when available.
package alert

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/store"
)

// ErrNotFound is returned when an alert id does not exist.
var ErrNotFound = errors.New("alert: not found")

// Alert is one saved prompt alert.
type Alert struct {
	ID         string    `json:"id"`
	Prompt     string    `json:"prompt"`
	MinSources int       `json:"min_sources"`
	Region     string    `json:"region"`
	Strictness float64   `json:"strictness"`
	Channels   []string  `json:"channels"`
	Enabled    bool      `json:"enabled"`
	NtfyTopic  string    `json:"ntfy_topic,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
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
}

// Patch is a partial update from PATCH /api/alerts/{id}.
type Patch struct {
	Prompt     *string   `json:"prompt"`
	MinSources *int      `json:"min_sources"`
	Region     *string   `json:"region"`
	Strictness *float64  `json:"strictness"`
	Channels   *[]string `json:"channels"`
	Enabled    *bool     `json:"enabled"`
}

// PushRecord is one encrypted Web Push subscription blob.
type PushRecord struct {
	ID        string    `json:"id"`
	Blob      []byte    `json:"blob"`
	CreatedAt time.Time `json:"created_at"`
}

// Store persists alerts. KV is always present; Postgres is optional.
type Store struct {
	kv  jetstream.KeyValue
	db  *store.Store
	log *slog.Logger
	now func() time.Time

	mu sync.Mutex
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
	s := &Store{kv: kv, db: db, log: log, now: time.Now}
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
	var a Alert
	if s.getKV(ctx, alertKey(id), &a) {
		return a, true
	}
	if s.db != nil {
		if a, err := s.getDB(ctx, id); err == nil {
			_ = s.putKV(ctx, alertKey(id), a)
			return a, true
		}
	}
	return Alert{}, false
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

// RecordHit stores a firing and its escalation state, best effort in Postgres.
func (s *Store) RecordHit(ctx context.Context, h Hit) {
	h.FiredAt = h.FiredAt.UTC()
	_ = s.putKV(ctx, hitKey(h.AlertID, h.StoryID), h)
	if s.db != nil {
		if err := s.insertHitDB(ctx, h); err != nil {
			s.log.Warn("alert: hit insert failed", "alert", h.AlertID, "story", h.StoryID, "err", err)
		}
	}
}

// HitState returns the last hit for a pair (dedupe/escalation).
func (s *Store) HitState(ctx context.Context, alertID, storyID string) (Hit, bool) {
	var h Hit
	if s.getKV(ctx, hitKey(alertID, storyID), &h) {
		return h, true
	}
	if s.db != nil {
		if h, ok := s.hitStateDB(ctx, alertID, storyID); ok {
			// Repopulate the hot copy so the next story change is a KV read.
			_ = s.putKV(ctx, hitKey(alertID, storyID), h)
			return h, true
		}
	}
	return Hit{}, false
}

// Hits returns an alert's hits, newest first. alertID may be empty for all.
func (s *Store) Hits(ctx context.Context, alertID string) []Hit {
	if s.db != nil {
		if hits, err := s.listHitsDB(ctx, alertID); err == nil {
			return hits
		}
	}
	var out []Hit
	prefix := "alerthit."
	for _, k := range s.keys(ctx, prefix) {
		var h Hit
		if !s.getKV(ctx, k, &h) {
			continue
		}
		if alertID != "" && h.AlertID != alertID {
			continue
		}
		out = append(out, h)
	}
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

// AllowRate applies a per-alert minimum interval.
func (s *Store) AllowRate(ctx context.Context, alertID string, min time.Duration) bool {
	key := "alertrate." + alertID
	var last int64
	s.getKV(ctx, key, &last)
	now := s.now().Unix()
	if last > 0 && now-last < int64(min.Seconds()) {
		return false
	}
	_ = s.putKV(ctx, key, now)
	return true
}

// Attempts returns the current delivery attempt count for one hit.
func (s *Store) Attempts(ctx context.Context, alertID, storyID string) int {
	var n int
	s.getKV(ctx, "alertattempt."+alertID+"."+storyID, &n)
	return n
}

// BumpAttempt increments the delivery attempt counter for one hit.
func (s *Store) BumpAttempt(ctx context.Context, alertID, storyID string) int {
	key := "alertattempt." + alertID + "." + storyID
	var n int
	s.getKV(ctx, key, &n)
	n++
	_ = s.putKV(ctx, key, n)
	return n
}

// ClearAttempt removes the attempt counter after a successful delivery.
func (s *Store) ClearAttempt(ctx context.Context, alertID, storyID string) {
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	_ = s.kv.Delete(opCtx, "alertattempt."+alertID+"."+storyID)
}

// SavePush stores an encrypted subscription blob. Postgres keeps only the
// ciphertext; endpoint and keys are never written in plaintext.
func (s *Store) SavePush(ctx context.Context, id string, blob []byte) error {
	rec := PushRecord{ID: id, Blob: blob, CreatedAt: s.now().UTC()}
	if err := s.putKV(ctx, "pushsub."+id, rec); err != nil {
		return err
	}
	if s.db != nil {
		if err := s.savePushDB(ctx, rec); err != nil {
			s.log.Warn("alert: push subscription mirror failed", "id", id, "err", err)
		}
	}
	return nil
}

// PushSubscriptions returns the encrypted subscriptions, KV first and the
// durable mirror when KV expired.
func (s *Store) PushSubscriptions(ctx context.Context) []PushRecord {
	var out []PushRecord
	for _, k := range s.keys(ctx, "pushsub.") {
		var p PushRecord
		if s.getKV(ctx, k, &p) {
			out = append(out, p)
		}
	}
	if len(out) == 0 && s.db != nil {
		if records, err := s.pushSubscriptionsDB(ctx); err == nil {
			return records
		}
	}
	return out
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
