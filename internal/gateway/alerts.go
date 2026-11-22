package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/httpguard"
	"github.com/ankojh/dor/internal/push"
	"github.com/ankojh/dor/internal/session"
)

const (
	maxAlertBody = 64 << 10
	// maxAlertPromptRunes bounds an alert or backtest prompt. Alert prompts
	// are model input on every matching story, so an unbounded body would let
	// one request drive repeated paid work.
	maxAlertPromptRunes = 1000
)

func promptWithinBounds(prompt string) bool {
	return utf8.RuneCountInString(strings.TrimSpace(prompt)) <= maxAlertPromptRunes
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if !httpguard.RequireJSON(w, r) {
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAlertBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON: " + err.Error()})
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unexpected trailing data"})
		return false
	}
	return true
}

func (s *Server) alertsUnavailable(w http.ResponseWriter) bool {
	if s.cfg.Alerts != nil {
		return false
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"alerts":    []any{},
		"available": false,
		"error":     "alert store unavailable",
	})
	return true
}

// ownedAlert loads an alert only when the caller's anonymous session owns it.
// A foreign id is reported as not found so its existence does not leak.
func (s *Server) ownedAlert(w http.ResponseWriter, r *http.Request, id string) (alert.Alert, bool) {
	owner := session.IDFromRequest(r)
	if owner == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "session unavailable"})
		return alert.Alert{}, false
	}
	a, ok := s.cfg.Alerts.Get(r.Context(), id)
	if !ok || a.Owner != owner {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "alert not found"})
		return alert.Alert{}, false
	}
	return a, true
}

// handleAlertsList serves GET /api/alerts for the caller's session only.
func (s *Server) handleAlertsList(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	alerts := s.cfg.Alerts.ListOwned(r.Context(), session.IDFromRequest(r))
	if alerts == nil {
		alerts = []alert.Alert{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts})
}

type alertInput struct {
	Prompt        string   `json:"prompt"`
	MinSources    int      `json:"min_sources"`
	Region        string   `json:"region"`
	Strictness    float64  `json:"strictness"`
	Channels      []string `json:"channels"`
	Enabled       *bool    `json:"enabled"`
	DailyDigest   bool     `json:"daily_digest"`
	DigestHourUTC int      `json:"digest_hour_utc"`
}

// handleAlertsCreate serves POST /api/alerts.
func (s *Server) handleAlertsCreate(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	var in alertInput
	if !decodeJSON(w, r, &in) {
		return
	}
	if !promptWithinBounds(in.Prompt) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prompt is too long"})
		return
	}
	owner := session.IDFromRequest(r)
	if owner == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "session unavailable"})
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	created, err := s.cfg.Alerts.Create(r.Context(), alert.Alert{
		Owner:         owner,
		Prompt:        in.Prompt,
		DailyDigest:   in.DailyDigest,
		DigestHourUTC: in.DigestHourUTC,
		MinSources:    in.MinSources,
		Region:        in.Region,
		Strictness:    in.Strictness,
		Channels:      in.Channels,
		Enabled:       enabled,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	s.alertOwners.Store(created.ID, owner)
	writeJSON(w, http.StatusCreated, map[string]any{"alert": created})
}

// handleAlertsPatch serves PATCH /api/alerts/{id}.
func (s *Server) handleAlertsPatch(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "alert id is required"})
		return
	}
	if _, ok := s.ownedAlert(w, r, id); !ok {
		return
	}
	var patch alert.Patch
	if !decodeJSON(w, r, &patch) {
		return
	}
	if patch.Prompt != nil && !promptWithinBounds(*patch.Prompt) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prompt is too long"})
		return
	}
	updated, err := s.cfg.Alerts.Update(r.Context(), id, patch)
	if err != nil {
		if errors.Is(err, alert.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "alert not found"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"alert": updated})
}

// handleAlertsDelete serves DELETE /api/alerts/{id}.
func (s *Server) handleAlertsDelete(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "alert id is required"})
		return
	}
	if _, ok := s.ownedAlert(w, r, id); !ok {
		return
	}
	if err := s.cfg.Alerts.Delete(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	s.alertOwners.Delete(id)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

type backtestInput struct {
	Prompt     string  `json:"prompt"`
	MinSources int     `json:"min_sources"`
	Region     string  `json:"region"`
	Strictness float64 `json:"strictness"`
}

// handleAlertsBacktest serves POST /api/alerts/backtest. It is bounded to the
// last 24 h; bounded scans report explicit partial/count metadata.
func (s *Server) handleAlertsBacktest(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Matcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"available": false,
			"error":     "matcher unavailable",
			"stories":   []any{},
		})
		return
	}
	var in backtestInput
	if !decodeJSON(w, r, &in) {
		return
	}
	in.Prompt = strings.TrimSpace(in.Prompt)
	if in.Prompt == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prompt is required"})
		return
	}
	if !promptWithinBounds(in.Prompt) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prompt is too long"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, s.cfg.Matcher.Backtest(ctx,
		in.Prompt, in.MinSources, in.Region, in.Strictness))
}

// handleAlertsHits serves GET /api/alerts/hits?alert_id= for alerts owned by
// the caller's session.
func (s *Server) handleAlertsHits(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	owner := session.IDFromRequest(r)
	if owner == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "session unavailable"})
		return
	}
	alertID := strings.TrimSpace(r.URL.Query().Get("alert_id"))
	owned := make(map[string]bool)
	if alertID != "" {
		if _, ok := s.ownedAlert(w, r, alertID); !ok {
			return
		}
		owned[alertID] = true
	} else {
		for _, a := range s.cfg.Alerts.ListOwned(r.Context(), owner) {
			owned[a.ID] = true
		}
		if len(owned) == 0 {
			writeJSON(w, http.StatusOK, map[string]any{"hits": []alert.Hit{}})
			return
		}
	}
	var hits []alert.Hit
	for _, hit := range s.cfg.Alerts.Hits(r.Context(), alertID) {
		if owned[hit.AlertID] {
			hits = append(hits, hit)
		}
	}
	if hits == nil {
		hits = []alert.Hit{}
	}
	// Use decimal strings at the HTTP boundary, matching protobuf uint64's
	// precise representation in the browser without changing durable KV JSON.
	type hitJSON struct {
		alert.Hit
		Generation string `json:"generation"`
	}
	encoded := make([]hitJSON, 0, len(hits))
	for _, hit := range hits {
		encoded = append(encoded, hitJSON{Hit: hit, Generation: strconv.FormatUint(hit.Generation, 10)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"hits": encoded})
}

// handleAlertsMute serves POST /api/alerts/{id}/mute.
func (s *Server) handleAlertsMute(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if _, ok := s.ownedAlert(w, r, id); !ok {
		return
	}
	var in struct {
		StoryID string `json:"story_id"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	if id == "" || strings.TrimSpace(in.StoryID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "alert id and story_id are required"})
		return
	}
	s.cfg.Alerts.Mute(r.Context(), id, in.StoryID)
	writeJSON(w, http.StatusOK, map[string]any{"muted": true})
}

// handlePushKey serves GET /api/push/key.
func (s *Server) handlePushKey(w http.ResponseWriter, r *http.Request) {
	available, reason := s.cfg.Push.Available()
	if !available {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"available": false, "reason": reason})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"available":  true,
		"public_key": s.cfg.Push.PublicKey,
	})
}

// handlePushSubscription serves POST /api/push/subscriptions. The endpoint and
// keys are AES-GCM encrypted before the KV write; the plaintext never reaches
// the alert store.
func (s *Server) handlePushSubscription(w http.ResponseWriter, r *http.Request) {
	available, reason := s.cfg.Push.Available()
	if !available {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"subscribed": false,
			"available":  false,
			"reason":     reason,
		})
		return
	}
	if s.cfg.Alerts == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"subscribed": false,
			"available":  false,
			"reason":     "alert store unavailable",
		})
		return
	}
	var in push.Subscription
	if !decodeJSON(w, r, &in) {
		return
	}
	if !strings.HasPrefix(in.Endpoint, "https://") ||
		strings.TrimSpace(in.Keys.P256DH) == "" || strings.TrimSpace(in.Keys.Auth) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "endpoint and keys are required"})
		return
	}
	blob, err := push.EncryptSubscription(s.cfg.Push.EncryptionKey, in)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "encrypt subscription"})
		return
	}
	if err := s.cfg.Alerts.SavePush(r.Context(), "push_"+newClientID(), blob); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"subscribed": false,
			"error":      err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscribed": true})
}
