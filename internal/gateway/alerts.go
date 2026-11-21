package gateway

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/alert"
)

const maxAlertBody = 64 << 10

// withCORS adds the headers the local web app needs and answers preflight.
// Credentials are not used; the gateway is bound to loopback by default.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
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

// handleAlertsList serves GET /api/alerts.
func (s *Server) handleAlertsList(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	alerts := s.cfg.Alerts.List(r.Context())
	if alerts == nil {
		alerts = []alert.Alert{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts})
}

type alertInput struct {
	Prompt     string   `json:"prompt"`
	MinSources int      `json:"min_sources"`
	Region     string   `json:"region"`
	Strictness float64  `json:"strictness"`
	Channels   []string `json:"channels"`
	Enabled    *bool    `json:"enabled"`
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
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	created, err := s.cfg.Alerts.Create(r.Context(), alert.Alert{
		Prompt:     in.Prompt,
		MinSources: in.MinSources,
		Region:     in.Region,
		Strictness: in.Strictness,
		Channels:   in.Channels,
		Enabled:    enabled,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
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
	var patch alert.Patch
	if !decodeJSON(w, r, &patch) {
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
	if err := s.cfg.Alerts.Delete(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

type backtestInput struct {
	Prompt     string  `json:"prompt"`
	MinSources int     `json:"min_sources"`
	Region     string  `json:"region"`
	Strictness float64 `json:"strictness"`
}

// handleAlertsBacktest serves POST /api/alerts/backtest. It is bounded to the
// last 24 h and 200 stories by the matcher.
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
	if strings.TrimSpace(in.Prompt) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prompt is required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, s.cfg.Matcher.Backtest(ctx,
		in.Prompt, in.MinSources, in.Region, in.Strictness))
}

// handleAlertsHits serves GET /api/alerts/hits?alert_id=.
func (s *Server) handleAlertsHits(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	hits := s.cfg.Alerts.Hits(r.Context(), strings.TrimSpace(r.URL.Query().Get("alert_id")))
	if hits == nil {
		hits = []alert.Hit{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"hits": hits})
}

// handleAlertsMute serves POST /api/alerts/{id}/mute.
func (s *Server) handleAlertsMute(w http.ResponseWriter, r *http.Request) {
	if s.alertsUnavailable(w) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
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

type pushSubscriptionInput struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		P256DH string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
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
	var in pushSubscriptionInput
	if !decodeJSON(w, r, &in) {
		return
	}
	if !strings.HasPrefix(in.Endpoint, "https://") ||
		strings.TrimSpace(in.Keys.P256DH) == "" || strings.TrimSpace(in.Keys.Auth) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "endpoint and keys are required"})
		return
	}
	blob, err := encryptPush(s.cfg.Push.EncryptionKey, in)
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

// encryptPush seals the subscription JSON with AES-GCM: nonce || ciphertext.
func encryptPush(key []byte, v any) ([]byte, error) {
	plain, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plain, nil), nil
}
