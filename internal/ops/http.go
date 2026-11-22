package ops

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/httpguard"
)

//go:embed ops.html
var opsPage []byte

const (
	opsCSRFCookie = "dor_ops_csrf"
	opsCSRFHeader = "X-Dor-Ops-Token"
	opsTokenTTL   = 12 * time.Hour
)

// GatewayInfo is the live gateway state the ops page shows. It is a callback so
// the same HTTP surface can be mounted on the gateway and on the admin server.
type GatewayInfo struct {
	Available   bool   `json:"available"`
	Clients     int    `json:"clients"`
	Seq         uint64 `json:"seq"`
	Ring        int    `json:"ring"`
	SnapshotSeq uint64 `json:"snapshot_seq"`
	Error       string `json:"error,omitempty"`
}

// HTTP serves /ops, /api/ops, the narrow runtime controls and the wrong-link
// report queue. Bus may be nil: the page then states that source health and
// report capture are unavailable instead of inventing values.
type HTTP struct {
	Bus        *bus.Bus
	Supervisor *Supervisor
	Gateway    func() GatewayInfo
	Logger     *slog.Logger

	mu        sync.Mutex
	sources   SourcesView
	sourcesAt time.Time
	reports   WrongLinksView
	reportsAt time.Time

	controlLimit *httpguard.Limiter
	reportLimit  *httpguard.Limiter
	opsSecret    [32]byte
	// Key optionally keys admission by a caller identity (the gateway's
	// anonymous session). Nil keeps the per-IP default, which is also what
	// the admin listener uses because it has no session middleware.
	Key func(*http.Request) string
}

// NewHTTP builds the backend ops surface.
func NewHTTP(b *bus.Bus, supervisor *Supervisor, log *slog.Logger) *HTTP {
	if log == nil {
		log = slog.Default()
	}
	h := &HTTP{
		Bus:          b,
		Supervisor:   supervisor,
		Logger:       log,
		controlLimit: httpguard.NewLimiter(10, 3),
		reportLimit:  httpguard.NewLimiter(20, 5),
	}
	if _, err := rand.Read(h.opsSecret[:]); err != nil {
		// Without a secret the CSRF token is forgeable. Refuse to serve the
		// ops surface rather than silently weakening it.
		panic("ops: generate session secret: " + err.Error())
	}
	return h
}

// Mount adds the ops routes to a mux. Cross-site callers are rejected by the
// origin guard applied by the server that mounts these routes.
func (h *HTTP) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /ops", h.handlePage)
	mux.HandleFunc("GET /api/ops", h.handleSnapshot)
	mux.Handle("POST /api/ops/control", h.controlLimit.MiddlewareKey(h.Key, http.HandlerFunc(h.handleControl)))
	mux.Handle("POST /api/report-wrong-link", h.reportLimit.MiddlewareKey(h.Key, http.HandlerFunc(h.handleWrongLink)))
	mux.HandleFunc("GET /api/report/wrong-link", h.handleWrongLinkList)
}

type opsResponse struct {
	GeneratedAt           string         `json:"generated_at"`
	Gateway               GatewayInfo    `json:"gateway"`
	Supervisor            *Snapshot      `json:"supervisor,omitempty"`
	SupervisorUnavailable string         `json:"supervisor_unavailable,omitempty"`
	Sources               SourcesView    `json:"sources"`
	WrongLinks            WrongLinksView `json:"wrong_links"`
}

func (h *HTTP) handlePage(w http.ResponseWriter, r *http.Request) {
	// The page carries the CSRF token. Only a same-origin navigation may read
	// it; a sibling localhost origin allowed by the CORS list must not.
	if origin := r.Header.Get("Origin"); origin != "" && !httpguard.SameOrigin(origin, r.Host) {
		httpguard.Deny(w, http.StatusForbidden, "ops page is same-origin only")
		return
	}
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
	token := ""
	if cookie, err := r.Cookie(opsCSRFCookie); err == nil && h.validOpsToken(cookie.Value) {
		token = cookie.Value
	}
	if token == "" {
		var err error
		token, err = h.newOpsToken()
		if err != nil {
			http.Error(w, "ops session unavailable", http.StatusInternalServerError)
			return
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     opsCSRFCookie,
		Value:    token,
		Path:     "/",
		MaxAge:   int(opsTokenTTL.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
	})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(bytes.ReplaceAll(opsPage, []byte("__DOR_OPS_TOKEN__"), []byte(token)))
}

// newOpsToken issues "<nonce>.<hmac>" so a cookie planted by a sibling
// localhost origin (cookies are shared across ports) still cannot be paired
// with a header the attacker can forge.
func (h *HTTP) newOpsToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(raw[:])
	return nonce + "." + h.signOpsToken(nonce), nil
}

func (h *HTTP) signOpsToken(nonce string) string {
	mac := hmac.New(sha256.New, h.opsSecret[:])
	_, _ = mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

func (h *HTTP) validOpsToken(token string) bool {
	nonce, signature, ok := strings.Cut(token, ".")
	if !ok || len(nonce) != 64 || len(signature) != 64 {
		return false
	}
	if _, err := hex.DecodeString(nonce); err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(signature), []byte(h.signOpsToken(nonce))) == 1
}

// validOpsCSRF requires the token from the page's HttpOnly cookie to be
// echoed in a request header. A foreign page cannot read either value, and a
// same-origin page that could would already run with this origin's authority.
func (h *HTTP) validOpsCSRF(r *http.Request) bool {
	cookie, err := r.Cookie(opsCSRFCookie)
	if err != nil || !h.validOpsToken(cookie.Value) {
		return false
	}
	header := strings.TrimSpace(r.Header.Get(opsCSRFHeader))
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(header)) == 1
}

func (h *HTTP) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp := opsResponse{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Gateway:     GatewayInfo{Available: false, Error: "gateway info not wired"},
		Sources:     h.loadSources(ctx),
		WrongLinks:  h.loadReports(ctx),
	}
	if h.Gateway != nil {
		resp.Gateway = h.Gateway()
	}
	if h.Supervisor == nil {
		resp.SupervisorUnavailable = "supervisor is not running in this process"
	} else {
		snap := h.Supervisor.Snapshot()
		resp.Supervisor = &snap
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}

type controlRequest struct {
	Action string `json:"action"`
	Agent  string `json:"agent,omitempty"`
	Value  int    `json:"value,omitempty"`
}

func (h *HTTP) handleControl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !httpguard.RequireJSON(w, r) {
		return
	}
	if !h.validOpsCSRF(r) {
		httpguard.Deny(w, http.StatusForbidden, "missing or invalid ops session; reload /ops")
		return
	}
	if h.Supervisor == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "supervisor is not running in this process"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	var req controlRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "invalid JSON: " + err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	msg, err := h.Supervisor.Control(ctx, req.Action, req.Agent, req.Value)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "message": msg})
}

// WrongLinkReport is one operator report waiting for labelling. It never
// carries a human label; Needs names the next actor and Status says it is
// pending.
type WrongLinkReport struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	A          string `json:"a"`
	B          string `json:"b"`
	Reason     string `json:"reason,omitempty"`
	ReportedAt string `json:"reported_at"`
	Status     string `json:"status"`
	Needs      string `json:"needs"`
	Labeler    string `json:"labeler,omitempty"`
	Origin     string `json:"origin"`
}

// WrongLinksView is the pending queue summary shown on /ops.
type WrongLinksView struct {
	Available bool              `json:"available"`
	Error     string            `json:"error,omitempty"`
	Count     int               `json:"count"`
	Latest    []WrongLinkReport `json:"latest"`
}

type wrongLinkRequest struct {
	A      string `json:"a"`
	B      string `json:"b"`
	Reason string `json:"reason"`
}

func (h *HTTP) handleWrongLink(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !httpguard.RequireJSON(w, r) {
		return
	}
	if h.Bus == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "bus is not wired"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	var req wrongLinkRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "invalid JSON"})
		return
	}
	report, err := h.captureWrongLink(r.Context(), req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	h.mu.Lock()
	h.reportsAt = time.Time{}
	h.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "report": report})
}

func (h *HTTP) captureWrongLink(ctx context.Context, req wrongLinkRequest) (WrongLinkReport, error) {
	a := strings.TrimSpace(req.A)
	b := strings.TrimSpace(req.B)
	if a == "" || b == "" {
		return WrongLinkReport{}, fmt.Errorf("both story ids are required")
	}
	if a == b {
		return WrongLinkReport{}, fmt.Errorf("a link needs two distinct story ids")
	}
	if len(a) > 200 || len(b) > 200 {
		return WrongLinkReport{}, fmt.Errorf("story ids must be at most 200 characters")
	}
	if len(req.Reason) > 500 {
		return WrongLinkReport{}, fmt.Errorf("reason must be at most 500 characters")
	}
	if a > b {
		a, b = b, a
	}
	kv, err := h.Bus.KV(ctx, bus.KVBucketEvalPending)
	if err != nil {
		return WrongLinkReport{}, fmt.Errorf("pending eval queue unavailable: %w", err)
	}
	id, err := newReportID()
	if err != nil {
		return WrongLinkReport{}, err
	}
	report := WrongLinkReport{
		ID:         id,
		Kind:       "wrong_link",
		A:          a,
		B:          b,
		Reason:     strings.TrimSpace(req.Reason),
		ReportedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:     "pending",
		Needs:      "agent-label",
		Origin:     "report-wrong-link",
	}
	data, err := json.Marshal(report)
	if err != nil {
		return WrongLinkReport{}, err
	}
	key := fmt.Sprintf("wronglink.%020d.%s", time.Now().UTC().UnixNano(), id)
	if _, err := kv.Put(ctx, key, data); err != nil {
		return WrongLinkReport{}, fmt.Errorf("store report: %w", err)
	}
	return report, nil
}

func newReportID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("report id: %w", err)
	}
	return "wrong-" + hex.EncodeToString(b[:]), nil
}

func (h *HTTP) handleWrongLinkList(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.loadReports(ctx))
}

func (h *HTTP) loadReports(ctx context.Context) WrongLinksView {
	h.mu.Lock()
	if !h.reportsAt.IsZero() && time.Since(h.reportsAt) < 5*time.Second {
		view := h.reports
		h.mu.Unlock()
		return view
	}
	h.mu.Unlock()

	view := WrongLinksView{Available: false}
	if h.Bus == nil {
		view.Error = "bus is not wired"
		return view
	}
	kv, err := h.Bus.KV(ctx, bus.KVBucketEvalPending)
	if err != nil {
		view.Error = "pending eval queue unavailable: " + err.Error()
		return view
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		view.Error = err.Error()
		return view
	}
	view.Available = true
	view.Count = len(keys)
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	limit := 20
	if len(keys) < limit {
		limit = len(keys)
	}
	view.Latest = make([]WrongLinkReport, 0, limit)
	for _, key := range keys[:limit] {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var report WrongLinkReport
		if json.Unmarshal(entry.Value(), &report) != nil {
			continue
		}
		view.Latest = append(view.Latest, report)
	}
	h.mu.Lock()
	h.reports = view
	h.reportsAt = time.Now()
	h.mu.Unlock()
	return view
}

// SourcesView summarises the real source_health records. Failing lists only
// non-ok sources and caps the list; unavailable records stay explicit.
type SourcesView struct {
	Available bool           `json:"available"`
	Error     string         `json:"error,omitempty"`
	Total     int            `json:"total"`
	ByState   map[string]int `json:"by_state"`
	Failing   []SourceRow    `json:"failing"`
	UpdatedAt string         `json:"updated_at,omitempty"`
}

// SourceRow is one non-ok source.
type SourceRow struct {
	ID               string `json:"id"`
	Kind             string `json:"kind"`
	State            string `json:"state"`
	LastError        string `json:"last_error,omitempty"`
	Failures         int    `json:"consecutive_failures"`
	LastSuccessUnix  int64  `json:"last_success_unix,omitempty"`
	NextAttemptUnix  int64  `json:"next_attempt_unix,omitempty"`
	QuarantinedUntil int64  `json:"quarantined_until_unix,omitempty"`
}

func (h *HTTP) loadSources(ctx context.Context) SourcesView {
	h.mu.Lock()
	if !h.sourcesAt.IsZero() && time.Since(h.sourcesAt) < 5*time.Second {
		view := h.sources
		h.mu.Unlock()
		return view
	}
	h.mu.Unlock()

	view := SourcesView{Available: false, ByState: map[string]int{}, Failing: []SourceRow{}}
	if h.Bus == nil {
		view.Error = "bus is not wired"
		return view
	}
	kv, err := h.Bus.KV(ctx, bus.KVBucketSourceHealth)
	if err != nil {
		view.Error = "source health unavailable: " + err.Error()
		return view
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		view.Error = err.Error()
		return view
	}
	view.Available = true
	view.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	var failing []SourceRow
	for _, key := range keys {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var rec fetch.HealthRecord
		if json.Unmarshal(entry.Value(), &rec) != nil {
			continue
		}
		view.Total++
		state := rec.State
		if state == "" {
			state = "unknown"
		}
		view.ByState[state]++
		if state == "ok" && rec.ConsecutiveFailures == 0 && rec.LastError == "" {
			continue
		}
		failing = append(failing, SourceRow{
			ID:               rec.SourceID,
			Kind:             rec.Kind,
			State:            state,
			LastError:        rec.LastError,
			Failures:         rec.ConsecutiveFailures,
			LastSuccessUnix:  rec.LastSuccess,
			NextAttemptUnix:  rec.NextAttempt,
			QuarantinedUntil: rec.QuarantinedUntil,
		})
	}
	sort.Slice(failing, func(i, j int) bool {
		if failing[i].State != failing[j].State {
			return failing[i].State == "quarantined"
		}
		return failing[i].Failures > failing[j].Failures
	})
	if len(failing) > 25 {
		failing = failing[:25]
	}
	view.Failing = failing
	h.mu.Lock()
	h.sources = view
	h.sourcesAt = time.Now()
	h.mu.Unlock()
	return view
}
