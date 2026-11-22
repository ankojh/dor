// Package gateway serves the realtime protocol: one binary WebSocket per
// browser, a bounded per-client output queue, resume from last_seq, and a
// snapshot fallback. It consumes delta.out from the DELTA stream; the
// publisher (internal/agents/publisher.go) is the single writer of that
// subject.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/httpguard"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/ops"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// DefaultAddr is the local gateway address from the plan/conventions.
const DefaultAddr = "127.0.0.1:8080"

// DefaultOrigins are the browser origins accepted by default. A non-browser
// client (no Origin header) is always accepted. A request whose Origin host
// equals the request Host is also accepted, so a deployment served from the
// gateway itself works without listing its own hostname.
var DefaultOrigins = []string{"localhost:*", "localhost", "127.0.0.1:*", "127.0.0.1"}

const (
	// maxGenerationConcurrency bounds paid text generation reached through
	// REST. The text service also bounds background prefetches.
	maxGenerationConcurrency = 2
	// maxTextIDLen bounds story ids and link endpoints in URL parameters.
	maxTextIDLen = 200
)

// beginGeneration reserves one paid-generation slot. It never waits: a
// request that finds both slots busy is throttled instead of piling up behind
// a slow model call.
func (s *Server) beginGeneration() bool {
	select {
	case s.generation <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) endGeneration() { <-s.generation }

// TextService is the brief/why surface the gateway serves. *textsvc.Service
// satisfies it; nil disables /api/text with a 503.
type TextService interface {
	Story(ctx context.Context, id string) (live.StoryText, bool)
	GenerateStory(ctx context.Context, id string) (live.StoryText, bool)
	Link(ctx context.Context, a, b string) (live.LinkText, bool)
	GenerateLink(ctx context.Context, a, b string) (live.LinkText, bool)
	PrefetchStory(id string)
	PrefetchLink(a, b string)
}

// PushConfig is the Web Push surface. Delivery is only advertised when both
// the VAPID key material and a sender are configured; subscriptions are
// encrypted before they reach the KV store.
type PushConfig struct {
	PublicKey     string
	EncryptionKey []byte
	Delivery      bool
}

// Available reports whether push can actually deliver, with an operator-facing
// reason when it cannot.
func (p PushConfig) Available() (bool, string) {
	if !p.Delivery {
		return false, "web push delivery is not configured (no VAPID sender)"
	}
	if strings.TrimSpace(p.PublicKey) == "" {
		return false, "VAPID public key is not configured"
	}
	switch len(p.EncryptionKey) {
	case 16, 24, 32:
		return true, ""
	default:
		return false, "push encryption key is not configured (expected 32 bytes)"
	}
}

// Config configures one gateway process.
type Config struct {
	Addr    string
	Origins []string
	Bus     *bus.Bus
	Logger  *slog.Logger
	Text    TextService
	Alerts  *alert.Store
	Matcher *matcher.Service
	Push    PushConfig
	Store   StoryStore
	Places  *geo.Gazetteer
	// Ops mounts /ops, /api/ops, the narrow runtime controls and the
	// wrong-link report queue. Nil serves only the gateway endpoints.
	Ops *ops.HTTP
}

// Server is one gateway replica.
type Server struct {
	cfg     Config
	log     *slog.Logger
	hub     *hub
	http    *http.Server
	ln      net.Listener
	ready   chan struct{}
	runCtx  context.Context
	cancel  context.CancelFunc
	cc      jetstream.ConsumeContext
	closed  sync.Once
	lastErr error
	mu      sync.Mutex

	guard      *httpguard.Allowlist
	alertLimit *httpguard.Limiter
	backtest   *httpguard.Limiter
	textLimit  *httpguard.Limiter
	pushLimit  *httpguard.Limiter
	generation chan struct{}
}

// New validates the configuration. Start performs the bind.
func New(cfg Config) (*Server, error) {
	if cfg.Bus == nil {
		return nil, errors.New("gateway: bus is required")
	}
	if strings.TrimSpace(cfg.Addr) == "" {
		cfg.Addr = DefaultAddr
	}
	if len(cfg.Origins) == 0 {
		cfg.Origins = DefaultOrigins
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{
		cfg:        cfg,
		log:        cfg.Logger.With("gateway", cfg.Addr),
		ready:      make(chan struct{}),
		guard:      httpguard.NewAllowlist(cfg.Origins),
		alertLimit: httpguard.NewLimiter(30, 10),
		backtest:   httpguard.NewLimiter(4, 2),
		textLimit:  httpguard.NewLimiter(30, 6),
		pushLimit:  httpguard.NewLimiter(10, 3),
		generation: make(chan struct{}, maxGenerationConcurrency),
	}
	s.hub = newHub(cfg.Bus, s.log)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.Handle("GET /api/text", s.textLimit.Middleware(http.HandlerFunc(s.handleText)))
	mux.HandleFunc("GET /api/stories", s.handleStories)
	mux.HandleFunc("GET /api/stories/{id}", s.handleStoryDetail)
	mux.HandleFunc("GET /api/places", s.handlePlaces)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/alerts", s.handleAlertsList)
	mux.Handle("POST /api/alerts", s.alertLimit.Middleware(http.HandlerFunc(s.handleAlertsCreate)))
	mux.Handle("POST /api/alerts/backtest", s.backtest.Middleware(http.HandlerFunc(s.handleAlertsBacktest)))
	mux.HandleFunc("GET /api/alerts/hits", s.handleAlertsHits)
	mux.Handle("PATCH /api/alerts/{id}", s.alertLimit.Middleware(http.HandlerFunc(s.handleAlertsPatch)))
	mux.Handle("DELETE /api/alerts/{id}", s.alertLimit.Middleware(http.HandlerFunc(s.handleAlertsDelete)))
	mux.Handle("POST /api/alerts/{id}/mute", s.alertLimit.Middleware(http.HandlerFunc(s.handleAlertsMute)))
	mux.HandleFunc("GET /api/push/key", s.handlePushKey)
	mux.Handle("POST /api/push/subscriptions", s.pushLimit.Middleware(http.HandlerFunc(s.handlePushSubscription)))
	if cfg.Ops != nil {
		cfg.Ops.Gateway = func() ops.GatewayInfo {
			return ops.GatewayInfo{
				Available:   true,
				Clients:     s.hub.clientCount(),
				Seq:         s.hub.latest(),
				Ring:        s.hub.ringLen(),
				SnapshotSeq: s.hub.snapshotSeq(),
			}
		}
		cfg.Ops.Mount(mux)
	}
	s.http = &http.Server{Handler: s.guard.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// Start binds the listener and starts the delta consumer. It returns once the
// consumer is established, so a successful Start means deltas will be served.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", s.cfg.Addr, err)
	}
	s.ln = ln
	runCtx, cancel := context.WithCancel(ctx)
	s.runCtx = runCtx
	s.cancel = cancel

	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.setError(err)
			s.log.Error("gateway http server stopped", "err", err)
		}
	}()
	go s.consumeDeltas(runCtx)
	select {
	case <-s.ready:
		s.mu.Lock()
		err := s.lastErr
		s.mu.Unlock()
		if err != nil {
			cancel()
			_ = ln.Close()
			return err
		}
		return nil
	case <-ctx.Done():
		cancel()
		_ = ln.Close()
		return ctx.Err()
	case <-time.After(10 * time.Second):
		cancel()
		_ = ln.Close()
		return errors.New("gateway: delta consumer did not start within 10s")
	}
}

// Addr returns the bound address (useful when Addr used port 0).
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.cfg.Addr
	}
	return s.ln.Addr().String()
}

// Stop stops accepting clients, closes them, and stops the delta consumer.
func (s *Server) Stop(ctx context.Context) error {
	var err error
	s.closed.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err = s.http.Shutdown(shutdownCtx)
		if s.cc != nil {
			s.cc.Stop()
		}
		s.hub.closeAll()
	})
	return err
}

func (s *Server) setError(err error) {
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
}

func (s *Server) consumeDeltas(ctx context.Context) {
	stream, err := s.cfg.Bus.StreamNamed(ctx, bus.StreamDelta)
	if err != nil {
		s.setError(fmt.Errorf("gateway: delta stream: %w", err))
		s.log.Error("gateway: delta stream unavailable", "err", err)
		close(s.ready)
		return
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: bus.SubjectDeltaOut,
		MaxAckPending: 8192,
	})
	if err != nil {
		s.setError(fmt.Errorf("gateway: delta consumer: %w", err))
		s.log.Error("gateway: delta consumer failed", "err", err)
		close(s.ready)
		return
	}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		var batch dorv1.DeltaBatch
		if err := proto.Unmarshal(msg.Data(), &batch); err != nil {
			s.log.Warn("gateway: undecodable delta batch", "err", err)
			return
		}
		s.hub.addBatch(&batch)
	})
	if err != nil {
		s.setError(fmt.Errorf("gateway: consume: %w", err))
		s.log.Error("gateway: consume failed", "err", err)
		close(s.ready)
		return
	}
	s.mu.Lock()
	s.cc = cc
	s.mu.Unlock()
	close(s.ready)
	s.log.Info("gateway consuming deltas", "subject", bus.SubjectDeltaOut)
	<-ctx.Done()
	cc.Stop()
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  s.cfg.Origins,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		s.log.Debug("gateway: websocket accept failed", "origin", r.Header.Get("Origin"), "err", err)
		return
	}
	conn.SetReadLimit(64 << 10)
	c := newClient(s.hub, s.cfg.Text, s.cfg.Matcher, conn, s.log)
	s.hub.addClient(c)
	c.run(r.Context())
}

// handleText serves story briefs and link explanations. Text never travels in
// deltas; this endpoint is the only text transport.
func (s *Server) handleText(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if s.cfg.Text == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "text service unavailable"})
		return
	}
	q := r.URL.Query()
	id := strings.TrimSpace(q.Get("story_id"))
	a := strings.TrimSpace(q.Get("link_a"))
	b := strings.TrimSpace(q.Get("link_b"))
	if id == "" && (a == "" || b == "") {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "story_id or link_a+link_b is required"})
		return
	}
	if len(id) > maxTextIDLen || len(a) > maxTextIDLen || len(b) > maxTextIDLen {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "story or link id is too long"})
		return
	}
	if !s.beginGeneration() {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "text generation is busy; retry shortly"})
		return
	}
	defer s.endGeneration()
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	if id != "" {
		text, ok := s.cfg.Text.GenerateStory(ctx, id)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "story", "id": id, "available": false, "error": "unknown story"})
			return
		}
		_ = json.NewEncoder(w).Encode(text)
		return
	}
	text, ok := s.cfg.Text.GenerateLink(ctx, a, b)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "link", "a": a, "b": b, "available": false, "error": "unknown link"})
		return
	}
	_ = json.NewEncoder(w).Encode(text)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"clients": s.hub.clientCount(),
		"seq":     s.hub.latest(),
	})
}

type healthSource struct {
	Source string          `json:"source"`
	Health json.RawMessage `json:"health"`
}

// handleHealth returns source health for the ops page and the in-app status
// bar. It reads the same source_health records the fetchers write.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out := map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"gateway": map[string]any{
			"clients":      s.hub.clientCount(),
			"seq":          s.hub.latest(),
			"ring":         s.hub.ringLen(),
			"snapshot_seq": s.hub.snapshotSeq(),
		},
	}
	kv, err := s.cfg.Bus.KV(r.Context(), bus.KVBucketSourceHealth)
	if err != nil {
		out["error"] = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	keys, err := kv.Keys(r.Context())
	if err != nil {
		out["error"] = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	sort.Strings(keys)
	sources := make([]healthSource, 0, len(keys))
	for _, key := range keys {
		entry, err := kv.Get(r.Context(), key)
		if err != nil {
			continue
		}
		sources = append(sources, healthSource{Source: key, Health: append(json.RawMessage(nil), entry.Value()...)})
	}
	out["count"] = len(sources)
	out["sources"] = sources
	_ = json.NewEncoder(w).Encode(out)
}
