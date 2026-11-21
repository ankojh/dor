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
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/ops"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// DefaultAddr is the local gateway address from the plan/conventions.
const DefaultAddr = "127.0.0.1:8080"

// DefaultOrigins are the browser origins accepted by default. A non-browser
// client (no Origin header) is always accepted.
var DefaultOrigins = []string{"localhost:*", "127.0.0.1:*"}

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
	s := &Server{cfg: cfg, log: cfg.Logger.With("gateway", cfg.Addr), ready: make(chan struct{})}
	s.hub = newHub(cfg.Bus, s.log)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/text", s.handleText)
	mux.HandleFunc("GET /api/stories", s.handleStories)
	mux.HandleFunc("GET /api/stories/{id}", s.handleStoryDetail)
	mux.HandleFunc("GET /api/places", s.handlePlaces)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/alerts", s.handleAlertsList)
	mux.HandleFunc("POST /api/alerts", s.handleAlertsCreate)
	mux.HandleFunc("POST /api/alerts/backtest", s.handleAlertsBacktest)
	mux.HandleFunc("GET /api/alerts/hits", s.handleAlertsHits)
	mux.HandleFunc("PATCH /api/alerts/{id}", s.handleAlertsPatch)
	mux.HandleFunc("DELETE /api/alerts/{id}", s.handleAlertsDelete)
	mux.HandleFunc("POST /api/alerts/{id}/mute", s.handleAlertsMute)
	mux.HandleFunc("GET /api/push/key", s.handlePushKey)
	mux.HandleFunc("POST /api/push/subscriptions", s.handlePushSubscription)
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
	s.http = &http.Server{Handler: withCORS(mux), ReadHeaderTimeout: 5 * time.Second}
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
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	if s.cfg.Text == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "text service unavailable"})
		return
	}
	q := r.URL.Query()
	if id := strings.TrimSpace(q.Get("story_id")); id != "" {
		text, ok := s.cfg.Text.GenerateStory(ctx, id)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "story", "id": id, "available": false, "error": "unknown story"})
			return
		}
		_ = json.NewEncoder(w).Encode(text)
		return
	}
	a := strings.TrimSpace(q.Get("link_a"))
	b := strings.TrimSpace(q.Get("link_b"))
	if a != "" && b != "" {
		text, ok := s.cfg.Text.GenerateLink(ctx, a, b)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "link", "a": a, "b": b, "available": false, "error": "unknown link"})
			return
		}
		_ = json.NewEncoder(w).Encode(text)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "story_id or link_a+link_b is required"})
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
	w.Header().Set("Access-Control-Allow-Origin", "*")
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
