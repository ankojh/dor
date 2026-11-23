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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/httpguard"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/maintenance"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/ops"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/session"
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
	ID      string
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

	readyOnce     sync.Once
	serving       atomic.Bool
	activated     atomic.Bool
	maintenanceKV jetstream.KeyValue
	leaseKV       jetstream.KeyValue
	gatewayID     string

	guard       *httpguard.Allowlist
	sessions    *session.Signer
	alertLimit  *httpguard.Limiter
	backtest    *httpguard.Limiter
	textLookup  *httpguard.Limiter
	textLimit   *httpguard.Limiter
	pushLimit   *httpguard.Limiter
	apiBackstop *httpguard.Limiter
	generation  chan struct{}
	alertOwners sync.Map // alert id -> owner session id
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
	signerCtx, signerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	sessions, err := session.LoadOrCreate(signerCtx, cfg.Bus)
	signerCancel()
	if err != nil {
		return nil, fmt.Errorf("gateway: session signer: %w", err)
	}
	s := &Server{
		cfg:         cfg,
		log:         cfg.Logger.With("gateway", cfg.Addr),
		ready:       make(chan struct{}),
		guard:       httpguard.NewAllowlist(cfg.Origins).WithHosts(httpguard.HostFromAddr(cfg.Addr)),
		sessions:    sessions,
		alertLimit:  httpguard.NewLimiter(30, 10),
		backtest:    httpguard.NewLimiter(4, 2),
		textLookup:  httpguard.NewLimiter(120, 20),
		textLimit:   httpguard.NewLimiter(30, 6),
		pushLimit:   httpguard.NewLimiter(10, 3),
		apiBackstop: httpguard.NewLimiter(600, 120),
		generation:  make(chan struct{}, maxGenerationConcurrency),
		gatewayID:   firstNonEmpty(cfg.ID, nuid.Next()),
	}
	s.hub = newHub(cfg.Bus, s.log)
	s.hub.alertOwner = s.resolveAlertOwner
	// Start preflight gates the real listener. The default keeps an
	// unstarted handler usable for in-process tests and embedding callers that
	// mount s.http.Handler directly.
	s.serving.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("GET /api/session", s.handleSession)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/text", s.handleText)
	mux.HandleFunc("GET /api/stories", s.handleStories)
	mux.HandleFunc("GET /api/stories/{id}", s.handleStoryDetail)
	mux.HandleFunc("GET /api/histogram", s.handleHistogram)
	mux.HandleFunc("GET /api/places", s.handlePlaces)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/alerts", s.handleAlertsList)
	mux.Handle("POST /api/alerts", s.alertLimit.MiddlewareKey(s.restKey, http.HandlerFunc(s.handleAlertsCreate)))
	mux.Handle("POST /api/alerts/backtest", s.backtest.MiddlewareKey(s.restKey, http.HandlerFunc(s.handleAlertsBacktest)))
	mux.HandleFunc("GET /api/alerts/hits", s.handleAlertsHits)
	mux.Handle("PATCH /api/alerts/{id}", s.alertLimit.MiddlewareKey(s.restKey, http.HandlerFunc(s.handleAlertsPatch)))
	mux.Handle("DELETE /api/alerts/{id}", s.alertLimit.MiddlewareKey(s.restKey, http.HandlerFunc(s.handleAlertsDelete)))
	mux.Handle("POST /api/alerts/{id}/mute", s.alertLimit.MiddlewareKey(s.restKey, http.HandlerFunc(s.handleAlertsMute)))
	mux.HandleFunc("GET /api/push/key", s.handlePushKey)
	mux.Handle("POST /api/push/subscriptions", s.pushLimit.MiddlewareKey(s.restKey, http.HandlerFunc(s.handlePushSubscription)))
	if cfg.Ops != nil {
		cfg.Ops.Key = s.restKey
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
	// Order: trusted Host/origin boundary, then the anonymous session, then
	// the coarse IP backstop, so per-route session limits and handlers can
	// both see the session id.
	handler := s.guard.Middleware(s.sessions.Middleware(s.apiBackstop.Middleware(s.servingMiddleware(mux))))
	s.http = &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// Start binds the listener and starts the delta consumer. It returns once the
// consumer is established, so a successful Start means deltas will be served.
func (s *Server) Start(ctx context.Context) error {
	kv, err := s.cfg.Bus.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return fmt.Errorf("gateway: maintenance control: %w", err)
	}
	s.maintenanceKV = kv
	leaseKV, err := s.cfg.Bus.KV(ctx, bus.KVBucketGatewayLease)
	if err != nil {
		return fmt.Errorf("gateway: gateway lease bucket: %w", err)
	}
	s.leaseKV = leaseKV
	gate, err := maintenance.ReadGate(ctx, kv)
	if err != nil {
		return fmt.Errorf("gateway: maintenance control: %w", err)
	}
	if gate.Initialized {
		s.activated.Store(true)
	}
	if !gate.Serving() {
		return fmt.Errorf("gateway: maintenance fence active (phase %s)", gate.Control.Phase)
	}
	if _, err := s.prepareBaseline(ctx, gate); err != nil {
		return err
	}
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
	go s.consumeLoop(runCtx)
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

// prepareBaseline installs the floor-aware snapshot before any client or delta
// can be served. A missing/short snapshot fails closed instead of lowering the
// floor.
func (s *Server) prepareBaseline(ctx context.Context, gate maintenance.Gate) (maintenance.Floor, error) {
	if !gate.Floor.Valid() {
		return gate.Floor, nil
	}
	_, snap, err := s.hub.fetchSnapshot(ctx)
	if err != nil {
		return gate.Floor, fmt.Errorf("gateway: maintenance snapshot: %w", err)
	}
	if snap.Seq < gate.Floor.MinimumResumeSeq {
		return gate.Floor, fmt.Errorf("gateway: snapshot seq %d is below maintenance floor %d", snap.Seq, gate.Floor.MinimumResumeSeq)
	}
	if err := s.hub.installBaseline(snap, gate.Floor); err != nil {
		return gate.Floor, err
	}
	return gate.Floor, nil
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
		// Leave a short-lived serving=false lease so a concurrent maintenance
		// exclusion has explicit proof this process stopped; the bucket TTL
		// retires it afterward.
		s.publishLease(maintenance.Control{}, false, maintenance.Floor{})
	})
	return err
}

func (s *Server) setError(err error) {
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
}

// servingMiddleware is the request-level half of the fleet fence. Health
// remains observable while every serving/API route fails closed.
func (s *Server) servingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.serving.Load() || r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "maintenance fence active"})
	})
}

// consumeLoop reconciles the durable maintenance control before serving. It
// rebuilds the baseline and DELTA consumer from raw D+1 after an advance, and
// suspends all local serving as soon as the fence closes.
func (s *Server) consumeLoop(ctx context.Context) {
	var current maintenance.Gate
	for {
		if ctx.Err() != nil {
			return
		}
		gate, err := maintenance.ReadGate(ctx, s.maintenanceKV)
		if err != nil {
			s.failClosed(fmt.Errorf("gateway: maintenance control: %w", err))
			s.wait(ctx, time.Second)
			continue
		}
		if s.activated.Load() && !gate.Initialized {
			s.failClosed(errors.New("gateway: active maintenance control disappeared"))
			s.wait(ctx, time.Second)
			continue
		}
		if gate.Initialized {
			s.activated.Store(true)
		}
		if !gate.Serving() {
			s.serving.Store(false)
			s.publishLease(gate.Control, false, gate.Floor)
			s.hub.suspend()
			s.hub.closeAll()
			s.wait(ctx, time.Second)
			continue
		}
		if _, err := s.prepareBaseline(ctx, gate); err != nil {
			s.failClosed(err)
			s.wait(ctx, time.Second)
			continue
		}
		cc, err := s.startDeltaConsumer(ctx, gate.Floor)
		if err != nil {
			s.failClosed(err)
			s.wait(ctx, time.Second)
			continue
		}
		s.serving.Store(true)
		s.mu.Lock()
		s.cc = cc
		s.lastErr = nil
		s.mu.Unlock()
		s.publishLease(gate.Control, true, gate.Floor)
		s.readyOnce.Do(func() { close(s.ready) })
		s.log.Info("gateway consuming deltas", "subject", bus.SubjectDeltaOut, "floor_t", gate.Floor.MinimumResumeSeq, "floor_d", gate.Floor.BootstrapStreamSeq)
		current = gate
		for {
			if !s.wait(ctx, time.Second) {
				if cc != nil {
					cc.Stop()
				}
				return
			}
			next, err := maintenance.ReadGate(ctx, s.maintenanceKV)
			if err != nil {
				s.publishLease(maintenance.Control{}, false, maintenance.Floor{})
			} else {
				s.publishLease(next.Control, next.Serving(), next.Floor)
			}
			if err != nil || !next.Serving() || (s.activated.Load() && !next.Initialized) {
				s.serving.Store(false)
				s.hub.suspend()
				s.hub.closeAll()
				if cc != nil {
					cc.Stop()
				}
				break
			}
			if next.Floor.MinimumResumeSeq != current.Floor.MinimumResumeSeq || next.Floor.BootstrapStreamSeq != current.Floor.BootstrapStreamSeq || next.Control.FleetEpoch != current.Control.FleetEpoch {
				s.serving.Store(false)
				s.hub.suspend()
				if cc != nil {
					cc.Stop()
				}
				break
			}
		}
	}
}

func (s *Server) wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (s *Server) failClosed(err error) {
	s.serving.Store(false)
	s.setError(err)
	s.publishLease(maintenance.Control{}, false, maintenance.Floor{})
	s.hub.suspend()
	s.hub.closeAll()
	s.readyOnce.Do(func() { close(s.ready) })
}

// publishLease advertises this process's serving state for the maintenance
// exclusion barrier. A lease write failure is deliberately non-fatal: the
// external quiesce barrier remains authoritative.
func (s *Server) publishLease(control maintenance.Control, serving bool, floor maintenance.Floor) {
	if s.leaseKV == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = maintenance.WriteGatewayLease(ctx, s.leaseKV, maintenance.GatewayLease{
		ID:            s.gatewayID,
		Environment:   s.cfg.Bus.EnvironmentID(),
		Epoch:         control.FleetEpoch,
		Serving:       serving,
		WireTombstone: true,
		FloorT:        floor.MinimumResumeSeq,
		FloorD:        floor.BootstrapStreamSeq,
	})
}

func (s *Server) deleteLease() {
	if s.leaseKV == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = maintenance.DeleteGatewayLease(ctx, s.leaseKV, s.gatewayID)
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// startDeltaConsumer starts at the raw position after the installed floor. The
// retained pre-floor DELTA prefix remains on the server but is never replayed.
func (s *Server) startDeltaConsumer(ctx context.Context, floor maintenance.Floor) (jetstream.ConsumeContext, error) {
	stream, err := s.cfg.Bus.StreamNamed(ctx, bus.StreamDelta)
	if err != nil {
		return nil, fmt.Errorf("gateway: delta stream: %w", err)
	}
	config := jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: bus.SubjectDeltaOut,
		MaxAckPending: 8192,
	}
	if floor.Valid() {
		config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		start := floor.BootstrapStreamSeq + 1
		if info, err := stream.Info(ctx); err == nil && info.State.FirstSeq > start {
			// Retention may have removed the exact raw successor while the
			// snapshot already covers it. Start at the oldest retained raw
			// message; addBatch discards deltas at or below the snapshot seq.
			start = info.State.FirstSeq
		}
		config.OptStartSeq = start
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("gateway: delta consumer: %w", err)
	}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		if !s.serving.Load() {
			return
		}
		var batch dorv1.DeltaBatch
		if err := proto.Unmarshal(msg.Data(), &batch); err != nil {
			s.log.Warn("gateway: undecodable delta batch", "err", err)
			return
		}
		s.hub.addBatch(&batch)
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: consume: %w", err)
	}
	return cc, nil
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
	// A nil *matcher.Service stored in the PromptSink interface is non-nil as
	// an interface value; convert explicitly so a gateway without a matcher
	// keeps working.
	var prompts PromptSink
	if s.cfg.Matcher != nil {
		prompts = s.cfg.Matcher
	}
	c := newClient(s.hub, s.cfg.Text, prompts, conn, s.log)
	c.sessionID = session.IDFromRequest(r)
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
	key := s.restKey(r)
	if !s.textLookup.Allow(key) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "too many text requests; retry shortly"})
		return
	}
	// Existence is a free read. A 404 for an unknown story or link must not
	// spend the paid-generation budget.
	lookupCtx, lookupCancel := context.WithTimeout(r.Context(), 5*time.Second)
	if id != "" {
		_, ok := s.cfg.Text.Story(lookupCtx, id)
		lookupCancel()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "story", "id": id, "available": false, "error": "unknown story"})
			return
		}
	} else {
		_, ok := s.cfg.Text.Link(lookupCtx, a, b)
		lookupCancel()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "link", "a": a, "b": b, "available": false, "error": "unknown link"})
			return
		}
	}
	if !s.textLimit.Allow(key) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "text generation limit reached; retry shortly"})
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

// handleSession is the bootstrap endpoint the app calls before opening the
// WebSocket. The middleware has already issued a cookie when needed; the
// response only confirms that the browser now has a session.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "session": session.IDFromRequest(r) != ""})
}

// restKey keys per-route admission by the anonymous session, falling back to
// the remote address for non-browser clients. The coarse IP backstop still
// applies separately.
func (s *Server) restKey(r *http.Request) string {
	// A request that just minted a session has no stable identity yet; charge
	// it to the address so omitting cookies does not reset every budget.
	if id := session.IDFromRequest(r); id != "" && !session.Issued(r) {
		return "session:" + id
	}
	return "ip:" + httpguard.ClientKey(r.RemoteAddr)
}

// resolveAlertOwner returns the owner session for an alert, or "" when the
// alert is unknown or has no owner. A failed lookup fails closed; alert
// deltas are private even at the cost of dropping a notification.
func (s *Server) resolveAlertOwner(alertID string) string {
	if alertID == "" || s.cfg.Alerts == nil {
		return ""
	}
	if owner, ok := s.alertOwners.Load(alertID); ok {
		return owner.(string)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	a, ok := s.cfg.Alerts.Get(ctx, alertID)
	if !ok {
		return ""
	}
	s.alertOwners.Store(alertID, a.Owner)
	return a.Owner
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
