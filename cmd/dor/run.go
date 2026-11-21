package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/gateway"
	"github.com/ankojh/dor/internal/record"

	"github.com/ankojh/dor/internal/store"
)

const version = "0.1.0-dev"

type config struct {
	agents          string
	natsURL         string
	natsListen      string
	natsStore       string
	adminAddr       string
	logLevel        string
	maxAckPending   int
	provision       bool
	shutdownTimeout time.Duration

	sourcesFile       string
	recordDir         string
	httpUserAgent     string
	rssShards         int
	rssShard          int
	rssConcurrency    int
	gdeltFile         string
	gdeltMaxItems     int
	gdeltExtraDomains string
	reliefWebAppname  string

	dbURL      string
	dbMaxConns int

	gatewayAddr    string
	gatewayOrigins string
}

func runCommand(args []string) error {
	cfg := config{
		agents:          envOr("DOR_AGENTS", "all"),
		natsURL:         envOr("DOR_NATS_URL", ""),
		natsListen:      envOr("DOR_NATS_LISTEN", bus.DefaultNATSListen),
		natsStore:       envOr("DOR_NATS_STORE", bus.DefaultStoreDir),
		adminAddr:       envOr("DOR_ADMIN_ADDR", "127.0.0.1:8091"),
		logLevel:        envOr("DOR_LOG_LEVEL", "info"),
		maxAckPending:   envIntOr("DOR_MAX_ACK_PENDING", bus.DefaultMaxAckPending),
		provision:       envBoolOr("DOR_PROVISION", false),
		shutdownTimeout: envDurationOr("DOR_SHUTDOWN_TIMEOUT", 45*time.Second),

		sourcesFile:       envOr("DOR_SOURCES_FILE", "deploy/sources/sources.yaml"),
		recordDir:         envOr("DOR_RECORD_DIR", ""),
		httpUserAgent:     envOr("DOR_HTTP_USER_AGENT", ""),
		rssShards:         envIntOr("DOR_RSS_SHARDS", 3),
		rssShard:          envIntOr("DOR_RSS_SHARD", -1),
		rssConcurrency:    envIntOr("DOR_RSS_CONCURRENCY", 8),
		gdeltFile:         envOr("DOR_GDELT_FILE", "gkg"),
		gdeltMaxItems:     envIntOr("DOR_GDELT_MAX_ITEMS", 300),
		gdeltExtraDomains: envOr("DOR_GDELT_EXTRA_DOMAINS", ""),
		reliefWebAppname:  envOr("DOR_RELIEFWEB_APPNAME", ""),

		dbURL:      envOr("DOR_DATABASE_URL", "postgres://dor:dor@127.0.0.1:5433/dor?sslmode=disable"),
		dbMaxConns: envIntOr("DOR_DB_MAX_CONNECTIONS", 10),

		gatewayAddr:    envOr("DOR_GATEWAY_ADDR", gateway.DefaultAddr),
		gatewayOrigins: envOr("DOR_GATEWAY_ORIGINS", "localhost:*,127.0.0.1:*"),
	}

	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.agents, "agents", cfg.agents, "agents to run: all, none, or a comma-separated list")
	fs.StringVar(&cfg.natsURL, "nats-url", cfg.natsURL, "existing NATS URL; empty starts the embedded server")
	fs.StringVar(&cfg.natsListen, "nats-listen", cfg.natsListen, "embedded NATS listen address")
	fs.StringVar(&cfg.natsStore, "nats-store", cfg.natsStore, "embedded JetStream store directory")
	fs.StringVar(&cfg.adminAddr, "admin-addr", cfg.adminAddr, "admin HTTP address for /healthz and /metrics; empty disables it")
	fs.StringVar(&cfg.logLevel, "log-level", cfg.logLevel, "log level: debug, info, warn, error")
	fs.IntVar(&cfg.maxAckPending, "max-ack-pending", cfg.maxAckPending, "default consumer MaxAckPending")
	fs.BoolVar(&cfg.provision, "provision", cfg.provision, "provision streams/KV on an external bus; embedded mode always provisions")
	fs.DurationVar(&cfg.shutdownTimeout, "shutdown-timeout", cfg.shutdownTimeout, "total shutdown budget")
	fs.StringVar(&cfg.sourcesFile, "sources-file", cfg.sourcesFile, "reviewed source seed list")
	fs.StringVar(&cfg.recordDir, "record-dir", cfg.recordDir, "record the raw stream to <dir> (empty disables record mode)")
	fs.StringVar(&cfg.httpUserAgent, "http-user-agent", cfg.httpUserAgent, "crawler User-Agent for source fetches")
	fs.IntVar(&cfg.rssShards, "rss-shards", cfg.rssShards, "number of RSS feed shards (plan: 3)")
	fs.IntVar(&cfg.rssShard, "rss-shard", cfg.rssShard, "run only this RSS shard (0-based); -1 runs every shard in this process")
	fs.IntVar(&cfg.rssConcurrency, "rss-concurrency", cfg.rssConcurrency, "concurrent fetches per RSS shard")
	fs.StringVar(&cfg.gdeltFile, "gdelt-file", cfg.gdeltFile, "GDELT file family: gkg or export")
	fs.IntVar(&cfg.gdeltMaxItems, "gdelt-max-items", cfg.gdeltMaxItems, "cap items published per GDELT file")
	fs.StringVar(&cfg.gdeltExtraDomains, "gdelt-extra-domains", cfg.gdeltExtraDomains, "comma-separated extra domains for the GDELT allowlist")
	fs.StringVar(&cfg.reliefWebAppname, "reliefweb-appname", cfg.reliefWebAppname, "approved ReliefWeb appname (empty keeps the adapter disabled)")

	fs.StringVar(&cfg.dbURL, "db-url", cfg.dbURL, "Postgres URL; empty or off disables durable writes")
	fs.IntVar(&cfg.dbMaxConns, "db-max-conns", cfg.dbMaxConns, "Postgres pool size")

	fs.StringVar(&cfg.gatewayAddr, "gateway-addr", cfg.gatewayAddr, "WebSocket gateway listen address; empty disables it")
	fs.StringVar(&cfg.gatewayOrigins, "gateway-origins", cfg.gatewayOrigins, "comma-separated WebSocket Origin patterns")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	logger, err := newLogger(cfg.logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := agents.SetupTracing(ctx, "dor")
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	connectCtx, cancelConnect := context.WithTimeout(ctx, 30*time.Second)
	b, err := bus.Connect(connectCtx, bus.Options{
		URL:           cfg.natsURL,
		ListenAddr:    cfg.natsListen,
		StoreDir:      cfg.natsStore,
		MaxAckPending: cfg.maxAckPending,
		Provision:     cfg.provision,
		Logger:        logger,
	})
	cancelConnect()
	if err != nil {
		return err
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Warn("bus close", "err", err)
		}
	}()

	metrics := agents.NewMetrics()

	fetchMetrics := fetch.NewMetrics(metrics.Registry())

	var recorder *record.Writer
	if cfg.recordDir != "" {
		recorder, err = record.NewWriter(record.Options{Dir: cfg.recordDir, Metrics: fetchMetrics})
		if err != nil {
			return err
		}
		defer func() {
			if err := recorder.Close(); err != nil {
				logger.Warn("record close", "err", err)
			}
			logger.Info("record mode stopped", "dir", cfg.recordDir,
				"written", recorder.Written(), "dropped", recorder.Dropped())
		}()
	}

	fetchConfig := agents.Config{
		SourcesFile:       cfg.sourcesFile,
		Recorder:          recorder,
		UserAgent:         cfg.httpUserAgent,
		RSSShards:         cfg.rssShards,
		RSSShard:          cfg.rssShard,
		RSSConcurrency:    cfg.rssConcurrency,
		GDELTFile:         cfg.gdeltFile,
		GDELTMaxItems:     cfg.gdeltMaxItems,
		GDELTExtraDomains: splitList(cfg.gdeltExtraDomains),
		ReliefWebAppname:  cfg.reliefWebAppname,
		FetchMetrics:      fetchMetrics,
	}

	var dbStore *store.Store
	var dbWriter *store.Writer
	var articleSink agents.ArticleSink
	if cfg.dbURL != "" && !strings.EqualFold(cfg.dbURL, "off") {
		openCtx, openCancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := store.Open(openCtx, store.Config{URL: cfg.dbURL, MaxConns: int32(cfg.dbMaxConns)})
		openCancel()
		if err != nil {
			logger.Warn("postgres unavailable; durable writes disabled, pipeline continues", "err", err)
		} else {
			migrateCtx, migrateCancel := context.WithTimeout(ctx, 20*time.Second)
			migrateErr := st.Migrate(migrateCtx)
			migrateCancel()
			if migrateErr != nil {
				logger.Warn("postgres migrations failed; durable writes disabled, pipeline continues", "err", migrateErr)
				st.Close()
			} else {
				w, werr := store.NewWriter(st, store.WriterOptions{Logger: logger})
				if werr != nil {
					logger.Warn("postgres writer unavailable; durable writes disabled", "err", werr)
					st.Close()
				} else {
					w.Start()
					dbStore = st
					dbWriter = w
					articleSink = w
					logger.Info("postgres store ready")
				}
			}
		}
	}
	if dbWriter != nil {
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := dbWriter.Close(closeCtx); err != nil {
				logger.Warn("store writer close", "err", err)
			}
			dbStore.Close()
		}()
	}

	registry, err := agents.BuiltinsWithDeps(ctx, b, metrics, logger, fetchConfig, agents.Deps{ArticleSink: articleSink})
	if err != nil {
		return err
	}
	selected, err := registry.Select(cfg.agents)
	if err != nil {
		return err
	}
	runner := agents.NewRunner(selected, logger)
	if err := runner.Start(ctx); err != nil {
		return err
	}

	var gw *gateway.Server
	if cfg.gatewayAddr != "" && runsPublisher(cfg.agents) {
		gw, err = gateway.New(gateway.Config{
			Addr:    cfg.gatewayAddr,
			Origins: splitList(cfg.gatewayOrigins),
			Bus:     b,
			Logger:  logger,
		})
		if err != nil {
			return err
		}
		if err := gw.Start(ctx); err != nil {
			return err
		}
		logger.Info("gateway listening", "addr", gw.Addr(), "origins", cfg.gatewayOrigins)
	}

	var admin *http.Server
	adminErr := make(chan error, 1)
	if cfg.adminAddr != "" {
		admin = &http.Server{
			Addr:              cfg.adminAddr,
			Handler:           adminHandler(b, runner, metrics),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				adminErr <- err
			}
		}()
	}

	mode := "embedded"
	if !b.Embedded() {
		mode = "external"
	}
	logger.Info("dor running",
		"version", version,
		"mode", mode,
		"nats_url", b.RedactedURL(),
		"agents", runner.Names(),
		"admin_addr", cfg.adminAddr,
		"store", articleSink != nil,
	)

	exitReason := "signal"
	select {
	case <-ctx.Done():
	case err := <-adminErr:
		logger.Error("admin server failed", "err", err)
		exitReason = "admin error"
	}

	stop()
	logger.Info("shutting down", "reason", exitReason)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()

	if admin != nil {
		adminCtx, adminCancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		if err := admin.Shutdown(adminCtx); err != nil {
			logger.Warn("admin shutdown", "err", err)
		}
		adminCancel()
	}
	if gw != nil {
		gwCtx, gwCancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		if err := gw.Stop(gwCtx); err != nil {
			logger.Warn("gateway shutdown", "err", err)
		}
		gwCancel()
	}
	if err := runner.Shutdown(shutdownCtx); err != nil {
		logger.Warn("agent shutdown", "err", err)
	}
	if exitReason == "admin error" {
		return errors.New("admin server failed")
	}
	logger.Info("stopped")
	return nil
}

func adminHandler(b *bus.Bus, runner *agents.Runner, metrics *agents.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		busOK := b.Connected()
		agentsOK, statuses := runner.Healthy()
		ok := busOK && agentsOK
		status := "ok"
		if !ok {
			status = "unhealthy"
		}
		busStatus := "connected"
		if !busOK {
			busStatus = "disconnected"
		}
		body := map[string]any{
			"status":   status,
			"bus":      busStatus,
			"nats_url": b.RedactedURL(),
			"agents":   statuses,
			"terminal": runner.TerminalCounts(),
			"time":     time.Now().UTC().Format(time.RFC3339),
		}
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(body)
	})
	return mux
}

func newLogger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		lvl = slog.LevelInfo
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown log level %q", level)
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})), nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envBoolOr(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	return strings.EqualFold(v, "1") || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}

func envIntOr(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDurationOr(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// runsPublisher reports whether this process should serve the gateway. The
// gateway is a component, not an agent; it runs when the publisher is selected.
func runsPublisher(selection string) bool {
	selection = strings.TrimSpace(selection)
	if selection == "all" {
		return true
	}
	for _, name := range strings.Split(selection, ",") {
		if strings.TrimSpace(name) == "publisher" {
			return true
		}
	}
	return false
}

// splitList parses a comma-separated flag value, dropping empty entries.
func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
