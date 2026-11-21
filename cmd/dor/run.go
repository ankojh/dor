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
	registry, err := agents.Builtins(b, metrics, logger)
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
