package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/gateway"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/httpguard"
	"github.com/ankojh/dor/internal/maintenance"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/ops"
	"github.com/ankojh/dor/internal/push"
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
	gatewayID      string
	gatewayOrigins string

	supervisor           bool
	supervisorInterval   time.Duration
	supervisorScaleLag   time.Duration
	supervisorAlarmLag   time.Duration
	supervisorAlarmFor   time.Duration
	supervisorCooldown   time.Duration
	supervisorBacklog    time.Duration
	supervisorRecovery   time.Duration
	supervisorMaxConc    int
	supervisorRestartMin time.Duration
	supervisorRestartMax time.Duration

	modelsConfig     string
	thresholdsConfig string
	budgetConfig     string
	geonamesDir      string
	photonURL        string
	teiURL           string
	relationShards   int
	relationShard    int
	relationDebounce time.Duration
	editorDebounce   time.Duration

	ntfyURL         string
	vapidPublicKey  string
	vapidPrivateKey string
	vapidSubject    string
	pushEncryptKey  string
	alertRate       time.Duration
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
		gatewayID:      envOr("DOR_GATEWAY_ID", ""),
		gatewayOrigins: envOr("DOR_GATEWAY_ORIGINS", "localhost:*,127.0.0.1:*"),

		supervisor:           envBoolOr("DOR_SUPERVISOR", true),
		supervisorInterval:   envDurationOr("DOR_SUPERVISOR_INTERVAL", 2*time.Second),
		supervisorScaleLag:   envDurationOr("DOR_SUPERVISOR_SCALE_LAG", 2*time.Second),
		supervisorAlarmLag:   envDurationOr("DOR_SUPERVISOR_ALARM_LAG", 5*time.Second),
		supervisorAlarmFor:   envDurationOr("DOR_SUPERVISOR_ALARM_FOR", time.Minute),
		supervisorCooldown:   envDurationOr("DOR_SUPERVISOR_SCALE_COOLDOWN", 30*time.Second),
		supervisorBacklog:    envDurationOr("DOR_SUPERVISOR_GDELT_BACKLOG", 20*time.Second),
		supervisorRecovery:   envDurationOr("DOR_SUPERVISOR_GDELT_RECOVERY", time.Minute),
		supervisorMaxConc:    envIntOr("DOR_SUPERVISOR_MAX_CONCURRENCY", 32),
		supervisorRestartMin: envDurationOr("DOR_SUPERVISOR_RESTART_MIN", 5*time.Second),
		supervisorRestartMax: envDurationOr("DOR_SUPERVISOR_RESTART_MAX", 5*time.Minute),

		modelsConfig:     envOr("DOR_MODELS_CONFIG", "config/models.yaml"),
		thresholdsConfig: envOr("DOR_THRESHOLDS_CONFIG", "config/thresholds.yaml"),
		budgetConfig:     envOr("DOR_BUDGET_CONFIG", "config/budget.yaml"),
		geonamesDir:      envOr("DOR_GEONAMES_DIR", geo.DefaultDir),
		photonURL:        envOr("DOR_PHOTON_URL", ""),
		teiURL:           envOr("DOR_TEI_URL", "http://127.0.0.1:8090"),
		relationShards:   envIntOr("DOR_RELATION_SHARDS", 3),
		relationShard:    envIntOr("DOR_RELATION_SHARD", -1),
		relationDebounce: envDurationOr("DOR_RELATION_DEBOUNCE", 3*time.Second),
		editorDebounce:   envDurationOr("DOR_EDITOR_DEBOUNCE", 5*time.Second),

		ntfyURL:         envOr("DOR_NTFY_URL", ""),
		vapidPublicKey:  envOr("DOR_VAPID_PUBLIC_KEY", ""),
		vapidPrivateKey: envOr("DOR_VAPID_PRIVATE_KEY", ""),
		vapidSubject:    envOr("DOR_VAPID_SUBJECT", ""),
		pushEncryptKey:  envOr("DOR_PUSH_ENCRYPTION_KEY", ""),
		alertRate:       envDurationOr("DOR_ALERT_RATE", time.Minute),
	}

	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.agents, "agents", cfg.agents, "agents to run: all, none, gateway, or a comma-separated list")
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
	fs.BoolVar(&cfg.supervisor, "supervisor", cfg.supervisor, "run the in-process Supervisor and /ops surface")
	fs.DurationVar(&cfg.supervisorInterval, "supervisor-interval", cfg.supervisorInterval, "Supervisor sampling interval")
	fs.DurationVar(&cfg.supervisorScaleLag, "supervisor-scale-lag", cfg.supervisorScaleLag, "scale an unordered consumer above this queue lag")
	fs.DurationVar(&cfg.supervisorAlarmLag, "supervisor-alarm-lag", cfg.supervisorAlarmLag, "queue-lag alarm threshold")
	fs.DurationVar(&cfg.supervisorAlarmFor, "supervisor-alarm-for", cfg.supervisorAlarmFor, "how long the alarm threshold must persist")
	fs.DurationVar(&cfg.supervisorCooldown, "supervisor-scale-cooldown", cfg.supervisorCooldown, "minimum time between scale actions per agent")
	fs.DurationVar(&cfg.supervisorBacklog, "supervisor-gdelt-backlog", cfg.supervisorBacklog, "pipeline backlog duration before GDELT is reduced or paused")
	fs.DurationVar(&cfg.supervisorRecovery, "supervisor-gdelt-recovery", cfg.supervisorRecovery, "quiet duration before GDELT returns to normal")
	fs.IntVar(&cfg.supervisorMaxConc, "supervisor-max-concurrency", cfg.supervisorMaxConc, "cap for any runtime concurrency scale action")
	fs.DurationVar(&cfg.supervisorRestartMin, "supervisor-restart-min", cfg.supervisorRestartMin, "first failed-agent restart backoff")
	fs.DurationVar(&cfg.supervisorRestartMax, "supervisor-restart-max", cfg.supervisorRestartMax, "maximum failed-agent restart backoff")
	fs.StringVar(&cfg.modelsConfig, "models-config", cfg.modelsConfig, "model pins and price config")
	fs.StringVar(&cfg.thresholdsConfig, "thresholds-config", cfg.thresholdsConfig, "decision thresholds")
	fs.StringVar(&cfg.budgetConfig, "budget-config", cfg.budgetConfig, "daily/per-API budget limits")
	fs.StringVar(&cfg.geonamesDir, "geonames-dir", cfg.geonamesDir, "GeoNames cache directory")
	fs.StringVar(&cfg.photonURL, "photon-url", cfg.photonURL, "Photon base URL; empty disables precise geocoding")
	fs.StringVar(&cfg.teiURL, "tei-url", cfg.teiURL, "TEI embedding base URL; empty disables remote embeddings")
	fs.IntVar(&cfg.relationShards, "relation-shards", cfg.relationShards, "number of relation-judge shards")
	fs.IntVar(&cfg.relationShard, "relation-shard", cfg.relationShard, "run only this relation shard (0-based); -1 runs all")
	fs.DurationVar(&cfg.relationDebounce, "relation-debounce", cfg.relationDebounce, "per-story relation debounce")
	fs.DurationVar(&cfg.editorDebounce, "editor-debounce", cfg.editorDebounce, "per-story editor debounce")
	fs.StringVar(&cfg.ntfyURL, "ntfy-url", cfg.ntfyURL, "ntfy base URL (empty disables external notifications)")
	fs.StringVar(&cfg.vapidPublicKey, "vapid-public-key", cfg.vapidPublicKey, "Web Push VAPID public key (empty disables push)")
	fs.StringVar(&cfg.vapidPrivateKey, "vapid-private-key", cfg.vapidPrivateKey, "Web Push VAPID private key (server only; never exposed)")
	fs.StringVar(&cfg.vapidSubject, "vapid-subject", cfg.vapidSubject, "VAPID subject: ops@example.com or an https URL")
	fs.StringVar(&cfg.pushEncryptKey, "push-encryption-key", cfg.pushEncryptKey, "base64 32-byte key encrypting push subscriptions at rest")
	fs.DurationVar(&cfg.alertRate, "alert-rate", cfg.alertRate, "minimum interval between external alerts per alert")
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
					// Do not start accepting writes while a durable
					// maintenance fence is already active.
					if gateKV, gerr := b.KV(ctx, bus.KVBucketPublisher); gerr == nil {
						if control, _, cerr := maintenance.ReadControl(ctx, gateKV); cerr == nil && control.Blocked() {
							w.SetMaintenanceBlocked(true)
						}
					}
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

	// The runtime participant must exist before agents start. It pauses raw
	// producers and blocks new writer admissions as soon as the durable
	// maintenance fence closes, and advertises the exact drain state.
	runtimeID := envOr("DOR_RUNTIME_ID", "")
	if runtimeID == "" {
		host, _ := os.Hostname()
		runtimeID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	maintenanceRuntime, err := maintenance.NewRuntime(ctx, b, runtimeID, logger)
	if err != nil {
		return fmt.Errorf("maintenance runtime: %w", err)
	}
	if dbWriter != nil {
		maintenanceRuntime.SetWriter(func() maintenance.WriterCounters {
			barrier := dbWriter.MaintenanceBarrier()
			return maintenance.WriterCounters{
				Verified:       true,
				Enqueued:       dbWriter.Stats().Enqueued,
				Pending:        barrier.Queue,
				PendingStories: barrier.StoryQueue,
				AuxQueue:       barrier.AuxQueue,
				Unsettled:      barrier.Unsettled,
				PendingEnrich:  barrier.PendingEnrichments,
				PendingAssign:  barrier.PendingAssignments,
				Flushing:       barrier.Flushing,
				Blocked:        barrier.Blocked,
				DroppedQueue:   barrier.DroppedQueue,
				DroppedBatch:   barrier.DroppedBatch,
				DroppedStories: barrier.DroppedStories,
				Errors:         barrier.Errors,
			}
		}, dbWriter.SetMaintenanceBlocked)
	}
	go maintenanceRuntime.Start(ctx)
	logger.Info("maintenance runtime ready", "runtime_id", runtimeID)

	// dbWriter is a *store.Writer: assigning it directly to an interface when
	// it is nil would produce a non-nil interface holding a nil pointer, and
	// the first completed story would panic on EnqueueStory. Keep the sink nil
	// unless durable writes are actually available.
	var storySink agents.StorySink
	if dbWriter != nil {
		storySink = dbWriter
	}
	enrichmentDeps, budgetMgr, budgetCfg := buildEnrichmentDeps(ctx, cfg, b, logger, articleSink, storySink)
	if dbStore != nil {
		enrichmentDeps.StoryIndex = dbStore
	}
	if dbWriter != nil {
		enrichmentDeps.LinkSink = dbWriter
	}
	enrichmentDeps.RelationShards = cfg.relationShards
	enrichmentDeps.RelationShard = cfg.relationShard
	enrichmentDeps.RelationDebounce = cfg.relationDebounce
	enrichmentDeps.EditorDebounce = cfg.editorDebounce
	if dbStore != nil {
		seedSources(ctx, dbStore, cfg.sourcesFile, logger)
	}
	textSvc := enrichmentDeps.Text
	if textSvc != nil && dbWriter != nil {
		textSvc.WithSink(dbWriter)
	}

	var alertStore *alert.Store
	if store, err := alert.New(ctx, b, dbStore, logger); err != nil {
		logger.Warn("alert store unavailable; alert API and notifier disabled", "err", err)
	} else {
		alertStore = store
	}
	embedder := embed.New(cfg.teiURL, 20*time.Second)
	if !embedder.Enabled() {
		logger.Info("TEI embedding disabled: DOR_TEI_URL is not set; prompt filters use lexical prefilter")
	}
	matcherSvc := matcher.New(b, logger, enrichmentDeps.Decider, embedder, alertStore, enrichmentDeps.Thresholds)
	if alertStore == nil {
		logger.Warn("saved alerts unavailable; prompt filters still match for connected clients")
	}

	pushEncryptionKey := decodeBase64Key(cfg.pushEncryptKey)
	var pushSender agents.PushSender
	if alertStore != nil {
		var reason string
		switch {
		case cfg.vapidPublicKey == "":
			reason = "DOR_VAPID_PUBLIC_KEY is not set"
		case cfg.vapidPrivateKey == "":
			reason = "DOR_VAPID_PRIVATE_KEY is not set"
		case cfg.vapidSubject == "":
			reason = "DOR_VAPID_SUBJECT is not set"
		case len(pushEncryptionKey) != 32:
			reason = "DOR_PUSH_ENCRYPTION_KEY must decode to 32 bytes"
		}
		if reason == "" {
			sender, err := push.NewSender(push.Config{
				Alerts:        alertStore,
				EncryptionKey: pushEncryptionKey,
				VAPIDPublic:   cfg.vapidPublicKey,
				VAPIDPrivate:  cfg.vapidPrivateKey,
				Subject:       cfg.vapidSubject,
				Logger:        logger,
			})
			if err != nil {
				reason = err.Error()
			} else {
				pushSender = sender
				logger.Info("web push sender ready")
			}
		}
		if pushSender == nil {
			logger.Info("web push unavailable", "reason", reason)
		}
	}

	enrichmentDeps.Push = pushSender
	enrichmentDeps.Alerts = alertStore
	enrichmentDeps.Matcher = matcherSvc
	enrichmentDeps.NtfyBaseURL = cfg.ntfyURL
	enrichmentDeps.AlertRate = cfg.alertRate
	registry, err := agents.BuiltinsWithDeps(ctx, b, metrics, logger, fetchConfig, enrichmentDeps)
	if err != nil {
		return err
	}
	selected, err := registry.Select(pipelineSelection(cfg.agents))
	if err != nil {
		return err
	}
	runner := agents.NewRunner(selected, logger)
	if err := runner.Start(ctx); err != nil {
		return err
	}

	var supervisor *ops.Supervisor
	if cfg.supervisor && len(selected) > 0 {
		supervisor = ops.New(ops.Config{
			Interval:          cfg.supervisorInterval,
			ScaleLag:          cfg.supervisorScaleLag,
			AlarmLag:          cfg.supervisorAlarmLag,
			AlarmFor:          cfg.supervisorAlarmFor,
			ScaleCooldown:     cfg.supervisorCooldown,
			BacklogFor:        cfg.supervisorBacklog,
			RecoveryFor:       cfg.supervisorRecovery,
			MaxConcurrency:    cfg.supervisorMaxConc,
			RestartBackoffMin: cfg.supervisorRestartMin,
			RestartBackoffMax: cfg.supervisorRestartMax,
		}, ops.Deps{
			Runner:    runner,
			Metrics:   metrics,
			Budget:    budgetMgr,
			BudgetCfg: budgetCfg,
			Logger:    logger,
		})
		supervisor.Start(ctx)
	}
	opsHTTP := ops.NewHTTP(b, supervisor, logger)

	var gw *gateway.Server
	if cfg.gatewayAddr != "" && runsGateway(cfg.agents) {
		gwCfg := gateway.Config{
			Addr:    cfg.gatewayAddr,
			ID:      cfg.gatewayID,
			Origins: splitList(cfg.gatewayOrigins),
			Bus:     b,
			Logger:  logger,
			Text:    textSvc,
			Alerts:  alertStore,
			Matcher: matcherSvc,
			Push: gateway.PushConfig{
				PublicKey:     strings.TrimSpace(cfg.vapidPublicKey),
				EncryptionKey: pushEncryptionKey,
				Delivery:      pushSender != nil,
			},
		}
		if dbStore != nil {
			gwCfg.Store = dbStore
		}
		gwCfg.Places = enrichmentDeps.Gazetteer
		gwCfg.Ops = opsHTTP
		gw, err = gateway.New(gwCfg)
		if err != nil {
			return err
		}
		if err := gw.Start(ctx); err != nil {
			return err
		}
		logger.Info("gateway listening", "addr", gw.Addr(), "gateway_id", cfg.gatewayID, "origins", cfg.gatewayOrigins)
	}

	var admin *http.Server
	adminErr := make(chan error, 1)
	if cfg.adminAddr != "" {
		originPatterns := splitList(cfg.gatewayOrigins)
		if len(originPatterns) == 0 {
			originPatterns = gateway.DefaultOrigins
		}
		origins := httpguard.NewAllowlist(originPatterns).WithHosts(httpguard.HostFromAddr(cfg.adminAddr))
		admin = &http.Server{
			Addr:              cfg.adminAddr,
			Handler:           origins.Middleware(adminHandler(b, runner, metrics, maintenanceRuntime, opsHTTP)),
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
		"ops_url", opsURL(cfg, gw),
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
	if supervisor != nil {
		supervisor.Stop()
	}
	if err := runner.Shutdown(shutdownCtx); err != nil {
		logger.Warn("agent shutdown", "err", err)
	}
	if closer, ok := enrichmentDeps.Enricher.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			logger.Warn("enricher close", "err", err)
		}
	}
	if exitReason == "admin error" {
		return errors.New("admin server failed")
	}
	logger.Info("stopped")
	return nil
}

func adminHandler(b *bus.Bus, runner *agents.Runner, metrics *agents.Metrics, runtime *maintenance.Runtime, opsHTTP ...*ops.HTTP) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	if len(opsHTTP) > 0 && opsHTTP[0] != nil {
		opsHTTP[0].Mount(mux)
	}
	mux.HandleFunc("GET /internal/maintenance/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if runtime == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"available": false, "error": "maintenance runtime unavailable"})
			return
		}
		_ = json.NewEncoder(w).Encode(runtime.State())
	})
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

// gateway is a process component, so strip it before registry resolution.
func pipelineSelection(selection string) string {
	var names []string
	for _, name := range strings.Split(selection, ",") {
		if name = strings.TrimSpace(name); name != "gateway" {
			names = append(names, name)
		}
	}
	return strings.Join(names, ",")
}

func runsPublisher(selection string) bool {
	for _, name := range strings.Split(selection, ",") {
		if name = strings.TrimSpace(name); name == "all" || name == "publisher" {
			return true
		}
	}
	return false
}

func runsGateway(selection string) bool {
	if runsPublisher(selection) {
		return true
	}
	for _, name := range strings.Split(selection, ",") {
		if strings.TrimSpace(name) == "gateway" {
			return true
		}
	}
	return false
}

// opsURL returns the reachable /ops page for this process, preferring the
// gateway (the plan's 8080 surface) over the admin listener.
func opsURL(cfg config, gw *gateway.Server) string {
	if gw != nil {
		return "http://" + gw.Addr() + "/ops"
	}
	if cfg.adminAddr != "" {
		return "http://" + cfg.adminAddr + "/ops"
	}
	return ""
}

// splitList parses a comma-separated flag value, dropping empty entries.
func decodeBase64Key(raw string) []byte {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	} {
		if b, err := encoding.DecodeString(raw); err == nil {
			return b
		}
	}
	return nil
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
