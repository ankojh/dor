package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	dorconfig "github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/sources"
	"github.com/ankojh/dor/internal/store"
	"github.com/ankojh/dor/internal/textsvc"
)

// buildEnrichmentDeps wires the real model clients when keys exist and leaves
// them nil otherwise. The agents always run; nil is an explicit, logged
// fallback, never a fake model answer. It also returns the one budget manager
// this process uses so the Supervisor reads the same daily counter.
func buildEnrichmentDeps(ctx context.Context, cfg config, b *bus.Bus, log *slog.Logger, articleSink agents.ArticleSink, storySink agents.StorySink) (agents.Deps, *budget.Manager, dorconfig.Budget) {
	deps := agents.Deps{
		ArticleSink: articleSink,
		StorySink:   storySink,
	}

	models := dorconfig.DefaultModels()
	if cfg.modelsConfig != "" {
		if m, err := dorconfig.LoadModels(cfg.modelsConfig); err != nil {
			log.Warn("models config unavailable; using pinned defaults", "path", cfg.modelsConfig, "err", err)
		} else {
			models = m
		}
	}
	thresholds := dorconfig.DefaultThresholds()
	if cfg.thresholdsConfig != "" {
		if t, err := dorconfig.LoadThresholds(cfg.thresholdsConfig); err != nil {
			log.Warn("thresholds config unavailable; using defaults", "path", cfg.thresholdsConfig, "err", err)
		} else {
			thresholds = t
		}
	}
	deps.Thresholds = thresholds

	budgetCfg := dorconfig.DefaultBudget()
	if cfg.budgetConfig != "" {
		if bcfg, err := dorconfig.LoadBudget(cfg.budgetConfig); err != nil {
			log.Warn("budget config unavailable; using defaults", "path", cfg.budgetConfig, "err", err)
		} else {
			budgetCfg = bcfg
		}
	}
	manager := newBudgetManager(ctx, b, budgetCfg, log)

	if client, model, err := newJevClient(models, manager); err != nil {
		log.Warn("jev client unavailable", "err", err)
	} else if client != nil {
		deps.Decider = client
		log.Info("jev client enabled", "model", model)
	}
	var completer textsvc.Completer
	if enricher := newEnricher(models, manager, log); enricher != nil {
		deps.Enricher = enricher
		completer = enricher
	}
	deps.Text = textsvc.New(b, log, completer)

	if wantsAgent(cfg.agents, agents.PlotDurable) {
		loadCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		g, err := geo.Load(loadCtx, cfg.geonamesDir, log)
		cancel()
		if err != nil {
			log.Warn("gazetteer unavailable; plotter will use hints/dateline fallback", "dir", cfg.geonamesDir, "err", err)
		} else {
			deps.Gazetteer = g
		}
	}
	deps.Photon = geo.NewPhoton(cfg.photonURL)
	if deps.Photon != nil {
		log.Info("photon geocoder enabled", "url", cfg.photonURL)
	}
	deps.Embedding = agents.NewTEIClient(cfg.teiURL, 5*time.Second)
	if deps.Embedding != nil {
		log.Info("TEI embedder enabled", "url", cfg.teiURL)
	}
	return deps, manager, budgetCfg
}

func newBudgetManager(ctx context.Context, b *bus.Bus, cfg dorconfig.Budget, log *slog.Logger) *budget.Manager {
	getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tokens, terr := b.KV(getCtx, bus.KVBucketBudgets)
	spend, serr := b.KV(getCtx, bus.KVBucketSpend)
	if terr != nil || serr != nil {
		log.Warn("budget KV unavailable; using process-local budget", "tokens_err", terr, "spend_err", serr)
		return budget.NewMemory(cfg)
	}
	return budget.New(budget.NewKVStore(tokens), budget.NewKVStore(spend), cfg)
}

func seedSources(ctx context.Context, st *store.Store, path string, log *slog.Logger) {
	if st == nil || strings.TrimSpace(path) == "" {
		return
	}
	file, err := sources.Load(path)
	if err != nil {
		log.Warn("source seed skipped", "path", path, "err", err)
		return
	}
	records := make([]store.SourceRecord, 0, len(file.Sources))
	for _, s := range file.Sources {
		records = append(records, store.SourceRecord{
			ID: s.ID, Name: displaySourceName(s.ID), Kind: string(s.Kind), URL: s.URL, Country: s.Country,
			Region: s.Region, Lang: s.Lang, IntervalS: s.IntervalS,
			Enabled: s.Enabled, TermsOK: s.TermsOK, TermsNote: s.TermsNote,
			Hazard: s.Hazard, Notes: s.Notes, Health: []byte(`{}`),
		})
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := st.UpsertSources(writeCtx, records); err != nil {
		log.Warn("source seed failed", "path", path, "count", len(records), "err", err)
		return
	}
	log.Info("sources seeded", "path", path, "count", len(records))
}

// newJevClient picks the jev host from the key that is set. DOR_JEV_API_KEY
// and OPENROUTER_API_KEY go to OpenRouter (or DOR_JEV_BASE_URL); a
// TYPESAFE_API_KEY on its own goes to TypeSafe's host. No key is (nil, nil).
func newJevClient(models dorconfig.Models, b decide.Budget) (*decide.Client, string, error) {
	if key := firstEnv("DOR_JEV_API_KEY", "OPENROUTER_API_KEY"); key != "" {
		client, err := decide.NewClientFromModels(models, os.Getenv("DOR_JEV_BASE_URL"), key, b)
		return client, models.Jev.Model, err
	}
	if key := firstEnv("TYPESAFE_API_KEY"); key != "" {
		client, err := decide.NewTypeSafeClientFromModels(models, firstEnv("DOR_JEV_BASE_URL", "TYPESAFE_BASE_URL"), key, b)
		return client, models.Jev.TypeSafe.Model, err
	}
	return nil, "", nil
}

// newEnricher builds the Claude-feature backend from DOR_LLM_BACKENDS, a
// comma-separated fallback order of api, claude-cli and codex-cli. Unset
// means "api" when ANTHROPIC_API_KEY is set and nothing otherwise. Backends
// that cannot start are logged and skipped; nil means no backend.
func newEnricher(models dorconfig.Models, b llm.Budget, log *slog.Logger) llm.Completer {
	order := strings.TrimSpace(os.Getenv("DOR_LLM_BACKENDS"))
	if order == "" {
		order = "api"
	}
	concurrency, _ := strconv.Atoi(os.Getenv("DOR_LLM_CLI_CONCURRENCY"))
	var backends []llm.Backend
	for _, name := range strings.Split(order, ",") {
		name = strings.TrimSpace(name)
		var (
			c   llm.Completer
			err error
		)
		switch name {
		case "":
			continue
		case "api":
			key := strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY"))
			if key == "" {
				if os.Getenv("DOR_LLM_BACKENDS") != "" {
					log.Warn("llm backend skipped: ANTHROPIC_API_KEY is not set", "backend", name)
				}
				continue
			}
			c, err = llm.NewFromConfig(models, key, os.Getenv("DOR_CLAUDE_BASE_URL"), b)
		case string(llm.CLIClaude):
			haiku, opus := claudeCLIModels(models)
			c, err = llm.NewCLI(llm.CLIConfig{
				Kind:        llm.CLIClaude,
				HaikuModel:  haiku,
				OpusModel:   opus,
				Effort:      models.Claude.Effort,
				Timeout:     models.Claude.Timeout.Std(),
				Concurrency: concurrency,
				Budget:      b,
				Logger:      log,
			})
			if err == nil {
				log.Info("claude-cli models", "haiku", haiku, "opus", opus)
			}
		case string(llm.CLICodex):
			c, err = llm.NewCLI(llm.CLIConfig{
				Kind:        llm.CLICodex,
				HaikuModel:  envOr("DOR_CODEX_MODEL", "gpt-6-luna"),
				OpusModel:   envOr("DOR_CODEX_OPUS_MODEL", "gpt-6-sol"),
				Effort:      models.Claude.Effort,
				Timeout:     models.Claude.Timeout.Std(),
				Concurrency: concurrency,
				Budget:      b,
				Logger:      log,
			})
		default:
			log.Warn("llm backend skipped: unknown name (want api, claude-cli or codex-cli)", "backend", name)
			continue
		}
		if err != nil {
			log.Warn("llm backend unavailable", "backend", name, "err", err)
			continue
		}
		backends = append(backends, llm.Backend{Name: name, Completer: c})
	}
	switch len(backends) {
	case 0:
		return nil
	case 1:
		log.Info("claude enricher enabled", "backend", backends[0].Name, "haiku", models.Claude.HaikuModel)
		return backends[0].Completer
	}
	chain := llm.NewChain(log, backends...)
	log.Info("claude enricher enabled", "backends", strings.Join(chain.Names(), " > "), "haiku", models.Claude.HaikuModel)
	return chain
}

// claudeCLIModels returns the models for the two tiers. Both default to the
// Haiku model so Opus-tier jobs do not drain a Pro subscription. A blank env
// value means unset, so a copied-and-left-empty line does not break startup.
func claudeCLIModels(models dorconfig.Models) (haiku, opus string) {
	haiku = firstEnv("DOR_CLAUDE_CLI_MODEL")
	if haiku == "" {
		haiku = models.Claude.HaikuModel
	}
	opus = firstEnv("DOR_CLAUDE_CLI_OPUS_MODEL")
	if opus == "" {
		opus = models.Claude.HaikuModel
	}
	return haiku, opus
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func wantsAgent(selection, name string) bool {
	selection = strings.TrimSpace(selection)
	if selection == "all" {
		return true
	}
	if selection == "" || selection == "none" {
		return false
	}
	for _, part := range strings.Split(selection, ",") {
		if strings.TrimSpace(part) == name {
			return true
		}
	}
	return false
}

// displaySourceName gives the reviewed source id a panel-friendly label when
// the YAML has no separate display name.
func displaySourceName(id string) string {
	words := strings.Fields(strings.ReplaceAll(strings.TrimSpace(id), "-", " "))
	for i, w := range words {
		if w == "" {
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return strings.Join(words, " ")
}
