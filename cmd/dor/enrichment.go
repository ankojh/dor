package main

import (
	"context"
	"log/slog"
	"os"
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

	if key := firstEnv("DOR_JEV_API_KEY", "OPENROUTER_API_KEY", "TYPESAFE_API_KEY"); key != "" {
		client, err := decide.NewClientFromModels(models, os.Getenv("DOR_JEV_BASE_URL"), key, manager)
		if err != nil {
			log.Warn("jev client unavailable", "err", err)
		} else {
			deps.Decider = client
			log.Info("jev client enabled", "model", models.Jev.Model)
		}
	}
	if key := strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")); key != "" {
		client, err := llm.NewFromConfig(models, key, os.Getenv("DOR_CLAUDE_BASE_URL"), manager)
		if err != nil {
			log.Warn("claude client unavailable", "err", err)
		} else {
			deps.Enricher = client
			log.Info("claude enricher enabled", "haiku", models.Claude.HaikuModel)
		}
	}
	var completer textsvc.Completer
	if deps.Enricher != nil {
		completer = deps.Enricher
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
			ID: s.ID, Kind: string(s.Kind), URL: s.URL, Country: s.Country,
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
