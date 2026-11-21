package agents

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/sources"
)

// RSSAgentName is the registry name of the RSS/Atom fetcher. One process can
// run every shard (local mode) or one shard (production replica).
const RSSAgentName = "rss"

// rssMaxItemsPerFetch caps one feed's contribution to a single poll. A cold
// start fetches a feed's whole window (some feeds carry 100+ items); at most 50
// are published per poll so a single feed cannot flood the wire in one burst.
// When the cap is hit the conditional validators are dropped, so the next poll
// gets a full body and can publish the remainder instead of a 304.
const rssMaxItemsPerFetch = 50

// rssColdStartMaxAge is the freshness cutoff for a new RSS item. On a cold
// start, an old feed window must not backfill the wire with days-old news.
const rssColdStartMaxAge = 48 * time.Hour

// NewRSSFetcher builds the RSS/Atom fetcher agent.
func newRSSFetcher(rt *fetcherRuntime) (Agent, error) {
	return NewBase(rt.bus, rt.metrics, rt.log, Spec{
		Name:     RSSAgentName,
		Producer: true,
		Produce:  rt.produceRSS,
	})
}

func (r *fetcherRuntime) produceRSS(ctx context.Context, out *Emitter) error {
	file, err := r.loadSources()
	if err != nil {
		return err
	}
	feeds := file.ForKind(sources.KindRSS)
	if len(feeds) == 0 {
		return fmt.Errorf("rss: no enabled rss sources in %s", r.cfg.SourcesFile)
	}
	shards := r.cfg.RSSShards
	if shards <= 0 {
		shards = 3
	}
	if r.cfg.RSSShard >= 0 {
		if r.cfg.RSSShard >= shards {
			return fmt.Errorf("rss: shard %d is outside 0..%d", r.cfg.RSSShard, shards-1)
		}
		sub := shardFeeds(feeds, r.cfg.RSSShard, shards)
		r.log.Info("rss shard starting", "shard", r.cfg.RSSShard, "shards", shards, "feeds", len(sub))
		r.runFeedShard(ctx, out, sub)
		return nil
	}

	// Local mode: all replicas in one process, each shard with its own
	// goroutines and concurrency limit.
	var wg sync.WaitGroup
	for shard := 0; shard < shards; shard++ {
		sub := shardFeeds(feeds, shard, shards)
		wg.Add(1)
		go func(shard int, sub []sources.Source) {
			defer wg.Done()
			r.log.Info("rss shard starting", "shard", shard, "shards", shards, "feeds", len(sub))
			r.runFeedShard(ctx, out, sub)
		}(shard, sub)
	}
	wg.Wait()
	return nil
}

// runFeedShard starts one goroutine per feed. The semaphore bounds real
// fetches, not sleeps, so a hanging feed holds one slot for at most one
// timeout while healthy feeds keep their cadence.
func (r *fetcherRuntime) runFeedShard(ctx context.Context, out *Emitter, feeds []sources.Source) {
	sem := make(chan struct{}, r.cfg.RSSConcurrency)
	var wg sync.WaitGroup
	for _, src := range feeds {
		wg.Add(1)
		go func(src sources.Source) {
			defer wg.Done()
			r.pollFeed(ctx, out, sem, src)
		}(src)
	}
	wg.Wait()
}

// pollFeed is one feed's persistent schedule. It never returns before ctx is
// done, and a failure only changes this feed's next attempt.
func (r *fetcherRuntime) pollFeed(ctx context.Context, out *Emitter, sem chan struct{}, src sources.Source) {
	state := fetch.NewFeedState(fetch.FeedStateOptions{Interval: src.Interval()})
	cond := fetch.Conditional{}
	if rec, ok := r.restoreHealth(ctx, src.ID); ok {
		cond.ETag = rec.ETag
		cond.LastModified = rec.LastModified
	}
	seen := newSeenSet(256, 48*time.Hour)
	first := true
	wasQuarantined := false

	for ctx.Err() == nil {
		now := r.now().UTC()
		if first {
			first = false
			// Spread the startup herd across the interval instead of polling
			// every feed at once.
			limit := src.Interval()
			if limit > 30*time.Second {
				limit = 30 * time.Second
			}
			if !waitUntil(ctx, r.now, now.Add(time.Duration(rand.Int63n(int64(limit)+1)))) {
				return
			}
		} else if !waitUntil(ctx, r.now, state.Next()) {
			return
		}
		now = r.now().UTC()
		state.Begin(now)
		state.ResetQuarantine(now)

		attempt := r.fetchFeedOnce(ctx, out, sem, src, &cond, seen, now)
		switch {
		case attempt.err == nil && attempt.notModified:
			state.OnNotModified(now)
		case attempt.err == nil:
			state.OnSuccess(now, attempt.items)
		case attempt.blocked:
			state.OnBlocked(now)
		case attempt.parseError:
			state.OnFailure(now, true)
		default:
			state.OnFailure(now, false)
		}
		cond = nextConditional(cond, attempt)
		if attempt.capped || (attempt.err != nil && attempt.items > 0) {
			// The remainder was not consumed (cap) or not fully published.
			// Drop the validators so the next attempt is a full fetch, not a
			// 304 that hides the rest of the body.
			cond = fetch.Conditional{}
		}

		rec := r.health.Record(ctx, fetch.Update{
			SourceID:            src.ID,
			Kind:                string(src.Kind),
			State:               state.State(),
			Success:             attempt.err == nil,
			Items:               attempt.items,
			Interval:            state.Interval(),
			Err:                 errorString(attempt.err),
			ETag:                cond.ETag,
			LastModified:        cond.LastModified,
			NextAttempt:         state.Next(),
			QuarantinedUntil:    state.QuarantinedUntil(),
			ConsecutiveFailures: state.Failures(),
			ParseFailures:       state.ParseFailures(),
		})
		quarantined := state.State() == fetch.StateQuarantined
		switch {
		case quarantined && !wasQuarantined:
			r.log.Warn("feed quarantined", "source", src.ID, "reason", rec.LastError,
				"until", state.QuarantinedUntil().Format(time.RFC3339))
		case !quarantined && wasQuarantined:
			r.log.Info("feed out of quarantine", "source", src.ID)
		case attempt.err != nil:
			r.log.Warn("feed fetch failed", "source", src.ID, "state", state.State().String(),
				"next", state.Next().Format(time.RFC3339), "err", attempt.err)
		case attempt.items > 0:
			r.log.Debug("feed published", "source", src.ID, "items", attempt.items,
				"interval", state.Interval().String())
		}
		wasQuarantined = quarantined
	}
}

// nextConditional updates the HTTP validators after one attempt. A full 200
// response replaces them unconditionally, so a server that stops sending
// ETag/Last-Modified clears the stale value instead of 304-ing forever; a 304
// or an error keeps the previous pair.
func nextConditional(prev fetch.Conditional, att feedAttempt) fetch.Conditional {
	if att.err != nil || att.notModified {
		return prev
	}
	return fetch.Conditional{ETag: att.etag, LastModified: att.lastModified}
}

type feedAttempt struct {
	items        int
	notModified  bool
	capped       bool
	etag         string
	lastModified string
	parseError   bool
	blocked      bool
	err          error
}

func (r *fetcherRuntime) fetchFeedOnce(ctx context.Context, out *Emitter, sem chan struct{}, src sources.Source, cond *fetch.Conditional, seen *seenSet, now time.Time) feedAttempt {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return feedAttempt{err: ctx.Err()}
	}
	defer func() { <-sem }()

	// Re-read after acquiring the slot: the wait can be long when many feeds
	// are due at once.
	attemptTime := r.now().UTC()
	resp, err := r.client.Get(ctx, src.URL, *cond, fetch.WithMaxBytes(16<<20))
	if err != nil {
		return feedAttempt{err: err, blocked: !fetch.IsRetryable(err)}
	}
	if resp.NotModified {
		return feedAttempt{notModified: true, etag: cond.ETag, lastModified: cond.LastModified}
	}
	parsed, err := fetch.ParseFeed(bytes.NewReader(resp.Body), resp.URL)
	if err != nil {
		return feedAttempt{err: err, parseError: true}
	}
	att := feedAttempt{etag: resp.ETag, lastModified: resp.LastModified}
	seen.Grow(len(parsed.Items))
	lang := parsed.Lang
	if lang == "" {
		lang = src.Lang
	}
	for _, item := range parsed.Items {
		if ctx.Err() != nil {
			return feedAttempt{items: att.items, etag: resp.ETag, lastModified: resp.LastModified, err: ctx.Err()}
		}
		if seen.Has(item.Link, now) {
			continue
		}
		if !item.Published.IsZero() && now.Sub(item.Published) > rssColdStartMaxAge {
			// Old window items are remembered but never published.
			seen.Add(item.Link, now)
			continue
		}
		rawID := RawArticleID("rss", item.Link)
		if r.seenKV.Has(ctx, rawID) {
			seen.Add(item.Link, now)
			continue
		}
		if att.items >= rssMaxItemsPerFetch {
			att.capped = true
			break
		}
		raw := &dorv1.RawArticle{
			Id:          rawID,
			SourceId:    src.ID,
			Kind:        dorv1.SourceKind_SOURCE_KIND_RSS,
			Url:         item.Link,
			Title:       item.Title,
			Summary:     item.Summary,
			PublishedAt: unixOrZero(item.Published),
			FetchedAt:   attemptTime.Unix(),
			Lang:        lang,
		}
		if raw.Title == "" {
			raw.Title = fetch.TitleFromURL(item.Link)
		}
		published, err := r.publishRaw(ctx, out, bus.SubjectArticleRaw, raw)
		if err != nil {
			return feedAttempt{items: att.items, etag: resp.ETag, lastModified: resp.LastModified, err: fmt.Errorf("publish %s: %w", item.Link, err)}
		}
		seen.Add(item.Link, now)
		if published {
			r.seenKV.Add(ctx, rawID)
			att.items++
		}
	}
	return att
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
