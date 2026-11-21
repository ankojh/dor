package agents

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/sources"
)

// HazardAgentName is the registry name of the hazard fetcher. Hazards publish
// to article.raw.priority so the normalizer and wire can take them first.
const HazardAgentName = "hazard"

// hazardMaxItemsPerFetch caps one upstream per poll. GDACS serves a 7-day
// window (hundreds of alerts on a cold start); only current events belong on
// the priority lane in one burst.
const hazardMaxItemsPerFetch = 100

// hazardMaxAge is the freshness window for a new hazard. Older window entries
// stay in the feed but do not backfill the priority lane on a cold start.
const hazardMaxAge = 48 * time.Hour

// NewHazardFetcher builds the hazard fetcher agent (USGS, GDACS, NASA EONET,
// ReliefWeb). Each upstream has its own goroutine and backoff, so one slow or
// broken feed cannot delay the others.
func newHazardFetcher(rt *fetcherRuntime) (Agent, error) {
	return NewBase(rt.bus, rt.metrics, rt.log, Spec{
		Name:     HazardAgentName,
		Producer: true,
		Produce:  rt.produceHazard,
	})
}

func (r *fetcherRuntime) produceHazard(ctx context.Context, out *Emitter) error {
	file, err := r.loadSources()
	if err != nil {
		return err
	}
	var srcs []sources.Source
	for _, kind := range []sources.Kind{sources.KindUSGS, sources.KindGDACS, sources.KindEONET, sources.KindReliefWeb} {
		srcs = append(srcs, file.ForKind(kind)...)
	}
	active := make([]sources.Source, 0, len(srcs))
	for _, src := range srcs {
		if src.Kind == sources.KindReliefWeb && r.cfg.ReliefWebAppname == "" {
			r.log.Warn("reliefweb source is enabled but DOR_RELIEFWEB_APPNAME is empty; skipping it",
				"source", src.ID)
			continue
		}
		active = append(active, src)
	}
	if len(active) == 0 {
		return fmt.Errorf("hazard: no enabled hazard sources in %s", r.cfg.SourcesFile)
	}
	var wg sync.WaitGroup
	for _, src := range active {
		wg.Add(1)
		go func(src sources.Source) {
			defer wg.Done()
			r.pollHazard(ctx, out, src)
		}(src)
	}
	wg.Wait()
	return nil
}

func (r *fetcherRuntime) pollHazard(ctx context.Context, out *Emitter, src sources.Source) {
	state := fetch.NewFeedState(fetch.FeedStateOptions{Interval: src.Interval()})
	cond := fetch.Conditional{}
	if rec, ok := r.restoreHealth(ctx, src.ID); ok {
		cond.ETag = rec.ETag
		cond.LastModified = rec.LastModified
	}
	seen := newSeenSet(1024, 48*time.Hour)
	first := true
	wasQuarantined := false

	for ctx.Err() == nil {
		now := r.now().UTC()
		if first {
			first = false
			if !waitUntil(ctx, r.now, now.Add(time.Duration(rand.Int63n(int64(15*time.Second))))) {
				return
			}
		} else if !waitUntil(ctx, r.now, state.Next()) {
			return
		}
		now = r.now().UTC()
		state.Begin(now)
		state.ResetQuarantine(now)

		items, notModified, etag, lastMod, err := r.fetchHazardOnce(ctx, out, src, cond, seen, now)
		parseErr := false
		if err != nil && !notModified {
			parseErr = isParseError(err)
		}
		switch {
		case err == nil && notModified:
			state.OnNotModified(now)
		case err == nil:
			state.OnSuccess(now, items)
		case parseErr:
			state.OnFailure(now, true)
		case !fetch.IsRetryable(err):
			state.OnBlocked(now)
		default:
			state.OnFailure(now, false)
		}
		if err == nil {
			// Clear stale validators when the server stops sending them.
			cond.ETag = etag
			cond.LastModified = lastMod
		}

		rec := r.health.Record(ctx, fetch.Update{
			SourceID:            src.ID,
			Kind:                string(src.Kind),
			State:               state.State(),
			Success:             err == nil,
			Items:               items,
			Interval:            state.Interval(),
			Err:                 errorString(err),
			ETag:                cond.ETag,
			LastModified:        cond.LastModified,
			NextAttempt:         state.Next(),
			QuarantinedUntil:    state.QuarantinedUntil(),
			ConsecutiveFailures: state.Failures(),
			ParseFailures:       state.ParseFailures(),
		})
		quarantined := state.State() == fetch.StateQuarantined
		switch {
		case err != nil:
			r.log.Warn("hazard fetch failed", "source", src.ID, "kind", src.Kind,
				"state", state.State().String(), "next", state.Next().Format(time.RFC3339), "err", err)
		case items > 0:
			r.log.Info("hazards published", "source", src.ID, "kind", src.Kind, "items", items)
		}
		if quarantined && !wasQuarantined {
			r.log.Warn("hazard source quarantined", "source", src.ID, "reason", rec.LastError,
				"until", state.QuarantinedUntil().Format(time.RFC3339))
		}
		wasQuarantined = quarantined
	}
}

func (r *fetcherRuntime) fetchHazardOnce(ctx context.Context, out *Emitter, src sources.Source, cond fetch.Conditional, seen *seenSet, now time.Time) (items int, notModified bool, etag, lastModified string, err error) {
	target := src.URL
	if src.Kind == sources.KindReliefWeb {
		target = withAppname(src.URL, r.cfg.ReliefWebAppname)
	}
	resp, err := r.client.Get(ctx, target, cond, fetch.WithMaxBytes(8<<20))
	if err != nil {
		return 0, false, "", "", err
	}
	if resp.NotModified {
		return 0, true, cond.ETag, cond.LastModified, nil
	}
	body := bytes.NewReader(resp.Body)
	var hazards []fetch.HazardItem
	switch src.Kind {
	case sources.KindUSGS:
		hazards, err = fetch.ParseUSGS(body)
	case sources.KindGDACS:
		hazards, err = fetch.ParseGDACS(body)
	case sources.KindEONET:
		hazards, err = fetch.ParseEONET(body)
	case sources.KindReliefWeb:
		hazards, err = fetch.ParseReliefWeb(body)
	default:
		return 0, false, resp.ETag, resp.LastModified, fmt.Errorf("hazard: unsupported kind %q", src.Kind)
	}
	if err != nil {
		return 0, false, resp.ETag, resp.LastModified, err
	}
	seen.Grow(len(hazards))
	for _, h := range hazards {
		if h.URL == "" {
			continue
		}
		// The seen key includes the revision: an escalation (a later PAGER
		// alert, a revised magnitude, a new GDACS alert level) is a new item.
		key := h.ID + "|" + h.Revision
		if seen.Has(key, now) {
			continue
		}
		if !h.Published.IsZero() && now.Sub(h.Published) > hazardMaxAge {
			seen.Add(key, now)
			continue
		}
		kindPrefix := hazardKindPrefix(src.Kind)
		rawID := RawArticleID(kindPrefix, key)
		if r.seenKV.Has(ctx, rawID) {
			seen.Add(key, now)
			continue
		}
		if items >= hazardMaxItemsPerFetch {
			continue
		}
		raw := &dorv1.RawArticle{
			Id:          rawID,
			SourceId:    src.ID,
			Kind:        sourceKind(src.Kind),
			Url:         h.URL,
			Title:       hazardTitle(src, h),
			Summary:     h.Summary,
			PublishedAt: unixOrZero(h.Published),
			FetchedAt:   now.Unix(),
			Hints: &dorv1.RawHints{
				Located:       h.Located,
				Lat:           h.Lat,
				Lon:           h.Lon,
				Precision:     precision(h.Precision),
				Themes:        h.Themes,
				Country:       h.Country,
				Place:         h.Place,
				Hazard:        h.Hazard,
				Magnitude:     h.Magnitude,
				MagnitudeUnit: h.MagnitudeUnit,
				Severity:      h.Severity,
				EventId:       h.ID,
				Revision:      h.Revision,
			},
		}
		accepted, err := r.publishRaw(ctx, out, bus.SubjectArticleRawPriority, raw)
		if err != nil {
			return items, false, resp.ETag, resp.LastModified, fmt.Errorf("publish %s: %w", h.URL, err)
		}
		seen.Add(key, now)
		if accepted {
			r.seenKV.Add(ctx, rawID)
			items++
		}
	}
	return items, false, resp.ETag, resp.LastModified, nil
}

func hazardTitle(src sources.Source, h fetch.HazardItem) string {
	if h.Title != "" {
		return h.Title
	}
	hazard := h.Hazard
	if hazard == "" {
		hazard = src.Hazard
	}
	if hazard == "" {
		hazard = "hazard"
	}
	if h.Country != "" {
		return humanizeTheme(hazard) + " in " + h.Country
	}
	return humanizeTheme(hazard) + " alert"
}

// isParseError distinguishes an adapter decoding failure from a network or
// HTTP error. The parsers wrap every decode error with a stable prefix.
func isParseError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, prefix := range []string{
		"fetch: parse ", "fetch: not a feed", "fetch: parse gdelt", "zip has no csv",
		"gdelt: lastupdate has no",
	} {
		if len(msg) >= len(prefix) && msg[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// hazardKindPrefix is the msg-id namespace for one hazard upstream.
func hazardKindPrefix(kind sources.Kind) string {
	switch kind {
	case sources.KindUSGS:
		return "usgs"
	case sources.KindGDACS:
		return "gdacs"
	case sources.KindEONET:
		return "eonet"
	case sources.KindReliefWeb:
		return "reliefweb"
	default:
		return "hazard"
	}
}

func sourceKind(kind sources.Kind) dorv1.SourceKind {
	switch kind {
	case sources.KindRSS:
		return dorv1.SourceKind_SOURCE_KIND_RSS
	case sources.KindGDELT:
		return dorv1.SourceKind_SOURCE_KIND_GDELT
	case sources.KindUSGS:
		return dorv1.SourceKind_SOURCE_KIND_USGS
	case sources.KindGDACS:
		return dorv1.SourceKind_SOURCE_KIND_GDACS
	case sources.KindEONET:
		return dorv1.SourceKind_SOURCE_KIND_EONET
	case sources.KindReliefWeb:
		return dorv1.SourceKind_SOURCE_KIND_RELIEFWEB
	default:
		return dorv1.SourceKind_SOURCE_KIND_UNSPECIFIED
	}
}

// withAppname adds ReliefWeb's required appname parameter.
func withAppname(raw, appname string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	q.Set("appname", appname)
	u.RawQuery = q.Encode()
	return u.String()
}
