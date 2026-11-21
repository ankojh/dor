package agents

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/sources"
)

// GDELTAgentName is the registry name of the GDELT 2.0 fetcher.
const GDELTAgentName = "gdelt"

// gdeltMaxCSVBytes bounds the uncompressed CSV so a malformed or hostile zip
// cannot exhaust memory.
const gdeltMaxCSVBytes = 128 << 20

// NewGDELTFetcher builds the GDELT 2.0 fetcher agent. It is a separate agent
// with its own goroutine and HTTP client path, so a GDELT outage can never
// block RSS.
func newGDELTFetcher(rt *fetcherRuntime) (Agent, error) {
	return NewBase(rt.bus, rt.metrics, rt.log, Spec{
		Name:     GDELTAgentName,
		Producer: true,
		Produce:  rt.produceGDELT,
	})
}

func (r *fetcherRuntime) produceGDELT(ctx context.Context, out *Emitter) error {
	file, err := r.loadSources()
	if err != nil {
		return err
	}
	all := file.ForKind(sources.KindGDELT)
	if len(all) == 0 {
		return fmt.Errorf("gdelt: no enabled gdelt source in %s", r.cfg.SourcesFile)
	}
	src := all[0]
	state := fetch.NewFeedState(fetch.FeedStateOptions{Interval: src.Interval()})
	lastFile := ""
	if rec, ok := r.restoreHealth(ctx, src.ID); ok {
		lastFile = rec.LastFile
	}
	seen := newSeenSet(4000, 48*time.Hour)
	cond := fetch.Conditional{}
	first := true
	wasQuarantined := false

	for ctx.Err() == nil {
		now := r.now().UTC()
		if first {
			first = false
		} else if !waitUntil(ctx, r.now, state.Next()) {
			return nil
		}
		now = r.now().UTC()
		state.Begin(now)
		state.ResetQuarantine(now)

		var (
			newFile    string
			items      int
			attemptErr error
			blocked    bool
			etag       string
			lastMod    string
		)
		resp, err := r.client.Get(ctx, src.URL, cond, fetch.WithMaxBytes(1<<20))
		switch {
		case err != nil:
			attemptErr = err
			blocked = !fetch.IsRetryable(err)
		case resp.NotModified:
			// Nothing new since the last check.
		default:
			etag, lastMod = resp.ETag, resp.LastModified
			newFile, err = fetch.GDELTLastFile(resp.Body, r.cfg.GDELTFile)
			if err != nil {
				attemptErr = err
				break
			}
			if newFile == lastFile {
				newFile = ""
				break
			}
			items, err = r.processGDELTFile(ctx, out, src, newFile, seen, now)
			if err != nil {
				// GDELT lists a file in lastupdate.txt before it finishes
				// uploading, so a 404 (or a partial zip) is transient. Never
				// quarantine a file download; back off and read lastupdate
				// again. Only a robots denial is a policy block.
				attemptErr = err
				blocked = errors.Is(err, fetch.ErrRobotsDisallowed)
				break
			}
			lastFile = newFile
		}

		switch {
		case attemptErr == nil:
			state.OnSuccess(now, 0)
			state.WithNext(now.Add(src.Interval()))
		case blocked:
			state.OnBlocked(now)
		default:
			// GDELT publishes a name before the bytes are available, and a
			// corrupt zip is transient too: back off, never quarantine the
			// whole source. Its separate agent keeps RSS unaffected.
			state.OnFailure(now, false)
		}
		if attemptErr == nil {
			// Clear stale validators when the server stops sending them.
			cond.ETag = etag
			cond.LastModified = lastMod
		}

		rec := r.health.Record(ctx, fetch.Update{
			SourceID:            src.ID,
			Kind:                string(src.Kind),
			State:               state.State(),
			Success:             attemptErr == nil,
			Items:               items,
			Interval:            state.Interval(),
			Err:                 errorString(attemptErr),
			ETag:                cond.ETag,
			LastModified:        cond.LastModified,
			LastFile:            lastFile,
			NextAttempt:         state.Next(),
			QuarantinedUntil:    state.QuarantinedUntil(),
			ConsecutiveFailures: state.Failures(),
			ParseFailures:       state.ParseFailures(),
		})
		quarantined := state.State() == fetch.StateQuarantined
		switch {
		case attemptErr != nil:
			r.log.Warn("gdelt fetch failed", "source", src.ID, "state", state.State().String(),
				"next", state.Next().Format(time.RFC3339), "err", attemptErr)
		case items > 0:
			r.log.Info("gdelt file published", "source", src.ID, "file", lastFile, "items", items)
		}
		if quarantined && !wasQuarantined {
			r.log.Warn("gdelt quarantined", "reason", rec.LastError,
				"until", state.QuarantinedUntil().Format(time.RFC3339))
		}
		wasQuarantined = quarantined
	}
	return nil
}

func (r *fetcherRuntime) processGDELTFile(ctx context.Context, out *Emitter, src sources.Source, fileURL string, seen *seenSet, now time.Time) (int, error) {
	resp, err := r.client.Get(ctx, fileURL, fetch.Conditional{}, fetch.WithMaxBytes(64<<20))
	if err != nil {
		return 0, fmt.Errorf("gdelt: download %s: %w", fileURL, err)
	}
	csvData, err := fetch.UnzipCSV(resp.Body, gdeltMaxCSVBytes)
	if err != nil {
		return 0, fmt.Errorf("gdelt: %s: %w", fileURL, err)
	}
	var parsed []fetch.GDELTItem
	if strings.EqualFold(r.cfg.GDELTFile, "export") {
		parsed, err = fetch.ParseGDELTExport(bytes.NewReader(csvData))
	} else {
		parsed, err = fetch.ParseGDELTGKG(bytes.NewReader(csvData))
	}
	if err != nil {
		return 0, err
	}

	allowDomains := append(append([]string(nil), defaultGDELTDomains...), r.cfg.GDELTExtraDomains...)
	seen.Grow(len(parsed))
	published := 0
	skippedNotLocated, skippedDomain, skippedTheme, skippedSeen := 0, 0, 0, 0
	for _, item := range parsed {
		if published >= r.cfg.GDELTMaxItems {
			break
		}
		if !item.Located {
			skippedNotLocated++
			continue
		}
		if !fetch.DomainAllowed(item.Domain, allowDomains) {
			skippedDomain++
			continue
		}
		if !gdeltThemeAllowed(item.Themes) {
			skippedTheme++
			continue
		}
		if seen.Has(item.URL, now) {
			skippedSeen++
			continue
		}
		rawID := RawArticleID("gdelt", item.URL)
		if r.seenKV.Has(ctx, rawID) {
			seen.Add(item.URL, now)
			skippedSeen++
			continue
		}
		raw := &dorv1.RawArticle{
			Id:          rawID,
			SourceId:    src.ID,
			Kind:        dorv1.SourceKind_SOURCE_KIND_GDELT,
			Url:         item.URL,
			Title:       gdeltTitle(item),
			Summary:     gdeltSummary(item),
			PublishedAt: unixOrZero(item.Published),
			FetchedAt:   now.Unix(),
			Hints: &dorv1.RawHints{
				Located:   true,
				Lat:       item.Lat,
				Lon:       item.Lon,
				Precision: precision(item.Precision),
				Themes:    item.Themes,
				Place:     item.Place,
				EventId:   item.RecordID,
			},
			Burst: true,
		}
		accepted, err := r.publishRaw(ctx, out, bus.SubjectArticleRaw, raw)
		if err != nil {
			return published, fmt.Errorf("publish %s: %w", item.URL, err)
		}
		seen.Add(item.URL, now)
		if accepted {
			r.seenKV.Add(ctx, rawID)
			published++
		}
	}
	r.log.Info("gdelt file filtered", "file", fileURL, "rows", len(parsed), "published", published,
		"not_located", skippedNotLocated, "domain", skippedDomain, "theme", skippedTheme, "seen", skippedSeen,
		"cap", r.cfg.GDELTMaxItems)
	return published, nil
}

// defaultGDELTThemes is the relevant-theme allowlist. A row is kept when any
// theme starts with one of these prefixes, so "ECON_" keeps ECON_INFLATION and
// "PROTEST" keeps PROTEST. It is deliberately narrower than all of GDELT.
var defaultGDELTThemes = []string{
	"ARMEDCONFLICT", "TERROR", "PROTEST", "REBELLION", "MILITARY",
	"NATURAL_DISASTER", "MANMADE_DISASTER", "WILDFIRE", "FLOOD", "EARTHQUAKE",
	"DROUGHT", "CYCLONE", "HURRICANE", "TSUNAMI", "VOLCANO", "LANDSLIDE",
	"EPIDEMIC", "DISEASE", "CRISISLEX",
	"REFUGEE", "SANCTIONS", "HUMAN_RIGHTS", "ELECTION",
	"ECON_", "ENERGY", "OIL", "TRADE", "NUCLEAR", "CYBER", "MARITIME",
	"FOOD_SECURITY", "CLIMATE", "ENV_",
}

func gdeltThemeAllowed(themes []string) bool {
	for _, theme := range themes {
		for _, prefix := range defaultGDELTThemes {
			if strings.HasPrefix(theme, prefix) {
				return true
			}
		}
	}
	return false
}

// gdeltDisplayThemes are the themes worth naming in a fallback headline. They
// are the accepted set, in priority order.
var gdeltDisplayThemes = []string{
	"ARMEDCONFLICT", "TERROR", "NATURAL_DISASTER", "FLOOD", "EARTHQUAKE",
	"CYCLONE", "HURRICANE", "WILDFIRE", "VOLCANO", "PROTEST", "REBELLION",
	"EPIDEMIC", "REFUGEE", "SANCTIONS", "ELECTION",
}

func gdeltTitle(item fetch.GDELTItem) string {
	if item.Title != "" {
		return item.Title
	}
	if title := fetch.TitleFromURL(item.URL); title != "" {
		return title
	}
	theme := ""
	for _, want := range gdeltDisplayThemes {
		for _, have := range item.Themes {
			if strings.HasPrefix(have, want) {
				theme = want
				break
			}
		}
		if theme != "" {
			break
		}
	}
	where := item.Place
	if where == "" {
		where = item.Country
	}
	switch {
	case theme != "" && where != "":
		return humanizeTheme(theme) + " in " + where
	case theme != "":
		return humanizeTheme(theme)
	case where != "":
		return "GDELT event in " + where
	default:
		return "GDELT event"
	}
}

func gdeltSummary(item fetch.GDELTItem) string {
	var parts []string
	if item.Place != "" {
		parts = append(parts, item.Place)
	}
	if len(item.Themes) > 0 {
		themes := item.Themes
		if len(themes) > 6 {
			themes = themes[:6]
		}
		parts = append(parts, strings.Join(themes, ", "))
	}
	if item.Actor1 != "" || item.Actor2 != "" {
		parts = append(parts, strings.TrimSpace(item.Actor1+" / "+item.Actor2))
	}
	return truncate(strings.Join(parts, " · "), 400)
}

func humanizeTheme(theme string) string {
	theme = strings.TrimSuffix(theme, "_")
	theme = strings.ToLower(strings.ReplaceAll(theme, "_", " "))
	if theme == "" {
		return "Event"
	}
	r := []rune(theme)
	r[0] = []rune(strings.ToUpper(string(r[0])))[0]
	return string(r)
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// precision maps an adapter precision to the wire enum.
func precision(p fetch.PlacePrecision) dorv1.Precision {
	switch p {
	case fetch.PrecisionPoint:
		return dorv1.Precision_PRECISION_POINT
	case fetch.PrecisionCity:
		return dorv1.Precision_PRECISION_CITY
	case fetch.PrecisionRegion:
		return dorv1.Precision_PRECISION_REGION
	default:
		return dorv1.Precision_PRECISION_COUNTRY
	}
}
