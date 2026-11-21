// Command feedcheck verifies every source in deploy/sources/sources.yaml:
// it fetches the feed or API with the production User-Agent, checks robots.txt,
// parses the response with the production adapter and records the status, item
// count and newest item time. The JSON report is the evidence behind
// deploy/sources/README.md.
//
//	go run ./tools/feedcheck -sources deploy/sources/sources.yaml -out /tmp/feedcheck.json
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/sources"
)

type result struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Region     string `json:"region,omitempty"`
	Country    string `json:"country,omitempty"`
	URL        string `json:"url"`
	Enabled    bool   `json:"enabled"`
	TermsOK    bool   `json:"terms_ok"`
	Robots     string `json:"robots"`
	HTTPStatus int    `json:"http_status,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	Items      int    `json:"items"`
	Located    int    `json:"located,omitempty"`
	Newest     string `json:"newest_item,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

type report struct {
	CheckedAt string   `json:"checked_at"`
	File      string   `json:"file"`
	UserAgent string   `json:"user_agent"`
	Results   []result `json:"results"`
}

func main() {
	sourcesFile := flag.String("sources", "deploy/sources/sources.yaml", "source seed list")
	outPath := flag.String("out", "", "write the JSON report here (default stdout)")
	concurrency := flag.Int("concurrency", 12, "parallel fetches")
	timeout := flag.Duration("timeout", 25*time.Second, "per-source timeout")
	onlyKind := flag.String("kind", "", "only check this kind (rss, gdelt, usgs, ...)")
	flag.Parse()

	file, err := sources.Load(*sourcesFile)
	if err != nil {
		fatal(err)
	}
	srcs := file.All()
	if *onlyKind != "" {
		filtered := srcs[:0]
		for _, s := range srcs {
			if string(s.Kind) == *onlyKind {
				filtered = append(filtered, s)
			}
		}
		srcs = filtered
	}
	ua := fetch.DefaultUserAgent
	if v := os.Getenv("DOR_HTTP_USER_AGENT"); v != "" {
		ua = v
	}
	client := fetch.NewClient(fetch.ClientOptions{UserAgent: ua, Timeout: *timeout})
	robots := fetch.NewRobotsCache(ua, 10*time.Second, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(len(srcs)+1)*time.Minute)
	defer cancel()

	results := make([]result, len(srcs))
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for i, src := range srcs {
		wg.Add(1)
		go func(i int, src sources.Source) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = check(ctx, client, robots, src)
		}(i, src)
	}
	wg.Wait()

	rep := report{
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
		File:      *sourcesFile,
		UserAgent: ua,
		Results:   results,
	}
	if *outPath != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fatal(err)
		}
		if err := os.WriteFile(*outPath, append(data, '\n'), 0o644); err != nil {
			fatal(err)
		}
	} else {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(data))
	}
	summarise(results, time.Since(start))
}

func check(ctx context.Context, client *fetch.Client, robots *fetch.RobotsCache, src sources.Source) result {
	res := result{
		ID: src.ID, Kind: string(src.Kind), Region: src.Region, Country: src.Country,
		URL: src.URL, Enabled: src.Enabled, TermsOK: src.TermsOK,
	}
	start := time.Now()
	defer func() { res.DurationMS = time.Since(start).Milliseconds() }()

	if u, err := url.Parse(src.URL); err == nil {
		decision, err := robots.Check(ctx, u)
		switch {
		case err != nil:
			res.Robots = "error: " + err.Error()
		case decision.Allowed:
			res.Robots = "allow"
		default:
			res.Robots = "disallow: " + decision.Reason
		}
	}
	resp, err := client.Get(ctx, src.URL, fetch.Conditional{})
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.HTTPStatus = resp.Status
	res.Bytes = len(resp.Body)

	switch src.Kind {
	case sources.KindRSS:
		feed, err := fetch.ParseFeed(bytes.NewReader(resp.Body), resp.URL)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		res.Items = len(feed.Items)
		res.Newest = newestFeedItem(feed.Items)
	case sources.KindGDELT:
		fileURL, err := fetch.GDELTLastFile(resp.Body, "gkg")
		if err != nil {
			res.Error = err.Error()
			return res
		}
		res.Items = 1
		file, err := client.Get(ctx, fileURL, fetch.Conditional{})
		if err != nil {
			res.Error = "lastupdate ok, file: " + err.Error()
			return res
		}
		csvData, err := fetch.UnzipCSV(file.Body, 0)
		if err != nil {
			res.Error = "lastupdate ok, unzip: " + err.Error()
			return res
		}
		items, err := fetch.ParseGDELTGKG(bytes.NewReader(csvData))
		if err != nil {
			res.Error = "lastupdate ok, parse: " + err.Error()
			return res
		}
		res.Items = len(items)
		located := 0
		var newest time.Time
		for _, item := range items {
			if item.Located {
				located++
			}
			if item.Published.After(newest) {
				newest = item.Published
			}
		}
		res.Located = located
		if !newest.IsZero() {
			res.Newest = newest.UTC().Format(time.RFC3339)
		}
		res.Bytes += len(file.Body)
	case sources.KindUSGS, sources.KindGDACS, sources.KindEONET, sources.KindReliefWeb:
		items, err := parseHazard(src.Kind, resp.Body)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		res.Items = len(items)
		located := 0
		var newest time.Time
		for _, item := range items {
			if item.Located {
				located++
			}
			if item.Published.After(newest) {
				newest = item.Published
			}
		}
		res.Located = located
		if !newest.IsZero() {
			res.Newest = newest.UTC().Format(time.RFC3339)
		}
	default:
		res.Error = "unsupported kind"
	}
	return res
}

func parseHazard(kind sources.Kind, body []byte) ([]fetch.HazardItem, error) {
	r := bytes.NewReader(body)
	switch kind {
	case sources.KindUSGS:
		return fetch.ParseUSGS(r)
	case sources.KindGDACS:
		return fetch.ParseGDACS(r)
	case sources.KindEONET:
		return fetch.ParseEONET(r)
	case sources.KindReliefWeb:
		return fetch.ParseReliefWeb(r)
	default:
		return nil, errors.New("unsupported kind")
	}
}

func newestFeedItem(items []fetch.FeedItem) string {
	var newest time.Time
	for _, item := range items {
		if item.Published.After(newest) {
			newest = item.Published
		}
	}
	if newest.IsZero() {
		return ""
	}
	return newest.UTC().Format(time.RFC3339)
}

func summarise(results []result, elapsed time.Duration) {
	ok, failed, disabled := 0, 0, 0
	byKind := map[string]int{}
	byRegion := map[string]int{}
	var failures []result
	for _, r := range results {
		byKind[r.Kind]++
		if !r.Enabled {
			disabled++
		}
		if r.Error == "" && r.Robots == "allow" && r.Items >= 0 {
			ok++
			if r.Kind == "rss" {
				byRegion[r.Region]++
			}
			continue
		}
		failed++
		if r.Enabled {
			failures = append(failures, r)
		}
	}
	fmt.Fprintf(os.Stderr, "\nfeedcheck: %d sources, %d ok, %d problems, %d disabled, %s\n",
		len(results), ok, failed, disabled, elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "by kind: %v\n", byKind)
	fmt.Fprintf(os.Stderr, "enabled rss by region: %v\n", byRegion)
	if len(failures) > 0 {
		sort.Slice(failures, func(i, j int) bool { return failures[i].ID < failures[j].ID })
		fmt.Fprintln(os.Stderr, "enabled sources that failed:")
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "  %-24s %-5s robots=%-8s http=%d %s\n",
				f.ID, f.Kind, f.Robots, f.HTTPStatus, f.Error)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "feedcheck:", err)
	os.Exit(1)
}
