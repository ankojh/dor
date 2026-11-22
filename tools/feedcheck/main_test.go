package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/sources"
)

const feedcheckTestRSS = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>Test feed</title><link>https://example.test/</link><description>test feed</description>
<item><title>First item</title><link>https://example.test/1</link><guid>1</guid><pubDate>Sun, 22 Nov 2026 10:00:00 GMT</pubDate></item>
<item><title>Second item</title><link>https://example.test/2</link><guid>2</guid><pubDate>Sun, 22 Nov 2026 11:00:00 GMT</pubDate></item>
</channel></rss>`

func feedcheckTestServer(t *testing.T, robots string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		if robots == "" {
			http.NotFound(w, nil)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, robots)
	})
	mux.HandleFunc("/feed", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = io.WriteString(w, feedcheckTestRSS)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func feedcheckTestClient(srv *httptest.Server, robots bool) (*fetch.Client, *fetch.RobotsCache) {
	var cache *fetch.RobotsCache
	if robots {
		cache = fetch.NewRobotsCacheWithTransport(fetch.DefaultUserAgent, 5*time.Second, nil, srv.Client().Transport)
	}
	client := fetch.NewClient(fetch.ClientOptions{
		UserAgent: fetch.DefaultUserAgent,
		Timeout:   5 * time.Second,
		Robots:    cache,
		Transport: srv.Client().Transport,
	})
	return client, cache
}

func TestCheckStopsOnRobotsDenialWithoutFetchingFeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, hits := feedcheckTestServer(t, "User-agent: *\nDisallow: /feed\n")
	client, robots := feedcheckTestClient(srv, true)

	res := check(ctx, client, robots, sources.Source{
		ID: "denied", Kind: sources.KindRSS, URL: srv.URL + "/feed", Enabled: true,
	})
	if hits.Load() != 0 {
		t.Fatalf("denied feed endpoint fetched %d times, want 0", hits.Load())
	}
	if !strings.HasPrefix(res.Robots, "disallow") {
		t.Fatalf("Robots = %q, want a disallow decision", res.Robots)
	}
	if !strings.Contains(res.Error, "robots") {
		t.Fatalf("Error = %q, want a robots denial", res.Error)
	}
	if res.Items != 0 {
		t.Fatalf("Items = %d, want 0 for a denied feed", res.Items)
	}
}

func TestCheckFetchesAndParsesAllowedFeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv, hits := feedcheckTestServer(t, "User-agent: *\nAllow: /\n")
	client, robots := feedcheckTestClient(srv, true)

	res := check(ctx, client, robots, sources.Source{
		ID: "allowed", Kind: sources.KindRSS, URL: srv.URL + "/feed", Enabled: true,
	})
	if hits.Load() != 1 {
		t.Fatalf("allowed feed endpoint fetched %d times, want 1", hits.Load())
	}
	if res.Robots != "allow" {
		t.Fatalf("Robots = %q, want allow", res.Robots)
	}
	if res.Error != "" {
		t.Fatalf("Error = %q, want empty", res.Error)
	}
	if res.Items != 2 {
		t.Fatalf("Items = %d, want 2", res.Items)
	}
	if res.HTTPStatus != http.StatusOK {
		t.Fatalf("HTTPStatus = %d, want 200", res.HTTPStatus)
	}
}
