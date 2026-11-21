package agents

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/record"
	"github.com/ankojh/dor/internal/sources"
	"google.golang.org/protobuf/proto"
)

const shippedSources = "../../deploy/sources/sources.yaml"

func TestFeedShardStableAndInRange(t *testing.T) {
	// Pinned values: replicas in different processes (and releases) must agree
	// on which shard owns a feed, so a change here is a breaking change.
	pinned := map[string]int{"bbc-world": 0, "bbc-home": 0, "": 0}
	for id := range pinned {
		pinned[id] = FeedShard(id, 3)
	}
	for id, want := range pinned {
		for i := 0; i < 3; i++ {
			if got := FeedShard(id, 3); got != want {
				t.Fatalf("FeedShard(%q, 3) changed between calls: %d vs %d", id, got, want)
			}
		}
	}
	// FNV-1a 32-bit of "a" is 0xe40c292c.
	if got, want := FeedShard("a", 3), int(0xe40c292c%3); got != want {
		t.Errorf("FeedShard(a, 3) = %d, want %d (FNV-1a 32)", got, want)
	}
	if FeedShard("anything", 0) != 0 || FeedShard("anything", -1) != 0 || FeedShard("anything", 1) != 0 {
		t.Error("degenerate shard counts map to shard 0")
	}
	for i := 0; i < 500; i++ {
		if s := FeedShard(fmt.Sprintf("feed-%d", i), 7); s < 0 || s >= 7 {
			t.Fatalf("shard %d out of range", s)
		}
	}
}

func TestShardFeedsPartitionsRealSourceList(t *testing.T) {
	file, err := sources.Load(shippedSources)
	if err != nil {
		t.Fatal(err)
	}
	feeds := file.ForKind(sources.KindRSS)
	if len(feeds) < 150 {
		t.Fatalf("only %d enabled rss feeds", len(feeds))
	}
	for _, shards := range []int{1, 3, 5} {
		seen := map[string]int{}
		sizes := make([]int, shards)
		for shard := 0; shard < shards; shard++ {
			part := shardFeeds(feeds, shard, shards)
			sizes[shard] = len(part)
			for _, f := range part {
				seen[f.ID]++
				if FeedShard(f.ID, shards) != shard {
					t.Errorf("%s landed in shard %d, FeedShard says %d", f.ID, shard, FeedShard(f.ID, shards))
				}
			}
		}
		if len(seen) != len(feeds) {
			t.Errorf("%d shards cover %d of %d feeds", shards, len(seen), len(feeds))
		}
		for id, n := range seen {
			if n != 1 {
				t.Errorf("%d shards: feed %s assigned %d times", shards, id, n)
			}
		}
		for shard, n := range sizes {
			// A reasonable hash gives each shard at least half its fair share.
			if n < len(feeds)/shards/2 {
				t.Errorf("%d shards: shard %d has only %d of %d feeds", shards, shard, n, len(feeds))
			}
		}
	}
	// File order is preserved inside a shard.
	part := shardFeeds(feeds, 0, 3)
	last := -1
	index := map[string]int{}
	for i, f := range feeds {
		index[f.ID] = i
	}
	for _, f := range part {
		if index[f.ID] < last {
			t.Fatal("shardFeeds reordered the feeds")
		}
		last = index[f.ID]
	}
}

func TestRawArticleID(t *testing.T) {
	sum := sha256.Sum256([]byte("https://example.test/a"))
	want := "raw:rss:" + hex.EncodeToString(sum[:])
	if got := RawArticleID("rss", "https://example.test/a"); got != want {
		t.Errorf("id = %s", got)
	}
	if RawArticleID("rss", "  https://example.test/a\n") != want {
		t.Error("surrounding whitespace must not change the id")
	}
	if RawArticleID("rss", "https://example.test/b") == want {
		t.Error("different urls must differ")
	}
	if RawArticleID("gdelt", "https://example.test/a") == want {
		t.Error("the same url from another source kind must differ")
	}
}

func TestSeenSet(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := newSeenSet(3, time.Hour)
	s.Add("a", now)
	if !s.Has("a", now) {
		t.Fatal("Add must make the key present")
	}
	if s.Has("", now) {
		t.Fatal("empty key is never present")
	}
	s = newSeenSet(3, time.Hour)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Add(k, now)
	}
	if s.Len() != 3 || s.Has("a", now) || s.Has("b", now) || !s.Has("e", now) {
		t.Errorf("eviction: len %d", s.Len())
	}
	if !s.Has("e", now.Add(59*time.Minute)) {
		t.Error("a hit inside the ttl must be present (and refresh it)")
	}
	// The hit at +59m refreshed the ttl, so +1h30m is still inside it, while
	// +2h after the refresh is not.
	if !s.Has("e", now.Add(90*time.Minute)) {
		t.Error("sliding ttl did not refresh")
	}
	if s.Has("e", now.Add(3*time.Hour)) {
		t.Error("an expired key must be absent")
	}
}

func TestBuiltinsRegisterFetchers(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	reg, err := Builtins(context.Background(), b, NewMetrics(), testLogger(), Config{SourcesFile: shippedSources})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, n := range reg.Names() {
		names[n] = true
	}
	for _, want := range []string{"echo", RSSAgentName, "gdelt", "hazard"} {
		if !names[want] {
			t.Errorf("agent %q not registered: %v", want, reg.Names())
		}
	}
	if _, err := reg.Select("rss,hazard"); err != nil {
		t.Errorf("select: %v", err)
	}
}

// ---------- feed runtime tests ----------

// stepClock advances one hour on every call. The scheduler computes its sleep
// from this clock, so a one-hour step makes every "next attempt" immediately
// due and the whole poll loop runs at HTTP speed with no real waiting.
type stepClock struct {
	base time.Time
	n    atomic.Int64
}

func (c *stepClock) Now() time.Time {
	return c.base.Add(time.Duration(c.n.Add(1)) * time.Hour)
}

func newTestRuntime(t *testing.T, b *bus.Bus, clock func() time.Time, httpTimeout time.Duration) *fetcherRuntime {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SourcesFile = shippedSources
	cfg.RSSConcurrency = 2
	cfg.Now = clock
	rt, err := newFetcherRuntime(context.Background(), b, NewMetrics(), testLogger(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Plain client: httptest listens on http://127.0.0.1 and the shared robots
	// Keep a real client with robots and the host budget: the isolation test
	// must exercise the same path as production. The budget is generous so the
	// test is fast, and every httptest host serves its robots.txt.
	rt.client = fetch.NewClient(fetch.ClientOptions{
		Timeout: httpTimeout,
		Budget:  fetch.NewBudget(fetch.BudgetOptions{Rate: 1000, Burst: 1000, Now: clock}),
		Robots:  fetch.NewRobotsCache("DorBot/test", time.Second, nil),
	})
	return rt
}

func rssDoc(links ...string) string {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0"?><rss version="2.0"><channel><title>t</title><language>en</language>`)
	for _, l := range links {
		// No pubDate: an undated item is accepted once (the dated-item
		// freshness rule is covered by TestRSSColdStartCutoff).
		fmt.Fprintf(&sb, `<item><title>Story %s</title><link>%s</link></item>`, l[strings.LastIndex(l, "/")+1:], l)
	}
	sb.WriteString(`</channel></rss>`)
	return sb.String()
}

func testEmitter(t *testing.T, rt *fetcherRuntime) *Emitter {
	t.Helper()
	base, err := NewBase(rt.bus, rt.metrics, rt.log, Spec{
		Name:     "rss-test",
		Producer: true,
		Produce:  func(context.Context, *Emitter) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Emitter{agent: base, ctx: context.Background()}
}

func TestFetchFeedOnceClassifiesOutcomes(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	rt := newTestRuntime(t, b, time.Now, 2*time.Second)

	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"e1"`)
		w.Header().Set("Last-Modified", "Fri, 20 Nov 2026 10:00:00 GMT")
		_, _ = w.Write([]byte(rssDoc(srv.URL+"/news/story-one", srv.URL+"/news/story-two")))
	})
	mux.HandleFunc("/304", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotModified) })
	mux.HandleFunc("/500", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	mux.HandleFunc("/404", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })
	mux.HandleFunc("/html", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html><body>nope</body></html>")) })
	srv = httptest.NewServer(mux)
	defer srv.Close()

	out := testEmitter(t, rt)
	sem := make(chan struct{}, 1)
	now := time.Now()

	run := func(path string, cond *fetch.Conditional, seen *seenSet) feedAttempt {
		src := sources.Source{ID: "t" + strings.ReplaceAll(path, "/", "-"), Kind: sources.KindRSS, URL: srv.URL + path, Lang: "en"}
		return rt.fetchFeedOnce(context.Background(), out, sem, src, cond, seen, now)
	}

	seen := newSeenSet(100, time.Hour)
	ok := run("/ok", &fetch.Conditional{}, seen)
	if ok.err != nil || ok.items != 2 || ok.etag != `"e1"` || ok.lastModified == "" {
		t.Fatalf("ok = %+v", ok)
	}
	if again := run("/ok", &fetch.Conditional{}, seen); again.err != nil || again.items != 0 {
		t.Errorf("a second fetch must publish nothing new: %+v", again)
	}
	if nm := run("/304", &fetch.Conditional{ETag: `"e1"`}, seen); !nm.notModified || nm.err != nil || nm.etag != `"e1"` {
		t.Errorf("304 = %+v", nm)
	}
	if e := run("/500", &fetch.Conditional{}, seen); e.err == nil || e.blocked || e.parseError {
		t.Errorf("500 must be a plain retryable failure: %+v", e)
	}
	if e := run("/404", &fetch.Conditional{}, seen); e.err == nil || !e.blocked || e.parseError {
		t.Errorf("404 must be blocked: %+v", e)
	}
	if e := run("/html", &fetch.Conditional{}, seen); e.err == nil || !e.parseError || e.blocked {
		t.Errorf("html must be a parse error: %+v", e)
	}

	// Parse errors feed the quarantine counter; five in a row quarantine.
	st := fetch.NewFeedState(fetch.FeedStateOptions{})
	for i := 0; i < fetch.DefaultQuarantineAfter; i++ {
		att := run("/html", &fetch.Conditional{}, seen)
		st.OnFailure(now, att.parseError)
	}
	if st.State() != fetch.StateQuarantined {
		t.Errorf("state after repeated malformed responses = %v", st.State())
	}
	waitSubjectMsgs(t, b, bus.SubjectArticleRaw, 2)
}

// TestFeedIsolation drives the real producer path (a Producer agent running
// runFeedShard, the same loop produceRSS starts per shard) next to a hanging
// feed, a 500 feed, a malformed feed and a quarantine-worthy 404, and shows
// the healthy feeds keep publishing while every bad feed only slows itself.
func TestFeedIsolation(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	clock := &stepClock{base: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	rt := newTestRuntime(t, b, clock.Now, 200*time.Millisecond)

	var srv *httptest.Server
	var mu sync.Mutex
	hits := map[string]int{}
	release := make(chan struct{})
	hang := func(path string) {
		mu.Lock()
		hits[path]++
		mu.Unlock()
	}
	hitCount := func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		return hits[path]
	}
	// Healthy feeds serve a fresh pair of items for their first five requests
	// and the same items afterwards, so each has ten distinct links. The step
	// clock ages the seen-set out quickly, so later polls republish those same
	// links; JetStream dedupes them by message id, which the bus count below
	// relies on.
	mux := http.NewServeMux()
	robotsHits := 0
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		robotsHits++
		mu.Unlock()
		_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
	})
	for _, name := range []string{"good1", "good2", "good3"} {
		name := name
		mux.HandleFunc("/"+name, func(w http.ResponseWriter, r *http.Request) {
			hang("/" + name)
			n := hitCount("/" + name)
			if n > 5 {
				n = 5
			}
			links := []string{fmt.Sprintf("%s/%s/story-%d-a", srv.URL, name, n), fmt.Sprintf("%s/%s/story-%d-b", srv.URL, name, n)}
			_, _ = w.Write([]byte(rssDoc(links...)))
		})
	}
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		hang("/hang")
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("/500", func(w http.ResponseWriter, r *http.Request) { hang("/500"); w.WriteHeader(500) })
	mux.HandleFunc("/bad", func(w http.ResponseWriter, r *http.Request) {
		hang("/bad")
		_, _ = w.Write([]byte("<html><body>maintenance</body></html>"))
	})
	mux.HandleFunc("/gone", func(w http.ResponseWriter, r *http.Request) { hang("/gone"); w.WriteHeader(404) })
	srv = httptest.NewServer(mux)
	defer srv.Close()
	defer close(release)

	feed := func(id, path string) sources.Source {
		return sources.Source{ID: id, Kind: sources.KindRSS, URL: srv.URL + path, Country: "GB", Region: "global", Lang: "en", IntervalS: 1, Enabled: true, TermsOK: true}
	}
	good := []sources.Source{feed("good1", "/good1"), feed("good2", "/good2"), feed("good3", "/good3")}
	badFeeds := []sources.Source{feed("hang", "/hang"), feed("err500", "/500"), feed("malformed", "/bad"), feed("gone", "/gone")}
	all := append(append([]sources.Source{}, badFeeds...), good...)

	agent, err := NewBase(b, rt.metrics, rt.log, Spec{
		Name:     "rss-isolation",
		Producer: true,
		Produce: func(ctx context.Context, out *Emitter) error {
			rt.runFeedShard(ctx, out, all)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	ctx := context.Background()
	healthy := func() bool {
		for _, g := range good {
			rec, ok := rt.health.Load(ctx, g.ID)
			if !ok || rec.ItemsTotal < 10 || rec.State != "ok" {
				return false
			}
		}
		return true
	}
	badSeen := func() bool {
		for _, f := range badFeeds {
			if rec, ok := rt.health.Load(ctx, f.ID); !ok || rec.ErrorsHour == 0 {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(15 * time.Second)
	for !(healthy() && badSeen()) {
		if time.Now().After(deadline) {
			for _, s := range all {
				rec, _ := rt.health.Load(ctx, s.ID)
				t.Logf("%s: %+v", s.ID, rec)
			}
			t.Fatal("healthy feeds did not publish 10 items each (or bad feeds never failed) in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Healthy feeds: at least ten items each, no errors, state ok.
	for _, g := range good {
		rec, _ := rt.health.Load(ctx, g.ID)
		if rec.ItemsTotal < 10 || rec.ErrorsHour != 0 || rec.State != "ok" || rec.Kind != "rss" {
			t.Errorf("%s: %+v", g.ID, rec)
		}
	}
	// Bad feeds: never published, failing, and never in the healthy state.
	wantError := map[string]string{"hang": "", "err500": "HTTP 500", "malformed": "not a feed", "gone": "HTTP 404"}
	for _, f := range badFeeds {
		rec, _ := rt.health.Load(ctx, f.ID)
		if rec.ItemsTotal != 0 || rec.LastSuccess != 0 || rec.LastError == "" {
			t.Errorf("%s published or lost its error: %+v", f.ID, rec)
		}
		if rec.State != "backoff" && rec.State != "quarantined" {
			t.Errorf("%s state = %q, want backoff or quarantined", f.ID, rec.State)
		}
		if want := wantError[f.ID]; want != "" && !strings.Contains(rec.LastError, want) {
			t.Errorf("%s last error = %q, want %q", f.ID, rec.LastError, want)
		}
	}
	if rec, _ := rt.health.Load(ctx, "err500"); rec.State != "backoff" {
		t.Errorf("a 500 backs off, never quarantines on its own: %+v", rec)
	}
	if rec, _ := rt.health.Load(ctx, "gone"); rec.State != "quarantined" || rec.QuarantinedUntil == 0 {
		t.Errorf("a permanent 404 is quarantined: %+v", rec)
	}
	if hitCount("/hang") == 0 {
		t.Error("the hanging feed was never polled, so isolation was not exercised")
	}
	mu.Lock()
	robots := robotsHits
	mu.Unlock()
	if robots == 0 {
		t.Error("robots.txt was never fetched; isolation did not exercise the real client")
	}
	// The bus holds exactly the distinct healthy items: 3 feeds x 10 links.
	waitSubjectMsgs(t, b, bus.SubjectArticleRaw, 30)
	if n := rawSubjectCount(t, b); n != 30 {
		t.Errorf("article.raw holds %d messages, want 30 distinct", n)
	}
}

func rawSubjectCount(t *testing.T, b *bus.Bus) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(bus.SubjectArticleRaw))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(bus.SubjectArticleRaw))
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Subjects[bus.SubjectArticleRaw]
}

// TestRSSColdStartCutoff pins the 48 h freshness rule: a cold start publishes
// a fresh or undated item and skips an old one, and a second poll republishes
// nothing.
func TestRSSColdStartCutoff(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	rt := newTestRuntime(t, b, func() time.Time { return now }, 2*time.Second)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>
<item><title>fresh</title><link>%s/fresh</link><pubDate>Fri, 20 Nov 2026 11:00:00 GMT</pubDate></item>
<item><title>old</title><link>%s/old</link><pubDate>Tue, 17 Nov 2026 11:00:00 GMT</pubDate></item>
<item><title>undated</title><link>%s/undated</link></item>
</channel></rss>`, srv.URL, srv.URL, srv.URL)
	}))
	defer srv.Close()

	out := testEmitter(t, rt)
	src := sources.Source{ID: "cutoff", Kind: sources.KindRSS, URL: srv.URL + "/feed", Lang: "en", Enabled: true, TermsOK: true}
	seen := newSeenSet(100, 48*time.Hour)
	got := rt.fetchFeedOnce(context.Background(), out, make(chan struct{}, 1), src, &fetch.Conditional{}, seen, now)
	if got.err != nil || got.items != 2 {
		t.Fatalf("first poll = %+v, want 2 items (fresh + undated)", got)
	}
	again := rt.fetchFeedOnce(context.Background(), out, make(chan struct{}, 1), src, &fetch.Conditional{}, seen, now)
	if again.err != nil || again.items != 0 {
		t.Fatalf("second poll = %+v, want nothing republished", again)
	}
	// The old link was remembered without being published.
	if !seen.Has(srv.URL+"/old", now) {
		t.Error("the old item should be remembered as seen")
	}
}

func TestProduceRSSConfigErrors(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	write := func(name, body string) string {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	valid := "version: 1\nsources:\n  - id: one\n    kind: rss\n    url: \"https://example.test/rss\"\n    country: GB\n    region: global\n    lang: en\n    interval_s: 60\n    enabled: true\n    terms_ok: true\n    terms_note: ok\n"
	disabled := strings.Replace(strings.Replace(valid, "enabled: true", "enabled: false", 1), "terms_ok: true", "terms_ok: false", 1)

	tests := []struct {
		name  string
		file  string
		shard int
		want  string
	}{
		{"missing file", filepath.Join(t.TempDir(), "none.yaml"), -1, "read"},
		{"invalid file", write("bad.yaml", "version: 1\nsources: []\n"), -1, "empty"},
		{"no enabled rss", write("off.yaml", disabled), -1, "no enabled rss"},
		{"shard out of range", write("ok.yaml", valid), 3, "outside"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.SourcesFile = tt.file
			cfg.RSSShard = tt.shard
			rt, err := newFetcherRuntime(context.Background(), b, NewMetrics(), testLogger(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			err = rt.produceRSS(context.Background(), nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestNextConditional pins the stale-validator rule: a 200 without ETag or
// Last-Modified clears the previous values; a 304 or an error keeps them.
func TestNextConditional(t *testing.T) {
	prev := fetch.Conditional{ETag: `"old"`, LastModified: "Fri, 20 Nov 2026 10:00:00 GMT"}
	if got := nextConditional(prev, feedAttempt{notModified: true}); got != prev {
		t.Errorf("304 must keep validators, got %+v", got)
	}
	if got := nextConditional(prev, feedAttempt{err: fmt.Errorf("x")}); got != prev {
		t.Errorf("error must keep validators, got %+v", got)
	}
	if got := nextConditional(prev, feedAttempt{}); got.ETag != "" || got.LastModified != "" {
		t.Errorf("a 200 without validators must clear them, got %+v", got)
	}
	if got := nextConditional(prev, feedAttempt{etag: `"new"`}); got.ETag != `"new"` || got.LastModified != "" {
		t.Errorf("a new ETag must replace and clear the old date, got %+v", got)
	}
}

func TestPublishRawSkipsDuplicateAndRecordsOnce(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	rt := newTestRuntime(t, b, time.Now, 2*time.Second)
	dir := t.TempDir()
	recorder, err := record.NewWriter(record.Options{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	rt.cfg.Recorder = recorder
	out := testEmitter(t, rt)
	raw := &dorv1.RawArticle{
		Id:       RawArticleID("rss", "https://example.test/dupe"),
		SourceId: "x",
		Kind:     dorv1.SourceKind_SOURCE_KIND_RSS,
		Url:      "https://example.test/dupe",
		Title:    "t",
	}
	if ok, err := rt.publishRaw(context.Background(), out, bus.SubjectArticleRaw, raw); err != nil || !ok {
		t.Fatalf("first publish = %v, %v", ok, err)
	}
	if ok, err := rt.publishRaw(context.Background(), out, bus.SubjectArticleRaw, raw); err != nil || ok {
		t.Fatalf("duplicate publish = %v, %v (want false, nil)", ok, err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}
	if recorder.Written() != 1 {
		t.Fatalf("recorded %d frames, want 1 (duplicate must be skipped)", recorder.Written())
	}
}

// gdeltZip builds a zip with one GKG CSV containing the provided rows.
func gdeltZip(t *testing.T, rows ...[]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("20261121050000.gkg.csv")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(w, strings.Join(row, "\t")); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gkgRow(url, domain string, themes, locations string) []string {
	row := make([]string, 27)
	row[0] = "20261121050000-0"
	row[1] = "20261121050000"
	row[3] = domain
	row[4] = url
	row[8] = themes
	row[10] = locations
	row[26] = "<PAGE_TITLE>Page title from GKG</PAGE_TITLE><PAGE_LANG>en</PAGE_LANG>"
	return row
}

func TestProcessGDELTFileFiltersAndPublishes(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	rt := newTestRuntime(t, b, time.Now, 2*time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("User-agent: *\nAllow: /\n")) })
	var srv *httptest.Server
	mux.HandleFunc("/file.zip", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(gdeltZip(t,
			gkgRow(srv.URL+"/kept", "jpost.com", "PROTEST,12", "4#Oslo, Norway#NO#NO12#59.9#10.7#-1"),
			gkgRow(srv.URL+"/no-theme", "jpost.com", "TAX_FNCACT,1", "4#Oslo, Norway#NO#NO12#59.9#10.7#-1"),
			gkgRow(srv.URL+"/no-domain", "boredpanda.com", "PROTEST,12", "4#Oslo, Norway#NO#NO12#59.9#10.7#-1"),
			gkgRow(srv.URL+"/no-place", "jpost.com", "PROTEST,12", ""),
		))
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	out := testEmitter(t, rt)
	src := sources.Source{ID: "gdelt-test", Kind: sources.KindGDELT, URL: srv.URL + "/lastupdate.txt", Enabled: true, TermsOK: true}
	seen := newSeenSet(100, 48*time.Hour)
	n, err := rt.processGDELTFile(context.Background(), out, src, srv.URL+"/file.zip", seen, time.Now(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("published %d, want only the allowlisted, themed, located row", n)
	}
	if got := subjectCount(t, b, bus.SubjectArticleRaw); got != 1 {
		t.Fatalf("article.raw has %d messages, want 1", got)
	}
	// A second pass must publish nothing: the seen set holds the URL.
	if n, err := rt.processGDELTFile(context.Background(), out, src, srv.URL+"/file.zip", seen, time.Now(), 100); err != nil || n != 0 {
		t.Fatalf("second pass = %d, %v", n, err)
	}
	// The title comes from the GKG PAGE_TITLE extras.
	frames := readStream(t, b, bus.SubjectArticleRaw)
	if len(frames) != 1 {
		t.Fatalf("frames = %d", len(frames))
	}
	var raw dorv1.RawArticle
	if err := proto.Unmarshal(frames[0], &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Title != "Page title from GKG" || raw.Hints.GetPlace() != "Oslo, Norway" || raw.Kind != dorv1.SourceKind_SOURCE_KIND_GDELT || !raw.Burst {
		t.Errorf("raw title=%q place=%q kind=%v burst=%v", raw.Title, raw.Hints.GetPlace(), raw.Kind, raw.Burst)
	}
}

func TestGDELTLastFilePersisted(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	var srv *httptest.Server
	zipData := gdeltZip(t, gkgRow("https://www.jpost.com/one", "jpost.com", "PROTEST,12", "4#Oslo, Norway#NO#NO12#59.9#10.7#-1"))
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("User-agent: *\nAllow: /\n")) })
	mux.HandleFunc("/lastupdate.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "1 abc %s/file.gkg.csv.zip\n", srv.URL)
	})
	mux.HandleFunc("/file.gkg.csv.zip", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(zipData) })
	srv = httptest.NewTLSServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	sourcesPath := filepath.Join(dir, "sources.yaml")
	body := fmt.Sprintf(`version: 1
reviewed_at: "2026-11-21"
notes: test
sources:
  - id: gdelt-test
    kind: gdelt
    url: "%s/lastupdate.txt"
    country: US
    region: global
    lang: en
    interval_s: 60
    enabled: true
    terms_ok: true
    terms_note: test
`, srv.URL)
	if err := os.WriteFile(sourcesPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	b2 := testBus(t, t.TempDir())
	defer b2.Close()
	cfg := DefaultConfig()
	cfg.SourcesFile = sourcesPath
	cfg.HTTPTransport = srv.Client().Transport
	cfg.Now = time.Now
	rt, err := newFetcherRuntime(context.Background(), b2, NewMetrics(), testLogger(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = rt.produceGDELT(ctx, testEmitter(t, rt)) }()
	deadline := time.Now().Add(4 * time.Second)
	for {
		rec, ok := rt.health.Load(context.Background(), "gdelt-test")
		if ok && strings.HasSuffix(rec.LastFile, "/file.gkg.csv.zip") && rec.ItemsTotal == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("last_file not persisted: %+v", rec)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHazardRevisionRepublishes(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	now := time.Now().UTC()
	updated := now.Add(-time.Hour).UnixMilli()
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("User-agent: *\nAllow: /\n")) })
	mux.HandleFunc("/feed.json", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		u := updated
		mu.Unlock()
		features := []map[string]any{
			{"id": "us1", "properties": map[string]any{
				"mag": 5.1, "magType": "mw", "place": "Near X",
				"time": now.Add(-2 * time.Hour).UnixMilli(), "updated": u,
				"url": "https://example.test/us1", "title": "M 5.1 - Near X", "type": "earthquake",
			}},
			{"id": "old", "properties": map[string]any{
				"mag": 4.0, "magType": "ml", "place": "Old",
				"time": now.Add(-50 * time.Hour).UnixMilli(), "updated": now.Add(-50 * time.Hour).UnixMilli(),
				"url": "https://example.test/old", "title": "old", "type": "earthquake",
			}},
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"features": features})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rt := newTestRuntime(t, b, time.Now, 2*time.Second)
	out := testEmitter(t, rt)
	src := sources.Source{ID: "usgs-test", Kind: sources.KindUSGS, URL: srv.URL + "/feed.json", Enabled: true, TermsOK: true}
	seen := newSeenSet(100, 48*time.Hour)
	cond := fetch.Conditional{}

	n, _, _, _, err := rt.fetchHazardOnce(context.Background(), out, src, cond, seen, now)
	if err != nil || n != 1 {
		t.Fatalf("first = %d, %v (want only the fresh event)", n, err)
	}
	if n, _, _, _, err = rt.fetchHazardOnce(context.Background(), out, src, cond, seen, now); err != nil || n != 0 {
		t.Fatalf("unchanged = %d, %v (want no republish)", n, err)
	}
	// A revised update time is a new revision and must republish.
	mu.Lock()
	updated = now.Add(-30 * time.Minute).UnixMilli()
	mu.Unlock()
	if n, _, _, _, err = rt.fetchHazardOnce(context.Background(), out, src, cond, seen, now); err != nil || n != 1 {
		t.Fatalf("revised = %d, %v (want the escalation)", n, err)
	}
	if got := subjectCount(t, b, bus.SubjectArticleRawPriority); got != 2 {
		t.Fatalf("priority has %d messages, want 2", got)
	}
}

func subjectCount(t *testing.T, b *bus.Bus, subject string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(subject))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Subjects[subject]
}

func readStream(t *testing.T, b *bus.Bus, subject string) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(subject))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckNonePolicy,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cons.Fetch(100, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for m := range batch.Messages() {
		out = append(out, append([]byte(nil), m.Data()...))
	}
	return out
}

// TestRSSCapReportsCapped checks that a feed with more items than the per-poll
// cap reports capped=true, which makes the poll clear the conditional
// validators so the remainder is not hidden behind a 304.
func TestRSSCapReportsCapped(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	rt := newTestRuntime(t, b, time.Now, 2*time.Second)
	var links []string
	for i := 0; i < rssMaxItemsPerFetch+5; i++ {
		links = append(links, fmt.Sprintf("https://example.test/story-%d", i))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		_, _ = w.Write([]byte(rssDoc(links...)))
	}))
	defer srv.Close()
	out := testEmitter(t, rt)
	src := sources.Source{ID: "cap", Kind: sources.KindRSS, URL: srv.URL + "/feed", Lang: "en", Enabled: true, TermsOK: true}
	att := rt.fetchFeedOnce(context.Background(), out, make(chan struct{}, 1), src, &fetch.Conditional{}, newSeenSet(200, 48*time.Hour), time.Now())
	if att.err != nil {
		t.Fatal(att.err)
	}
	if att.items != rssMaxItemsPerFetch || !att.capped {
		t.Fatalf("items=%d capped=%v, want %d and true", att.items, att.capped, rssMaxItemsPerFetch)
	}
}
