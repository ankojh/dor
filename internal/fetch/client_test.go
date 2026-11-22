package fetch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientConditionalGET(t *testing.T) {
	const etag = `"v1"`
	const lastMod = "Fri, 20 Nov 2026 10:00:00 GMT"
	var mu sync.Mutex
	var seen []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", lastMod)
		_, _ = w.Write([]byte("<rss/>"))
	}))
	defer srv.Close()

	c := NewClient(ClientOptions{UserAgent: "TestBot/1.0 (+https://example.test)"})
	ctx := context.Background()

	first, err := c.Get(ctx, srv.URL, Conditional{})
	if err != nil {
		t.Fatal(err)
	}
	if first.NotModified || string(first.Body) != "<rss/>" || first.ETag != etag || first.LastModified != lastMod {
		t.Fatalf("first = %+v", first)
	}
	second, err := c.Get(ctx, srv.URL, Conditional{ETag: first.ETag, LastModified: first.LastModified})
	if err != nil {
		t.Fatalf("304 must not be an error: %v", err)
	}
	if !second.NotModified || second.Status != http.StatusNotModified || len(second.Body) != 0 {
		t.Fatalf("second = %+v", second)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("requests = %d", len(seen))
	}
	if seen[0].Get("If-None-Match") != "" || seen[0].Get("If-Modified-Since") != "" {
		t.Errorf("first request carried validators: %v", seen[0])
	}
	if seen[1].Get("If-None-Match") != etag || seen[1].Get("If-Modified-Since") != lastMod {
		t.Errorf("second request validators = %v", seen[1])
	}
	for _, h := range seen {
		if h.Get("User-Agent") != "TestBot/1.0 (+https://example.test)" {
			t.Errorf("user agent = %q", h.Get("User-Agent"))
		}
	}
}

func TestClientDefaultUserAgent(t *testing.T) {
	if got := NewClient(ClientOptions{}).UserAgent(); got != DefaultUserAgent {
		t.Errorf("default UA = %q", got)
	}
}

func TestClientStatusErrors(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusTooManyRequests, true},
		{http.StatusNotFound, false},
		{http.StatusForbidden, false},
		{http.StatusGone, false},
	}
	for _, tt := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tt.status)
		}))
		_, err := NewClient(ClientOptions{}).Get(context.Background(), srv.URL, Conditional{})
		srv.Close()
		var se *StatusError
		if !errors.As(err, &se) || se.Status != tt.status {
			t.Fatalf("status %d: err = %v", tt.status, err)
		}
		if IsRetryable(err) != tt.retryable || se.Retryable() != tt.retryable {
			t.Errorf("status %d retryable = %v, want %v", tt.status, IsRetryable(err), tt.retryable)
		}
	}
}

func TestClientReliefWeb403(t *testing.T) {
	body := fixture(t, "reliefweb_403.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	resp, err := NewClient(ClientOptions{}).Get(context.Background(), srv.URL, Conditional{})
	var se *StatusError
	if !errors.As(err, &se) || se.Status != http.StatusForbidden {
		t.Fatalf("err = %v, want 403 StatusError", err)
	}
	if IsRetryable(err) {
		t.Error("403 must not be retried (the agent should block the source)")
	}
	if resp == nil || !strings.Contains(string(resp.Body), "approved appname") {
		t.Errorf("error body not surfaced: %+v", resp)
	}
}

func TestClientErrorsAndLimits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 1000)))
	}))
	defer srv.Close()
	if _, err := NewClient(ClientOptions{MaxBodyBytes: 100}).Get(context.Background(), srv.URL, Conditional{}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized body err = %v", err)
	}
	if _, err := NewClient(ClientOptions{}).Get(context.Background(), "ftp://example.test/x", Conditional{}); err == nil {
		t.Error("non-http scheme must be rejected")
	}
	if IsRetryable(nil) || IsRetryable(context.Canceled) || IsRetryable(ErrRobotsDisallowed) {
		t.Error("nil, cancel and robots denials are not retryable")
	}
	if !IsRetryable(errors.New("connection reset")) {
		t.Error("network errors are retryable")
	}
}

func TestClientTimeoutOnHangingServer(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	start := time.Now()
	_, err := NewClient(ClientOptions{Timeout: 150 * time.Millisecond}).Get(context.Background(), srv.URL, Conditional{})
	if err == nil || !IsRetryable(err) {
		t.Fatalf("hang err = %v, want retryable timeout", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("timeout took %v", time.Since(start))
	}
}

func robotsServer(t *testing.T, status int, body string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			if hits != nil {
				hits.Add(1)
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func robotsAllowed(t *testing.T, c *RobotsCache, base, path string) bool {
	t.Helper()
	u, err := url.Parse(base + path)
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.Check(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	return d.Allowed
}

func TestRobotsRules(t *testing.T) {
	const body = `# comment
User-agent: *
Disallow: /private
Allow: /private/public
Disallow: /tie
Allow: /tie
Disallow: /q?

User-agent: OtherBot
Disallow: /
`
	srv := robotsServer(t, 200, body, nil)
	c := NewRobotsCache("DorBot/0.1 (+https://example.test)", time.Second, nil)
	tests := []struct {
		path string
		want bool
	}{
		{"/", true},
		{"/news/a", true},
		{"/private", false},
		{"/private/x", false},
		{"/private/public/a", true}, // longest match wins
		{"/tie/x", true},            // equal length: allow wins
		{"/q?a=1", false},
	}
	for _, tt := range tests {
		if got := robotsAllowed(t, c, srv.URL, tt.path); got != tt.want {
			t.Errorf("%s allowed = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestRobotsSpecificGroupBeatsWildcard(t *testing.T) {
	const body = `User-agent: *
Disallow: /

User-agent: dorbot
Disallow: /admin
`
	srv := robotsServer(t, 200, body, nil)
	c := NewRobotsCache("DorBot/0.1", time.Second, nil)
	if !robotsAllowed(t, c, srv.URL, "/news") {
		t.Error("specific group allows /news even though * disallows everything")
	}
	if robotsAllowed(t, c, srv.URL, "/admin/x") {
		t.Error("specific group disallows /admin")
	}
	// A different agent only sees the wildcard group.
	other := NewRobotsCache("SomeoneElse/1.0", time.Second, nil)
	if robotsAllowed(t, other, srv.URL, "/news") {
		t.Error("wildcard disallow-all must apply to other agents")
	}
}

func TestRobotsConsecutiveUserAgentsShareRules(t *testing.T) {
	srv := robotsServer(t, 200, "User-agent: a\nUser-agent: dorbot\nDisallow: /x\n", nil)
	c := NewRobotsCache("DorBot/1", time.Second, nil)
	if robotsAllowed(t, c, srv.URL, "/x/y") {
		t.Error("stacked user-agent lines must share the rule block")
	}
}

func TestRobotsWildcardAndAnchor(t *testing.T) {
	const body = `User-agent: *
Disallow: /*/rss.xml
Disallow: /exact$
Disallow: /prefix*
Allow: /prefix/ok
`
	srv := robotsServer(t, 200, body, nil)
	c := NewRobotsCache("DorBot/0.1", time.Second, nil)
	tests := []struct {
		path string
		want bool
	}{
		{"/updates/rss.xml", false}, // wildcard prefix
		{"/a/b/rss.xml", false},     // wildcard spans a slash
		{"/rss.xml", true},          // pattern needs a leading segment
		{"/exact", false},           // $ anchors the end
		{"/exact/more", true},       // anchored pattern does not match past it
		{"/prefix/anything", false}, // prefix wildcard
		{"/prefix/ok", true},        // longer Allow wins
		{"/prefix/ok/deeper", true}, // still longer than /prefix*
	}
	for _, tt := range tests {
		if got := robotsAllowed(t, c, srv.URL, tt.path); got != tt.want {
			t.Errorf("%s allowed = %v, want %v", tt.path, got, tt.want)
		}
	}
}

// TestRobotsReliefWebRules uses the real reliefweb.int robots.txt captured on
// 2026-11-20. The site disallows /*/rss.xml, which must block
// /updates/rss.xml without blocking the rest of the site.
func TestRobotsReliefWebRules(t *testing.T) {
	body := fixture(t, "reliefweb_robots.txt")
	srv := robotsServer(t, 200, string(body), nil)
	c := NewRobotsCache("DorBot/0.1 (+https://github.com/ankojh/dor)", time.Second, nil)
	if robotsAllowed(t, c, srv.URL, "/updates/rss.xml") {
		t.Error("/updates/rss.xml must be disallowed by /*/rss.xml")
	}
	if !robotsAllowed(t, c, srv.URL, "/topics/health") {
		t.Error("/topics/health must remain allowed")
	}
}

func TestRobotsStatusHandling(t *testing.T) {
	for _, status := range []int{404, 403, 410} {
		srv := robotsServer(t, status, "User-agent: *\nDisallow:\n", nil)
		c := NewRobotsCache("DorBot/1", time.Second, nil)
		if !robotsAllowed(t, c, srv.URL, "/a") {
			t.Errorf("HTTP %d must mean no robots (allow)", status)
		}
	}
	for _, status := range []int{429, 500, 503} {
		srv := robotsServer(t, status, "", nil)
		c := NewRobotsCache("DorBot/1", time.Second, nil)
		u, _ := url.Parse(srv.URL + "/a")
		_, err := c.Check(context.Background(), u)
		if !errors.Is(err, ErrRobotsUnavailable) {
			t.Errorf("HTTP %d err = %v, want ErrRobotsUnavailable", status, err)
		}
		if !IsRetryable(err) {
			t.Errorf("HTTP %d robots failure must be retryable", status)
		}
	}
	t.Run("unreachable is retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		base := srv.URL
		srv.Close()
		c := NewRobotsCache("DorBot/1", 500*time.Millisecond, nil)
		u, _ := url.Parse(base + "/a")
		_, err := c.Check(context.Background(), u)
		if !errors.Is(err, ErrRobotsUnavailable) || !IsRetryable(err) {
			t.Fatalf("err = %v, want retryable ErrRobotsUnavailable", err)
		}
	})
}

func TestRobotsCacheTTL(t *testing.T) {
	var hits atomic.Int32
	srv := robotsServer(t, 200, "User-agent: *\nDisallow: /no\n", &hits)
	c := NewRobotsCache("DorBot/1", time.Second, nil)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		robotsAllowed(t, c, srv.URL, "/a")
	}
	if hits.Load() != 1 {
		t.Fatalf("robots fetched %d times within the TTL, want 1", hits.Load())
	}
	now = now.Add(c.ttl + time.Second)
	robotsAllowed(t, c, srv.URL, "/a")
	if hits.Load() != 2 {
		t.Fatalf("robots fetched %d times after expiry, want 2", hits.Load())
	}
}

func TestRobotsErrorTTLIsShorter(t *testing.T) {
	var hits atomic.Int32
	srv := robotsServer(t, 503, "", &hits)
	c := NewRobotsCache("DorBot/1", time.Second, nil)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	u, _ := url.Parse(srv.URL + "/a")
	for i := 0; i < 2; i++ {
		if _, err := c.Check(context.Background(), u); !errors.Is(err, ErrRobotsUnavailable) {
			t.Fatalf("err = %v", err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d within error TTL", hits.Load())
	}
	now = now.Add(c.errorTTL + time.Second)
	if _, err := c.Check(context.Background(), u); !errors.Is(err, ErrRobotsUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("hits = %d after error TTL", hits.Load())
	}
}

func TestRobotsSingleflight(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			hits.Add(1)
			<-release
			_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := NewRobotsCache("DorBot/1", 2*time.Second, nil)
	u, _ := url.Parse(srv.URL + "/a")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Check(context.Background(), u)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if hits.Load() != 1 {
		t.Fatalf("robots fetched %d times, want one singleflight fetch", hits.Load())
	}
}

func TestRobotsBodyCap(t *testing.T) {
	big := "User-agent: *\nDisallow: /" + strings.Repeat("a", robotsMaxBodyBytes)
	srv := robotsServer(t, 200, big, nil)
	c := NewRobotsCache("DorBot/1", time.Second, nil)
	u, _ := url.Parse(srv.URL + "/a")
	if _, err := c.Check(context.Background(), u); !errors.Is(err, ErrRobotsUnavailable) {
		t.Fatalf("oversized robots.txt err = %v, want ErrRobotsUnavailable", err)
	}
}

func TestClientHonorsRobotsDisallow(t *testing.T) {
	var secretHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret\n"))
		case "/secret/feed.xml":
			secretHits.Add(1)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{
		UserAgent: "DorBot/0.1",
		Robots:    NewRobotsCache("DorBot/0.1", time.Second, nil),
	})
	_, err := c.Get(context.Background(), srv.URL+"/secret/feed.xml", Conditional{})
	if !errors.Is(err, ErrRobotsDisallowed) {
		t.Fatalf("err = %v, want ErrRobotsDisallowed", err)
	}
	if IsRetryable(err) {
		t.Error("robots denial must not be retryable")
	}
	if secretHits.Load() != 0 {
		t.Error("the disallowed path was fetched anyway")
	}
	if resp, err := c.Get(context.Background(), srv.URL+"/open/feed.xml", Conditional{}); err != nil || string(resp.Body) != "ok" {
		t.Errorf("allowed path: %v %+v", err, resp)
	}
}

func TestClientRetriesWhenRobotsUnavailable(t *testing.T) {
	for _, status := range []int{429, 503} {
		var pageHits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/robots.txt" {
				w.WriteHeader(status)
				return
			}
			pageHits.Add(1)
		}))
		c := NewClient(ClientOptions{Robots: NewRobotsCache("DorBot/0.1", time.Second, nil)})
		_, err := c.Get(context.Background(), srv.URL+"/feed", Conditional{})
		srv.Close()
		if !errors.Is(err, ErrRobotsUnavailable) {
			t.Fatalf("HTTP %d err = %v, want ErrRobotsUnavailable", status, err)
		}
		if !IsRetryable(err) {
			t.Errorf("HTTP %d robots failure must be retryable", status)
		}
		if pageHits.Load() != 0 {
			t.Errorf("HTTP %d: page fetched although robots was unavailable", status)
		}
	}
}

func TestClientRejectsHTTPSDowngradeRedirect(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("plain"))
	}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/x", http.StatusFound)
	}))
	defer tlsSrv.Close()
	c := NewClient(ClientOptions{Transport: tlsSrv.Client().Transport})
	_, err := c.Get(context.Background(), tlsSrv.URL+"/feed", Conditional{})
	if err == nil || !strings.Contains(err.Error(), "https->http") {
		t.Fatalf("err = %v, want an https->http rejection", err)
	}
}

func TestClientCrossHostRedirectRechecksRobots(t *testing.T) {
	secretHits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret\n"))
			return
		}
		secretHits++
		_, _ = w.Write([]byte("secret"))
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		http.Redirect(w, r, target.URL+"/secret/feed", http.StatusFound)
	}))
	defer source.Close()
	c := NewClient(ClientOptions{Robots: NewRobotsCache("DorBot/0.1", time.Second, nil)})
	_, err := c.Get(context.Background(), source.URL+"/feed", Conditional{})
	if !errors.Is(err, ErrRobotsDisallowed) {
		t.Fatalf("err = %v, want ErrRobotsDisallowed for the redirect target", err)
	}
	if secretHits != 0 {
		t.Error("the cross-host redirect target was fetched despite robots")
	}
}

func TestClientSameHostRedirectRechecksRobots(t *testing.T) {
	privateHits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /private\n"))
		case "/feed":
			http.Redirect(w, r, "/private/feed", http.StatusFound)
		default:
			privateHits++
			_, _ = w.Write([]byte("private"))
		}
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{Robots: NewRobotsCache("DorBot/0.1", time.Second, nil)})
	_, err := c.Get(context.Background(), srv.URL+"/feed", Conditional{})
	if !errors.Is(err, ErrRobotsDisallowed) {
		t.Fatalf("err = %v, want ErrRobotsDisallowed for the same-host redirect target", err)
	}
	if privateHits != 0 {
		t.Error("the same-host redirect target was fetched despite robots")
	}
}

func TestClientBodyCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 200)))
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{MaxBodyBytes: 1 << 20})
	_, err := c.Get(context.Background(), srv.URL, Conditional{}, WithMaxBytes(50))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if resp, err := c.Get(context.Background(), srv.URL, Conditional{}, WithMaxBytes(500)); err != nil || len(resp.Body) != 200 {
		t.Fatalf("wider cap: %v %d", err, len(resp.Body))
	}
}

func TestClientRedirectLimit(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/loop", http.StatusFound)
	}))
	defer srv.Close()
	c := NewClient(ClientOptions{})
	_, err := c.Get(context.Background(), srv.URL+"/loop", Conditional{})
	if err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("err = %v, want a redirect limit error", err)
	}
}
