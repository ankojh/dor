// Package httpguard enforces the browser-origin boundary for the local REST
// surfaces and provides small in-memory admission limiters. Dor is loopback
// only in v1, but a page on another site can still reach loopback; every
// browser request is therefore checked against the configured origin
// allowlist, and mutations additionally reject cross-site fetch metadata and
// non-JSON content types.
package httpguard

import (
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

// Allowlist matches browser origins against host patterns. Patterns use the
// same host:port form as the WebSocket OriginPatterns, e.g. "localhost:*" or
// "app.example.com". A request whose Origin host equals the request Host is
// always allowed, mirroring the WebSocket accept rule.
type Allowlist struct {
	patterns []string
}

// NewAllowlist builds an allowlist from comma-separated-pattern style entries.
// Empty entries are ignored.
func NewAllowlist(patterns []string) *Allowlist {
	clean := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern = strings.ToLower(strings.TrimSpace(pattern)); pattern != "" {
			clean = append(clean, pattern)
		}
	}
	return &Allowlist{patterns: clean}
}

// Allows reports whether the Origin header is acceptable for a request to
// requestHost. A missing or "null" origin is not accepted; callers treat a
// missing Origin as a non-browser client.
func (a *Allowlist) Allows(origin, requestHost string) bool {
	if SameOrigin(origin, requestHost) {
		return true
	}
	u, err := originURL(origin)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Host)
	for _, pattern := range a.patterns {
		if matched, err := path.Match(pattern, host); err == nil && matched {
			return true
		}
	}
	return false
}

// SameOrigin reports whether origin is an http(s) origin whose host equals
// requestHost. It is the rule used for requests served by the same listener.
func SameOrigin(origin, requestHost string) bool {
	u, err := originURL(origin)
	return err == nil && strings.EqualFold(u.Host, requestHost)
}

func originURL(origin string) (*url.URL, error) {
	origin = strings.TrimSpace(origin)
	if origin == "" || strings.EqualFold(origin, "null") {
		return nil, fmt.Errorf("empty or null origin")
	}
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid origin %q", origin)
	}
	return u, nil
}

// Middleware applies the origin checks, answers CORS preflight for allowed
// origins, and echoes only the matching origin. It never emits a wildcard.
func (a *Allowlist) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if !a.Allows(origin, r.Host) {
				Deny(w, http.StatusForbidden, "origin not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		crossSite := strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site")
		// A CORS call from an explicitly allowed origin is legitimate even
		// across sites (the web app may use its own API origin). If there is
		// no Origin, the metadata is the only evidence and a cross-site
		// mutation is refused.
		if isMutation(r.Method) && crossSite && origin == "" {
			Deny(w, http.StatusForbidden, "cross-site request refused")
			return
		}
		// An embed, script, beacon or form has no Origin header and a
		// non-fetch Sec-Fetch-Mode. Reject those for every API path: GET
		// /api/text can start paid generation, and no legitimate cross-site
		// use reads these endpoints that way. A legitimate CORS or
		// same-origin fetch keeps its Origin or a fetch mode.
		if origin == "" && strings.HasPrefix(r.URL.Path, "/api/") {
			mode := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Mode")))
			if crossSite || mode == "no-cors" || mode == "navigate" {
				Deny(w, http.StatusForbidden, "cross-site request refused")
				return
			}
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Dor-Ops-Token")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireJSON rejects a mutation whose body is not declared as JSON. Browsers
// may send text/plain across origins without a preflight, so accepting it as
// JSON would undo the origin boundary. A request with no body is allowed to
// omit the header.
func RequireJSON(w http.ResponseWriter, r *http.Request) bool {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		if r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
			Deny(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
			return false
		}
		return true
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || (mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json")) {
		Deny(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	return true
}

// Deny writes a small JSON error. It does not add or remove CORS headers:
// an origin refusal happens before Allow-Origin is set, while an admission
// refusal to an already-allowed origin keeps that origin readable.
func Deny(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": message})
}

func isMutation(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// Limiter is a bounded in-memory token bucket keyed by client host. It is
// deliberately small: the gateway is loopback-only in v1 and the limit exists
// to stop a runaway page or script, not to model a multi-tenant quota.
type Limiter struct {
	mu      sync.Mutex
	rate    float64 // tokens per second
	burst   float64
	buckets map[string]*bucket
	now     func() time.Time

	// maxBuckets bounds memory if a caller ever sees many distinct hosts.
	maxBuckets int
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewLimiter builds a limiter allowing perMinute sustained requests with the
// given burst. Values below one request per minute still admit the burst.
func NewLimiter(perMinute float64, burst int) *Limiter {
	if perMinute < 0 {
		perMinute = 0
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		rate:       perMinute / 60,
		burst:      float64(burst),
		buckets:    make(map[string]*bucket),
		now:        time.Now,
		maxBuckets: 4096,
	}
}

// Allow consumes one token for key and reports whether it was available.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= l.maxBuckets {
			l.evictLocked(now)
		}
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (l *Limiter) evictLocked(now time.Time) {
	cutoff := now.Add(-10 * time.Minute)
	for key, b := range l.buckets {
		if b.last.Before(cutoff) {
			delete(l.buckets, key)
		}
	}
	// A flood of fresh hosts should not grow without bound; dropping the map
	// only forgets recent history, it never bypasses the burst cap per key.
	if len(l.buckets) >= l.maxBuckets {
		l.buckets = make(map[string]*bucket)
	}
}

// Middleware rejects requests over the limit with 429 and Retry-After.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.Allow(ClientKey(r.RemoteAddr)) {
			w.Header().Set("Retry-After", "60")
			Deny(w, http.StatusTooManyRequests, "too many requests; retry shortly")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ClientKey returns the host portion of a RemoteAddr, falling back to the
// whole value when it has no port.
func ClientKey(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	if remoteAddr == "" {
		return "unknown"
	}
	return remoteAddr
}
