package fetch

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// RobotsDecision is one robots.txt answer.
type RobotsDecision struct {
	Allowed bool
	Reason  string
}

// ErrRobotsUnavailable is a transient robots.txt failure (timeout, 429, 5xx or
// a network error). It is deliberately an error rather than a cached
// disallow: the fetch is retried with backoff, and a broken robots endpoint
// never quarantines the source.
var ErrRobotsUnavailable = errors.New("fetch: robots.txt unavailable")

const (
	// robotsMaxBodyBytes caps a robots.txt response.
	robotsMaxBodyBytes = 512 << 10
	// robotsErrorTTL is how long a transient robots failure is remembered
	// before another attempt. It is an error, not a decision.
	robotsErrorTTL = time.Minute
)

type robotsRule struct {
	allow   bool
	pattern string
	length  int
	re      *regexp.Regexp
}

type robotsGroup struct {
	agents []string
	rules  []robotsRule
}

type robotsEntry struct {
	groups  []robotsGroup
	expires time.Time
	// err is a transient failure. While cached, checks return it so callers
	// back off instead of hammering the host.
	err error
}

// RobotsCache fetches and caches robots.txt per scheme://host. The default TTL
// is one hour. One fetch per host is shared by every caller (singleflight), and
// the body is size-capped.
type RobotsCache struct {
	client   *http.Client
	ua       string
	timeout  time.Duration
	ttl      time.Duration
	errorTTL time.Duration
	now      func() time.Time
	log      *slog.Logger
	mu       sync.Mutex
	entries  map[string]robotsEntry
	calls    map[string]*robotsCall
	maxHosts int
}

type robotsCall struct {
	done  chan struct{}
	entry robotsEntry
}

// NewRobotsCache builds a cache with the default HTTP transport.
func NewRobotsCache(ua string, timeout time.Duration, log *slog.Logger) *RobotsCache {
	return NewRobotsCacheWithTransport(ua, timeout, log, nil)
}

// NewRobotsCacheWithTransport is NewRobotsCache with an injected transport
// (a test TLS server, or a deployment with a proxy).
func NewRobotsCacheWithTransport(ua string, timeout time.Duration, log *slog.Logger, transport http.RoundTripper) *RobotsCache {
	if ua == "" {
		ua = DefaultUserAgent
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("fetch: too many robots.txt redirects")
			}
			if req.URL.Scheme == "http" && via[0].URL.Scheme == "https" {
				return errors.New("fetch: refusing https->http robots.txt redirect")
			}
			return nil
		},
	}
	return &RobotsCache{
		client:   client,
		ua:       ua,
		timeout:  timeout,
		ttl:      time.Hour,
		errorTTL: robotsErrorTTL,
		now:      time.Now,
		log:      log,
		entries:  make(map[string]robotsEntry),
		calls:    make(map[string]*robotsCall),
		maxHosts: 2048,
	}
}

// Check reports whether the path may be fetched. A real Disallow blocks; a
// transient failure returns ErrRobotsUnavailable (retryable). A 4xx means "no
// robots" and allows the path.
func (c *RobotsCache) Check(ctx context.Context, u *url.URL) (RobotsDecision, error) {
	key := u.Scheme + "://" + u.Host
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && c.now().Before(entry.expires) {
		c.mu.Unlock()
		return entry.decide(u, c.ua)
	}
	if call, ok := c.calls[key]; ok {
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.entry.decide(u, c.ua)
		case <-ctx.Done():
			return RobotsDecision{}, ctx.Err()
		}
	}
	call := &robotsCall{done: make(chan struct{})}
	c.calls[key] = call
	c.mu.Unlock()

	// The fetch is detached from one caller's cancellation so followers of the
	// singleflight are not punished when the leader's request is cancelled.
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
	entry := c.fetch(fetchCtx, key)
	cancel()

	c.mu.Lock()
	if len(c.entries) >= c.maxHosts {
		now := c.now()
		for k, v := range c.entries {
			if now.After(v.expires) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.maxHosts {
			// Still full: drop an arbitrary entry rather than grow forever.
			for k := range c.entries {
				delete(c.entries, k)
				break
			}
		}
	}
	c.entries[key] = entry
	call.entry = entry
	close(call.done)
	delete(c.calls, key)
	c.mu.Unlock()
	return entry.decide(u, c.ua)
}

func (e robotsEntry) decide(u *url.URL, ua string) (RobotsDecision, error) {
	if e.err != nil {
		return RobotsDecision{}, e.err
	}
	if len(e.groups) == 0 {
		return RobotsDecision{Allowed: true, Reason: "no rules"}, nil
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	matched := matchGroups(e.groups, ua)
	if len(matched) == 0 {
		return RobotsDecision{Allowed: true, Reason: "no matching user-agent group"}, nil
	}
	var best robotsRule
	found := false
	for _, group := range matched {
		for _, rule := range group.rules {
			if !rule.re.MatchString(path) {
				continue
			}
			if !found || rule.length > best.length || (rule.length == best.length && rule.allow) {
				best = rule
				found = true
			}
		}
	}
	if !found {
		return RobotsDecision{Allowed: true, Reason: "no matching rule"}, nil
	}
	if best.allow {
		return RobotsDecision{Allowed: true, Reason: "allow " + best.pattern}, nil
	}
	return RobotsDecision{Allowed: false, Reason: "disallow " + best.pattern}, nil
}

func (c *RobotsCache) fetch(ctx context.Context, base string) robotsEntry {
	now := c.now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/robots.txt", nil)
	if err != nil {
		return robotsEntry{err: fmt.Errorf("%w: %v", ErrRobotsUnavailable, err), expires: now.Add(c.errorTTL)}
	}
	req.Header.Set("User-Agent", c.ua)
	resp, err := c.client.Do(req)
	if err != nil {
		c.log.Debug("robots.txt unreachable; retry later", "host", base, "err", err)
		return robotsEntry{err: fmt.Errorf("%w: %v", ErrRobotsUnavailable, err), expires: now.Add(c.errorTTL)}
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode <= 299:
		body, err := io.ReadAll(io.LimitReader(resp.Body, robotsMaxBodyBytes+1))
		if err != nil {
			return robotsEntry{err: fmt.Errorf("%w: read: %v", ErrRobotsUnavailable, err), expires: now.Add(c.errorTTL)}
		}
		if len(body) > robotsMaxBodyBytes {
			return robotsEntry{err: fmt.Errorf("%w: body exceeds %d bytes", ErrRobotsUnavailable, robotsMaxBodyBytes), expires: now.Add(c.errorTTL)}
		}
		return robotsEntry{groups: parseRobots(bytes.NewReader(body)), expires: now.Add(c.ttl)}
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		c.log.Debug("robots.txt transient error; retry later", "host", base, "status", resp.StatusCode)
		return robotsEntry{
			err:     fmt.Errorf("%w: HTTP %d", ErrRobotsUnavailable, resp.StatusCode),
			expires: now.Add(c.errorTTL),
		}
	default:
		// 4xx (other than 429): no robots file, everything is allowed.
		return robotsEntry{expires: now.Add(c.ttl)}
	}
}

// parseRobots reads the group/rule subset of RFC 9309 that matters here,
// including `*` wildcards and a trailing `$` anchor.
func parseRobots(r io.Reader) []robotsGroup {
	var groups []robotsGroup
	var current *robotsGroup
	inAgents := false
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			inAgents = false
			continue
		}
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:colon]))
		value := strings.TrimSpace(line[colon+1:])
		switch key {
		case "user-agent":
			if !inAgents {
				groups = append(groups, robotsGroup{})
				current = &groups[len(groups)-1]
				inAgents = true
			}
			current.agents = append(current.agents, strings.ToLower(value))
		case "allow", "disallow":
			if current == nil {
				continue
			}
			inAgents = false
			if value == "" {
				continue
			}
			rule, ok := compileRobotsRule(key == "allow", value)
			if ok {
				current.rules = append(current.rules, rule)
			}
		default:
			inAgents = false
		}
	}
	return groups
}

// compileRobotsRule turns a robots path pattern into a matcher. `*` matches any
// sequence; a trailing `$` anchors the end of the path.
func compileRobotsRule(allow bool, pattern string) (robotsRule, bool) {
	length := len(pattern)
	anchored := strings.HasSuffix(pattern, "$")
	if anchored {
		pattern = strings.TrimSuffix(pattern, "$")
	}
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			b.WriteString(".*")
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
		}
	}
	if anchored {
		b.WriteString("$")
	}
	re, err := regexp.Compile(b.String())
	if err != nil {
		return robotsRule{}, false
	}
	return robotsRule{allow: allow, pattern: pattern, length: length, re: re}, true
}

func matchGroups(groups []robotsGroup, ua string) []robotsGroup {
	product := strings.ToLower(strings.SplitN(ua, "/", 2)[0])
	var exact []robotsGroup
	var wildcard []robotsGroup
	for _, g := range groups {
		for _, agent := range g.agents {
			agent = strings.TrimSpace(agent)
			switch {
			case agent == "*":
				wildcard = append(wildcard, g)
			case product != "" && (agent == product || strings.HasPrefix(product, agent)):
				exact = append(exact, g)
			}
		}
	}
	if len(exact) > 0 {
		return exact
	}
	return wildcard
}
