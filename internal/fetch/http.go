// Package fetch implements the source adapters the fetcher agents use: a
// polite HTTP client (honest User-Agent, robots.txt, per-host token buckets,
// conditional GET, timeouts), parsers for RSS/Atom, GDELT, USGS, GDACS, NASA
// EONET and ReliefWeb, and source-health bookkeeping (backoff, quarantine,
// metrics, KV).
//
// Parsers take an io.Reader and return plain items, so adapter tests run on
// recorded fixtures with no network.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultUserAgent is the honest crawler identity. Override it with
// DOR_HTTP_USER_AGENT when a deploy has a real contact URL.
const DefaultUserAgent = "DorBot/0.1 (+https://github.com/ankojh/dor)"

// DefaultMaxBodyBytes caps one response. Feeds are small; GDELT zips are a few
// MB; the cap keeps a hostile or broken upstream from exhausting memory.
const DefaultMaxBodyBytes = 64 << 20

// ErrRobotsDisallowed means robots.txt forbids fetching this path.
var ErrRobotsDisallowed = errors.New("fetch: robots.txt disallows this path")

// ErrTooLarge means the response exceeded ClientOptions.MaxBodyBytes.
var ErrTooLarge = errors.New("fetch: response exceeds the size limit")

// StatusError is a non-2xx response. 429 and 5xx are worth retrying; other
// 4xx usually are not.
type StatusError struct {
	URL    string
	Status int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("fetch: %s returned HTTP %d", e.URL, e.Status)
}

// Retryable reports whether the status is transient.
func (e *StatusError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

// Conditional carries the HTTP validators from the previous fetch.
type Conditional struct {
	ETag         string
	LastModified string
}

// Response is one completed fetch.
type Response struct {
	URL          string
	Status       int
	Body         []byte
	ETag         string
	LastModified string
	FetchedAt    time.Time
	NotModified  bool
}

// ClientOptions configures a Client.
type ClientOptions struct {
	// UserAgent is the crawler identity. Empty uses DefaultUserAgent.
	UserAgent string
	// Timeout bounds one request including body read. Default 20 s.
	Timeout time.Duration
	// MaxBodyBytes caps the body size. Default DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// Budget is the shared per-host token bucket. Nil means no throttling.
	Budget *Budget
	// Robots is the robots.txt cache. Nil means robots is not consulted.
	Robots *RobotsCache
	// Transport overrides the HTTP transport (tests, proxies).
	Transport http.RoundTripper
}

// GetOption customises one request.
type GetOption func(*getOptions)

type getOptions struct {
	maxBytes int64
}

// WithMaxBytes caps one response body, overriding the client default.
func WithMaxBytes(n int64) GetOption {
	return func(o *getOptions) {
		if n > 0 {
			o.maxBytes = n
		}
	}
}

// Client is a polite HTTP fetcher shared by every fetcher agent in a process.
type Client struct {
	ua      string
	timeout time.Duration
	maxBody int64
	budget  *Budget
	robots  *RobotsCache
	client  *http.Client
}

// NewClient builds a client. It never returns nil.
func NewClient(opts ClientOptions) *Client {
	if opts.UserAgent == "" {
		opts.UserAgent = DefaultUserAgent
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Second
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = DefaultMaxBodyBytes
	}
	transport := opts.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	c := &Client{
		ua:      opts.UserAgent,
		timeout: opts.Timeout,
		maxBody: opts.MaxBodyBytes,
		budget:  opts.Budget,
		robots:  opts.Robots,
	}
	c.client = &http.Client{
		Timeout:   opts.Timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("fetch: too many redirects")
			}
			last := via[len(via)-1]
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("fetch: refusing redirect to scheme %q", req.URL.Scheme)
			}
			if req.URL.Scheme == "http" && last.URL.Scheme == "https" {
				return errors.New("fetch: refusing https->http redirect")
			}
			if req.URL.Host != last.URL.Host {
				// A cross-host redirect needs its own robots and host budget.
				if c.robots != nil {
					decision, err := c.robots.Check(req.Context(), req.URL)
					if err != nil {
						return err
					}
					if !decision.Allowed {
						return fmt.Errorf("%w: %s (%s)", ErrRobotsDisallowed, req.URL.Path, decision.Reason)
					}
				}
				if c.budget != nil {
					if err := c.budget.Take(req.Context(), req.URL.Host); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	return c
}

// UserAgent returns the identity sent with every request.
func (c *Client) UserAgent() string { return c.ua }

// Get fetches rawURL, honouring robots.txt, the shared host budget and the
// conditional validators. A 304 response is returned with NotModified set and
// no error. opts can cap the body for one kind of source.
func (c *Client) Get(ctx context.Context, rawURL string, cond Conditional, opts ...GetOption) (*Response, error) {
	goOpts := getOptions{maxBytes: c.maxBody}
	for _, opt := range opts {
		opt(&goOpts)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("fetch: parse url %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("fetch: unsupported scheme %q", u.Scheme)
	}
	if c.robots != nil {
		decision, err := c.robots.Check(ctx, u)
		if err != nil {
			return nil, err
		}
		if !decision.Allowed {
			return nil, fmt.Errorf("%w: %s (%s)", ErrRobotsDisallowed, u.Path, decision.Reason)
		}
	}
	if c.budget != nil {
		if err := c.budget.Take(ctx, u.Host); err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch: build request: %w", err)
	}
	req.Header.Set("User-Agent", c.ua)
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml, application/json, */*;q=0.5")
	if cond.ETag != "" {
		req.Header.Set("If-None-Match", cond.ETag)
	}
	if cond.LastModified != "" {
		req.Header.Set("If-Modified-Since", cond.LastModified)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %s: %w", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	out := &Response{
		URL:          resp.Request.URL.String(),
		Status:       resp.StatusCode,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		FetchedAt:    time.Now().UTC(),
	}
	if resp.StatusCode == http.StatusNotModified {
		out.NotModified = true
		return out, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, goOpts.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("fetch: read %s: %w", rawURL, err)
	}
	if int64(len(body)) > goOpts.maxBytes {
		return nil, fmt.Errorf("%w: %s", ErrTooLarge, rawURL)
	}
	out.Body = body
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return out, &StatusError{URL: rawURL, Status: resp.StatusCode}
	}
	return out, nil
}

// IsRetryable reports whether a fetch error is worth retrying with backoff.
// Robots denials and permanent 4xx responses are not.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRobotsDisallowed) || errors.Is(err, context.Canceled) {
		return false
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.Retryable()
	}
	return true
}

// TitleFromURL derives a readable headline from a URL slug. GDELT and some
// feeds carry no title; the slug is publisher-supplied, so this invents
// nothing. It returns "" when the path has no usable slug.
func TitleFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	path := strings.TrimSuffix(u.Path, "/")
	segments := strings.Split(path, "/")
	for i := len(segments) - 1; i >= 0 && i >= len(segments)-3; i-- {
		seg := segments[i]
		seg = strings.TrimSuffix(seg, ".html")
		seg = strings.TrimSuffix(seg, ".htm")
		seg = strings.TrimSuffix(seg, ".cms")
		seg = strings.TrimSuffix(seg, ".amp")
		if unescaped, err := url.PathUnescape(seg); err == nil {
			seg = unescaped
		}
		seg = strings.TrimSpace(seg)
		if len(seg) < 12 || looksOpaque(seg) {
			continue
		}
		title := strings.NewReplacer("-", " ", "_", " ", "+", " ").Replace(seg)
		title = strings.Join(strings.Fields(title), " ")
		if len([]rune(title)) < 12 {
			continue
		}
		r := []rune(title)
		r[0] = []rune(strings.ToUpper(string(r[0])))[0]
		return truncateRunes(string(r), 180)
	}
	return ""
}

// looksOpaque rejects numeric ids and ids with too few letters to read.
func looksOpaque(seg string) bool {
	letters := 0
	for _, r := range seg {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			letters++
		}
	}
	return letters < 6
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	cut := r[:max]
	for i := len(cut) - 1; i > max/2; i-- {
		if cut[i] == ' ' {
			return strings.TrimSpace(string(cut[:i])) + "…"
		}
	}
	return strings.TrimSpace(string(cut)) + "…"
}
