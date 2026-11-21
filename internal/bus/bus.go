package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

const (
	// DefaultNATSListen is the address of the embedded server in local mode.
	DefaultNATSListen = "127.0.0.1:4222"
	// DefaultStoreDir is where the embedded server keeps JetStream state.
	DefaultStoreDir = "./.dor/nats"

	// DefaultMaxAckPending caps unacknowledged messages per consumer.
	DefaultMaxAckPending = 256

	// DefaultDuplicateWindow is the documented processing-domain dedupe horizon.
	// Per-stream policies override it; kept as a constant for callers that need
	// the default value.
	DefaultDuplicateWindow = DefaultProcessingTTL
	// DefaultStreamMaxAge is the INGEST/ENRICH default. Kept for compatibility;
	// use StreamPolicy for per-domain values.
	DefaultStreamMaxAge = DefaultIngestMaxAge
)

// KV bucket names and their TTLs. Values are JSON or protobuf blobs written by
// agents; the bus only owns the bucket lifecycle. `budgets` is for short-lived
// rate-limit buckets, never for daily spend counters (M0·3 persists those
// durably).
const (
	KVBucketSeen         = "seen"
	KVBucketJoins        = "joins"
	KVBucketBudgets      = "budgets"
	KVBucketLiveState    = "live_state"
	KVBucketSourceHealth = "source_health"

	// KVLiveStateSnapshotKey holds the publisher's cached Snapshot binary;
	// gateway replicas serve it on hello/resync.
	KVLiveStateSnapshotKey = "snapshot"
	// KVPublisherSeqKey holds the publisher's last assigned delta sequence.
	KVPublisherSeqKey = "publisher.seq"

	KVSeenTTL         = 48 * time.Hour
	KVJoinsTTL        = 30 * time.Second
	KVBudgetsTTL      = time.Hour
	KVLiveStateTTL    = 48 * time.Hour
	KVSourceHealthTTL = 7 * 24 * time.Hour
)

// Header names shared by every agent.
const (
	// HeaderMsgID is the JetStream dedupe id. Every publish through Bus sets it.
	HeaderMsgID = "Nats-Msg-Id"
	// HeaderArticleID carries the article id so one trace follows an article
	// through the whole pipeline.
	HeaderArticleID = "Dor-Article-Id"
	// HeaderOriginalSubject/HeaderOriginalMsgID/HeaderError describe a message
	// copied to the DLQ.
	HeaderOriginalSubject = "Dor-Original-Subject"
	HeaderOriginalMsgID   = "Dor-Original-Msg-Id"
	HeaderError           = "Dor-Error"
)

// Options configures a Bus.
type Options struct {
	// URL connects to an existing server. Empty starts the embedded server.
	URL string
	// ListenAddr is the embedded server address (default 127.0.0.1:4222).
	ListenAddr string
	// StoreDir is the embedded server's JetStream file store.
	StoreDir string
	// Provision makes this client the provisioning owner on an external server.
	// Embedded mode always provisions. Ordinary clients never rewrite streams or
	// KV; they bind and validate.
	Provision bool
	// StreamPolicies overrides individual stream defaults. Zero fields keep the
	// default.
	StreamPolicies map[string]StreamPolicy
	// StreamMaxAge overrides MaxAge on every stream (legacy/dev use).
	StreamMaxAge time.Duration
	// DuplicateWindow overrides the dedupe window on every stream (legacy/dev
	// use).
	DuplicateWindow time.Duration
	// MaxAckPending is the default for consumers that do not set one.
	MaxAckPending int
	// Logger is optional.
	Logger *slog.Logger
}

// Bus is a connected NATS client plus, in local mode, the embedded server that
// backs it.
type Bus struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	srv      *server.Server
	opts     Options
	url      string
	owner    bool
	policies map[string]StreamPolicy

	streamMu sync.RWMutex
	streams  map[string]jetstream.Stream

	kvMu sync.Mutex
	kv   map[string]jetstream.KeyValue

	log        *slog.Logger
	closing    atomic.Bool
	closedCh   chan struct{}
	closedOnce sync.Once
}

// Connect starts the embedded server when opts.URL is empty, connects a NATS
// client, and then either provisions the stream/KV topology (owner) or binds
// and validates it (ordinary client).
func Connect(ctx context.Context, opts Options) (*Bus, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = DefaultNATSListen
	}
	if opts.StoreDir == "" {
		opts.StoreDir = DefaultStoreDir
	}
	if opts.MaxAckPending <= 0 {
		opts.MaxAckPending = DefaultMaxAckPending
	}

	b := &Bus{
		opts:     opts,
		owner:    opts.URL == "" || opts.Provision,
		streams:  make(map[string]jetstream.Stream),
		kv:       make(map[string]jetstream.KeyValue),
		log:      opts.Logger,
		closedCh: make(chan struct{}),
	}
	policies, err := b.resolvePolicies()
	if err != nil {
		return nil, err
	}
	b.policies = policies

	url := opts.URL
	if url == "" {
		srv, embeddedURL, err := startEmbedded(opts.ListenAddr, opts.StoreDir)
		if err != nil {
			return nil, err
		}
		b.srv = srv
		url = embeddedURL
	}
	b.url = url

	nc, err := nats.Connect(url,
		nats.Name("dor"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.Timeout(5*time.Second),
		nats.DrainTimeout(10*time.Second),
		nats.ClosedHandler(func(_ *nats.Conn) { b.signalClosed() }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if b.closing.Load() {
				return
			}
			b.log.Warn("nats error", "err", redactError(err, url))
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if b.closing.Load() {
				return
			}
			b.log.Warn("nats disconnected", "err", redactError(err, url))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			b.log.Info("nats reconnected", "url", RedactedURL(nc.ConnectedUrl()))
		}),
	)
	if err != nil {
		_ = b.shutdownEmbedded()
		return nil, fmt.Errorf("bus: connect %s: %w", RedactedURL(url), redactError(err, url))
	}
	b.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		_ = b.shutdownEmbedded()
		return nil, fmt.Errorf("bus: jetstream: %w", err)
	}
	b.js = js

	if b.owner {
		err = b.provisionAll(ctx)
	} else {
		err = b.bindAndValidate(ctx)
	}
	if err != nil {
		_ = b.Close()
		return nil, err
	}
	if b.owner {
		b.log.Info("bus provisioned", "nats_url", b.RedactedURL(), "streams", len(b.policies))
	}
	return b, nil
}

func (b *Bus) resolvePolicies() (map[string]StreamPolicy, error) {
	policies := DefaultStreamPolicies()
	for name, override := range b.opts.StreamPolicies {
		policy, ok := policies[name]
		if !ok {
			return nil, fmt.Errorf("bus: unknown stream policy override %q", name)
		}
		if len(override.Subjects) > 0 {
			policy.Subjects = append([]string(nil), override.Subjects...)
		}
		if override.Storage != 0 {
			policy.Storage = override.Storage
		}
		if override.MaxAge > 0 {
			policy.MaxAge = override.MaxAge
		}
		if override.MaxBytes > 0 {
			policy.MaxBytes = override.MaxBytes
		}
		if override.Duplicates > 0 {
			policy.Duplicates = override.Duplicates
		}
		policies[name] = policy
	}
	for name, policy := range policies {
		if b.opts.StreamMaxAge > 0 {
			policy.MaxAge = b.opts.StreamMaxAge
		}
		if b.opts.DuplicateWindow > 0 {
			policy.Duplicates = b.opts.DuplicateWindow
		}
		if policy.MaxAge <= 0 || policy.MaxBytes <= 0 {
			return nil, fmt.Errorf("bus: stream %s needs a positive MaxAge and MaxBytes", name)
		}
		if policy.Duplicates < 100*time.Millisecond {
			return nil, fmt.Errorf("bus: stream %s duplicate window must be >= 100ms", name)
		}
		if policy.Duplicates > policy.MaxAge {
			return nil, fmt.Errorf("bus: stream %s duplicate window %s is larger than max age %s", name, policy.Duplicates, policy.MaxAge)
		}
		if policy.Storage != jetstream.FileStorage && policy.Storage != jetstream.MemoryStorage {
			return nil, fmt.Errorf("bus: stream %s has unknown storage %v", name, policy.Storage)
		}
		policies[name] = policy
	}
	return policies, nil
}

func (b *Bus) provisionAll(ctx context.Context) error {
	var lastErr error
	deadline := time.Now().Add(15 * time.Second)
	for {
		lastErr = nil
		for _, name := range StreamOrder() {
			policy := b.policies[name]
			stream, err := b.js.CreateOrUpdateStream(ctx, policyConfig(name, policy))
			if err != nil {
				lastErr = fmt.Errorf("bus: provision stream %s: %w", name, err)
				break
			}
			b.streamMu.Lock()
			b.streams[name] = stream
			b.streamMu.Unlock()
		}
		if lastErr == nil {
			lastErr = b.provisionKV(ctx)
		}
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (b *Bus) bindAndValidate(ctx context.Context) error {
	for _, name := range StreamOrder() {
		stream, err := b.js.Stream(ctx, name)
		if err != nil {
			return fmt.Errorf("bus: stream %s is not provisioned: %w (start the owner with --provision or embedded mode)", name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("bus: inspect stream %s: %w", name, err)
		}
		b.warnPolicyMismatch(name, b.policies[name], info.Config)
		b.streamMu.Lock()
		b.streams[name] = stream
		b.streamMu.Unlock()
	}
	for _, bucket := range kvBuckets() {
		kv, err := b.js.KeyValue(ctx, bucket.name)
		if err != nil {
			return fmt.Errorf("bus: kv bucket %s is not provisioned: %w", bucket.name, err)
		}
		status, err := kv.Status(ctx)
		if err != nil {
			return fmt.Errorf("bus: inspect kv %s: %w", bucket.name, err)
		}
		if status.TTL() != bucket.ttl {
			b.log.Warn("kv TTL differs from provisioned default; keeping server policy",
				"bucket", bucket.name, "want", bucket.ttl, "got", status.TTL())
		}
		b.kvMu.Lock()
		b.kv[bucket.name] = kv
		b.kvMu.Unlock()
	}
	return nil
}

func (b *Bus) warnPolicyMismatch(name string, want StreamPolicy, got jetstream.StreamConfig) {
	if !sameStrings(want.Subjects, got.Subjects) {
		b.log.Warn("stream subjects differ from provisioned default; keeping server policy",
			"stream", name, "want", want.Subjects, "got", got.Subjects)
	}
	if want.Storage != got.Storage {
		b.log.Warn("stream storage differs from provisioned default; keeping server policy",
			"stream", name, "want", want.Storage, "got", got.Storage)
	}
	if want.MaxAge != got.MaxAge {
		b.log.Warn("stream max age differs from provisioned default; keeping server policy",
			"stream", name, "want", want.MaxAge, "got", got.MaxAge)
	}
	if want.MaxBytes != got.MaxBytes {
		b.log.Warn("stream max bytes differs from provisioned default; keeping server policy",
			"stream", name, "want", want.MaxBytes, "got", got.MaxBytes)
	}
	if want.Duplicates != got.Duplicates {
		b.log.Warn("stream duplicate window differs from provisioned default; keeping server policy",
			"stream", name, "want", want.Duplicates, "got", got.Duplicates)
	}
}

type kvSpec struct {
	name string
	ttl  time.Duration
}

func kvBuckets() []kvSpec {
	return []kvSpec{
		{KVBucketSeen, KVSeenTTL},
		{KVBucketJoins, KVJoinsTTL},
		{KVBucketBudgets, KVBudgetsTTL},
		{KVBucketLiveState, KVLiveStateTTL},
		{KVBucketSourceHealth, KVSourceHealthTTL},
	}
}

func (b *Bus) provisionKV(ctx context.Context) error {
	for _, spec := range kvBuckets() {
		kv, err := b.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:  spec.name,
			History: 1,
			TTL:     spec.ttl,
			Storage: jetstream.FileStorage,
		})
		if err != nil {
			return fmt.Errorf("bus: provision kv %s: %w", spec.name, err)
		}
		b.kvMu.Lock()
		b.kv[spec.name] = kv
		b.kvMu.Unlock()
	}
	return nil
}

func startEmbedded(listenAddr, storeDir string) (*server.Server, string, error) {
	host, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, "", fmt.Errorf("bus: bad embedded listen address %q: %w", listenAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, "", fmt.Errorf("bus: bad embedded port %q: %w", portStr, err)
	}
	// nats-server treats Port 0 as "use the default 4222", not "pick a free
	// port". Tests pass :0, so pick one ourselves and retry a few times if the
	// port was taken between the probe and the bind.
	attempts := 1
	if port == 0 {
		attempts = 5
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		bindPort := port
		if bindPort == 0 {
			bindPort, err = freePort(host)
			if err != nil {
				return nil, "", err
			}
		}
		srv, err := server.NewServer(&server.Options{
			Host:      host,
			Port:      bindPort,
			JetStream: true,
			StoreDir:  storeDir,
			NoLog:     true,
			NoSigs:    true,
		})
		if err != nil {
			return nil, "", fmt.Errorf("bus: create embedded nats server: %w", err)
		}
		go srv.Start()
		if srv.ReadyForConnections(10 * time.Second) {
			addr, ok := srv.Addr().(*net.TCPAddr)
			if !ok || addr == nil {
				srv.Shutdown()
				srv.WaitForShutdown()
				return nil, "", errors.New("bus: embedded nats server has no address")
			}
			urlHost := addr.IP.String()
			if urlHost == "" || urlHost == "0.0.0.0" || urlHost == "::" {
				urlHost = host
			}
			url := "nats://" + net.JoinHostPort(urlHost, strconv.Itoa(addr.Port))
			return srv, url, nil
		}
		srv.Shutdown()
		srv.WaitForShutdown()
		lastErr = fmt.Errorf("bus: embedded nats server did not become ready on %s:%d", host, bindPort)
		if port != 0 {
			break
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("bus: could not start embedded nats server on %s", listenAddr)
	}
	return nil, "", lastErr
}

func freePort(host string) (int, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, fmt.Errorf("bus: find free port on %s: %w", host, err)
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("bus: free port listener has no TCP address")
	}
	return addr.Port, nil
}

// URL returns the raw server URL, including credentials if configured. Use
// RedactedURL for anything that is logged or returned over HTTP.
func (b *Bus) URL() string { return b.url }

// RedactedURL returns the connected URL with userinfo removed.
func (b *Bus) RedactedURL() string { return RedactedURL(b.url) }

// RedactedURL strips userinfo from one or more comma-separated NATS URLs.
func RedactedURL(raw string) string {
	parts := strings.Split(raw, ",")
	for i, part := range parts {
		parts[i] = redactOneURL(strings.TrimSpace(part))
	}
	return strings.Join(parts, ",")
}

func redactOneURL(raw string) string {
	if raw == "" {
		return raw
	}
	if !strings.Contains(raw, "://") {
		// No scheme: userinfo can still appear as user:pass@host.
		if at := strings.LastIndex(raw, "@"); at >= 0 {
			return raw[at+1:]
		}
		return raw
	}
	if u, err := url.Parse(raw); err == nil {
		if u.User == nil {
			return raw
		}
		u.User = nil
		return u.String()
	}
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		// Unknown shape: drop everything before the last @ to be safe.
		if at := strings.LastIndex(raw, "@"); at >= 0 {
			return raw[at+1:]
		}
		return raw
	}
	prefix := raw[:schemeEnd+3]
	rest := raw[len(prefix):]
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		return prefix + rest[at+1:]
	}
	return raw
}

// urlUserInfo is the fallback for URL text that is not one of the configured
// components. It is restricted to the authority: an @ in a path or query is
// not userinfo and must be preserved.
var urlUserInfo = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://)[^/?#\s]*@`)

// redactText removes userinfo from URL-looking tokens in arbitrary text.
func redactText(text string) string {
	return urlUserInfo.ReplaceAllString(text, "$1")
}

// redactError removes credentials from an error. The configured server list is
// handled component by component, including the raw, strconv.Quote and
// url.Parse-normalized forms, because nats.Connect reports the offending
// component (sometimes quoted) rather than the whole list. The authority-only
// regex is a fallback for any URL-shaped text not covered by exact replacement.
func redactError(err error, rawURL string) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	for _, component := range strings.Split(rawURL, ",") {
		text = redactComponent(text, component)
	}
	text = redactText(text)
	if text == err.Error() {
		return err
	}
	return errors.New(text)
}

func redactComponent(text, component string) string {
	component = strings.TrimSpace(component)
	if component == "" {
		return text
	}
	redacted := RedactedURL(component)
	if redacted == component {
		return text
	}
	text = strings.ReplaceAll(text, strconv.Quote(component), strconv.Quote(redacted))
	text = strings.ReplaceAll(text, component, redacted)
	if parsed, err := url.Parse(component); err == nil {
		normalized := parsed.String()
		if normalized != component {
			normalizedRedacted := RedactedURL(normalized)
			text = strings.ReplaceAll(text, strconv.Quote(normalized), strconv.Quote(normalizedRedacted))
			text = strings.ReplaceAll(text, normalized, normalizedRedacted)
		}
	}
	return text
}

// NC returns the underlying NATS connection.
func (b *Bus) NC() *nats.Conn { return b.nc }

// JS returns the JetStream context.
func (b *Bus) JS() jetstream.JetStream { return b.js }

// Owner reports whether this client provisioned the streams and KV.
func (b *Bus) Owner() bool { return b.owner }

// Stream returns the dev ECHO stream. It exists for the M0 echo probes only;
// new code should use StreamNamed or StreamForSubject.
func (b *Bus) Stream() jetstream.Stream {
	b.streamMu.RLock()
	defer b.streamMu.RUnlock()
	return b.streams[StreamEcho]
}

// StreamName returns the dev ECHO stream name. Compatibility for the M0 probe.
func (b *Bus) StreamName() string { return StreamEcho }

// StreamNamed returns a provisioned stream handle.
func (b *Bus) StreamNamed(ctx context.Context, name string) (jetstream.Stream, error) {
	b.streamMu.RLock()
	stream, ok := b.streams[name]
	b.streamMu.RUnlock()
	if ok {
		return stream, nil
	}
	stream, err := b.js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("bus: stream %s: %w", name, err)
	}
	b.streamMu.Lock()
	b.streams[name] = stream
	b.streamMu.Unlock()
	return stream, nil
}

// StreamNames returns the provisioned stream names in deterministic order.
func (b *Bus) StreamNames() []string { return StreamOrder() }

// Policy returns the resolved policy for a stream.
func (b *Bus) Policy(name string) (StreamPolicy, bool) {
	policy, ok := b.policies[name]
	return policy, ok
}

// MaxAckPending returns the configured consumer default.
func (b *Bus) MaxAckPending() int { return b.opts.MaxAckPending }

// Connected reports whether the NATS client is currently connected.
func (b *Bus) Connected() bool { return b.nc != nil && b.nc.IsConnected() }

// Embedded reports whether this bus owns an embedded server.
func (b *Bus) Embedded() bool { return b.srv != nil }

// KV returns an existing bucket. It never creates or rewrites one.
func (b *Bus) KV(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	b.kvMu.Lock()
	kv, ok := b.kv[bucket]
	b.kvMu.Unlock()
	if ok {
		return kv, nil
	}
	kv, err := b.js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("bus: kv %s: %w", bucket, err)
	}
	b.kvMu.Lock()
	b.kv[bucket] = kv
	b.kvMu.Unlock()
	return kv, nil
}

// Close drains the client, waits for the closed callback, then shuts down the
// embedded server. Unacked messages are redelivered on the next start.
func (b *Bus) Close() error {
	if b.closing.Swap(true) {
		return nil
	}
	var errs []error
	if b.nc != nil {
		if err := b.nc.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			errs = append(errs, err)
		}
		select {
		case <-b.closedCh:
		case <-time.After(15 * time.Second):
			b.nc.Close()
			select {
			case <-b.closedCh:
			case <-time.After(5 * time.Second):
				errs = append(errs, errors.New("bus: NATS closed callback did not fire"))
			}
		}
	}
	if err := b.shutdownEmbedded(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (b *Bus) signalClosed() {
	b.closedOnce.Do(func() { close(b.closedCh) })
}

func (b *Bus) shutdownEmbedded() error {
	if b.srv == nil {
		return nil
	}
	b.srv.Shutdown()
	b.srv.WaitForShutdown()
	return nil
}

// PublishOptions control one publish.
type PublishOptions struct {
	MsgID   string
	Headers nats.Header
}

// PublishOption customises Publish.
type PublishOption func(*PublishOptions)

// WithMsgID sets the JetStream dedupe id. Agents that can be retried must
// derive a stable, stage-namespaced id from their input.
func WithMsgID(id string) PublishOption {
	return func(o *PublishOptions) { o.MsgID = id }
}

// WithHeaders adds headers. They are merged over the default headers.
func WithHeaders(h nats.Header) PublishOption {
	return func(o *PublishOptions) {
		if o.Headers == nil {
			o.Headers = nats.Header{}
		}
		for k, vs := range h {
			for _, v := range vs {
				o.Headers.Add(k, v)
			}
		}
	}
}

// Publish writes one message. It always sets Nats-Msg-Id; when the caller does
// not supply one a unique id is generated. Callers that can be retried must
// supply a stable id namespaced by stage (for example "clean:<articleID>").
func (b *Bus) Publish(ctx context.Context, subject string, data []byte, opts ...PublishOption) (*jetstream.PubAck, error) {
	po := PublishOptions{}
	for _, opt := range opts {
		opt(&po)
	}
	if po.MsgID == "" {
		po.MsgID = nuid.Next()
	}
	h := nats.Header{}
	for k, vs := range po.Headers {
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	h.Set(HeaderMsgID, po.MsgID)
	msg := &nats.Msg{Subject: subject, Data: data, Header: h}
	ack, err := b.js.PublishMsg(ctx, msg, jetstream.WithMsgID(po.MsgID))
	if err != nil {
		return nil, fmt.Errorf("bus: publish %s: %w", subject, err)
	}
	return ack, nil
}

// ConsumerOptions configure a durable pull consumer.
type ConsumerOptions struct {
	Durable        string
	FilterSubject  string
	FilterSubjects []string
	MaxAckPending  int
	AckWait        time.Duration
	MaxDeliver     int // -1 means unlimited; 0 defaults to -1
	BackOff        []time.Duration
	DeliverPolicy  jetstream.DeliverPolicy
}

// EnsureConsumer creates or updates this agent's durable consumer on the
// stream that owns its filters. Creating a consumer is allowed for ordinary
// clients; streams and KV are not.
func (b *Bus) EnsureConsumer(ctx context.Context, opts ConsumerOptions) (jetstream.Consumer, error) {
	if strings.TrimSpace(opts.Durable) == "" {
		return nil, errors.New("bus: consumer durable name is required")
	}
	filters := normalizeFilters(opts.FilterSubject, opts.FilterSubjects)
	if len(filters) == 0 {
		return nil, errors.New("bus: consumer filter subject is required")
	}
	streamName, err := StreamForFilters(filters)
	if err != nil {
		return nil, err
	}
	if opts.MaxAckPending <= 0 {
		opts.MaxAckPending = b.opts.MaxAckPending
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 30 * time.Second
	}
	if opts.MaxDeliver == 0 {
		opts.MaxDeliver = -1
	}
	stream, err := b.StreamNamed(ctx, streamName)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		Durable:       opts.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       opts.AckWait,
		MaxAckPending: opts.MaxAckPending,
		MaxDeliver:    opts.MaxDeliver,
		BackOff:       append([]time.Duration(nil), opts.BackOff...),
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		DeliverPolicy: opts.DeliverPolicy,
	}
	if len(filters) == 1 {
		cfg.FilterSubject = filters[0]
	} else {
		cfg.FilterSubjects = filters
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("bus: ensure consumer %s on %s: %w", opts.Durable, streamName, err)
	}
	return consumer, nil
}

func normalizeFilters(single string, many []string) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	add(single)
	for _, s := range many {
		add(s)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
