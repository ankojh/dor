package agents

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/fetch"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/record"
	"github.com/ankojh/dor/internal/sources"
	"github.com/nats-io/nats.go/jetstream"
)

// Config is the fetcher configuration owned by cmd/dor. Every flag has a
// DOR_* fallback (see docs/conventions.md).
type Config struct {
	// SourcesFile is the reviewed seed list. Default
	// deploy/sources/sources.yaml.
	SourcesFile string
	// Recorder receives every raw message the fetchers publish, after a
	// successful non-duplicate ack. Nil disables recording.
	Recorder *record.Writer
	// UserAgent is the honest crawler identity.
	UserAgent string
	// RSSShards is the number of feed shards (plan: 3).
	RSSShards int
	// RSSShard selects one shard in this process (0-based). -1 runs every
	// shard in-process, which is the local mode.
	RSSShard int
	// RSSConcurrency bounds concurrent fetches per shard.
	RSSConcurrency int
	// GDELTFile selects the GDELT 2.0 file family: "gkg" (themes, default) or
	// "export" (CAMEO events).
	GDELTFile string
	// GDELTMaxItems caps items published per GDELT file.
	GDELTMaxItems int
	// GDELTExtraDomains adds to the source-quality allowlist.
	GDELTExtraDomains []string
	// ReliefWebAppname enables the ReliefWeb adapter. The API rejects
	// anonymous appnames, so it ships disabled until an approved one exists.
	ReliefWebAppname string
	// FetchMetrics lets the caller share the fetch/record metric family with a
	// record writer it creates. Nil means the runtime registers its own.
	FetchMetrics *fetch.Metrics
	// HTTPTransport overrides the HTTP transport (tests use an httptest TLS
	// server's transport). Nil uses http.DefaultTransport.
	HTTPTransport http.RoundTripper
	// Now is injectable for tests.
	Now func() time.Time
}

// DefaultConfig returns the production defaults.
func DefaultConfig() Config {
	return Config{
		SourcesFile:    "deploy/sources/sources.yaml",
		UserAgent:      fetch.DefaultUserAgent,
		RSSShards:      3,
		RSSShard:       -1,
		RSSConcurrency: 8,
		GDELTFile:      "gkg",
		GDELTMaxItems:  300,
	}
}

// fetcherRuntime is everything the three fetcher agents share: the source
// registry, one polite HTTP client, the health store, the persistent seen
// store and the recorder.
type fetcherRuntime struct {
	cfg     Config
	bus     *bus.Bus
	metrics *Metrics
	log     *slog.Logger
	now     func() time.Time

	fetchMetrics *fetch.Metrics
	health       *fetch.HealthStore
	seenKV       *seenStore
	client       *fetch.Client
	gdelt        *gdeltGate

	sourcesOnce sync.Once
	sources     *sources.File
	sourcesErr  error
}

// newFetcherRuntime builds the shared pieces. A failure to bind the health
// bucket is fatal (state would silently disappear); the seen and budget
// buckets degrade to in-memory or local behaviour with a warning.
func newFetcherRuntime(ctx context.Context, b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg Config) (*fetcherRuntime, error) {
	if cfg.SourcesFile == "" {
		cfg.SourcesFile = DefaultConfig().SourcesFile
	}
	if cfg.RSSShards <= 0 {
		cfg.RSSShards = 3
	}
	if cfg.RSSConcurrency <= 0 {
		cfg.RSSConcurrency = 8
	}
	if cfg.GDELTFile == "" {
		cfg.GDELTFile = "gkg"
	}
	if cfg.GDELTMaxItems <= 0 {
		cfg.GDELTMaxItems = 300
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	fm := cfg.FetchMetrics
	if fm == nil {
		fm = fetch.NewMetrics(metrics.Registry())
	}

	healthKV, err := b.KV(ctx, bus.KVBucketSourceHealth)
	if err != nil {
		return nil, fmt.Errorf("fetchers: source-health kv: %w", err)
	}
	health := fetch.NewHealthStore(healthKV, fm, log)

	seenKV, err := b.KV(ctx, bus.KVBucketSeen)
	if err != nil {
		log.Warn("fetchers: seen kv unavailable; republication protection is in-memory only", "err", err)
		seenKV = nil
	}

	budgetKV, err := b.KV(ctx, bus.KVBucketBudgets)
	if err != nil {
		log.Warn("fetchers: budgets kv unavailable; using a per-process host limiter", "err", err)
		budgetKV = nil
	}
	budget := fetch.NewBudget(fetch.BudgetOptions{KV: budgetKV, Now: now, Log: log})
	robots := fetch.NewRobotsCacheWithTransport(cfg.UserAgent, 10*time.Second, log, cfg.HTTPTransport)
	client := fetch.NewClient(fetch.ClientOptions{
		UserAgent: cfg.UserAgent,
		Budget:    budget,
		Robots:    robots,
		Transport: cfg.HTTPTransport,
	})

	return &fetcherRuntime{
		cfg:          cfg,
		bus:          b,
		metrics:      metrics,
		log:          log,
		now:          now,
		fetchMetrics: fm,
		health:       health,
		seenKV:       newSeenStore(seenKV),
		client:       client,
		gdelt:        newGDELTGate(),
	}, nil
}

// loadSources parses the seed list once.
func (r *fetcherRuntime) loadSources() (*sources.File, error) {
	r.sourcesOnce.Do(func() {
		r.sources, r.sourcesErr = sources.Load(r.cfg.SourcesFile)
		if r.sourcesErr == nil {
			counts := r.sources.Count()
			r.log.Info("source list loaded",
				"file", r.cfg.SourcesFile, "total", counts.Total,
				"enabled", counts.Enabled, "rss", counts.ByKind["rss"])
		}
	})
	return r.sources, r.sourcesErr
}

// restoreHealth loads a source's persisted health record, seeds the in-memory
// counters and metrics, and returns it so the caller can reuse the validators
// and GDELT's last file.
func (r *fetcherRuntime) restoreHealth(ctx context.Context, sourceID string) (fetch.HealthRecord, bool) {
	rec, ok := r.health.Load(ctx, sourceID)
	if !ok {
		return fetch.HealthRecord{}, false
	}
	r.health.Restore(rec)
	return rec, true
}

// publishRaw serializes and publishes one raw article. It returns true only
// when JetStream accepted a new message (not a duplicate), which is also when
// the frame is recorded. The message id is the raw article id, so a retry or a
// restart republishes the same id and JetStream dedupes it.
func (r *fetcherRuntime) publishRaw(ctx context.Context, out *Emitter, subject string, raw *dorv1.RawArticle) (bool, error) {
	if raw == nil {
		return false, errors.New("fetchers: nil raw article")
	}
	if raw.Id == "" {
		return false, errors.New("fetchers: raw article has no id")
	}
	data, err := proto.Marshal(raw)
	if err != nil {
		return false, fmt.Errorf("fetchers: marshal raw article: %w", err)
	}
	ctx = WithArticleID(ctx, raw.Id)
	ack, err := out.PublishAck(ctx, subject, data, bus.WithMsgID(raw.Id))
	if err != nil {
		return false, err
	}
	if ack != nil && ack.Duplicate {
		return false, nil
	}
	if r.cfg.Recorder != nil {
		r.cfg.Recorder.Record(subject, raw.Id, data)
	}
	return true, nil
}

// RawArticleID is the stable, source-scoped fetcher id:
// raw:<kind>:<sha256(key)>. RSS and GDELT can carry the same URL without
// colliding, and hazards key on the event id plus its revision so an
// escalation is a new message. The normalizer later assigns the canonical
// article id, hash(canonical URL).
func RawArticleID(kind, key string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return "raw:" + kind + ":" + hex.EncodeToString(sum[:])
}

// FeedShard returns the replica for a feed id: FNV-1a 32-bit mod shards.
func FeedShard(id string, shards int) int {
	if shards <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(shards))
}

func shardFeeds(feeds []sources.Source, shard, shards int) []sources.Source {
	out := make([]sources.Source, 0, len(feeds)/shards+1)
	for _, f := range feeds {
		if FeedShard(f.ID, shards) == shard {
			out = append(out, f)
		}
	}
	return out
}

// seenStore persists published raw ids in the `seen` KV bucket for 48 hours,
// so a restart does not republish a window that is still on the feeds. Keys
// are `fetch.<sha256(rawID)>` (the bucket TTL supplies the 48 h expiry).
type seenStore struct {
	kv keyValueStore
}

// keyValueStore is the subset of jetstream.KeyValue used here, so a test fake
// only has to implement these two methods (and the entry's Value).
type keyValueStore interface {
	Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error)
	Put(ctx context.Context, key string, value []byte) (uint64, error)
}

func newSeenStore(kv keyValueStore) *seenStore { return &seenStore{kv: kv} }

const seenKVTimeout = 500 * time.Millisecond

func seenKVKey(rawID string) string {
	sum := sha256.Sum256([]byte(rawID))
	return "fetch." + hex.EncodeToString(sum[:16])
}

// Has reports whether rawID was already published within the bucket TTL.
func (s *seenStore) Has(ctx context.Context, rawID string) bool {
	if s == nil || s.kv == nil {
		return false
	}
	opCtx, cancel := context.WithTimeout(ctx, seenKVTimeout)
	defer cancel()
	_, err := s.kv.Get(opCtx, seenKVKey(rawID))
	return err == nil
}

// Add records a successful publish. Errors are ignored: the in-memory seen set
// and JetStream dedupe are the fast paths, and a KV outage must not stop ingest.
func (s *seenStore) Add(ctx context.Context, rawID string) {
	if s == nil || s.kv == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, seenKVTimeout)
	defer cancel()
	_, _ = s.kv.Put(opCtx, seenKVKey(rawID), []byte{1})
}

// seenSet is a bounded LRU of already-published item keys with a sliding TTL.
// A hit refreshes the TTL and moves the existing node to the back; overflow
// and expiry evict from the front. Map and list are always the same size, so
// a long run has no unreachable map entries or stale order entries.
type seenSet struct {
	maxSeen int
	ttl     time.Duration
	ll      *list.List
	items   map[string]*list.Element
}

type seenEntry struct {
	key string
	at  time.Time
}

func newSeenSet(max int, ttl time.Duration) *seenSet {
	if max <= 0 {
		max = 256
	}
	if ttl <= 0 {
		ttl = 48 * time.Hour
	}
	return &seenSet{maxSeen: max, ttl: ttl, ll: list.New(), items: make(map[string]*list.Element, max)}
}

// Grow sizes the set at twice the observed feed window, capped at 4096.
func (s *seenSet) Grow(window int) {
	want := window * 2
	if want > 4096 {
		want = 4096
	}
	if want <= s.maxSeen {
		return
	}
	s.maxSeen = want
	s.trim()
}

func (s *seenSet) trim() {
	for s.ll.Len() > s.maxSeen {
		s.remove(s.ll.Front())
	}
}

func (s *seenSet) remove(e *list.Element) {
	if e == nil {
		return
	}
	ent := e.Value.(*seenEntry)
	s.ll.Remove(e)
	delete(s.items, ent.key)
}

// Has reports whether key is present and not expired, refreshing its TTL and
// moving it to the most-recently-used end.
func (s *seenSet) Has(key string, now time.Time) bool {
	if key == "" {
		return false
	}
	e, ok := s.items[key]
	if !ok {
		return false
	}
	ent := e.Value.(*seenEntry)
	if now.Sub(ent.at) >= s.ttl {
		s.remove(e)
		return false
	}
	ent.at = now
	s.ll.MoveToBack(e)
	return true
}

// Add records a key after a successful publish.
func (s *seenSet) Add(key string, now time.Time) {
	if key == "" {
		return
	}
	if e, ok := s.items[key]; ok {
		ent := e.Value.(*seenEntry)
		ent.at = now
		s.ll.MoveToBack(e)
		return
	}
	s.items[key] = s.ll.PushBack(&seenEntry{key: key, at: now})
	s.trim()
}

// Len returns the current map size (tests and the acceptance report).
func (s *seenSet) Len() int { return len(s.items) }

// listLen returns the list size; tests assert it always equals Len.
func (s *seenSet) listLen() int { return s.ll.Len() }

// waitUntil sleeps until at or ctx is done. Delay is measured with the
// runtime's clock so a test can drive time; the timer itself is real.
func waitUntil(ctx context.Context, now func() time.Time, at time.Time) bool {
	delay := at.Sub(now())
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
