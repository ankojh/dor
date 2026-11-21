package agents

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
)

// NormalizerDurable is the durable consumer name of the normalizer.
const NormalizerDurable = "normalize"

// ArticleSink is the non-blocking durable-store path used by the normalizer.
// *store.Writer satisfies it. Nil is allowed: the pipeline still publishes
// clean articles when Postgres is absent.
type ArticleSink interface {
	Enqueue(record store.ArticleRecord) bool
}

// NormalizerConfig tunes the normalizer. Zero values use defaults.
type NormalizerConfig struct {
	// Sink receives normalized article rows. Nil disables durable writes.
	Sink ArticleSink
	// NearDupHamming is the SimHash distance that counts as a near duplicate.
	// Default 3.
	NearDupHamming int
	// Now is injectable for tests. Default time.Now.
	Now func() time.Time
}

// NewNormalizer builds the normalize agent. It consumes article.raw and
// article.raw.priority and publishes article.clean.
func NewNormalizer(b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg NormalizerConfig) (Agent, error) {
	if cfg.NearDupHamming <= 0 {
		cfg.NearDupHamming = 3
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	n := &normalizer{
		bus:     b,
		log:     log,
		cfg:     cfg,
		metrics: newNormalizerMetrics(metrics.Registry()),
	}
	base, err := NewBase(b, metrics, log, Spec{
		Name:           NormalizerDurable,
		Durable:        NormalizerDurable,
		FilterSubjects: []string{bus.SubjectArticleRaw, bus.SubjectArticleRawPriority},
		MaxAckPending:  512,
		AckWait:        30 * time.Second,
		Concurrency:    2,
		Handle:         n.handle,
	})
	if err != nil {
		return nil, err
	}
	return base, nil
}

type normalizer struct {
	bus     *bus.Bus
	log     *slog.Logger
	cfg     NormalizerConfig
	metrics *normalizerMetrics

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error
}

func (n *normalizer) handle(ctx context.Context, msg *Message, out *Emitter) error {
	if len(msg.Data) == 0 {
		n.drop("malformed")
		n.log.Warn("normalizer: empty raw article", "msg_id", msg.ID())
		return nil
	}
	var raw dorv1.RawArticle
	if err := proto.Unmarshal(msg.Data, &raw); err != nil {
		n.drop("malformed")
		n.log.Warn("normalizer: malformed raw article", "msg_id", msg.ID(), "err", err)
		return nil
	}
	clean, record, reason, ok := n.normalize(&raw)
	if !ok {
		n.drop(reason)
		n.log.Debug("normalizer: dropped article", "raw_id", raw.Id, "reason", reason)
		return nil
	}
	if reason := n.duplicate(ctx, clean); reason != "" {
		n.drop(reason)
		return nil
	}

	data, err := proto.Marshal(clean)
	if err != nil {
		return fmt.Errorf("normalizer: marshal clean article: %w", err)
	}
	if err := out.Publish(ctx, bus.SubjectArticleClean, data, bus.WithMsgID("clean:"+clean.Id)); err != nil {
		return fmt.Errorf("normalizer: publish clean: %w", err)
	}
	n.metrics.published.Inc()
	n.remember(ctx, clean)
	if n.cfg.Sink != nil && !n.cfg.Sink.Enqueue(record) {
		n.drop("store")
		n.log.Debug("normalizer: store queue full", "article_id", clean.Id)
	}
	return nil
}

func (n *normalizer) normalize(raw *dorv1.RawArticle) (*dorv1.Article, store.ArticleRecord, string, bool) {
	canonical, err := CanonicalURL(raw.Url)
	if err != nil {
		return nil, store.ArticleRecord{}, "malformed", false
	}
	title := cleanText(raw.Title)
	if title == "" {
		return nil, store.ArticleRecord{}, "malformed", false
	}
	dateline, stripped := splitDateline(title)
	if strings.TrimSpace(stripped) != "" {
		title = stripped
	}
	title = truncateText(title, 500)
	lede := truncateText(cleanText(raw.Summary), 1000)
	now := n.cfg.Now().UTC()
	published := raw.PublishedAt
	if published <= 0 {
		published = raw.FetchedAt
	}
	if published <= 0 {
		published = now.Unix()
	}
	publishedAt := time.Unix(published, 0).UTC()
	lang := strings.TrimSpace(raw.Lang)
	if lang == "" {
		lang = "en"
	}
	id := ArticleID(canonical)
	clean := &dorv1.Article{
		Id:          id,
		SourceId:    raw.SourceId,
		Url:         canonical,
		Title:       title,
		Lede:        lede,
		PublishedAt: published,
		Lang:        lang,
		Dateline:    dateline,
		Hints:       raw.Hints,
		Burst:       raw.Burst,
	}
	record := store.ArticleRecord{
		ID:          id,
		SourceID:    raw.SourceId,
		URL:         canonical,
		Title:       title,
		Lede:        lede,
		PublishedAt: publishedAt,
		CreatedAt:   publishedAt,
		Lang:        lang,
		Dateline:    clean.Dateline,
	}
	return clean, record, "", true
}

// CanonicalURL strips fragments, default ports, tracking parameters and
// trailing slashes. It is exported because the gateway and later agents need
// the same identity rule.
func CanonicalURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", fmt.Errorf("URL has no host")
	}
	host = strings.TrimPrefix(host, "www.")
	port := u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		host = host + ":" + port
	}
	u.Host = host
	u.User = nil
	u.Fragment = ""
	path := u.Path
	path = strings.TrimSuffix(path, "/amp")
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		path = "/"
	}
	u.Path = path
	u.RawPath = ""
	q := u.Query()
	for key := range q {
		if trackingParam(key) {
			q.Del(key)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func trackingParam(key string) bool {
	k := strings.ToLower(key)
	if strings.HasPrefix(k, "utm_") || strings.HasPrefix(k, "pk_") {
		return true
	}
	switch k {
	case "fbclid", "gclid", "dclid", "msclkid", "yclid", "twclid", "igshid",
		"mc_cid", "mc_eid", "_hsenc", "_hsmi", "mkt_tok", "ref", "source",
		"cmpid", "ocid", "ns_campaign", "ns_source", "ns_mchannel", "ns_linkname",
		"ito", "at_medium", "at_campaign", "s_kwcid", "vero_id", "vero_conv",
		"wt_mc", "spm", "share", "share_token":
		return true
	}
	return false
}

// ArticleID is the stable clean-article id: SHA-256 of the canonical URL.
func ArticleID(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// cleanText unescapes HTML and removes tags, collapsing whitespace. It never
// stores a full body; callers truncate to a lede length.
func cleanText(s string) string {
	s = html.UnescapeString(s)
	s = tagRE.ReplaceAllString(s, " ")
	return strings.Join(strings.Fields(s), " ")
}

var tagRE = regexp.MustCompile(`<[^>]*>`)

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return strings.TrimSpace(s[:n])
}

var (
	datelineAgencyRE = regexp.MustCompile(`^([A-Z][A-Za-z .'’-]{1,40})\s*\([^)]*\)\s*[-–—]\s*(.*)$`)
	datelineCapsRE   = regexp.MustCompile(`^([A-Z][A-Z .'’-]{2,30})\s*[-–—]\s+(.*)$`)
)

// splitDateline removes a leading "CITY (Agency) -" or "CITY -" dateline and
// returns the city and the remaining title. A normal sentence beginning with a
// capitalised word is not a dateline unless it is all caps or has an agency.
func splitDateline(s string) (string, string) {
	if m := datelineAgencyRE.FindStringSubmatch(s); len(m) == 3 {
		return strings.TrimSpace(m[1]), strings.TrimSpace(m[2])
	}
	if m := datelineCapsRE.FindStringSubmatch(s); len(m) == 3 {
		return strings.TrimSpace(m[1]), strings.TrimSpace(m[2])
	}
	return "", ""
}

// SimHash is a 64-bit fingerprint over lower-cased word frequencies.
func SimHash(text string) uint64 {
	words := strings.Fields(strings.ToLower(text))
	if len(words) == 0 {
		return 0
	}
	weights := make(map[string]int, len(words))
	for _, w := range words {
		w = strings.Trim(w, ".,:;!?\"'()[]{}“”‘’—–-")
		if len(w) < 3 {
			continue
		}
		weights[w]++
	}
	var bits [64]int
	for word, weight := range weights {
		h := fnv64a(word)
		for i := 0; i < 64; i++ {
			if h&(uint64(1)<<uint(i)) != 0 {
				bits[i] += weight
			} else {
				bits[i] -= weight
			}
		}
	}
	var out uint64
	for i := 0; i < 64; i++ {
		if bits[i] > 0 {
			out |= uint64(1) << uint(i)
		}
	}
	return out
}

func fnv64a(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

func hamming(a, b uint64) int {
	x := a ^ b
	n := 0
	for x != 0 {
		x &= x - 1
		n++
	}
	return n
}

const simHashBands = 4

func simHashKeys(id string, h uint64) []string {
	keys := make([]string, 0, simHashBands)
	for band := 0; band < simHashBands; band++ {
		part := uint16(h >> uint(band*16))
		keys = append(keys, fmt.Sprintf("norm.sim.%d.%04x.%s", band, part, id))
	}
	return keys
}

func (n *normalizer) duplicate(ctx context.Context, clean *dorv1.Article) string {
	kv := n.dedupeKV(ctx)
	if kv == nil {
		return ""
	}
	if _, err := kv.Get(ctx, "norm.exact."+clean.Id); err == nil {
		return "exact"
	} else if !isKVNotFound(err) {
		n.metrics.dedupeErrors.Inc()
		n.log.Warn("normalizer: exact dedupe lookup failed; continuing", "err", err)
	}
	text := clean.Title + " " + clean.Lede
	if len(strings.Fields(text)) < 4 {
		return ""
	}
	sim := SimHash(text)
	for _, key := range simHashKeys(clean.Id, sim) {
		prefix := key[:strings.LastIndexByte(key, '.')+1]
		lister, err := kv.ListKeysFiltered(ctx, prefix+"*")
		if err != nil {
			if !isKVNotFound(err) {
				n.metrics.dedupeErrors.Inc()
			}
			continue
		}
		matched := false
		for candidate := range lister.Keys() {
			entry, err := kv.Get(ctx, candidate)
			if err != nil || len(entry.Value()) != 8 {
				continue
			}
			if hamming(sim, binary.BigEndian.Uint64(entry.Value())) <= n.cfg.NearDupHamming {
				matched = true
				break
			}
		}
		_ = lister.Stop()
		if matched {
			return "near"
		}
	}
	return ""
}

func (n *normalizer) remember(ctx context.Context, clean *dorv1.Article) {
	kv := n.dedupeKV(ctx)
	if kv == nil {
		return
	}
	if _, err := kv.Put(ctx, "norm.exact."+clean.Id, []byte{1}); err != nil {
		n.metrics.dedupeErrors.Inc()
		n.log.Warn("normalizer: exact dedupe write failed", "err", err)
	}
	sim := SimHash(clean.Title + " " + clean.Lede)
	if len(strings.Fields(clean.Title+" "+clean.Lede)) < 4 {
		return
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], sim)
	for _, key := range simHashKeys(clean.Id, sim) {
		if _, err := kv.Put(ctx, key, value[:]); err != nil {
			n.metrics.dedupeErrors.Inc()
			n.log.Warn("normalizer: simhash write failed", "err", err)
		}
	}
}

func isKVNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrKeyNotFound) ||
		errors.Is(err, jetstream.ErrKeyDeleted) ||
		errors.Is(err, jetstream.ErrNoKeysFound)
}

func (n *normalizer) dedupeKV(ctx context.Context) jetstream.KeyValue {
	n.kvOnce.Do(func() {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		n.kv, n.kvErr = n.bus.KV(c, bus.KVBucketSeen)
		if n.kvErr != nil {
			n.log.Warn("normalizer: seen kv unavailable; dedupe is disabled", "err", n.kvErr)
		}
	})
	return n.kv
}

func (n *normalizer) drop(reason string) {
	n.metrics.dropped.WithLabelValues(reason).Inc()
}

type normalizerMetrics struct {
	published    prometheus.Counter
	dropped      *prometheus.CounterVec
	dedupeErrors prometheus.Counter
}

func newNormalizerMetrics(reg prometheus.Registerer) *normalizerMetrics {
	m := &normalizerMetrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_normalizer_published_total",
			Help: "Clean articles published to article.clean.",
		}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dor_normalizer_dropped_total",
			Help: "Raw articles dropped by reason: malformed, exact, near, store.",
		}, []string{"reason"}),
		dedupeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dor_normalizer_dedupe_errors_total",
			Help: "KV dedupe lookup/write errors. Dedupe fails open.",
		}),
	}
	reg.MustRegister(m.published, m.dropped, m.dedupeErrors)
	return m
}
