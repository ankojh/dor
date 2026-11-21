package agents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// EmbedDurable is the embedder consumer name.
const EmbedDurable = "embed"

const (
	embeddingDim   = 384
	embedBatchSize = 64
	embedFlushAge  = 50 * time.Millisecond
	embedCacheSize = 2048
)

// EmbeddingClient is the remote TEI surface. The embedder falls back to a
// deterministic hash vector when it is nil or returns an error.
type EmbeddingClient interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// TEIClient calls a Hugging Face Text Embeddings Inference /embed endpoint.
type TEIClient struct {
	url    string
	model  string
	client *http.Client
}

// NewTEIClient returns nil when url is empty, disabling the remote path.
func NewTEIClient(url string, timeout time.Duration) *TEIClient {
	url = strings.TrimSpace(strings.TrimRight(url, "/"))
	if url == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &TEIClient{url: url, model: "bge-small-en-v1.5", client: &http.Client{Timeout: timeout}}
}

// Model returns the embedding model label written on the bus.
func (t *TEIClient) Model() string { return t.model }

// Embed sends one logical batch. The main TEI instance rejects HTTP requests
// larger than 32 inputs, so the 64-item agent window is split into bounded
// requests while preserving order.
func (t *TEIClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if t == nil {
		return nil, fmt.Errorf("embed: TEI disabled")
	}
	const maxBatch = 32
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += maxBatch {
		end := start + maxBatch
		if end > len(texts) {
			end = len(texts)
		}
		part, err := t.embedBatch(ctx, texts[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

func (t *TEIClient) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	// The bundled TEI server accepts at most 32 inputs per HTTP request.
	// Keep the agent's larger batching window while respecting that limit.
	const maxRequestBatch = 32
	if len(texts) > maxRequestBatch {
		vectors := make([][]float32, 0, len(texts))
		for start := 0; start < len(texts); start += maxRequestBatch {
			end := min(start+maxRequestBatch, len(texts))
			chunk, err := t.Embed(ctx, texts[start:end])
			if err != nil {
				return nil, err
			}
			vectors = append(vectors, chunk...)
		}
		return vectors, nil
	}
	payload, err := json.Marshal(map[string]any{"inputs": texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url+"/embed", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "dor-embed/0.1 (+https://github.com/ankojh/dor)")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: TEI: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed: TEI HTTP %d", resp.StatusCode)
	}
	var out [][]float32
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	if len(out) != len(texts) {
		return nil, fmt.Errorf("embed: TEI returned %d vectors for %d inputs", len(out), len(texts))
	}
	for i := range out {
		if len(out[i]) != embeddingDim {
			return nil, fmt.Errorf("embed: TEI vector %d has %d dims, want %d", i, len(out[i]), embeddingDim)
		}
		out[i] = normalizeVector(out[i])
	}
	return out, nil
}

// NewEmbedder builds the vector agent. Its handler only enqueues; a single
// batch loop calls TEI with up to 64 articles or after 50 ms and publishes
// article.vec. A full queue or TEI failure produces a degraded hash vector so
// the clusterer still receives a vector.
func NewEmbedder(b *bus.Bus, metrics *Metrics, log *slog.Logger, deps Deps) (Agent, error) {
	e := &embedder{
		deps:  deps,
		log:   log,
		in:    make(chan *dorv1.Article, 2048),
		cache: make(map[string][]float32),
	}
	base, err := NewBase(b, metrics, log, Spec{
		Name:           EmbedDurable,
		Durable:        EmbedDurable,
		FilterSubjects: []string{bus.SubjectArticleClean},
		MaxAckPending:  1024,
		AckWait:        30 * time.Second,
		Concurrency:    1,
		Handle:         e.handle,
	})
	if err != nil {
		return nil, err
	}
	e.base = base
	return e, nil
}

type embedder struct {
	base Agent
	deps Deps
	log  *slog.Logger
	in   chan *dorv1.Article

	cacheMu    sync.Mutex
	cache      map[string][]float32
	cacheOrder []string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	started atomic.Bool
	stopped atomic.Bool
}

func (e *embedder) Name() string { return e.base.Name() }
func (e *embedder) Consume(ctx context.Context) error {
	return e.base.Consume(ctx)
}
func (e *embedder) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	return e.base.Publish(ctx, subject, data, opts...)
}

func (e *embedder) Start(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: embed already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.wg.Add(1)
	go e.batchLoop(runCtx)
	if err := e.base.Start(ctx); err != nil {
		cancel()
		e.wg.Wait()
		return err
	}
	return nil
}

func (e *embedder) Stop(ctx context.Context) error {
	if !e.stopped.CompareAndSwap(false, true) {
		return nil
	}
	err := e.base.Stop(ctx)
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return err
}

func (e *embedder) handle(ctx context.Context, msg *Message, out *Emitter) error {
	var article dorv1.Article
	if err := proto.Unmarshal(msg.Data, &article); err != nil {
		return Terminal(fmt.Errorf("embed: decode article: %w", err))
	}
	if article.Id == "" {
		return Terminal(fmt.Errorf("embed: article has no id"))
	}
	select {
	case e.in <- &article:
		return nil
	default:
		vec := fallbackVector(article.GetTitle() + " " + article.GetLede())
		if err := e.publishWith(out, ctx, &article, vec, "hash-384-v1", true); err != nil {
			return err
		}
		return nil
	}
}

func (e *embedder) batchLoop(ctx context.Context) {
	defer e.wg.Done()
	ticker := time.NewTicker(embedFlushAge)
	defer ticker.Stop()
	batch := make([]*dorv1.Article, 0, embedBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		e.processBatch(ctx, batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case article := <-e.in:
			batch = append(batch, article)
			if len(batch) >= embedBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (e *embedder) processBatch(ctx context.Context, batch []*dorv1.Article) {
	type missing struct {
		article *dorv1.Article
		text    string
		key     string
	}
	var need []missing
	for _, article := range batch {
		text := article.GetTitle() + "\n" + article.GetLede()
		key := embedKey(text)
		if vec, ok := e.cacheGet(key); ok {
			if err := e.publish(ctx, article, vec, "bge-small-en-v1.5", false); err != nil {
				e.log.Warn("embed: publish cached", "article_id", article.Id, "err", err)
			}
			continue
		}
		need = append(need, missing{article: article, text: text, key: key})
	}
	if len(need) == 0 {
		return
	}
	texts := make([]string, len(need))
	for i, m := range need {
		texts[i] = m.text
	}
	var vectors [][]float32
	var err error
	model := "hash-384-v1"
	degraded := true
	if e.deps.Embedding != nil {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		vectors, err = e.deps.Embedding.Embed(callCtx, texts)
		cancel()
		if err != nil {
			e.log.Warn("embed: TEI unavailable; using degraded hash vectors", "count", len(texts), "err", err)
		} else if len(vectors) == len(need) {
			model = "bge-small-en-v1.5"
			if tei, ok := e.deps.Embedding.(*TEIClient); ok {
				model = tei.Model()
			}
			degraded = false
		} else {
			e.log.Warn("embed: TEI returned wrong count; using degraded hash vectors", "want", len(need), "got", len(vectors))
			vectors = nil
		}
	}
	for i, m := range need {
		vec := fallbackVector(m.text)
		if i < len(vectors) && len(vectors[i]) == embeddingDim {
			vec = vectors[i]
			e.cachePut(m.key, vec)
		}
		if perr := e.publish(ctx, m.article, vec, model, degraded); perr != nil {
			e.log.Warn("embed: publish", "article_id", m.article.Id, "err", perr)
		}
	}
}

func (e *embedder) publish(ctx context.Context, article *dorv1.Article, vec []float32, model string, degraded bool) error {
	msg := &dorv1.EmbeddedArticle{Article: article, Vector: vec, Model: model, Degraded: degraded}
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return e.base.Publish(ctx, bus.SubjectArticleVec, data,
		bus.WithMsgID("vec:"+article.Id),
		bus.WithHeaders(nats.Header{bus.HeaderArticleID: []string{article.Id}}),
	)
}

func (e *embedder) publishWith(out *Emitter, ctx context.Context, article *dorv1.Article, vec []float32, model string, degraded bool) error {
	msg := &dorv1.EmbeddedArticle{Article: article, Vector: vec, Model: model, Degraded: degraded}
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return out.Publish(ctx, bus.SubjectArticleVec, data, bus.WithMsgID("vec:"+article.Id))
}

func (e *embedder) cacheGet(key string) ([]float32, bool) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	vec, ok := e.cache[key]
	if !ok {
		return nil, false
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	return out, true
}

func (e *embedder) cachePut(key string, vec []float32) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	if _, ok := e.cache[key]; !ok {
		e.cacheOrder = append(e.cacheOrder, key)
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	e.cache[key] = out
	for len(e.cacheOrder) > embedCacheSize {
		old := e.cacheOrder[0]
		e.cacheOrder = e.cacheOrder[1:]
		delete(e.cache, old)
	}
}

func embedKey(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:16])
}

func normalizeVector(v []float32) []float32 {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return v
	}
	norm = math.Sqrt(norm)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / norm)
	}
	return out
}

// fallbackVector is a deterministic feature-hash embedding. It is not a model
// output and every message carrying it is marked degraded.
func fallbackVector(text string) []float32 {
	out := make([]float32, embeddingDim)
	words := strings.Fields(strings.ToLower(text))
	for _, word := range words {
		word = strings.Trim(word, ".,:;!?\"'()[]{}“”‘’—–-")
		if len(word) < 3 {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(word))
		sum := h.Sum64()
		idx := int(sum % embeddingDim)
		sign := float32(1)
		if sum&(1<<63) != 0 {
			sign = -1
		}
		out[idx] += sign * float32(math.Log1p(float64(len(word))))
	}
	return normalizeVector(out)
}
