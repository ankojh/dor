package agents

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// PublisherAgentName is the registry name of the publisher. One publisher per
// deployment owns the delta sequence.
const PublisherAgentName = "publisher"

const (
	defaultFlushInterval    = 250 * time.Millisecond
	defaultSnapshotInterval = 5 * time.Second
	publisherStoryTTL       = 48 * time.Hour
	// publisherWireHistory is the bounded cold-start wire history placed in
	// every snapshot. It is not a replay of old deltas; rows carry no seq.
	publisherWireHistory = 500
)

// NewPublisher builds the publisher. It runs two consumers because its inputs
// live in two streams: article.clean/article.placed in ENRICH and
// story.changed.* / link.changed in STORY. A single JetStream consumer cannot
// span streams.
func NewPublisher(b *bus.Bus, metrics *Metrics, log *slog.Logger) (Agent, error) {
	core := newPublisherCore(b, log)

	enrich, err := NewBase(b, metrics, log, Spec{
		Name:    "publisher-enrich",
		Durable: "publisher-enrich",
		FilterSubjects: []string{
			bus.SubjectArticleClean,
			bus.SubjectArticlePlaced,
		},
		MaxAckPending: 512,
		AckWait:       30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var article dorv1.Article
			if err := proto.Unmarshal(msg.Data, &article); err != nil {
				if msg.Subject == bus.SubjectArticleClean {
					return Terminal(fmt.Errorf("publisher: article: %w", err))
				}
				// article.placed gets a dedicated shape in M2; an unknown
				// body is not a wire event we can use yet.
				return nil
			}
			core.onArticle(&article, msg.Subject == bus.SubjectArticlePlaced)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	story, err := NewBase(b, metrics, log, Spec{
		Name:    "publisher-story",
		Durable: "publisher-story",
		FilterSubjects: []string{
			bus.SubjectStoryChangedPrefix + "*",
			bus.SubjectLinkChanged,
		},
		MaxAckPending: 512,
		AckWait:       30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			if msg.Subject == bus.SubjectLinkChanged {
				var link dorv1.Link
				if err := proto.Unmarshal(msg.Data, &link); err != nil {
					return Terminal(fmt.Errorf("publisher: link: %w", err))
				}
				core.onLink(&link)
				return nil
			}
			var s dorv1.Story
			if err := proto.Unmarshal(msg.Data, &s); err != nil {
				return Terminal(fmt.Errorf("publisher: story: %w", err))
			}
			core.onStory(&s)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return &publisherAgent{
		name:   PublisherAgentName,
		core:   core,
		enrich: enrich,
		story:  story,
	}, nil
}

// publisherAgent composes the two consumers and the flush/snapshot loops into
// one registry agent named "publisher".
type publisherAgent struct {
	name   string
	core   *publisherCore
	enrich Agent
	story  Agent

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu      sync.Mutex
	cancel  context.CancelFunc
	lastErr error
	wg      sync.WaitGroup
}

func (p *publisherAgent) Name() string { return p.name }

func (p *publisherAgent) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: %s already started", p.name)
	}
	if p.stopped.Load() {
		return fmt.Errorf("agents: %s is stopped", p.name)
	}
	if err := p.core.loadSeq(ctx); err != nil {
		p.core.log.Warn("publisher: could not restore seq; starting from the stream tail", "err", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancel = cancel
	p.mu.Unlock()

	if err := p.enrich.Start(ctx); err != nil {
		cancel()
		return err
	}
	if err := p.story.Start(ctx); err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.enrich.Stop(stopCtx)
		stopCancel()
		cancel()
		return err
	}
	p.running.Store(true)
	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		p.core.flushLoop(runCtx, defaultFlushInterval)
	}()
	go func() {
		defer p.wg.Done()
		p.core.snapshotLoop(runCtx, defaultSnapshotInterval)
	}()
	go func() {
		defer p.wg.Done()
		p.core.expireLoop(runCtx, time.Minute)
	}()
	p.core.log.Info("publisher started")
	// Publish the first snapshot immediately so a gateway started in the same
	// process never serves an empty cold-start snapshot longer than necessary.
	p.core.writeSnapshot(ctx)
	return nil
}

// Consume is the blocking form used by tests and custom callers. The runner
// uses Start.
func (p *publisherAgent) Consume(ctx context.Context) error {
	if err := p.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (p *publisherAgent) Stop(ctx context.Context) error {
	if p.stopped.Swap(true) {
		return nil
	}
	p.running.Store(false)
	p.mu.Lock()
	cancel := p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wg.Wait()
	// A final flush and snapshot preserve the last seq and live state.
	p.core.flush(context.Background())
	p.core.writeSnapshot(context.Background())

	var errs []error
	if err := p.story.Stop(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := p.enrich.Stop(ctx); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("publisher stop: %w", errs[0])
	}
	return nil
}

func (p *publisherAgent) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	return p.enrich.Publish(ctx, subject, data, opts...)
}

func (p *publisherAgent) Running() bool {
	return p.running.Load() && !p.stopped.Load()
}

func (p *publisherAgent) LastError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastErr != nil {
		return p.lastErr
	}
	if err := p.enrich.(HealthReporter).LastError(); err != nil {
		return err
	}
	return p.story.(HealthReporter).LastError()
}

func (p *publisherAgent) TerminalCount() int64 {
	return p.enrich.(HealthReporter).TerminalCount() + p.story.(HealthReporter).TerminalCount()
}

// publisherCore owns the delta sequence, per-entity coalescing, the in-memory
// live state and the cached snapshot. It is safe for use by both consumers.
type publisherCore struct {
	bus *bus.Bus
	log *slog.Logger
	now func() time.Time

	flushMu sync.Mutex // serialises flush (and seq assignment)
	mu      sync.Mutex // guards pending and live state

	seq      uint64
	pending  map[string]*dorv1.Delta
	order    []string
	stories  map[string]*dorv1.Story
	links    map[string]*dorv1.Link
	wire     []*dorv1.WireRow
	changed  bool
	lastSnap time.Time
}

func newPublisherCore(b *bus.Bus, log *slog.Logger) *publisherCore {
	return &publisherCore{
		bus:     b,
		log:     log,
		now:     time.Now,
		pending: make(map[string]*dorv1.Delta),
		stories: make(map[string]*dorv1.Story),
		links:   make(map[string]*dorv1.Link),
	}
}

// loadSeq restores the last assigned delta sequence from KV and, if newer,
// from the retained delta stream. This keeps the sequence monotonic across a
// restart even if the KV write lagged.
func (c *publisherCore) loadSeq(ctx context.Context) error {
	var seq uint64
	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err == nil {
		entry, getErr := kv.Get(ctx, bus.KVPublisherSeqKey)
		if getErr == nil {
			_, _ = fmt.Sscanf(string(entry.Value()), "%d", &seq)
		}
	}
	stream, streamErr := c.bus.StreamNamed(ctx, bus.StreamDelta)
	if streamErr == nil {
		if msg, msgErr := stream.GetLastMsgForSubject(ctx, bus.SubjectDeltaOut); msgErr == nil {
			var batch dorv1.DeltaBatch
			if proto.Unmarshal(msg.Data, &batch) == nil {
				for _, d := range batch.Deltas {
					if d.Seq > seq {
						seq = d.Seq
					}
				}
			}
		}
	}
	c.mu.Lock()
	if seq > c.seq {
		c.seq = seq
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (c *publisherCore) onArticle(a *dorv1.Article, placed bool) {
	if a == nil || a.Id == "" {
		return
	}
	place := ""
	if a.Hints != nil {
		place = a.Hints.GetPlace()
	}
	if place == "" {
		place = a.Dateline
	}
	join := dorv1.JoinKind_JOIN_KIND_NEW
	if placed && a.Join != dorv1.JoinKind_JOIN_KIND_NEW {
		join = a.Join
	}
	category := dorv1.Category_CATEGORY_OTHER
	c.mu.Lock()
	if a.StoryId != "" {
		if story, ok := c.stories[a.StoryId]; ok && story != nil {
			category = story.Category
		}
	}
	c.mu.Unlock()
	wire := &dorv1.WireRow{
		ArticleId:    a.Id,
		StoryId:      a.StoryId,
		Title:        a.Title,
		Source:       a.SourceId,
		Place:        place,
		Category:     category,
		At:           a.PublishedAt,
		Join:         join,
		PromptScores: nil,
	}
	c.mu.Lock()
	for i, row := range c.wire {
		if row.ArticleId == wire.ArticleId {
			c.wire = append(c.wire[:i], c.wire[i+1:]...)
			break
		}
	}
	c.wire = append(c.wire, wire)
	if len(c.wire) > publisherWireHistory {
		c.wire = append([]*dorv1.WireRow(nil), c.wire[len(c.wire)-publisherWireHistory:]...)
	}
	c.mu.Unlock()
	c.enqueue("wire:"+a.Id, &dorv1.Delta{Body: &dorv1.Delta_Wire{Wire: wire}})
}

func (c *publisherCore) expireLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.expireStories(c.now())
		}
	}
}

func (c *publisherCore) expireStories(now time.Time) {
	c.mu.Lock()
	ids := make([]string, 0)
	for id, s := range c.stories {
		if s == nil || s.UpdatedAt <= 0 {
			continue
		}
		if now.Sub(time.Unix(s.UpdatedAt, 0)) >= publisherStoryTTL {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.enqueue("story:"+id, &dorv1.Delta{Body: &dorv1.Delta_StoryExpire{StoryExpire: id}})
	}
}

func (c *publisherCore) onStory(s *dorv1.Story) {
	if s == nil || s.Id == "" {
		return
	}
	c.enqueue("story:"+s.Id, &dorv1.Delta{Body: &dorv1.Delta_StoryUpsert{StoryUpsert: s}})
}

func (c *publisherCore) onLink(l *dorv1.Link) {
	if l == nil || l.A == "" || l.B == "" {
		return
	}
	a, b := l.A, l.B
	if a > b {
		a, b = b, a
	}
	key := "link:" + a + "|" + b
	if l.Strength <= 0 && l.Confidence <= 0 {
		// The relation judge's removal tombstone.
		c.enqueue(key, &dorv1.Delta{Body: &dorv1.Delta_LinkRemove{
			LinkRemove: &dorv1.LinkKey{A: a, B: b},
		}})
		return
	}
	c.enqueue(key, &dorv1.Delta{Body: &dorv1.Delta_LinkUpsert{LinkUpsert: l}})
}

func (c *publisherCore) enqueue(key string, delta *dorv1.Delta) {
	c.mu.Lock()
	if _, ok := c.pending[key]; !ok {
		c.order = append(c.order, key)
	}
	c.pending[key] = delta
	c.changed = true
	c.mu.Unlock()
}

// flush assigns the next sequence numbers in enqueue order and publishes one
// DeltaBatch to delta.out. Coalescing means one delta per entity per flush.
func (c *publisherCore) flush(ctx context.Context) {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}
	deltas := make([]*dorv1.Delta, 0, len(c.pending))
	for _, key := range c.order {
		d, ok := c.pending[key]
		if !ok {
			continue
		}
		c.seq++
		d.Seq = c.seq
		deltas = append(deltas, d)
		switch body := d.Body.(type) {
		case *dorv1.Delta_StoryUpsert:
			if body.StoryUpsert != nil {
				c.stories[body.StoryUpsert.Id] = body.StoryUpsert
			}
		case *dorv1.Delta_StoryExpire:
			delete(c.stories, body.StoryExpire)
		case *dorv1.Delta_LinkUpsert:
			if body.LinkUpsert != nil {
				c.links[body.LinkUpsert.A+"|"+body.LinkUpsert.B] = body.LinkUpsert
			}
		case *dorv1.Delta_LinkRemove:
			if body.LinkRemove != nil {
				delete(c.links, body.LinkRemove.A+"|"+body.LinkRemove.B)
			}
		}
	}
	c.pending = make(map[string]*dorv1.Delta)
	c.order = nil
	lastSeq := c.seq
	c.changed = false
	c.mu.Unlock()

	batch := &dorv1.DeltaBatch{Deltas: deltas}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
	if err != nil {
		c.log.Warn("publisher: marshal batch", "err", err)
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.bus.Publish(pubCtx, bus.SubjectDeltaOut, data,
		bus.WithMsgID(fmt.Sprintf("delta:%d", lastSeq))); err != nil {
		c.log.Warn("publisher: publish delta batch", "seq", lastSeq, "err", err)
		return
	}
	c.persistSeq(pubCtx, lastSeq)
}

func (c *publisherCore) persistSeq(ctx context.Context, seq uint64) {
	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, _ = kv.Put(opCtx, bus.KVPublisherSeqKey, []byte(fmt.Sprintf("%d", seq)))
}

func (c *publisherCore) flushLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

func (c *publisherCore) snapshotLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.writeSnapshot(context.Background())
			return
		case <-ticker.C:
			c.writeSnapshot(ctx)
		}
	}
}

// writeSnapshot stores the cached Snapshot binary in live_state so every
// gateway replica can serve the same state without its own story store.
func (c *publisherCore) writeSnapshot(ctx context.Context) {
	c.mu.Lock()
	storyIDs := make([]string, 0, len(c.stories))
	for id := range c.stories {
		storyIDs = append(storyIDs, id)
	}
	sort.Strings(storyIDs)
	linkKeys := make([]string, 0, len(c.links))
	for k := range c.links {
		linkKeys = append(linkKeys, k)
	}
	sort.Strings(linkKeys)
	snapshot := &dorv1.Snapshot{
		Seq:        c.seq,
		Stories:    make([]*dorv1.Story, 0, len(storyIDs)),
		Links:      make([]*dorv1.Link, 0, len(linkKeys)),
		BuiltAt:    c.now().Unix(),
		RecentWire: append([]*dorv1.WireRow(nil), c.wire...),
	}
	for _, id := range storyIDs {
		snapshot.Stories = append(snapshot.Stories, c.stories[id])
	}
	for _, k := range linkKeys {
		snapshot.Links = append(snapshot.Links, c.links[k])
	}
	c.lastSnap = c.now()
	c.mu.Unlock()

	kv, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := kv.Put(opCtx, bus.KVLiveStateSnapshotKey, data); err != nil {
		c.log.Debug("publisher: snapshot write failed", "err", err)
	}
}
