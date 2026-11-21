package agents

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
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

// NewPublisher builds the publisher. Its three consumers bind ENRICH, STORY and ALERT separately; a JetStream
// consumer cannot span streams. Bounded pools keep the 250ms commit barrier
// from limiting input throughput to one event per flush.
func NewPublisher(b *bus.Bus, metrics *Metrics, log *slog.Logger) (Agent, error) {
	core := newPublisherCore(b, log)
	core.metrics = metrics
	if metrics != nil {
		metrics.RegisterAgent(PublisherAgentName)
	}

	enrich, err := NewBase(b, metrics, log, Spec{
		Name:    "publisher-enrich",
		Durable: "publisher-enrich",
		FilterSubjects: []string{
			bus.SubjectArticleClean,
			bus.SubjectArticlePlaced,
			bus.SubjectMatchScore,
		},
		MaxAckPending:  512,
		Concurrency:    64,
		MaxConcurrency: 64,
		AckWait:        30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			if msg.Subject == bus.SubjectMatchScore {
				var score dorv1.MatchScore
				if err := proto.Unmarshal(msg.Data, &score); err != nil {
					return Terminal(fmt.Errorf("publisher: match score: %w", err))
				}
				return core.acceptInput(ctx, msg, "score:"+score.ArticleId, func() { core.onMatchScore(&score) })
			}
			var article dorv1.Article
			if err := proto.Unmarshal(msg.Data, &article); err != nil {
				if msg.Subject == bus.SubjectArticleClean {
					return Terminal(fmt.Errorf("publisher: article: %w", err))
				}
				// article.placed gets a dedicated shape in M2; an unknown
				// body is not a wire event we can use yet.
				return nil
			}
			stage := "clean:"
			if msg.Subject == bus.SubjectArticlePlaced {
				stage = "placed:"
			}
			return core.acceptInput(ctx, msg, stage+article.Id, func() { core.onArticle(&article, msg.Subject == bus.SubjectArticlePlaced) })
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
		MaxAckPending:  512,
		Concurrency:    64,
		MaxConcurrency: 64,
		AckWait:        30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			if msg.Subject == bus.SubjectLinkChanged {
				var link dorv1.Link
				if err := proto.Unmarshal(msg.Data, &link); err != nil {
					return Terminal(fmt.Errorf("publisher: link: %w", err))
				}
				return core.acceptInput(ctx, msg, "link:"+publisherLinkKey(link.A, link.B), func() { core.onLink(&link) })
			}
			var s dorv1.Story
			if err := proto.Unmarshal(msg.Data, &s); err != nil {
				return Terminal(fmt.Errorf("publisher: story: %w", err))
			}
			return core.acceptInput(ctx, msg, "story:"+s.Id, func() { core.onStory(&s) })
		},
	})
	if err != nil {
		return nil, err
	}

	alertBase, err := NewBase(b, metrics, log, Spec{
		Name:           "publisher-alert",
		Durable:        "publisher-alert",
		FilterSubject:  bus.SubjectMatchAlert,
		MaxAckPending:  128,
		Concurrency:    64,
		MaxConcurrency: 64,
		AckWait:        30 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var hit dorv1.AlertHit
			if err := proto.Unmarshal(msg.Data, &hit); err != nil {
				return Terminal(fmt.Errorf("publisher: alert hit: %w", err))
			}
			return core.acceptInput(ctx, msg, publisherAlertKey(&hit), func() { core.onAlert(&hit) })
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
		alert:  alertBase,
	}, nil
}

// publisherAgent composes the three consumers and the flush/snapshot loops into
// one registry agent named "publisher".
type publisherAgent struct {
	name   string
	core   *publisherCore
	enrich *Base
	story  *Base
	alert  *Base

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu      sync.Mutex
	cancel  context.CancelFunc
	runCtx  context.Context
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
	if err := p.core.restore(ctx); err != nil {
		return err
	}
	p.startLoops(ctx)
	if err := p.enrich.Start(p.runCtx); err != nil {
		_ = p.Stop(ctx)
		return err
	}
	for _, consumer := range []*Base{p.story, p.alert} {
		if err := consumer.Start(p.runCtx); err != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = p.Stop(stopCtx)
			stopCancel()
			return err
		}
	}
	p.core.log.Info("publisher started")
	return nil
}

func (p *publisherAgent) startLoops(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancel = cancel
	p.runCtx = runCtx
	p.mu.Unlock()
	p.core.onFence = func() {
		cancel()
		go func() {
			ctx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			for _, b := range []*Base{p.enrich, p.story, p.alert} {
				_ = b.Stop(ctx)
			}
		}()
	}
	p.running.Store(true)
	p.wg.Add(4)
	go func() { defer p.wg.Done(); p.core.compactionLoop(runCtx) }()
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
}

func (p *publisherAgent) Restart(ctx context.Context) error {
	if err := p.Stop(ctx); err != nil {
		return err
	}
	if err := p.core.restore(ctx); err != nil {
		return err
	}
	p.stopped.Store(false)
	p.mu.Lock()
	p.lastErr = nil
	p.mu.Unlock()
	p.startLoops(context.WithoutCancel(ctx))
	for _, consumer := range []*Base{p.enrich, p.story, p.alert} {
		if err := consumer.Restart(ctx); err != nil {
			_ = p.Stop(ctx)
			return err
		}
	}
	p.core.log.Info("publisher restarted")
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
	// Keep flushing while Base drains its handlers, which await durable commits.
	var errs []error
	for _, consumer := range []*Base{p.alert, p.story, p.enrich} {
		if err := consumer.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	p.mu.Lock()
	cancel := p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wg.Wait()
	if err := p.core.release(ctx); err != nil {
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
	return p.running.Load() && !p.stopped.Load() && !p.core.fenced.Load()
}

func (p *publisherAgent) LastError() error {
	if p.core.fenced.Load() {
		return fmt.Errorf("publisher: publication owner fenced")
	}
	p.core.errMu.Lock()
	err := p.core.flushErr
	p.core.errMu.Unlock()
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastErr != nil {
		return p.lastErr
	}
	if err := p.enrich.LastError(); err != nil {
		return err
	}
	if err := p.story.LastError(); err != nil {
		return err
	}
	return p.alert.LastError()
}

func (p *publisherAgent) TerminalCount() int64 {
	return p.enrich.TerminalCount() +
		p.story.TerminalCount() +
		p.alert.TerminalCount()
}

// publisherCore owns the delta sequence, per-entity coalescing, the in-memory
// live state and the cached snapshot. It is safe for use by both consumers.
type publisherCore struct {
	bus *bus.Bus
	log *slog.Logger
	now func() time.Time

	flushMu       sync.Mutex // serialises flush, ownership and checkpoint writes
	ingestMu      sync.Mutex // seals inputs and their acknowledgement barriers together
	checkpoint    publisherCheckpoint
	revision      uint64
	owner         string
	restored      bool
	inflight      *publisherBatch
	lastChunkGC   time.Time
	fenced        atomic.Bool
	storage       *publication.Store
	committed     *publication.State
	scoreOrder    []string
	fenceDone     chan struct{}
	onFence       func()
	errMu         sync.Mutex
	flushErr      error
	metrics       *Metrics
	waiters       []chan struct{}
	pendingCredit int64
	capacity      chan struct{}
	publish       func(context.Context, []byte, string) error
	mu            sync.Mutex // guards pending and live state

	seq                uint64
	pending            map[string]*dorv1.Delta
	order              []string
	stories            map[string]*dorv1.Story
	links              map[string]*dorv1.Link
	wire               []*dorv1.WireRow
	wireRows           map[string]*dorv1.WireRow
	wireOrder          []string
	revisions          map[string]uint64
	committedRevisions map[string]uint64
	scores             map[string]*dorv1.MatchScore
	placed             map[string]bool
}

func newPublisherCore(b *bus.Bus, log *slog.Logger) *publisherCore {
	if log == nil {
		log = slog.Default()
	}
	return &publisherCore{
		bus:                b,
		log:                log,
		now:                time.Now,
		pending:            make(map[string]*dorv1.Delta),
		stories:            make(map[string]*dorv1.Story),
		links:              make(map[string]*dorv1.Link),
		wireRows:           make(map[string]*dorv1.WireRow),
		revisions:          make(map[string]uint64),
		committedRevisions: make(map[string]uint64),
		scores:             make(map[string]*dorv1.MatchScore),
		placed:             make(map[string]bool),
		capacity:           make(chan struct{}),
	}
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
	wire := &dorv1.WireRow{
		ArticleId:    a.Id,
		StoryId:      a.StoryId,
		Title:        a.Title,
		Source:       a.SourceId,
		Place:        place,
		Category:     category,
		At:           a.PublishedAt,
		Join:         join,
		Lang:         a.Lang,
		PromptScores: nil,
	}
	if hints := a.Hints; hints != nil {
		wire.Country = hints.Country
		wire.Lat = hints.Lat
		wire.Lon = hints.Lon
		wire.Located = hints.Located
	}
	c.mu.Lock()
	// A placement update rebuilds story/category in place. Keep prompt scores
	// already computed for this article; they are prompt-keyed and portable.
	if prev, ok := c.wireRows[a.Id]; ok && prev != nil {
		wire.PromptScores = prev.PromptScores
		wire.ApproxPrompts = prev.ApproxPrompts
		if !placed && c.placed[a.Id] {
			wire.StoryId, wire.Join, wire.Category = prev.StoryId, prev.Join, prev.Category
			wire.Country, wire.Place = prev.Country, prev.Place
			wire.Lat, wire.Lon, wire.Located = prev.Lat, prev.Lon, prev.Located
		}
	}
	if wire.StoryId != "" {
		if story, ok := c.stories[wire.StoryId]; ok && story != nil {
			wire.Category = story.Category
			if !wire.Located && len(story.Locations) > 0 {
				idx := int(story.Primary)
				if idx < 0 || idx >= len(story.Locations) {
					idx = 0
				}
				loc := story.Locations[idx]
				if loc.Lat != 0 || loc.Lon != 0 {
					wire.Lat = float64(loc.Lat)
					wire.Lon = float64(loc.Lon)
					wire.Located = true
				}
			}
		}
	}
	if placed {
		c.placed[a.Id] = true
	}
	if score := c.scores[a.Id]; score != nil {
		wire.PromptScores, wire.ApproxPrompts = score.Scores, score.Approx
	}
	if _, ok := c.wireRows[wire.ArticleId]; !ok {
		c.wireOrder = append(c.wireOrder, wire.ArticleId)
	}
	c.wireRows[wire.ArticleId] = wire
	for len(c.wireOrder) > 1500 {
		old := c.wireOrder[0]
		c.wireOrder = c.wireOrder[1:]
		delete(c.wireRows, old)
		delete(c.scores, old)
		if i := slices.Index(c.scoreOrder, old); i >= 0 {
			c.scoreOrder = slices.Delete(c.scoreOrder, i, i+1)
		}
		delete(c.placed, old)
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

// onMatchScore folds matcher scores into the latest wire row. The publisher
// emits a new delta with a new sequence; the browser upserts by article id.
func (c *publisherCore) onMatchScore(score *dorv1.MatchScore) {
	if score == nil || score.ArticleId == "" {
		return
	}
	c.mu.Lock()
	if c.scores[score.ArticleId] == nil {
		c.scoreOrder = append(c.scoreOrder, score.ArticleId)
	}
	c.scores[score.ArticleId] = proto.Clone(score).(*dorv1.MatchScore)
	// Bound pending pre-wire scores separately from active wire scores. Evict
	// oldest pending IDs deterministically; active score updates cannot randomly
	// erase an unrelated article still waiting for its first wire row.
	pending := 0
	for _, id := range c.scoreOrder {
		if c.wireRows[id] == nil {
			pending++
		}
	}
	for i := 0; pending > 1500 && i < len(c.scoreOrder); {
		id := c.scoreOrder[i]
		if c.wireRows[id] == nil {
			delete(c.scores, id)
			c.scoreOrder = slices.Delete(c.scoreOrder, i, i+1)
			pending--
		} else {
			i++
		}
	}
	row, ok := c.wireRows[score.ArticleId]
	if ok {
		row = proto.Clone(row).(*dorv1.WireRow)
		row.PromptScores = score.Scores
		row.ApproxPrompts = score.Approx
		c.wireRows[score.ArticleId] = row
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	c.enqueue("wire:"+row.ArticleId, &dorv1.Delta{Body: &dorv1.Delta_Wire{Wire: row}})
}

func (c *publisherCore) onAlert(hit *dorv1.AlertHit) {
	if hit == nil || hit.AlertId == "" {
		return
	}
	key := publisherAlertKey(hit)
	c.enqueue(key, &dorv1.Delta{Body: &dorv1.Delta_Alert{Alert: hit}})
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
	c.mu.Unlock()
}

func publisherLinkKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

func publisherAlertKey(hit *dorv1.AlertHit) string {
	return fmt.Sprintf("alert:%s:%s:%s:%d", hit.AlertId, hit.StoryId, hit.Level, hit.Generation)
}
