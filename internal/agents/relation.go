package agents

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
)

// RelationAgentName is the registry name of the relation judge.
const RelationAgentName = "relation"

// LinkSink is the non-blocking durable mirror for links. *store.Writer
// satisfies it; nil keeps the relation judge KV-only.
type LinkSink interface {
	EnqueueLink(store.LinkRecord) bool
}

// RelateDecider is the jev surface used here. *decide.Client satisfies it.
type RelateDecider interface {
	Decide(ctx context.Context, req decide.Request) (decide.Response, error)
}

// RelationConfig configures the judge.
type RelationConfig struct {
	Decider    RelateDecider
	Persist    LinkSink
	Thresholds config.Thresholds
	Shards     int
	Shard      int // -1 runs every shard in this process
	Debounce   time.Duration
	Workers    int
	// LinkHalfLife is the heat-scaled half-life of a relation's strength.
	// Zero uses the production default.
	LinkHalfLife time.Duration
	Now          func() time.Time
}

const (
	relationLinkHalfLife = 12 * time.Hour
	relationDecayEvery   = 30 * time.Second
	relationDecayMinStep = 0.02
	relationLinkFloor    = 0.05
	relationPairLocks    = 128
)

// NewRelationJudge builds the sharded relation judge. With a nil decider it
// emits similarity-only thematic links marked unverified.
func NewRelationJudge(b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg RelationConfig) (Agent, error) {
	if cfg.Shards <= 0 {
		cfg.Shards = 3
	}
	if cfg.Debounce <= 0 {
		cfg.Debounce = 3 * time.Second
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.LinkHalfLife <= 0 {
		cfg.LinkHalfLife = relationLinkHalfLife
	}
	core := &relationCore{
		bus:          b,
		log:          log,
		cfg:          cfg,
		now:          cfg.Now,
		index:        make(map[string]*relationEntry),
		pending:      make(map[string]time.Time),
		links:        make(map[string]*relationLink),
		sink:         cfg.Persist,
		work:         make(chan string, 64),
		pairLocks:    make([]sync.Mutex, relationPairLocks),
		shards:       cfg.Shards,
		shard:        cfg.Shard,
		linkHalfLife: cfg.LinkHalfLife,
	}
	filters, err := relationFilters(cfg.Shards, cfg.Shard)
	if err != nil {
		return nil, err
	}
	durable := "relation"
	if cfg.Shard >= 0 {
		durable = fmt.Sprintf("relation-%d", cfg.Shard)
	}
	base, err := NewBase(b, metrics, log, Spec{
		Name:           RelationAgentName,
		Durable:        durable,
		FilterSubjects: filters,
		Ordered:        true,
		MaxAckPending:  1,
		AckWait:        60 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var story dorv1.Story
			if err := proto.Unmarshal(msg.Data, &story); err != nil {
				return Terminal(fmt.Errorf("relation: story: %w", err))
			}
			core.observe(ctx, &story)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &relationAgent{name: RelationAgentName, core: core, base: base}, nil
}

// relationFilters binds every story shard on every judge. Candidate indexing
// must be global: a pair can only link when the judge can see both endpoint
// stories, regardless of which shard owns the judgement. Judgement ownership is
// decided per pair in ownsPair, not by the input filter.
func relationFilters(shards, shard int) ([]string, error) {
	if shard >= 0 && shard >= shards {
		return nil, fmt.Errorf("relation: shard %d is outside 0..%d", shard, shards-1)
	}
	out := make([]string, 0, bus.StoryShardCount)
	for i := 0; i < bus.StoryShardCount; i++ {
		out = append(out, bus.StoryChangedSubjectForShard(i))
	}
	return out, nil
}

type relationAgent struct {
	name string
	core *relationCore
	base *Base

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (r *relationAgent) Name() string { return r.name }

func (r *relationAgent) Start(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: %s already started", r.name)
	}
	if r.stopped.Load() {
		return fmt.Errorf("agents: %s is stopped", r.name)
	}
	if err := r.core.init(ctx); err != nil {
		return err
	}
	if err := r.base.Start(ctx); err != nil {
		return err
	}
	r.startLoops(ctx)
	r.core.log.Info("relation judge started", "shard", r.core.cfg.Shard, "shards", r.core.cfg.Shards)
	return nil
}

func (r *relationAgent) startLoops(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	r.running.Store(true)
	r.wg.Add(1 + r.core.cfg.Workers)
	go func() {
		defer r.wg.Done()
		r.core.scheduleLoop(runCtx)
	}()
	for i := 0; i < r.core.cfg.Workers; i++ {
		go func() {
			defer r.wg.Done()
			r.core.worker(runCtx)
		}()
	}
}

func (r *relationAgent) Restart(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	r.running.Store(false)
	r.stopped.Store(false)
	if err := r.base.Restart(ctx); err != nil {
		return err
	}
	r.startLoops(context.WithoutCancel(ctx))
	r.core.log.Info("relation judge restarted", "shard", r.core.cfg.Shard, "shards", r.core.cfg.Shards)
	return nil
}

func (r *relationAgent) Consume(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (r *relationAgent) Stop(ctx context.Context) error {
	if r.stopped.Swap(true) {
		return nil
	}
	r.running.Store(false)
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	return r.base.Stop(ctx)
}

func (r *relationAgent) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	return r.base.Publish(ctx, subject, data, opts...)
}

func (r *relationAgent) Running() bool {
	return r.running.Load() && !r.stopped.Load() && r.base.Running()
}

func (r *relationAgent) LastError() error { return r.base.LastError() }
func (r *relationAgent) TerminalCount() int64 {
	return r.base.TerminalCount()
}

type relationEntry struct {
	story     *dorv1.Story
	meta      live.StoryMeta
	vec       []float32
	entities  []string
	lat, lon  float64
	firstSeen int64
	updated   int64
	// rev is the local revision of this observation. A judgement captures the
	// revisions of both endpoints and discards its result if either moved.
	rev uint64
}

// relationLink is the in-memory relation plus the strength last persisted and
// published. Decay accumulates in memory and emits only visible steps.
type relationLink struct {
	link              *dorv1.Link
	publishedStrength float64
}

type relationCore struct {
	bus *bus.Bus
	log *slog.Logger
	cfg RelationConfig
	now func() time.Time

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error

	mu        sync.Mutex
	index     map[string]*relationEntry
	pending   map[string]time.Time
	links     map[string]*relationLink
	sink      LinkSink
	work      chan string
	pairLocks []sync.Mutex
	revision  atomic.Uint64
	lastDecay time.Time

	shards       int
	shard        int
	linkHalfLife time.Duration
}

func (c *relationCore) init(ctx context.Context) error {
	c.kvOnce.Do(func() {
		c.kv, c.kvErr = c.bus.KV(ctx, bus.KVBucketLiveState)
	})
	if c.kvErr != nil {
		c.log.Warn("relation: live_state unavailable; links are in-memory only", "err", c.kvErr)
		c.kv = nil
	}
	c.lastDecay = c.now()
	if c.kv != nil {
		restoreCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		c.restore(restoreCtx)
	}
	return nil
}

// restore rebuilds the candidate index from all live stories (not just this
// shard's input) and the link table from live state. Without it a restarted
// judge cannot re-evaluate or decay existing links.
func (c *relationCore) restore(ctx context.Context) {
	now := c.now().UTC()
	lister, err := c.kv.ListKeysFiltered(ctx, "story.*")
	if err != nil {
		if !isKVNotFound(err) {
			c.log.Warn("relation: restore stories", "err", err)
		}
		return
	}
	restored := 0
	for key := range lister.Keys() {
		if strings.HasPrefix(key, "storymeta.") || strings.HasPrefix(key, "storymap.") {
			continue
		}
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var story dorv1.Story
		if proto.Unmarshal(entry.Value(), &story) != nil || story.Id == "" {
			continue
		}
		updated := story.UpdatedAt
		if updated == 0 {
			updated = story.FirstSeen
		}
		if updated == 0 || now.Sub(time.Unix(updated, 0)) > clusterStoryTTL {
			continue
		}
		rel := &relationEntry{
			story:     &story,
			firstSeen: story.FirstSeen,
			updated:   updated,
			rev:       c.revision.Add(1),
		}
		if rel.firstSeen == 0 {
			rel.firstSeen = updated
		}
		if meta, ok := live.LoadStoryMeta(ctx, c.kv, story.Id); ok {
			rel.meta = meta
			rel.vec = meta.Embedding
			rel.entities = meta.Entities
			if meta.FirstSeen > 0 {
				rel.firstSeen = meta.FirstSeen
			}
		}
		rel.lat, rel.lon = storyCoords(&story)
		c.index[story.Id] = rel
		restored++
	}
	_ = lister.Stop()

	linkLister, err := c.kv.ListKeysFiltered(ctx, "link.*")
	if err != nil {
		if !isKVNotFound(err) {
			c.log.Warn("relation: restore links", "err", err)
		}
		return
	}
	links := 0
	for key := range linkLister.Keys() {
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var link dorv1.Link
		if proto.Unmarshal(entry.Value(), &link) != nil {
			continue
		}
		link.A, link.B = live.SortPair(link.A, link.B)
		if link.A == "" || link.B == "" || (link.Strength <= 0 && link.Confidence <= 0) {
			// A removal tombstone already did its job.
			_ = c.kv.Delete(ctx, key)
			continue
		}
		if c.index[link.A] == nil || c.index[link.B] == nil {
			// The endpoint expired while this judge was down.
			_ = c.kv.Delete(ctx, key)
			continue
		}
		c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength)}
		links++
	}
	_ = linkLister.Stop()
	c.log.Info("relation: restored live state", "stories", restored, "links", links)
}

// observe indexes a story on every judge and schedules a debounced judgement.
// Judgement execution is still partitioned per pair in ownsPair, so observing
// on every shard does not double-judge a pair.
func (c *relationCore) observe(ctx context.Context, story *dorv1.Story) {
	if story == nil || story.Id == "" {
		return
	}
	now := c.now()
	entry := &relationEntry{story: story, firstSeen: story.FirstSeen}
	if entry.firstSeen == 0 {
		entry.firstSeen = now.Unix()
	}
	if meta, ok := live.LoadStoryMeta(ctx, c.kv, story.Id); ok {
		entry.meta = meta
		entry.vec = meta.Embedding
		entry.entities = meta.Entities
		if meta.FirstSeen > 0 {
			entry.firstSeen = meta.FirstSeen
		}
	}
	entry.lat, entry.lon = storyCoords(story)
	// Recency is the story's content age, not the observation time: cooling
	// deltas carry an unchanged UpdatedAt and must not keep the index alive
	// past the 48 h live-state horizon.
	entry.updated = story.UpdatedAt
	if entry.updated <= 0 {
		entry.updated = now.Unix()
	}
	if entry.updated > now.Unix() {
		entry.updated = now.Unix()
	}
	entry.rev = c.revision.Add(1)
	c.mu.Lock()
	if prev := c.index[story.Id]; prev != nil {
		// A metadata read can race the clusterer's write. Do not lose the
		// embedding/entities or the original event time on a partial read.
		if len(entry.vec) == 0 {
			entry.vec = prev.vec
		}
		if len(entry.entities) == 0 {
			entry.entities = prev.entities
		}
		if entry.firstSeen == now.Unix() && prev.firstSeen > 0 {
			entry.firstSeen = prev.firstSeen
		}
	}
	c.index[story.Id] = entry
	c.pending[story.Id] = now.Add(c.cfg.Debounce)
	c.mu.Unlock()
}

func storyCoords(story *dorv1.Story) (float64, float64) {
	if story == nil || len(story.Locations) == 0 {
		return 0, 0
	}
	idx := int(story.Primary)
	if idx < 0 || idx >= len(story.Locations) {
		idx = 0
	}
	if story.Locations[idx] == nil {
		return 0, 0
	}
	return float64(story.Locations[idx].Lat), float64(story.Locations[idx].Lon)
}

// pairKey is the stable pair identity used by the link table and locks.
func pairKey(a, b string) string {
	a, b = live.SortPair(a, b)
	return a + "." + b
}

// pairShard assigns each unordered pair to exactly one judge. Both endpoint
// updates reach every judge through the global index, so the owner always has a
// trigger even when neither endpoint's story shard belongs to it.
func pairShard(a, b string, shards int) int {
	if shards <= 1 {
		return 0
	}
	a, b = live.SortPair(a, b)
	return bus.StoryShard(a+"|"+b) % shards
}

func (c *relationCore) ownsPair(a, b string) bool {
	return c.shard < 0 || pairShard(a, b, c.shards) == c.shard
}

func (c *relationCore) lockPair(a, b string) *sync.Mutex {
	a, b = live.SortPair(a, b)
	key := a + "|" + b
	h := bus.StoryShard(key) * 131
	return &c.pairLocks[h%len(c.pairLocks)]
}

func (c *relationCore) scheduleLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := c.now()
			var due []string
			c.mu.Lock()
			for id, at := range c.pending {
				if !now.Before(at) {
					due = append(due, id)
				}
			}
			c.mu.Unlock()
			for _, id := range due {
				select {
				case c.work <- id:
					c.mu.Lock()
					delete(c.pending, id)
					c.mu.Unlock()
				default:
					c.mu.Lock()
					c.pending[id] = now.Add(500 * time.Millisecond)
					c.mu.Unlock()
				}
			}
			c.decay(ctx, now)
		}
	}
}

func (c *relationCore) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-c.work:
			c.judge(ctx, id)
		}
	}
}

// decay cools links with the heat of their endpoint stories, evicts the 48 h
// index and cascades link removal for evicted stories. Visible steps are
// persisted and published; small changes accumulate in memory first.
func (c *relationCore) decay(ctx context.Context, now time.Time) {
	if !c.lastDecay.IsZero() && now.Sub(c.lastDecay) < relationDecayEvery {
		return
	}
	dt := now.Sub(c.lastDecay)
	if c.lastDecay.IsZero() || dt < 0 {
		dt = relationDecayEvery
	}
	c.lastDecay = now

	type victim struct {
		a, b string
	}
	type fade struct {
		a, b     string
		revision uint64
		heat     float64
	}
	var victims []victim
	var fades []fade
	c.mu.Lock()
	cutoff := now.Add(-clusterStoryTTL).Unix()
	for id, e := range c.index {
		if e == nil || e.updated < cutoff {
			delete(c.index, id)
			delete(c.pending, id)
		}
	}
	for key, l := range c.links {
		if l == nil || l.link == nil {
			delete(c.links, key)
			continue
		}
		a, b := l.link.A, l.link.B
		ea, eb := c.index[a], c.index[b]
		if ea == nil || eb == nil {
			// Leave the table entry for removeLinkLocked to delete. Deleting
			// it here would make the tombstone write a no-op.
			victims = append(victims, victim{a: a, b: b})
			continue
		}
		if c.ownsPair(a, b) {
			heat := math.Max(float64(ea.story.GetHeat()), float64(eb.story.GetHeat()))
			if heat < 0 {
				heat = 0
			}
			if heat > 1 {
				heat = 1
			}
			fades = append(fades, fade{a: a, b: b, revision: live.LinkRevision(l.link), heat: heat})
		}
	}
	c.mu.Unlock()

	for _, v := range victims {
		c.tombstone(ctx, v.a, v.b)
	}
	for _, f := range fades {
		c.fadeLink(ctx, f.a, f.b, f.revision, f.heat, dt)
	}
}

// fadeLink applies heat-scaled decay to one link. Hot stories keep their links
// longer; quiet stories lose strength faster. A new judgement or removal
// between the snapshot and this write changes the revision and cancels it.
func (c *relationCore) fadeLink(ctx context.Context, a, b string, revision uint64, heat float64, dt time.Duration) {
	lock := c.lockPair(a, b)
	lock.Lock()
	defer lock.Unlock()

	key := pairKey(a, b)
	c.mu.Lock()
	rel := c.links[key]
	if rel == nil || rel.link == nil || live.LinkRevision(rel.link) != revision {
		c.mu.Unlock()
		return
	}
	// A hot story halves the decay rate; a cold one quadruples it.
	tau := c.linkHalfLife.Seconds() * (0.2 + 0.8*heat)
	if tau <= 0 {
		tau = c.linkHalfLife.Seconds()
	}
	factor := math.Exp(-dt.Seconds() / tau)
	strength := float64(rel.link.Strength) * factor
	confidence := float64(rel.link.Confidence) * factor
	if strength < 0 {
		strength = 0
	}
	if confidence < 0 {
		confidence = 0
	}
	rel.link.Strength = float32(strength)
	rel.link.Confidence = float32(confidence)
	shouldPublish := rel.publishedStrength-strength >= relationDecayMinStep
	shouldRemove := strength < relationLinkFloor || confidence < relationLinkFloor
	if shouldRemove {
		c.mu.Unlock()
		c.removeLinkLocked(ctx, a, b)
		return
	}
	if !shouldPublish {
		c.mu.Unlock()
		return
	}
	link := rel.link
	c.mu.Unlock()
	c.upsertLinkLocked(ctx, link)
}

func (c *relationCore) lockForPairKey(key string) *sync.Mutex {
	h := bus.StoryShard(key)*131 + 17
	return &c.pairLocks[h%len(c.pairLocks)]
}

// tombstone removes a link whose endpoint left the index. It is safe when the
// pair was never in this process's table.
func (c *relationCore) tombstone(ctx context.Context, a, b string) {
	lock := c.lockPair(a, b)
	lock.Lock()
	defer lock.Unlock()
	c.removeLinkLocked(ctx, a, b)
}

// invalidateLinkText drops the cached "why" for a changed relation. The
// explainer checks the revision itself; deleting here keeps live_state tidy.
func (c *relationCore) invalidateLinkText(ctx context.Context, a, b string) {
	if c.kv == nil {
		return
	}
	_ = c.kv.Delete(ctx, live.LinkTextKey(a, b))
}

func (c *relationCore) judge(ctx context.Context, id string) {
	c.mu.Lock()
	entry := c.index[id]
	c.mu.Unlock()
	if entry == nil {
		return
	}
	candidates := c.candidates(entry, c.now())
	for _, other := range candidates {
		if ctx.Err() != nil {
			return
		}
		if !c.ownsPair(entry.story.Id, other.story.Id) {
			continue
		}
		c.judgePair(ctx, entry, other)
	}
}

// judgePair serialises one pair, judges it from the current story revisions and
// discards a result whose inputs changed while the model was answering. A
// changed story is rescheduled by observe, so the pair is re-judged with the
// newer state instead of letting a stale answer win.
func (c *relationCore) judgePair(ctx context.Context, a, b *relationEntry) {
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	if aid == bid {
		return
	}
	lock := c.lockPair(aid, bid)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	ea, eb := c.index[aid], c.index[bid]
	c.mu.Unlock()
	if ea == nil || eb == nil {
		return
	}
	revA, revB := ea.rev, eb.rev

	// The request order is the sorted order, so the direction is relative to
	// the same pair the Link stores.
	first, second := ea, eb
	if first.story.Id != aid {
		first, second = eb, ea
	}
	inA := c.storyInput(ctx, first)
	inB := c.storyInput(ctx, second)
	if c.cfg.Decider == nil {
		c.fallbackFenced(ctx, aid, revA, bid, revB, ea, eb)
		return
	}
	priority := relationPriority(ea, eb, c.now())
	resp, err := c.cfg.Decider.Decide(ctx, relateRequest(inA, inB, priority))
	if err != nil {
		c.log.Debug("relation: jev unavailable; similarity-only link", "a", aid, "b", bid, "err", err)
		c.fallbackFenced(ctx, aid, revA, bid, revB, ea, eb)
		return
	}
	res := decide.CombineRelate(resp, inA, inB, c.cfg.Thresholds)
	if !res.Answered {
		c.fallbackFenced(ctx, aid, revA, bid, revB, ea, eb)
		return
	}
	if !c.currentRevisions(aid, revA, bid, revB) {
		c.reschedule(aid)
		c.reschedule(bid)
		return
	}
	if !res.Accepted {
		c.removeLinkLocked(ctx, aid, bid)
		return
	}
	link := &dorv1.Link{
		A:          aid,
		B:          bid,
		Type:       linkType(res.Type),
		Dir:        linkDir(res.Direction),
		Confidence: float32(res.RelatedP),
		Strength:   float32(res.Strength),
		UpdatedAt:  c.now().Unix(),
	}
	c.upsertLinkLocked(ctx, link)
}

func (c *relationCore) currentRevisions(aid string, revA uint64, bid string, revB uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ea, eb := c.index[aid], c.index[bid]
	return ea != nil && eb != nil && ea.rev == revA && eb.rev == revB
}

func (c *relationCore) reschedule(id string) {
	c.mu.Lock()
	c.pending[id] = c.now().Add(c.cfg.Debounce)
	c.mu.Unlock()
}

// relationPriority tags a likely causal/reaction pair as default work so the
// documented thematic shed order (95% budget) cannot drop it. Shared named
// entities plus a bounded time gap, or a same-category pair close in time and
// place, are the cheap signals available before the model answers.
func relationPriority(a, b *relationEntry, now time.Time) budget.Priority {
	if a == nil || b == nil {
		return budget.PriorityThematic
	}
	gap := math.Abs(float64(a.firstSeen - b.firstSeen))
	if shareEntity(a.entities, b.entities) && gap <= 48*3600 {
		return budget.PriorityDefault
	}
	sameCategory := a.story.GetCategory() != dorv1.Category_CATEGORY_OTHER && a.story.GetCategory() == b.story.GetCategory()
	if sameCategory && gap <= 24*3600 && validCoords(a.lat, a.lon) && validCoords(b.lat, b.lon) {
		if haversineKM(a.lat, a.lon, b.lat, b.lon) <= 150 {
			return budget.PriorityDefault
		}
	}
	return budget.PriorityThematic
}

// fallbackFenced runs the deterministic fallback only when both inputs still
// hold the revisions the judgement started with.
func (c *relationCore) fallbackFenced(ctx context.Context, aid string, revA uint64, bid string, revB uint64, a, b *relationEntry) {
	if !c.currentRevisions(aid, revA, bid, revB) {
		c.reschedule(aid)
		c.reschedule(bid)
		return
	}
	c.fallbackLocked(ctx, a, b)
}

// fallbackLocked emits a similarity-only thematic link, explicitly unverified.
// It never invents a causal or reaction type. The caller holds the pair lock.
func (c *relationCore) fallbackLocked(ctx context.Context, a, b *relationEntry) {
	score := cheapScore(a, b, c.now())
	if score < 0.3 {
		return
	}
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	c.upsertLinkLocked(ctx, &dorv1.Link{
		A:          aid,
		B:          bid,
		Type:       dorv1.LinkType_LINK_TYPE_THEMATIC,
		Dir:        dorv1.Direction_DIRECTION_UNCLEAR,
		Confidence: float32(score),
		Strength:   float32(score),
		UpdatedAt:  c.now().Unix(),
		Unverified: true,
	})
}

// relateRequest sets the budget priority the judge selected from cheap pair
// signals. A decider implementation that ignores Request.Priority still works.
func relateRequest(a, b decide.Story, priority budget.Priority) decide.Request {
	req := decide.RelateRequest(a, b)
	req.Priority = priority
	return req
}

// upsertLinkLocked writes one relation. The caller holds the pair lock.
func (c *relationCore) upsertLinkLocked(ctx context.Context, link *dorv1.Link) {
	if link == nil || link.A == "" || link.B == "" {
		return
	}
	link.A, link.B = live.SortPair(link.A, link.B)
	if link.UpdatedAt == 0 {
		link.UpdatedAt = c.now().Unix()
	}
	live.PutLink(ctx, c.kv, link)
	key := pairKey(link.A, link.B)
	c.mu.Lock()
	c.links[key] = &relationLink{link: link, publishedStrength: float64(link.Strength)}
	c.mu.Unlock()
	c.invalidateLinkText(ctx, link.A, link.B)
	if c.sink != nil {
		c.sink.EnqueueLink(store.LinkRecord{
			A: link.A, B: link.B, Type: int16(link.Type), Direction: int16(link.Dir),
			Confidence: link.Confidence, Strength: link.Strength, Unverified: link.Unverified,
			UpdatedAt: time.Unix(link.UpdatedAt, 0).UTC(),
			DecaysAt:  time.Unix(link.UpdatedAt, 0).UTC().Add(48 * time.Hour),
		})
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data,
		bus.WithMsgID(linkMessageID(link)))
	if err != nil {
		c.log.Warn("relation: publish link", "a", link.A, "b", link.B, "err", err)
	}
}

// linkMessageID is revision-based, not second-resolution. Two different
// judgements in the same second therefore have different ids, while a replay
// of the identical relation is deduplicated by JetStream.
func linkMessageID(link *dorv1.Link) string {
	return fmt.Sprintf("link:%s.%s:%016x", link.A, link.B, live.LinkRevision(link))
}

// removeLinkLocked writes the zero-strength tombstone the publisher maps to
// Delta.link_remove. The caller holds the pair lock. A pair that exists in KV
// but not in this process's table is still removed.
func (c *relationCore) removeLinkLocked(ctx context.Context, a, b string) {
	a, b = live.SortPair(a, b)
	key := pairKey(a, b)
	c.mu.Lock()
	existing := c.links[key]
	delete(c.links, key)
	c.mu.Unlock()
	if existing == nil {
		return
	}
	tombstone := &dorv1.Link{
		A:         a,
		B:         b,
		UpdatedAt: c.now().Unix(),
	}
	if existing != nil && existing.link != nil {
		tombstone.Type = existing.link.Type
		tombstone.Dir = existing.link.Dir
	}
	if c.sink != nil {
		c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true})
	}
	live.PutLink(ctx, c.kv, tombstone)
	c.invalidateLinkText(ctx, a, b)
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(tombstone)
	if err != nil {
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, _ = c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data,
		bus.WithMsgID(linkMessageID(tombstone)))
}

func (c *relationCore) storyInput(ctx context.Context, e *relationEntry) decide.Story {
	in := decide.Story{
		ID:       e.story.Id,
		Title:    e.story.Title,
		Entities: e.entities,
		Place:    storyPlace(e.story),
		Category: categoryName(e.story.Category),
	}
	if brief, ok := live.LoadStoryText(ctx, c.kv, e.story.Id); ok && brief.Available {
		in.Brief = brief.Brief
	}
	return in
}

func categoryName(c dorv1.Category) string {
	return lowerASCII(strings.TrimPrefix(c.String(), "CATEGORY_"))
}

func lowerASCII(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		out = append(out, ch)
	}
	return string(out)
}

func storyPlace(story *dorv1.Story) string {
	if story == nil || len(story.Locations) == 0 {
		return ""
	}
	idx := int(story.Primary)
	if idx < 0 || idx >= len(story.Locations) {
		idx = 0
	}
	return story.Locations[idx].Name
}

func linkType(t string) dorv1.LinkType {
	switch t {
	case "causal":
		return dorv1.LinkType_LINK_TYPE_CAUSAL
	case "reaction":
		return dorv1.LinkType_LINK_TYPE_REACTION
	default:
		return dorv1.LinkType_LINK_TYPE_THEMATIC
	}
}

func linkDir(d string) dorv1.Direction {
	switch d {
	case "a_to_b":
		return dorv1.Direction_DIRECTION_A_TO_B
	case "b_to_a":
		return dorv1.Direction_DIRECTION_B_TO_A
	default:
		return dorv1.Direction_DIRECTION_UNCLEAR
	}
}

// candidates ranks recent stories cheaply: cosine, rarity-weighted entity
// overlap, time proximity and place distance. It returns top-20 neighbours plus
// up to ten entity-sharing stories, capped at 30 and then 12 finalists.
func (c *relationCore) candidates(self *relationEntry, now time.Time) []*relationEntry {
	c.mu.Lock()
	all := make([]*relationEntry, 0, len(c.index))
	for id, e := range c.index {
		if id == self.story.Id {
			continue
		}
		if now.Unix()-e.updated > int64((48 * time.Hour).Seconds()) {
			continue
		}
		all = append(all, e)
	}
	c.mu.Unlock()
	type scored struct {
		entry *relationEntry
		score float64
		share bool
	}
	list := make([]scored, 0, len(all))
	for _, e := range all {
		score := cheapScore(self, e, now)
		share := shareEntity(self.entities, e.entities)
		if score < 0.2 && !share {
			continue
		}
		list = append(list, scored{entry: e, score: score, share: share})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].score != list[j].score {
			return list[i].score > list[j].score
		}
		return list[i].entry.story.Id < list[j].entry.story.Id
	})
	top := list
	if len(top) > 20 {
		top = top[:20]
	}
	seen := make(map[string]bool, len(top))
	out := make([]*relationEntry, 0, 30)
	for _, s := range top {
		seen[s.entry.story.Id] = true
		out = append(out, s.entry)
	}
	entityAdded := 0
	for _, s := range list {
		if entityAdded >= 10 || len(out) >= 30 {
			break
		}
		if !s.share || seen[s.entry.story.Id] {
			continue
		}
		seen[s.entry.story.Id] = true
		out = append(out, s.entry)
		entityAdded++
	}
	if len(out) > 12 {
		out = out[:12]
	}
	return out
}

func cheapScore(a, b *relationEntry, now time.Time) float64 {
	cos := cosine(a.vec, b.vec)
	ent := entityOverlap(a.entities, b.entities)
	gap := math.Abs(float64(a.firstSeen - b.firstSeen))
	timeProx := math.Exp(-gap / 86400)
	place := 0.5
	if validCoords(a.lat, a.lon) && validCoords(b.lat, b.lon) {
		place = math.Exp(-haversineKM(a.lat, a.lon, b.lat, b.lon) / 2000)
	}
	return 0.5*cos + 0.25*ent + 0.15*timeProx + 0.10*place
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func entityOverlap(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[string]bool, len(a))
	for _, e := range a {
		set[normalizeEntity(e)] = true
	}
	shared := 0
	seen := make(map[string]bool, len(b))
	for _, e := range b {
		e = normalizeEntity(e)
		if e == "" || seen[e] {
			continue
		}
		seen[e] = true
		if set[e] {
			shared++
		}
	}
	union := len(set) + len(seen) - shared
	if union == 0 {
		return 0
	}
	return float64(shared) / float64(union)
}

func shareEntity(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, e := range a {
		set[normalizeEntity(e)] = true
	}
	for _, e := range b {
		if set[normalizeEntity(e)] {
			return true
		}
	}
	return false
}

func normalizeEntity(e string) string {
	out := make([]rune, 0, len(e))
	for _, r := range e {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		out = append(out, r)
	}
	return string(out)
}

func validCoords(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)
}

func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371
	phi1, phi2 := lat1*math.Pi/180, lat2*math.Pi/180
	dphi := (lat2 - lat1) * math.Pi / 180
	dlambda := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dphi/2)*math.Sin(dphi/2) +
		math.Cos(phi1)*math.Cos(phi2)*math.Sin(dlambda/2)*math.Sin(dlambda/2)
	return 2 * r * math.Asin(math.Min(1, math.Sqrt(a)))
}
