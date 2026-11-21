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
	Now        func() time.Time
}

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
	core := &relationCore{
		bus:     b,
		log:     log,
		cfg:     cfg,
		now:     cfg.Now,
		index:   make(map[string]*relationEntry),
		pending: make(map[string]time.Time),
		links:   make(map[string]*dorv1.Link),
		sink:    cfg.Persist,
		work:    make(chan string, 64),
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

func relationFilters(shards, shard int) ([]string, error) {
	if shard < 0 {
		out := make([]string, 0, bus.StoryShardCount)
		for i := 0; i < bus.StoryShardCount; i++ {
			out = append(out, bus.StoryChangedSubjectForShard(i))
		}
		return out, nil
	}
	if shard >= shards {
		return nil, fmt.Errorf("relation: shard %d is outside 0..%d", shard, shards-1)
	}
	var out []string
	for i := shard; i < bus.StoryShardCount; i += shards {
		out = append(out, bus.StoryChangedSubjectForShard(i))
	}
	return out, nil
}

type relationAgent struct {
	name string
	core *relationCore
	base Agent

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
	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	if err := r.core.init(runCtx); err != nil {
		cancel()
		return err
	}
	if err := r.base.Start(ctx); err != nil {
		cancel()
		return err
	}
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
	r.core.log.Info("relation judge started", "shard", r.core.cfg.Shard, "shards", r.core.cfg.Shards)
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
	return r.running.Load() && !r.stopped.Load() && r.base.(HealthReporter).Running()
}

func (r *relationAgent) LastError() error { return r.base.(HealthReporter).LastError() }
func (r *relationAgent) TerminalCount() int64 {
	return r.base.(HealthReporter).TerminalCount()
}

type relationEntry struct {
	story    *dorv1.Story
	meta     live.StoryMeta
	vec      []float32
	entities []string
	lat, lon float64
	seen     int64
}

type relationCore struct {
	bus *bus.Bus
	log *slog.Logger
	cfg RelationConfig
	now func() time.Time

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error

	mu      sync.Mutex
	index   map[string]*relationEntry
	pending map[string]time.Time
	links   map[string]*dorv1.Link
	sink    LinkSink
	work    chan string
}

func (c *relationCore) init(ctx context.Context) error {
	c.kvOnce.Do(func() {
		c.kv, c.kvErr = c.bus.KV(ctx, bus.KVBucketLiveState)
	})
	if c.kvErr != nil {
		c.log.Warn("relation: live_state unavailable; links are in-memory only", "err", c.kvErr)
		c.kv = nil
	}
	return nil
}

func (c *relationCore) observe(ctx context.Context, story *dorv1.Story) {
	if story == nil || story.Id == "" {
		return
	}
	entry := &relationEntry{story: story, vec: nil, seen: story.FirstSeen}
	if entry.seen == 0 {
		entry.seen = c.now().Unix()
	}
	if meta, ok := live.LoadStoryMeta(ctx, c.kv, story.Id); ok {
		entry.meta = meta
		entry.vec = meta.Embedding
		entry.entities = meta.Entities
		if meta.FirstSeen > 0 {
			entry.seen = meta.FirstSeen
		}
	}
	if idx := int(story.Primary); idx >= 0 && idx < len(story.Locations) {
		entry.lat = float64(story.Locations[idx].Lat)
		entry.lon = float64(story.Locations[idx].Lon)
	}
	c.mu.Lock()
	c.index[story.Id] = entry
	c.pending[story.Id] = c.now().Add(c.cfg.Debounce)
	c.mu.Unlock()
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

func (c *relationCore) decay(ctx context.Context, now time.Time) {
	c.mu.Lock()
	var remove []*dorv1.Link
	for _, l := range c.links {
		if now.Unix()-l.UpdatedAt > int64((48 * time.Hour).Seconds()) {
			remove = append(remove, l)
		}
	}
	c.mu.Unlock()
	for _, l := range remove {
		c.removeLink(ctx, l.A, l.B)
	}
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
		c.judgePair(ctx, entry, other)
	}
}

func (c *relationCore) judgePair(ctx context.Context, a, b *relationEntry) {
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	if aid == bid {
		return
	}
	// The request order is the sorted order, so the direction is relative to
	// the same pair the Link stores.
	first, second := a, b
	if first.story.Id != aid {
		first, second = b, a
	}
	inA := c.storyInput(ctx, first)
	inB := c.storyInput(ctx, second)
	if c.cfg.Decider == nil {
		c.fallback(ctx, a, b)
		return
	}
	resp, err := c.cfg.Decider.Decide(ctx, decide.RelateRequest(inA, inB))
	if err != nil {
		c.log.Debug("relation: jev unavailable; similarity-only link", "a", aid, "b", bid, "err", err)
		c.fallback(ctx, a, b)
		return
	}
	res := decide.CombineRelate(resp, inA, inB, c.cfg.Thresholds)
	if !res.Answered {
		c.fallback(ctx, a, b)
		return
	}
	if !res.Accepted {
		c.removeLink(ctx, aid, bid)
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
	c.upsertLink(ctx, link)
}

// fallback emits a similarity-only thematic link, explicitly unverified. It
// never invents a causal or reaction type.
func (c *relationCore) fallback(ctx context.Context, a, b *relationEntry) {
	score := cheapScore(a, b, c.now())
	if score < 0.3 {
		return
	}
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	c.upsertLink(ctx, &dorv1.Link{
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

func (c *relationCore) upsertLink(ctx context.Context, link *dorv1.Link) {
	if link == nil || link.A == "" || link.B == "" {
		return
	}
	link.A, link.B = live.SortPair(link.A, link.B)
	live.PutLink(ctx, c.kv, link)
	c.mu.Lock()
	c.links[link.A+"."+link.B] = link
	c.mu.Unlock()
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
		bus.WithMsgID(fmt.Sprintf("link:%s.%s:%d", link.A, link.B, link.UpdatedAt)))
	if err != nil {
		c.log.Warn("relation: publish link", "a", link.A, "b", link.B, "err", err)
	}
}

func (c *relationCore) removeLink(ctx context.Context, a, b string) {
	c.mu.Lock()
	existing, ok := c.links[a+"."+b]
	delete(c.links, a+"."+b)
	c.mu.Unlock()
	if !ok || existing == nil {
		return
	}
	// A zero-strength/confidence link is the removal tombstone the publisher
	// maps to Delta.link_remove.
	tombstone := &dorv1.Link{
		A:         a,
		B:         b,
		Type:      existing.Type,
		Dir:       existing.Dir,
		UpdatedAt: c.now().Unix(),
	}
	if c.sink != nil {
		c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true})
	}
	live.PutLink(ctx, c.kv, tombstone)
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(tombstone)
	if err != nil {
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, _ = c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data,
		bus.WithMsgID(fmt.Sprintf("link:%s.%s:rm:%d", a, b, tombstone.UpdatedAt)))
}

func (c *relationCore) storyInput(ctx context.Context, e *relationEntry) decide.Story {
	in := decide.Story{
		ID:       e.story.Id,
		Title:    e.story.Title,
		Entities: e.meta.Entities,
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
		if now.Unix()-e.seen > int64((48 * time.Hour).Seconds()) {
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
	gap := math.Abs(float64(a.seen - b.seen))
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
