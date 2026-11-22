package agents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
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
		judged:       make(map[string][2]uint64),
		pairs:        make(map[string]*relationPairRecord),
		dirty:        make(map[string]bool),
		removals:     make(map[string]*relationRemovalRecord),
		dirtyRem:     make(map[string]bool),
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
	durable := "relation-v2"
	if cfg.Shard >= 0 {
		durable = fmt.Sprintf("relation-v2-%d", cfg.Shard)
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
			return core.observeSeq(ctx, &story, msg.StreamSequence)
		},
	})
	if err != nil {
		return nil, err
	}
	return &relationAgent{name: RelationAgentName, core: core, base: base, durable: durable}, nil
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
	name    string
	core    *relationCore
	base    *Base
	durable string

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
	go r.retireLegacyConsumer(ctx)
	r.core.log.Info("relation judge started", "shard", r.core.cfg.Shard, "shards", r.core.cfg.Shards)
	return nil
}

// retireLegacyConsumer removes the pre-v2 durable after the widened consumer
// has caught up. Filter widening alone does not replay messages the old durable
// acknowledged under its narrower filter; the new DeliverAll durable does.
func (r *relationAgent) retireLegacyConsumer(ctx context.Context) {
	stream, err := r.core.bus.StreamNamed(ctx, bus.StreamStory)
	if err != nil {
		return
	}
	legacy := "relation"
	if r.core.cfg.Shard >= 0 {
		legacy = fmt.Sprintf("relation-%d", r.core.cfg.Shard)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		consumer, err := stream.Consumer(ctx, r.durable)
		if err == nil {
			if info, infoErr := consumer.Info(ctx); infoErr == nil && info.NumPending == 0 && info.NumAckPending == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	if err := stream.DeleteConsumer(ctx, legacy); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		r.core.log.Debug("relation: legacy consumer retirement deferred", "durable", legacy, "err", err)
	}
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
	// contentSeq is the durable content revision (JetStream stream sequence of
	// the latest content-changing story.changed, or a local counter in tests).
	contentSeq uint64
	judgedSeq  uint64
}

// relationStoryRecord is the durable per-story trigger state. The story/meta
// payload makes restore independent of the separate clusterer writes, and
// contentSeq is recorded before the input is acknowledged.
type relationStoryRecord struct {
	Story      []byte `json:"story"`
	Meta       []byte `json:"meta"`
	ContentSeq uint64 `json:"content_seq"`
	JudgedSeq  uint64 `json:"judged_seq"`
	Updated    int64  `json:"updated"`
}

// relationPairRecord is the durable judgement/output state for one unordered
// pair. Published is set only after the link KV write and link.changed publish
// succeed, so a failed output is retried instead of suppressed.
type relationPairRecord struct {
	A         string `json:"a"`
	B         string `json:"b"`
	SeqA      uint64 `json:"seq_a"`
	SeqB      uint64 `json:"seq_b"`
	Outcome   string `json:"outcome"` // link|remove|none
	Link      []byte `json:"link,omitempty"`
	Published bool   `json:"published"`
}

// relationRemovalRecord is the durable outbox for decay/eviction tombstones.
// Judgement removals use the pair ledger; lifecycle removals need their own
// record because they may supersede an already-published link.
type relationRemovalRecord struct {
	A         string `json:"a"`
	B         string `json:"b"`
	SeqA      uint64 `json:"seq_a"`
	SeqB      uint64 `json:"seq_b"`
	Published bool   `json:"published"`
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
	judged    map[string][2]uint64
	pairs     map[string]*relationPairRecord
	dirty     map[string]bool
	removals  map[string]*relationRemovalRecord
	dirtyRem  map[string]bool
	sink      LinkSink
	work      chan string
	pairLocks []sync.Mutex
	revision  atomic.Uint64
	localSeq  atomic.Uint64
	lastDecay time.Time
	lastRetry time.Time

	// storyLocks serialise observe and the final seq check/commit for one
	// story inside this process. Durable pair records provide the cross-process
	// correctness; these locks avoid duplicate decider calls.
	storyLocks [128]sync.Mutex

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
	c.localSeq.Store(1)
	if c.kv != nil {
		restoreCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		c.restore(restoreCtx)
	}
	return nil
}

func relationStoryKey(id string) string { return "rel.s." + id }

func relationPairKey(a, b string) string {
	a, b = live.SortPair(a, b)
	return "rel.p." + a + "." + b
}

func relationRemovalKey(a, b string) string {
	a, b = live.SortPair(a, b)
	return "rel.r." + a + "." + b
}

func (c *relationCore) storyLock(id string) *sync.Mutex {
	h := bus.StoryShard(id)
	return &c.storyLocks[h%len(c.storyLocks)]
}

// restore rebuilds the candidate index from all live stories (not just this
// shard's input) and the link table from live state. Without it a restarted
// judge cannot re-evaluate or decay existing links.
func (c *relationCore) restore(ctx context.Context) {
	now := c.now().UTC()
	restored := 0
	// Durable trigger records are authoritative. Restore schedules every story
	// whose latest content has not been fully judged, which recovers triggers
	// acknowledged before a crash and seeds initial comparisons after a
	// consumer/filter upgrade.
	if lister, err := c.kv.ListKeysFiltered(ctx, "rel.s.>"); err == nil {
		for key := range lister.Keys() {
			entry, err := c.kv.Get(ctx, key)
			if err != nil {
				continue
			}
			var rec relationStoryRecord
			if json.Unmarshal(entry.Value(), &rec) != nil {
				continue
			}
			var story dorv1.Story
			if proto.Unmarshal(rec.Story, &story) != nil || story.Id == "" {
				continue
			}
			var meta live.StoryMeta
			if len(rec.Meta) > 0 {
				_ = json.Unmarshal(rec.Meta, &meta)
			}
			rel := relationEntryFromRecord(&story, meta, rec, c.revision.Add(1))
			rel.updated = rec.Updated
			if rel.updated == 0 {
				rel.updated = story.UpdatedAt
			}
			if rel.updated == 0 || now.Sub(time.Unix(rel.updated, 0)) > clusterStoryTTL {
				_ = c.kv.Delete(ctx, key)
				continue
			}
			rel.lat, rel.lon = storyCoords(&story)
			if latest, ok := live.LoadStory(ctx, c.kv, story.Id); ok {
				// Heat is derived and the clusterer owns its latest value.
				story.Heat = latest.Heat
				rel.story = &story
			}
			judged := c.loadJudgedSeq(ctx, story.Id)
			rel.judgedSeq = judged
			if judged < rec.ContentSeq || rec.ContentSeq == 0 {
				c.pending[story.Id] = now.Add(c.cfg.Debounce)
			}
			c.index[story.Id] = rel
			restored++
		}
		_ = lister.Stop()
	}

	// Legacy stories (written before rel.s existed) still need a durable
	// record; create one and schedule the initial comparison.
	if lister, err := c.kv.ListKeysFiltered(ctx, "story.*"); err == nil {
		for key := range lister.Keys() {
			if !strings.HasPrefix(key, "story.") || strings.HasPrefix(key, "storymeta.") || strings.HasPrefix(key, "storymap.") {
				continue
			}
			id := strings.TrimPrefix(key, "story.")
			if c.index[id] != nil {
				continue
			}
			entry, err := c.kv.Get(ctx, key)
			if err != nil {
				continue
			}
			story := &dorv1.Story{}
			if proto.Unmarshal(entry.Value(), story) != nil || story.Id == "" {
				continue
			}
			updated := story.UpdatedAt
			if updated == 0 {
				updated = story.FirstSeen
			}
			if updated == 0 || now.Sub(time.Unix(updated, 0)) > clusterStoryTTL {
				continue
			}
			meta, _ := live.LoadStoryMeta(ctx, c.kv, story.Id)
			rec := relationStoryRecord{ContentSeq: 1, JudgedSeq: 0, Updated: updated}
			rec.Story, _ = proto.Marshal(story)
			rec.Meta, _ = json.Marshal(meta)
			if err := c.storeStoryRecord(ctx, story.Id, &rec); err != nil {
				c.log.Debug("relation: legacy story record deferred", "story", story.Id, "err", err)
				continue
			}
			// Another process may have accepted a newer revision while this one
			// was scanning legacy stories; use that authoritative record.
			if stored, _, ok := c.loadStoryRecord(ctx, story.Id); ok {
				rec = *stored
				if len(rec.Story) > 0 {
					latest := &dorv1.Story{}
					if proto.Unmarshal(rec.Story, latest) == nil {
						story = latest
					}
				}
				if len(rec.Meta) > 0 {
					_ = json.Unmarshal(rec.Meta, &meta)
				}
			}
			rel := relationEntryFromRecord(story, meta, rec, c.revision.Add(1))
			rel.updated = rec.Updated
			if rel.updated == 0 {
				rel.updated = updated
			}
			rel.lat, rel.lon = storyCoords(story)
			c.index[story.Id] = rel
			c.pending[story.Id] = now.Add(c.cfg.Debounce)
			restored++
		}
		_ = lister.Stop()
	}

	// Pair records are the durable judgement ledger. Populate the local
	// dedupe cache and the retry set; the ledger is bounded by the 48 h TTL
	// and explicit eviction.
	if lister, err := c.kv.ListKeysFiltered(ctx, "rel.p.>"); err == nil {
		for key := range lister.Keys() {
			entry, err := c.kv.Get(ctx, key)
			if err != nil {
				continue
			}
			var rec relationPairRecord
			if json.Unmarshal(entry.Value(), &rec) != nil || rec.A == "" || rec.B == "" {
				continue
			}
			pkey := relationPairKey(rec.A, rec.B)
			c.pairs[pkey] = &rec
			if rec.Published {
				c.judged[pairKey(rec.A, rec.B)] = [2]uint64{rec.SeqA, rec.SeqB}
			} else {
				c.dirty[pkey] = true
			}
		}
		_ = lister.Stop()
	}

	if lister, err := c.kv.ListKeysFiltered(ctx, "rel.r.>"); err == nil {
		for key := range lister.Keys() {
			entry, err := c.kv.Get(ctx, key)
			if err != nil {
				continue
			}
			var rec relationRemovalRecord
			if json.Unmarshal(entry.Value(), &rec) != nil || rec.A == "" || rec.B == "" {
				continue
			}
			rkey := relationRemovalKey(rec.A, rec.B)
			c.removals[rkey] = &rec
			if !rec.Published {
				c.dirtyRem[rkey] = true
			}
		}
		_ = lister.Stop()
	}

	linkLister, err := c.kv.ListKeysFiltered(ctx, "link.>")
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

func relationEntryFromRecord(story *dorv1.Story, meta live.StoryMeta, rec relationStoryRecord, rev uint64) *relationEntry {
	e := &relationEntry{
		story:      story,
		meta:       meta,
		vec:        meta.Embedding,
		entities:   meta.Entities,
		firstSeen:  meta.FirstSeen,
		updated:    rec.Updated,
		rev:        rev,
		contentSeq: rec.ContentSeq,
		judgedSeq:  rec.JudgedSeq,
	}
	if e.firstSeen == 0 {
		e.firstSeen = story.FirstSeen
	}
	if e.updated == 0 {
		e.updated = story.UpdatedAt
	}
	return e
}

// observe indexes a story on every judge and schedules a debounced judgement.
// Judgement execution is still partitioned per pair in ownsPair, so observing
// on every shard does not double-judge a pair.
func (c *relationCore) observe(ctx context.Context, story *dorv1.Story) {
	_ = c.observeSeq(ctx, story, 0)
}

// observeSeq records the latest content revision before acknowledging the
// input. streamSeq is the JetStream sequence of the story.changed message; a
// message older than the recorded content is an obsolete replay and is
// ignored, even if it arrives after a restart restored newer state.
func (c *relationCore) observeSeq(ctx context.Context, story *dorv1.Story, streamSeq uint64) error {
	if story == nil || story.Id == "" {
		return nil
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

	lock := c.storyLock(story.Id)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	prev := c.index[story.Id]
	if prev != nil {
		// A metadata read can race the clusterer's write. Do not lose the
		// embedding/entities or the original event time on a partial read.
		if len(entry.vec) == 0 {
			entry.vec = prev.vec
		}
		if len(entry.entities) == 0 {
			entry.entities = prev.entities
		}
		if entry.meta.UpdatedAt == 0 {
			entry.meta = prev.meta
		}
		if entry.firstSeen == now.Unix() && prev.firstSeen > 0 {
			entry.firstSeen = prev.firstSeen
		}
	}
	c.mu.Unlock()

	// Reject an obsolete replay before it can replace newer restored content.
	if prev != nil && prev.contentSeq > 0 {
		if streamSeq > 0 && streamSeq <= prev.contentSeq {
			return nil
		}
		if streamSeq == 0 && prev.updated > 0 && entry.updated > 0 && entry.updated < prev.updated {
			return nil
		}
	}

	changed := prev == nil || !storyContentEqual(prev.story, story) ||
		prev.meta.UpdatedAt != entry.meta.UpdatedAt ||
		!slices.Equal(prev.entities, entry.entities) ||
		!equalVectors(prev.vec, entry.vec)
	if !changed {
		// Heat cooling republishes a story with unchanged content. Refresh the
		// heat used by decay without advancing the content revision or
		// scheduling a judgement.
		c.mu.Lock()
		// Replace, never mutate: judges and candidate scoring hold entry
		// pointers after releasing c.mu.
		updated := *prev
		updated.story = story
		updated.lat, updated.lon = entry.lat, entry.lon
		c.index[story.Id] = &updated
		c.mu.Unlock()
		return nil
	}

	seq := streamSeq
	if seq == 0 {
		seq = c.localSeq.Add(1)
	}
	rec := relationStoryRecord{ContentSeq: seq, Updated: entry.updated}
	if prev != nil {
		rec.JudgedSeq = prev.judgedSeq
	}
	rec.Story, _ = proto.Marshal(story)
	rec.Meta, _ = json.Marshal(entry.meta)
	if err := c.storeStoryRecord(ctx, story.Id, &rec); err != nil {
		return err
	}
	entry.contentSeq = seq
	entry.rev = c.revision.Add(1)
	c.mu.Lock()
	c.index[story.Id] = entry
	c.pending[story.Id] = now.Add(c.cfg.Debounce)
	c.mu.Unlock()
	return nil
}

func (c *relationCore) loadStoryRecord(ctx context.Context, storyID string) (*relationStoryRecord, uint64, bool) {
	if c.kv == nil {
		return nil, 0, false
	}
	entry, err := c.kv.Get(ctx, relationStoryKey(storyID))
	if err != nil {
		return nil, 0, false
	}
	var rec relationStoryRecord
	if json.Unmarshal(entry.Value(), &rec) != nil {
		return nil, 0, false
	}
	return &rec, entry.Revision(), true
}

// storeStoryRecord writes the durable content revision with CAS. A concurrent
// story update from another judge either wins (higher sequence) or is retried.
func (c *relationCore) storeStoryRecord(ctx context.Context, storyID string, rec *relationStoryRecord) error {
	if c.kv == nil {
		return nil
	}
	key := relationStoryKey(storyID)
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := c.kv.Get(ctx, key)
		if err == nil {
			var existing relationStoryRecord
			if json.Unmarshal(entry.Value(), &existing) == nil {
				if existing.ContentSeq >= rec.ContentSeq {
					return nil
				}
				if existing.JudgedSeq > rec.JudgedSeq {
					rec.JudgedSeq = existing.JudgedSeq
				}
			}
			if _, err := c.kv.Update(ctx, key, data, entry.Revision()); err == nil {
				return nil
			}
			continue
		}
		if !isKVNotFound(err) {
			return err
		}
		if _, err := c.kv.Create(ctx, key, data); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
	}
	return fmt.Errorf("relation: story record contention for %s", storyID)
}

// storyContentEqual compares the relation-relevant story fields. Heat is
// derived and deliberately excluded.
func storyContentEqual(a, b *dorv1.Story) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Id != b.Id || a.Title != b.Title || a.Category != b.Category ||
		a.Primary != b.Primary || a.SourceCount != b.SourceCount ||
		a.FirstSeen != b.FirstSeen || a.UpdatedAt != b.UpdatedAt ||
		len(a.Locations) != len(b.Locations) {
		return false
	}
	for i := range a.Locations {
		if !proto.Equal(a.Locations[i], b.Locations[i]) {
			return false
		}
	}
	return true
}

func equalVectors(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
	return a + "|" + b
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
			if c.lastRetry.IsZero() || now.Sub(c.lastRetry) >= 5*time.Second {
				c.lastRetry = now
				c.retryOutputs(ctx)
			}
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
	var evicted []string
	var evictedPairs []string
	c.mu.Lock()
	cutoff := now.Add(-clusterStoryTTL).Unix()
	for id, e := range c.index {
		if e == nil || e.updated < cutoff {
			delete(c.index, id)
			delete(c.pending, id)
			evicted = append(evicted, id)
		}
	}
	for _, id := range evicted {
		for key := range c.judged {
			a, b, ok := strings.Cut(key, "|")
			if ok && (a == id || b == id) {
				delete(c.judged, key)
			}
		}
		for key, rec := range c.pairs {
			if rec != nil && (rec.A == id || rec.B == id) {
				delete(c.pairs, key)
				delete(c.dirty, key)
				evictedPairs = append(evictedPairs, key)
			}
		}
		for key, rec := range c.removals {
			if rec != nil && (rec.A == id || rec.B == id) && rec.Published {
				delete(c.removals, key)
				delete(c.dirtyRem, key)
				evictedPairs = append(evictedPairs, key)
			}
		}
	}
	for key, rec := range c.removals {
		if rec != nil && rec.Published && c.index[rec.A] == nil && c.index[rec.B] == nil {
			delete(c.removals, key)
			delete(c.dirtyRem, key)
			evictedPairs = append(evictedPairs, key)
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

	for _, id := range evicted {
		if c.kv != nil {
			_ = c.kv.Delete(ctx, relationStoryKey(id))
			_ = c.kv.Delete(ctx, relationJudgedKey(id, c.shard))
		}
	}
	for _, key := range evictedPairs {
		if c.kv != nil {
			_ = c.kv.Delete(ctx, key)
		}
	}
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
	seq := uint64(0)
	if entry != nil {
		seq = entry.contentSeq
	}
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
		if err := c.judgePair(ctx, entry, other); err != nil {
			c.log.Warn("relation: judgement deferred", "a", entry.story.Id, "b", other.story.Id, "err", err)
			c.reschedule(id)
			return
		}
	}
	c.markStoryJudged(ctx, id, seq)
}

// judgePair serialises one pair, judges it from the current content revisions
// and commits the result through a durable pair record. A result whose inputs
// changed before the commit is discarded; a failed output stays unpublished
// and is retried from the record.
//
// Remaining crash window: a process that dies after the decider answers but
// before the pair record commits loses that answer and re-judges the pair after
// restart. The model may answer differently; the result is never half-written
// or published without a durable record.
func (c *relationCore) judgePair(ctx context.Context, a, b *relationEntry) error {
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	if aid == bid {
		return nil
	}
	lock := c.lockPair(aid, bid)
	lock.Lock()
	defer lock.Unlock()

	ea := c.authoritativeEntry(ctx, aid)
	eb := c.authoritativeEntry(ctx, bid)
	if ea == nil || eb == nil {
		return nil
	}
	if !c.ownsPair(aid, bid) {
		return nil
	}
	seqA, seqB := ea.contentSeq, eb.contentSeq
	if seqA == 0 {
		seqA = c.entrySeq(aid)
	}
	if seqB == 0 {
		seqB = c.entrySeq(bid)
	}
	key := relationPairKey(aid, bid)
	c.mu.Lock()
	last, judged := c.judged[pairKey(aid, bid)]
	c.mu.Unlock()
	if judged && last[0] == seqA && last[1] == seqB {
		return nil
	}
	rec, rev, exists, err := c.loadPairRecord(ctx, key)
	if err != nil {
		return err
	}
	if exists {
		if rec.SeqA == seqA && rec.SeqB == seqB {
			if rec.Published {
				c.cacheJudged(aid, seqA, bid, seqB)
				return nil
			}
			return c.applyPairOutput(ctx, key, rec, rev)
		}
		if rec.SeqA > seqA || rec.SeqB > seqB {
			return nil
		}
	}

	first, second := ea, eb
	if first.story.Id != aid {
		first, second = eb, ea
	}
	inA := c.storyInput(ctx, first)
	inB := c.storyInput(ctx, second)
	var link *dorv1.Link
	if c.cfg.Decider == nil {
		link = fallbackLink(ea, eb, c.now())
	} else {
		priority := relationPriority(ea, eb, c.now())
		resp, err := c.cfg.Decider.Decide(ctx, relateRequest(inA, inB, priority))
		if err != nil {
			c.log.Debug("relation: jev unavailable; similarity-only link", "a", aid, "b", bid, "err", err)
			link = fallbackLink(ea, eb, c.now())
		} else {
			res := decide.CombineRelate(resp, inA, inB, c.cfg.Thresholds)
			if !res.Answered {
				link = fallbackLink(ea, eb, c.now())
			} else if !res.Accepted {
				link = nil
			} else {
				link = &dorv1.Link{
					A:          aid,
					B:          bid,
					Type:       linkType(res.Type),
					Dir:        linkDir(res.Direction),
					Confidence: float32(res.RelatedP),
					Strength:   float32(res.Strength),
					UpdatedAt:  c.now().Unix(),
				}
			}
		}
	}
	// Fence before writing the durable result. The concurrent writer cannot
	// change the pair record between this check and the CAS commit because the
	// pair record CAS rejects an older sequence pair; a later content change
	// reschedules both stories.
	if !c.seqsCurrent(ctx, aid, seqA, bid, seqB) {
		c.reschedule(aid)
		c.reschedule(bid)
		return nil
	}
	pr := &relationPairRecord{A: aid, B: bid, SeqA: seqA, SeqB: seqB}
	if link != nil {
		pr.Outcome = "link"
		pr.Link, _ = proto.MarshalOptions{Deterministic: true}.Marshal(link)
	} else if c.linkExists(ctx, aid, bid) {
		pr.Outcome = "remove"
	} else {
		pr.Outcome = "none"
	}
	rev, stored, err := c.storePairRecord(ctx, key, pr, rev, exists)
	if err != nil {
		return err
	}
	if !stored {
		// A concurrent writer owns this pair at a newer revision. Apply its
		// record if its output is still pending; never publish ours.
		winner, winnerRev, ok, loadErr := c.loadPairRecord(ctx, key)
		if loadErr != nil || !ok || winner.Published {
			return loadErr
		}
		return c.applyPairOutput(ctx, key, winner, winnerRev)
	}
	return c.applyPairOutput(ctx, key, pr, rev)
}

func (c *relationCore) cacheJudged(aid string, seqA uint64, bid string, seqB uint64) {
	c.mu.Lock()
	c.judged[pairKey(aid, bid)] = [2]uint64{seqA, seqB}
	c.mu.Unlock()
}

// authoritativeEntry returns the entry whose payload and revision come from
// the same durable record. A lagging shard must never judge an old in-memory
// payload under a newer stored revision.
func (c *relationCore) authoritativeEntry(ctx context.Context, id string) *relationEntry {
	if rec, rev, ok := c.loadStoryRecord(ctx, id); ok && len(rec.Story) > 0 {
		var story dorv1.Story
		if proto.Unmarshal(rec.Story, &story) == nil && story.Id != "" {
			var meta live.StoryMeta
			if len(rec.Meta) > 0 {
				_ = json.Unmarshal(rec.Meta, &meta)
			}
			e := relationEntryFromRecord(&story, meta, *rec, c.revision.Add(1))
			e.lat, e.lon = storyCoords(&story)
			e.meta = meta
			e.vec = meta.Embedding
			e.entities = meta.Entities
			e.updated = rec.Updated
			if e.updated == 0 {
				e.updated = story.UpdatedAt
			}
			c.mu.Lock()
			if cur := c.index[id]; cur != nil && cur.contentSeq == rec.ContentSeq && storyContentEqual(cur.story, e.story) {
				e.story.Heat = cur.story.Heat
			}
			if cur := c.index[id]; cur == nil || cur.contentSeq <= rec.ContentSeq {
				c.index[id] = e
			}
			c.mu.Unlock()
			_ = rev
			return e
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.index[id]
}

// currentSeqs reads the authoritative content revisions for both endpoints.
func (c *relationCore) currentSeqs(ctx context.Context, aid, bid string) (uint64, uint64) {
	seqA, seqB := c.entrySeq(aid), c.entrySeq(bid)
	if c.kv != nil {
		for _, item := range []struct {
			id  string
			out *uint64
		}{{aid, &seqA}, {bid, &seqB}} {
			if entry, err := c.kv.Get(ctx, relationStoryKey(item.id)); err == nil {
				var rec relationStoryRecord
				if json.Unmarshal(entry.Value(), &rec) == nil && rec.ContentSeq > *item.out {
					*item.out = rec.ContentSeq
				}
			}
		}
	}
	return seqA, seqB
}

func (c *relationCore) entrySeq(id string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.index[id]; e != nil {
		if e.contentSeq > 0 {
			return e.contentSeq
		}
		return e.rev
	}
	return 0
}

func (c *relationCore) seqsCurrent(ctx context.Context, aid string, seqA uint64, bid string, seqB uint64) bool {
	gotA, gotB := c.currentSeqs(ctx, aid, bid)
	return gotA == seqA && gotB == seqB
}

func (c *relationCore) reschedule(id string) {
	c.mu.Lock()
	c.pending[id] = c.now().Add(c.cfg.Debounce)
	c.mu.Unlock()
}

// markStoryJudged records the content revision this shard finished evaluating.
// Per-shard keys avoid one shard clearing another shard's pending pairs.
func (c *relationCore) markStoryJudged(ctx context.Context, id string, seq uint64) {
	if seq == 0 || c.kv == nil {
		return
	}
	key := relationJudgedKey(id, c.shard)
	if _, err := c.kv.Put(ctx, key, []byte(fmt.Sprint(seq))); err != nil {
		c.log.Debug("relation: judged revision write deferred", "story", id, "err", err)
		return
	}
	c.mu.Lock()
	if e := c.index[id]; e != nil && e.contentSeq == seq {
		updated := *e
		updated.judgedSeq = seq
		c.index[id] = &updated
	}
	c.mu.Unlock()
}

func relationJudgedKey(id string, shard int) string {
	return fmt.Sprintf("rel.j.%s.%d", id, shard)
}

func (c *relationCore) loadJudgedSeq(ctx context.Context, id string) uint64 {
	if c.kv == nil {
		return 0
	}
	entry, err := c.kv.Get(ctx, relationJudgedKey(id, c.shard))
	if err != nil {
		return 0
	}
	var seq uint64
	_, _ = fmt.Sscanf(string(entry.Value()), "%d", &seq)
	return seq
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

// fallbackLink emits a similarity-only thematic link, explicitly unverified. It
// never invents a causal or reaction type.
func fallbackLink(a, b *relationEntry, now time.Time) *dorv1.Link {
	score := cheapScore(a, b, now)
	if score < 0.3 {
		return nil
	}
	aid, bid := live.SortPair(a.story.Id, b.story.Id)
	return &dorv1.Link{
		A:          aid,
		B:          bid,
		Type:       dorv1.LinkType_LINK_TYPE_THEMATIC,
		Dir:        dorv1.Direction_DIRECTION_UNCLEAR,
		Confidence: float32(score),
		Strength:   float32(score),
		UpdatedAt:  now.Unix(),
		Unverified: true,
	}
}

// relateRequest sets the budget priority the judge selected from cheap pair
// signals. A decider implementation that ignores Request.Priority still works.
func relateRequest(a, b decide.Story, priority budget.Priority) decide.Request {
	req := decide.RelateRequest(a, b)
	req.Priority = priority
	return req
}

func (c *relationCore) loadPairRecord(ctx context.Context, key string) (*relationPairRecord, uint64, bool, error) {
	if c.kv == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if rec := c.pairs[key]; rec != nil {
			cp := *rec
			return &cp, 0, true, nil
		}
		return nil, 0, false, nil
	}
	entry, err := c.kv.Get(ctx, key)
	if err != nil {
		if isKVNotFound(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	var rec relationPairRecord
	if json.Unmarshal(entry.Value(), &rec) != nil {
		return nil, 0, false, errors.New("relation: malformed pair record")
	}
	return &rec, entry.Revision(), true, nil
}

// storePairRecord writes the durable judgement with CAS. An older sequence
// pair never overwrites a newer record.
func (c *relationCore) storePairRecord(ctx context.Context, key string, rec *relationPairRecord, rev uint64, exists bool) (uint64, bool, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, false, err
	}
	if c.kv == nil {
		c.mu.Lock()
		cp := *rec
		c.pairs[key] = &cp
		c.mu.Unlock()
		return 0, true, nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		if exists {
			if updated, err := c.kv.Update(ctx, key, data, rev); err == nil {
				c.rememberPair(key, rec)
				return updated, true, nil
			}
		} else {
			if created, err := c.kv.Create(ctx, key, data); err == nil {
				c.rememberPair(key, rec)
				return created, true, nil
			} else if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
				return 0, false, err
			}
		}
		// Reconcile with the winner.
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			if isKVNotFound(err) {
				exists = false
				continue
			}
			return 0, false, err
		}
		var current relationPairRecord
		if json.Unmarshal(entry.Value(), &current) == nil {
			if current.SeqA > rec.SeqA || current.SeqB > rec.SeqB {
				return entry.Revision(), false, nil
			}
			if current.SeqA == rec.SeqA && current.SeqB == rec.SeqB {
				// The first committed result for an input revision pair is
				// immutable, even while unpublished. A second overlapping
				// judgement must recover and apply that result, not overwrite
				// it with a different answer.
				return entry.Revision(), false, nil
			}
		}
		rev, exists = entry.Revision(), true
	}
	return 0, false, errors.New("relation: pair record contention")
}

func (c *relationCore) rememberPair(key string, rec *relationPairRecord) {
	c.mu.Lock()
	cp := *rec
	c.pairs[key] = &cp
	if rec.Published {
		delete(c.dirty, key)
	} else {
		c.dirty[key] = true
	}
	c.mu.Unlock()
}

func (c *relationCore) deletePairRecord(ctx context.Context, a, b string, rev uint64) {
	key := relationPairKey(a, b)
	c.mu.Lock()
	delete(c.pairs, key)
	delete(c.dirty, key)
	delete(c.judged, key)
	c.mu.Unlock()
	if c.kv == nil {
		return
	}
	if rev > 0 {
		// Conditional so a concurrent newer judgement is never deleted.
		_ = c.kv.Purge(ctx, key, jetstream.LastRevision(rev))
		return
	}
	_ = c.kv.Delete(ctx, key)
}

func (c *relationCore) linkExists(ctx context.Context, a, b string) bool {
	c.mu.Lock()
	rel := c.links[pairKey(a, b)]
	c.mu.Unlock()
	if rel != nil && rel.link != nil {
		return true
	}
	if c.kv != nil {
		if link, ok := live.LoadLink(ctx, c.kv, a, b); ok && link.Strength > 0 && link.Confidence > 0 {
			return true
		}
	}
	return false
}

// applyPairOutput writes/publishes a durable pair outcome. Published is set
// only after every output succeeds, so a crash or failed publish leaves the
// record recoverable.
func (c *relationCore) applyPairOutput(ctx context.Context, key string, rec *relationPairRecord, rev uint64) error {
	// Capture the stream fence before checking either ledger. The server
	// rejects an output if any link publication wins after this snapshot.
	last, err := c.lastLinkSequence(ctx)
	if err != nil {
		return err
	}
	if rec.Outcome == "link" {
		removal, removalRev, ok, err := c.loadRemovalRecord(ctx, relationRemovalKey(rec.A, rec.B))
		if err != nil {
			return err
		}
		if ok && removalRev > rev && removal.SeqA >= rec.SeqA && removal.SeqB >= rec.SeqB {
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			return nil
		}
	}
	switch rec.Outcome {
	case "none":
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		return c.markPairPublished(ctx, key, rec, rev)
	case "remove":
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		a, b := live.SortPair(rec.A, rec.B)
		tombstone := &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()}
		if c.kv != nil && !live.PutLink(ctx, c.kv, tombstone) {
			return errors.New("relation: link tombstone KV write failed")
		}
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			// A newer judgement landed while the tombstone was written.
			// Restore its link if it is already published; drop this output.
			if winner, _, ok, _ := c.loadPairRecord(ctx, key); ok && winner.Outcome == "link" && winner.Published && len(winner.Link) > 0 {
				var link dorv1.Link
				if proto.Unmarshal(winner.Link, &link) == nil {
					live.PutLink(ctx, c.kv, &link)
					link.A, link.B = live.SortPair(link.A, link.B)
					c.mu.Lock()
					c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength)}
					c.mu.Unlock()
				}
			}
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if err := c.publishLinkFenced(ctx, tombstone, rec.SeqA, rec.SeqB, true, last, rev); err != nil {
			c.restoreNewerPairLink(ctx, rec.A, rec.B)
			if current, seqErr := c.lastLinkSequence(ctx); seqErr == nil && current != last {
				c.reschedule(rec.A)
				c.reschedule(rec.B)
				return nil
			}
			return err
		}
		c.mu.Lock()
		delete(c.links, pairKey(a, b))
		c.mu.Unlock()
		if c.sink != nil {
			c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true})
		}
		c.invalidateLinkText(ctx, a, b)
		return c.markPairPublished(ctx, key, rec, rev)
	case "link":
		var link dorv1.Link
		if err := proto.Unmarshal(rec.Link, &link); err != nil {
			return err
		}
		link.A, link.B = live.SortPair(link.A, link.B)
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if c.kv != nil && !live.PutLink(ctx, c.kv, &link) {
			return errors.New("relation: link KV write failed")
		}
		// Re-check after the write: a story update may have landed while the
		// KV write was in flight.
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.rollbackLink(ctx, &link)
			c.deletePairRecord(ctx, rec.A, rec.B, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if err := c.publishLinkFenced(ctx, &link, rec.SeqA, rec.SeqB, false, last, rev); err != nil {
			c.restoreNewerPairLink(ctx, rec.A, rec.B)
			return err
		}
		c.mu.Lock()
		c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength)}
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
		return c.markPairPublished(ctx, key, rec, rev)
	default:
		return fmt.Errorf("relation: unknown pair outcome %q", rec.Outcome)
	}
}

func (c *relationCore) rollbackLink(ctx context.Context, link *dorv1.Link) {
	if c.kv == nil {
		return
	}
	current, ok := live.LoadLink(ctx, c.kv, link.A, link.B)
	if !ok || live.LinkRevision(current) != live.LinkRevision(link) {
		return
	}
	_ = c.kv.Delete(ctx, live.LinkKey(link.A, link.B))
	c.mu.Lock()
	if rel := c.links[pairKey(link.A, link.B)]; rel != nil && rel.link != nil && live.LinkRevision(rel.link) == live.LinkRevision(link) {
		delete(c.links, pairKey(link.A, link.B))
	}
	c.mu.Unlock()
}

func (c *relationCore) publishLink(ctx context.Context, link *dorv1.Link, seqA, seqB uint64, remove bool) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		return err
	}
	msgID := pairMessageID(link.A, link.B, seqA, seqB, remove)
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data, bus.WithMsgID(msgID)); err != nil {
		return fmt.Errorf("relation: publish link: %w", err)
	}
	return nil
}

// lastLinkSequence and the expected-subject-sequence header serialize link
// output against other relation instances at the JetStream publish boundary.
func (c *relationCore) lastLinkSequence(ctx context.Context) (uint64, error) {
	stream, err := c.bus.StreamNamed(ctx, bus.StreamStory)
	if err != nil {
		return 0, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, bus.SubjectLinkChanged)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return msg.Sequence, nil
}

func (c *relationCore) publishLinkFenced(ctx context.Context, link *dorv1.Link, seqA, seqB uint64, remove bool, last, generation uint64) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		return err
	}
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	id := pairMessageID(link.A, link.B, seqA, seqB, remove)
	if remove {
		id = fmt.Sprintf("%s:%d", id, generation)
	}
	_, err = c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data,
		bus.WithMsgID(id),
		bus.WithHeaders(map[string][]string{"Nats-Expected-Last-Subject-Sequence": {fmt.Sprint(last)}}))
	return err
}

func pairMessageID(a, b string, seqA, seqB uint64, remove bool) string {
	a, b = live.SortPair(a, b)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", a, b, seqA, seqB)))
	suffix := ""
	if remove {
		suffix = ":rm"
	}
	return "link:" + a + "." + b + ":" + hex.EncodeToString(sum[:8]) + suffix
}

func (c *relationCore) markPairPublished(ctx context.Context, key string, rec *relationPairRecord, rev uint64) error {
	rec.Published = true
	if c.kv == nil {
		c.rememberPair(key, rec)
		return nil
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	// The record may have been superseded while the output was in flight.
	if _, err := c.kv.Update(ctx, key, data, rev); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return nil
		}
		return err
	}
	c.rememberPair(key, rec)
	c.cacheJudged(rec.A, rec.SeqA, rec.B, rec.SeqB)
	return nil
}

// retryOutputs republishes durable outputs whose publish previously failed.
func (c *relationCore) retryOutputs(ctx context.Context) {
	c.mu.Lock()
	dirty := make([]string, 0, len(c.dirty))
	for key := range c.dirty {
		dirty = append(dirty, key)
	}
	c.mu.Unlock()
	for _, key := range dirty {
		rec, rev, ok, err := c.loadPairRecord(ctx, key)
		if err != nil || !ok || rec.Published {
			continue
		}
		if err := c.applyPairOutput(ctx, key, rec, rev); err != nil {
			c.log.Debug("relation: output retry deferred", "pair", key, "err", err)
		}
	}
	c.retryRemovals(ctx)
}

// upsertLinkLocked persists a decayed link. The caller holds the pair lock.
func (c *relationCore) upsertLinkLocked(ctx context.Context, link *dorv1.Link) {
	if link == nil || link.A == "" || link.B == "" {
		return
	}
	link.A, link.B = live.SortPair(link.A, link.B)
	if link.UpdatedAt == 0 {
		link.UpdatedAt = c.now().Unix()
	}
	if c.kv != nil {
		live.PutLink(ctx, c.kv, link)
	}
	c.mu.Lock()
	c.links[pairKey(link.A, link.B)] = &relationLink{link: link, publishedStrength: float64(link.Strength)}
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
	if _, err := c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data, bus.WithMsgID(linkMessageID(link))); err != nil {
		c.log.Debug("relation: decay publish deferred", "a", link.A, "b", link.B, "err", err)
	}
}

func linkMessageID(link *dorv1.Link) string {
	return fmt.Sprintf("link:%s.%s:%016x", link.A, link.B, live.LinkRevision(link))
}

// removeLinkLocked writes a decay/eviction tombstone. The caller holds the
// pair lock. The durable judgement ledger entry is dropped with the link.
// removeLinkLocked enqueues a decay/eviction tombstone in the durable removal
// outbox. The link stays locally until the removal is published, so a failed
// publication remains retryable.
func (c *relationCore) removeLinkLocked(ctx context.Context, a, b string) {
	a, b = live.SortPair(a, b)
	key := pairKey(a, b)
	c.mu.Lock()
	existing := c.links[key]
	c.mu.Unlock()
	if existing == nil {
		if c.kv == nil {
			return
		}
		link, ok := live.LoadLink(ctx, c.kv, a, b)
		if !ok || (link.Strength <= 0 && link.Confidence <= 0) {
			return
		}
	}
	seqA, seqB := c.currentSeqs(ctx, a, b)
	rkey := relationRemovalKey(a, b)
	rec, rev, ok, err := c.loadRemovalRecord(ctx, rkey)
	if err != nil {
		return
	}
	if ok && rec.Published && rec.SeqA == seqA && rec.SeqB == seqB {
		_, pairRev, pairOK, _ := c.loadPairRecord(ctx, relationPairKey(a, b))
		if !pairOK || pairRev <= rev {
			c.dropLocalLink(a, b)
			return
		}
	}
	if ok && (rec.SeqA > seqA || rec.SeqB > seqB) {
		return
	}
	next := &relationRemovalRecord{A: a, B: b, SeqA: seqA, SeqB: seqB}
	rev, stored, err := c.storeRemovalRecord(ctx, rkey, next, rev, ok)
	if err != nil {
		return
	}
	if !stored {
		if winner, winnerRev, ok, loadErr := c.loadRemovalRecord(ctx, rkey); loadErr == nil && ok && !winner.Published {
			_ = c.applyRemovalOutput(ctx, rkey, winner, winnerRev)
		}
		return
	}
	_ = c.applyRemovalOutput(ctx, rkey, next, rev)
}

func (c *relationCore) loadRemovalRecord(ctx context.Context, key string) (*relationRemovalRecord, uint64, bool, error) {
	if c.kv == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if rec := c.removals[key]; rec != nil {
			cp := *rec
			return &cp, 0, true, nil
		}
		return nil, 0, false, nil
	}
	entry, err := c.kv.Get(ctx, key)
	if err != nil {
		if isKVNotFound(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	var rec relationRemovalRecord
	if json.Unmarshal(entry.Value(), &rec) != nil {
		return nil, 0, false, errors.New("relation: malformed removal record")
	}
	return &rec, entry.Revision(), true, nil
}

func (c *relationCore) storeRemovalRecord(ctx context.Context, key string, rec *relationRemovalRecord, rev uint64, exists bool) (uint64, bool, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, false, err
	}
	if c.kv == nil {
		c.mu.Lock()
		cp := *rec
		c.removals[key] = &cp
		c.mu.Unlock()
		return 0, true, nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		if exists {
			if updated, err := c.kv.Update(ctx, key, data, rev); err == nil {
				c.rememberRemoval(key, rec)
				return updated, true, nil
			}
		} else {
			if created, err := c.kv.Create(ctx, key, data); err == nil {
				c.rememberRemoval(key, rec)
				return created, true, nil
			} else if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
				return 0, false, err
			}
		}
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			if isKVNotFound(err) {
				exists = false
				continue
			}
			return 0, false, err
		}
		var current relationRemovalRecord
		if json.Unmarshal(entry.Value(), &current) == nil {
			if current.SeqA > rec.SeqA || current.SeqB > rec.SeqB || (current.Published && current.SeqA == rec.SeqA && current.SeqB == rec.SeqB) {
				return entry.Revision(), false, nil
			}
		}
		rev, exists = entry.Revision(), true
	}
	return 0, false, errors.New("relation: removal record contention")
}

func (c *relationCore) rememberRemoval(key string, rec *relationRemovalRecord) {
	c.mu.Lock()
	cp := *rec
	c.removals[key] = &cp
	if rec.Published {
		delete(c.dirtyRem, key)
	} else {
		c.dirtyRem[key] = true
	}
	c.mu.Unlock()
}

func (c *relationCore) applyRemovalOutput(ctx context.Context, key string, rec *relationRemovalRecord, rev uint64) error {
	last, err := c.lastLinkSequence(ctx)
	if err != nil {
		return err
	}
	a, b := live.SortPair(rec.A, rec.B)
	c.mu.Lock()
	_, hasA := c.index[a]
	_, hasB := c.index[b]
	c.mu.Unlock()
	endpointsLive := hasA && hasB
	if endpointsLive && !c.seqsCurrent(ctx, a, rec.SeqA, b, rec.SeqB) {
		c.restoreNewerPairLink(ctx, a, b)
		c.deleteRemovalRecord(ctx, a, b, rev)
		c.reschedule(a)
		c.reschedule(b)
		return nil
	}
	if winner, _, ok, _ := c.loadPairRecord(ctx, relationPairKey(a, b)); ok && winner.Outcome == "link" && winner.Published &&
		(winner.SeqA > rec.SeqA || winner.SeqB > rec.SeqB) {
		// A newer judgement owns the link. Drop this lifecycle removal.
		c.deleteRemovalRecord(ctx, a, b, rev)
		return nil
	}
	tombstone := &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()}
	if c.kv != nil && !live.PutLink(ctx, c.kv, tombstone) {
		return errors.New("relation: removal tombstone KV write failed")
	}
	if endpointsLive && !c.seqsCurrent(ctx, a, rec.SeqA, b, rec.SeqB) {
		c.restoreNewerPairLink(ctx, a, b)
		c.deleteRemovalRecord(ctx, a, b, rev)
		c.reschedule(a)
		c.reschedule(b)
		return nil
	}
	if err := c.publishLinkFenced(ctx, tombstone, rec.SeqA, rec.SeqB, true, last, rev); err != nil {
		c.restoreNewerPairLink(ctx, a, b)
		return err
	}
	c.dropLocalLink(a, b)
	if c.sink != nil {
		c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true})
	}
	c.invalidateLinkText(ctx, a, b)
	c.markRemovalPublished(ctx, key, rec, rev)
	return nil
}

func (c *relationCore) restoreNewerPairLink(ctx context.Context, a, b string) {
	if c.kv == nil {
		return
	}
	rec, pairRev, ok, err := c.loadPairRecord(ctx, relationPairKey(a, b))
	if err != nil {
		return
	}
	removal, removalRev, removed, removalErr := c.loadRemovalRecord(ctx, relationRemovalKey(a, b))
	if removalErr != nil {
		return
	}
	if removed && removal.Published && (!ok || (removalRev > pairRev && removal.SeqA >= rec.SeqA && removal.SeqB >= rec.SeqB)) {
		x, y := live.SortPair(a, b)
		live.PutLink(ctx, c.kv, &dorv1.Link{A: x, B: y, UpdatedAt: c.now().Unix()})
		c.dropLocalLink(a, b)
		return
	}
	if !ok || rec.Outcome != "link" || !rec.Published || len(rec.Link) == 0 {
		return
	}
	var link dorv1.Link
	if proto.Unmarshal(rec.Link, &link) != nil {
		return
	}
	link.A, link.B = live.SortPair(link.A, link.B)
	live.PutLink(ctx, c.kv, &link)
	c.mu.Lock()
	c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength)}
	c.mu.Unlock()
}

func (c *relationCore) dropLocalLink(a, b string) {
	c.mu.Lock()
	delete(c.links, pairKey(a, b))
	delete(c.judged, pairKey(a, b))
	c.mu.Unlock()
}

func (c *relationCore) deleteRemovalRecord(ctx context.Context, a, b string, rev uint64) {
	key := relationRemovalKey(a, b)
	c.mu.Lock()
	delete(c.removals, key)
	delete(c.dirtyRem, key)
	c.mu.Unlock()
	if c.kv == nil {
		return
	}
	if rev > 0 {
		_ = c.kv.Purge(ctx, key, jetstream.LastRevision(rev))
		return
	}
	_ = c.kv.Delete(ctx, key)
}

func (c *relationCore) markRemovalPublished(ctx context.Context, key string, rec *relationRemovalRecord, rev uint64) {
	rec.Published = true
	if c.kv == nil {
		c.rememberRemoval(key, rec)
		return
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if _, err := c.kv.Update(ctx, key, data, rev); err != nil {
		return
	}
	c.rememberRemoval(key, rec)
}

func (c *relationCore) retryRemovals(ctx context.Context) {
	c.mu.Lock()
	dirty := make([]string, 0, len(c.dirtyRem))
	for key := range c.dirtyRem {
		dirty = append(dirty, key)
	}
	c.mu.Unlock()
	for _, key := range dirty {
		rec, rev, ok, err := c.loadRemovalRecord(ctx, key)
		if err != nil || !ok || rec.Published {
			continue
		}
		if err := c.applyRemovalOutput(ctx, key, rec, rev); err != nil {
			c.log.Debug("relation: removal retry deferred", "pair", key, "err", err)
		}
	}
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
