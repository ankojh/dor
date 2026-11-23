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
		abandoned:    make(map[string]bool),
		projecting:   make(map[string]bool),
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

// relationPairRecord is the single durable per-pair generation authority for
// every mutation that can change the link projection: judged link/remove/none,
// visible positive decay and lifecycle/eviction removal. Generation is assigned
// by the successful CAS of this record, not by stream order or wall clock.
// Published is set only after this generation's KV, stream and sink projection
// succeeds, so a failed or interrupted output is retried instead of suppressed.
type relationPairRecord struct {
	A          string `json:"a"`
	B          string `json:"b"`
	SeqA       uint64 `json:"seq_a"`
	SeqB       uint64 `json:"seq_b"`
	Outcome    string `json:"outcome"` // link|remove|none
	Link       []byte `json:"link,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	Published  bool   `json:"published"`
}

// relationRemovalRecord is the durable removal outbox for decay/eviction
// tombstones. It mirrors the pair generation that owns the removal. The pair
// record remains authoritative; an outbox row that is older than the pair is
// discarded and never published.
type relationRemovalRecord struct {
	A          string `json:"a"`
	B          string `json:"b"`
	SeqA       uint64 `json:"seq_a"`
	SeqB       uint64 `json:"seq_b"`
	Generation uint64 `json:"generation,omitempty"`
	Published  bool   `json:"published"`
}

// relationLink is the in-memory relation plus the pair generation it was last
// projected from and the strength last persisted/published. Decay accumulates
// in memory and emits only visible steps; a stale owner whose generation no
// longer matches the durable authority cannot project its derived work.
type relationLink struct {
	link              *dorv1.Link
	publishedStrength float64
	generation        uint64
}

type relationCore struct {
	bus *bus.Bus
	log *slog.Logger
	cfg RelationConfig
	now func() time.Time

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error

	mu         sync.Mutex
	index      map[string]*relationEntry
	pending    map[string]time.Time
	links      map[string]*relationLink
	judged     map[string][2]uint64
	pairs      map[string]*relationPairRecord
	dirty      map[string]bool
	removals   map[string]*relationRemovalRecord
	dirtyRem   map[string]bool
	abandoned  map[string]bool
	projecting map[string]bool
	sink       LinkSink
	work       chan string
	pairLocks  []sync.Mutex
	revision   atomic.Uint64
	localSeq   atomic.Uint64
	lastDecay  time.Time
	lastRetry  time.Time

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

// pairRecordGeneration normalises legacy records written before the generation
// field existed. They are the first generation of their pair.
func pairRecordGeneration(rec *relationPairRecord) uint64 {
	if rec == nil || rec.Generation == 0 {
		return 1
	}
	return rec.Generation
}

func removalRecordGeneration(rec *relationRemovalRecord) uint64 {
	if rec == nil || rec.Generation == 0 {
		return 1
	}
	return rec.Generation
}

func relationLinkRevisionBytes(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}
	var link dorv1.Link
	if proto.Unmarshal(data, &link) != nil {
		return 0
	}
	return live.LinkRevision(&link)
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
			pkey := relationPairKey(rec.A, rec.B)
			pair := c.pairs[pkey]
			if pair == nil || (pair.Outcome != "remove" && pair.SeqA <= rec.SeqA && pair.SeqB <= rec.SeqB) {
				// Legacy outbox written before every removal had a pair
				// authority. A removal at the same or newer input than the
				// recorded link is the newer intent; adopt it durably so
				// restart and redelivery cannot resurrect the old link.
				if _, _, stored, adoptErr := c.storeRemovalAuthority(ctx, pkey, rec.A, rec.B, rec.SeqA, rec.SeqB, 0); adoptErr != nil {
					c.log.Debug("relation: legacy removal adoption deferred", "pair", rkey, "err", adoptErr)
				} else if stored {
					c.markPairDirty(pkey)
				}
			}
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
	projected := make(map[string]*dorv1.Link)
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
		if link.A == "" || link.B == "" {
			continue
		}
		projected[pairKey(link.A, link.B)] = &link
		if link.Strength <= 0 && link.Confidence <= 0 {
			// A removal tombstone already did its job; keep the record in the
			// projection map so restore can compare it with the authority.
			_ = c.kv.Delete(ctx, key)
			continue
		}
		if c.index[link.A] == nil || c.index[link.B] == nil {
			// The endpoint expired while this judge was down.
			_ = c.kv.Delete(ctx, key)
			continue
		}
		generation := uint64(0)
		if rec := c.pairs[relationPairKey(link.A, link.B)]; rec != nil && rec.Outcome == "link" &&
			relationLinkRevisionBytes(rec.Link) == live.LinkRevision(&link) {
			generation = pairRecordGeneration(rec)
		}
		c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength), generation: generation}
		links++
	}
	_ = linkLister.Stop()
	c.verifyPairProjections(ctx, projected)
	c.log.Info("relation: restored live state", "stories", restored, "links", links)
}

// verifyPairProjections compares every published pair authority with the KV
// projection. A mismatch (including the B-V2-01 tombstone-over-unpublished-
// winner case) becomes a new unpublished generation, so retry repairs KV,
// stream and sink from the same durable record.
func (c *relationCore) verifyPairProjections(ctx context.Context, projected map[string]*dorv1.Link) {
	type pairProjection struct {
		rec *relationPairRecord
		key string
	}
	c.mu.Lock()
	list := make([]pairProjection, 0, len(c.pairs))
	for key, rec := range c.pairs {
		if rec != nil {
			list = append(list, pairProjection{rec: rec, key: key})
		}
	}
	c.mu.Unlock()
	for _, item := range list {
		rec := item.rec
		a, b := live.SortPair(rec.A, rec.B)
		if c.index[a] == nil || c.index[b] == nil {
			continue
		}
		actual := projected[pairKey(a, b)]
		needsRepair := false
		switch rec.Outcome {
		case "link":
			var desired dorv1.Link
			if proto.Unmarshal(rec.Link, &desired) != nil {
				continue
			}
			needsRepair = actual == nil || live.LinkRevision(actual) != live.LinkRevision(&desired)
		case "remove":
			needsRepair = actual != nil && (actual.Strength > 0 || actual.Confidence > 0)
		case "none":
			needsRepair = actual != nil && (actual.Strength > 0 || actual.Confidence > 0)
		}
		if !needsRepair || !rec.Published {
			continue
		}
		c.bumpPairForRepair(ctx, item.key, rec)
	}
}

// bumpPairForRepair turns a published projection mismatch into an unpublished
// generation of the same desired content. The CAS keeps a concurrent newer
// judgement as the winner.
func (c *relationCore) bumpPairForRepair(ctx context.Context, key string, observed *relationPairRecord) {
	if c.kv == nil {
		c.markPairDirty(key)
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		rec, rev, ok, err := c.loadPairRecord(ctx, key)
		if err != nil || !ok || !rec.Published {
			return
		}
		if pairRecordGeneration(rec) != pairRecordGeneration(observed) {
			return
		}
		next := *rec
		next.Generation, next.Published = pairRecordGeneration(rec)+1, false
		data, err := json.Marshal(&next)
		if err != nil {
			return
		}
		if _, err := c.kv.Update(ctx, key, data, rev); err != nil {
			continue
		}
		c.rememberPair(key, &next)
		return
	}
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
// longer; quiet stories lose strength faster. Decay is a first-class pair
// generation: it is committed through the durable authority only when the
// current link is still the one this fade derived from. A newer judgement,
// removal or restart state rejects the stale decay.
func (c *relationCore) fadeLink(ctx context.Context, a, b string, revision uint64, heat float64, dt time.Duration) {
	lock := c.lockPair(a, b)
	lock.Lock()
	defer lock.Unlock()

	a, b = live.SortPair(a, b)
	key := pairKey(a, b)
	c.mu.Lock()
	rel := c.links[key]
	if rel == nil || rel.link == nil || live.LinkRevision(rel.link) != revision {
		c.mu.Unlock()
		return
	}
	base := proto.Clone(rel.link).(*dorv1.Link)
	publishedStrength := rel.publishedStrength
	generation := rel.generation
	c.mu.Unlock()

	// A hot story halves the decay rate; a cold one quadruples it.
	tau := c.linkHalfLife.Seconds() * (0.2 + 0.8*heat)
	if tau <= 0 {
		tau = c.linkHalfLife.Seconds()
	}
	factor := math.Exp(-dt.Seconds() / tau)
	strength := float64(base.Strength) * factor
	confidence := float64(base.Confidence) * factor
	if strength < 0 {
		strength = 0
	}
	if confidence < 0 {
		confidence = 0
	}
	decayed := proto.Clone(base).(*dorv1.Link)
	decayed.Strength = float32(strength)
	decayed.Confidence = float32(confidence)
	shouldPublish := publishedStrength-strength >= relationDecayMinStep
	shouldRemove := strength < relationLinkFloor || confidence < relationLinkFloor

	auth, _, ok, err := c.loadPairRecord(ctx, relationPairKey(a, b))
	if err != nil {
		return
	}
	if ok {
		if auth.Outcome != "link" {
			return
		}
		if generation != 0 {
			// Decay must be derived from the current pair generation. Content
			// revision alone can collide when two generations produced the same
			// link bytes with different inputs.
			if pairRecordGeneration(auth) != generation {
				return
			}
		} else if relationLinkRevisionBytes(auth.Link) != revision {
			return
		}
		if !c.seqsCurrent(ctx, auth.A, auth.SeqA, auth.B, auth.SeqB) {
			c.reschedule(a)
			c.reschedule(b)
			return
		}
	} else if c.kv != nil {
		actual, exists := live.LoadLink(ctx, c.kv, a, b)
		if !exists || live.LinkRevision(actual) != revision {
			return
		}
	}
	if shouldRemove {
		c.removeLinkFencedGen(ctx, a, b, revision, generation)
		return
	}
	if !shouldPublish {
		c.mu.Lock()
		if cur := c.links[key]; cur != nil && cur.link != nil && live.LinkRevision(cur.link) == revision {
			c.links[key] = &relationLink{link: decayed, publishedStrength: cur.publishedStrength, generation: cur.generation}
		}
		c.mu.Unlock()
		return
	}

	seedA, seedB := c.currentSeqs(ctx, a, b)
	expectedGeneration := uint64(0)
	if ok {
		seedA, seedB = auth.SeqA, auth.SeqB
		expectedGeneration = generation
	}
	_, stored, err := c.storeDecayRecord(ctx, relationPairKey(a, b), decayed, revision, expectedGeneration, seedA, seedB)
	if err != nil {
		c.log.Debug("relation: decay record deferred", "a", a, "b", b, "err", err)
		return
	}
	if !stored {
		return
	}
	rec, recRev, exists, err := c.loadPairRecord(ctx, relationPairKey(a, b))
	if err != nil || !exists {
		return
	}
	if err := c.applyPairOutput(ctx, relationPairKey(a, b), rec, recRev); err != nil {
		c.log.Debug("relation: decay projection deferred", "a", a, "b", b, "err", err)
	}
}

// tombstone removes a link whose endpoint left the index. It is safe when the
// pair was never in this process's table.
func (c *relationCore) tombstone(ctx context.Context, a, b string) {
	lock := c.lockPair(a, b)
	lock.Lock()
	defer lock.Unlock()
	c.mu.Lock()
	c.abandoned[pairKey(a, b)] = true
	c.mu.Unlock()
	c.removeLinkFenced(ctx, a, b, 0)
}

func (c *relationCore) pairAbandoned(a, b string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.abandoned[pairKey(a, b)]
}

func (c *relationCore) clearAbandoned(a, b string) {
	c.mu.Lock()
	delete(c.abandoned, pairKey(a, b))
	c.mu.Unlock()
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

// storePairRecord writes a durable judgement with CAS and assigns the next
// generation. An older sequence pair never overwrites a newer record, and the
// first committed result for an input revision pair is immutable even while
// unpublished.
func (c *relationCore) storePairRecord(ctx context.Context, key string, rec *relationPairRecord, rev uint64, exists bool) (uint64, bool, error) {
	if c.kv == nil {
		c.mu.Lock()
		current := c.pairs[key]
		if current != nil {
			if current.SeqA > rec.SeqA || current.SeqB > rec.SeqB ||
				(current.SeqA == rec.SeqA && current.SeqB == rec.SeqB) {
				c.mu.Unlock()
				return 0, false, nil
			}
			rec.Generation = pairRecordGeneration(current) + 1
		} else if rec.Generation == 0 {
			rec.Generation = 1
		}
		cp := *rec
		c.pairs[key] = &cp
		c.mu.Unlock()
		return 0, true, nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		if exists {
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
					return entry.Revision(), false, nil
				}
			}
			next := *rec
			next.Generation = pairRecordGeneration(&current) + 1
			data, err := json.Marshal(&next)
			if err != nil {
				return 0, false, err
			}
			if updated, err := c.kv.Update(ctx, key, data, entry.Revision()); err == nil {
				rec.Generation = next.Generation
				c.rememberPair(key, rec)
				return updated, true, nil
			}
			rev, exists = entry.Revision(), true
			continue
		}
		next := *rec
		if next.Generation == 0 {
			next.Generation = 1
		}
		data, err := json.Marshal(&next)
		if err != nil {
			return 0, false, err
		}
		if created, err := c.kv.Create(ctx, key, data); err == nil {
			rec.Generation = next.Generation
			c.rememberPair(key, rec)
			return created, true, nil
		} else if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return 0, false, err
		}
		exists = true
	}
	return 0, false, errors.New("relation: pair record contention")
}

// storeDecayRecord commits one visible positive-decay generation. It only
// succeeds when the current durable record is still the link the decay derived
// from. The caller may pass exists=false for a legacy KV-only link; a concurrent
// creator is reconciled the same way.
func (c *relationCore) storeDecayRecord(ctx context.Context, key string, decayed *dorv1.Link, baseRevision, baseGeneration, seedSeqA, seedSeqB uint64) (uint64, bool, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(decayed)
	if err != nil {
		return 0, false, err
	}
	if c.kv == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		current := c.pairs[key]
		if current == nil {
			if baseGeneration != 0 {
				return 0, false, nil
			}
			next := &relationPairRecord{A: decayed.A, B: decayed.B, SeqA: seedSeqA, SeqB: seedSeqB,
				Outcome: "link", Link: data, Generation: 1}
			cp := *next
			c.pairs[key] = &cp
			return 0, true, nil
		}
		if current.Outcome != "link" {
			return 0, false, nil
		}
		if baseGeneration != 0 {
			if pairRecordGeneration(current) != baseGeneration {
				return 0, false, nil
			}
		} else if current.SeqA != seedSeqA || current.SeqB != seedSeqB ||
			relationLinkRevisionBytes(current.Link) != baseRevision {
			return 0, false, nil
		}
		next := *current
		next.Link, next.Generation, next.Published = data, pairRecordGeneration(current)+1, false
		cp := next
		c.pairs[key] = &cp
		return 0, true, nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			if !isKVNotFound(err) {
				return 0, false, err
			}
			if baseGeneration != 0 {
				return 0, false, nil
			}
			next := &relationPairRecord{A: decayed.A, B: decayed.B, SeqA: seedSeqA, SeqB: seedSeqB,
				Outcome: "link", Link: data, Generation: 1}
			raw, err := json.Marshal(next)
			if err != nil {
				return 0, false, err
			}
			if created, err := c.kv.Create(ctx, key, raw); err == nil {
				c.rememberPair(key, next)
				return created, true, nil
			}
			continue
		}
		var current relationPairRecord
		if json.Unmarshal(entry.Value(), &current) != nil {
			return 0, false, errors.New("relation: malformed pair record")
		}
		if current.Outcome != "link" {
			return entry.Revision(), false, nil
		}
		if baseGeneration != 0 {
			if pairRecordGeneration(&current) != baseGeneration {
				return entry.Revision(), false, nil
			}
		} else if current.SeqA != seedSeqA || current.SeqB != seedSeqB ||
			relationLinkRevisionBytes(current.Link) != baseRevision {
			return entry.Revision(), false, nil
		}
		next := current
		next.Link, next.Generation, next.Published = data, pairRecordGeneration(&current)+1, false
		raw, err := json.Marshal(&next)
		if err != nil {
			return 0, false, err
		}
		if updated, err := c.kv.Update(ctx, key, raw, entry.Revision()); err == nil {
			c.rememberPair(key, &next)
			return updated, true, nil
		}
	}
	return 0, false, errors.New("relation: decay record contention")
}

// storeRemovalAuthority CASes the pair authority to a removal generation. The
// returned record is the projection the caller must apply. stored=false means
// another generation already owns the pair; the caller must not publish this
// removal.
func (c *relationCore) storeRemovalAuthority(ctx context.Context, key, a, b string, seqA, seqB, expectedGeneration uint64) (*relationPairRecord, uint64, bool, error) {
	desired := &relationPairRecord{A: a, B: b, SeqA: seqA, SeqB: seqB, Outcome: "remove"}
	if c.kv == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		current := c.pairs[key]
		if current == nil {
			if expectedGeneration != 0 {
				return nil, 0, false, nil
			}
			desired.Generation = 1
			cp := *desired
			c.pairs[key] = &cp
			return &cp, 0, true, nil
		}
		if expectedGeneration != 0 && pairRecordGeneration(current) != expectedGeneration {
			return nil, 0, false, nil
		}
		if current.SeqA > seqA || current.SeqB > seqB {
			return nil, 0, false, nil
		}
		if current.Outcome == "remove" && current.SeqA == seqA && current.SeqB == seqB {
			cp := *current
			return &cp, 0, false, nil
		}
		desired.Generation = pairRecordGeneration(current) + 1
		desired.Published = false
		cp := *desired
		c.pairs[key] = &cp
		return &cp, 0, true, nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			if !isKVNotFound(err) {
				return nil, 0, false, err
			}
			if expectedGeneration != 0 {
				return nil, 0, false, nil
			}
			desired.Generation = 1
			raw, err := json.Marshal(desired)
			if err != nil {
				return nil, 0, false, err
			}
			if created, err := c.kv.Create(ctx, key, raw); err == nil {
				c.rememberPair(key, desired)
				return desired, created, true, nil
			}
			continue
		}
		var current relationPairRecord
		if json.Unmarshal(entry.Value(), &current) != nil {
			return nil, 0, false, errors.New("relation: malformed pair record")
		}
		if expectedGeneration != 0 && pairRecordGeneration(&current) != expectedGeneration {
			return nil, entry.Revision(), false, nil
		}
		if current.SeqA > seqA || current.SeqB > seqB {
			return nil, 0, false, nil
		}
		if current.Outcome == "remove" && current.SeqA == seqA && current.SeqB == seqB {
			cp := current
			return &cp, entry.Revision(), false, nil
		}
		next := current
		next.SeqA, next.SeqB, next.Outcome, next.Link = seqA, seqB, "remove", nil
		next.Generation, next.Published = pairRecordGeneration(&current)+1, false
		raw, err := json.Marshal(&next)
		if err != nil {
			return nil, 0, false, err
		}
		if updated, err := c.kv.Update(ctx, key, raw, entry.Revision()); err == nil {
			c.rememberPair(key, &next)
			return &next, updated, true, nil
		}
	}
	return nil, 0, false, errors.New("relation: removal authority contention")
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
	if rec.Outcome != "remove" {
		delete(c.abandoned, pairKey(rec.A, rec.B))
	}
	c.mu.Unlock()
}

func (c *relationCore) deletePairRecord(ctx context.Context, a, b string, rev uint64) {
	key := relationPairKey(a, b)
	c.mu.Lock()
	delete(c.pairs, key)
	delete(c.dirty, key)
	delete(c.judged, pairKey(a, b))
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

// pairAuthorityCurrent reports whether rec is still the exact durable
// generation that owns this pair. A legacy zero generation is generation one.
func (c *relationCore) pairAuthorityCurrent(ctx context.Context, key string, rec *relationPairRecord) (*relationPairRecord, uint64, bool, error) {
	current, rev, ok, err := c.loadPairRecord(ctx, key)
	if err != nil || !ok {
		return nil, 0, false, err
	}
	return current, rev, pairRecordGeneration(current) == pairRecordGeneration(rec), nil
}

func (c *relationCore) markPairDirty(key string) {
	c.mu.Lock()
	c.dirty[key] = true
	c.mu.Unlock()
}

// projectAuthority restores the KV projection from the current pair authority
// without emitting an obsolete generation to the stream. It is the repair path
// used when a losing writer has already touched KV. The current generation's
// owner (or restart retry) publishes the stream and marks Published.
func (c *relationCore) projectAuthority(ctx context.Context, a, b string) {
	a, b = live.SortPair(a, b)
	key := relationPairKey(a, b)
	c.mu.Lock()
	if c.projecting[key] {
		c.mu.Unlock()
		return
	}
	c.projecting[key] = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.projecting, key)
		c.mu.Unlock()
	}()
	for attempt := 0; attempt < 4; attempt++ {
		rec, pairRev, ok, err := c.loadPairRecord(ctx, key)
		if err != nil {
			return
		}
		if !ok {
			if removal, _, removed, removalErr := c.loadRemovalRecord(ctx, relationRemovalKey(a, b)); removalErr == nil && removed && removal.Published {
				if c.kv != nil {
					live.PutLink(ctx, c.kv, &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()})
				}
				if _, _, exists, loadErr := c.loadPairRecord(ctx, key); loadErr != nil {
					return
				} else if exists {
					continue
				}
				c.dropLocalLink(a, b)
			}
			return
		}
		var link *dorv1.Link
		switch rec.Outcome {
		case "link":
			if len(rec.Link) == 0 {
				return
			}
			var desired dorv1.Link
			if proto.Unmarshal(rec.Link, &desired) != nil {
				return
			}
			desired.A, desired.B = live.SortPair(desired.A, desired.B)
			link = &desired
			if c.kv != nil {
				live.PutLink(ctx, c.kv, link)
			}
		case "remove":
			if c.kv != nil {
				live.PutLink(ctx, c.kv, &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()})
			}
		case "none":
			if c.linkExists(ctx, a, b) {
				// A positive projection survived from an older generation. The
				// current no-link authority must emit an explicit removal so
				// publisher state converges; this is the current generation,
				// not the stale caller.
				if last, seqErr := c.lastLinkSequence(ctx); seqErr == nil {
					if applyErr := c.applyPairRemoval(ctx, key, rec, pairRev, last); applyErr == nil {
						return
					}
				}
				c.markPairDirty(key)
				return
			}
			if c.kv != nil {
				live.PutLink(ctx, c.kv, &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()})
			}
		default:
			return
		}
		// A newer generation can claim while this repair is writing. In that
		// case rewrite from the new authority instead of leaving stale KV.
		current, _, still, loadErr := c.loadPairRecord(ctx, key)
		if loadErr != nil {
			return
		}
		if !still || current == nil || pairRecordGeneration(current) != pairRecordGeneration(rec) {
			continue
		}
		switch rec.Outcome {
		case "link":
			c.mu.Lock()
			c.links[pairKey(link.A, link.B)] = &relationLink{link: link, publishedStrength: float64(link.Strength), generation: pairRecordGeneration(rec)}
			c.mu.Unlock()
		case "remove", "none":
			c.dropLocalLink(a, b)
		}
		if rec.Published {
			c.cacheJudged(rec.A, rec.SeqA, rec.B, rec.SeqB)
		} else {
			c.markPairDirty(key)
		}
		return
	}
}

func (c *relationCore) retirePairRecord(ctx context.Context, key string, rec *relationPairRecord, rev uint64) {
	if current, _, ok, err := c.pairAuthorityCurrent(ctx, key, rec); err == nil && ok && current != nil {
		c.deletePairRecord(ctx, rec.A, rec.B, rev)
		if rec.Outcome == "remove" {
			c.retireRemovalOutbox(ctx, rec.A, rec.B, pairRecordGeneration(rec))
		}
	}
}

// retireRemovalOutbox drops the compatibility outbox for a pair generation
// that is being retired by stale-content rejection. Without this a retry would
// adopt the stale removal again and publish it.
func (c *relationCore) retireRemovalOutbox(ctx context.Context, a, b string, generation uint64) {
	rec, rev, ok, err := c.loadRemovalRecord(ctx, relationRemovalKey(a, b))
	if err != nil || !ok {
		return
	}
	if generation != 0 && rec.Generation != 0 && rec.Generation != generation {
		return
	}
	c.deleteRemovalRecord(ctx, a, b, rev)
}

func (c *relationCore) endpointsLive(a, b string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, hasA := c.index[a]
	_, hasB := c.index[b]
	return hasA && hasB
}

// applyPairOutput writes/publishes one durable pair generation. The pair record
// is the authority: every projection step is gated by it, and a losing writer
// restores the current winner (even an unpublished winner) instead of exposing
// stale KV or stream state.
func (c *relationCore) applyPairOutput(ctx context.Context, key string, rec *relationPairRecord, rev uint64) error {
	// Capture the stream fence before checking either ledger. The server
	// rejects an output if any link publication wins after this snapshot.
	last, err := c.lastLinkSequence(ctx)
	if err != nil {
		return err
	}
	_, _, current, err := c.pairAuthorityCurrent(ctx, key, rec)
	if err != nil {
		return err
	}
	if !current {
		c.projectAuthority(ctx, rec.A, rec.B)
		return nil
	}
	switch rec.Outcome {
	case "none":
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.retirePairRecord(ctx, key, rec, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if c.linkExists(ctx, rec.A, rec.B) {
			// A positive projection survived from an older generation. Clear it
			// through this authoritative no-link generation.
			return c.applyPairRemoval(ctx, key, rec, rev, last)
		}
		return c.markPairPublished(ctx, key, rec, rev)
	case "remove":
		return c.applyPairRemoval(ctx, key, rec, rev, last)
	case "link":
		var link dorv1.Link
		if err := proto.Unmarshal(rec.Link, &link); err != nil {
			return err
		}
		link.A, link.B = live.SortPair(link.A, link.B)
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.retirePairRecord(ctx, key, rec, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if c.kv != nil && !live.PutLink(ctx, c.kv, &link) {
			return errors.New("relation: link KV write failed")
		}
		// Re-check after the write: a newer generation may have projected while
		// the KV write was in flight.
		_, _, current, authErr := c.pairAuthorityCurrent(ctx, key, rec)
		if authErr != nil {
			return authErr
		}
		if !current {
			c.projectAuthority(ctx, rec.A, rec.B)
			return nil
		}
		if !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
			c.rollbackLink(ctx, &link)
			c.retirePairRecord(ctx, key, rec, rev)
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		if err := c.publishLinkFenced(ctx, &link, rec.SeqA, rec.SeqB, false, last, pairRecordGeneration(rec)); err != nil {
			c.projectAuthority(ctx, rec.A, rec.B)
			if current, seqErr := c.lastLinkSequence(ctx); seqErr == nil && current != last {
				c.reschedule(rec.A)
				c.reschedule(rec.B)
				return nil
			}
			return err
		}
		if _, _, still, authErr := c.pairAuthorityCurrent(ctx, key, rec); authErr != nil {
			return authErr
		} else if !still {
			c.projectAuthority(ctx, rec.A, rec.B)
			return nil
		}
		enqueued := true
		if c.sink != nil {
			enqueued = c.sink.EnqueueLink(relationLinkSinkRecord(&link, pairRecordGeneration(rec)))
		}
		// The sink may block or fail while a newer generation completes. Never
		// mark this generation published over stale SQL state; repair from the
		// current authority instead.
		if _, _, still, authErr := c.pairAuthorityCurrent(ctx, key, rec); authErr != nil {
			return authErr
		} else if !still {
			c.repairStaleSink(ctx, rec.A, rec.B)
			return nil
		}
		if !enqueued {
			return errors.New("relation: link sink enqueue rejected")
		}
		c.mu.Lock()
		c.links[pairKey(link.A, link.B)] = &relationLink{link: &link, publishedStrength: float64(link.Strength), generation: pairRecordGeneration(rec)}
		c.mu.Unlock()
		c.invalidateLinkText(ctx, link.A, link.B)
		return c.markPairPublished(ctx, key, rec, rev)
	default:
		return fmt.Errorf("relation: unknown pair outcome %q", rec.Outcome)
	}
}

// applyPairRemoval projects a remove/none generation as a zero-strength
// tombstone and a stream removal. Every exposure is gated by the authority.
func (c *relationCore) applyPairRemoval(ctx context.Context, key string, rec *relationPairRecord, rev, last uint64) error {
	a, b := live.SortPair(rec.A, rec.B)
	liveEndpoints := c.endpointsLive(a, b)
	if liveEndpoints && !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
		c.retirePairRecord(ctx, key, rec, rev)
		c.reschedule(rec.A)
		c.reschedule(rec.B)
		return nil
	}
	var previous *dorv1.Link
	if c.kv != nil {
		previous, _ = live.LoadLink(ctx, c.kv, a, b)
	}
	tombstone := &dorv1.Link{A: a, B: b, UpdatedAt: c.now().Unix()}
	if c.kv != nil && !live.PutLink(ctx, c.kv, tombstone) {
		return errors.New("relation: link tombstone KV write failed")
	}
	_, _, current, err := c.pairAuthorityCurrent(ctx, key, rec)
	if err != nil {
		return err
	}
	if !current {
		c.projectAuthority(ctx, a, b)
		return nil
	}
	if liveEndpoints && !c.seqsCurrent(ctx, rec.A, rec.SeqA, rec.B, rec.SeqB) {
		// Content changed while the tombstone was in flight. Do not let the
		// removal publish or survive as a retryable outbox; restore whatever
		// link was projected before this stale generation and rejudge.
		if c.kv != nil {
			if previous != nil && (previous.Strength > 0 || previous.Confidence > 0) {
				live.PutLink(ctx, c.kv, previous)
			} else {
				_ = c.kv.Delete(ctx, live.LinkKey(a, b))
			}
		}
		c.retirePairRecord(ctx, key, rec, rev)
		c.reschedule(rec.A)
		c.reschedule(rec.B)
		return nil
	}
	if err := c.publishLinkFenced(ctx, tombstone, rec.SeqA, rec.SeqB, true, last, pairRecordGeneration(rec)); err != nil {
		c.projectAuthority(ctx, a, b)
		if current, seqErr := c.lastLinkSequence(ctx); seqErr == nil && current != last {
			c.reschedule(rec.A)
			c.reschedule(rec.B)
			return nil
		}
		return err
	}
	enqueued := true
	if c.sink != nil {
		enqueued = c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true, Generation: pairRecordGeneration(rec)})
	}
	if _, _, still, authErr := c.pairAuthorityCurrent(ctx, key, rec); authErr != nil {
		return authErr
	} else if !still {
		c.repairStaleSink(ctx, a, b)
		return nil
	}
	if !enqueued {
		return errors.New("relation: removal sink enqueue rejected")
	}
	c.dropLocalLink(a, b)
	c.invalidateLinkText(ctx, a, b)
	return c.markPairPublished(ctx, key, rec, rev)
}

func relationLinkSinkRecord(link *dorv1.Link, generation uint64) store.LinkRecord {
	return store.LinkRecord{
		A: link.A, B: link.B, Type: int16(link.Type), Direction: int16(link.Dir),
		Confidence: link.Confidence, Strength: link.Strength, Unverified: link.Unverified,
		UpdatedAt:  time.Unix(link.UpdatedAt, 0).UTC(),
		DecaysAt:   time.Unix(link.UpdatedAt, 0).UTC().Add(48 * time.Hour),
		Generation: generation,
	}
}

// enqueueAuthorityProjection mirrors the current durable winner into the sink.
// It is used after a stale generation's sink completed and may have overwritten
// the newer SQL projection out of order.
func (c *relationCore) enqueueAuthorityProjection(ctx context.Context, a, b string) bool {
	if c.sink == nil {
		return true
	}
	a, b = live.SortPair(a, b)
	rec, _, ok, err := c.loadPairRecord(ctx, relationPairKey(a, b))
	if err != nil {
		return false
	}
	if !ok {
		if removal, _, removed, removalErr := c.loadRemovalRecord(ctx, relationRemovalKey(a, b)); removalErr == nil && removed && removal.Published {
			return c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true, Generation: removalRecordGeneration(removal)})
		}
		return true
	}
	switch rec.Outcome {
	case "link":
		if len(rec.Link) == 0 {
			return false
		}
		var link dorv1.Link
		if proto.Unmarshal(rec.Link, &link) != nil {
			return false
		}
		link.A, link.B = live.SortPair(link.A, link.B)
		return c.sink.EnqueueLink(relationLinkSinkRecord(&link, pairRecordGeneration(rec)))
	case "remove", "none":
		return c.sink.EnqueueLink(store.LinkRecord{A: a, B: b, Remove: true, Generation: pairRecordGeneration(rec)})
	default:
		return false
	}
}

// repairStaleSink restores KV from the current authority and then re-enqueues
// the current SQL projection behind any stale record that just completed. If
// the sink rejects the repair, make a published winner a retryable generation
// instead of leaving the database permanently behind.
func (c *relationCore) repairStaleSink(ctx context.Context, a, b string) {
	c.projectAuthority(ctx, a, b)
	if c.sink == nil {
		return
	}
	if c.enqueueAuthorityProjection(ctx, a, b) {
		return
	}
	key := relationPairKey(a, b)
	if rec, _, ok, err := c.loadPairRecord(ctx, key); err == nil && ok && rec.Published {
		c.bumpPairForRepair(ctx, key, rec)
		return
	}
	c.markPairDirty(key)
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

// lastLinkSequence and the expected-subject-sequence header reject a stream
// message once a later link generation has published. The header is a second
// line of defence behind the durable pair generation check, not the authority.
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
	_, err = c.bus.Publish(pubCtx, bus.SubjectLinkChanged, data,
		bus.WithMsgID(pairMessageID(link.A, link.B, seqA, seqB, remove, generation)),
		bus.WithHeaders(map[string][]string{"Nats-Expected-Last-Subject-Sequence": {fmt.Sprint(last)}}))
	return err
}

func pairMessageID(a, b string, seqA, seqB uint64, remove bool, generation uint64) string {
	a, b = live.SortPair(a, b)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", a, b, seqA, seqB)))
	suffix := ""
	if remove {
		suffix = ":rm"
	}
	return fmt.Sprintf("link:%s.%s:%s:g%d%s", a, b, hex.EncodeToString(sum[:8]), generation, suffix)
}

func (c *relationCore) markPairPublished(ctx context.Context, key string, rec *relationPairRecord, rev uint64) error {
	next := *rec
	next.Published = true
	if c.kv == nil {
		c.rememberPair(key, &next)
		c.cacheJudged(rec.A, rec.SeqA, rec.B, rec.SeqB)
		return nil
	}
	data, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	// CAS on the exact generation revision. A newer generation must never be
	// marked published by an older projection.
	updated, err := c.kv.Update(ctx, key, data, rev)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return nil
		}
		return err
	}
	c.rememberPair(key, &next)
	c.cacheJudged(rec.A, rec.SeqA, rec.B, rec.SeqB)
	if rec.Outcome == "remove" || rec.Outcome == "none" {
		c.markRemovalOutboxPublished(ctx, rec.A, rec.B, pairRecordGeneration(rec))
	}
	a, b := live.SortPair(rec.A, rec.B)
	if next.Outcome != "remove" {
		c.clearAbandoned(a, b)
		return nil
	}
	if c.pairAbandoned(a, b) && !c.endpointsLive(a, b) {
		// The endpoints were evicted and the tombstone is durable. Retaining
		// pair state for an abandoned pair would grow without bound.
		c.deletePairRecord(ctx, a, b, updated)
		if out, outRev, ok, loadErr := c.loadRemovalRecord(ctx, relationRemovalKey(a, b)); loadErr == nil && ok && out.Published {
			c.deleteRemovalRecord(ctx, a, b, outRev)
		}
		c.clearAbandoned(a, b)
	}
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
		if err != nil {
			continue
		}
		if !ok || rec.Published {
			c.mu.Lock()
			delete(c.dirty, key)
			c.mu.Unlock()
			continue
		}
		if err := c.applyPairOutput(ctx, key, rec, rev); err != nil {
			c.log.Debug("relation: output retry deferred", "pair", key, "err", err)
		}
	}
	c.retryRemovals(ctx)
}

// removeLinkLocked removes a link without a base-generation fence. It is used
// for endpoint eviction, where the link content itself may no longer be
// trusted.
func (c *relationCore) removeLinkLocked(ctx context.Context, a, b string) {
	c.removeLinkFenced(ctx, a, b, 0)
}

// removeLinkFenced commits a lifecycle/decay removal as a pair generation. A
// non-zero base revision requires the current durable authority to still be
// exactly that link; a stale decay cannot remove a newer judgement.
func (c *relationCore) removeLinkFenced(ctx context.Context, a, b string, baseRevision uint64) {
	c.removeLinkFencedGen(ctx, a, b, baseRevision, 0)
}

func (c *relationCore) removeLinkFencedGen(ctx context.Context, a, b string, baseRevision, baseGeneration uint64) {
	a, b = live.SortPair(a, b)
	key := pairKey(a, b)
	c.mu.Lock()
	existing := c.links[key]
	c.mu.Unlock()
	var kvLink *dorv1.Link
	if existing == nil {
		if c.kv == nil {
			return
		}
		link, ok := live.LoadLink(ctx, c.kv, a, b)
		if !ok || (link.Strength <= 0 && link.Confidence <= 0) {
			return
		}
		kvLink = link
	} else if baseRevision != 0 && c.kv != nil {
		kvLink, _ = live.LoadLink(ctx, c.kv, a, b)
	}
	pkey := relationPairKey(a, b)
	pair, pairRev, pairOK, err := c.loadPairRecord(ctx, pkey)
	if err != nil {
		return
	}
	if baseGeneration != 0 {
		if !pairOK || pairRecordGeneration(pair) != baseGeneration {
			return
		}
	} else if baseRevision != 0 {
		if pairOK {
			if pair.Outcome != "link" || relationLinkRevisionBytes(pair.Link) != baseRevision {
				return
			}
		} else if kvLink != nil {
			if live.LinkRevision(kvLink) != baseRevision {
				return
			}
		} else if existing == nil || existing.link == nil || live.LinkRevision(existing.link) != baseRevision {
			return
		}
	}
	seqA, seqB := c.currentSeqs(ctx, a, b)
	if pairOK && (pair.SeqA > seqA || pair.SeqB > seqB) {
		return
	}
	expectedGeneration := uint64(0)
	if baseGeneration != 0 {
		expectedGeneration = baseGeneration
	} else if baseRevision != 0 && pairOK {
		expectedGeneration = pairRecordGeneration(pair)
	}
	if pairOK && pair.Outcome == "remove" && pair.SeqA == seqA && pair.SeqB == seqB {
		if expectedGeneration != 0 && pairRecordGeneration(pair) != expectedGeneration {
			return
		}
		c.mirrorRemovalOutbox(ctx, a, b, seqA, seqB, pairRecordGeneration(pair))
		if pair.Published {
			c.markRemovalOutboxPublished(ctx, a, b, pairRecordGeneration(pair))
			c.dropLocalLink(a, b)
			return
		}
		if err := c.applyPairOutput(ctx, pkey, pair, pairRev); err != nil {
			c.log.Debug("relation: removal projection deferred", "a", a, "b", b, "err", err)
		}
		return
	}
	next, rev, stored, err := c.storeRemovalAuthority(ctx, pkey, a, b, seqA, seqB, expectedGeneration)
	if err != nil {
		c.log.Debug("relation: removal authority deferred", "a", a, "b", b, "err", err)
		return
	}
	if !stored {
		current, currentRev, ok, loadErr := c.loadPairRecord(ctx, pkey)
		if loadErr != nil || !ok {
			return
		}
		if current.Outcome != "remove" {
			return
		}
		c.mirrorRemovalOutbox(ctx, a, b, current.SeqA, current.SeqB, pairRecordGeneration(current))
		if !current.Published {
			_ = c.applyPairOutput(ctx, pkey, current, currentRev)
		} else {
			c.markRemovalOutboxPublished(ctx, a, b, pairRecordGeneration(current))
			c.dropLocalLink(a, b)
		}
		return
	}
	c.mirrorRemovalOutbox(ctx, a, b, seqA, seqB, pairRecordGeneration(next))
	if err := c.applyPairOutput(ctx, pkey, next, rev); err != nil {
		c.log.Debug("relation: removal projection deferred", "a", a, "b", b, "err", err)
	}
}

// mirrorRemovalOutbox keeps the compatibility removal record aligned with the
// authoritative pair generation. It is never allowed to outrun the pair.
func (c *relationCore) mirrorRemovalOutbox(ctx context.Context, a, b string, seqA, seqB, generation uint64) {
	rkey := relationRemovalKey(a, b)
	old, rev, ok, err := c.loadRemovalRecord(ctx, rkey)
	if err != nil {
		return
	}
	if ok {
		oldGen := removalRecordGeneration(old)
		if oldGen > generation {
			return
		}
		if oldGen == generation && old.Published {
			c.mu.Lock()
			delete(c.dirtyRem, rkey)
			c.mu.Unlock()
			return
		}
	}
	next := &relationRemovalRecord{A: a, B: b, SeqA: seqA, SeqB: seqB, Generation: generation}
	if ok && old.Published && removalRecordGeneration(old) == generation {
		next.Published = true
	}
	if _, stored, err := c.storeRemovalRecord(ctx, rkey, next, rev, ok); err != nil || !stored {
		return
	}
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
			if current.SeqA > rec.SeqA || current.SeqB > rec.SeqB {
				return entry.Revision(), false, nil
			}
			if current.Published && current.SeqA == rec.SeqA && current.SeqB == rec.SeqB &&
				removalRecordGeneration(&current) >= removalRecordGeneration(rec) {
				return entry.Revision(), false, nil
			}
			if removalRecordGeneration(&current) > removalRecordGeneration(rec) {
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

// applyRemovalOutput retries a removal outbox row. The pair record is the
// authority; the outbox only requests/remembers a removal that the pair record
// must also own. A stale outbox is discarded instead of being projected.
func (c *relationCore) applyRemovalOutput(ctx context.Context, key string, rec *relationRemovalRecord, rev uint64) error {
	a, b := live.SortPair(rec.A, rec.B)
	pkey := relationPairKey(a, b)
	pair, pairRev, ok, err := c.loadPairRecord(ctx, pkey)
	if err != nil {
		return err
	}
	if !ok {
		adopted, adoptedRev, _, err := c.adoptRemovalAuthority(ctx, pkey, rec)
		if err != nil || adopted == nil {
			return err
		}
		if adopted.Published {
			c.markRemovalOutboxPublished(ctx, a, b, pairRecordGeneration(adopted))
			return nil
		}
		return c.applyPairOutput(ctx, pkey, adopted, adoptedRev)
	}
	if pair.Outcome != "remove" {
		if pair.Outcome == "link" && rec.SeqA >= pair.SeqA && rec.SeqB >= pair.SeqB {
			// Legacy outbox from before removal intent lived in the pair
			// authority. A same-or-newer-input removal supersedes the link.
			next, nextRev, stored, adoptErr := c.storeRemovalAuthority(ctx, pkey, a, b, rec.SeqA, rec.SeqB, 0)
			if adoptErr != nil {
				return adoptErr
			}
			if stored {
				return c.applyPairOutput(ctx, pkey, next, nextRev)
			}
			current, currentRev, ok, loadErr := c.loadPairRecord(ctx, pkey)
			if loadErr != nil || !ok {
				return loadErr
			}
			if current.Outcome != "remove" {
				c.deleteRemovalRecord(ctx, a, b, rev)
				return nil
			}
			return c.applyPairOutput(ctx, pkey, current, currentRev)
		}
		// A genuinely newer judged link/none owns the pair. This outbox is
		// obsolete.
		c.deleteRemovalRecord(ctx, a, b, rev)
		if !pair.Published {
			return c.applyPairOutput(ctx, pkey, pair, pairRev)
		}
		return nil
	}
	pairGen, outGen := pairRecordGeneration(pair), removalRecordGeneration(rec)
	if outGen > pairGen {
		c.deleteRemovalRecord(ctx, a, b, rev)
		if !pair.Published {
			return c.applyPairOutput(ctx, pkey, pair, pairRev)
		}
		return nil
	}
	if outGen < pairGen {
		c.mirrorRemovalOutbox(ctx, a, b, pair.SeqA, pair.SeqB, pairGen)
	}
	if pair.Published {
		c.markRemovalOutboxPublished(ctx, a, b, pairGen)
		return nil
	}
	return c.applyPairOutput(ctx, pkey, pair, pairRev)
}

// adoptRemovalAuthority upgrades a legacy removal outbox (written before every
// pair had a pair record) into the single pair authority.
func (c *relationCore) adoptRemovalAuthority(ctx context.Context, key string, rec *relationRemovalRecord) (*relationPairRecord, uint64, bool, error) {
	next := &relationPairRecord{
		A: rec.A, B: rec.B, SeqA: rec.SeqA, SeqB: rec.SeqB,
		Outcome: "remove", Generation: removalRecordGeneration(rec), Published: rec.Published,
	}
	if c.kv == nil {
		c.rememberPair(key, next)
		return next, 0, true, nil
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return nil, 0, false, err
	}
	if created, err := c.kv.Create(ctx, key, raw); err == nil {
		c.rememberPair(key, next)
		return next, created, true, nil
	}
	current, rev, exists, err := c.loadPairRecord(ctx, key)
	if err != nil {
		return nil, 0, false, err
	}
	if !exists {
		return nil, 0, false, nil
	}
	return current, rev, false, nil
}

// markRemovalOutboxPublished mirrors a completed removal generation. It never
// marks an outbox generation that the pair authority has already superseded.
func (c *relationCore) markRemovalOutboxPublished(ctx context.Context, a, b string, generation uint64) {
	rkey := relationRemovalKey(a, b)
	rec, rev, ok, err := c.loadRemovalRecord(ctx, rkey)
	if err != nil || !ok {
		return
	}
	if rec.Published {
		c.mu.Lock()
		delete(c.dirtyRem, rkey)
		c.mu.Unlock()
		return
	}
	if rec.Generation != 0 && generation != 0 && rec.Generation != generation {
		return
	}
	next := *rec
	next.Published = true
	if c.kv == nil {
		c.rememberRemoval(rkey, &next)
		return
	}
	data, err := json.Marshal(&next)
	if err != nil {
		return
	}
	if _, err := c.kv.Update(ctx, rkey, data, rev); err != nil {
		return
	}
	c.rememberRemoval(rkey, &next)
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

func (c *relationCore) retryRemovals(ctx context.Context) {
	c.mu.Lock()
	dirty := make([]string, 0, len(c.dirtyRem))
	for key := range c.dirtyRem {
		dirty = append(dirty, key)
	}
	c.mu.Unlock()
	for _, key := range dirty {
		rec, rev, ok, err := c.loadRemovalRecord(ctx, key)
		if err != nil || !ok {
			continue
		}
		if rec.Published {
			c.mu.Lock()
			delete(c.dirtyRem, key)
			c.mu.Unlock()
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
