package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func relationTestStory(id string, updated int64, heat float32, lat, lon float32) *dorv1.Story {
	return &dorv1.Story{
		Id:        id,
		Title:     "Story " + id,
		Category:  dorv1.Category_CATEGORY_POLITICS,
		Heat:      heat,
		FirstSeen: updated - 600,
		UpdatedAt: updated,
		Locations: []*dorv1.Location{{
			PlaceId: "place",
			Name:    "Place",
			Lat:     lat,
			Lon:     lon,
			Role:    dorv1.Role_ROLE_ORIGIN,
			Weight:  1,
		}},
	}
}

func putRelationMeta(t *testing.T, ctx context.Context, b *bus.Bus, id string, vec []float32, entities []string, firstSeen, updated int64) {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(live.StoryMeta{
		ID:         id,
		ArticleIDs: []string{id + "-article"},
		SourceIDs:  []string{"wire"},
		Embedding:  vec,
		Entities:   entities,
		FirstSeen:  firstSeen,
		UpdatedAt:  updated,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryMetaKey(id), data); err != nil {
		t.Fatal(err)
	}
}

func publishRelationStory(t *testing.T, ctx context.Context, b *bus.Bus, story *dorv1.Story, suffix string) {
	t.Helper()
	data, err := proto.Marshal(story)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, bus.StoryChangedSubject(story.Id), data,
		bus.WithMsgID(fmt.Sprintf("story:%s:%s", story.Id, suffix))); err != nil {
		t.Fatal(err)
	}
}

func relationPairIDs(t *testing.T) (string, string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		a := fmt.Sprintf("rel-a-%d", i)
		b := fmt.Sprintf("rel-b-%d", i)
		if bus.StoryShard(a) != bus.StoryShard(b) {
			return a, b
		}
	}
	t.Fatal("could not find two story ids on different shards")
	return "", ""
}

func waitRelationLink(t *testing.T, ctx context.Context, b *bus.Bus, a, other string, timeout time.Duration) *dorv1.Link {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	var got *dorv1.Link
	waitForTimeout(t, timeout, "relation link", func() bool {
		link, ok := live.LoadLink(ctx, kv, a, other)
		if ok && link.Strength > 0 {
			got = link
			return true
		}
		return false
	})
	return got
}

func waitForTimeout(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRelationCrossShardLinkAppears starts three judges on shards 0/1/2 and
// publishes two stories whose ids belong to different story.changed shards. The
// global index plus per-pair ownership must produce exactly one thematic,
// unverified link even with a nil decider.
func TestRelationCrossShardLinkAppears(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	a, bb := relationPairIDs(t)
	now := time.Now().Unix()
	if bus.StoryShard(a) == bus.StoryShard(bb) {
		t.Fatal("test pair is not cross-shard")
	}
	vec := goldenBase()
	aStory := relationTestStory(a, now, 0.8, 40.7, -74.0)
	bStory := relationTestStory(bb, now, 0.8, 40.7, -74.0)
	putRelationMeta(t, ctx, b, a, vec, []string{"Acme Corp"}, now-600, now)
	putRelationMeta(t, ctx, b, bb, vec, []string{"Acme Corp"}, now-600, now)

	metrics := NewMetrics()
	agents := make([]Agent, 0, 3)
	for shard := 0; shard < 3; shard++ {
		agent, err := NewRelationJudge(b, metrics, testLogger(), RelationConfig{
			Shards:   3,
			Shard:    shard,
			Debounce: 40 * time.Millisecond,
			Workers:  2,
		})
		if err != nil {
			t.Fatalf("new relation judge %d: %v", shard, err)
		}
		agents = append(agents, agent)
	}
	runner := startRunner(t, agents...)
	defer stopRunner(t, runner)

	publishRelationStory(t, ctx, b, aStory, "1")
	publishRelationStory(t, ctx, b, bStory, "1")

	link := waitRelationLink(t, ctx, b, a, bb, 20*time.Second)
	if link.A > link.B {
		t.Fatalf("link endpoints are not sorted: %s > %s", link.A, link.B)
	}
	if link.Type != dorv1.LinkType_LINK_TYPE_THEMATIC {
		t.Fatalf("link type = %v, want thematic", link.Type)
	}
	if !link.Unverified {
		t.Fatal("nil-decider link is not marked unverified")
	}
	if link.Dir != dorv1.Direction_DIRECTION_UNCLEAR {
		t.Fatalf("link direction = %v, want unclear", link.Dir)
	}

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	lister, err := kv.ListKeysFiltered(ctx, "link.>")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lister.Stop() }()
	count := 0
	for range lister.Keys() {
		count++
	}
	if count != 1 {
		t.Fatalf("live_state holds %d links, want exactly one pair", count)
	}
}

type countingDecider struct {
	mu    sync.Mutex
	calls int
}

func (d *countingDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return notRelatedResponse(), nil
}

func (d *countingDecider) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func notRelatedResponse() decide.Response {
	return decide.Response{Answers: map[string]decide.Answer{
		"related": {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: 0.05},
	}}
}

func acceptedRelationResponse() decide.Response {
	return decide.Response{Answers: map[string]decide.Answer{
		"related":   {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: 0.95},
		"type":      {Type: decide.TypeChoice, Present: true, Choice: "thematic", Probabilities: map[string]float64{"thematic": 0.9}},
		"direction": {Type: decide.TypeChoice, Present: true, Choice: "unclear", Probabilities: map[string]float64{"unclear": 0.9}},
		"strength":  {Type: decide.TypeScore, Present: true, HasScore: true, Score: 2, Probabilities: map[string]float64{"2": 0.8}},
	}}
}

// TestRelationPairJudgedOnce verifies that once one endpoint is already indexed,
// a new update to the other endpoint is judged by exactly one shard's decider.
func TestRelationPairJudgedOnce(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	a, bb := relationPairIDs(t)
	now := time.Now().Unix()
	vec := goldenBase()
	putRelationMeta(t, ctx, b, a, vec, []string{"Acme Corp"}, now-600, now)
	putRelationMeta(t, ctx, b, bb, vec, []string{"Acme Corp"}, now-600, now)

	metrics := NewMetrics()
	deciders := make([]*countingDecider, 0, 3)
	cores := make([]*relationCore, 0, 3)
	agents := make([]Agent, 0, 3)
	for shard := 0; shard < 3; shard++ {
		decider := &countingDecider{}
		agent, err := NewRelationJudge(b, metrics, testLogger(), RelationConfig{
			Decider:  decider,
			Shards:   3,
			Shard:    shard,
			Debounce: 40 * time.Millisecond,
			Workers:  1,
		})
		if err != nil {
			t.Fatalf("new relation judge %d: %v", shard, err)
		}
		deciders = append(deciders, decider)
		cores = append(cores, agent.(*relationAgent).core)
		agents = append(agents, agent)
	}
	runner := startRunner(t, agents...)
	defer stopRunner(t, runner)

	publishRelationStory(t, ctx, b, relationTestStory(a, now, 0.8, 40.7, -74.0), "1")
	waitForTimeout(t, 20*time.Second, "first story observed", func() bool {
		for _, core := range cores {
			core.mu.Lock()
			indexed := core.index[a] != nil
			pending, work := len(core.pending), len(core.work)
			core.mu.Unlock()
			if !indexed || pending != 0 || work != 0 {
				return false
			}
		}
		return true
	})
	// Let the last in-flight judgement drain before the second trigger.
	time.Sleep(250 * time.Millisecond)

	publishRelationStory(t, ctx, b, relationTestStory(bb, now, 0.8, 40.7, -74.0), "1")
	waitForTimeout(t, 20*time.Second, "pair judged", func() bool {
		total := 0
		for _, d := range deciders {
			total += d.count()
		}
		return total > 0
	})
	time.Sleep(250 * time.Millisecond)

	total := 0
	for i, d := range deciders {
		if got := d.count(); got > 0 {
			total += got
			if !cores[i].ownsPair(a, bb) {
				t.Fatalf("shard %d judged a pair it does not own", i)
			}
		}
	}
	if total != 1 {
		t.Fatalf("pair judged %d times across shards, want 1", total)
	}
}

type blockingDecider struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func newBlockingDecider() *blockingDecider {
	return &blockingDecider{started: make(chan struct{}, 64), release: make(chan struct{}, 64)}
}

func (d *blockingDecider) Decide(_ context.Context, _ decide.Request) (decide.Response, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.started <- struct{}{}
	<-d.release
	return acceptedRelationResponse(), nil
}

func (d *blockingDecider) Close() { close(d.release) }

// TestRelationStaleJudgementIsFenced interrupts a judgement with a newer
// story.changed for one endpoint. The stale answer must not be written and the
// pair must be re-judged against the newer revision.
func TestRelationStaleJudgementIsFenced(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	a, bb := relationPairIDs(t)
	now := time.Now().Unix()
	vec := goldenBase()
	putRelationMeta(t, ctx, b, a, vec, []string{"Acme Corp"}, now-600, now)
	putRelationMeta(t, ctx, b, bb, vec, []string{"Acme Corp"}, now-600, now)

	decider := newBlockingDecider()
	agent, err := NewRelationJudge(b, NewMetrics(), testLogger(), RelationConfig{
		Decider:  decider,
		Shards:   1,
		Shard:    -1,
		Debounce: 40 * time.Millisecond,
		Workers:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	core := agent.(*relationAgent).core
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)
	defer decider.Close()

	publishRelationStory(t, ctx, b, relationTestStory(a, now, 0.8, 40.7, -74.0), "1")
	waitForTimeout(t, 20*time.Second, "first story indexed", func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return core.index[a] != nil && len(core.pending) == 0 && len(core.work) == 0
	})
	time.Sleep(250 * time.Millisecond)

	publishRelationStory(t, ctx, b, relationTestStory(bb, now, 0.8, 40.7, -74.0), "1")
	select {
	case <-decider.started:
	case <-time.After(20 * time.Second):
		t.Fatal("decider was never called")
	}

	// Change one endpoint while the model is answering.
	updated := relationTestStory(a, now, 0.9, 41.0, -74.5)
	updated.Title = "Completely revised story " + a
	publishRelationStory(t, ctx, b, updated, "2")

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the judge has observed the new revision before the blocked
	// answer returns; otherwise the fence would correctly see only the old one.
	waitForTimeout(t, 20*time.Second, "revised story observed", func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return core.index[a] != nil && core.index[a].story.GetTitle() == updated.Title
	})
	decider.release <- struct{}{} // release the stale call
	select {
	case <-decider.started:
	case <-time.After(20 * time.Second):
		t.Fatal("pair was not re-judged after the story changed")
	}
	if link, ok := live.LoadLink(ctx, kv, a, bb); ok && link.Strength > 0 {
		t.Fatalf("stale judgement wrote a link: %+v", link)
	}
	decider.release <- struct{}{} // release the fresh judgement

	link := waitRelationLink(t, ctx, b, a, bb, 20*time.Second)
	if link.Type != dorv1.LinkType_LINK_TYPE_THEMATIC {
		t.Fatalf("re-judged link type = %v, want thematic", link.Type)
	}
}

type errorDecider struct{}

func (errorDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	return decide.Response{}, fmt.Errorf("model unavailable")
}

type refusingDecider struct{}

func (refusingDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	return decide.Response{Answers: map[string]decide.Answer{
		"related": {Type: decide.TypeNoul, Present: true, Refusal: true},
	}}, nil
}

func relationCoreForTest(t *testing.T, ctx context.Context, b *bus.Bus, decider RelateDecider, now time.Time) *relationCore {
	t.Helper()
	agent, err := NewRelationJudge(b, NewMetrics(), testLogger(), RelationConfig{
		Decider: decider,
		Shards:  1,
		Shard:   -1,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new relation judge: %v", err)
	}
	core := agent.(*relationAgent).core
	if err := core.init(ctx); err != nil {
		t.Fatal(err)
	}
	return core
}

func relationTestEntry(core *relationCore, id string, vec []float32, entities []string, firstSeen int64, lat, lon float64) *relationEntry {
	story := relationTestStory(id, firstSeen, 0.8, float32(lat), float32(lon))
	return &relationEntry{
		story:     story,
		vec:       vec,
		entities:  entities,
		lat:       lat,
		lon:       lon,
		firstSeen: firstSeen,
		updated:   firstSeen,
		rev:       core.revision.Add(1),
	}
}

// TestRelationUnverifiedFallback exercises every degraded decider mode: a nil
// model, a model error and a refusal all produce a similarity-only thematic
// link marked unverified. Below-threshold similarity produces nothing.
func TestRelationUnverifiedFallback(t *testing.T) {
	cases := []struct {
		name    string
		decider RelateDecider
	}{
		{name: "nil decider", decider: nil},
		{name: "erroring decider", decider: errorDecider{}},
		{name: "refusing decider", decider: refusingDecider{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testBus(t, t.TempDir())
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			now := time.Now().UTC()
			core := relationCoreForTest(t, ctx, b, tc.decider, now)

			firstSeen := now.Unix() - 600
			a := relationTestEntry(core, "fallback-a", goldenBase(), []string{"Acme Corp"}, firstSeen, 40.7, -74)
			bb := relationTestEntry(core, "fallback-b", goldenBase(), []string{"Acme Corp"}, firstSeen+30, 40.7, -74)
			core.index[a.story.Id] = a
			core.index[bb.story.Id] = bb

			core.judgePair(ctx, a, bb)

			kv, err := b.KV(ctx, bus.KVBucketLiveState)
			if err != nil {
				t.Fatal(err)
			}
			link, ok := live.LoadLink(ctx, kv, a.story.Id, bb.story.Id)
			if !ok {
				t.Fatal("fallback produced no link")
			}
			if link.Type != dorv1.LinkType_LINK_TYPE_THEMATIC {
				t.Fatalf("fallback type = %v, want thematic (never causal/reaction)", link.Type)
			}
			if !link.Unverified {
				t.Fatal("fallback link is not marked unverified")
			}
			if link.Dir != dorv1.Direction_DIRECTION_UNCLEAR {
				t.Fatalf("fallback direction = %v, want unclear", link.Dir)
			}
			if link.Strength <= 0 {
				t.Fatalf("fallback strength = %v, want positive", link.Strength)
			}
		})
	}

	t.Run("below threshold similarity", func(t *testing.T) {
		b := testBus(t, t.TempDir())
		defer b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		now := time.Now().UTC()
		core := relationCoreForTest(t, ctx, b, nil, now)

		orthogonal := make([]float32, embeddingDim)
		orthogonal[1] = 1
		a := relationTestEntry(core, "weak-a", goldenBase(), nil, now.Unix()-200000, 0, 0)
		bb := relationTestEntry(core, "weak-b", orthogonal, nil, now.Unix(), 0, 0)
		core.index[a.story.Id] = a
		core.index[bb.story.Id] = bb

		core.judgePair(ctx, a, bb)

		kv, err := b.KV(ctx, bus.KVBucketLiveState)
		if err != nil {
			t.Fatal(err)
		}
		if link, ok := live.LoadLink(ctx, kv, a.story.Id, bb.story.Id); ok && link.Strength > 0 {
			t.Fatalf("below-threshold pair produced a link: %+v", link)
		}
	})
}

// TestRelationRestoreRebuildsIndexAndLinks seeds live state exactly as a
// restarted judge finds it: the index must include every live story, the link
// table must be restored, and an orphan link whose endpoint is gone must be
// deleted. Decay must then see the restored link.
func TestRelationRestoreRebuildsIndexAndLinks(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UTC()
	nowUnix := now.Unix()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"restore-a", "restore-b"} {
		story := relationTestStory(id, nowUnix-60, 0.8, 40.7, -74.0)
		data, err := proto.Marshal(story)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := kv.Put(ctx, live.StoryKey(id), data); err != nil {
			t.Fatal(err)
		}
		putRelationMeta(t, ctx, b, id, goldenBase(), []string{"Acme Corp"}, nowUnix-600, nowUnix-60)
	}
	restoredLink := &dorv1.Link{A: "restore-a", B: "restore-b", Strength: 0.7, Confidence: 0.8, UpdatedAt: nowUnix - 60}
	if !live.PutLink(ctx, kv, restoredLink) {
		t.Fatal("seed link")
	}
	orphan := &dorv1.Link{A: "restore-a", B: "restore-gone", Strength: 0.7, Confidence: 0.8, UpdatedAt: nowUnix - 60}
	if !live.PutLink(ctx, kv, orphan) {
		t.Fatal("seed orphan link")
	}

	core := relationCoreForTest(t, ctx, b, nil, now)
	if core.index["restore-a"] == nil || core.index["restore-b"] == nil {
		t.Fatalf("index after restore = %v, want both stories", core.index)
	}
	key := pairKey("restore-a", "restore-b")
	if core.links[key] == nil {
		t.Fatalf("link table after restore = %v, want the live pair", core.links)
	}
	if _, ok := live.LoadLink(ctx, kv, orphan.A, orphan.B); ok {
		t.Fatal("orphan link whose endpoint is missing was not deleted")
	}

	core.lastDecay = now.Add(-relationDecayEvery)
	core.decay(ctx, now)
	core.mu.Lock()
	got := core.links[key]
	core.mu.Unlock()
	if got == nil {
		t.Fatal("decay dropped the restored link")
	}
	if float64(got.link.Strength) >= 0.7 {
		t.Fatalf("restored link did not fade: %v", got.link.Strength)
	}
}

// TestRelationDecayEvictsIndexAndCascadesLinks checks the 48 h index horizon and
// the link tombstone cascade for a story that left the index.
func TestRelationDecayEvictsIndexAndCascadesLinks(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UTC()
	core := relationCoreForTest(t, ctx, b, nil, now)
	old := relationTestEntry(core, "evict-old", goldenBase(), []string{"Acme Corp"}, now.Add(-49*time.Hour).Unix(), 10, 10)
	fresh := relationTestEntry(core, "evict-fresh", goldenBase(), []string{"Acme Corp"}, now.Unix(), 10, 10)
	core.index[old.story.Id] = old
	core.index[fresh.story.Id] = fresh

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	link := &dorv1.Link{A: old.story.Id, B: fresh.story.Id, Strength: 0.9, Confidence: 0.9, UpdatedAt: now.Unix()}
	if !live.PutLink(ctx, kv, link) {
		t.Fatal("seed link")
	}
	key := pairKey(old.story.Id, fresh.story.Id)
	core.links[key] = &relationLink{link: link, publishedStrength: 0.9}
	core.lastDecay = now.Add(-relationDecayEvery)

	core.decay(ctx, now)

	core.mu.Lock()
	_, indexHasOld := core.index[old.story.Id]
	_, tableHasLink := core.links[key]
	core.mu.Unlock()
	if indexHasOld {
		t.Fatal("index entry older than 48 h was not evicted")
	}
	if tableHasLink {
		t.Fatal("link touching the evicted story remains in the table")
	}
	tombstone, ok := live.LoadLink(ctx, kv, link.A, link.B)
	if !ok {
		t.Fatal("removal tombstone was not written")
	}
	if tombstone.Strength != 0 || tombstone.Confidence != 0 {
		t.Fatalf("tombstone = %+v, want zero strength/confidence", tombstone)
	}
}

// TestRelationDecayFadesQuietLinkAndRemovesBelowFloor drives heat-scaled decay
// with a fake clock: a quiet pair fades visibly, and a pair pushed under the
// floor is tombstoned.
func TestRelationDecayFadesQuietLinkAndRemovesBelowFloor(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UTC()
	agent, err := NewRelationJudge(b, NewMetrics(), testLogger(), RelationConfig{
		Shards:       1,
		Shard:        -1,
		LinkHalfLife: time.Hour,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	core := agent.(*relationAgent).core
	if err := core.init(ctx); err != nil {
		t.Fatal(err)
	}
	quietA := relationTestEntry(core, "quiet-a", goldenBase(), []string{"Acme Corp"}, now.Unix()-600, 10, 10)
	quietB := relationTestEntry(core, "quiet-b", goldenBase(), []string{"Acme Corp"}, now.Unix()-500, 10, 10)
	quietA.story.Heat, quietB.story.Heat = 0.1, 0.1
	weakA := relationTestEntry(core, "weak-a", goldenBase(), nil, now.Unix()-400, 20, 20)
	weakB := relationTestEntry(core, "weak-b", goldenBase(), nil, now.Unix()-300, 20, 20)
	weakA.story.Heat, weakB.story.Heat = 0.5, 0.5
	for _, e := range []*relationEntry{quietA, quietB, weakA, weakB} {
		core.index[e.story.Id] = e
	}

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	quietKey := pairKey(quietA.story.Id, quietB.story.Id)
	quietLink := &dorv1.Link{A: quietA.story.Id, B: quietB.story.Id, Strength: 0.9, Confidence: 0.9, UpdatedAt: now.Unix()}
	if !live.PutLink(ctx, kv, quietLink) {
		t.Fatal("seed quiet link")
	}
	core.links[quietKey] = &relationLink{link: quietLink, publishedStrength: 0.9}
	weakKey := pairKey(weakA.story.Id, weakB.story.Id)
	weakLink := &dorv1.Link{A: weakA.story.Id, B: weakB.story.Id, Strength: 0.04, Confidence: 0.04, UpdatedAt: now.Unix()}
	if !live.PutLink(ctx, kv, weakLink) {
		t.Fatal("seed weak link")
	}
	core.links[weakKey] = &relationLink{link: weakLink, publishedStrength: 0.04}
	core.lastDecay = now.Add(-relationDecayEvery)

	core.decay(ctx, now)

	core.mu.Lock()
	quiet := core.links[quietKey]
	_, weakStill := core.links[weakKey]
	core.mu.Unlock()
	if quiet == nil {
		t.Fatal("quiet link was removed instead of fading")
	}
	if float64(quiet.link.Strength) >= 0.9 {
		t.Fatalf("quiet link did not fade: %v", quiet.link.Strength)
	}
	if weakStill {
		t.Fatal("link below the strength floor remains in the table")
	}
	tombstone, ok := live.LoadLink(ctx, kv, weakLink.A, weakLink.B)
	if !ok || tombstone.Strength != 0 {
		t.Fatalf("below-floor link tombstone = %+v (ok=%v)", tombstone, ok)
	}
	persisted, ok := live.LoadLink(ctx, kv, quietLink.A, quietLink.B)
	if !ok || float64(persisted.Strength) >= 0.9 {
		t.Fatalf("faded link was not persisted: %+v (ok=%v)", persisted, ok)
	}
}
