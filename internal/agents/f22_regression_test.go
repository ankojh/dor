package agents

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Ported from the F2·2 independent review probes. Each asserts the accepted
// post-fix behaviour, not the original defect.

func TestPublisherFreshUpdateSurvivesExpiry(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())

	now := time.Now()
	old := &dorv1.Story{Id: "s-expiry", Title: "old", UpdatedAt: now.Add(-49 * time.Hour).Unix()}
	c.onStory(old)
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := proto.Clone(old).(*dorv1.Story)
	fresh.Title = "fresh acknowledged update"
	fresh.UpdatedAt = now.Unix()
	ack := make(chan error, 1)
	go func() {
		ack <- c.acceptInput(ctx, &Message{Subject: bus.StoryChangedSubject(fresh.Id), StreamSequence: 100}, "story:"+fresh.Id, func() { c.onStory(fresh) })
	}()
	waitFor(t, "pending fresh update", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.pending["story:"+fresh.Id].GetStoryUpsert() != nil
	})
	c.expireStories(now)
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	if c.committed.Stories[fresh.Id] == nil {
		t.Fatal("fresh input ACKed, but authority committed story_expire; replay is now suppressed by the committed input revision")
	}
	if got := c.committed.Stories[fresh.Id].Title; got != fresh.Title {
		t.Fatalf("committed story title = %q, want %q", got, fresh.Title)
	}
}

func TestClusterTornDerivedWriteRecoversFromCheckpoint(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	makeCluster := func() *clusterer {
		agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{ClusterNow: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		return agent.(*clusterer)
	}
	c := makeCluster()
	a := goldenArticle("torn-a", "src1", "Market fire contained in capital", "Firefighters contained the fire", now.Unix())
	place := goldenPlace("capital", 40, -74)
	class := goldenClass(dorv1.Category_CATEGORY_DISASTER, "Fire Department")
	putJoinState(t, ctx, b, a.Id, joinState{Article: a, Place: place, Class: class, Vector: &dorv1.EmbeddedArticle{Article: a, Vector: goldenBase()}})
	processGoldenJoin(t, ctx, c, a.Id)
	sid := c.articleStory[a.Id]
	old := c.stories[sid]

	a2 := goldenArticle("torn-b", "src2", a.Title, a.Lede, now.Unix())
	putJoinState(t, ctx, b, a2.Id, joinState{Article: a2, Place: place, Class: class, Vector: &dorv1.EmbeddedArticle{Article: a2, Vector: goldenBase()}})
	// Simulate a crash after the derived story write, before storymeta. The
	// authoritative checkpoint still has the pre-merge membership, so recovery
	// must not treat the torn story as a second merge.
	c.mergeStory(old, a2, place, class.Category, goldenBase(), class.Entities, now)
	old.ArticleIDs = appendUnique(old.ArticleIDs, a2.Id)
	old.SourceIDs = appendUnique(old.SourceIDs, a2.SourceId)
	expectedHeat := old.Story.Heat
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	sd, _ := proto.Marshal(old.Story)
	if _, err := kv.Put(ctx, live.StoryKey(sid), sd); err != nil {
		t.Fatal(err)
	}

	restarted := makeCluster()
	if err := restarted.load(ctx); err != nil {
		t.Fatal(err)
	}
	// The checkpoint is authoritative: the torn derived story must have been
	// repaired back to the checkpoint membership.
	if got := restarted.stories[sid]; got == nil {
		t.Fatal("checkpoint story missing after restart")
	} else if len(got.ArticleIDs) != 1 {
		t.Fatalf("torn derived write leaked membership into recovery: %v", got.ArticleIDs)
	}
	processGoldenJoin(t, ctx, restarted, a2.Id)
	if got := restarted.stories[sid].Story.Heat; got != expectedHeat {
		t.Fatalf("same article merged twice after torn story/storymeta write: heat %f -> %f", expectedHeat, got)
	}
	if ids := restarted.stories[sid].ArticleIDs; len(ids) != 2 {
		t.Fatalf("recovered membership = %v, want both articles exactly once", ids)
	}
}

func TestClusterOrphanCommitRemovedOnLoad(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	c := newGoldenClusterer(t, b)
	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.markCommitted(ctx, joins, "expired-story", "old-article"); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := joins.Get(ctx, commitKey("expired-story")); err == nil {
		t.Fatal("orphan commit survives load with no live stories")
	}
}

func seedReviewRelation(t *testing.T, b *bus.Bus, ctx context.Context, now time.Time) (*dorv1.Story, *dorv1.Story) {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	a := relationTestStory("review-a", now.Unix(), .8, 40, -74)
	bb := relationTestStory("review-b", now.Unix(), .8, 40, -74)
	for _, story := range []*dorv1.Story{a, bb} {
		sd, err := proto.Marshal(story)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := kv.Put(ctx, live.StoryKey(story.Id), sd); err != nil {
			t.Fatal(err)
		}
		putRelationMeta(t, ctx, b, story.Id, goldenBase(), []string{"Shared"}, story.FirstSeen, story.UpdatedAt)
	}
	return a, bb
}

func TestRelationRestoreSchedulesAcknowledgedTriggers(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	first := relationCoreForTest(t, ctx, b, nil, now)
	a, bb := seedReviewRelation(t, b, ctx, now)
	first.observe(ctx, a)
	first.observe(ctx, bb)
	if len(first.pending) != 2 {
		t.Fatal("invalid setup")
	}
	// Both observe handlers returned successfully (input ACK), then the process
	// died before the debounce fired.
	fresh := relationCoreForTest(t, ctx, b, nil, now)
	if len(fresh.pending) == 0 {
		t.Fatalf("restored %d stories but 0 pending judgements: acknowledged triggers are lost on restart", len(fresh.index))
	}
	if fresh.index[a.Id] == nil || fresh.index[bb.Id] == nil {
		t.Fatal("restore lost indexed stories")
	}
}

func TestRelationDurableDedupeAcrossCores(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	a, bb := seedReviewRelation(t, b, ctx, now)
	d := &acceptedCounter{}
	for i := 0; i < 2; i++ {
		c := relationCoreForTest(t, ctx, b, d, now)
		if err := c.judgePair(ctx, c.index[a.Id], c.index[bb.Id]); err != nil {
			t.Fatalf("judge pair in core %d: %v", i, err)
		}
	}
	if d.count() != 1 {
		t.Fatalf("same endpoint contents judged %d times across independent cores; dedupe/locks not durable or shared", d.count())
	}
}

type blockingLinkKV struct {
	jetstream.KeyValue
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (k *blockingLinkKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	if strings.HasPrefix(key, "link.") {
		k.once.Do(func() { close(k.entered); <-k.release })
	}
	return k.KeyValue.Put(ctx, key, value)
}

func TestRelationFenceCoversLinkWrite(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	a, bb := seedReviewRelation(t, b, ctx, now)
	c := relationCoreForTest(t, ctx, b, &acceptedCounter{}, now)
	kv := &blockingLinkKV{KeyValue: c.kv, entered: make(chan struct{}), release: make(chan struct{})}
	c.kv = kv
	done := make(chan struct{})
	go func() {
		_ = c.judgePair(ctx, c.index[a.Id], c.index[bb.Id])
		close(done)
	}()
	select {
	case <-kv.entered:
	case <-ctx.Done():
		t.Fatal("no link write")
	}
	changed := proto.Clone(a).(*dorv1.Story)
	changed.Title = "Updated while old answer is being persisted"
	c.observe(ctx, changed)
	close(kv.release)
	<-done
	if link, ok := live.LoadLink(ctx, kv.KeyValue, a.Id, bb.Id); ok && link.Strength > 0 {
		t.Fatal("old answer persisted and published after endpoint revision changed between currentRevisions and PutLink")
	}
}

func TestRelationEvictionBoundsJudgedPairs(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	a, bb := seedReviewRelation(t, b, ctx, now)
	c := relationCoreForTest(t, ctx, b, &acceptedCounter{}, now)
	if err := c.judgePair(ctx, c.index[a.Id], c.index[bb.Id]); err != nil {
		t.Fatal(err)
	}
	c.decay(ctx, now.Add(49*time.Hour))
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.judged) != 0 {
		t.Fatalf("all endpoints evicted but %d judged pair revisions retained", len(c.judged))
	}
	if len(c.pairs) != 0 {
		t.Fatalf("all endpoints evicted but %d durable pair records retained", len(c.pairs))
	}
}

func TestClusterHazardRevisionStaysInStory(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	c := newGoldenClusterer(t, b)
	now := time.Now()
	titles := []string{"M 6.1 - 15 km S of Town Alpha", "M 7.4 - 90 km W of City Beta"}
	for i, title := range titles {
		h := &dorv1.RawHints{Hazard: "earthquake", EventId: "usgs-event-1", Revision: fmt.Sprint(i)}
		id := articleIDFor("https://earthquake.usgs.gov/earthquakes/eventpage/event-1", h)
		a := goldenArticle(id, "usgs", title, "", now.Unix())
		a.Hints = h
		putJoinState(t, ctx, b, id, joinState{Article: a, Place: goldenPlace("earthquake", 40, -74), Class: goldenClass(dorv1.Category_CATEGORY_DISASTER), Vector: &dorv1.EmbeddedArticle{Article: a}})
		processGoldenJoin(t, ctx, c, id)
	}
	if len(c.stories) != 1 {
		t.Fatalf("same stable hazard event, revised magnitude/epicenter, produced %d stories in no-embed mode", len(c.stories))
	}
}

func TestPublisherCoolingRacesFlushSafely(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	now := time.Now()
	c.onStory(&dorv1.Story{Id: "cool-race", Heat: .8, UpdatedAt: now.Add(-time.Hour).Unix()})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				c.coolStories(now)
				runtime.Gosched()
			}
		}
	}()
	for i := 0; i < 20; i++ {
		c.onStory(&dorv1.Story{Id: "cool-race", Heat: .8, Title: fmt.Sprint(i), UpdatedAt: now.Add(-time.Hour).Unix()})
		if err := c.flush(ctx); err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	wg.Wait()
}

func TestRelationFailedPublishRetries(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	a, bb := seedReviewRelation(t, b, ctx, now)
	d := &acceptedCounter{}
	c := relationCoreForTest(t, ctx, b, d, now)
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(bus.SubjectLinkChanged))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxMsgs = 1
	cfg.Discard = jetstream.DiscardNew
	if _, err := b.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	filler, _ := proto.Marshal(a)
	if _, err := b.Publish(ctx, bus.StoryChangedSubject(a.Id), filler, bus.WithMsgID("fill-story-stream")); err != nil {
		t.Fatal(err)
	}
	if err := c.judgePair(ctx, c.index[a.Id], c.index[bb.Id]); err == nil {
		t.Fatal("judge pair succeeded although the link.changed stream rejected the publish")
	}
	if _, ok := live.LoadLink(ctx, c.kv, a.Id, bb.Id); !ok {
		t.Fatal("invalid setup: no KV link")
	}
	rec, _, ok, err := c.loadPairRecord(ctx, relationPairKey(a.Id, bb.Id))
	if err != nil || !ok {
		t.Fatalf("pair record after failed publish: ok=%v err=%v", ok, err)
	}
	if rec.Published {
		t.Fatal("pair marked published after a failed link.changed publish")
	}
	cfg.MaxMsgs = -1
	if _, err := b.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	c.retryOutputs(ctx)
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("restored stream, %d link messages; pair already marked judged after failed Publish, so no retry", got)
	}
	rec, _, ok, err = c.loadPairRecord(ctx, relationPairKey(a.Id, bb.Id))
	if err != nil || !ok {
		t.Fatalf("pair record after retry: ok=%v err=%v", ok, err)
	}
	if !rec.Published {
		t.Fatal("pair record still unpublished after the retry published the link")
	}
}

func TestClusterExpiryCascadeDrains(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{ClusterNow: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	c := agent.(*clusterer)
	c.stories["first"] = &storyState{Story: &dorv1.Story{Id: "first"}, ExpiresAt: now.Add(-time.Second)}
	c.expireStories(ctx)
	now = now.Add(30 * time.Second)
	c.stories["last"] = &storyState{Story: &dorv1.Story{Id: "last"}, ExpiresAt: now.Add(-time.Second)}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	link := &dorv1.Link{A: "last", B: "live", Strength: .7, Confidence: .8}
	if !live.PutLink(ctx, kv, link) {
		t.Fatal("seed link")
	}
	c.expireStories(ctx)
	now = now.Add(2 * time.Minute)
	c.expireStories(ctx)
	if _, ok := live.LoadLink(ctx, kv, "last", "live"); ok {
		t.Fatal("queued expiry cascade never drained after time threshold without a subsequent new story expiry")
	}
}
