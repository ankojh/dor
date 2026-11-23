package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

func recheckJudge(t *testing.T, c *relationCore, a, b string) {
	t.Helper()
	if err := c.judgePair(context.Background(), c.index[a], c.index[b]); err != nil {
		t.Fatal(err)
	}
	link, ok := live.LoadLink(context.Background(), c.kv, a, b)
	if !ok || link.Strength <= 0 {
		t.Fatalf("judge did not create link: %+v, %v", link, ok)
	}
}

func TestRecheckRemovalRearmsForNewContent(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, &acceptedCounter{}, now)
	a := relationTestStory("rearm-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("rearm-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, other, 20)
	recheckJudge(t, c, a.Id, other.Id)
	c.tombstone(ctx, a.Id, other.Id)
	old, _, ok, err := c.loadRemovalRecord(ctx, relationRemovalKey(a.Id, other.Id))
	if err != nil || !ok || !old.Published {
		t.Fatalf("first removal incomplete: %+v %v", old, err)
	}
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "real later content"
	regressionObserve(t, c, latest, 30)
	recheckJudge(t, c, a.Id, other.Id)
	c.tombstone(ctx, a.Id, other.Id)
	got, ok := live.LoadLink(ctx, c.kv, a.Id, other.Id)
	if !ok || got.Strength > 0 {
		t.Fatalf("old published rel.r suppressed removal of new content link: %+v, ok=%v; link.changed=%d", got, ok, subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged))
	}
}

func TestRecheckAuthoritativeReadPreservesCooling(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, &acceptedCounter{}, now)
	a := relationTestStory("cold-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("cold-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, other, 20)
	cooled := proto.Clone(a).(*dorv1.Story)
	cooled.Heat = .1
	regressionObserve(t, c, cooled, 30)
	if got := c.index[a.Id].story.Heat; got != .1 {
		t.Fatalf("setup heat=%v", got)
	}
	recheckJudge(t, c, a.Id, other.Id)
	if got := c.index[a.Id].story.Heat; got != .1 {
		t.Fatalf("authoritative content hydration reset heat from .1 to %v", got)
	}
}

func TestRecheckRemovalCacheBoundedAfterExpiry(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	for i := 0; i < 20; i++ {
		a, other := fmt.Sprintf("expire-a-%d", i), fmt.Sprintf("expire-b-%d", i)
		c.index[a] = &relationEntry{story: relationTestStory(a, now.Unix(), .8, 40, -74), updated: now.Add(-49 * time.Hour).Unix()}
		c.index[other] = &relationEntry{story: relationTestStory(other, now.Unix(), .8, 40, -74), updated: now.Add(-49 * time.Hour).Unix()}
		l := &dorv1.Link{A: a, B: other, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
		if !live.PutLink(ctx, c.kv, l) {
			t.Fatal("seed link")
		}
		c.links[pairKey(a, other)] = &relationLink{link: l, publishedStrength: .8}
		c.lastDecay = time.Time{}
		c.decay(ctx, now)
	}
	c.lastDecay = time.Time{}
	c.decay(ctx, now.Add(72*time.Hour))
	if len(c.index) != 0 || len(c.links) != 0 {
		t.Fatalf("setup not expired: index=%d links=%d", len(c.index), len(c.links))
	}
	if len(c.removals) != 0 {
		t.Fatalf("published rel.r cache retained %d expired pairs after all endpoints disappeared and another horizon elapsed", len(c.removals))
	}
}

func TestRecheckMarkerAppliesWithoutFurtherContentOrCooling(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := fixedCluster(t, b, now)
	st := &storyState{Story: &dorv1.Story{Id: "settled-title", Title: "Old source heading", TitleSource: "source", Heat: 0, UpdatedAt: now.Add(-24 * time.Hour).Unix()}, UpdatedAt: now.Add(-24 * time.Hour).Unix(), ExpiresAt: now.Add(24 * time.Hour)}
	c.stories[st.Story.Id] = st
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.writeCheckpoint(ctx, kv, st); err != nil {
		t.Fatal(err)
	}
	if !live.PutStory(ctx, kv, st.Story) {
		t.Fatal("seed story")
	}
	if _, err = kv.Put(ctx, live.StoryTitleKey(st.Story.Id), []byte(`{"title":"New editor heading"}`)); err != nil {
		t.Fatal(err)
	}
	c.coolStories(ctx)
	got, ok := live.LoadStory(ctx, kv, st.Story.Id)
	if !ok || got.Title != "New editor heading" {
		t.Fatalf("marker-only editor title never reaches settled live story: %+v, ok=%v", got, ok)
	}
}

func TestRecheckMarkerOnlyRestartKeepsPreviouslyCooledHeat(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := fixedCluster(t, b, now)
	st := &storyState{Story: &dorv1.Story{Id: "marker-after-cool", Title: "Source heading", TitleSource: "source", Heat: .8, UpdatedAt: now.Add(-time.Hour).Unix()}, ArticleIDs: []string{"a"}, UpdatedAt: now.Add(-time.Hour).Unix(), ExpiresAt: now.Add(47 * time.Hour)}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.writeCheckpoint(ctx, kv, st); err != nil {
		t.Fatal(err)
	}
	cooled := proto.Clone(st.Story).(*dorv1.Story)
	cooled.Heat = .2
	if !live.PutStory(ctx, kv, cooled) {
		t.Fatal("seed cooling")
	}
	if _, err = kv.Put(ctx, live.StoryTitleKey(st.Story.Id), []byte(`{"title":"New editor heading"}`)); err != nil {
		t.Fatal(err)
	}
	restarted := fixedCluster(t, b, now)
	if err = restarted.load(ctx); err != nil {
		t.Fatal(err)
	}
	got := restarted.stories[st.Story.Id].Story
	if got.Title != "New editor heading" || got.TitleSource != "editor" {
		t.Fatalf("title failed: %+v", got)
	}
	if got.Heat != .2 {
		t.Fatalf("marker first applied at restart changed previously persisted cooled heat .2 to %v", got.Heat)
	}
}

func TestRecheckPendingJudgementCannotResurrectRemovedLink(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("recover-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("recover-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, other, 20)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	data, err := proto.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "link", Link: data}
	if _, stored, err := c.storePairRecord(ctx, relationPairKey(a.Id, other.Id), rec, 0, false); err != nil || !stored {
		t.Fatalf("store: %v %v", stored, err)
	}
	// KV write succeeded before a crash or transient publish failure; the
	// unpublished pair record will be restored along with the positive KV link.
	if !live.PutLink(ctx, c.kv, link) {
		t.Fatal("seed link")
	}
	restored := relationCoreForTest(t, ctx, b, nil, now)
	restored.fadeLink(ctx, a.Id, other.Id, live.LinkRevision(restored.links[pairKey(a.Id, other.Id)].link), 0, 24*time.Hour)
	removal, _, ok, err := restored.loadRemovalRecord(ctx, relationRemovalKey(a.Id, other.Id))
	if err != nil || !ok || !removal.Published {
		t.Fatalf("removal failed: %+v %v", removal, err)
	}
	restored.retryOutputs(ctx)
	got, ok := live.LoadLink(ctx, restored.kv, a.Id, other.Id)
	if !ok || got.Strength > 0 {
		t.Fatalf("pending rel.p resurrected a published rel.r tombstone: %+v ok=%v", got, ok)
	}
}

type recheckKVReadHook struct {
	jetstream.KeyValue
	target string
	reads  int
	after  func()
}

func (k *recheckKVReadHook) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	entry, err := k.KeyValue.Get(ctx, key)
	if key == k.target {
		k.reads++
		if k.reads == 2 {
			k.after()
		}
	}
	return entry, err
}

func TestRecheckStaleRemoveCannotPublishAfterNewerLink(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	slow := relationCoreForTest(t, ctx, b, nil, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("publish-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("publish-b", now.Unix(), .8, 40, -74)
	for _, c := range []*relationCore{slow, fast} {
		regressionObserve(t, c, a, 10)
		regressionObserve(t, c, other, 20)
	}
	key := relationPairKey(a.Id, other.Id)
	old := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "remove"}
	oldRev, stored, err := slow.storePairRecord(ctx, key, old, 0, false)
	if err != nil || !stored {
		t.Fatalf("store old: %v %v", stored, err)
	}
	// The second seqsCurrent has read both old records, but has not returned.
	// Another process commits AND publishes the new judgement during that read.
	hook := &recheckKVReadHook{KeyValue: slow.kv, target: relationStoryKey(other.Id), after: func() {
		latest := proto.Clone(a).(*dorv1.Story)
		latest.Title = "new content"
		regressionObserve(t, fast, latest, 30)
		link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
		data, _ := proto.Marshal(link)
		rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: data}
		rev, stored, err := fast.storePairRecord(ctx, key, rec, oldRev, true)
		if err != nil || !stored {
			t.Fatalf("store new: %v %v", stored, err)
		}
		if err = fast.applyPairOutput(ctx, key, rec, rev); err != nil {
			t.Fatal(err)
		}
	}}
	slow.kv = hook
	if err = slow.applyPairOutput(ctx, key, old, oldRev); err != nil {
		t.Fatal(err)
	}
	bodies := collectSubject(t, b, bus.SubjectLinkChanged)
	if len(bodies) != 1 {
		t.Fatalf("link.changed outputs = %d, want only the winning newer link", len(bodies))
	}
	var published dorv1.Link
	if err = proto.Unmarshal([]byte(bodies[0]), &published); err != nil {
		t.Fatal(err)
	}
	if published.Strength <= 0 {
		t.Fatalf("stale removal published a tombstone over the newer link: %+v", &published)
	}
	if published.A != a.Id || published.B != other.Id {
		t.Fatalf("published link pair = %s/%s, want %s/%s", published.A, published.B, a.Id, other.Id)
	}
	rec := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if rec.Outcome != "link" || rec.SeqA != 30 || rec.SeqB != 20 || !rec.Published {
		t.Fatalf("authoritative ledger = %+v, want published winning link at seq 30/20", rec)
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if link, ok := live.LoadLink(ctx, kv, a.Id, other.Id); !ok || link.Strength <= 0 {
		t.Fatalf("authoritative link state = %+v, ok=%v, want the winning newer link", link, ok)
	}
}

func TestRecheckPendingRemovalSurvivesProcessRestart(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("pending-r-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("pending-r-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, other, 20)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	if !live.PutLink(ctx, c.kv, link) {
		t.Fatal("seed")
	}
	c.links[pairKey(a.Id, other.Id)] = &relationLink{link: link, publishedStrength: .8}
	stream, _ := b.StreamNamed(ctx, bus.StreamStory)
	info, _ := stream.Info(ctx)
	cfg := info.Config
	cfg.MaxMsgs = 1
	cfg.Discard = jetstream.DiscardNew
	if _, err := b.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, bus.StoryChangedSubject(a.Id), []byte("filler"), bus.WithMsgID("recheck-r-fill")); err != nil {
		t.Fatal(err)
	}
	c.tombstone(ctx, a.Id, other.Id)
	cfg.MaxMsgs = -1
	if _, err := b.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	restarted := relationCoreForTest(t, ctx, b, nil, now)
	restarted.retryOutputs(ctx)
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("restarted removal outputs=%d, want1", got)
	}
	data, err := restarted.kv.Get(ctx, relationRemovalKey(a.Id, other.Id))
	if err != nil {
		t.Fatal(err)
	}
	var rec relationRemovalRecord
	if err = json.Unmarshal(data.Value(), &rec); err != nil || !rec.Published {
		t.Fatalf("pending removal did not complete: %+v %v", rec, err)
	}
}

func TestRecheckObserveJudgedRace(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("judged-race", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2500; i++ {
			c.mu.Lock()
			seq := c.index[a.Id].contentSeq
			c.mu.Unlock()
			c.markStoryJudged(ctx, a.Id, seq)
		}
	}()
	for i := 0; i < 2500; i++ {
		updated := proto.Clone(a).(*dorv1.Story)
		updated.Title = fmt.Sprintf("changed content %d", i)
		if err := c.observeSeq(ctx, updated, uint64(2+i)); err != nil {
			t.Error(err)
			break
		}
	}
	wg.Wait()
}
