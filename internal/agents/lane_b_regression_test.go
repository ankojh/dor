package agents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Regression coverage for lane B of the final combined review. Each test
// asserts the accepted post-fix behaviour, not the original defect.

// legacyWriteHook injects a newer accepted observation at the exact write that
// the old unconditional legacy `Put` used. New code writes with CAS; the hook
// fires on Create/Update/Put so it covers both implementations.
type legacyWriteHook struct {
	jetstream.KeyValue
	once   sync.Once
	before func()
}

func (k *legacyWriteHook) fire(key string) {
	if key == relationStoryKey("legacy-a") {
		k.once.Do(k.before)
	}
}

func (k *legacyWriteHook) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	k.fire(key)
	return k.KeyValue.Put(ctx, key, value)
}

func (k *legacyWriteHook) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	k.fire(key)
	return k.KeyValue.Create(ctx, key, value, opts...)
}

func (k *legacyWriteHook) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	k.fire(key)
	return k.KeyValue.Update(ctx, key, value, revision)
}

type regressionCapture struct{ state []byte }

func (d *regressionCapture) Decide(_ context.Context, req decide.Request) (decide.Response, error) {
	d.state, _ = json.Marshal(req.State)
	return acceptedRelationResponse(), nil
}

func regressionObserve(t *testing.T, c *relationCore, story *dorv1.Story, seq uint64) {
	t.Helper()
	if err := c.observeSeq(context.Background(), story, seq); err != nil {
		t.Fatal(err)
	}
}

// B02: a lagging payload must never be judged under a newer durable revision.
func TestRelationLocalPayloadCannotBorrowNewerRevision(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	d := &regressionCapture{}
	slow := relationCoreForTest(t, ctx, b, d, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("payload-a", now.Unix(), .8, 40, -74)
	a.Title = "OLD_PAYLOAD"
	bb := relationTestStory("payload-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, slow, a, 10)
	regressionObserve(t, slow, bb, 20)
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "NEW_PAYLOAD"
	regressionObserve(t, fast, latest, 30)

	if err := slow.judgePair(ctx, slow.index[a.Id], slow.index[bb.Id]); err != nil {
		t.Fatal(err)
	}
	rec := pairRecordForTest(t, ctx, b, a.Id, bb.Id)
	if rec.SeqA != 30 {
		t.Fatalf("pair record seq = %d, want the authoritative 30", rec.SeqA)
	}
	if bytes.Contains(d.state, []byte("OLD_PAYLOAD")) {
		t.Fatalf("lagging payload was judged and committed under revision 30: %s", d.state)
	}
	if !bytes.Contains(d.state, []byte("NEW_PAYLOAD")) {
		t.Fatalf("authoritative payload was not supplied to the decider: %s", d.state)
	}
}

// B03: overlapping judgements of one input revision pair commit one immutable
// result and KV/stream agree on it.
func TestRelationConcurrentPairResultsHaveOneWinner(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	a := relationTestStory("winner-a", now.Unix(), .8, 40, -74)
	bb := relationTestStory("winner-b", now.Unix(), .8, 40, -74)
	c1 := relationCoreForTest(t, ctx, b, nil, now)
	c2 := relationCoreForTest(t, ctx, b, nil, now)
	for _, c := range []*relationCore{c1, c2} {
		regressionObserve(t, c, a, 10)
		regressionObserve(t, c, bb, 20)
	}
	key := relationPairKey(a.Id, bb.Id)
	mk := func(strength float32) *relationPairRecord {
		link := &dorv1.Link{A: a.Id, B: bb.Id, Strength: strength, Confidence: .9, UpdatedAt: now.Unix()}
		ld, err := proto.Marshal(link)
		if err != nil {
			t.Fatal(err)
		}
		return &relationPairRecord{A: a.Id, B: bb.Id, SeqA: 10, SeqB: 20, Outcome: "link", Link: ld}
	}
	first, second := mk(.6), mk(.9)
	r1, stored1, err := c1.storePairRecord(ctx, key, first, 0, false)
	if err != nil || !stored1 {
		t.Fatalf("first result: stored=%v err=%v", stored1, err)
	}
	_, stored2, err := c2.storePairRecord(ctx, key, second, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if stored2 {
		t.Fatal("second overlapping result overwrote the first equal-input result")
	}
	if err := c1.applyPairOutput(ctx, key, first, r1); err != nil {
		t.Fatal(err)
	}
	// The loser recovers the winner instead of publishing its own answer.
	winner, winnerRev, ok, err := c2.loadPairRecord(ctx, key)
	if err != nil || !ok {
		t.Fatalf("winner record: ok=%v err=%v", ok, err)
	}
	if winner.Published {
		c2.cacheJudged(a.Id, winner.SeqA, bb.Id, winner.SeqB)
	} else if err := c2.applyPairOutput(ctx, key, winner, winnerRev); err != nil {
		t.Fatal(err)
	}

	kvLink, ok := live.LoadLink(ctx, c1.kv, a.Id, bb.Id)
	if !ok || kvLink.Strength != .6 {
		t.Fatalf("KV link = %+v (ok=%v), want the first winner at 0.6", kvLink, ok)
	}
	bodies := collectSubject(t, b, bus.SubjectLinkChanged)
	if len(bodies) != 1 {
		t.Fatalf("link.changed messages = %d, want 1", len(bodies))
	}
	var wire dorv1.Link
	if err := proto.Unmarshal([]byte(bodies[0]), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Strength != kvLink.Strength {
		t.Fatalf("stream/KV disagree: wire %.2f KV %.2f", wire.Strength, kvLink.Strength)
	}
}

// B04: a stale pending removal must not erase a newer published link.
func TestRelationStaleRemoveCannotEraseNewerLink(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("remove-a", now.Unix(), .8, 40, -74)
	bb := relationTestStory("remove-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, bb, 20)
	key := relationPairKey(a.Id, bb.Id)
	old := &relationPairRecord{A: a.Id, B: bb.Id, SeqA: 10, SeqB: 20, Outcome: "remove"}
	oldRev, stored, err := c.storePairRecord(ctx, key, old, 0, false)
	if err != nil || !stored {
		t.Fatalf("old removal: stored=%v err=%v", stored, err)
	}
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "new content"
	regressionObserve(t, c, latest, 30)
	link := &dorv1.Link{A: a.Id, B: bb.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	ld, err := proto.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	newer := &relationPairRecord{A: a.Id, B: bb.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: ld}
	rev, stored, err := c.storePairRecord(ctx, key, newer, oldRev, true)
	if err != nil || !stored {
		t.Fatalf("newer link: stored=%v err=%v", stored, err)
	}
	if err := c.applyPairOutput(ctx, key, newer, rev); err != nil {
		t.Fatal(err)
	}
	// Resume the old removal after the newer link is published.
	if err := c.applyPairOutput(ctx, key, old, oldRev); err != nil {
		t.Fatal(err)
	}
	got, ok := live.LoadLink(ctx, c.kv, a.Id, bb.Id)
	if !ok || got.Strength == 0 {
		t.Fatal("old pending removal erased a newer published link although the ledger still says the newer link is published")
	}
}

// B05: legacy restore uses the same monotonic CAS and keeps newer accepted
// content instead of rolling it back to sequence 1.
func TestRelationLegacyRestorePreservesNewerRecord(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	slow := relationCoreForTest(t, ctx, b, nil, now) // restored before the legacy story exists
	fast := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("legacy-a", now.Unix(), .8, 40, -74)
	a.Title = "legacy old"
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	sd, err := proto.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryKey(a.Id), sd); err != nil {
		t.Fatal(err)
	}
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "newest accepted content"
	// The fast process accepts newer content after the slow process has
	// scanned rel.s but before its legacy write lands. The hook fires on the
	// old unconditional Put and on the new CAS Create/Update.
	slow.kv = &legacyWriteHook{KeyValue: slow.kv, before: func() { regressionObserve(t, fast, latest, 100) }}
	slow.restore(ctx)

	rec := loadRelationStoryRecord(t, ctx, b, a.Id)
	if rec.ContentSeq != 100 {
		t.Fatalf("slow legacy restore overwrote newer durable seq 100 with seq %d", rec.ContentSeq)
	}
	var payload dorv1.Story
	if err := proto.Unmarshal(rec.Story, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Title != latest.Title {
		t.Fatalf("legacy restore kept payload %q, want %q", payload.Title, latest.Title)
	}
	slow.mu.Lock()
	title := slow.index[a.Id].story.Title
	slow.mu.Unlock()
	if title != latest.Title {
		t.Fatalf("restored index payload = %q, want %q", title, latest.Title)
	}
}

// B06: explicit distinct hazard identities veto a similarity merge.
func TestClusterDifferentHazardEventsNeverMerge(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := fixedCluster(t, b, now)
	for i := 0; i < 2; i++ {
		a := goldenArticle(fmt.Sprintf("hazard-%d", i), "usgs", "M 6.1 - 15 km S of Town Alpha", "", now.Unix())
		a.Hints = &dorv1.RawHints{Hazard: "earthquake", EventId: fmt.Sprintf("different-event-%d", i), Revision: "1"}
		putJoinState(t, ctx, b, a.Id, joinState{Article: a, Place: goldenPlace("Town Alpha", 40, -74), Class: goldenClass(dorv1.Category_CATEGORY_DISASTER), Vector: &dorv1.EmbeddedArticle{Article: a, Vector: goldenBase()}})
		processGoldenJoin(t, ctx, c, a.Id)
	}
	if len(c.stories) != 2 {
		t.Fatalf("two explicit different USGS event IDs with equal text/embeddings merged into %d story", len(c.stories))
	}
	for _, st := range c.stories {
		if len(st.EventKeys) != 1 {
			t.Fatalf("story %s event identities = %v, want exactly one", st.Story.Id, st.EventKeys)
		}
	}
}

// B07: heat-only observations replace immutable entries; concurrent candidate
// scoring must be race-free. Run this under -race.
func TestRelationHeatObservationRace(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a := relationTestStory("heat-a", now.Unix(), .8, 40, -74)
	bb := relationTestStory("heat-b", now.Unix(), .8, 40, -74)
	regressionObserve(t, c, a, 10)
	regressionObserve(t, c, bb, 20)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			hot := proto.Clone(a).(*dorv1.Story)
			hot.Heat = float32(i%10) / 10
			if err := c.observeSeq(ctx, hot, uint64(100+i)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		c.mu.Lock()
		entry := c.index[a.Id]
		c.mu.Unlock()
		c.candidates(entry, now)
	}
	wg.Wait()
}

// B08: decay/eviction removals use a durable outbox; a rejected publish is
// retried, and a published removal is not re-published.
func TestRelationDecayRemovalRetriesAfterPublishFailure(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a, bb := "decay-a", "decay-b"
	link := &dorv1.Link{A: a, B: bb, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	if !live.PutLink(ctx, c.kv, link) {
		t.Fatal("seed link")
	}
	c.links[pairKey(a, bb)] = &relationLink{link: link, publishedStrength: .8}

	stream, err := b.StreamNamed(ctx, bus.StreamStory)
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
	if _, err := b.Publish(ctx, bus.StoryChangedSubject(a), []byte("filler"), bus.WithMsgID("decay-filler")); err != nil {
		t.Fatal(err)
	}
	c.removeLinkLocked(ctx, a, bb)

	removal, _, ok, err := c.loadRemovalRecord(ctx, relationRemovalKey(a, bb))
	if err != nil || !ok {
		t.Fatalf("removal outbox record: ok=%v err=%v", ok, err)
	}
	if removal.Published {
		t.Fatal("removal marked published after the link.changed stream rejected the tombstone")
	}
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 0 {
		t.Fatalf("link.changed messages before retry = %d, want 0", got)
	}

	cfg.MaxMsgs = -1
	if _, err := b.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	c.retryRemovals(ctx)

	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("link.changed removal messages after retry = %d, want 1", got)
	}
	removal, _, ok, err = c.loadRemovalRecord(ctx, relationRemovalKey(a, bb))
	if err != nil || !ok || !removal.Published {
		t.Fatalf("removal not retired after publish: %+v (ok=%v err=%v)", removal, ok, err)
	}
	c.mu.Lock()
	_, stillLocal := c.links[pairKey(a, bb)]
	c.mu.Unlock()
	if stillLocal {
		t.Fatal("published removal kept the local link")
	}

	// Both retry paths and a repeated removal must not publish again.
	c.retryOutputs(ctx)
	c.retryRemovals(ctx)
	c.removeLinkLocked(ctx, a, bb)
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("published removal re-emitted: %d messages, want 1", got)
	}
}

// B09: cooling preserves the editor-owned title and source.
func TestClusterCoolingPreservesEditorOwnedTitle(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := fixedCluster(t, b, now)
	st := &storyState{Story: &dorv1.Story{Id: "editor-cooling", Title: "Original source title", TitleSource: "source", Heat: .8, UpdatedAt: now.Add(-time.Hour).Unix()}, UpdatedAt: now.Add(-time.Hour).Unix(), ExpiresAt: now.Add(47 * time.Hour), CooledAt: now.Add(-6 * time.Hour).UnixNano()}
	c.stories[st.Story.Id] = st
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writeCheckpoint(ctx, kv, st); err != nil {
		t.Fatal(err)
	}
	edited := proto.Clone(st.Story).(*dorv1.Story)
	edited.Title = "Editor-owned title"
	edited.TitleSource = "editor"
	data, err := proto.Marshal(edited)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryKey(edited.Id), data); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, "storytitle."+edited.Id, []byte(`{"title":"Editor-owned title"}`)); err != nil {
		t.Fatal(err)
	}
	c.coolStories(ctx)
	after, ok := live.LoadStory(ctx, kv, edited.Id)
	if !ok {
		t.Fatal("cooled story missing")
	}
	if after.Title != edited.Title || after.TitleSource != "editor" {
		t.Fatalf("quiet-story cooling overwrote editor title with %q (source %q)", after.Title, after.TitleSource)
	}
	if after.Heat >= .8 {
		t.Fatalf("cooling did not decay heat: %v", after.Heat)
	}
}

// B10: an editor marker must not block adoption of persisted cooled heat.
func TestClusterCooledEditorStorySurvivesRestart(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now()
	c := fixedCluster(t, b, now)
	st := &storyState{Story: &dorv1.Story{Id: "editor-heat", Title: "Edited title", TitleSource: "editor", Heat: .8, UpdatedAt: now.Add(-time.Hour).Unix()}, ArticleIDs: []string{"a"}, UpdatedAt: now.Add(-time.Hour).Unix(), ExpiresAt: now.Add(47 * time.Hour)}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writeCheckpoint(ctx, kv, st); err != nil {
		t.Fatal(err)
	}
	cooled := proto.Clone(st.Story).(*dorv1.Story)
	cooled.Heat = .2
	data, err := proto.Marshal(cooled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryKey(cooled.Id), data); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, "storytitle."+cooled.Id, []byte(`{"title":"Edited title"}`)); err != nil {
		t.Fatal(err)
	}
	reloaded := fixedCluster(t, b, now)
	if err := reloaded.load(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reloaded.stories[cooled.Id].Story.Heat; got != cooled.Heat {
		t.Fatalf("editor marker made recovery revert cooled heat %.1f -> %.1f", cooled.Heat, got)
	}
	if got := reloaded.stories[cooled.Id].Story.Title; got != "Edited title" {
		t.Fatalf("editor title lost on restart: %q", got)
	}
}

// B09+B10 combined: the editor title overlays while membership, locations and
// heat continue to follow cluster state through cooling and restart.
func TestClusterEditorOverlayComposesWithClusterContent(t *testing.T) {
	b := durabilityBus(t)
	ctx := context.Background()
	now := time.Now().UTC()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{
		ClusterNow:          func() time.Time { return now },
		ClusterHeatHalfLife: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := agent.(*clusterer)
	place := goldenPlace("capital", 40, -74)
	class := goldenClass(dorv1.Category_CATEGORY_DISASTER, "Agency")
	a1 := goldenArticle("overlay-a1", "src1", "Original source title", "lede one", now.Unix())
	putJoinState(t, ctx, b, a1.Id, joinState{Article: a1, Place: place, Class: class, Vector: &dorv1.EmbeddedArticle{Article: a1, Vector: goldenBase()}})
	processGoldenJoin(t, ctx, c, a1.Id)
	sid := c.articleStory[a1.Id]

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, "storytitle."+sid, []byte(`{"title":"Editor-owned headline"}`)); err != nil {
		t.Fatal(err)
	}

	// A cluster content merge must keep the editor title while membership,
	// locations and provenance follow the merged cluster state.
	a2 := goldenArticle("overlay-a2", "src2", "Original source title", "lede two", now.Unix()+1)
	putJoinState(t, ctx, b, a2.Id, joinState{Article: a2, Place: goldenPlace("harbor", 41, -75), Class: class, Vector: &dorv1.EmbeddedArticle{Article: a2, Vector: goldenBase()}})
	processGoldenJoin(t, ctx, c, a2.Id)

	merged, ok := live.LoadStory(ctx, kv, sid)
	if !ok {
		t.Fatal("merged story missing")
	}
	if merged.Title != "Editor-owned headline" || merged.TitleSource != "editor" {
		t.Fatalf("content merge lost editor title: %q (%q)", merged.Title, merged.TitleSource)
	}
	if merged.SourceCount != 2 {
		t.Fatalf("source count after merge = %d, want 2", merged.SourceCount)
	}
	if len(merged.Locations) < 2 {
		t.Fatalf("locations after merge = %d, want both cluster locations", len(merged.Locations))
	}
	locationCount := len(merged.Locations)
	mergedHeat := merged.Heat
	if mergedHeat <= clusterHeatFloor {
		t.Fatalf("merged heat = %v, want positive cluster heat", mergedHeat)
	}

	// Cooling writes a derived heat value and must compose with the marker.
	st := c.stories[sid]
	st.Story.Heat = .8
	st.Story.UpdatedAt = now.Add(-time.Hour).Unix()
	st.UpdatedAt = now.Add(-time.Hour).Unix()
	st.CooledAt = now.Add(-2 * time.Hour).UnixNano()
	if _, err := c.writeCheckpoint(ctx, kv, st); err != nil {
		t.Fatal(err)
	}
	c.coolStories(ctx)
	cooled, ok := live.LoadStory(ctx, kv, sid)
	if !ok {
		t.Fatal("cooled story missing")
	}
	if cooled.Title != "Editor-owned headline" || cooled.TitleSource != "editor" {
		t.Fatalf("cooling lost editor title: %q (%q)", cooled.Title, cooled.TitleSource)
	}
	if cooled.SourceCount != 2 || len(cooled.Locations) != locationCount {
		t.Fatalf("cooling changed cluster content: sources=%d locations=%d", cooled.SourceCount, len(cooled.Locations))
	}
	if cooled.Heat >= .8 {
		t.Fatalf("cooling did not decay heat: %v", cooled.Heat)
	}

	reloaded := fixedCluster(t, b, now)
	if err := reloaded.load(ctx); err != nil {
		t.Fatal(err)
	}
	restored := reloaded.stories[sid]
	if restored == nil {
		t.Fatal("story missing after restart")
	}
	if restored.Story.Title != "Editor-owned headline" || restored.Story.TitleSource != "editor" {
		t.Fatalf("restart lost editor title: %q (%q)", restored.Story.Title, restored.Story.TitleSource)
	}
	if restored.Story.SourceCount != 2 || len(restored.Story.Locations) != locationCount {
		t.Fatalf("restart lost cluster content: sources=%d locations=%d", restored.Story.SourceCount, len(restored.Story.Locations))
	}
	if restored.Story.Heat != cooled.Heat {
		t.Fatalf("restart reverted cooled heat %v -> %v", cooled.Heat, restored.Story.Heat)
	}
}

// B01: marker retirement must never erase a join recreated after the marker
// snapshot.
func TestJoinsPurgeDeletesNeverErasesResurrectedJoin(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("join.done-%04d", i)
		if _, err := kv.Put(ctx, key, []byte("old")); err != nil {
			t.Fatal(err)
		}
		if err := kv.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	victim := "join.gc-article"
	if _, err := kv.Put(ctx, victim, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, victim); err != nil {
		t.Fatal(err)
	}
	article := goldenArticle("gc-article", "wire", "Current article after previous story expiry", "", time.Now().Unix())
	fresh, err := json.Marshal(joinState{Article: article, Place: goldenPlace("City", 40, -74), Class: goldenClass(dorv1.Category_CATEGORY_OTHER), Vector: &dorv1.EmbeddedArticle{Article: article, Vector: goldenBase()}, UpdatedAt: time.Now().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	injected := make(chan error, 1)
	sub, err := b.NC().Subscribe("$JS.API.STREAM.PURGE.KV_joins", func(_ *nats.Msg) {
		once.Do(func() { _, err := kv.Put(ctx, victim, fresh); injected <- err })
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := b.NC().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-injected:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("purge API observer did not run")
	}
	if _, err := kv.Get(ctx, victim); err != nil {
		c := newGoldenClusterer(t, b)
		if processErr := c.process(ctx, article.Id); processErr != nil {
			t.Fatal(processErr)
		}
		t.Fatalf("PurgeDeletes erased a newly acknowledged complete join after snapshotting its old marker: %v; stories after process=%d", err, len(c.stories))
	}
}

// Restart clears a preloaded retry delay so a durable join still forms a story.
func TestClusterRestartProcessesDurableJoin(t *testing.T) {
	b := durabilityBus(t)
	c := newGoldenClusterer(t, b)
	r := startRunner(t, c)
	defer stopRunner(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c.queuedMu.Lock()
	c.retryAt["after-restart"] = time.Now().Add(time.Hour)
	c.queuedMu.Unlock()
	if err := r.Restart(ctx, ClusterDurable); err != nil {
		t.Fatal(err)
	}
	a := goldenArticle("after-restart", "wire", "Road reopened following repair", "", time.Now().Unix())
	putJoinState(t, ctx, b, a.Id, joinState{Article: a, Place: goldenPlace("City", 40, -74), Class: goldenClass(dorv1.Category_CATEGORY_OTHER), Vector: &dorv1.EmbeddedArticle{Article: a, Vector: goldenBase()}})
	waitFor(t, "durable join processed after restart and retryAt cleared", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.articleStory[a.Id] != ""
	})
}
