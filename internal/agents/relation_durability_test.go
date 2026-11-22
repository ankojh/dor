package agents

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func loadRelationStoryRecord(t *testing.T, ctx context.Context, b *bus.Bus, id string) relationStoryRecord {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, relationStoryKey(id))
	if err != nil {
		t.Fatal(err)
	}
	var rec relationStoryRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestRelationRevisionOrdering covers durable content ordering: an older
// stream sequence replay cannot replace newer restored content, a real content
// update advances contentSeq and schedules, and a heat-only republish does
// neither.
func TestRelationRevisionOrdering(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	core := relationCoreForTest(t, ctx, b, nil, now)
	story := relationTestStory("rev-order", now.Unix(), 0.8, 40, -74)

	if err := core.observeSeq(ctx, story, 10); err != nil {
		t.Fatal(err)
	}
	if got := loadRelationStoryRecord(t, ctx, b, story.Id).ContentSeq; got != 10 {
		t.Fatalf("content seq after first observe = %d, want 10", got)
	}

	newer := proto.Clone(story).(*dorv1.Story)
	newer.Title = "newer content at sequence twenty"
	if err := core.observeSeq(ctx, newer, 20); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	entry := core.index[story.Id]
	gotSeq, gotTitle, gotRev := entry.contentSeq, entry.story.Title, entry.rev
	core.mu.Unlock()
	if gotSeq != 20 || gotTitle != newer.Title {
		t.Fatalf("newer content not applied: seq=%d title=%q", gotSeq, gotTitle)
	}

	older := proto.Clone(story).(*dorv1.Story)
	older.Title = "older retained replay"
	older.UpdatedAt = now.Add(-time.Hour).Unix()
	if err := core.observeSeq(ctx, older, 15); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	entry = core.index[story.Id]
	gotSeq, gotTitle, gotRevAfterReplay := entry.contentSeq, entry.story.Title, entry.rev
	core.mu.Unlock()
	if gotSeq != 20 || gotTitle != newer.Title {
		t.Fatalf("obsolete replay replaced newer content: seq=%d title=%q", gotSeq, gotTitle)
	}
	if gotRevAfterReplay != gotRev {
		t.Fatalf("obsolete replay advanced the local revision %d -> %d", gotRev, gotRevAfterReplay)
	}
	if got := loadRelationStoryRecord(t, ctx, b, story.Id).ContentSeq; got != 20 {
		t.Fatalf("obsolete replay rewrote the durable content seq to %d", got)
	}

	// Heat-only republish: same content, derived heat differs.
	core.mu.Lock()
	delete(core.pending, story.Id)
	core.mu.Unlock()
	heatOnly := proto.Clone(newer).(*dorv1.Story)
	heatOnly.Heat = 0.1
	if err := core.observeSeq(ctx, heatOnly, 30); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	entry = core.index[story.Id]
	gotSeq, gotRev = entry.contentSeq, entry.rev
	_, pending := core.pending[story.Id]
	core.mu.Unlock()
	if gotSeq != 20 {
		t.Fatalf("heat-only republish advanced content seq to %d", gotSeq)
	}
	if gotRev != gotRevAfterReplay {
		t.Fatalf("heat-only republish advanced local revision %d -> %d", gotRevAfterReplay, gotRev)
	}
	if pending {
		t.Fatal("heat-only republish scheduled a judgement")
	}
	if got := loadRelationStoryRecord(t, ctx, b, story.Id).ContentSeq; got != 20 {
		t.Fatalf("heat-only republish rewrote content seq to %d", got)
	}

	changed := proto.Clone(newer).(*dorv1.Story)
	changed.Title = "real content update at sequence forty"
	if err := core.observeSeq(ctx, changed, 40); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	entry = core.index[story.Id]
	gotSeq = entry.contentSeq
	_, pending = core.pending[story.Id]
	core.mu.Unlock()
	if gotSeq != 40 {
		t.Fatalf("content seq after real update = %d, want 40", gotSeq)
	}
	if !pending {
		t.Fatal("real content update did not schedule a judgement")
	}
}

func pairRecordForTest(t *testing.T, ctx context.Context, b *bus.Bus, a, other string) relationPairRecord {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, relationPairKey(a, other))
	if err != nil {
		t.Fatal(err)
	}
	var rec relationPairRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestRelationPairCASOneRecord checks two independent cores judging the same
// revision pair commit one durable pair record and publish one link.
func TestRelationPairCASOneRecord(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	a, bb := "cas-core-a", "cas-core-b"

	deciders := make([]*acceptedCounter, 0, 2)
	cores := make([]*relationCore, 0, 2)
	for i := 0; i < 2; i++ {
		d := &acceptedCounter{}
		core := relationCoreForTest(t, ctx, b, d, now)
		core.index[a] = &relationEntry{story: relationTestStory(a, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 60, updated: now.Unix(), rev: 1, contentSeq: 1}
		core.index[bb] = &relationEntry{story: relationTestStory(bb, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 30, updated: now.Unix(), rev: 2, contentSeq: 2}
		cores = append(cores, core)
		deciders = append(deciders, d)
	}
	for _, core := range cores {
		if err := core.judgePair(ctx, core.index[a], core.index[bb]); err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for _, d := range deciders {
		total += d.count()
	}
	if total != 1 {
		t.Fatalf("decider calls across independent cores = %d, want 1", total)
	}
	rec := pairRecordForTest(t, ctx, b, a, bb)
	if !rec.Published || rec.SeqA != 1 || rec.SeqB != 2 {
		t.Fatalf("pair record = %+v, want published seq 1/2", rec)
	}
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("link.changed messages = %d, want 1", got)
	}
}

// TestRelationStalePairWriteCannotOverwrite drives a judgement with older
// content revisions after a newer pair record exists. The stale judgement must
// neither call the decider nor replace the newer durable record.
func TestRelationStalePairWriteCannotOverwrite(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	a, bb := "stale-pair-a", "stale-pair-b"
	d := &acceptedCounter{}
	core := relationCoreForTest(t, ctx, b, d, now)

	newer := &relationPairRecord{A: a, B: bb, SeqA: 20, SeqB: 20, Outcome: "none"}
	rev, stored, err := core.storePairRecord(ctx, relationPairKey(a, bb), newer, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev == 0 || !stored {
		t.Fatal("pair record was not created")
	}
	// Stale in-memory entries claim revisions 10/10.
	core.index[a] = &relationEntry{story: relationTestStory(a, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 60, updated: now.Unix(), rev: 1, contentSeq: 10}
	core.index[bb] = &relationEntry{story: relationTestStory(bb, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 30, updated: now.Unix(), rev: 2, contentSeq: 10}

	if err := core.judgePair(ctx, core.index[a], core.index[bb]); err != nil {
		t.Fatal(err)
	}
	if got := d.count(); got != 0 {
		t.Fatalf("stale judgement called the decider %d times", got)
	}
	rec := pairRecordForTest(t, ctx, b, a, bb)
	if rec.SeqA != 20 || rec.SeqB != 20 {
		t.Fatalf("stale judgement overwrote the newer pair record: %+v", rec)
	}
}

// TestRelationEvictionDropsDurableTriggerState checks that endpoint eviction
// removes the rel.s trigger, rel.p pair and rel.j judged records, not just the
// in-memory maps.
func TestRelationEvictionDropsDurableTriggerState(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	d := &acceptedCounter{}
	core := relationCoreForTest(t, ctx, b, d, now)
	a, bb := seedReviewRelation(t, b, ctx, now)
	if err := core.observeSeq(ctx, a, 1); err != nil {
		t.Fatal(err)
	}
	if err := core.observeSeq(ctx, bb, 2); err != nil {
		t.Fatal(err)
	}
	if err := core.judgePair(ctx, core.index[a.Id], core.index[bb.Id]); err != nil {
		t.Fatal(err)
	}
	core.markStoryJudged(ctx, a.Id, 1)
	core.markStoryJudged(ctx, bb.Id, 2)

	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		relationStoryKey(a.Id),
		relationStoryKey(bb.Id),
		relationPairKey(a.Id, bb.Id),
		relationJudgedKey(a.Id, -1),
		relationJudgedKey(bb.Id, -1),
	}
	for _, key := range keys {
		if _, err := kv.Get(ctx, key); err != nil {
			t.Fatalf("setup missing %s: %v", key, err)
		}
	}

	core.decay(ctx, now.Add(49*time.Hour))

	core.mu.Lock()
	indexLen, pairLen, judgedLen := len(core.index), len(core.pairs), len(core.judged)
	core.mu.Unlock()
	if indexLen != 0 || pairLen != 0 || judgedLen != 0 {
		t.Fatalf("eviction left index=%d pairs=%d judged=%d", indexLen, pairLen, judgedLen)
	}
	for _, key := range keys {
		if _, err := kv.Get(ctx, key); err == nil {
			t.Fatalf("eviction left durable trigger state %s", key)
		}
	}
}

// TestRelationRestoreRetriesUnpublishedPair restores a durable pair whose link
// output never published, then retries it into the link table and stream.
func TestRelationRestoreRetriesUnpublishedPair(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	a, bb := "retry-pair-a", "retry-pair-b"
	link := &dorv1.Link{A: a, B: bb, Type: dorv1.LinkType_LINK_TYPE_THEMATIC, Dir: dorv1.Direction_DIRECTION_UNCLEAR, Confidence: 0.7, Strength: 0.7, UpdatedAt: now.Unix()}
	linkData, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	rec := relationPairRecord{A: a, B: bb, SeqA: 1, SeqB: 1, Outcome: "link", Link: linkData, Published: false}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, relationPairKey(a, bb), data); err != nil {
		t.Fatal(err)
	}

	core := relationCoreForTest(t, ctx, b, nil, now)
	core.index[a] = &relationEntry{story: relationTestStory(a, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 60, updated: now.Unix(), rev: 1, contentSeq: 1}
	core.index[bb] = &relationEntry{story: relationTestStory(bb, now.Unix(), 0.8, 40, -74), vec: goldenBase(), entities: []string{"Shared"}, lat: 40, lon: -74, firstSeen: now.Unix() - 30, updated: now.Unix(), rev: 2, contentSeq: 1}

	core.retryOutputs(ctx)

	if got, ok := live.LoadLink(ctx, kv, a, bb); !ok || got.Strength <= 0 {
		t.Fatalf("unpublished pair output was not retried: link=%+v ok=%v", got, ok)
	}
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("link.changed messages after retry = %d, want 1", got)
	}
	rec = pairRecordForTest(t, ctx, b, a, bb)
	if !rec.Published {
		t.Fatal("pair record still unpublished after the retry")
	}
}
