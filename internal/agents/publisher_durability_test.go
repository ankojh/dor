package agents

import (
	"context"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func durabilityBus(t *testing.T) *bus.Bus {
	t.Helper()
	b := testBus(t, filepath.Join(t.TempDir(), "nats"))
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func durabilityCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func TestPublisherRestoreCheckpointRestoresFullLiveState(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()

	first := newPublisherCore(b, testLogger())
	if err := first.restore(ctx); err != nil {
		t.Fatal(err)
	}
	story := &dorv1.Story{Id: "s-1", Title: "full story", Category: dorv1.Category_CATEGORY_POLITICS, UpdatedAt: 123}
	link := &dorv1.Link{A: "s-1", B: "s-2", Strength: .7, Confidence: .8}
	row := &dorv1.WireRow{ArticleId: "a-1", StoryId: "s-1", Title: "wire row", Lat: 37.774929, Lon: -122.419416, Located: true}
	first.onStory(story)
	first.onLink(link)
	first.onArticle(&dorv1.Article{Id: row.ArticleId, StoryId: row.StoryId, Title: row.Title, Hints: &dorv1.RawHints{Lat: row.Lat, Lon: row.Lon, Located: true}}, false)
	if err := first.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.release(ctx); err != nil {
		t.Fatal(err)
	}

	second := newPublisherCore(b, testLogger())
	if err := second.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer second.release(context.Background())
	second.mu.Lock()
	defer second.mu.Unlock()
	if second.seq != 3 {
		t.Fatalf("restored seq=%d, want 3", second.seq)
	}
	if got := second.stories[story.Id]; got == nil || !proto.Equal(got, story) {
		t.Fatalf("restored story=%v, want %v", got, story)
	}
	if got := second.links[link.A+"|"+link.B]; got == nil || !proto.Equal(got, link) {
		t.Fatalf("restored link=%v, want %v", got, link)
	}
	if got := second.wireRows[row.ArticleId]; got == nil || got.Title != row.Title || got.StoryId != story.Id {
		t.Fatalf("restored wire row=%v", got)
	}
}

func TestPublisherSnapshotOverwriteGuardAndPublicProjection(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())

	c.onStory(&dorv1.Story{Id: "retained", Locations: []*dorv1.Location{{Lat: 37.774929, Lon: -122.419416}}})
	c.onLink(&dorv1.Link{A: "retained", B: "weak", Strength: .29})
	c.onLink(&dorv1.Link{A: "retained", B: "strong", Strength: .3})
	c.onArticle(&dorv1.Article{Id: "wire", Hints: &dorv1.RawHints{Lat: 37.774929, Lon: -122.419416, Located: true}}, false)
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	var snap dorv1.Snapshot
	if err := proto.Unmarshal(entry.Value(), &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Links) != 1 || snap.Links[0].B != "strong" {
		t.Fatalf("snapshot links=%v, want only strength >= .3", snap.Links)
	}
	wantLat := math.Round(37.774929*1e5) / 1e5
	wantLon := math.Round(-122.419416*1e5) / 1e5
	if got := float64(snap.Stories[0].Locations[0].Lat); math.Abs(got-wantLat) > 1e-5 {
		t.Fatalf("story latitude %.8f, want quantized %.8f", got, wantLat)
	}
	if got := snap.RecentWire[0].Lat; math.Abs(got-wantLat) > 1e-9 {
		t.Fatalf("wire latitude %.8f, want %.8f", got, wantLat)
	}
	if got := snap.RecentWire[0].Lon; math.Abs(got-wantLon) > 1e-9 {
		t.Fatalf("wire longitude %.8f, want %.8f", got, wantLon)
	}

	c.mu.Lock()
	c.stories = make(map[string]*dorv1.Story)
	c.mu.Unlock()
	if err := c.writeSnapshot(ctx); err == nil {
		t.Fatal("expected refusal to replace nonempty snapshot with empty state")
	}
	entry, err = kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	var after dorv1.Snapshot
	if err := proto.Unmarshal(entry.Value(), &after); err != nil {
		t.Fatal(err)
	}
	if len(after.Stories) != 1 || after.Stories[0].Id != "retained" {
		t.Fatalf("guard lost existing story: %v", after.Stories)
	}
}

func TestPublisherFailedPublishRetriesImmutableBatchAndDelaysAck(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())

	ack := make(chan error, 1)
	go func() { ack <- c.accept(ctx, func() { c.onStory(&dorv1.Story{Id: "retry", Title: "first"}) }) }()
	waitFor(t, "pending publisher input", func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) > 0 })
	var payloads [][]byte
	var ids []string
	c.publish = func(_ context.Context, data []byte, id string) error {
		payloads = append(payloads, append([]byte(nil), data...))
		ids = append(ids, id)
		if len(payloads) == 1 {
			return errors.New("injected publish failure")
		}
		return nil
	}
	if err := c.flush(ctx); err == nil {
		t.Fatal("expected injected publish failure")
	}
	select {
	case err := <-ack:
		t.Fatalf("input acknowledged before commit: %v", err)
	default:
	}
	c.onStory(&dorv1.Story{Id: "retry", Title: "newer update"})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("input acknowledgement did not follow snapshot commit")
	}
	if len(payloads) != 2 || !reflect.DeepEqual(payloads[0], payloads[1]) {
		t.Fatalf("retry payload changed: %x / %x", payloads[0], payloads[1])
	}
	if ids[0] != ids[1] || ids[0] != "delta:1" {
		t.Fatalf("retry msg IDs=%v, want same delta:1", ids)
	}
	var batch dorv1.DeltaBatch
	if err := proto.Unmarshal(payloads[0], &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Deltas) != 1 || batch.Deltas[0].Seq != 1 || batch.Deltas[0].GetStoryUpsert().Title != "first" {
		t.Fatalf("pending batch mutated after newer input: %v", &batch)
	}
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if c.seq != 2 {
		t.Fatalf("seq=%d after newer update, want 2", c.seq)
	}
}

func TestPublisherRestartsSavedPendingBatchAndFencesStaleOwner(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	old := newPublisherCore(b, testLogger())
	if err := old.restore(ctx); err != nil {
		t.Fatal(err)
	}
	old.onStory(&dorv1.Story{Id: "pending", Title: "durable"})
	old.publish = func(context.Context, []byte, string) error { return errors.New("offline") }
	if err := old.flush(ctx); err == nil {
		t.Fatal("expected publish failure")
	}
	if err := old.release(ctx); err != nil {
		t.Fatal(err)
	}

	fresh := newPublisherCore(b, testLogger())
	if err := fresh.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer fresh.release(context.Background())
	if fresh.inflight == nil || !fresh.inflight.reserved {
		t.Fatal("pending batch not restored")
	}
	var saved dorv1.DeltaBatch
	if err := proto.Unmarshal(fresh.inflight.data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Deltas) != 1 || saved.Deltas[0].Seq != 1 || saved.Deltas[0].GetStoryUpsert().Title != "durable" {
		t.Fatalf("restored batch=%v", &saved)
	}
	fresh.publish = func(_ context.Context, data []byte, id string) error {
		if !reflect.DeepEqual(data, fresh.inflight.data) || id != "delta:1" {
			t.Fatalf("recovery changed batch/id: id=%q", id)
		}
		return nil
	}
	if err := fresh.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if fresh.seq != 1 {
		t.Fatalf("recovered seq=%d, want 1", fresh.seq)
	}

	// The former owner cannot commit after a successful lease takeover.
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var cp publisherCheckpoint
	if err := decodePublisherCheckpoint(entry.Value(), &cp); err != nil {
		t.Fatal(err)
	}
	cp.LeaseUntil = time.Now().Add(-time.Second).UnixNano()
	data, err := encodePublisherCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, data, entry.Revision()); err != nil {
		t.Fatal(err)
	}
	takeover := newPublisherCore(b, testLogger())
	if err := takeover.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer takeover.release(context.Background())
	if err := fresh.flush(ctx); err == nil {
		t.Fatal("stale owner flush succeeded after takeover")
	}
	if !fresh.fenced.Load() {
		t.Fatal("fenced owner health state was not marked")
	}
}

func TestPublisherCheckpointBucketDoesNotExpire(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	bucket, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	status, err := bucket.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.TTL() != 0 {
		t.Fatalf("publisher checkpoint bucket TTL=%s, want no expiry", status.TTL())
	}
}

func TestPublisherPerEntityStreamRevisionsRejectOutOfOrderTombstone(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	canceled, stop := context.WithCancel(context.Background())
	stop()
	accept := func(msg *Message, key string, apply func()) {
		t.Helper()
		if err := c.acceptInput(canceled, msg, key, apply); err == nil {
			t.Fatal("canceled input should remain unacked until a commit")
		}
	}

	// Both story versions have the same second-level timestamp. Stream sequence
	// decides order for this story, while the independent link subject can advance.
	accept(&Message{Subject: bus.SubjectStoryChangedPrefix + "s", StreamSequence: 12}, "story:s", func() {
		c.onStory(&dorv1.Story{Id: "s", Title: "later", UpdatedAt: 100})
	})
	accept(&Message{Subject: bus.SubjectLinkChanged, StreamSequence: 3}, "link:a|b", func() {
		c.onLink(&dorv1.Link{A: "a", B: "b", Strength: .9, Confidence: .9})
	})
	accept(&Message{Subject: bus.SubjectStoryChangedPrefix + "s", StreamSequence: 11}, "story:s", func() {
		c.onStory(&dorv1.Story{Id: "s", Title: "older", UpdatedAt: 100})
	})
	accept(&Message{Subject: bus.SubjectLinkChanged, StreamSequence: 4}, "link:a|b", func() {
		c.onLink(&dorv1.Link{A: "a", B: "b"}) // removal tombstone
	})
	accept(&Message{Subject: bus.SubjectLinkChanged, StreamSequence: 2}, "link:a|b", func() {
		c.onLink(&dorv1.Link{A: "a", B: "b", Strength: .9, Confidence: .9})
	})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.stories["s"]; got == nil || got.Title != "later" {
		t.Fatalf("same-second story order ignored, got %v", got)
	}
	if _, exists := c.links["a|b"]; exists {
		t.Fatal("stale link upsert resurrected a tombstoned link")
	}
	if c.committedRevisions[bus.StreamForSubject(bus.SubjectStoryChangedPrefix+"s")+"/story:s"] != 12 {
		t.Fatalf("story revision not committed: %v", c.committedRevisions)
	}
}

func TestPublisherLateCleanKeepsPlacedFieldsAndScoreBeforeWire(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	canceled, stop := context.WithCancel(context.Background())
	stop()
	accept := func(msg *Message, key string, apply func()) {
		t.Helper()
		if err := c.acceptInput(canceled, msg, key, apply); err == nil {
			t.Fatal("canceled input should remain unacked until a commit")
		}
	}
	accept(&Message{Subject: bus.SubjectMatchScore, StreamSequence: 1}, "score:article", func() {
		c.onMatchScore(&dorv1.MatchScore{ArticleId: "article", Scores: map[string]float32{"prompt": .8}, Approx: []string{"prompt"}})
	})
	accept(&Message{Subject: bus.SubjectArticlePlaced, StreamSequence: 2}, "placed:article", func() {
		c.onArticle(&dorv1.Article{Id: "article", StoryId: "placed-story", Title: "placed title", Join: dorv1.JoinKind_JOIN_KIND_JOINED, Hints: &dorv1.RawHints{Located: true, Lat: 37.774929, Lon: -122.419416, Place: "San Francisco", Country: "US"}}, true)
	})
	accept(&Message{Subject: bus.SubjectArticleClean, StreamSequence: 3}, "clean:article", func() {
		c.onArticle(&dorv1.Article{Id: "article", Title: "clean title", Hints: &dorv1.RawHints{Place: "raw dateline"}}, false)
	})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	row := c.wireRows["article"]
	if row == nil {
		t.Fatal("score-before-wire did not produce a wire row")
	}
	if row.Title != "clean title" {
		t.Fatalf("clean title did not update row: %q", row.Title)
	}
	if row.StoryId != "placed-story" || row.Join != dorv1.JoinKind_JOIN_KIND_JOINED || row.Place != "San Francisco" || row.Country != "US" || !row.Located {
		t.Fatalf("late clean overwrote placement: %+v", row)
	}
	if len(row.ApproxPrompts) != 1 || row.ApproxPrompts[0] != "prompt" || row.PromptScores["prompt"] != .8 {
		t.Fatalf("score-before-wire was lost: %+v", row)
	}
}

func TestPublisherCoreAdmissionSustainsFiftyArticlesPerSecondAcrossFanout(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())

	const articles = 75
	var inputSeq atomic.Uint64
	errs := make(chan error, articles*4)
	stopFlush := make(chan struct{})
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopFlush:
				return
			case <-ticker.C:
				if err := c.flush(ctx); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	start := time.Now()
	var workers sync.WaitGroup
	for i := 0; i < articles; i++ {
		id := fmt.Sprintf("article-%03d", i)
		workers.Add(4)
		go func(id string) {
			defer workers.Done()
			seq := inputSeq.Add(1)
			errs <- c.acceptInput(ctx, &Message{Subject: bus.SubjectArticleClean, StreamSequence: seq}, "clean:"+id, func() {
				c.onArticle(&dorv1.Article{Id: id, Title: "clean " + id}, false)
			})
		}(id)
		go func(id string) {
			defer workers.Done()
			seq := inputSeq.Add(1)
			errs <- c.acceptInput(ctx, &Message{Subject: bus.SubjectArticlePlaced, StreamSequence: seq}, "placed:"+id, func() {
				c.onArticle(&dorv1.Article{Id: id, StoryId: "story-" + id, Title: "placed " + id}, true)
			})
		}(id)
		go func(id string) {
			defer workers.Done()
			seq := inputSeq.Add(1)
			errs <- c.acceptInput(ctx, &Message{Subject: bus.SubjectStoryChangedPrefix + "story-" + id, StreamSequence: seq}, "story:story-"+id, func() {
				c.onStory(&dorv1.Story{Id: "story-" + id, Title: "story " + id, UpdatedAt: 1})
			})
		}(id)
		go func(id string) {
			defer workers.Done()
			seq := inputSeq.Add(1)
			errs <- c.acceptInput(ctx, &Message{Subject: bus.SubjectMatchScore, StreamSequence: seq}, "score:"+id, func() {
				c.onMatchScore(&dorv1.MatchScore{ArticleId: id, Scores: map[string]float32{"p": .5}})
			})
		}(id)
	}
	workers.Wait()
	elapsed := time.Since(start)
	close(stopFlush)
	<-flushDone
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if rate := float64(articles) / elapsed.Seconds(); rate < 50 {
		t.Fatalf("publisher fanout rate %.1f articles/s, want >=50 (elapsed %s)", rate, elapsed)
	}
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending, waiters, stories, rows := len(c.pending), len(c.waiters), len(c.stories), len(c.wireRows)
	c.mu.Unlock()
	if pending != 0 || waiters != 0 {
		t.Fatalf("publisher queue lag remains after drain: pending=%d waiters=%d", pending, waiters)
	}
	if stories != articles || rows != articles {
		t.Fatalf("fanout state stories=%d wire rows=%d, want %d each", stories, rows, articles)
	}
}

func TestPublisherRestoresCompressedLargeLegacySnapshot(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	snap := &dorv1.Snapshot{Seq: 22, Stories: make([]*dorv1.Story, 4645), Links: make([]*dorv1.Link, 13302)}
	for i := range snap.Stories {
		snap.Stories[i] = &dorv1.Story{Id: fmt.Sprintf("story-%d", i), Title: "Production globe story with a representative title and descriptive body"}
	}
	for i := range snap.Links {
		snap.Links[i] = &dorv1.Link{A: fmt.Sprintf("story-%d", i%4645), B: fmt.Sprintf("target-%d", i), Strength: .7}
	}
	encoded, err := live.MarshalSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 1024*1024 {
		t.Fatalf("compressed snapshot still exceeds KV payload limit: %d bytes", len(encoded))
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, bus.KVLiveStateSnapshotKey, encoded); err != nil {
		t.Fatal(err)
	}

	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	c.mu.Lock()
	stories, links, seq := len(c.stories), len(c.links), c.seq
	c.mu.Unlock()
	if stories != len(snap.Stories) || links != len(snap.Links) || seq != snap.Seq {
		t.Fatalf("large legacy recovery stories=%d links=%d seq=%d", stories, links, seq)
	}
}

func TestPublisherConsumersSustainFiftyArticlesPerSecondAndDrain(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	agent, err := NewPublisher(b, NewMetrics(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	publisher := agent.(*publisherAgent)
	if err := publisher.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := publisher.Stop(stopCtx); err != nil {
			t.Errorf("stop publisher: %v", err)
		}
	}()

	enrich, err := b.JS().Consumer(ctx, bus.StreamEnrich, "publisher-enrich")
	if err != nil {
		t.Fatal(err)
	}
	storyConsumer, err := b.JS().Consumer(ctx, bus.StreamStory, "publisher-story")
	if err != nil {
		t.Fatal(err)
	}
	readLag := func() (int, error) {
		ei, err := enrich.Info(ctx)
		if err != nil {
			return 0, err
		}
		si, err := storyConsumer.Info(ctx)
		if err != nil {
			return 0, err
		}
		return int(ei.NumPending) + ei.NumAckPending + int(si.NumPending) + si.NumAckPending, nil
	}
	publish := func(subject, id string, message proto.Message) {
		t.Helper()
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Publish(ctx, subject, data, bus.WithMsgID(id)); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}

	const articleCount = 150 // three seconds at 50 articles/s
	var maxLag int
	start := time.Now()
	next := time.NewTicker(20 * time.Millisecond)
	defer next.Stop()
	for i := 0; i < articleCount; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-next.C:
			}
		}
		id := fmt.Sprintf("rate-%03d", i)
		storyID := "story-" + id
		publish(bus.SubjectArticleClean, "clean:"+id, &dorv1.Article{Id: id, Title: "clean " + id})
		publish(bus.SubjectArticlePlaced, "placed:"+id, &dorv1.Article{Id: id, StoryId: storyID, Title: "placed " + id, Join: dorv1.JoinKind_JOIN_KIND_JOINED})
		publish(bus.SubjectMatchScore, "score:"+id, &dorv1.MatchScore{ArticleId: id, Scores: map[string]float32{"p": .75}})
		publish(bus.SubjectStoryChangedPrefix+"0", "story:"+id, &dorv1.Story{Id: storyID, Title: "story " + id, UpdatedAt: 1})
		publish(bus.SubjectLinkChanged, "link:"+id, &dorv1.Link{A: storyID, B: fmt.Sprintf("related-%03d", i), Strength: .65, Confidence: .8})
		if i%5 == 0 {
			if lag, err := readLag(); err == nil && lag > maxLag {
				maxLag = lag
			}
		}
	}
	publishedDuration := time.Since(start)
	publishedRate := float64(articleCount) / publishedDuration.Seconds()
	deadline := time.Now().Add(10 * time.Second)
	lag := -1
	for time.Now().Before(deadline) {
		var err error
		lag, err = readLag()
		if err == nil && lag == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if lag != 0 {
		t.Fatalf("consumer lag did not drain; lag=%d", lag)
	}
	if maxLag < lag {
		maxLag = lag
	}
	if maxLag > 256 {
		t.Fatalf("consumer queue lag grew beyond bound: max=%d (publisher rate %.1f articles/s)", maxLag, publishedRate)
	}

	publisher.core.mu.Lock()
	stories, links, rows := len(publisher.core.stories), len(publisher.core.links), len(publisher.core.wireRows)
	publisher.core.mu.Unlock()
	if stories != articleCount || links != articleCount || rows != articleCount {
		t.Fatalf("drained publisher state stories=%d links=%d wire=%d, want %d each", stories, links, rows, articleCount)
	}
	t.Logf("publisher input rate %.1f articles/s over %s; max combined consumer lag=%d; final lag=%d", publishedRate, publishedDuration, maxLag, lag)
}

func TestPublisherOversizedSnapshotPreservesLastGoodValue(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	c.onStory(&dorv1.Story{Id: "keep", Title: "known good"})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}

	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rng := mathrand.New(mathrand.NewSource(81))
	title := make([]byte, 1800*1024)
	for i := range title {
		title[i] = alphabet[rng.Intn(len(alphabet))]
	}
	c.mu.Lock()
	c.stories["keep"] = &dorv1.Story{Id: "keep", Title: string(title)}
	c.mu.Unlock()
	if err := c.writeSnapshot(ctx); err == nil {
		t.Fatal("expected oversized snapshot to be rejected")
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	var got dorv1.Snapshot
	if err := live.UnmarshalSnapshot(entry.Value(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Seq != 1 || len(got.Stories) != 1 || got.Stories[0].Title != "known good" {
		t.Fatalf("oversized write replaced last good snapshot: seq=%d stories=%d title=%q", got.Seq, len(got.Stories), got.Stories[0].Title)
	}
}

func TestPublisherExpectedLastSequenceFencesDelayedOldOwner(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	old := newPublisherCore(b, testLogger())
	if err := old.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer old.release(context.Background())
	old.onStory(&dorv1.Story{Id: "old", Title: "delayed stale output"})
	entered := make(chan struct{})
	releasePublish := make(chan struct{})
	old.publish = func(context.Context, []byte, string) error {
		close(entered)
		<-releasePublish
		return nil
	}
	oldResult := make(chan error, 1)
	go func() { oldResult <- old.flush(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("old owner did not reach delayed publish seam")
	}

	// Simulate lease expiry/takeover while the old owner is paused after its
	// checkpoint reservation but before its NATS publish.
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var cp publisherCheckpoint
	if err := decodePublisherCheckpoint(entry.Value(), &cp); err != nil {
		t.Fatal(err)
	}
	cp.Owner, cp.LeaseUntil, cp.Pending = "", 0, nil
	cp.PendingRevisions, cp.PendingScores = nil, nil
	data, err := encodePublisherCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, data, entry.Revision()); err != nil {
		t.Fatal(err)
	}

	current := newPublisherCore(b, testLogger())
	if err := current.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer current.release(context.Background())
	current.onStory(&dorv1.Story{Id: "new", Title: "current owner output"})
	if err := current.flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(releasePublish)
	select {
	case err := <-oldResult:
		if err == nil {
			t.Fatal("delayed stale owner unexpectedly published after a newer owner")
		}
	case <-ctx.Done():
		t.Fatal("delayed stale owner did not return")
	}

	stream, err := b.StreamNamed(ctx, bus.StreamDelta)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.LastSeq != 1 {
		t.Fatalf("DELTA stream last sequence=%d, want only current-owner publication at 1", info.State.LastSeq)
	}
	msg, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var batch dorv1.DeltaBatch
	if err := proto.Unmarshal(msg.Data, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Deltas) != 1 || batch.Deltas[0].GetStoryUpsert().Id != "new" {
		t.Fatalf("stale output occupies sequence 1: %v", &batch)
	}
}

func TestPublisherAlertCoalescingIncludesGeneration(t *testing.T) {
	c := newPublisherCore(nil, testLogger())
	c.onAlert(&dorv1.AlertHit{AlertId: "a", StoryId: "s", Level: "escalated", Generation: 1})
	c.onAlert(&dorv1.AlertHit{AlertId: "a", StoryId: "s", Level: "escalated", Generation: 2})
	if len(c.pending) != 2 {
		t.Fatalf("different alert generations coalesced together: pending keys=%v", c.order)
	}
	if c.pending[publisherAlertKey(&dorv1.AlertHit{AlertId: "a", StoryId: "s", Level: "escalated", Generation: 1})] == nil ||
		c.pending[publisherAlertKey(&dorv1.AlertHit{AlertId: "a", StoryId: "s", Level: "escalated", Generation: 2})] == nil {
		t.Fatalf("generation-qualified alert keys missing: %v", c.order)
	}
}
