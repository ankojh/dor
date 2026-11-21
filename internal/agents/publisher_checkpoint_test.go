package agents

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	mathrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

func largeCheckpoint(t *testing.T, seed int64) publisherCheckpoint {
	t.Helper()
	snapshot, err := live.MarshalSnapshot(&dorv1.Snapshot{Seq: 42, Stories: []*dorv1.Story{{Id: "checkpoint-story", Title: "checkpoint state"}}})
	if err != nil {
		t.Fatal(err)
	}
	rng := mathrand.New(mathrand.NewSource(seed))
	revisions := make(map[string]uint64, 20000)
	for i := 0; i < 20000; i++ {
		random := make([]byte, 96)
		if _, err := rng.Read(random); err != nil {
			t.Fatal(err)
		}
		revisions[fmt.Sprintf("revision:%05d:%s", i, hex.EncodeToString(random))] = rng.Uint64()
	}
	return publisherCheckpoint{Snapshot: snapshot, StreamSeq: 42, Revisions: revisions}
}

func persistChunkRoot(t *testing.T, ctx context.Context, kv jetstream.KeyValue, revision uint64, wire []byte) uint64 {
	t.Helper()
	if revision == 0 {
		next, err := kv.Create(ctx, publisherCheckpointKey, wire)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	next, err := kv.Update(ctx, publisherCheckpointKey, wire, revision)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestPublisherChunkedCheckpointOverflowRoundTrip(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	core := newPublisherCore(b, testLogger())
	cp := largeCheckpoint(t, 101)
	wire, staged, err := core.stageCheckpoint(ctx, kv, cp)
	if err != nil {
		t.Fatal(err)
	}
	if len(staged.Chunks) < 2 || staged.Size <= publisherChunkSize {
		t.Fatalf("checkpoint did not overflow into chunks: chunks=%d bytes=%d", len(staged.Chunks), staged.Size)
	}
	if int64(len(wire))+1024 > b.NC().MaxPayload() {
		t.Fatalf("root manifest is too large: %d bytes", len(wire))
	}
	root := publisherCheckpoint{}
	if err := decodePublisherCheckpoint(wire, &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Snapshot) != 0 || root.Size != staged.Size || root.Hash != staged.Hash || len(root.Chunks) != len(staged.Chunks) {
		t.Fatalf("root manifest does not describe staged checkpoint: %+v", root)
	}
	if _, err := kv.Create(ctx, publisherCheckpointKey, wire); err != nil {
		t.Fatal(err)
	}
	if err := core.hydrateCheckpoint(ctx, kv, &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Revisions) != len(cp.Revisions) || root.StreamSeq != cp.StreamSeq || root.Hash != staged.Hash {
		t.Fatalf("hydrated checkpoint counts/state differ: revisions=%d stream_seq=%d", len(root.Revisions), root.StreamSeq)
	}
	for key, want := range cp.Revisions {
		if root.Revisions[key] != want {
			t.Fatalf("revision[%q]=%d, want %d", key, root.Revisions[key], want)
		}
		break
	}
}

func TestPublisherMalformedChunkedRootsFailClosedAndPreserveSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		breakManifest func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint)
	}{
		{name: "missing chunk", breakManifest: func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint) {
			if err := kv.Purge(ctx, root.Chunks[0]); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "corrupt chunk", breakManifest: func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint) {
			entry, err := kv.Get(ctx, root.Chunks[0])
			if err != nil {
				t.Fatal(err)
			}
			data := append([]byte(nil), entry.Value()...)
			data[0] ^= 0xff
			if _, err := kv.Put(ctx, root.Chunks[0], data); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "out of order manifest", breakManifest: func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint) {
			root.Chunks[0], root.Chunks[1] = root.Chunks[1], root.Chunks[0]
			data, err := encodePublisherCheckpoint(root)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := kv.Get(ctx, publisherCheckpointKey)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Update(ctx, publisherCheckpointKey, data, entry.Revision()); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong chunk length", breakManifest: func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint) {
			entry, err := kv.Get(ctx, root.Chunks[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Put(ctx, root.Chunks[0], entry.Value()[:len(entry.Value())-1]); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "size bound", breakManifest: func(t *testing.T, ctx context.Context, kv jetstream.KeyValue, root publisherCheckpoint) {
			root.Size = publisherCheckpointLimit + 1
			data, err := encodePublisherCheckpoint(root)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := kv.Get(ctx, publisherCheckpointKey)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Update(ctx, publisherCheckpointKey, data, entry.Revision()); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := durabilityBus(t)
			ctx, cancel := durabilityCtx(t)
			defer cancel()
			liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
			if err != nil {
				t.Fatal(err)
			}
			good := &dorv1.Snapshot{Seq: 777, Stories: []*dorv1.Story{{Id: "last-good", Title: "preserved"}}}
			goodBytes, err := live.MarshalSnapshot(good)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := liveKV.Create(ctx, bus.KVLiveStateSnapshotKey, goodBytes); err != nil {
				t.Fatal(err)
			}

			kv, err := b.KV(ctx, bus.KVBucketPublisher)
			if err != nil {
				t.Fatal(err)
			}
			core := newPublisherCore(b, testLogger())
			wire, _, err := core.stageCheckpoint(ctx, kv, largeCheckpoint(t, 303))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Create(ctx, publisherCheckpointKey, wire); err != nil {
				t.Fatal(err)
			}
			root := publisherCheckpoint{}
			if err := decodePublisherCheckpoint(wire, &root); err != nil {
				t.Fatal(err)
			}
			tc.breakManifest(t, ctx, kv, root)

			if err := newPublisherCore(b, testLogger()).restore(ctx); err == nil {
				t.Fatal("corrupt current checkpoint unexpectedly restored")
			}
			entry, err := liveKV.Get(ctx, bus.KVLiveStateSnapshotKey)
			if err != nil {
				t.Fatal(err)
			}
			var after dorv1.Snapshot
			if err := live.UnmarshalSnapshot(entry.Value(), &after); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(good, &after) {
				t.Fatalf("failed restore changed last-good snapshot: seq=%d stories=%v", after.Seq, after.Stories)
			}
		})
	}
}

func TestPublisherRootCASFailureLeavesUnreferencedChunks(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	core := newPublisherCore(b, testLogger())
	if err := core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer core.release(context.Background())
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	current, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}

	cp := largeCheckpoint(t, 404)
	cp.Owner, cp.LeaseUntil = core.owner, time.Now().Add(publisherLease).UnixNano()
	wire, staged, err := core.stageCheckpoint(ctx, kv, cp)
	if err != nil {
		t.Fatal(err)
	}
	competitor := core.checkpoint
	competitor.Owner = "takeover-owner"
	competitor.LeaseUntil = time.Now().Add(publisherLease).UnixNano()
	competitorWire, err := encodePublisherCheckpoint(competitor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, competitorWire, current.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, wire, current.Revision()); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("stale root CAS error=%v, want revision mismatch", err)
	}
	rootEntry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var root publisherCheckpoint
	if err := decodePublisherCheckpoint(rootEntry.Value(), &root); err != nil {
		t.Fatal(err)
	}
	if root.Owner != "takeover-owner" || len(root.Chunks) != 0 {
		t.Fatalf("failed CAS changed root: %+v", root)
	}
	for _, key := range staged.Chunks {
		if _, err := kv.Get(ctx, key); err != nil {
			t.Fatalf("staged orphan chunk %q missing after root CAS failure: %v", key, err)
		}
	}
}

func TestPublisherLeaseOnlyRootUpdatesReuseIdenticalChunks(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	core := newPublisherCore(b, testLogger())
	if err := core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer core.release(context.Background())
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}

	cp := largeCheckpoint(t, 505)
	if err := core.saveCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	first := core.checkpoint
	if len(first.Chunks) < 2 {
		t.Fatalf("checkpoint did not chunk: %d", len(first.Chunks))
	}
	revisions := make(map[string]uint64, len(first.Chunks))
	for _, key := range first.Chunks {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		revisions[key] = entry.Revision()
	}
	if err := core.saveCheckpoint(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := core.checkpoint
	if first.Hash != second.Hash || len(first.Chunks) != len(second.Chunks) {
		t.Fatal("lease-only renewal changed checkpoint content hash")
	}
	for _, key := range second.Chunks {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Revision() != revisions[key] {
			t.Fatalf("lease-only root update rewrote shared chunk %q revision %d -> %d", key, revisions[key], entry.Revision())
		}
	}
}

type blockedGetKV struct {
	jetstream.KeyValue
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockedGetKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == b.key {
		b.once.Do(func() {
			close(b.entered)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
	return b.KeyValue.Get(ctx, key)
}

func TestPublisherChunkReaderOverlapsWriterWithoutMixedGeneration(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	core := newPublisherCore(b, testLogger())
	if err := core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer core.release(context.Background())
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	old := largeCheckpoint(t, 606)
	if err := core.saveCheckpoint(ctx, old); err != nil {
		t.Fatal(err)
	}
	oldRootEntry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var oldRoot publisherCheckpoint
	if err := decodePublisherCheckpoint(oldRootEntry.Value(), &oldRoot); err != nil {
		t.Fatal(err)
	}

	readerKV := &blockedGetKV{KeyValue: kv, key: oldRoot.Chunks[0], entered: make(chan struct{}), release: make(chan struct{})}
	readResult := make(chan error, 1)
	var read publisherCheckpoint
	go func() { read = oldRoot; readResult <- core.hydrateCheckpoint(ctx, readerKV, &read) }()
	select {
	case <-readerKV.entered:
	case <-ctx.Done():
		t.Fatal("reader did not reach first chunk")
	}

	newer := largeCheckpoint(t, 707)
	wire, staged, err := core.stageCheckpoint(ctx, kv, newer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, wire, oldRootEntry.Revision()); err != nil {
		t.Fatal(err)
	}
	close(readerKV.release)
	if err := <-readResult; err != nil {
		t.Fatalf("reader could not finish retired but retained generation: %v", err)
	}
	if len(read.Revisions) != len(old.Revisions) {
		t.Fatalf("overlapping reader returned mixed generation size=%d, want %d", len(read.Revisions), len(old.Revisions))
	}
	for key := range old.Revisions {
		if _, ok := read.Revisions[key]; !ok {
			t.Fatalf("overlapping reader lost old-generation key %q", key)
		}
		break
	}
	newRootEntry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var newRoot publisherCheckpoint
	if err := decodePublisherCheckpoint(newRootEntry.Value(), &newRoot); err != nil {
		t.Fatal(err)
	}
	if err := core.hydrateCheckpoint(ctx, kv, &newRoot); err != nil {
		t.Fatal(err)
	}
	if len(newRoot.Revisions) != len(staged.Revisions) {
		t.Fatalf("writer root revisions=%d, want %d", len(newRoot.Revisions), len(staged.Revisions))
	}
}

func TestPublisherChunkGCTakeoverRevalidatesRootBeforePurge(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := durabilityCtx(t)
	defer cancel()
	core := newPublisherCore(b, testLogger())
	if err := core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer core.release(context.Background())
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.saveCheckpoint(ctx, largeCheckpoint(t, 808)); err != nil {
		t.Fatal(err)
	}
	rootEntry, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		t.Fatal(err)
	}
	var root publisherCheckpoint
	if err := decodePublisherCheckpoint(rootEntry.Value(), &root); err != nil {
		t.Fatal(err)
	}
	_, orphan, err := core.stageCheckpoint(ctx, kv, largeCheckpoint(t, 909))
	if err != nil {
		t.Fatal(err)
	}

	clockReachedCandidate := make(chan struct{})
	continueGC := make(chan struct{})
	var once sync.Once
	core.now = func() time.Time {
		once.Do(func() { close(clockReachedCandidate); <-continueGC })
		return time.Now().Add(publisherChunkGrace + time.Minute)
	}
	gcResult := make(chan error, 1)
	go func() { gcResult <- core.gcChunks(ctx, kv) }()
	select {
	case <-clockReachedCandidate:
	case <-ctx.Done():
		t.Fatal("GC did not reach an orphan candidate")
	}

	root.Owner, root.LeaseUntil = "takeover-owner", time.Now().Add(publisherLease).UnixNano()
	takeoverWire, err := encodePublisherCheckpoint(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, publisherCheckpointKey, takeoverWire, rootEntry.Revision()); err != nil {
		t.Fatal(err)
	}
	close(continueGC)
	if err := <-gcResult; err == nil {
		t.Fatal("GC did not stop after ownership root changed")
	}
	for _, key := range orphan.Chunks {
		if _, err := kv.Get(ctx, key); err != nil {
			t.Fatalf("GC purged a candidate after takeover root change: %s: %v", key, err)
		}
	}
}

func TestPublisherPrunesOnlyInactiveCommittedInputRevisions(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := publisher.Stop(stopCtx); err != nil {
			t.Errorf("stop publisher: %v", err)
		}
	}()

	publish := func(subject, id string, message proto.Message) uint64 {
		t.Helper()
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := b.Publish(ctx, subject, data, bus.WithMsgID(id))
		if err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
		return ack.Sequence
	}
	storySeq := publish(bus.SubjectStoryChangedPrefix+"0", "story-active", &dorv1.Story{Id: "active", Title: "active story"})
	publish(bus.SubjectLinkChanged, "link-upsert", &dorv1.Link{A: "a", B: "b", Strength: .8})
	linkSeq := publish(bus.SubjectLinkChanged, "link-remove", &dorv1.Link{A: "a", B: "b"})
	cleanSeq := publish(bus.SubjectArticleClean, "article-clean", &dorv1.Article{Id: "evicted", Title: "wire row"})
	alertSeq := publish(bus.SubjectMatchAlert, "alert-fired", &dorv1.AlertHit{AlertId: "alert", StoryId: "s", Level: "new", Generation: 3})

	ackFloor := func(stream, durable string, want uint64) uint64 {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			consumer, err := b.JS().Consumer(ctx, stream, durable)
			if err == nil {
				info, infoErr := consumer.Info(ctx)
				if infoErr == nil && info.AckFloor.Stream >= want {
					return info.AckFloor.Stream
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s/%s did not ack through stream sequence %d", stream, durable, want)
		return 0
	}
	storyFloor := ackFloor(bus.StreamStory, "publisher-story", linkSeq)
	enrichFloor := ackFloor(bus.StreamEnrich, "publisher-enrich", cleanSeq)
	alertFloor := ackFloor(bus.StreamAlert, "publisher-alert", alertSeq)
	if storyFloor < linkSeq || enrichFloor < cleanSeq || alertFloor < alertSeq {
		t.Fatalf("actual AckFloors story=%d enrich=%d alert=%d", storyFloor, enrichFloor, alertFloor)
	}
	waitFor(t, "publisher live-state commit", func() bool {
		publisher.core.mu.Lock()
		defer publisher.core.mu.Unlock()
		return publisher.core.stories["active"] != nil && publisher.core.links["a|b"] == nil && publisher.core.wireRows["evicted"] != nil
	})
	publisher.core.mu.Lock()
	delete(publisher.core.wireRows, "evicted") // model an entity evicted from bounded live state
	publisher.core.mu.Unlock()

	activeKey := bus.StreamStory + "/story:active"
	linkKey := bus.StreamStory + "/link:a|b"
	cleanKey := bus.StreamEnrich + "/clean:evicted"
	alertKey := bus.StreamAlert + "/" + publisherAlertKey(&dorv1.AlertHit{AlertId: "alert", StoryId: "s", Level: "new", Generation: 3})
	unackedKey := bus.StreamStory + "/story:unacked"
	stagedKey := bus.StreamStory + "/story:staged"
	publisher.core.flushMu.Lock()
	publisher.core.mu.Lock()
	publisher.core.checkpoint.Revisions[activeKey] = storySeq
	publisher.core.checkpoint.Revisions[linkKey] = linkSeq
	publisher.core.checkpoint.Revisions[cleanKey] = cleanSeq
	publisher.core.checkpoint.Revisions[alertKey] = alertSeq
	publisher.core.checkpoint.Revisions[unackedKey] = storyFloor + 10
	publisher.core.checkpoint.Revisions[stagedKey] = storyFloor
	publisher.core.revisions = maps.Clone(publisher.core.checkpoint.Revisions)
	publisher.core.committedRevisions = maps.Clone(publisher.core.checkpoint.Revisions)
	publisher.core.revisions[stagedKey] = storyFloor + 1 // newer uncommitted admission protects its older checkpoint value
	publisher.core.mu.Unlock()
	publisher.core.flushMu.Unlock()

	if err := publisher.core.pruneInputRevisions(ctx); err != nil {
		t.Fatal(err)
	}
	publisher.core.mu.Lock()
	defer publisher.core.mu.Unlock()
	for _, key := range []string{linkKey, cleanKey, alertKey} {
		if _, exists := publisher.core.checkpoint.Revisions[key]; exists {
			t.Errorf("inactive acknowledged revision %q was not pruned", key)
		}
		if _, exists := publisher.core.committedRevisions[key]; exists {
			t.Errorf("inactive acknowledged revision %q remains in memory", key)
		}
	}
	if publisher.core.checkpoint.Revisions[activeKey] != storySeq {
		t.Errorf("active story revision was pruned: %d", publisher.core.checkpoint.Revisions[activeKey])
	}
	if publisher.core.checkpoint.Revisions[unackedKey] != storyFloor+10 {
		t.Errorf("unacked revision was pruned: %d", publisher.core.checkpoint.Revisions[unackedKey])
	}
	if publisher.core.checkpoint.Revisions[stagedKey] != storyFloor || publisher.core.revisions[stagedKey] != storyFloor+1 {
		t.Errorf("staged-lower-sequence revision was changed: checkpoint=%d in-memory=%d", publisher.core.checkpoint.Revisions[stagedKey], publisher.core.revisions[stagedKey])
	}
}
