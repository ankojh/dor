package publication

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

func publicationTestStore(t *testing.T, owner string, lease time.Duration) (*Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: filepath.Join(t.TempDir(), "nats")})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatalf("open publisher bucket: %v", err)
	}
	return New(kv, owner, lease, 1024*1024), ctx
}

func publicationRecord(t *testing.T, ordinal, previousSeq, previousRaw, seq uint64, body *dorv1.Delta) *Record {
	t.Helper()
	data, err := proto.Marshal(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{body}})
	if err != nil {
		t.Fatal(err)
	}
	return &Record{Ordinal: ordinal, PreviousSeq: previousSeq, PreviousRaw: previousRaw, Data: data}
}

func TestStateApplyReplaysMetadataOnlyPrunesAndOrdersExactly(t *testing.T) {
	state := NewState(nil)
	state.Seq, state.Raw, state.Ordinal = 8, 12, 4
	state.Rows["wire-a"] = &dorv1.WireRow{ArticleId: "wire-a"}
	state.Rows["wire-b"] = &dorv1.WireRow{ArticleId: "wire-b"}
	state.WireOrder = []string{"wire-a", "wire-b"}
	state.Revisions["STORY/story:old"] = 3
	state.Revisions["STORY/link:gone"] = 6
	state.Scores["wire-a"] = &dorv1.MatchScore{ArticleId: "wire-a", Scores: map[string]float32{"prompt": .2}}
	state.ScoreOrder = []string{"wire-a"}

	row := &dorv1.WireRow{ArticleId: "wire-c", StoryId: "story-c", Title: "new wire"}
	batch, err := proto.Marshal(&dorv1.DeltaBatch{Deltas: []*dorv1.Delta{
		{Seq: 9, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "story-c", Title: "committed"}}},
		{Seq: 10, Body: &dorv1.Delta_LinkUpsert{LinkUpsert: &dorv1.Link{A: "story-c", B: "story-d", Strength: .7}}},
		{Seq: 11, Body: &dorv1.Delta_Wire{Wire: row}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	record := &Record{
		Ordinal: 5, PreviousSeq: 8, PreviousRaw: 12, Data: batch,
		Revisions:       map[string]uint64{"STORY/story:story-c": 20, "ENRICH/placed:wire-c": 31},
		RevisionDeletes: []string{"STORY/link:gone"},
		Scores:          map[string]*dorv1.MatchScore{"wire-c": {ArticleId: "wire-c", Scores: map[string]float32{"prompt": .9}}},
		ScoreDeletes:    []string{"wire-a"},
		WireOrder:       OrderChange{Remove: []string{"wire-a"}, Append: []string{"wire-c"}},
		ScoreOrder:      OrderChange{Remove: []string{"wire-a"}, Append: []string{"wire-c"}},
	}
	if err := state.Apply(record, 13); err != nil {
		t.Fatalf("apply data and metadata record: %v", err)
	}
	if state.Seq != 11 || state.Raw != 13 || state.Ordinal != 5 {
		t.Fatalf("watermarks=(seq=%d raw=%d ordinal=%d)", state.Seq, state.Raw, state.Ordinal)
	}
	if got := state.Stories["story-c"]; got == nil || got.Title != "committed" {
		t.Fatalf("story replay result=%v", got)
	}
	if got := state.Links["story-c|story-d"]; got == nil || got.Strength != .7 {
		t.Fatalf("link replay result=%v", got)
	}
	if _, ok := state.Rows["wire-a"]; ok || state.Rows["wire-b"] == nil || state.Rows["wire-c"] == nil {
		t.Fatalf("wire rows after ordered eviction=%v", state.Rows)
	}
	if !reflect.DeepEqual(state.WireOrder, []string{"wire-b", "wire-c"}) || !reflect.DeepEqual(state.ScoreOrder, []string{"wire-c"}) {
		t.Fatalf("persisted orders wire=%v score=%v", state.WireOrder, state.ScoreOrder)
	}
	if _, ok := state.Revisions["STORY/link:gone"]; ok || state.Revisions["STORY/story:story-c"] != 20 || state.Revisions["ENRICH/placed:wire-c"] != 31 {
		t.Fatalf("revision updates/deletion=%v", state.Revisions)
	}
	if _, ok := state.Scores["wire-a"]; ok || state.Scores["wire-c"].Scores["prompt"] != .9 {
		t.Fatalf("score upsert/deletion=%v", state.Scores)
	}

	// A metadata-only commit advances the ordinal and revisions without moving
	// the public sequence or raw DELTA sequence.
	metadata := &Record{Ordinal: 6, PreviousSeq: 11, PreviousRaw: 13, RevisionDeletes: []string{"ENRICH/clean:wire-c"}, Scores: map[string]*dorv1.MatchScore{"wire-c": {ArticleId: "wire-c", Scores: map[string]float32{"prompt": .95}}}}
	if err := state.Apply(metadata, 13); err != nil {
		t.Fatalf("apply metadata-only record: %v", err)
	}
	if state.Seq != 11 || state.Raw != 13 || state.Ordinal != 6 || state.Scores["wire-c"].Scores["prompt"] != .95 {
		t.Fatalf("metadata-only result watermarks=(%d,%d,%d), score=%v", state.Seq, state.Raw, state.Ordinal, state.Scores["wire-c"])
	}
}

func TestDiffOrderPatchReconstructsDeterministicEvictionOrder(t *testing.T) {
	for _, tc := range []struct {
		before, after  []string
		remove, append []string
	}{
		{before: []string{"a", "b", "c"}, after: []string{"b", "c", "d"}, remove: []string{"a"}, append: []string{"d"}},
		{before: []string{"a", "b"}, after: []string{"b", "a"}, remove: []string{"a"}, append: []string{"a"}},
		{before: []string{"a", "b"}, after: []string{"a", "c", "b"}, remove: []string{"b"}, append: []string{"c", "b"}},
	} {
		patch := DiffOrder(tc.before, tc.after)
		if !reflect.DeepEqual(patch.Remove, tc.remove) || !reflect.DeepEqual(patch.Append, tc.append) {
			t.Fatalf("DiffOrder(%v,%v)=%+v, want remove=%v append=%v", tc.before, tc.after, patch, tc.remove, tc.append)
		}
		if got := patch.Apply(tc.before); !reflect.DeepEqual(got, tc.after) {
			t.Fatalf("patch %v applied to %v produced %v, want %v", patch, tc.before, got, tc.after)
		}
	}
}

func TestDecodeRootRejectsMalformedAndUnboundedDescriptors(t *testing.T) {
	validBlob := Blob{ID: strings.Repeat("A", 22), Hash: strings.Repeat("0", 64), Size: 1, Count: 1}
	valid := Root{Version: 2, Base: Entry{Blob: validBlob}}
	badGap := valid
	badGap.Head = 2
	badGap.Entries = []Entry{{Ordinal: 2, Blob: validBlob}}
	badBase := valid
	badBase.Base.Blob.Count = 2
	for name, data := range map[string][]byte{
		"version":    []byte(`{"version":1}`),
		"gap":        mustJSON(t, badGap),
		"base-count": mustJSON(t, badBase),
		"root-limit": []byte(strings.Repeat("x", 512*1024+1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRoot(data); err == nil {
				t.Fatal("malformed root was accepted")
			}
		})
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestStorePendingSurvivesTakeoverUntilCommitted(t *testing.T) {
	storeA, ctx := publicationTestStore(t, "owner-a", time.Second)
	initial := NewState(nil)
	if err := storeA.Initialize(ctx, initial, 0, nil); err != nil {
		t.Fatal(err)
	}
	record := publicationRecord(t, 1, 0, 0, 1, &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "story-1", Title: "exact payload"}}})
	if err := storeA.Reserve(ctx, record); err != nil {
		t.Fatalf("reserve pending record: %v", err)
	}
	committed, pending, err := storeA.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Ordinal != 0 || committed.Seq != 0 || committed.Stories["story-1"] != nil {
		t.Fatalf("pending record leaked into committed state: %+v", committed)
	}
	if pending == nil || pending.Ordinal != record.Ordinal || !reflect.DeepEqual(pending.Data, record.Data) {
		t.Fatalf("loaded pending record=%+v, want exact ordinal/data", pending)
	}
	if err := storeA.Commit(ctx, 2, 2, 2); err == nil {
		t.Fatal("commit with the wrong ordinal unexpectedly succeeded")
	}
	if err := storeA.Commit(ctx, 1, 2, 1); err == nil {
		t.Fatal("commit with a sequence that disagrees with pending delta unexpectedly succeeded")
	}
	if err := storeA.Commit(ctx, 1, 1, 2); err == nil {
		t.Fatal("commit with a raw stream sequence that disagrees with pending delta unexpectedly succeeded")
	}
	_, stillPending, err := storeA.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stillPending == nil || stillPending.Ordinal != record.Ordinal || !reflect.DeepEqual(stillPending.Data, record.Data) {
		t.Fatalf("failed commit changed pending record: %+v", stillPending)
	}

	storeB := New(storeA.KV, "owner-b", time.Second, storeA.Limit)
	storeB.Now = func() time.Time { return storeA.Now().Add(2 * time.Second) }
	if err := storeB.Claim(ctx); err != nil {
		t.Fatalf("successor claim after lease expiry: %v", err)
	}
	if err := storeA.Commit(ctx, 1, 1, 1); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner's commit error=%v, want ErrFenced", err)
	}
	committed, pending, err = storeB.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Ordinal != 0 || pending == nil || !reflect.DeepEqual(pending.Data, record.Data) {
		t.Fatalf("takeover changed committed/pending state: committed=%+v pending=%+v", committed, pending)
	}
	if err := storeB.Commit(ctx, 1, 1, 1); err != nil {
		t.Fatalf("successor commit exact pending record: %v", err)
	}
	committed, pending, err = storeB.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != nil || committed.Ordinal != 1 || committed.Seq != 1 || committed.Raw != 1 || committed.Stories["story-1"].Title != "exact payload" {
		t.Fatalf("committed successor state=%+v pending=%+v", committed, pending)
	}
	if err := storeA.Reserve(ctx, &Record{Ordinal: 2, PreviousSeq: 1, PreviousRaw: 1}); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner's successor reserve error=%v, want ErrFenced", err)
	}
}

func TestReserveIsIdempotentOnlyForExactPendingRecord(t *testing.T) {
	store, ctx := publicationTestStore(t, "owner", time.Minute)
	if err := store.Initialize(ctx, NewState(nil), 0, nil); err != nil {
		t.Fatal(err)
	}
	record := publicationRecord(t, 1, 0, 0, 1, &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "s1", Title: "same"}}})
	if err := store.Reserve(ctx, record); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reserve(ctx, record); err != nil {
		t.Fatalf("reserving exact pending record again: %v", err)
	}
	after, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Pending == nil || after.Pending == nil || before.Pending.Blob.ID != after.Pending.Blob.ID || before.Pending.Blob.Hash != after.Pending.Blob.Hash {
		t.Fatalf("idempotent reserve changed pending descriptor: before=%+v after=%+v", before.Pending, after.Pending)
	}

	conflict := publicationRecord(t, 1, 0, 0, 1, &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "s1", Title: "different"}}})
	if err := store.Reserve(ctx, conflict); err == nil {
		t.Fatal("different record replaced existing pending payload")
	}
	afterConflict, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterConflict.Pending == nil || afterConflict.Pending.Blob.ID != before.Pending.Blob.ID {
		t.Fatalf("conflicting reserve changed pending descriptor: %+v", afterConflict.Pending)
	}
}

func TestStoreRecoversMetadataOnlyPruneAndOrderPatch(t *testing.T) {
	store, ctx := publicationTestStore(t, "owner", time.Minute)
	base := NewState(nil)
	base.Rows["wire-a"] = &dorv1.WireRow{ArticleId: "wire-a"}
	base.Rows["wire-b"] = &dorv1.WireRow{ArticleId: "wire-b"}
	base.WireOrder = []string{"wire-a", "wire-b"}
	base.Revisions["STORY/link:expired"] = 3
	base.Scores["wire-a"] = &dorv1.MatchScore{ArticleId: "wire-a", Scores: map[string]float32{"prompt": .1}}
	base.ScoreOrder = []string{"wire-a"}
	if err := store.Initialize(ctx, base, 0, nil); err != nil {
		t.Fatal(err)
	}
	metadata := &Record{
		Ordinal: 1, PreviousSeq: 0, PreviousRaw: 0,
		Revisions:       map[string]uint64{"STORY/story:new": 10},
		RevisionDeletes: []string{"STORY/link:expired"},
		Scores:          map[string]*dorv1.MatchScore{"wire-b": {ArticleId: "wire-b", Scores: map[string]float32{"prompt": .8}}},
		ScoreDeletes:    []string{"wire-a"},
		WireOrder:       OrderChange{Remove: []string{"wire-a"}, Append: []string{"wire-c"}},
		ScoreOrder:      OrderChange{Remove: []string{"wire-a"}, Append: []string{"wire-b"}},
	}
	if err := store.Reserve(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, 1, 0, 0); err != nil {
		t.Fatalf("commit metadata-only record: %v", err)
	}
	recovered, pending, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != nil || recovered.Ordinal != 1 || recovered.Seq != 0 || recovered.Raw != 0 {
		t.Fatalf("metadata-only recovery watermarks=(%d,%d,%d), pending=%+v", recovered.Ordinal, recovered.Seq, recovered.Raw, pending)
	}
	if !reflect.DeepEqual(recovered.WireOrder, []string{"wire-b", "wire-c"}) || !reflect.DeepEqual(recovered.ScoreOrder, []string{"wire-b"}) {
		t.Fatalf("recovered orders wire=%v score=%v", recovered.WireOrder, recovered.ScoreOrder)
	}
	if _, ok := recovered.Rows["wire-a"]; ok || recovered.Rows["wire-b"] == nil || recovered.Rows["wire-c"] != nil {
		t.Fatalf("recovered rows after order patch=%v", recovered.Rows)
	}
	if _, ok := recovered.Revisions["STORY/link:expired"]; ok || recovered.Revisions["STORY/story:new"] != 10 {
		t.Fatalf("recovered revision metadata=%v", recovered.Revisions)
	}
	if _, ok := recovered.Scores["wire-a"]; ok || recovered.Scores["wire-b"].Scores["prompt"] != .8 {
		t.Fatalf("recovered score metadata=%v", recovered.Scores)
	}
}

func TestCompactionPreservesNewerCommittedHeadAndPendingRecord(t *testing.T) {
	store, ctx := publicationTestStore(t, "owner", time.Minute)
	if err := store.Initialize(ctx, NewState(nil), 0, nil); err != nil {
		t.Fatal(err)
	}
	first := publicationRecord(t, 1, 0, 0, 1, &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "s1", Title: "one"}}})
	if err := store.Reserve(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	oldView, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldView = oldView.Clone()

	second := publicationRecord(t, 2, 1, 1, 2, &dorv1.Delta{Seq: 2, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "s2", Title: "two"}}})
	if err := store.Reserve(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, 2, 2, 2); err != nil {
		t.Fatal(err)
	}
	pending := &Record{Ordinal: 3, PreviousSeq: 2, PreviousRaw: 2, Revisions: map[string]uint64{"ENRICH/clean:pending": 50}}
	if err := store.Reserve(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(ctx, oldView); err != nil {
		t.Fatalf("compact captured ordinal 1 while head/pending are newer: %v", err)
	}
	got, gotPending, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Ordinal != 2 || got.Seq != 2 || got.Raw != 2 || got.Stories["s1"] == nil || got.Stories["s2"] == nil {
		t.Fatalf("compacted committed head=%+v", got)
	}
	if gotPending == nil || gotPending.Ordinal != 3 || gotPending.Revisions["ENRICH/clean:pending"] != 50 {
		t.Fatalf("compaction lost pending record: %+v", gotPending)
	}
	root, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root.Base.Ordinal != 1 || len(root.Entries) != 1 || root.Entries[0].Ordinal != 2 || root.Pending == nil || root.Pending.Ordinal != 3 {
		t.Fatalf("compaction root base=%+v entries=%+v pending=%+v", root.Base, root.Entries, root.Pending)
	}
}

func TestGCHonorsReaderGraceAndOwnerFence(t *testing.T) {
	store, ctx := publicationTestStore(t, "owner-a", time.Minute)
	now := time.Now()
	store.Now = func() time.Time { return now }
	if err := store.Initialize(ctx, NewState(nil), 0, nil); err != nil {
		t.Fatal(err)
	}
	rootBefore, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldBase := rootBefore.Base.Blob

	record := publicationRecord(t, 1, 0, 0, 1, &dorv1.Delta{Seq: 1, Body: &dorv1.Delta_StoryUpsert{StoryUpsert: &dorv1.Story{Id: "s1", Title: "reader-visible"}}})
	if err := store.Reserve(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	current, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(ctx, current); err != nil {
		t.Fatal(err)
	}
	// A reader holding the old root's immutable descriptor remains able to
	// finish during the grace period after a new base has been installed.
	if _, err := store.ReadBlob(ctx, oldBase); err != nil {
		t.Fatalf("retired base unavailable during grace: %v", err)
	}
	now = now.Add(Grace - time.Second)
	if err := store.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadBlob(ctx, oldBase); err != nil {
		t.Fatalf("GC removed retired base inside grace: %v", err)
	}
	currentRoot, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	currentBase := currentRoot.Base.Blob
	now = now.Add(2 * time.Second)
	if err := store.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadBlob(ctx, oldBase); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("expired retired base read error=%v, want missing chunk", err)
	}
	if _, err := store.ReadBlob(ctx, currentBase); err != nil {
		t.Fatalf("GC removed current base: %v", err)
	}

	storeB := New(store.KV, "owner-b", time.Minute, store.Limit)
	storeB.Now = func() time.Time { return now.Add(2 * time.Minute) }
	if err := storeB.Claim(ctx); err != nil {
		t.Fatalf("successor claim: %v", err)
	}
	if err := store.GC(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("old owner's GC error=%v, want ErrFenced", err)
	}
}

func TestLoadFailsClosedOnCorruptCurrentAuthority(t *testing.T) {
	store, ctx := publicationTestStore(t, "owner", time.Minute)
	initial := NewState(nil)
	initial.Stories["s1"] = &dorv1.Story{Id: "s1", Title: "authoritative"}
	if err := store.Initialize(ctx, initial, 0, nil); err != nil {
		t.Fatal(err)
	}
	root, _, err := store.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := blobKey(root.Base.Blob, 0)
	entry, err := store.KV.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.KV.Update(ctx, key, []byte("corrupt authoritative base"), entry.Revision()); err != nil {
		t.Fatal(err)
	}
	state, pending, err := store.Load(ctx)
	if err == nil {
		t.Fatal("corrupt current base was accepted")
	}
	if state != nil || pending != nil {
		t.Fatalf("corrupt authority fabricated state=%+v pending=%+v", state, pending)
	}
}

func TestTakeoverRetiresAbandonedStageAndGCCollectsItAfterGrace(t *testing.T) {
	storeA, ctx := publicationTestStore(t, "owner-a", time.Second)
	now := time.Now()
	storeA.Now = func() time.Time { return now }
	if err := storeA.Initialize(ctx, NewState(nil), 0, nil); err != nil {
		t.Fatal(err)
	}
	base, _, err := storeA.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := storeA.stage(ctx, []byte("orphan staged payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := storeA.ReadRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := storeA.ReadBlob(ctx, orphan); err != nil {
		t.Fatalf("staged orphan was not fully written: %v", err)
	}

	storeB := New(storeA.KV, "owner-b", time.Second, storeA.Limit)
	storeB.Now = func() time.Time { return now.Add(2 * time.Second) }
	if err := storeB.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	root, _, err := storeB.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Staging) != 0 || len(root.Retired) == 0 || root.Retired[len(root.Retired)-1].Blob.ID != orphan.ID {
		t.Fatalf("takeover did not move abandoned stage to retirement ledger: staging=%+v retired=%+v", root.Staging, root.Retired)
	}
	storeB.Now = func() time.Time { return now.Add(2*time.Second + Grace + time.Second) }
	if err := storeB.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := storeB.ReadBlob(ctx, orphan); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("orphan stage after grace read error=%v, want missing chunk", err)
	}
	if _, err := storeB.ReadBlob(ctx, base.Base.Blob); err != nil {
		t.Fatalf("orphan GC damaged current base: %v", err)
	}
}
