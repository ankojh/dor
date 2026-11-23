package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// The V2-03 tests keep the cross-process interleavings in committed source so
// that the durable CAS and projection fences, rather than a local pair lock,
// are what make the assertions pass.

type relationAuthorityPutHook struct {
	jetstream.KeyValue
	target string
	once   sync.Once
	before func()
}

func (k *relationAuthorityPutHook) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	if key == k.target {
		k.once.Do(k.before)
	}
	return k.KeyValue.Put(ctx, key, value)
}

type relationAuthorityPublishedHook struct {
	jetstream.KeyValue
	target  string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (k *relationAuthorityPublishedHook) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	if key == k.target {
		var rec relationPairRecord
		if json.Unmarshal(value, &rec) == nil && rec.Published {
			k.once.Do(func() { close(k.reached); <-k.release })
		}
	}
	return k.KeyValue.Update(ctx, key, value, rev)
}

type relationAuthoritySink struct {
	mu      sync.Mutex
	records []store.LinkRecord
	store   *store.Store
	err     error
}

type relationAuthorityEightDecider struct{}

func (relationAuthorityEightDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	return decide.Response{Answers: map[string]decide.Answer{
		"related":   {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: .95},
		"type":      {Type: decide.TypeChoice, Present: true, Choice: "thematic"},
		"direction": {Type: decide.TypeChoice, Present: true, Choice: "unclear"},
		"strength":  {Type: decide.TypeScore, Present: true, HasScore: true, Score: .8 * 3 / (.95 * .9)},
	}}, nil
}

func (s *relationAuthoritySink) EnqueueLink(rec store.LinkRecord) bool {
	s.mu.Lock()
	s.records = append(s.records, rec)
	s.mu.Unlock()
	if s.store == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.store.UpsertLinks(ctx, []store.LinkRecord{rec}); err != nil {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		return false
	}
	return true
}

func (s *relationAuthoritySink) snapshot() ([]store.LinkRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.LinkRecord(nil), s.records...), s.err
}

func v203SeedPair(t *testing.T, ctx context.Context, b *bus.Bus, cores ...*relationCore) (*dorv1.Story, *dorv1.Story) {
	t.Helper()
	now := time.Now()
	a := relationTestStory("v203-authority-a", now.Unix(), .8, 40, -74)
	other := relationTestStory("v203-authority-b", now.Unix(), .8, 40, -74)
	for _, c := range cores {
		regressionObserve(t, c, a, 10)
		regressionObserve(t, c, other, 20)
	}
	return a, other
}

func v203OutputState(t *testing.T, ctx context.Context, b *bus.Bus, c *relationCore, a, other string, strength float32) {
	t.Helper()
	ledger := pairRecordForTest(t, ctx, b, a, other)
	if ledger.Outcome != "link" || ledger.SeqA != 30 || ledger.SeqB != 20 {
		t.Fatalf("pair authority = outcome %q seq %d/%d, want winning link at 30/20", ledger.Outcome, ledger.SeqA, ledger.SeqB)
	}
	kvLink, ok := live.LoadLink(ctx, c.kv, a, other)
	if !ok || kvLink.Strength != strength {
		t.Fatalf("KV projection = %+v, exists=%v, want strength %v", kvLink, ok, strength)
	}
	bodies := collectSubject(t, b, bus.SubjectLinkChanged)
	if len(bodies) == 0 {
		t.Fatal("link.changed has no output")
	}
	var last dorv1.Link
	if err := proto.Unmarshal([]byte(bodies[len(bodies)-1]), &last); err != nil {
		t.Fatal(err)
	}
	if last.Strength != strength || last.A != a || last.B != other {
		t.Fatalf("last link.changed = %+v, want pair %s/%s at strength %v", &last, a, other, strength)
	}
	publisher := v203PublisherFromStream(t, bodies)
	snap := publisher.snapshotLocked()
	wantKey := publisherLinkKey(a, other)
	found := false
	for _, link := range snap.Links {
		if publisherLinkKey(link.A, link.B) == wantKey {
			found = true
			if link.Strength != strength {
				t.Fatalf("publisher snapshot pair %s strength=%v, want %v", wantKey, link.Strength, strength)
			}
		}
	}
	if !found {
		t.Fatalf("publisher snapshot omitted winning pair %s", wantKey)
	}
}

func v203PublisherFromStream(t *testing.T, bodies []string) *publisherCore {
	t.Helper()
	publisher := newPublisherCore(nil, testLogger())
	for _, body := range bodies {
		var link dorv1.Link
		if err := proto.Unmarshal([]byte(body), &link); err != nil {
			t.Fatal(err)
		}
		publisher.onLink(&link)
		key := "link:" + publisherLinkKey(link.A, link.B)
		publisher.mu.Lock()
		publisher.applyDeltaLocked(publisher.pending[key])
		publisher.mu.Unlock()
	}
	return publisher
}

func v203SnapshotHasLink(snap *dorv1.Snapshot, key string) bool {
	if snap == nil {
		return false
	}
	for _, link := range snap.Links {
		if publisherLinkKey(link.A, link.B) == key {
			return true
		}
	}
	return false
}

func TestRelationAuthorityV2OldRemoveCannotTearUnpublishedWinner(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Log("failure-before evidence is preserved in review-v2/lanes/b-pipeline/evidence/v2-confirmed-probes.log and critical-race.log: old remove left KV absent after restart while ledger/stream retained strength .8")
	now := time.Now()
	sink := &relationAuthoritySink{}
	var db *store.Store
	if strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL")) != "" {
		db = relationAuthorityTestStore(t)
		if err := db.Migrate(ctx); err != nil {
			t.Fatalf("migrate private test schema: %v", err)
		}
		sink.store = db
	}
	slow := relationCoreForTest(t, ctx, b, nil, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	a, other := v203SeedPair(t, ctx, b, slow, fast)
	key := relationPairKey(a.Id, other.Id)
	old := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "remove"}
	oldRev, stored, err := slow.storePairRecord(ctx, key, old, 0, false)
	if err != nil || !stored {
		t.Fatalf("seed old remove: stored=%v err=%v", stored, err)
	}
	hook := &relationAuthorityPublishedHook{KeyValue: fast.kv, target: key, reached: make(chan struct{}), release: make(chan struct{})}
	fast.kv, fast.sink = hook, sink
	fastDone := make(chan error, 1)
	slow.kv = &relationAuthorityPutHook{KeyValue: slow.kv, target: live.LinkKey(a.Id, other.Id), before: func() {
		latest := proto.Clone(a).(*dorv1.Story)
		latest.Title = "authority V2 content at sequence 30"
		regressionObserve(t, fast, latest, 30)
		link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
		data, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(link)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: data}
		rev, ok, storeErr := fast.storePairRecord(ctx, key, rec, oldRev, true)
		if storeErr != nil || !ok {
			t.Fatalf("store newer generation: stored=%v err=%v", ok, storeErr)
		}
		go func() { fastDone <- fast.applyPairOutput(ctx, key, rec, rev) }()
		select {
		case <-hook.reached:
		case <-ctx.Done():
			t.Fatal("winner never reached final Published CAS")
		}
	}}
	oldDone := make(chan error, 1)
	go func() { oldDone <- slow.applyPairOutput(ctx, key, old, oldRev) }()
	select {
	case err := <-oldDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("old removal did not finish while winner Published CAS was paused")
	}
	// At this point the newer record is deliberately unpublished. Every public
	// projection and the synchronous sink must already reflect its authority.
	v203OutputState(t, ctx, b, fast, a.Id, other.Id, .8)
	if rec := pairRecordForTest(t, ctx, b, a.Id, other.Id); rec.Published {
		t.Fatal("test did not hold the winner before its Published CAS")
	}
	if records, sinkErr := sink.snapshot(); sinkErr != nil || len(records) == 0 || records[len(records)-1].Remove || records[len(records)-1].Strength != .8 {
		t.Fatalf("sync sink before winner CAS = %+v err=%v, want link strength .8", records, sinkErr)
	}
	if db != nil {
		v203AssertDBLink(t, ctx, db, a.Id, other.Id, .8)
	}
	close(hook.release)
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("winner did not finish after Published CAS release")
	}
	restarted := relationCoreForTest(t, ctx, b, nil, now)
	restarted.sink = sink
	restarted.retryOutputs(ctx)
	v203OutputState(t, ctx, b, restarted, a.Id, other.Id, .8)
	if rec := pairRecordForTest(t, ctx, b, a.Id, other.Id); !rec.Published {
		t.Fatal("winning authority remained unpublished after retry")
	}
	if records, sinkErr := sink.snapshot(); sinkErr != nil || len(records) == 0 || records[len(records)-1].Remove || records[len(records)-1].Strength != .8 {
		t.Fatalf("sync sink after restart = %+v err=%v, want link strength .8", records, sinkErr)
	}
	if db != nil {
		v203AssertDBLink(t, ctx, db, a.Id, other.Id, .8)
	}
}

func v203AssertDBLink(t *testing.T, ctx context.Context, st *store.Store, a, other string, want float32) {
	t.Helper()
	var strength float32
	if err := st.Pool().QueryRow(ctx, `SELECT strength FROM links WHERE a=$1 AND b=$2`, a, other).Scan(&strength); err != nil {
		t.Fatalf("read winning DB link: %v", err)
	}
	if strength != want {
		t.Fatalf("database strength = %v, want %v", strength, want)
	}
	var count int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM links WHERE a=$1 AND b=$2`, a, other).Scan(&count); err != nil || count != 1 {
		t.Fatalf("database link count = %d err=%v, want one winning row and no stale tombstone", count, err)
	}
}

func TestRelationAuthorityV2StaleDecayAndCurrentDecay(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Log("failure-before evidence is preserved in review-v2/lanes/b-pipeline/evidence/v2-confirmed-probes.log and critical-race.log: stale fadeLink changed KV and stream from the winning .57 judgement to .24772097")
	now := time.Now()
	sink := &relationAuthoritySink{}
	slow := relationCoreForTest(t, ctx, b, nil, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	a, other := v203SeedPair(t, ctx, b, slow, fast)
	key := relationPairKey(a.Id, other.Id)
	initial := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	initialBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	first := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "link", Link: initialBytes}
	firstRev, stored, err := slow.storePairRecord(ctx, key, first, 0, false)
	if err != nil || !stored {
		t.Fatalf("store initial link: stored=%v err=%v", stored, err)
	}
	slow.sink = sink
	if err := slow.applyPairOutput(ctx, key, first, firstRev); err != nil {
		t.Fatal(err)
	}
	oldRevision := live.LinkRevision(initial)
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "fresh content at sequence 30"
	regressionObserve(t, fast, latest, 30)
	winning := &dorv1.Link{A: a.Id, B: other.Id, Strength: .57, Confidence: .71, UpdatedAt: now.Unix() + 1}
	winningBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(winning)
	if err != nil {
		t.Fatal(err)
	}
	winner := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: winningBytes}
	winnerRev, stored, err := fast.storePairRecord(ctx, key, winner, firstRev, true)
	if err != nil || !stored {
		t.Fatalf("store fresh judgement: stored=%v err=%v", stored, err)
	}
	fast.sink = sink
	if err := fast.applyPairOutput(ctx, key, winner, winnerRev); err != nil {
		t.Fatal(err)
	}
	winningAuthority := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	slow.fadeLink(ctx, a.Id, other.Id, oldRevision, 0, 2*time.Hour)
	slow.fadeLink(ctx, a.Id, other.Id, oldRevision, 0, 72*time.Hour) // below-floor stale removal attempt
	if got := pairRecordForTest(t, ctx, b, a.Id, other.Id); got.Generation != winningAuthority.Generation || got.SeqA != 30 || got.Outcome != "link" {
		t.Fatalf("stale decay changed authority: %+v, winner=%+v", got, winningAuthority)
	}
	v203OutputState(t, ctx, b, fast, a.Id, other.Id, .57)
	if records, sinkErr := sink.snapshot(); sinkErr != nil || len(records) == 0 || records[len(records)-1].Remove || records[len(records)-1].Strength != .57 {
		t.Fatalf("sink after stale decay = %+v err=%v, want winner strength .57", records, sinkErr)
	}
	current, ok := live.LoadLink(ctx, fast.kv, a.Id, other.Id)
	if !ok {
		t.Fatal("winning link missing before current decay")
	}
	fast.fadeLink(ctx, a.Id, other.Id, live.LinkRevision(current), 0, 2*time.Hour)
	decayed, ok := live.LoadLink(ctx, fast.kv, a.Id, other.Id)
	if !ok || !(decayed.Strength > relationLinkFloor && decayed.Strength < .57) {
		t.Fatalf("current-owner decay did not visibly fade link: %+v exists=%v", decayed, ok)
	}
	v203OutputState(t, ctx, b, fast, a.Id, other.Id, decayed.Strength)
	restart := relationCoreForTest(t, ctx, b, nil, now)
	restart.retryOutputs(ctx)
	recovered, ok := live.LoadLink(ctx, restart.kv, a.Id, other.Id)
	if !ok || recovered.Strength != decayed.Strength {
		t.Fatalf("restart lost current decay: recovered=%+v exists=%v want strength %v", recovered, ok, decayed.Strength)
	}
	v203OutputState(t, ctx, b, restart, a.Id, other.Id, decayed.Strength)
	if rec := pairRecordForTest(t, ctx, b, a.Id, other.Id); !rec.Published {
		t.Fatalf("current decay not published in durable authority after restart: %+v", rec)
	}
}

func TestRelationAuthorityV2UnpublishedWinnerRepairsAfterRestart(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	c := relationCoreForTest(t, ctx, b, nil, now)
	a, other := v203SeedPair(t, ctx, b, c)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .64, Confidence: .76, UpdatedAt: now.Unix()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "link", Link: data}
	_, stored, err := c.storePairRecord(ctx, relationPairKey(a.Id, other.Id), rec, 0, false)
	if err != nil || !stored {
		t.Fatalf("seed unpublished winner: stored=%v err=%v", stored, err)
	}
	if !live.PutLink(ctx, c.kv, &dorv1.Link{A: a.Id, B: other.Id, UpdatedAt: now.Unix()}) {
		t.Fatal("seed stale tombstone projection")
	}
	restarted := relationCoreForTest(t, ctx, b, nil, now)
	restarted.retryOutputs(ctx)
	got, ok := live.LoadLink(ctx, restarted.kv, a.Id, other.Id)
	if !ok || got.Strength != .64 {
		t.Fatalf("unpublished winner not repaired after restart: %+v exists=%v", got, ok)
	}
	ledger := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if !ledger.Published || ledger.Outcome != "link" {
		t.Fatalf("repaired pair record = %+v", ledger)
	}
}

func TestRelationAuthorityV2StaleDecayCannotResurrectRemoval(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	sink := &relationAuthoritySink{}
	var db *store.Store
	if strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL")) != "" {
		db = relationAuthorityTestStore(t)
		if err := db.Migrate(ctx); err != nil {
			t.Fatalf("migrate private test schema: %v", err)
		}
		sink.store = db
	}
	slow := relationCoreForTest(t, ctx, b, relationAuthorityEightDecider{}, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	slow.sink, fast.sink = sink, sink
	a, other := v203SeedPair(t, ctx, b, slow, fast)
	if err := slow.judgePair(ctx, slow.index[a.Id], slow.index[other.Id]); err != nil {
		t.Fatalf("slow 10/20 judgement: %v", err)
	}
	oldLink, ok := live.LoadLink(ctx, slow.kv, a.Id, other.Id)
	if !ok || oldLink.Strength != .8 {
		t.Fatalf("initial judged link = %+v exists=%v, want strength .8", oldLink, ok)
	}
	oldRevision := live.LinkRevision(oldLink)
	oldAuthority := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if oldAuthority.Outcome != "link" || oldAuthority.SeqA != 10 || oldAuthority.SeqB != 20 || !oldAuthority.Published {
		t.Fatalf("initial judged authority = %+v, want published link at 10/20", oldAuthority)
	}

	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "new content before lifecycle removal"
	regressionObserve(t, fast, latest, 30)
	fast.removeLinkFenced(ctx, a.Id, other.Id, oldRevision)
	removedAuthority := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if removedAuthority.Outcome != "remove" || removedAuthority.SeqA != 30 || removedAuthority.SeqB != 20 || !removedAuthority.Published || removedAuthority.Generation <= oldAuthority.Generation {
		t.Fatalf("newer removal authority = %+v, want published removal generation after link", removedAuthority)
	}
	tombstone, ok := live.LoadLink(ctx, fast.kv, a.Id, other.Id)
	if !ok || tombstone.Strength != 0 || tombstone.Confidence != 0 {
		t.Fatalf("removal KV projection = %+v exists=%v, want zero-strength tombstone", tombstone, ok)
	}
	entry, err := fast.kv.Get(ctx, live.LinkKey(a.Id, other.Id))
	if err != nil {
		t.Fatalf("read tombstone KV revision: %v", err)
	}
	tombstoneKVRevision := entry.Revision()
	bodiesBefore := collectSubject(t, b, bus.SubjectLinkChanged)
	if len(bodiesBefore) < 2 {
		t.Fatalf("link.changed outputs=%d, want initial link and removal", len(bodiesBefore))
	}
	var streamTombstone dorv1.Link
	if err := proto.Unmarshal([]byte(bodiesBefore[len(bodiesBefore)-1]), &streamTombstone); err != nil {
		t.Fatal(err)
	}
	if streamTombstone.A != a.Id || streamTombstone.B != other.Id || streamTombstone.Strength != 0 || streamTombstone.Confidence != 0 {
		t.Fatalf("last link.changed = %+v, want zero-strength removal tombstone", &streamTombstone)
	}
	publisher := v203PublisherFromStream(t, bodiesBefore)
	if v203SnapshotHasLink(publisher.snapshotLocked(), publisherLinkKey(a.Id, other.Id)) {
		t.Fatal("publisher snapshot retained the removed pair")
	}
	recordsBefore, sinkErr := sink.snapshot()
	if sinkErr != nil || len(recordsBefore) != 2 {
		t.Fatalf("sync sink state before stale decay = %+v err=%v, want link then removal", recordsBefore, sinkErr)
	}
	if recordsBefore[0].Remove || recordsBefore[0].Strength != .8 || !recordsBefore[1].Remove {
		t.Fatalf("sync sink did not record link then removal: %+v", recordsBefore)
	}
	if db != nil {
		v203AssertDBLinkAbsent(t, ctx, db, a.Id, other.Id)
	}

	// The slow process still holds the old live link locally. Neither ordinary
	// nor below-floor decay may resurrect it after the newer durable removal.
	slow.fadeLink(ctx, a.Id, other.Id, oldRevision, 0, 2*time.Hour)
	slow.fadeLink(ctx, a.Id, other.Id, oldRevision, 0, 72*time.Hour)
	afterAuthority := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if afterAuthority.Generation != removedAuthority.Generation || afterAuthority.Outcome != "remove" || !afterAuthority.Published {
		t.Fatalf("stale decay changed removal authority: before=%+v after=%+v", removedAuthority, afterAuthority)
	}
	afterTombstone, ok := live.LoadLink(ctx, slow.kv, a.Id, other.Id)
	if !ok || afterTombstone.Strength != 0 || afterTombstone.Confidence != 0 {
		t.Fatalf("stale decay changed KV tombstone: %+v exists=%v", afterTombstone, ok)
	}
	entry, err = slow.kv.Get(ctx, live.LinkKey(a.Id, other.Id))
	if err != nil {
		t.Fatalf("read KV after stale decay: %v", err)
	}
	if entry.Revision() != tombstoneKVRevision {
		t.Fatalf("stale decay changed KV revision: got=%d want=%d", entry.Revision(), tombstoneKVRevision)
	}
	bodiesAfter := collectSubject(t, b, bus.SubjectLinkChanged)
	if len(bodiesAfter) != len(bodiesBefore) {
		t.Fatalf("stale decay emitted %d stream outputs, want unchanged %d", len(bodiesAfter), len(bodiesBefore))
	}
	afterPublisher := v203PublisherFromStream(t, bodiesAfter)
	if v203SnapshotHasLink(afterPublisher.snapshotLocked(), publisherLinkKey(a.Id, other.Id)) {
		t.Fatal("publisher snapshot resurrected the removed pair after stale decay")
	}
	recordsAfter, sinkErr := sink.snapshot()
	if sinkErr != nil || len(recordsAfter) != len(recordsBefore) || !recordsAfter[len(recordsAfter)-1].Remove {
		t.Fatalf("stale decay changed synchronous sink: before=%+v after=%+v err=%v", recordsBefore, recordsAfter, sinkErr)
	}
	if db != nil {
		v203AssertDBLinkAbsent(t, ctx, db, a.Id, other.Id)
	}

	restarted := relationCoreForTest(t, ctx, b, nil, now)
	restarted.sink = sink
	restarted.retryOutputs(ctx)
	recoveredAuthority := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if recoveredAuthority.Outcome != "remove" || !recoveredAuthority.Published || recoveredAuthority.Generation != removedAuthority.Generation {
		t.Fatalf("restart changed removal authority: %+v", recoveredAuthority)
	}
	recovered, ok := live.LoadLink(ctx, restarted.kv, a.Id, other.Id)
	if ok && (recovered.Strength > 0 || recovered.Confidence > 0) {
		t.Fatalf("restart resurrected positive link in KV: %+v", recovered)
	}
	restartBodies := collectSubject(t, b, bus.SubjectLinkChanged)
	restartPublisher := v203PublisherFromStream(t, restartBodies)
	if v203SnapshotHasLink(restartPublisher.snapshotLocked(), publisherLinkKey(a.Id, other.Id)) {
		t.Fatal("restart/retry publisher snapshot contains the removed pair")
	}
	recordsAfterRestart, sinkErr := sink.snapshot()
	if sinkErr != nil || len(recordsAfterRestart) != len(recordsBefore) || !recordsAfterRestart[len(recordsAfterRestart)-1].Remove {
		t.Fatalf("restart/retry changed sink state: before=%+v after=%+v err=%v", recordsBefore, recordsAfterRestart, sinkErr)
	}
	if db != nil {
		v203AssertDBLinkAbsent(t, ctx, db, a.Id, other.Id)
	}
}

func v203AssertDBLinkAbsent(t *testing.T, ctx context.Context, st *store.Store, a, other string) {
	t.Helper()
	var count int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM links WHERE a=$1 AND b=$2`, a, other).Scan(&count); err != nil || count != 0 {
		t.Fatalf("database link count=%d err=%v, want no resurrected row", count, err)
	}
}

func TestRelationAuthorityV2DatabaseReadback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires disposable PostgreSQL16/pgvector scratch database")
	}
	st := relationAuthorityTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate private test schema: %v", err)
	}
	now := time.Now().UTC()
	sink := &relationAuthoritySink{store: st}
	b := durabilityBus(t)
	c := relationCoreForTest(t, ctx, b, nil, now)
	c.sink = sink
	a, other := v203SeedPair(t, ctx, b, c)
	key := relationPairKey(a.Id, other.Id)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: data}
	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "DB winner content"
	regressionObserve(t, c, latest, 30)
	rev, stored, err := c.storePairRecord(ctx, key, rec, 0, false)
	if err != nil || !stored {
		t.Fatalf("store DB winner: stored=%v err=%v", stored, err)
	}
	if err := c.applyPairOutput(ctx, key, rec, rev); err != nil {
		t.Fatal(err)
	}
	// A delayed older tombstone must not reach the durable sink or erase the
	// winning SQL row after the pair authority has advanced.
	old := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "remove", Generation: 1}
	if err := c.applyPairOutput(ctx, key, old, rev-1); err != nil {
		t.Fatal(err)
	}
	v203AssertDBLink(t, ctx, st, a.Id, other.Id, .8)
	if records, sinkErr := sink.snapshot(); sinkErr != nil || len(records) != 1 || records[0].Remove || records[0].Strength != .8 {
		t.Fatalf("database sink records = %+v err=%v, want only winning .8 link", records, sinkErr)
	}
}

func relationAuthorityTestStore(t *testing.T) *store.Store {
	t.Helper()
	// Implementation follows store/store_test.go's private-schema pattern.
	// The DSN is deliberately never included in errors or test output.
	raw := strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL"))
	if raw == "" {
		t.Skip("set DOR_TEST_DATABASE_URL to disposable dor_v2_fix_v2_03 PostgreSQL16/pgvector database")
	}
	pc, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("could not parse DOR_TEST_DATABASE_URL (connection details redacted)")
	}
	if pc.ConnConfig.Database == "dor" {
		t.Fatal("refusing live dor database for relation authority integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	control, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatal("could not connect to disposable test database (connection details redacted)")
	}
	schema := fmt.Sprintf("dor_test_v203_%d_%d", os.Getpid(), time.Now().UnixNano())
	if len(schema) > 63 || !strings.HasPrefix(schema, "dor_test_v203_") {
		_ = control.Close(ctx)
		t.Fatalf("invalid private test schema name %q", schema)
	}
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := control.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = control.Close(ctx)
		t.Fatalf("create private test schema: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		_, _ = control.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		_ = control.Close(ctx)
		t.Fatal("parse disposable test URL")
	}
	query := u.Query()
	query.Set("options", "-csearch_path="+schema+",public")
	u.RawQuery = query.Encode()
	st, err := store.Open(ctx, store.Config{URL: u.String(), MaxConns: 2})
	if err != nil {
		_, _ = control.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
		_ = control.Close(ctx)
		t.Fatal("could not open store on private test schema (connection details redacted)")
	}
	cleanup := func() {
		st.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := control.Exec(dropCtx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop private test schema: %v", err)
		}
		if err := control.Close(dropCtx); err != nil {
			t.Errorf("close test database control connection: %v", err)
		}
	}
	t.Cleanup(cleanup)
	return st
}
