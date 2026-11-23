package agents

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// v203ProjectionSink models the durable SQL link projection. The hook runs
// before the row mutation, so a test can hold one owner inside the sink while a
// newer owner completes. It is deliberately synchronous and deterministic.
type v203ProjectionSink struct {
	mu      sync.Mutex
	rows    map[string]store.LinkRecord
	records []store.LinkRecord
	err     error
	store   *store.Store
	before  func(store.LinkRecord)
}

func newV203ProjectionSink() *v203ProjectionSink {
	return &v203ProjectionSink{rows: make(map[string]store.LinkRecord)}
}

func v203ProjectionKey(a, b string) string { return a + "|" + b }

func (s *v203ProjectionSink) EnqueueLink(rec store.LinkRecord) bool {
	if s.before != nil {
		s.before(rec)
	}
	s.mu.Lock()
	s.records = append(s.records, rec)
	if rec.Remove {
		delete(s.rows, v203ProjectionKey(rec.A, rec.B))
	} else {
		s.rows[v203ProjectionKey(rec.A, rec.B)] = rec
	}
	st := s.store
	s.mu.Unlock()
	if st != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := st.UpsertLinks(ctx, []store.LinkRecord{rec}); err != nil {
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()
			return false
		}
	}
	return true
}

func (s *v203ProjectionSink) row(a, b string) (store.LinkRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.rows[v203ProjectionKey(a, b)]
	return rec, ok
}

func (s *v203ProjectionSink) snapshot() ([]store.LinkRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.LinkRecord(nil), s.records...), s.err
}

func v203ConvergenceSink(t *testing.T, ctx context.Context) *v203ProjectionSink {
	t.Helper()
	sink := newV203ProjectionSink()
	if strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL")) == "" {
		return sink
	}
	st := relationAuthorityTestStore(t)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate convergence test schema: %v", err)
	}
	sink.store = st
	return sink
}

// TestRelationAuthorityV2StaleSinkCannotOverwriteWinner holds an old removal
// inside the SQL sink until a newer link completes. The old sink then applies a
// stale DELETE; production must detect that and re-enqueue the current winner so
// the database converges instead of staying behind the ledger/KV/stream.
func TestRelationAuthorityV2StaleSinkCannotOverwriteWinner(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	now := time.Now()
	sink := v203ConvergenceSink(t, ctx)
	slow := relationCoreForTest(t, ctx, b, nil, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	slow.sink, fast.sink = sink, sink
	a, other := v203SeedPair(t, ctx, b, slow, fast)
	key := relationPairKey(a.Id, other.Id)
	old := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "remove"}
	oldRev, stored, err := slow.storePairRecord(ctx, key, old, 0, false)
	if err != nil || !stored {
		t.Fatalf("seed old removal: stored=%v err=%v", stored, err)
	}

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sink.before = func(rec store.LinkRecord) {
		if rec.Remove && rec.A == a.Id && rec.B == other.Id {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
	}
	oldDone := make(chan error, 1)
	go func() { oldDone <- slow.applyPairOutput(ctx, key, old, oldRev) }()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("old removal never reached the sink")
	}

	latest := proto.Clone(a).(*dorv1.Story)
	latest.Title = "newer winner during sink"
	regressionObserve(t, fast, latest, 30)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	winner := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 30, SeqB: 20, Outcome: "link", Link: data}
	winnerRev, stored, err := fast.storePairRecord(ctx, key, winner, oldRev, true)
	if err != nil || !stored {
		t.Fatalf("store newer winner: stored=%v err=%v", stored, err)
	}
	if err := fast.applyPairOutput(ctx, key, winner, winnerRev); err != nil {
		t.Fatal(err)
	}
	if row, ok := sink.row(a.Id, other.Id); !ok || row.Remove || row.Strength != .8 {
		t.Fatalf("winner projection before stale sink release = %+v ok=%v", row, ok)
	}
	close(release)
	select {
	case err := <-oldDone:
		if err != nil {
			t.Fatalf("stale sink completion returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("stale sink completion did not finish")
	}

	if row, ok := sink.row(a.Id, other.Id); !ok || row.Remove || row.Strength != .8 {
		t.Fatalf("stale SQL sink was not repaired: %+v ok=%v", row, ok)
	}
	records, sinkErr := sink.snapshot()
	if sinkErr != nil || len(records) == 0 || records[len(records)-1].Remove || records[len(records)-1].Strength != .8 {
		t.Fatalf("sink record order = %+v err=%v, want current link last", records, sinkErr)
	}
	if got := pairRecordForTest(t, ctx, b, a.Id, other.Id); got.Outcome != "link" || got.SeqA != 30 || !got.Published {
		t.Fatalf("winner ledger after stale sink = %+v", got)
	}
	if kvLink, ok := live.LoadLink(ctx, fast.kv, a.Id, other.Id); !ok || kvLink.Strength != .8 {
		t.Fatalf("winner KV after stale sink = %+v ok=%v", kvLink, ok)
	}
	bodies := collectSubject(t, b, bus.SubjectLinkChanged)
	var last dorv1.Link
	if len(bodies) == 0 || proto.Unmarshal([]byte(bodies[len(bodies)-1]), &last) != nil || last.Strength != .8 {
		t.Fatalf("last link.changed after stale sink = %+v bodies=%d", &last, len(bodies))
	}
	if sink.store != nil {
		v203AssertDBLink(t, ctx, sink.store, a.Id, other.Id, .8)
	}
	restarted := relationCoreForTest(t, ctx, b, nil, now)
	restarted.sink = sink
	restarted.retryOutputs(ctx)
	if row, ok := sink.row(a.Id, other.Id); !ok || row.Remove || row.Strength != .8 {
		t.Fatalf("restart lost repaired SQL projection: %+v ok=%v", row, ok)
	}
	if sink.store != nil {
		v203AssertDBLink(t, ctx, sink.store, a.Id, other.Id, .8)
	}
}

type v203RejectOnceSink struct {
	mu       sync.Mutex
	rejected bool
	parent   *v203ProjectionSink
}

func (s *v203RejectOnceSink) EnqueueLink(rec store.LinkRecord) bool {
	s.mu.Lock()
	reject := !s.rejected
	if reject {
		s.rejected = true
	}
	s.mu.Unlock()
	if reject {
		return false
	}
	return s.parent.EnqueueLink(rec)
}

// TestRelationAuthorityV2SinkRejectionRemainsRetryable checks that a false
// sink result does not mark the generation Published. The next retry must
// complete KV/stream/sink and mark the generation published exactly once.
func TestRelationAuthorityV2SinkRejectionRemainsRetryable(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	parent := newV203ProjectionSink()
	sink := &v203RejectOnceSink{parent: parent}
	c := relationCoreForTest(t, ctx, b, nil, now)
	c.sink = sink
	a, other := v203SeedPair(t, ctx, b, c)
	key := relationPairKey(a.Id, other.Id)
	link := &dorv1.Link{A: a.Id, B: other.Id, Strength: .8, Confidence: .9, UpdatedAt: now.Unix()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	rec := &relationPairRecord{A: a.Id, B: other.Id, SeqA: 10, SeqB: 20, Outcome: "link", Link: data}
	rev, stored, err := c.storePairRecord(ctx, key, rec, 0, false)
	if err != nil || !stored {
		t.Fatalf("store link: stored=%v err=%v", stored, err)
	}
	if err := c.applyPairOutput(ctx, key, rec, rev); err == nil {
		t.Fatal("link output succeeded although the sink rejected it")
	}
	if got := pairRecordForTest(t, ctx, b, a.Id, other.Id); got.Published {
		t.Fatalf("rejected sink marked generation published: %+v", got)
	}
	c.retryOutputs(ctx)
	if got := pairRecordForTest(t, ctx, b, a.Id, other.Id); !got.Published {
		t.Fatalf("retry did not publish after sink recovered: %+v", got)
	}
	if row, ok := parent.row(a.Id, other.Id); !ok || row.Remove || row.Strength != .8 {
		t.Fatalf("sink retry row = %+v ok=%v", row, ok)
	}
	if got := subjectMessageCount(t, ctx, b, bus.SubjectLinkChanged); got != 1 {
		t.Fatalf("link.changed after sink retry = %d, want 1", got)
	}
}

type v203PairGetHook struct {
	jetstream.KeyValue
	target string
	reads  int
	after  func()
}

func (k *v203PairGetHook) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	entry, err := k.KeyValue.Get(ctx, key)
	if key == k.target {
		k.reads++
		if k.reads == 2 && k.after != nil {
			k.after()
		}
	}
	return entry, err
}

// TestRelationAuthorityV2RemovalCASFencesSameInputDecay advances a newer
// same-input decay generation between the removal precheck and its authority
// CAS. The old below-floor removal must not overwrite that newer generation.
func TestRelationAuthorityV2RemovalCASFencesSameInputDecay(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	sink := newV203ProjectionSink()
	slow := relationCoreForTest(t, ctx, b, nil, now)
	fast := relationCoreForTest(t, ctx, b, nil, now)
	slow.sink, fast.sink = sink, sink
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
	if err := slow.applyPairOutput(ctx, key, first, firstRev); err != nil {
		t.Fatal(err)
	}
	oldRevision := live.LinkRevision(initial)

	hook := &v203PairGetHook{KeyValue: slow.kv, target: key}
	hook.after = func() {
		decayed := &dorv1.Link{A: a.Id, B: other.Id, Strength: .57, Confidence: .71, UpdatedAt: now.Unix()}
		if _, stored, err := fast.storeDecayRecord(ctx, key, decayed, 0, 1, 10, 20); err != nil || !stored {
			t.Fatalf("concurrent decay generation: stored=%v err=%v", stored, err)
		}
		rec, rev, ok, err := fast.loadPairRecord(ctx, key)
		if err != nil || !ok {
			t.Fatalf("load concurrent decay: ok=%v err=%v", ok, err)
		}
		if err := fast.applyPairOutput(ctx, key, rec, rev); err != nil {
			t.Fatalf("apply concurrent decay: %v", err)
		}
	}
	slow.kv = hook
	slow.fadeLink(ctx, a.Id, other.Id, oldRevision, 0, 72*time.Hour)

	got := pairRecordForTest(t, ctx, b, a.Id, other.Id)
	if got.Outcome != "link" || got.SeqA != 10 || got.SeqB != 20 || got.Generation < 2 || !got.Published {
		t.Fatalf("stale removal overwrote same-input decay authority: %+v", got)
	}
	kvLink, ok := live.LoadLink(ctx, slow.kv, a.Id, other.Id)
	if !ok || kvLink.Strength != .57 {
		t.Fatalf("KV after fenced same-input removal = %+v ok=%v", kvLink, ok)
	}
	if row, ok := sink.row(a.Id, other.Id); !ok || row.Remove || row.Strength != .57 {
		t.Fatalf("sink after fenced same-input removal = %+v ok=%v", row, ok)
	}
	bodies := collectSubject(t, b, bus.SubjectLinkChanged)
	var last dorv1.Link
	if len(bodies) == 0 || proto.Unmarshal([]byte(bodies[len(bodies)-1]), &last) != nil || last.Strength != .57 {
		t.Fatalf("last link.changed after fenced removal = %+v bodies=%d", &last, len(bodies))
	}
}
