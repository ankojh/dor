package budget

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
)

func bucketDoc(t *testing.T, s Store, api string) bucket {
	t.Helper()
	raw, err := s.Get(context.Background(), "tokens."+api)
	if err != nil {
		t.Fatalf("read bucket: %v", err)
	}
	var b bucket
	if err := jsonUnmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReplayAfterMoreThan512Completions(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	var first Reservation
	for i := 0; i < 513; i++ {
		r := cycle(t, m, req("jev", PriorityDefault, 1, 0, 0.001), Usage{CostUSD: 0.001, InputTokens: 1})
		if i == 0 {
			first = r
		}
	}
	before := snap(t, m)
	if before.Calls != 513 || len(before.Pending) != 0 {
		t.Fatalf("before = calls %d pending %d", before.Calls, len(before.Pending))
	}
	// Replaying the very first reservation, long after any ring would have dropped it.
	if err := m.Record(context.Background(), first, Usage{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Record(context.Background(), first, Usage{CostUSD: 5, InputTokens: 999}); err != nil {
		t.Fatal(err)
	}
	after := snap(t, m)
	if after.Calls != before.Calls || math.Abs(after.USD-before.USD) > 1e-12 || after.InputTokens != before.InputTokens {
		t.Errorf("replay changed the counter: %+v -> %+v", before, after)
	}
}

// flakyStore fails the next Update once when armed.
type flakyStore struct {
	Store
	armed atomic.Bool
	err   error
}

func (f *flakyStore) Update(ctx context.Context, key string, fn func([]byte, bool) ([]byte, error)) error {
	if f.armed.CompareAndSwap(true, false) {
		return f.err
	}
	return f.Store.Update(ctx, key, fn)
}

func TestPartialRecordResumesWithoutReapplyingDaily(t *testing.T) {
	cfg := testCfg()
	cfg.TokenBuckets["jev"] = config.TokenBucket{Capacity: 100, RefillPerSecond: 0.001}
	boom := errors.New("token store down")
	flaky := &flakyStore{Store: NewMemoryStore(), err: boom}
	spend := NewMemoryStore()
	m := New(flaky, spend, cfg).WithClock((&clock{now: t0}).Now)
	ctx := context.Background()

	v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.5))
	if !v.Allowed {
		t.Fatalf("check = %+v", v)
	}
	if got := bucketTokens(t, flaky.Store, "jev"); got != 0 {
		t.Fatalf("bucket after check = %v", got)
	}

	flaky.armed.Store(true)
	usage := Usage{CostUSD: 0.3, InputTokens: 10}
	if err := m.Record(ctx, *v.Reservation, usage); !errors.Is(err, boom) {
		t.Fatalf("first record err = %v, want the injected error", err)
	}
	d := snap(t, m)
	if d.Calls != 1 || math.Abs(d.USD-0.3) > 1e-9 || len(d.Pending) != 0 {
		t.Errorf("the daily phase must already be applied: %+v", d)
	}
	if got := bucketTokens(t, flaky.Store, "jev"); got != 0 {
		t.Errorf("the bucket is untouched by the failed phase: %v", got)
	}
	if bucketDoc(t, flaky.Store, "jev").Pending[v.Reservation.ID].Tokens != 100 {
		t.Error("the token entry must stay pending so a retry can finish")
	}

	if err := m.Record(ctx, *v.Reservation, usage); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := bucketTokens(t, flaky.Store, "jev"); got != 90 {
		t.Errorf("bucket = %v, want 90 (reserved 100, actual 10)", got)
	}
	d = snap(t, m)
	if d.Calls != 1 || math.Abs(d.USD-0.3) > 1e-9 || d.InputTokens != 10 {
		t.Errorf("the daily phase was re-applied: %+v", d)
	}
	if len(bucketDoc(t, flaky.Store, "jev").Pending) != 0 {
		t.Error("token pending must be empty after the retry")
	}
	// A third Record is a complete no-op.
	if err := m.Record(ctx, *v.Reservation, Usage{CostUSD: 9, InputTokens: 50}); err != nil {
		t.Fatal(err)
	}
	if got := bucketTokens(t, flaky.Store, "jev"); got != 90 {
		t.Errorf("replay changed the bucket: %v", got)
	}
	if d2 := snap(t, m); d2.Calls != 1 || math.Abs(d2.USD-0.3) > 1e-9 {
		t.Errorf("replay changed the counter: %+v", d2)
	}
}

func TestTokenPhaseFailureInCheckRefundsTheDollars(t *testing.T) {
	boom := errors.New("token store down")
	flaky := &flakyStore{Store: NewMemoryStore(), err: boom}
	m := New(flaky, NewMemoryStore(), testCfg()).WithClock((&clock{now: t0}).Now)
	flaky.armed.Store(true)
	if _, err := m.Check(context.Background(), req("jev", PriorityDefault, 10, 0, 0.7)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if d := snap(t, m); d.USD != 0 || len(d.Pending) != 0 {
		t.Errorf("a failed token phase must not leave dollars reserved: %+v", d)
	}
}

func TestConcurrentDuplicateRecordAppliesOnce(t *testing.T) {
	for round := 0; round < 30; round++ {
		m, _, tokens, _ := newManager(testCfg())
		v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.5))
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := m.Record(context.Background(), *v.Reservation, Usage{CostUSD: 0.3, InputTokens: 40}); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		d := snap(t, m)
		if d.Calls != 1 || math.Abs(d.USD-0.3) > 1e-9 || d.InputTokens != 40 || len(d.Pending) != 0 {
			t.Fatalf("round %d: %+v, want one application", round, d)
		}
		if got := bucketTokens(t, tokens, "jev"); got != 960 {
			t.Fatalf("round %d: bucket = %v, want 960 (reconciled once)", round, got)
		}
	}
}

func TestStalePendingIsCollectedAndStaysCharged(t *testing.T) {
	m, c, tokens, _ := newManager(testCfg())
	ctx := context.Background()
	stale := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.5))
	if !stale.Allowed || m.StaleCollected() != 0 {
		t.Fatalf("stale=%+v collected=%d", stale, m.StaleCollected())
	}
	d := snap(t, m)
	if _, ok := d.Pending[stale.Reservation.ID]; !ok {
		t.Fatal("the reservation must be pending")
	}

	c.Advance(16 * time.Minute) // past the 15m PendingTTL
	fresh := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.25))
	if !fresh.Allowed {
		t.Fatalf("fresh = %+v", fresh)
	}
	d = snap(t, m)
	if _, ok := d.Pending[stale.Reservation.ID]; ok {
		t.Error("the stale daily entry must be collected")
	}
	if _, ok := d.Pending[fresh.Reservation.ID]; !ok {
		t.Error("the fresh entry must remain")
	}
	if math.Abs(d.USD-0.75) > 1e-9 {
		t.Errorf("the stale estimate stays charged: USD = %v, want 0.75", d.USD)
	}
	if bucketDoc(t, tokens, "jev").Pending[stale.Reservation.ID].Tokens != 0 {
		t.Error("the stale token entry must be collected")
	}
	if got := m.StaleCollected(); got != 2 {
		t.Errorf("StaleCollected = %d, want 2 (one daily entry and one token entry)", got)
	}

	// A late Record for the collected reservation changes nothing.
	beforeDaily, beforeBucket := snap(t, m), bucketTokens(t, tokens, "jev")
	if err := m.Record(ctx, *stale.Reservation, Usage{CostUSD: 0.4, InputTokens: 80}); err != nil {
		t.Fatal(err)
	}
	afterDaily := snap(t, m)
	if afterDaily.Calls != beforeDaily.Calls || math.Abs(afterDaily.USD-beforeDaily.USD) > 1e-9 || afterDaily.InputTokens != beforeDaily.InputTokens {
		t.Errorf("late record changed the counter: %+v -> %+v", beforeDaily, afterDaily)
	}
	if got := bucketTokens(t, tokens, "jev"); got != beforeBucket {
		t.Errorf("late record changed the bucket: %v -> %v", beforeBucket, got)
	}
	// The fresh one still reconciles normally.
	if err := m.Record(ctx, *fresh.Reservation, Usage{CostUSD: 0.1, InputTokens: 10}); err != nil {
		t.Fatal(err)
	}
	if d := snap(t, m); d.Calls != 1 || math.Abs(d.USD-0.6) > 1e-9 {
		t.Errorf("fresh record: %+v, want 0.5 (stale) + 0.1", d)
	}
}

func TestPendingEntriesInsideTheTTLAreNotCollected(t *testing.T) {
	m, c, _, _ := newManager(testCfg())
	first := mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0.1))
	c.Advance(14 * time.Minute)
	mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0.1))
	if m.StaleCollected() != 0 {
		t.Errorf("collected = %d, nothing is stale yet", m.StaleCollected())
	}
	if _, ok := snap(t, m).Pending[first.Reservation.ID]; !ok {
		t.Error("an entry inside the TTL must survive")
	}
}

func TestPendingIsBoundedByInFlightCalls(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	var held []Reservation
	for i := 0; i < 5; i++ {
		held = append(held, *mustCheck(t, m, req("mystery", PriorityDefault, 1, 0, 0.01)).Reservation)
	}
	for i := 0; i < 200; i++ {
		cycle(t, m, req("mystery", PriorityDefault, 1, 0, 0.01), Usage{CostUSD: 0.01})
	}
	if n := len(snap(t, m).Pending); n != 5 {
		t.Errorf("pending = %d, want only the 5 in-flight reservations", n)
	}
	for _, r := range held {
		if err := m.Record(context.Background(), r, Usage{CostUSD: 0.01}); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(snap(t, m).Pending); n != 0 {
		t.Errorf("pending = %d after everything completed", n)
	}
}

func TestUnknownReservationRecordIsANoop(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	cycle(t, m, req("jev", PriorityDefault, 100, 0, 0.1), Usage{CostUSD: 0.1, InputTokens: 100})
	before, tok := snap(t, m), bucketTokens(t, tokens, "jev")
	if err := m.Record(context.Background(), Reservation{ID: "made-up", API: "jev", Tokens: 500, USD: 3}, Usage{CostUSD: 3, InputTokens: 500}); err != nil {
		t.Fatal(err)
	}
	after := snap(t, m)
	if after.Calls != before.Calls || after.USD != before.USD || bucketTokens(t, tokens, "jev") != tok {
		t.Errorf("an unknown reservation changed state: %+v -> %+v", before, after)
	}
	// An empty id is unknown too.
	if err := m.Record(context.Background(), Reservation{API: "jev"}, Usage{CostUSD: 1}); err != nil {
		t.Fatal(err)
	}
	if snap(t, m).USD != before.USD {
		t.Error("an id-less record must not book spend")
	}
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }
