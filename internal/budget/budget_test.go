package budget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
)

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func testCfg() config.Budget {
	return config.Budget{
		Version:        1,
		DailyUSD:       10,
		ShedBriefAt:    0.8,
		ShedWhyAt:      0.9,
		ShedThematicAt: 0.95,
		PendingTTL:     config.Duration(15 * time.Minute),
		TokenBuckets: map[string]config.TokenBucket{
			"jev": {Capacity: 1000, RefillPerSecond: 10},
		},
	}
}

func newManager(cfg config.Budget) (*Manager, *clock, *MemoryStore, *MemoryStore) {
	c := &clock{now: t0}
	tokens, spend := NewMemoryStore(), NewMemoryStore()
	return New(tokens, spend, cfg).WithClock(c.Now), c, tokens, spend
}

func bucketTokens(t *testing.T, s Store, api string) float64 {
	t.Helper()
	raw, err := s.Get(context.Background(), "tokens."+api)
	if err != nil {
		t.Fatalf("read bucket: %v", err)
	}
	var b bucket
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	return b.Tokens
}

func req(api string, p Priority, in, out int64, usd float64) Request {
	return Request{API: api, Priority: p, EstimatedInputTokens: in, EstimatedOutputTokens: out, EstimatedUSD: usd}
}

func mustCheck(t *testing.T, m *Manager, r Request) Verdict {
	t.Helper()
	v, err := m.Check(context.Background(), r)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	return v
}

// seedDaily writes today's counter directly. It is test setup only: an unknown
// reservation is a no-op for Record by design, so spend that did not come from
// a Check has to be planted.
func seedDaily(t *testing.T, m *Manager, usd float64, calls, input, output int64) {
	t.Helper()
	now := m.now().UTC()
	raw, err := json.Marshal(Daily{Day: now.Format("2006-01-02"), USD: usd, Calls: calls, InputTokens: input, OutputTokens: output, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.spend.Put(context.Background(), "daily."+now.Format("2006-01-02"), raw); err != nil {
		t.Fatal(err)
	}
}

// cycle runs one Check followed by Record with the given actual usage.
func cycle(t *testing.T, m *Manager, r Request, u Usage) Reservation {
	t.Helper()
	v := mustCheck(t, m, r)
	if !v.Allowed {
		t.Fatalf("cycle check denied: %+v", v)
	}
	if err := m.Record(context.Background(), *v.Reservation, u); err != nil {
		t.Fatal(err)
	}
	return *v.Reservation
}

func snap(t *testing.T, m *Manager) Daily {
	t.Helper()
	d, err := m.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestTokenBucketStartsFullReservesAndDenies(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	v := mustCheck(t, m, req("jev", PriorityDefault, 400, 100, 0))
	if !v.Allowed || v.Reason != "ok" || v.DailyCapUSD != 10 || v.Reservation == nil {
		t.Fatalf("first check = %+v", v)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 500 {
		t.Errorf("tokens after reserving 500 = %v, want 500", got)
	}
	if v := mustCheck(t, m, req("jev", PriorityDefault, 300, 200, 0)); !v.Allowed {
		t.Fatalf("second check denied: %+v", v)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 0 {
		t.Errorf("tokens = %v, want 0", got)
	}
	v = mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0))
	if v.Allowed || v.Reason != "token-bucket" || v.Reservation != nil {
		t.Errorf("empty bucket verdict = %+v", v)
	}
}

func TestReservationFields(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	a := mustCheck(t, m, req("jev", PriorityDefault, 40, 10, 0.25)).Reservation
	b := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0)).Reservation
	if a.API != "jev" || a.Day != "2026-11-20" || a.Tokens != 50 || a.USD != 0.25 || a.ID == "" {
		t.Errorf("reservation = %+v", a)
	}
	if a.ID == b.ID {
		t.Error("reservation ids must be unique")
	}
}

func TestTokenBucketDenialDoesNotReserveTokensOrDollars(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	mustCheck(t, m, req("jev", PriorityDefault, 900, 0, 1))
	v := mustCheck(t, m, req("jev", PriorityDefault, 200, 0, 2))
	if v.Allowed || v.Reason != "token-bucket" {
		t.Fatalf("verdict = %+v", v)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 100 {
		t.Errorf("a denied call must leave tokens untouched, got %v", got)
	}
	if d := snap(t, m); math.Abs(d.USD-1) > 1e-9 {
		t.Errorf("a token-bucket denial must refund its dollars: spend = %v, want 1", d.USD)
	}
}

func TestTokenBucketRefillsAndCaps(t *testing.T) {
	m, c, _, _ := newManager(testCfg())
	mustCheck(t, m, req("jev", PriorityDefault, 1000, 0, 0)) // drain
	if v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0)); v.Allowed {
		t.Fatal("bucket should be empty")
	}
	c.Advance(5 * time.Second) // +50
	if v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0)); v.Allowed {
		t.Error("50 refilled tokens must not cover 100")
	}
	c.Advance(5 * time.Second) // 100 total
	if v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0)); !v.Allowed {
		t.Error("100 refilled tokens must cover 100")
	}
	c.Advance(24 * time.Hour)
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1000, 0, 0)); !v.Allowed {
		t.Error("a long idle bucket refills to capacity")
	}
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0)); v.Allowed {
		t.Error("refill must be capped at capacity (1000)")
	}
}

func TestRequestLargerThanCapacityIsDenied(t *testing.T) {
	m, c, _, _ := newManager(testCfg())
	c.Advance(time.Hour)
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1001, 0, 0)); v.Allowed || v.Reason != "token-bucket" {
		t.Errorf("verdict = %+v", v)
	}
}

func TestUnknownAPIHasNoBucketButDailyCapApplies(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	if v := mustCheck(t, m, req("mystery", PriorityDefault, 50_000_000, 50_000_000, 0)); !v.Allowed {
		t.Fatalf("an unconfigured API is not throttled: %+v", v)
	}
	seedDaily(t, m, 10, 1, 0, 0)
	v := mustCheck(t, m, req("mystery", PriorityDefault, 1, 0, 0))
	if v.Allowed || v.Reason != "daily-cap" {
		t.Errorf("the daily cap still applies: %+v", v)
	}
}

// Reviewer reproduction: two sequential $0.75 checks under a $1 cap.
func TestSequentialChecksReserveDollars(t *testing.T) {
	cfg := testCfg()
	cfg.DailyUSD = 1
	m, _, _, _ := newManager(cfg)
	first := mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0.75))
	if !first.Allowed {
		t.Fatalf("first = %+v", first)
	}
	second := mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0.75))
	if second.Allowed || second.Reason != "daily-cap" || second.Reservation != nil {
		t.Errorf("second must be denied: %+v", second)
	}
	if d := snap(t, m); math.Abs(d.USD-0.75) > 1e-9 {
		t.Errorf("persisted spend = %v, want 0.75", d.USD)
	}
	// Recording the actual cost reconciles the reservation, it does not double count.
	if err := m.Record(context.Background(), *first.Reservation, Usage{CostUSD: 0.5, InputTokens: 10}); err != nil {
		t.Fatal(err)
	}
	if d := snap(t, m); math.Abs(d.USD-0.5) > 1e-9 || d.Calls != 1 {
		t.Errorf("after record: %+v", d)
	}
	if v := mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0.5)); !v.Allowed {
		t.Errorf("reconciled spend frees headroom: %+v", v)
	}
}

func TestConcurrentRecordsSum(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	var wg sync.WaitGroup
	var reservations []Reservation
	for i := 0; i < 2; i++ {
		reservations = append(reservations, *mustCheck(t, m, req("jev", PriorityDefault, 5, 0, 0.10)).Reservation)
	}
	for _, res := range reservations {
		wg.Add(1)
		go func(res Reservation) {
			defer wg.Done()
			if err := m.Record(context.Background(), res, Usage{CostUSD: 0.10, InputTokens: 5}); err != nil {
				t.Error(err)
			}
		}(res)
	}
	wg.Wait()
	d := snap(t, m)
	if math.Abs(d.USD-0.20) > 1e-9 || d.Calls != 2 || d.InputTokens != 10 {
		t.Errorf("snapshot = %+v, want 0.20 over 2 calls", d)
	}
}

func TestConcurrentTokenChecksAdmitExactlyOne(t *testing.T) {
	cfg := testCfg()
	cfg.TokenBuckets["jev"] = config.TokenBucket{Capacity: 100, RefillPerSecond: 1}
	for round := 0; round < 30; round++ {
		m, _, _, _ := newManager(cfg)
		var wg sync.WaitGroup
		allowed := make(chan bool, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, err := m.Check(context.Background(), req("jev", PriorityDefault, 80, 0, 0))
				if err != nil {
					t.Error(err)
				}
				allowed <- v.Allowed
			}()
		}
		wg.Wait()
		close(allowed)
		n := 0
		for a := range allowed {
			if a {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("round %d: %d of 2 concurrent 80-token checks admitted against 100 capacity, want exactly 1", round, n)
		}
	}
}

func TestConcurrentDollarChecksNeverOvershoot(t *testing.T) {
	cfg := testCfg()
	cfg.DailyUSD = 1
	m, _, _, _ := newManager(cfg)
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := m.Check(context.Background(), req("mystery", PriorityDefault, 1, 0, 0.30))
			if err != nil {
				t.Error(err)
			}
			if v.Allowed {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted != 3 {
		t.Errorf("admitted %d × $0.30 under a $1 cap, want 3", admitted)
	}
	if d := snap(t, m); d.USD > 1+1e-9 {
		t.Errorf("spend %v exceeded the cap", d.USD)
	}
}

func TestZeroUsageRecordRefundsAndDuplicateIsNoop(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	ctx := context.Background()
	v := mustCheck(t, m, req("jev", PriorityDefault, 400, 100, 0.5))
	if d := snap(t, m); d.USD != 0.5 {
		t.Fatalf("reserved spend = %v", d.USD)
	}
	if err := m.Record(ctx, *v.Reservation, Usage{}); err != nil {
		t.Fatal(err)
	}
	if d := snap(t, m); d.USD != 0 {
		t.Errorf("a zero-usage record refunds the dollars: %v", d.USD)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 1000 {
		t.Errorf("a zero-usage record refunds the tokens: %v", got)
	}
	// A second Record for the same reservation changes nothing.
	before := snap(t, m)
	if err := m.Record(ctx, *v.Reservation, Usage{CostUSD: 9, InputTokens: 900}); err != nil {
		t.Fatal(err)
	}
	after := snap(t, m)
	if after.USD != before.USD || after.Calls != before.Calls || after.InputTokens != before.InputTokens {
		t.Errorf("duplicate record changed state: %+v -> %+v", before, after)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 1000 {
		t.Errorf("duplicate record changed the bucket: %v", got)
	}
}

func TestRecordReconcilesReservation(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	v := mustCheck(t, m, req("jev", PriorityDefault, 400, 100, 0)) // 500 left
	if err := m.Record(context.Background(), *v.Reservation, Usage{InputTokens: 100, OutputTokens: 50}); err != nil {
		t.Fatal(err)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 850 {
		t.Errorf("tokens after reconcile = %v, want 850 (1000 - 150 actual)", got)
	}
}

func TestRecordReconcileIsClamped(t *testing.T) {
	m, c, tokens, _ := newManager(testCfg())
	ctx := context.Background()
	// A refund can never lift the bucket above capacity, even after a long refill.
	v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0)) // 900 left
	c.Advance(10 * time.Minute)                                  // refills to capacity, inside the pending TTL
	if err := m.Record(ctx, *v.Reservation, Usage{}); err != nil {
		t.Fatal(err)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 1000 {
		t.Errorf("tokens must clamp to capacity, got %v", got)
	}
	// Usage far above the reservation cannot push it below zero.
	v = mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0))
	if err := m.Record(ctx, *v.Reservation, Usage{InputTokens: 5000}); err != nil {
		t.Fatal(err)
	}
	if got := bucketTokens(t, tokens, "jev"); got != 0 {
		t.Errorf("tokens must clamp to zero, got %v", got)
	}
}

func TestRecordAccumulatesAndSnapshot(t *testing.T) {
	m, c, _, _ := newManager(testCfg())
	for i := 0; i < 3; i++ {
		cycle(t, m, req("jev", PriorityDefault, 1, 0, 0.1), Usage{CostUSD: 0.25, InputTokens: 100, OutputTokens: 10})
		c.Advance(time.Second)
	}
	d := snap(t, m)
	if d.Day != "2026-11-20" || d.Calls != 3 || d.InputTokens != 300 || d.OutputTokens != 30 || math.Abs(d.USD-0.75) > 1e-9 {
		t.Errorf("snapshot = %+v", d)
	}
	if len(d.Pending) != 0 {
		t.Errorf("pending must be empty once every call is recorded: %v", d.Pending)
	}
	if !d.UpdatedAt.Equal(t0.Add(2 * time.Second)) {
		t.Errorf("updated_at = %v", d.UpdatedAt)
	}
}

func TestRecordUsesReservationDay(t *testing.T) {
	ctx := context.Background()
	c := &clock{now: time.Date(2026, 11, 20, 23, 59, 30, 0, time.UTC)}
	spend := NewMemoryStore()
	m := New(NewMemoryStore(), spend, testCfg()).WithClock(c.Now)
	v := mustCheck(t, m, req("mystery", PriorityDefault, 1, 0, 1))
	c.now = time.Date(2026, 11, 21, 0, 0, 30, 0, time.UTC)
	if err := m.Record(ctx, *v.Reservation, Usage{CostUSD: 0.4}); err != nil {
		t.Fatal(err)
	}
	raw, err := spend.Get(ctx, "daily.2026-11-20")
	if err != nil {
		t.Fatal(err)
	}
	var old Daily
	_ = json.Unmarshal(raw, &old)
	if math.Abs(old.USD-0.4) > 1e-9 {
		t.Errorf("the reservation's own day is reconciled: %+v", old)
	}
	if d := snap(t, m); d.Day != "2026-11-21" || d.USD != 0 {
		t.Errorf("the new day is untouched: %+v", d)
	}
}

func TestDailyCapBoundaries(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	ctx := context.Background()
	seedDaily(t, m, 9.5, 1, 0, 0)
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0.6)); v.Allowed || v.Reason != "daily-cap" {
		t.Errorf("9.5+0.6 > 10 must be denied: %+v", v)
	}
	v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0.5))
	if !v.Allowed {
		t.Fatalf("9.5+0.5 == 10 is allowed: %+v", v)
	}
	// The reservation itself fills the cap.
	if d := snap(t, m); d.USD != 10 {
		t.Errorf("reserved spend = %v", d.USD)
	}
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0)); v.Allowed || v.Reason != "daily-cap" || v.ShedLevel != 3 {
		t.Errorf("at the cap everything is denied: %+v", v)
	}
	if err := m.Record(ctx, *v.Reservation, Usage{CostUSD: 0.5}); err != nil {
		t.Fatal(err)
	}
	seedDaily(t, m, 13, 2, 0, 0) // an in-flight overshoot
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0)); v.Allowed {
		t.Error("spend above the cap must be denied")
	}
}

func TestZeroDailyCapDeniesEverything(t *testing.T) {
	cfg := testCfg()
	cfg.DailyUSD = 0
	m, _, _, _ := newManager(cfg)
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1, 0, 0)); v.Allowed || v.Reason != "daily-cap" {
		t.Errorf("verdict = %+v", v)
	}
	if m.ShedLevel(0) != 3 {
		t.Error("a zero cap sheds everything")
	}
}

func TestShedLevelAndSheds(t *testing.T) {
	m, _, _, _ := newManager(testCfg())
	levels := []struct {
		ratio float64
		level int
	}{{0, 0}, {0.79, 0}, {0.8, 1}, {0.89, 1}, {0.9, 2}, {0.94, 2}, {0.95, 3}, {0.99, 3}, {1, 3}, {1.5, 3}}
	for _, c := range levels {
		if got := m.ShedLevel(c.ratio); got != c.level {
			t.Errorf("ShedLevel(%v) = %d, want %d", c.ratio, got, c.level)
		}
	}
	ratios := [5]float64{0.5, 0.8, 0.9, 0.95, 1.0}
	rows := []struct {
		p    Priority
		want [5]bool
	}{
		{PriorityBrief, [5]bool{false, true, true, true, true}},
		{PriorityWhy, [5]bool{false, false, true, true, true}},
		{PriorityThematic, [5]bool{false, false, false, true, true}},
		{PriorityDefault, [5]bool{false, false, false, false, false}},
		{Priority("unknown"), [5]bool{false, false, false, false, false}},
	}
	for _, r := range rows {
		for i, ratio := range ratios {
			if got := m.Sheds(r.p, ratio); got != r.want[i] {
				t.Errorf("Sheds(%s, %v) = %v, want %v", r.p, ratio, got, r.want[i])
			}
		}
	}
}

func TestCheckAppliesShedOrderBeforeTheCap(t *testing.T) {
	type tc struct {
		spend   float64
		allowed map[Priority]bool
	}
	all := func(brief, why, thematic, def bool) map[Priority]bool {
		return map[Priority]bool{PriorityBrief: brief, PriorityWhy: why, PriorityThematic: thematic, PriorityDefault: def}
	}
	cases := []tc{
		{7.9, all(true, true, true, true)},
		{8, all(false, true, true, true)},
		{8.9, all(false, true, true, true)},
		{9, all(false, false, true, true)},
		{9.4, all(false, false, true, true)},
		{9.5, all(false, false, false, true)}, // thematic is reachable before the cap
		{9.99, all(false, false, false, true)},
		{10, all(false, false, false, false)},
	}
	for _, c := range cases {
		for p, want := range c.allowed {
			m, _, _, _ := newManager(testCfg())
			seedDaily(t, m, c.spend, 1, 0, 0)
			v := mustCheck(t, m, req("mystery", p, 1, 0, 0))
			if v.Allowed != want {
				t.Errorf("spend %v priority %s: allowed = %v (%s), want %v", c.spend, p, v.Allowed, v.Reason, want)
			}
			if !v.Allowed && c.spend < 10 && v.Reason != "shed:"+string(p) {
				t.Errorf("spend %v priority %s: reason = %q", c.spend, p, v.Reason)
			}
			if !v.Allowed && c.spend >= 10 && v.Reason != "daily-cap" {
				t.Errorf("at the cap the reason is daily-cap, got %q", v.Reason)
			}
		}
	}
}

func TestShedDoesNotReserve(t *testing.T) {
	m, _, tokens, _ := newManager(testCfg())
	cycle(t, m, req("jev", PriorityDefault, 100, 0, 0), Usage{InputTokens: 100}) // creates the bucket document
	seedDaily(t, m, 8.5, 1, 0, 0)
	before := bucketTokens(t, tokens, "jev")
	if v := mustCheck(t, m, req("jev", PriorityBrief, 500, 0, 0.1)); v.Allowed {
		t.Fatal("brief must be shed")
	}
	if got := bucketTokens(t, tokens, "jev"); got != before {
		t.Errorf("a shed call must not touch the token bucket: %v -> %v", before, got)
	}
	if d := snap(t, m); math.Abs(d.USD-8.5) > 1e-9 {
		t.Errorf("a shed call must not reserve dollars: %v", d.USD)
	}
}

func TestDailySpendPersistsByUTCDay(t *testing.T) {
	ctx := context.Background()
	c := &clock{now: time.Date(2026, 11, 20, 23, 59, 0, 0, time.UTC)}
	tokens, spend := NewMemoryStore(), NewMemoryStore()
	m := New(tokens, spend, testCfg()).WithClock(c.Now)
	seedDaily(t, m, 4, 1, 0, 0)
	if _, err := spend.Get(ctx, "daily.2026-11-20"); err != nil {
		t.Fatalf("expected key daily.2026-11-20: %v", err)
	}
	c.now = time.Date(2026, 11, 20, 23, 59, 59, 0, time.UTC)
	if d := snap(t, m); d.USD != 4 {
		t.Errorf("spend within the day = %v", d.USD)
	}
	m2 := New(tokens, spend, testCfg()).WithClock(c.Now)
	if d := snap(t, m2); d.USD != 4 || d.Calls != 1 {
		t.Errorf("restart lost spend: %+v", d)
	}
	c.now = time.Date(2026, 11, 21, 0, 0, 1, 0, time.UTC)
	if d := snap(t, m2); d.USD != 0 || d.Calls != 0 || d.Day != "2026-11-21" {
		t.Errorf("new day = %+v", d)
	}
	seedDaily(t, m2, 1, 1, 0, 0)
	for key, want := range map[string]float64{"daily.2026-11-20": 4, "daily.2026-11-21": 1} {
		raw, err := spend.Get(ctx, key)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		var day Daily
		if err := json.Unmarshal(raw, &day); err != nil || day.USD != want {
			t.Errorf("%s = %+v (%v), want %v", key, day, err, want)
		}
	}
}

func TestDayIsUTCNotLocal(t *testing.T) {
	zone := time.FixedZone("UTC+5", 5*3600)
	c := &clock{now: time.Date(2026, 11, 21, 2, 0, 0, 0, zone)} // 21:00 UTC on the 20th
	spend := NewMemoryStore()
	m := New(NewMemoryStore(), spend, testCfg()).WithClock(c.Now)
	seedDaily(t, m, 1, 1, 0, 0)
	if _, err := spend.Get(context.Background(), "daily.2026-11-20"); err != nil {
		t.Errorf("expected the UTC day key: %v", err)
	}
}

func TestNewDayClearsCapAndShedding(t *testing.T) {
	m, c, _, _ := newManager(testCfg())
	seedDaily(t, m, 10, 1, 0, 0)
	if v := mustCheck(t, m, req("mystery", PriorityDefault, 1, 0, 0)); v.Allowed {
		t.Fatal("capped today")
	}
	c.now = time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	if v := mustCheck(t, m, req("mystery", PriorityBrief, 1, 0, 0)); !v.Allowed || v.ShedLevel != 0 {
		t.Errorf("a new day clears the cap and shedding: %+v", v)
	}
}

type failingStore struct{ err error }

func (f failingStore) Get(context.Context, string) ([]byte, error) { return nil, f.err }
func (f failingStore) Put(context.Context, string, []byte) error   { return f.err }
func (f failingStore) Update(context.Context, string, func([]byte, bool) ([]byte, error)) error {
	return f.err
}

func TestStoreErrorsPropagate(t *testing.T) {
	boom := errors.New("boom")
	ctx := context.Background()
	m := New(NewMemoryStore(), failingStore{boom}, testCfg())
	if _, err := m.Check(ctx, req("jev", PriorityDefault, 1, 0, 0)); !errors.Is(err, boom) {
		t.Errorf("check err = %v", err)
	}
	if err := m.Record(ctx, Reservation{ID: "x", API: "jev"}, Usage{}); !errors.Is(err, boom) {
		t.Errorf("record err = %v", err)
	}
	if _, err := m.Snapshot(ctx); !errors.Is(err, boom) {
		t.Errorf("snapshot err = %v", err)
	}
	m = New(failingStore{boom}, NewMemoryStore(), testCfg())
	if _, err := m.Check(ctx, req("jev", PriorityDefault, 1, 0, 0)); !errors.Is(err, boom) {
		t.Errorf("token store err = %v", err)
	}
}

func TestCorruptStoredValueIsAnError(t *testing.T) {
	ctx := context.Background()
	spend := NewMemoryStore()
	_ = spend.Put(ctx, "daily.2026-11-20", []byte("{not json"))
	m := New(NewMemoryStore(), spend, testCfg()).WithClock((&clock{now: t0}).Now)
	if _, err := m.Check(ctx, req("jev", PriorityDefault, 1, 0, 0)); err == nil {
		t.Error("corrupt daily spend must not be silently treated as zero")
	}
	if _, err := m.Snapshot(ctx); err == nil {
		t.Error("snapshot of corrupt spend must error")
	}
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	in := []byte("abc")
	_ = s.Put(ctx, "k", in)
	in[0] = 'z'
	out, _ := s.Get(ctx, "k")
	if string(out) != "abc" {
		t.Errorf("store aliased the input: %q", out)
	}
	out[0] = 'y'
	if again, _ := s.Get(ctx, "k"); string(again) != "abc" {
		t.Errorf("store aliased the output: %q", again)
	}
	if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing key err = %v", err)
	}
}

func TestMemoryStoreUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.Update(ctx, "k", func(old []byte, exists bool) ([]byte, error) {
		if exists || len(old) != 0 {
			t.Errorf("first update sees exists=%v old=%q", exists, old)
		}
		return []byte("1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	// ErrAbort and callback errors write nothing.
	if err := s.Update(ctx, "k", func([]byte, bool) ([]byte, error) { return []byte("2"), ErrAbort }); !errors.Is(err, ErrAbort) {
		t.Errorf("err = %v", err)
	}
	boom := errors.New("boom")
	if err := s.Update(ctx, "k", func([]byte, bool) ([]byte, error) { return []byte("3"), boom }); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
	if got, _ := s.Get(ctx, "k"); string(got) != "1" {
		t.Errorf("value = %q, want 1", got)
	}
	// Concurrent increments lose nothing.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Update(ctx, "n", func(old []byte, exists bool) ([]byte, error) {
				n := 0
				if exists {
					fmt.Sscan(string(old), &n)
				}
				return []byte(fmt.Sprint(n + 1)), nil
			})
		}()
	}
	wg.Wait()
	if got, _ := s.Get(ctx, "n"); string(got) != "50" {
		t.Errorf("counter = %q, want 50", got)
	}
}

func TestNewMemory(t *testing.T) {
	m := NewMemory(testCfg())
	if v := mustCheck(t, m, req("jev", PriorityDefault, 10, 0, 0)); !v.Allowed {
		t.Errorf("verdict = %+v", v)
	}
}
