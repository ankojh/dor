package budget_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/bus"
)

func connect(t *testing.T, dir string) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   dir,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	return b
}

func stores(t *testing.T, b *bus.Bus) (tokens, spend *budget.KVStore) {
	t.Helper()
	ctx := context.Background()
	tk, err := b.KV(ctx, bus.KVBucketBudgets)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := b.KV(ctx, bus.KVBucketSpend)
	if err != nil {
		t.Fatal(err)
	}
	ts, ss := budget.NewKVStore(tk), budget.NewKVStore(sp)
	// Heavy contention in these tests: allow many CAS attempts.
	ts.MaxRetries, ss.MaxRetries = 1000, 1000
	return ts, ss
}

func cfg() config.Budget {
	return config.Budget{
		Version: 1, DailyUSD: 10, ShedBriefAt: 0.8, ShedWhyAt: 0.9, ShedThematicAt: 0.95,
		TokenBuckets: map[string]config.TokenBucket{"jev": {Capacity: 100, RefillPerSecond: 0.001}},
	}
}

func TestKVStorePutGetMissing(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	_, spend := stores(t, b)
	ctx := context.Background()
	if _, err := spend.Get(ctx, "nope"); !errors.Is(err, budget.ErrNotFound) {
		t.Fatalf("missing key err = %v, want ErrNotFound", err)
	}
	if err := spend.Put(ctx, "daily.2026-11-20", []byte(`{"usd":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := spend.Get(ctx, "daily.2026-11-20")
	if err != nil || string(got) != `{"usd":1}` {
		t.Errorf("get = %q, %v", got, err)
	}
}

func TestKVStoreUpdateSemantics(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	_, s := stores(t, b)
	ctx := context.Background()
	if err := s.Update(ctx, "k", func(old []byte, exists bool) ([]byte, error) {
		if exists || len(old) != 0 {
			t.Errorf("first update: exists=%v old=%q", exists, old)
		}
		return []byte("1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, "k", func(old []byte, exists bool) ([]byte, error) {
		if !exists || string(old) != "1" {
			t.Errorf("second update: exists=%v old=%q", exists, old)
		}
		return []byte("2"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, "k", func([]byte, bool) ([]byte, error) { return []byte("9"), budget.ErrAbort }); !errors.Is(err, budget.ErrAbort) {
		t.Errorf("err = %v", err)
	}
	if got, _ := s.Get(ctx, "k"); string(got) != "2" {
		t.Errorf("an aborted update wrote %q", got)
	}
}

func TestKVStoreConcurrentUpdatesAreAtomic(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	_, s := stores(t, b)
	ctx := context.Background()
	var wg sync.WaitGroup
	const n = 12
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Update(ctx, "counter", func(old []byte, exists bool) ([]byte, error) {
				v := 0
				if exists {
					for _, c := range old {
						v = v*10 + int(c-'0')
					}
				}
				v++
				return []byte(itoa(v)), nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got, _ := s.Get(ctx, "counter"); string(got) != itoa(n) {
		t.Errorf("counter = %q, want %d (lost updates)", got, n)
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

func TestSpendBucketTTL(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	ctx := context.Background()
	kv, err := b.KV(ctx, bus.KVBucketSpend)
	if err != nil {
		t.Fatal(err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.TTL() != 48*time.Hour || status.TTL() != bus.KVSpendTTL {
		t.Errorf("spend ttl = %s, want 48h", status.TTL())
	}
}

func TestTwoManagersOneKVSumRecordsAndAdmitOnce(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	ctx := context.Background()
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	t1, s1 := stores(t, b)
	t2, s2 := stores(t, b)
	m1 := budget.New(t1, s1, cfg()).WithClock(clock)
	m2 := budget.New(t2, s2, cfg()).WithClock(clock)

	// Concurrent checks, then concurrent records, across both managers sum exactly (CAS, no lost update).
	var wg sync.WaitGroup
	const perManager = 5
	type held struct {
		m   *budget.Manager
		res budget.Reservation
	}
	var mu sync.Mutex
	var reservations []held
	for _, m := range []*budget.Manager{m1, m2} {
		for i := 0; i < perManager; i++ {
			wg.Add(1)
			go func(m *budget.Manager) {
				defer wg.Done()
				v, err := m.Check(ctx, budget.Request{API: "mystery", EstimatedInputTokens: 7, EstimatedUSD: 0.10})
				if err != nil || !v.Allowed {
					t.Errorf("check: %+v %v", v, err)
					return
				}
				mu.Lock()
				reservations = append(reservations, held{m, *v.Reservation})
				mu.Unlock()
			}(m)
		}
	}
	wg.Wait()
	for _, h := range reservations {
		wg.Add(1)
		go func(h held) {
			defer wg.Done()
			if err := h.m.Record(ctx, h.res, budget.Usage{CostUSD: 0.10, InputTokens: 7}); err != nil {
				t.Error(err)
			}
		}(h)
	}
	wg.Wait()
	d, err := m1.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := 0.10 * 2 * perManager; math.Abs(d.USD-want) > 1e-9 || d.Calls != 2*perManager || d.InputTokens != 7*2*perManager {
		t.Errorf("snapshot = %+v, want %.2f over %d calls", d, want, 2*perManager)
	}
	if d2, _ := m2.Snapshot(ctx); d2.USD != d.USD || d2.Calls != d.Calls {
		t.Errorf("managers disagree: %+v vs %+v", d, d2)
	}

	// Two managers racing for the last tokens of a 100-token bucket: exactly one wins.
	results := make(chan bool, 2)
	for _, m := range []*budget.Manager{m1, m2} {
		wg.Add(1)
		go func(m *budget.Manager) {
			defer wg.Done()
			v, err := m.Check(ctx, budget.Request{API: "jev", EstimatedInputTokens: 80})
			if err != nil {
				t.Error(err)
			}
			results <- v.Allowed
		}(m)
	}
	wg.Wait()
	close(results)
	won := 0
	for r := range results {
		if r {
			won++
		}
	}
	if won != 1 {
		t.Errorf("%d of 2 managers admitted 80 tokens from a 100 bucket, want exactly 1", won)
	}
}

func TestDollarReservationsAcrossManagers(t *testing.T) {
	b := connect(t, t.TempDir())
	defer b.Close()
	ctx := context.Background()
	c := cfg()
	c.DailyUSD = 1
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	t1, s1 := stores(t, b)
	t2, s2 := stores(t, b)
	m1 := budget.New(t1, s1, c).WithClock(clock)
	m2 := budget.New(t2, s2, c).WithClock(clock)

	v1, err := m1.Check(ctx, budget.Request{API: "mystery", EstimatedInputTokens: 1, EstimatedUSD: 0.75})
	if err != nil || !v1.Allowed {
		t.Fatalf("first = %+v %v", v1, err)
	}
	v2, err := m2.Check(ctx, budget.Request{API: "mystery", EstimatedInputTokens: 1, EstimatedUSD: 0.75})
	if err != nil || v2.Allowed || v2.Reason != "daily-cap" {
		t.Errorf("the second manager must see the first's reservation: %+v %v", v2, err)
	}
	if d, _ := m2.Snapshot(ctx); math.Abs(d.USD-0.75) > 1e-9 {
		t.Errorf("persisted spend = %v, want 0.75", d.USD)
	}
}

func TestDailySpendSurvivesBusRestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ctx := context.Background()

	b1 := connect(t, dir)
	tokens, spend := stores(t, b1)
	m1 := budget.New(tokens, spend, cfg()).WithClock(clock)
	v, err := m1.Check(ctx, budget.Request{API: "jev", EstimatedInputTokens: 50, EstimatedUSD: 1})
	if err != nil || !v.Allowed {
		t.Fatalf("check: %+v %v", v, err)
	}
	if err := m1.Record(ctx, *v.Reservation, budget.Usage{CostUSD: 3.5, InputTokens: 90, OutputTokens: 5}); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	b2 := connect(t, dir)
	defer b2.Close()
	tokens2, spend2 := stores(t, b2)
	m2 := budget.New(tokens2, spend2, cfg()).WithClock(clock)
	d, err := m2.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(d.USD-3.5) > 1e-9 || d.Calls != 1 || d.InputTokens != 90 || d.OutputTokens != 5 {
		t.Errorf("spend lost across restart: %+v", d)
	}
	// The recorded-id set survives too, so a replayed Record stays a no-op.
	if err := m2.Record(ctx, *v.Reservation, budget.Usage{CostUSD: 9}); err != nil {
		t.Fatal(err)
	}
	if d, _ := m2.Snapshot(ctx); math.Abs(d.USD-3.5) > 1e-9 || d.Calls != 1 {
		t.Errorf("duplicate record after restart changed spend: %+v", d)
	}
	now = now.Add(10 * time.Hour)
	late, err := m2.Check(ctx, budget.Request{API: "jev", EstimatedInputTokens: 1, EstimatedUSD: 6.5})
	if err != nil || !late.Allowed {
		t.Fatalf("late check: %+v %v", late, err)
	}
	if err := m2.Record(ctx, *late.Reservation, budget.Usage{CostUSD: 6.5}); err != nil {
		t.Fatal(err)
	}
	if v, _ := m2.Check(ctx, budget.Request{API: "jev", EstimatedInputTokens: 1}); v.Allowed || v.Reason != "daily-cap" {
		t.Errorf("cap should hold after restart: %+v", v)
	}
}
