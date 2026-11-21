package fetch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// tryTake reports whether a token was available immediately: the clock is
// frozen, so a Take that would have to wait only returns on ctx expiry.
func tryTake(t *testing.T, b *Budget, host string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := b.Take(ctx, host)
	if err == nil {
		return true
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Take: %v", err)
	}
	return false
}

func TestBudgetKVBurstAndRefill(t *testing.T) {
	kv := newFakeKV()
	clock := newFakeClock()
	b := NewBudget(BudgetOptions{KV: kv, Rate: 1, Burst: 3, Now: clock.Now})

	for i := 0; i < 3; i++ {
		if !tryTake(t, b, "a.example") {
			t.Fatalf("burst token %d unavailable", i+1)
		}
	}
	if tryTake(t, b, "a.example") {
		t.Fatal("bucket should be empty after the burst")
	}
	// Another host has its own bucket.
	if !tryTake(t, b, "b.example") {
		t.Fatal("hosts must not share a bucket")
	}

	clock.Advance(2 * time.Second) // refills two tokens
	if !tryTake(t, b, "a.example") || !tryTake(t, b, "a.example") {
		t.Fatal("two tokens should have refilled")
	}
	if tryTake(t, b, "a.example") {
		t.Fatal("only two tokens were refilled")
	}

	clock.Advance(time.Hour) // refill is capped at the burst size
	for i := 0; i < 3; i++ {
		if !tryTake(t, b, "a.example") {
			t.Fatalf("token %d after a long idle", i+1)
		}
	}
	if tryTake(t, b, "a.example") {
		t.Fatal("refill must be capped at burst")
	}
	if _, ok := kv.raw(BudgetKey("a.example")); !ok {
		t.Errorf("bucket not stored under %s", BudgetKey("a.example"))
	}
	if _, ok := kv.raw("host:a.example:8080"); ok {
		t.Error("a host with a port must be sanitized, not stored raw")
	}
	// The real bucket rejects ':', so the sanitized key must be legal.
	if err := checkKey(BudgetKey("a.example:8080")); err != nil {
		t.Errorf("BudgetKey produced an invalid key: %v", err)
	}
}

func TestBudgetKVSharedBetweenReplicas(t *testing.T) {
	kv := newFakeKV()
	clock := newFakeClock()
	r1 := NewBudget(BudgetOptions{KV: kv, Rate: 1, Burst: 2, Now: clock.Now})
	r2 := NewBudget(BudgetOptions{KV: kv, Rate: 1, Burst: 2, Now: clock.Now})
	if !tryTake(t, r1, "h") || !tryTake(t, r2, "h") {
		t.Fatal("two tokens expected across replicas")
	}
	if tryTake(t, r1, "h") || tryTake(t, r2, "h") {
		t.Fatal("replicas must share one allowance")
	}
}

func TestBudgetLocalFallbackWithoutKV(t *testing.T) {
	clock := newFakeClock()
	b := NewBudget(BudgetOptions{Rate: 2, Burst: 2, Now: clock.Now})
	if !tryTake(t, b, "h") || !tryTake(t, b, "h") {
		t.Fatal("burst unavailable")
	}
	if tryTake(t, b, "h") {
		t.Fatal("local limiter must limit without a KV")
	}
	clock.Advance(500 * time.Millisecond) // 2 tokens/s -> one token
	if !tryTake(t, b, "h") {
		t.Fatal("local bucket did not refill")
	}
	if tryTake(t, b, "h") {
		t.Fatal("only one token was refilled")
	}
}

func TestBudgetKVFailureFallsBackToLocal(t *testing.T) {
	kv := newFakeKV()
	kv.getErr = errors.New("kv down")
	clock := newFakeClock()
	b := NewBudget(BudgetOptions{KV: kv, Rate: 1, Burst: 2, Now: clock.Now})
	if !tryTake(t, b, "h") || !tryTake(t, b, "h") {
		t.Fatal("fallback burst unavailable")
	}
	if tryTake(t, b, "h") {
		t.Fatal("a broken KV must not remove the rate limit")
	}
}

func TestBudgetNilAndCancelledContext(t *testing.T) {
	var b *Budget
	if err := b.Take(context.Background(), "h"); err != nil {
		t.Fatalf("nil budget must not throttle: %v", err)
	}
	clock := newFakeClock()
	local := NewBudget(BudgetOptions{Rate: 1, Burst: 1, Now: clock.Now})
	if !tryTake(t, local, "h") {
		t.Fatal("first token")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := local.Take(ctx, "h"); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled Take = %v", err)
	}
}

func TestBudgetConcurrentTakesNeverExceedBurst(t *testing.T) {
	kv := newFakeKV()
	clock := newFakeClock()
	b := NewBudget(BudgetOptions{KV: kv, Rate: 1, Burst: 4, Now: clock.Now})
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if b.Take(ctx, "h") == nil {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != 4 {
		t.Fatalf("granted %d tokens with a frozen clock, want exactly the burst of 4", granted)
	}
}
