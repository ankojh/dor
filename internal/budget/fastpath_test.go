package budget

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
)

// countingStore wraps a Store and counts writes. The token-bucket fast path
// must answer a denial from a read alone, so a denied Check must not Update or
// Put either store.
type countingStore struct {
	Store
	gets    atomic.Int64
	puts    atomic.Int64
	updates atomic.Int64
}

func (s *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, key)
}

func (s *countingStore) Put(ctx context.Context, key string, value []byte) error {
	s.puts.Add(1)
	return s.Store.Put(ctx, key, value)
}

func (s *countingStore) Update(ctx context.Context, key string, fn func([]byte, bool) ([]byte, error)) error {
	s.updates.Add(1)
	return s.Store.Update(ctx, key, fn)
}

func (s *countingStore) writes() int64 { return s.puts.Load() + s.updates.Load() }

func newCountingManager(cfg config.Budget) (*Manager, *clock, *countingStore, *countingStore) {
	c := &clock{now: t0}
	tokens := &countingStore{Store: NewMemoryStore()}
	spend := &countingStore{Store: NewMemoryStore()}
	return New(tokens, spend, cfg).WithClock(c.Now), c, tokens, spend
}

// TestTokenBucketFastPathDeniesWithoutWrites drains a bucket, then checks that
// a call the bucket cannot cover is denied with no write to either store. After
// enough clock advance the refill admits the call, which still reserves and
// reconciles normally.
func TestTokenBucketFastPathDeniesWithoutWrites(t *testing.T) {
	ctx := context.Background()
	m, c, tokens, spend := newCountingManager(testCfg())

	// Drain the 1000-token bucket through the normal path so the document
	// exists and is stored at t0.
	if v := mustCheck(t, m, req("jev", PriorityDefault, 1000, 0, 0)); !v.Allowed {
		t.Fatalf("drain check = %+v", v)
	}
	beforeSpendWrites := spend.writes()
	beforeTokenWrites := tokens.writes()
	if beforeSpendWrites == 0 || beforeTokenWrites == 0 {
		t.Fatalf("the admitted drain should write both stores: spend=%d tokens=%d", beforeSpendWrites, beforeTokenWrites)
	}

	v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.25))
	if v.Allowed || v.Reason != "token-bucket" || v.Reservation != nil {
		t.Fatalf("denied verdict = %+v", v)
	}
	if got := spend.writes(); got != beforeSpendWrites {
		t.Errorf("a token-bucket denial wrote the spend store: %d -> %d", beforeSpendWrites, got)
	}
	if got := tokens.writes(); got != beforeTokenWrites {
		t.Errorf("a token-bucket denial wrote the token store: %d -> %d", beforeTokenWrites, got)
	}
	if d := snap(t, m); math.Abs(d.USD) > 1e-9 || d.Calls != 0 {
		t.Errorf("a denied call must not reserve dollars: %+v", d)
	}

	// 5 s refills 50 tokens: still short of 100, still a read-only denial.
	c.Advance(5 * time.Second)
	if v := mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0)); v.Allowed || v.Reason != "token-bucket" {
		t.Errorf("50 refilled tokens must not cover 100: %+v", v)
	}
	if got := spend.writes(); got != beforeSpendWrites {
		t.Errorf("the second denial wrote the spend store: %d -> %d", beforeSpendWrites, got)
	}

	// 10 s refills 100 tokens: the fast path falls through and the CAS path
	// admits with a reservation.
	c.Advance(5 * time.Second)
	v = mustCheck(t, m, req("jev", PriorityDefault, 100, 0, 0.25))
	if !v.Allowed || v.Reason != "ok" || v.Reservation == nil {
		t.Fatalf("refilled check = %+v", v)
	}
	if v.Reservation.Tokens != 100 || v.Reservation.API != "jev" {
		t.Errorf("reservation = %+v", v.Reservation)
	}
	if d := snap(t, m); math.Abs(d.USD-0.25) > 1e-9 {
		t.Errorf("an admitted check reserves its dollars: %v", d.USD)
	}

	// Record reconciles the reservation: 40 actual tokens leave 60 in the
	// bucket, and the dollar counter becomes the actual cost.
	if err := m.Record(ctx, *v.Reservation, Usage{CostUSD: 0.10, InputTokens: 40}); err != nil {
		t.Fatal(err)
	}
	if got := bucketTokens(t, tokens.Store, "jev"); got != 60 {
		t.Errorf("tokens after reconcile = %v, want 60 (100 reserved, 40 actual)", got)
	}
	d := snap(t, m)
	if math.Abs(d.USD-0.10) > 1e-9 || d.Calls != 1 || d.InputTokens != 40 {
		t.Errorf("snapshot after record = %+v", d)
	}
}

// A missing bucket key is not short: the CAS path creates a full bucket.
func TestTokenBucketFastPathMissingKeyAdmitted(t *testing.T) {
	ctx := context.Background()
	m, _, tokens, _ := newCountingManager(testCfg())
	v, err := m.Check(ctx, req("jev", PriorityDefault, 10, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !v.Allowed || v.Reservation == nil {
		t.Fatalf("a missing bucket starts full: %+v", v)
	}
	raw, err := tokens.Get(ctx, "tokens.jev")
	if err != nil {
		t.Fatalf("the bucket document was not created: %v", err)
	}
	var b bucket
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.Tokens != 990 {
		t.Errorf("tokens = %v, want 990", b.Tokens)
	}
}

// A corrupt bucket value must not be decoded by the fast path. It falls
// through to the CAS path, which reports the decode error instead of panicking
// or treating the bucket as short.
func TestTokenBucketFastPathCorruptValueFallsThrough(t *testing.T) {
	ctx := context.Background()
	m, _, tokens, _ := newCountingManager(testCfg())
	if err := tokens.Put(ctx, "tokens.jev", []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("corrupt bucket panicked: %v", r)
		}
	}()
	v, err := m.Check(ctx, req("jev", PriorityDefault, 1, 0, 0))
	if v.Reason == "token-bucket" {
		t.Fatalf("the fast path must not deny on a corrupt bucket: %+v", v)
	}
	if err == nil && !v.Allowed {
		t.Fatalf("a corrupt bucket that does not error must fall through to admission: %+v", v)
	}
}

// retryBackoff is full jitter in [d/2, d] with d = base << min(attempt, 5), so
// it never exceeds 32*base.
func TestRetryBackoffBounds(t *testing.T) {
	for _, base := range []time.Duration{time.Millisecond, 10 * time.Millisecond, time.Second} {
		for attempt := 0; attempt <= 12; attempt++ {
			d := base << min(attempt, 5)
			if d > 32*base {
				t.Fatalf("attempt %d: d = %v exceeds 32*base = %v", attempt, d, 32*base)
			}
			for i := 0; i < 200; i++ {
				got := retryBackoff(base, attempt)
				if got < d/2 || got > d {
					t.Fatalf("attempt %d: retryBackoff(%v) = %v, want [%v, %v]", attempt, base, got, d/2, d)
				}
				if got > 32*base {
					t.Fatalf("attempt %d: retryBackoff(%v) = %v exceeds 32*base", attempt, base, got)
				}
			}
		}
		// Attempt 0 is in [base/2, base].
		for i := 0; i < 500; i++ {
			got := retryBackoff(base, 0)
			if got < base/2 || got > base {
				t.Fatalf("attempt 0: retryBackoff(%v) = %v, want [%v, %v]", base, got, base/2, base)
			}
		}
	}
}
