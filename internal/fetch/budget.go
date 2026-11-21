package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// budgetKVTimeout bounds one KV round trip. A KV outage must never hold a fetch
// slot for longer than this; the caller falls back to a local limiter.
const budgetKVTimeout = 500 * time.Millisecond

// Budget is a per-host token bucket shared by every replica through the
// `budgets` KV bucket (key "host.<host>"). It keeps the fetchers polite: at
// most Rate requests per second per host with a Burst allowance. The KV is
// shared so three RSS replicas do not each get their own allowance.
//
// If the KV is unavailable the bucket falls back to an in-process limiter
// rather than stopping ingest; the fallback is logged once per host per
// fallbackWarnInterval so a KV outage is visible instead of silent.
type Budget struct {
	kv    jetstream.KeyValue
	rate  float64
	burst float64
	now   func() time.Time
	log   *slog.Logger

	mu       sync.Mutex
	local    map[string]*localBucket
	warned   map[string]time.Time
	lastWarn time.Time
}

type bucketValue struct {
	Tokens    float64 `json:"tokens"`
	UpdatedMS int64   `json:"updated_ms"`
}

type localBucket struct {
	tokens float64
	last   time.Time
}

// errNoBudgetToken is returned internally when the bucket is empty; Take waits
// and retries instead of surfacing it.
var errNoBudgetToken = errors.New("fetch: no budget token")

// fallbackWarnInterval rate-limits fallback warnings per host.
const fallbackWarnInterval = 10 * time.Minute

// BudgetOptions configure a Budget.
type BudgetOptions struct {
	KV    jetstream.KeyValue
	Rate  float64 // tokens per second, default 1
	Burst float64 // bucket size, default 4
	Now   func() time.Time
	Log   *slog.Logger
}

// NewBudget builds a budget. A nil KV means a local-only limiter.
func NewBudget(opts BudgetOptions) *Budget {
	if opts.Rate <= 0 {
		opts.Rate = 1
	}
	if opts.Burst <= 0 {
		opts.Burst = 4
	}
	if opts.Burst < 1 {
		opts.Burst = 1
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Budget{
		kv:     opts.KV,
		rate:   opts.Rate,
		burst:  opts.Burst,
		now:    opts.Now,
		log:    opts.Log,
		local:  make(map[string]*localBucket),
		warned: make(map[string]time.Time),
	}
}

// BudgetKey is the KV key for one host. NATS KV keys allow only
// [-/_=.a-zA-Z0-9], so the port separator and any other character is
// replaced with "_".
func BudgetKey(host string) string {
	var b strings.Builder
	b.WriteString("host.")
	for i := 0; i < len(host); i++ {
		ch := host[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '-', ch == '/', ch == '_', ch == '=', ch == '.':
			b.WriteByte(ch)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Take blocks until one request to host is allowed or ctx is done.
func (b *Budget) Take(ctx context.Context, host string) error {
	if b == nil {
		return nil
	}
	if b.kv == nil {
		return b.takeLocal(ctx, host)
	}
	for {
		wait, err := b.takeKV(ctx, host)
		if err == nil {
			return nil
		}
		if err != errNoBudgetToken {
			// The KV is unavailable; a local limiter still keeps us polite.
			b.warnFallback(host, err)
			return b.takeLocal(ctx, host)
		}
		if wait <= 0 {
			wait = time.Second / time.Duration(b.rate)
		}
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *Budget) warnFallback(host string, err error) {
	if b.log == nil {
		return
	}
	now := b.now()
	b.mu.Lock()
	last, ok := b.warned[host]
	if ok && now.Sub(last) < fallbackWarnInterval {
		b.mu.Unlock()
		return
	}
	b.warned[host] = now
	if len(b.warned) > 4096 {
		for h, at := range b.warned {
			if now.Sub(at) > fallbackWarnInterval {
				delete(b.warned, h)
			}
		}
	}
	b.mu.Unlock()
	b.log.Warn("host budget KV unavailable; using a local limiter", "host", host, "err", err)
}

func (b *Budget) takeKV(ctx context.Context, host string) (time.Duration, error) {
	key := BudgetKey(host)
	for attempt := 0; attempt < 5; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, budgetKVTimeout)
		now := b.now()
		var state bucketValue
		var revision uint64
		entry, err := b.kv.Get(opCtx, key)
		cancel()
		switch {
		case err == nil:
			if err := json.Unmarshal(entry.Value(), &state); err != nil {
				state = bucketValue{}
			}
			revision = entry.Revision()
		case errors.Is(err, jetstream.ErrKeyNotFound):
			state = bucketValue{Tokens: b.burst, UpdatedMS: now.UnixMilli()}
		default:
			return 0, fmt.Errorf("fetch: budget read: %w", err)
		}
		if state.UpdatedMS <= 0 {
			state = bucketValue{Tokens: b.burst, UpdatedMS: now.UnixMilli()}
		}
		if nowMS := now.UnixMilli(); nowMS > state.UpdatedMS {
			state.Tokens = math.Min(b.burst, state.Tokens+b.rate*float64(nowMS-state.UpdatedMS)/1000)
			state.UpdatedMS = nowMS
		}
		if state.Tokens < 1 {
			missing := 1 - state.Tokens
			return time.Duration(missing / b.rate * float64(time.Second)), errNoBudgetToken
		}
		state.Tokens--
		payload, err := json.Marshal(state)
		if err != nil {
			return 0, err
		}
		opCtx, cancel = context.WithTimeout(ctx, budgetKVTimeout)
		if revision == 0 {
			_, err = b.kv.Create(opCtx, key, payload)
		} else {
			_, err = b.kv.Update(opCtx, key, payload, revision)
		}
		cancel()
		if err == nil {
			return 0, nil
		}
		// A lost create race or a CAS conflict re-reads and retries.
	}
	return 0, errors.New("fetch: budget contention")
}

func (b *Budget) takeLocal(ctx context.Context, host string) error {
	for {
		b.mu.Lock()
		bucket := b.local[host]
		now := b.now()
		if bucket == nil {
			bucket = &localBucket{tokens: b.burst, last: now}
			b.local[host] = bucket
		}
		if elapsed := now.Sub(bucket.last); elapsed > 0 {
			bucket.tokens = math.Min(b.burst, bucket.tokens+b.rate*elapsed.Seconds())
			bucket.last = now
		}
		if bucket.tokens >= 1 {
			bucket.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - bucket.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()
		if wait <= 0 {
			wait = time.Millisecond
		}
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
