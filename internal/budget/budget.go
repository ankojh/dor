// Package budget enforces Dor's model spend limits.
//
// Two layers are checked before every jev or Claude call:
//
//   - a token bucket per API in the short-lived `budgets` NATS KV bucket
//     (1 h TTL; refills itself, so a missing key starts full);
//   - a daily USD cap keyed by UTC day in the `spend` bucket (48 h TTL), so the
//     cap survives idle hours and process restarts.
//
// Admission is atomic. Check reserves the estimated dollars in the daily
// counter and the estimated tokens in the API bucket, and returns a
// Reservation. Record reconciles that reservation with the actual usage, or
// refunds it when the call failed. KV updates are compare-and-swap on the
// bucket revision; the memory store holds one lock across the whole
// read-modify-write.
//
// A hard cap denies at 100%. Below it the shed order applies: story briefs at
// 80%, "why" prefetch at 90%, thematic links at 95%. The thresholds are
// configurable in config/budget.yaml.
//
// Wiring:
//
//	tokens, err := b.KV(ctx, bus.KVBucketBudgets)
//	spend, err := b.KV(ctx, bus.KVBucketSpend)
//	mgr := budget.New(budget.NewKVStore(tokens), budget.NewKVStore(spend), cfg)
//
// budget.NewMemory serves tests and mock runs. It never touches NATS.
package budget

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/config"
)

// Priority labels what a call is for. The shed order is briefs, then "why"
// prefetch, then thematic links.
type Priority string

const (
	// PriorityDefault is work that is not part of the shed order. It is denied
	// only by the hard daily cap or the API token bucket.
	PriorityDefault Priority = "default"
	// PriorityBrief is story editor output. Shed first under pressure.
	PriorityBrief Priority = "brief"
	// PriorityWhy is the explainer ("why") prefetch. Shed second.
	PriorityWhy Priority = "why"
	// PriorityThematic is thematic link judging. Shed third.
	PriorityThematic Priority = "thematic"
)

// ErrNotFound is returned by a Store for a key that does not exist.
var ErrNotFound = errors.New("budget: key not found")

// ErrAbort tells Store.Update to abandon the write. Check uses it for a
// denial; the callback has already filled the verdict.
var ErrAbort = errors.New("budget: abort update")

// Store is the key/value surface the limiter needs. Update must apply fn and
// its write atomically: one lock for memory, compare-and-swap for KV.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Update(ctx context.Context, key string, fn func(old []byte, exists bool) ([]byte, error)) error
}

// Request is one call about to be made.
type Request struct {
	API                   string
	Priority              Priority
	EstimatedInputTokens  int64
	EstimatedOutputTokens int64
	EstimatedUSD          float64
}

// Reservation is what an allowed Check reserved. Record reconciles it.
type Reservation struct {
	ID     string
	API    string
	Day    string
	Tokens int64
	USD    float64
}

// Verdict is the limiter's answer.
type Verdict struct {
	Allowed     bool
	Reason      string
	SpendUSD    float64
	DailyCapUSD float64
	ShedLevel   int
	Reservation *Reservation
}

// Usage is the actual cost of a completed call.
type Usage struct {
	CostUSD      float64
	InputTokens  int64
	OutputTokens int64
}

// PendingUSD is one outstanding dollar reservation in the daily document.
type PendingUSD struct {
	USD float64   `json:"usd"`
	At  time.Time `json:"at"`
}

// Daily is the spend counter for one UTC day. USD includes pending
// reservations until Record reconciles them.
type Daily struct {
	Day          string                `json:"day"`
	USD          float64               `json:"usd"`
	Calls        int64                 `json:"calls"`
	InputTokens  int64                 `json:"input_tokens"`
	OutputTokens int64                 `json:"output_tokens"`
	Pending      map[string]PendingUSD `json:"pending,omitempty"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

// PendingTokens is one outstanding token reservation in a bucket document.
type PendingTokens struct {
	Tokens int64     `json:"tokens"`
	At     time.Time `json:"at"`
}

type bucket struct {
	Tokens    float64                  `json:"tokens"`
	Pending   map[string]PendingTokens `json:"pending,omitempty"`
	UpdatedAt time.Time                `json:"updated_at"`
}

// Manager applies the limits. It is safe for concurrent use.
type Manager struct {
	cfg    config.Budget
	tokens Store
	spend  Store
	now    func() time.Time
	newID  func() string
	stale  atomic.Int64
}

// New builds a manager over two stores.
func New(tokens, spend Store, cfg config.Budget) *Manager {
	return &Manager{cfg: cfg, tokens: tokens, spend: spend, now: time.Now, newID: newReservationID}
}

// NewMemory builds an in-memory manager for tests and mock runs.
func NewMemory(cfg config.Budget) *Manager {
	return New(NewMemoryStore(), NewMemoryStore(), cfg)
}

// WithClock replaces the clock. Tests use it to cross a UTC midnight.
func (m *Manager) WithClock(now func() time.Time) *Manager {
	m.now = now
	return m
}

// StaleCollected reports how many expired pending reservations were garbage
// collected. They stay charged at the estimate, which is conservative.
func (m *Manager) StaleCollected() int64 { return m.stale.Load() }

// Check reserves estimated dollars and tokens and returns a Reservation.
// A denial carries the reason and leaves no reservation.
func (m *Manager) Check(ctx context.Context, req Request) (Verdict, error) {
	now := m.now().UTC()
	day := now.Format("2006-01-02")
	dayKey := "daily." + day
	res := &Reservation{
		ID:     m.newID(),
		API:    req.API,
		Day:    day,
		Tokens: req.EstimatedInputTokens + req.EstimatedOutputTokens,
		USD:    req.EstimatedUSD,
	}

	var verdict Verdict
	err := m.spend.Update(ctx, dayKey, func(old []byte, exists bool) ([]byte, error) {
		d := Daily{Day: day}
		if exists {
			if err := json.Unmarshal(old, &d); err != nil {
				return nil, fmt.Errorf("budget: decode daily spend: %w", err)
			}
		}
		m.gcDaily(&d, now)
		if m.cfg.DailyUSD <= 0 {
			verdict = Verdict{Reason: "daily-cap", SpendUSD: d.USD, DailyCapUSD: m.cfg.DailyUSD}
			return nil, ErrAbort
		}
		verdict.SpendUSD = d.USD
		verdict.DailyCapUSD = m.cfg.DailyUSD
		verdict.ShedLevel = m.ShedLevel(d.USD / m.cfg.DailyUSD)
		if d.USD >= m.cfg.DailyUSD {
			verdict.Reason = "daily-cap"
			return nil, ErrAbort
		}
		if req.EstimatedUSD > 0 && d.USD+req.EstimatedUSD > m.cfg.DailyUSD {
			verdict.Reason = "daily-cap"
			return nil, ErrAbort
		}
		if m.Sheds(req.Priority, d.USD/m.cfg.DailyUSD) {
			verdict.Reason = "shed:" + string(req.Priority)
			return nil, ErrAbort
		}
		if d.Pending == nil {
			d.Pending = make(map[string]PendingUSD)
		}
		d.Pending[res.ID] = PendingUSD{USD: req.EstimatedUSD, At: now}
		d.USD += req.EstimatedUSD
		d.UpdatedAt = now
		return json.Marshal(d)
	})
	if errors.Is(err, ErrAbort) {
		verdict.Allowed = false
		return verdict, nil
	}
	if err != nil {
		return Verdict{}, err
	}

	// Reserve the token bucket after the dollar reservation. If the bucket is
	// empty, refund the dollars immediately: a denied call must not consume
	// daily budget.
	tokenDenied := false
	spec := m.bucketFor(req.API)
	err = m.tokens.Update(ctx, "tokens."+req.API, func(old []byte, exists bool) ([]byte, error) {
		b := bucket{Tokens: spec.Capacity, UpdatedAt: now}
		if exists {
			if err := json.Unmarshal(old, &b); err != nil {
				return nil, fmt.Errorf("budget: decode token bucket %s: %w", req.API, err)
			}
		}
		m.gcBucket(&b, now)
		elapsed := now.Sub(b.UpdatedAt).Seconds()
		if elapsed > 0 {
			b.Tokens = clamp(b.Tokens+elapsed*spec.RefillPerSecond, 0, spec.Capacity)
		}
		need := float64(res.Tokens)
		if b.Tokens < need {
			tokenDenied = true
			return nil, ErrAbort
		}
		if b.Pending == nil {
			b.Pending = make(map[string]PendingTokens)
		}
		b.Pending[res.ID] = PendingTokens{Tokens: res.Tokens, At: now}
		b.Tokens = clamp(b.Tokens-need, 0, spec.Capacity)
		b.UpdatedAt = now
		return json.Marshal(b)
	})
	if err != nil {
		if refundErr := m.refundDaily(ctx, dayKey, res, now); refundErr != nil && !errors.Is(refundErr, ErrAbort) {
			return Verdict{}, refundErr
		}
		if errors.Is(err, ErrAbort) && tokenDenied {
			verdict.Allowed = false
			verdict.Reason = "token-bucket"
			return verdict, nil
		}
		return Verdict{}, err
	}
	verdict.Allowed = true
	verdict.Reason = "ok"
	verdict.Reservation = res
	return verdict, nil
}

// Record reconciles a reservation with actual usage. Each store is
// independently idempotent: a reservation id is removed from the daily pending
// map and the token pending map exactly once. A retry after a partial success
// leaves the unfinished phase pending, so it resumes naturally.
func (m *Manager) Record(ctx context.Context, res Reservation, usage Usage) error {
	now := m.now().UTC()
	day := res.Day
	if day == "" {
		day = now.Format("2006-01-02")
	}
	dayKey := "daily." + day
	err := m.spend.Update(ctx, dayKey, func(old []byte, exists bool) ([]byte, error) {
		d := Daily{Day: day}
		if exists {
			if err := json.Unmarshal(old, &d); err != nil {
				return nil, fmt.Errorf("budget: decode daily spend: %w", err)
			}
		}
		m.gcDaily(&d, now)
		p, ok := d.Pending[res.ID]
		if !ok {
			// Already reconciled, garbage-collected, or unknown. Do not
			// touch the counter; the token phase may still be pending.
			return nil, ErrAbort
		}
		delete(d.Pending, res.ID)
		d.USD += usage.CostUSD - p.USD
		if d.USD < 0 {
			d.USD = 0
		}
		d.Calls++
		d.InputTokens += usage.InputTokens
		d.OutputTokens += usage.OutputTokens
		d.UpdatedAt = now
		return json.Marshal(d)
	})
	if err != nil && !errors.Is(err, ErrAbort) {
		return err
	}

	actual := usage.InputTokens + usage.OutputTokens
	spec := m.bucketFor(res.API)
	err = m.tokens.Update(ctx, "tokens."+res.API, func(old []byte, exists bool) ([]byte, error) {
		b := bucket{Tokens: spec.Capacity, UpdatedAt: now}
		if exists {
			if err := json.Unmarshal(old, &b); err != nil {
				return nil, fmt.Errorf("budget: decode token bucket %s: %w", res.API, err)
			}
		}
		m.gcBucket(&b, now)
		elapsed := now.Sub(b.UpdatedAt).Seconds()
		if elapsed > 0 {
			b.Tokens = clamp(b.Tokens+elapsed*spec.RefillPerSecond, 0, spec.Capacity)
		}
		p, ok := b.Pending[res.ID]
		if !ok {
			return nil, ErrAbort
		}
		delete(b.Pending, res.ID)
		b.Tokens = clamp(b.Tokens+float64(p.Tokens)-float64(actual), 0, spec.Capacity)
		b.UpdatedAt = now
		return json.Marshal(b)
	})
	if errors.Is(err, ErrAbort) {
		return nil
	}
	return err
}

func (m *Manager) pendingTTL() time.Duration {
	if m.cfg.PendingTTL.Std() > 0 {
		return m.cfg.PendingTTL.Std()
	}
	return 15 * time.Minute
}

func (m *Manager) gcDaily(d *Daily, now time.Time) {
	if len(d.Pending) == 0 {
		return
	}
	ttl := m.pendingTTL()
	for id, p := range d.Pending {
		if now.Sub(p.At) > ttl {
			// The estimate stays charged; dropping the entry only makes the
			// reservation unreconcilable, which is conservative.
			delete(d.Pending, id)
			m.stale.Add(1)
		}
	}
}

func (m *Manager) gcBucket(b *bucket, now time.Time) {
	if len(b.Pending) == 0 {
		return
	}
	ttl := m.pendingTTL()
	for id, p := range b.Pending {
		if now.Sub(p.At) > ttl {
			delete(b.Pending, id)
			m.stale.Add(1)
		}
	}
}

// Snapshot returns the current day's counter, including outstanding
// reservations. It never mutates anything.
func (m *Manager) Snapshot(ctx context.Context) (Daily, error) {
	return m.loadDaily(ctx, m.now().UTC())
}

// ShedLevel maps the spend ratio to the shed stage: 0 none, 1 briefs, 2 briefs
// and "why", 3 all three. The hard cap is separate and denies at 1.0.
func (m *Manager) ShedLevel(ratio float64) int {
	if m.cfg.DailyUSD <= 0 {
		return 3
	}
	switch {
	case ratio >= m.cfg.ShedThematicAt:
		return 3
	case ratio >= m.cfg.ShedWhyAt:
		return 2
	case ratio >= m.cfg.ShedBriefAt:
		return 1
	default:
		return 0
	}
}

// Sheds reports whether a priority is shed at this spend ratio. It is the hook
// later agents call; Check applies the same rule.
func (m *Manager) Sheds(priority Priority, ratio float64) bool {
	switch m.ShedLevel(ratio) {
	case 1:
		return priority == PriorityBrief
	case 2:
		return priority == PriorityBrief || priority == PriorityWhy
	case 3:
		return priority == PriorityBrief || priority == PriorityWhy || priority == PriorityThematic
	default:
		return false
	}
}

func (m *Manager) bucketFor(api string) config.TokenBucket {
	if b, ok := m.cfg.TokenBuckets[api]; ok {
		return b
	}
	// Unknown API: no bucket configured, so do not throttle it. A missing
	// entry is a config mistake, but failing every call is worse than running
	// unbounded for a local build; the daily cap still applies.
	return config.TokenBucket{Capacity: 1e18, RefillPerSecond: 1e18}
}

func (m *Manager) refundDaily(ctx context.Context, key string, res *Reservation, now time.Time) error {
	if res.USD == 0 && res.ID == "" {
		return nil
	}
	return m.spend.Update(ctx, key, func(old []byte, exists bool) ([]byte, error) {
		var d Daily
		if exists {
			if err := json.Unmarshal(old, &d); err != nil {
				return nil, fmt.Errorf("budget: decode daily spend: %w", err)
			}
		}
		m.gcDaily(&d, now)
		p, ok := d.Pending[res.ID]
		if !ok {
			return nil, ErrAbort
		}
		delete(d.Pending, res.ID)
		d.USD -= p.USD
		if d.USD < 0 {
			d.USD = 0
		}
		d.UpdatedAt = now
		return json.Marshal(d)
	})
}

func (m *Manager) loadDaily(ctx context.Context, now time.Time) (Daily, error) {
	day := now.UTC().Format("2006-01-02")
	raw, err := m.spend.Get(ctx, "daily."+day)
	if errors.Is(err, ErrNotFound) {
		return Daily{Day: day}, nil
	}
	if err != nil {
		return Daily{}, err
	}
	var d Daily
	if err := json.Unmarshal(raw, &d); err != nil {
		return Daily{}, fmt.Errorf("budget: decode daily spend: %w", err)
	}
	return d, nil
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func newReservationID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("res-%d", time.Now().UnixNano())
	}
	return "res-" + hex.EncodeToString(b[:])
}

// MemoryStore is an in-memory Store for tests and mock runs. Update holds one
// mutex across the whole read-modify-write.
type MemoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

// Put implements Store. It is for seeding and tests; the Manager uses Update.
func (s *MemoryStore) Put(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), value...)
	return nil
}

// Update implements Store atomically.
func (s *MemoryStore) Update(_ context.Context, key string, fn func(old []byte, exists bool) ([]byte, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.data[key]
	newValue, err := fn(append([]byte(nil), old...), exists)
	if err != nil {
		return err
	}
	s.data[key] = append([]byte(nil), newValue...)
	return nil
}

// KVStore adapts a NATS JetStream key/value bucket.
type KVStore struct {
	kv         jetstream.KeyValue
	RetryDelay time.Duration
	MaxRetries int
}

// NewKVStore wraps a bucket. Pass bus.KVBucketBudgets for token buckets and
// bus.KVBucketSpend for daily spend.
func NewKVStore(kv jetstream.KeyValue) *KVStore {
	return &KVStore{kv: kv, RetryDelay: 2 * time.Millisecond, MaxRetries: 16}
}

// Get implements Store.
func (s *KVStore) Get(ctx context.Context, key string) ([]byte, error) {
	entry, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("budget: kv get %s: %w", key, err)
	}
	return entry.Value(), nil
}

// Put implements Store. It is for seeding and tests; the Manager uses Update.
func (s *KVStore) Put(ctx context.Context, key string, value []byte) error {
	if _, err := s.kv.Put(ctx, key, value); err != nil {
		return fmt.Errorf("budget: kv put %s: %w", key, err)
	}
	return nil
}

// Update implements compare-and-swap on the KV revision with bounded retry.
func (s *KVStore) Update(ctx context.Context, key string, fn func(old []byte, exists bool) ([]byte, error)) error {
	retries := s.MaxRetries
	if retries <= 0 {
		retries = 16
	}
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		entry, err := s.kv.Get(ctx, key)
		var old []byte
		exists := true
		revision := uint64(0)
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			exists = false
		} else if err != nil {
			return fmt.Errorf("budget: kv get %s: %w", key, err)
		} else {
			old = entry.Value()
			revision = entry.Revision()
		}
		value, fnErr := fn(old, exists)
		if fnErr != nil {
			return fnErr
		}
		if !exists {
			_, err = s.kv.Create(ctx, key, value)
		} else {
			_, err = s.kv.Update(ctx, key, value, revision)
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) &&
			!errors.Is(err, jetstream.ErrKeyRevisionMismatch) &&
			!errors.Is(err, jetstream.ErrKeyNotFound) &&
			!errors.Is(err, jetstream.ErrKeyDeleted) {
			return fmt.Errorf("budget: kv update %s: %w", key, err)
		}
		lastErr = err
		if s.RetryDelay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.RetryDelay):
			}
		}
	}
	return fmt.Errorf("budget: kv update %s: no progress after %d attempts: %w", key, retries, lastErr)
}
