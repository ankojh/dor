package fetch

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var kvKeyPattern = regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`)

// checkKey enforces the same key charset as NATS KV, so a test cannot pass
// with a key the real bucket would reject.
func checkKey(key string) error {
	if !kvKeyPattern.MatchString(key) {
		return fmt.Errorf("invalid kv key %q", key)
	}
	return nil
}

// fakeKV implements the subset of jetstream.KeyValue the fetch package uses.
// Any other method panics through the nil embedded interface, which is the
// point: a test must not depend on calls the code does not make.
type fakeKV struct {
	jetstream.KeyValue

	mu      sync.Mutex
	data    map[string][]byte
	rev     map[string]uint64
	counter uint64
	// getErr, when set, makes every Get fail with a non-NotFound error.
	getErr error
	puts   int
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	key   string
	value []byte
	rev   uint64
}

func (e fakeEntry) Key() string        { return e.key }
func (e fakeEntry) Value() []byte      { return e.value }
func (e fakeEntry) Revision() uint64   { return e.rev }
func (e fakeEntry) Created() time.Time { return time.Time{} }

func newFakeKV() *fakeKV {
	return &fakeKV{data: map[string][]byte{}, rev: map[string]uint64{}}
}

func (k *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.getErr != nil {
		return nil, k.getErr
	}
	v, ok := k.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return fakeEntry{key: key, value: append([]byte(nil), v...), rev: k.rev[key]}, nil
}

func (k *fakeKV) Create(_ context.Context, key string, value []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	if err := checkKey(key); err != nil {
		return 0, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.data[key]; ok {
		return 0, jetstream.ErrKeyExists
	}
	return k.putLocked(key, value), nil
}

func (k *fakeKV) Update(_ context.Context, key string, value []byte, last uint64) (uint64, error) {
	if err := checkKey(key); err != nil {
		return 0, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.rev[key] != last {
		return 0, errors.New("wrong last sequence")
	}
	return k.putLocked(key, value), nil
}

func (k *fakeKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	if err := checkKey(key); err != nil {
		return 0, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.puts++
	return k.putLocked(key, value), nil
}

func (k *fakeKV) putLocked(key string, value []byte) uint64 {
	k.counter++
	k.data[key] = append([]byte(nil), value...)
	k.rev[key] = k.counter
	return k.counter
}

func (k *fakeKV) raw(key string) ([]byte, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.data[key]
	return v, ok
}
