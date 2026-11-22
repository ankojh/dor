// Package live defines the shared hot-state contract in the `live_state` KV
// bucket. The clusterer writes stories, story metadata and placed articles;
// the relation judge writes links; the editor/explainer write text caches.
// Everything here is metadata: none of it is placed in frontend deltas except
// the Story/Link protobufs themselves.
package live

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Key prefixes in the live_state bucket.
const (
	StoryPrefix     = "story."
	StoryMetaPrefix = "storymeta."
	ArticlePrefix   = "article."
	LinkPrefix      = "link."
	StoryTextPrefix = "storytext."
	LinkTextPrefix  = "linktext."
)

// kv is the subset of jetstream.KeyValue used here.
type kv interface {
	Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error)
	Put(ctx context.Context, key string, value []byte) (uint64, error)
}

const opTimeout = 500 * time.Millisecond

// sanitize keeps a key inside the NATS KV charset.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '/', c == '_', c == '=', c == '.':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// StoryKey is the KV key for a story protobuf.
func StoryKey(id string) string { return StoryPrefix + sanitize(id) }

// StoryMetaKey is the KV key for a story metadata JSON document.
func StoryMetaKey(id string) string { return StoryMetaPrefix + sanitize(id) }

// ArticleKey is the KV key for a clean article protobuf.
func ArticleKey(id string) string { return ArticlePrefix + sanitize(id) }

// LinkKey is the KV key for a link, with a < b.
func LinkKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return LinkPrefix + sanitize(a) + "." + sanitize(b)
}

// StoryTextKey is the KV key for a story brief cache.
func StoryTextKey(id string) string { return StoryTextPrefix + sanitize(id) }

// LinkTextKey is the KV key for a link explanation cache.
func LinkTextKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return LinkTextPrefix + sanitize(a) + "." + sanitize(b)
}

// StoryMeta is the story record shared with the clusterer. Arrays are never
// null in the wire form; this struct only decodes.
type StoryMeta struct {
	ID         string    `json:"id"`
	ArticleIDs []string  `json:"article_ids"`
	SourceIDs  []string  `json:"source_ids"`
	Embedding  []float32 `json:"embedding"`
	Entities   []string  `json:"entities"`
	FirstSeen  int64     `json:"first_seen"`
	UpdatedAt  int64     `json:"updated_at"`
}

// StoryText is the cached editor output. Available=false means the fallback
// (best headline, no brief) is in force.
type StoryText struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Title       string `json:"title"`
	Brief       string `json:"brief"`
	Available   bool   `json:"available"`
	SourceCount int    `json:"source_count"`
	GeneratedAt string `json:"generated_at"`
	Model       string `json:"model,omitempty"`
}

// LinkText is the cached "why" sentence. Revision is the relation revision
// (see LinkRevision) the text was generated for; a mismatch means the cached
// explanation describes an older relation.
type LinkText struct {
	Kind        string `json:"kind"`
	A           string `json:"a"`
	B           string `json:"b"`
	Why         string `json:"why"`
	Available   bool   `json:"available"`
	GeneratedAt string `json:"generated_at"`
	Model       string `json:"model,omitempty"`
	Revision    uint64 `json:"revision,omitempty"`
}

func get(ctx context.Context, k kv, key string) ([]byte, bool) {
	if k == nil {
		return nil, false
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	e, err := k.Get(opCtx, key)
	if err != nil {
		return nil, false
	}
	return append([]byte(nil), e.Value()...), true
}

func put(ctx context.Context, k kv, key string, data []byte) bool {
	if k == nil {
		return false
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_, err := k.Put(opCtx, key, data)
	return err == nil
}

// LoadStory reads a story protobuf.
func LoadStory(ctx context.Context, k kv, id string) (*dorv1.Story, bool) {
	data, ok := get(ctx, k, StoryKey(id))
	if !ok {
		return nil, false
	}
	var s dorv1.Story
	if proto.Unmarshal(data, &s) != nil {
		return nil, false
	}
	return &s, true
}

// LoadStoryMeta reads story metadata.
func LoadStoryMeta(ctx context.Context, k kv, id string) (StoryMeta, bool) {
	data, ok := get(ctx, k, StoryMetaKey(id))
	if !ok {
		return StoryMeta{}, false
	}
	var m StoryMeta
	if json.Unmarshal(data, &m) != nil {
		return StoryMeta{}, false
	}
	if m.ID == "" {
		m.ID = id
	}
	return m, true
}

// LoadArticle reads a clean article protobuf.
func LoadArticle(ctx context.Context, k kv, id string) (*dorv1.Article, bool) {
	data, ok := get(ctx, k, ArticleKey(id))
	if !ok {
		return nil, false
	}
	var a dorv1.Article
	if proto.Unmarshal(data, &a) != nil {
		return nil, false
	}
	return &a, true
}

// LoadLink reads a link protobuf.
func LoadLink(ctx context.Context, k kv, a, b string) (*dorv1.Link, bool) {
	data, ok := get(ctx, k, LinkKey(a, b))
	if !ok {
		return nil, false
	}
	var l dorv1.Link
	if proto.Unmarshal(data, &l) != nil {
		return nil, false
	}
	return &l, true
}

// LinkRevision is the stable content revision of a relation. It changes when
// the pair, type, direction, confidence, strength, verification or update time
// changes, so a cached explanation generated for an older relation can be
// recognised and discarded. It is deterministic across processes and restarts.
func LinkRevision(l *dorv1.Link) uint64 {
	if l == nil {
		return 0
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(l)
	if err != nil {
		return 0
	}
	sum := sha256.Sum256(data)
	return binary.BigEndian.Uint64(sum[:8])
}

// PutLink writes a link. A zero-strength link is a tombstone for removal.
func PutLink(ctx context.Context, k kv, l *dorv1.Link) bool {
	if l == nil {
		return false
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(l)
	if err != nil {
		return false
	}
	return put(ctx, k, LinkKey(l.A, l.B), data)
}

// LoadStoryText reads the cached brief.
func LoadStoryText(ctx context.Context, k kv, id string) (StoryText, bool) {
	data, ok := get(ctx, k, StoryTextKey(id))
	if !ok {
		return StoryText{}, false
	}
	var t StoryText
	if json.Unmarshal(data, &t) != nil {
		return StoryText{}, false
	}
	return t, true
}

// PutStoryText writes the cached brief.
func PutStoryText(ctx context.Context, k kv, t StoryText) bool {
	t.Kind = "story"
	data, err := json.Marshal(t)
	if err != nil {
		return false
	}
	return put(ctx, k, StoryTextKey(t.ID), data)
}

// LoadLinkText reads the cached "why".
func LoadLinkText(ctx context.Context, k kv, a, b string) (LinkText, bool) {
	data, ok := get(ctx, k, LinkTextKey(a, b))
	if !ok {
		return LinkText{}, false
	}
	var t LinkText
	if json.Unmarshal(data, &t) != nil {
		return LinkText{}, false
	}
	return t, true
}

// PutLinkText writes the cached "why".
func PutLinkText(ctx context.Context, k kv, t LinkText) bool {
	if t.A > t.B {
		t.A, t.B = t.B, t.A
	}
	t.Kind = "link"
	data, err := json.Marshal(t)
	if err != nil {
		return false
	}
	return put(ctx, k, LinkTextKey(t.A, t.B), data)
}

// SortPair orders two story ids as the Link contract requires.
func SortPair(a, b string) (string, string) {
	if a > b {
		return b, a
	}
	return a, b
}

// SortedIDs is a small helper for deterministic snapshot order.
func SortedIDs(m map[string]bool) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
