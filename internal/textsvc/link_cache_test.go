package textsvc

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func textsvcTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   t.TempDir(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func textsvcTestService(t *testing.T, b *bus.Bus, now time.Time) *Service {
	t.Helper()
	svc := New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	svc.now = func() time.Time { return now }
	return svc
}

func putTextsvcLink(t *testing.T, ctx context.Context, b *bus.Bus, link *dorv1.Link) {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if !live.PutLink(ctx, kv, link) {
		t.Fatal("seed link")
	}
}

// TestExplainerLinkCacheIsVersionedByRelationRevision checks that cached
// explanations are bound to the relation revision. A changed link must never
// serve the old text, and with no model the newly written fallback carries the
// current revision.
func TestExplainerLinkCacheIsVersionedByRelationRevision(t *testing.T) {
	b := textsvcTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()

	t.Run("stale successful explanation", func(t *testing.T) {
		link := &dorv1.Link{A: "s-a", B: "s-b", Type: dorv1.LinkType_LINK_TYPE_THEMATIC, Strength: 0.7, Confidence: 0.8, UpdatedAt: now.Add(-time.Hour).Unix()}
		putTextsvcLink(t, ctx, b, link)
		oldRevision := live.LinkRevision(link)

		kv, err := b.KV(ctx, bus.KVBucketLiveState)
		if err != nil {
			t.Fatal(err)
		}
		if !live.PutLinkText(ctx, kv, live.LinkText{
			A: "s-a", B: "s-b", Why: "old explanation", Available: true,
			GeneratedAt: now.Add(-time.Minute).Format(time.RFC3339), Revision: oldRevision,
		}) {
			t.Fatal("seed cached text")
		}

		changed := proto.Clone(link).(*dorv1.Link)
		changed.Strength = 0.9
		changed.UpdatedAt = now.Unix()
		putTextsvcLink(t, ctx, b, changed)
		newRevision := live.LinkRevision(changed)
		if newRevision == oldRevision {
			t.Fatal("link revision did not change")
		}

		svc := textsvcTestService(t, b, now)
		got, ok := svc.Link(ctx, "s-a", "s-b")
		if !ok {
			t.Fatal("Link returned not-found")
		}
		if got.Revision != newRevision {
			t.Fatalf("Link revision = %d, want %d", got.Revision, newRevision)
		}
		if got.Available || got.Why != "" {
			t.Fatalf("Link served stale text: %+v", got)
		}

		generated, ok := svc.GenerateLink(ctx, "s-a", "s-b")
		if !ok {
			t.Fatal("GenerateLink returned not-found")
		}
		if generated.Revision != newRevision {
			t.Fatalf("generated fallback revision = %d, want %d", generated.Revision, newRevision)
		}
		if generated.Available || generated.Why != "" {
			t.Fatalf("GenerateLink served stale text: %+v", generated)
		}
		stored, ok := live.LoadLinkText(ctx, kv, "s-a", "s-b")
		if !ok || stored.Revision != newRevision {
			t.Fatalf("stored fallback = %+v (ok=%v), want revision %d", stored, ok, newRevision)
		}
	})

	t.Run("fresh fallback for an older revision", func(t *testing.T) {
		link := &dorv1.Link{A: "s-c", B: "s-d", Type: dorv1.LinkType_LINK_TYPE_THEMATIC, Strength: 0.6, Confidence: 0.7, UpdatedAt: now.Add(-time.Hour).Unix()}
		putTextsvcLink(t, ctx, b, link)

		svc := textsvcTestService(t, b, now)
		first, ok := svc.GenerateLink(ctx, "s-c", "s-d")
		if !ok {
			t.Fatal("first GenerateLink returned not-found")
		}
		if first.Revision == 0 {
			t.Fatal("first fallback has no revision")
		}

		changed := proto.Clone(link).(*dorv1.Link)
		changed.Strength = 0.95
		changed.UpdatedAt = now.Unix()
		putTextsvcLink(t, ctx, b, changed)
		newRevision := live.LinkRevision(changed)
		if newRevision == first.Revision {
			t.Fatal("link revision did not change")
		}

		generated, ok := svc.GenerateLink(ctx, "s-c", "s-d")
		if !ok {
			t.Fatal("second GenerateLink returned not-found")
		}
		if generated.Revision != newRevision {
			t.Fatalf("GenerateLink served a fresh fallback for the old revision: got %d, want %d", generated.Revision, newRevision)
		}
	})
}
