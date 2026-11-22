package agents

import (
	"context"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// An expired story and every link touching it must leave the published
// snapshot, while a fresh story and unrelated link stay.
func TestPublisherSnapshotDropsExpiredStoryAndLinks(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())

	now := time.Now().UTC()
	old := &dorv1.Story{Id: "snap-old", Title: "Old", Heat: 0.6, FirstSeen: now.Add(-49 * time.Hour).Unix(), UpdatedAt: now.Add(-49 * time.Hour).Unix()}
	keep := &dorv1.Story{Id: "snap-keep", Title: "Keep", Heat: 0.6, FirstSeen: now.Unix(), UpdatedAt: now.Unix()}
	other := &dorv1.Story{Id: "snap-other", Title: "Other", Heat: 0.6, FirstSeen: now.Unix(), UpdatedAt: now.Unix()}
	c.onStory(old)
	c.onStory(keep)
	c.onStory(other)
	c.onLink(&dorv1.Link{A: "snap-old", B: "snap-keep", Type: dorv1.LinkType_LINK_TYPE_THEMATIC, Strength: 0.8, Confidence: 0.8, UpdatedAt: now.Unix()})
	c.onLink(&dorv1.Link{A: "snap-keep", B: "snap-other", Type: dorv1.LinkType_LINK_TYPE_THEMATIC, Strength: 0.8, Confidence: 0.8, UpdatedAt: now.Unix()})
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}

	c.expireStories(now)
	if err := c.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.writeSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	var snap dorv1.Snapshot
	if err := live.UnmarshalSnapshot(entry.Value(), &snap); err != nil {
		t.Fatal(err)
	}
	storyIDs := map[string]bool{}
	for _, story := range snap.Stories {
		storyIDs[story.Id] = true
	}
	if storyIDs["snap-old"] {
		t.Fatal("expired story remains in the snapshot")
	}
	if !storyIDs["snap-keep"] || !storyIDs["snap-other"] {
		t.Fatalf("fresh stories missing from snapshot: %v", storyIDs)
	}
	for _, link := range snap.Links {
		if link.A == "snap-old" || link.B == "snap-old" {
			t.Fatalf("expired story link remains in the snapshot: %v", link)
		}
	}
	want := false
	for _, link := range snap.Links {
		if link.A == "snap-keep" && link.B == "snap-other" {
			want = true
		}
	}
	if !want {
		t.Fatalf("unrelated fresh link missing from snapshot: %v", snap.Links)
	}
	if len(snap.Stories) != 2 || len(snap.Links) != 1 {
		t.Fatalf("snapshot stories=%d links=%d, want 2/1", len(snap.Stories), len(snap.Links))
	}
}
