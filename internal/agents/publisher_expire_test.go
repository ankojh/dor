package agents

import (
	"testing"
	"time"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

// TestPublisherExpireStoriesCascadesLinks drives the publisher's expire path
// directly: an expired story queues a StoryExpire delta and every link touching
// it queues an explicit LinkRemove so connected clients drop it too.
func TestPublisherExpireStoriesCascadesLinks(t *testing.T) {
	core := newPublisherCore(nil, testLogger())
	now := time.Now().UTC()
	core.stories["old-story"] = &dorv1.Story{
		Id:        "old-story",
		Title:     "Old",
		UpdatedAt: now.Add(-49 * time.Hour).Unix(),
	}
	core.stories["fresh-story"] = &dorv1.Story{
		Id:        "fresh-story",
		Title:     "Fresh",
		UpdatedAt: now.Add(-time.Hour).Unix(),
	}
	core.links["fresh-story|old-story"] = &dorv1.Link{A: "fresh-story", B: "old-story", Strength: 0.6}
	core.links["another-story|fresh-story"] = &dorv1.Link{A: "another-story", B: "fresh-story", Strength: 0.6}

	core.expireStories(now)

	core.mu.Lock()
	defer core.mu.Unlock()
	expire := core.pending["story:old-story"]
	if expire == nil || expire.GetStoryExpire() != "old-story" {
		t.Fatalf("story expire delta = %v, want old-story", expire)
	}
	remove := core.pending["link:fresh-story|old-story"]
	if remove == nil || remove.GetLinkRemove() == nil {
		t.Fatalf("link remove delta = %v, want old-story/fresh-story", remove)
	}
	if got := remove.GetLinkRemove(); got.GetA() != "fresh-story" || got.GetB() != "old-story" {
		t.Fatalf("link remove pair = %v, want sorted fresh-story/old-story", got)
	}
	if core.pending["link:another-story|fresh-story"] != nil {
		t.Fatal("link between live stories was removed")
	}
	if core.links["fresh-story|old-story"] != nil {
		t.Fatal("expired link remains in the publisher link table")
	}
	if core.stories["fresh-story"] == nil {
		t.Fatal("fresh story was expired")
	}
}

// TestPublisherCoolingDropsHeatWithoutMovingUpdatedAt checks the derived heat
// decay: a quiet story's emitted story upsert is cooler, while UpdatedAt (the
// expiry anchor) is byte-for-byte unchanged.
func TestPublisherCoolingDropsHeatWithoutMovingUpdatedAt(t *testing.T) {
	core := newPublisherCore(nil, testLogger())
	core.restored = true
	now := time.Now().UTC()
	updated := now.Add(-10 * time.Minute)
	story := &dorv1.Story{
		Id:        "quiet-story",
		Title:     "Quiet",
		Heat:      0.8,
		FirstSeen: updated.Add(-time.Hour).Unix(),
		UpdatedAt: updated.Unix(),
	}
	core.stories[story.Id] = story
	core.cooledAt[story.Id] = now.Add(-2 * time.Hour).UnixNano()

	core.coolStories(now)

	core.mu.Lock()
	defer core.mu.Unlock()
	delta := core.pending["story:"+story.Id]
	if delta == nil || delta.GetStoryUpsert() == nil {
		t.Fatalf("cooling queued %v, want a story upsert", delta)
	}
	cooled := delta.GetStoryUpsert()
	if cooled.Heat >= story.Heat {
		t.Fatalf("heat did not drop: %v -> %v", story.Heat, cooled.Heat)
	}
	if cooled.Heat <= publisherHeatFloor {
		t.Fatalf("heat collapsed to %v instead of fading", cooled.Heat)
	}
	if cooled.UpdatedAt != story.UpdatedAt {
		t.Fatalf("cooling moved UpdatedAt %d -> %d", story.UpdatedAt, cooled.UpdatedAt)
	}
	if story.Heat != 0.8 {
		t.Fatalf("cooling mutated live state heat to %v", story.Heat)
	}
}
