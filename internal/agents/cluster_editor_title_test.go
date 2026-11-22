package agents

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// The cs checkpoint is authoritative for membership, but F1·2's editor title
// marker must survive recovery and be republished to the derived story key.
func TestClusterRecoveryPreservesEditorTitleMarker(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	story := &dorv1.Story{Id: "editor-title", Title: "Cluster title", Category: dorv1.Category_CATEGORY_OTHER, SourceCount: 1, Heat: 0.5, FirstSeen: now.Unix(), UpdatedAt: now.Unix()}
	meta := live.StoryMeta{ID: story.Id, ArticleIDs: []string{"a-editor"}, SourceIDs: []string{"src"}, FirstSeen: story.FirstSeen, UpdatedAt: story.UpdatedAt}
	sd, _ := proto.Marshal(story)
	md, _ := json.Marshal(meta)
	liveKV, _ := b.KV(ctx, bus.KVBucketLiveState)
	liveKV.Put(ctx, live.StoryKey(story.Id), sd)
	liveKV.Put(ctx, live.StoryMetaKey(story.Id), md)
	marker, _ := json.Marshal(map[string]string{"title": "Editor title"})
	if _, err := liveKV.Put(ctx, "storytitle."+story.Id, marker); err != nil {
		t.Fatal(err)
	}

	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	c := agent.(*clusterer)
	if err := c.load(ctx); err != nil {
		t.Fatal(err)
	}
	st := c.stories[story.Id]
	if st == nil {
		t.Fatal("story not restored")
	}
	if st.Story.Title != "Editor title" {
		t.Fatalf("recovered title = %q, want editor title", st.Story.Title)
	}
	derived, ok := live.LoadStory(ctx, liveKV, story.Id)
	if !ok || derived.Title != "Editor title" {
		t.Fatalf("derived story title = %q (ok=%v), want editor title", derived.GetTitle(), ok)
	}
}
