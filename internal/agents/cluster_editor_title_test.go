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

func seedEditorTitleStory(t *testing.T, now time.Time) (*bus.Bus, *clusterer, *storyState, context.Context) {
	t.Helper()
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	story := &dorv1.Story{Id: "editor-title", Title: "Cluster title", Category: dorv1.Category_CATEGORY_OTHER, SourceCount: 1, Heat: 0.8,
		FirstSeen: now.Add(-time.Hour).Unix(), UpdatedAt: now.Add(-time.Hour).Unix(),
		Locations: []*dorv1.Location{{PlaceId: "p", Name: "Place", Lat: 1, Lon: 2, Role: dorv1.Role_ROLE_VENUE}}}
	meta := live.StoryMeta{ID: story.Id, ArticleIDs: []string{"a-editor"}, SourceIDs: []string{"src"}, FirstSeen: story.FirstSeen, UpdatedAt: story.UpdatedAt}
	sd, _ := proto.Marshal(story)
	md, _ := json.Marshal(meta)
	liveKV, _ := b.KV(ctx, bus.KVBucketLiveState)
	liveKV.Put(ctx, live.StoryKey(story.Id), sd)
	liveKV.Put(ctx, live.StoryMetaKey(story.Id), md)
	marker, _ := json.Marshal(map[string]string{"title": "Editor title", "generated_at": now.UTC().Format(time.RFC3339)})
	if _, err := liveKV.Put(ctx, "storytitle."+story.Id, marker); err != nil {
		t.Fatal(err)
	}
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{ClusterNow: func() time.Time { return now }})
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
	return b, c, st, ctx
}

// The cs checkpoint is authoritative for membership, but F1·2's editor title
// marker must survive recovery and be republished to the derived story key.
func TestClusterRecoveryPreservesEditorTitleMarker(t *testing.T) {
	_, _, st, _ := seedEditorTitleStory(t, time.Now().UTC())
	if st.Story.Title != "Editor title" || st.Story.TitleSource != "editor" {
		t.Fatalf("recovered title = %q source=%q, want editor", st.Story.Title, st.Story.TitleSource)
	}
}

// A later cluster content update must keep the editor title while taking
// membership/source/location fields from the cluster state.
func TestClusterContentUpdateKeepsEditorTitle(t *testing.T) {
	b, c, st, ctx := seedEditorTitleStory(t, time.Now().UTC())
	st.Story.Title = "source title from cluster"
	st.Story.SourceCount = 3
	st.ArticleIDs = append(st.ArticleIDs, "a-second")
	st.SourceIDs = append(st.SourceIDs, "src2")
	article, _ := articleRecordForTest(st.Story.Id, "a-second")
	if err := c.persist(ctx, st, article); err != nil {
		t.Fatal(err)
	}
	liveKV, _ := b.KV(ctx, bus.KVBucketLiveState)
	got, ok := live.LoadStory(ctx, liveKV, st.Story.Id)
	if !ok {
		t.Fatal("derived story missing")
	}
	if got.Title != "Editor title" || got.TitleSource != "editor" {
		t.Fatalf("content update reverted editor title: %q source=%q", got.Title, got.TitleSource)
	}
	if got.SourceCount != 3 || len(got.Locations) != 1 {
		t.Fatalf("cluster-owned fields changed: source=%d locations=%d", got.SourceCount, len(got.Locations))
	}
}

// Cooling republishes the story; it must apply the marker title first.
func TestClusterCoolingKeepsEditorTitle(t *testing.T) {
	b, c, st, ctx := seedEditorTitleStory(t, time.Now().UTC())
	st.CooledAt = time.Unix(st.UpdatedAt, 0).Add(-time.Hour).UnixNano()
	st.Story.Heat = 0.8
	st.Story.UpdatedAt = time.Now().Add(-time.Hour).Unix()
	c.coolStories(ctx)
	liveKV, _ := b.KV(ctx, bus.KVBucketLiveState)
	got, ok := live.LoadStory(ctx, liveKV, st.Story.Id)
	if !ok {
		t.Fatal("cooled story missing")
	}
	if got.Title != "Editor title" || got.TitleSource != "editor" {
		t.Fatalf("cooling reverted editor title: %q source=%q", got.Title, got.TitleSource)
	}
	if got.Heat >= 0.8 {
		t.Fatalf("heat did not cool: %f", got.Heat)
	}
}

func articleRecordForTest(storyID, articleID string) (*dorv1.Article, bool) {
	return &dorv1.Article{Id: articleID, SourceId: "src2", Title: "second", PublishedAt: time.Now().Unix()}, true
}
