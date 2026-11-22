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

// A completion marker records only articles whose outputs actually committed.
// It must union with the previous marker rather than copying storymeta, which
// can contain an article whose emission is still pending.
func TestClusterCommitMarkerUnionsOnlyEmittedArticles(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	cluster := agent.(*clusterer)
	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	for _, articleID := range []string{"a1", "a2", "a1"} {
		if err := cluster.markCommitted(ctx, joins, "s-x", articleID); err != nil {
			t.Fatal(err)
		}
	}
	entry, err := joins.Get(ctx, commitKey("s-x"))
	if err != nil {
		t.Fatal(err)
	}
	var rec storyCommit
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.ArticleIDs) != 2 || rec.ArticleIDs[0] != "a1" || rec.ArticleIDs[1] != "a2" {
		t.Fatalf("completion articles = %v, want [a1 a2]", rec.ArticleIDs)
	}

	// Load treats only the marked article as committed; the other stays
	// resumable even though storymeta already lists it.
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	story := &dorv1.Story{Id: "s-load", Title: "Load", Category: dorv1.Category_CATEGORY_OTHER, SourceCount: 1, Heat: 0.5, FirstSeen: now.Unix(), UpdatedAt: now.Unix()}
	sd, _ := proto.Marshal(story)
	meta := storyMeta{ID: story.Id, ArticleIDs: []string{"a1", "a2"}, SourceIDs: []string{"src"}, FirstSeen: story.FirstSeen, UpdatedAt: story.UpdatedAt}
	md, _ := json.Marshal(meta)
	if _, err := liveKV.Put(ctx, live.StoryKey(story.Id), sd); err != nil {
		t.Fatal(err)
	}
	if _, err := liveKV.Put(ctx, live.StoryMetaKey(story.Id), md); err != nil {
		t.Fatal(err)
	}
	if err := cluster.markCommitted(ctx, joins, story.Id, "a1"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.load(ctx); err != nil {
		t.Fatal(err)
	}
	if cluster.articleStory["a1"] != story.Id {
		t.Fatalf("a1 committed mapping = %q, want %q", cluster.articleStory["a1"], story.Id)
	}
	if cluster.resume["a2"] != story.Id {
		t.Fatalf("a2 resume mapping = %q, want %q", cluster.resume["a2"], story.Id)
	}
	if cluster.articleStory["a2"] != "" {
		t.Fatalf("a2 was marked committed without an output: %q", cluster.articleStory["a2"])
	}
}
