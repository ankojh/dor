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

func fixedCluster(t *testing.T, b *bus.Bus, now time.Time) *clusterer {
	t.Helper()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{ClusterNow: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return agent.(*clusterer)
}

func seedClusterPair(t *testing.T, ctx context.Context, b *bus.Bus, c *clusterer, now time.Time) (*dorv1.Article, *dorv1.Article, string) {
	t.Helper()
	a1 := goldenArticle("cs-a1", "src1", "Bridge closure strands commuters", "A bridge closure stranded commuters.", now.Unix())
	a2 := goldenArticle("cs-a2", "src2", a1.Title, a1.Lede, now.Unix())
	place := goldenPlace("capital", 40, -74)
	class := goldenClass(dorv1.Category_CATEGORY_DISASTER, "Transport Authority")
	for _, article := range []*dorv1.Article{a1, a2} {
		putJoinState(t, ctx, b, article.Id, joinState{Article: article, Place: place, Class: class, Vector: &dorv1.EmbeddedArticle{Article: article, Vector: goldenBase()}})
	}
	processGoldenJoin(t, ctx, c, a1.Id)
	sid := c.articleStory[a1.Id]
	return a1, a2, sid
}

func mergeForCrash(c *clusterer, st *storyState, article *dorv1.Article, place *dorv1.PlacedArticle, category dorv1.Category, now time.Time) {
	c.mergeStory(st, article, place, category, goldenBase(), []string{"Transport Authority"}, now)
	st.ArticleIDs = appendUnique(st.ArticleIDs, article.Id)
	st.SourceIDs = appendUnique(st.SourceIDs, article.SourceId)
	st.UpdatedAt = now.Unix()
	st.Story.UpdatedAt = st.UpdatedAt
	st.Story.SourceCount = uint32(len(st.SourceIDs))
	st.ExpiresAt = now.Add(clusterStoryTTL)
}

// TestClusterCheckpointCrashWindow covers the two sides of the single
// authoritative write. After cs, derived keys are republished from it and the
// merge is not applied twice. Before cs, partial derived writes are discarded.
func TestClusterCheckpointCrashWindow(t *testing.T) {
	t.Run("crash after checkpoint before derived writes", func(t *testing.T) {
		b := durabilityBus(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		now := time.Now().UTC()
		c := fixedCluster(t, b, now)
		a1, a2, sid := seedClusterPair(t, ctx, b, c, now)
		_ = a1
		place := goldenPlace("capital", 40, -74)
		class := goldenClass(dorv1.Category_CATEGORY_DISASTER, "Transport Authority")

		old := c.stories[sid]
		mergeForCrash(c, old, a2, place, class.Category, now)
		expectedHeat := old.Story.Heat
		liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
		if err != nil {
			t.Fatal(err)
		}
		// Record the new membership authoritatively, then stop before the
		// derived story/storymeta writes.
		if _, err := c.writeCheckpoint(ctx, liveKV, old); err != nil {
			t.Fatal(err)
		}

		restarted := fixedCluster(t, b, now)
		if err := restarted.load(ctx); err != nil {
			t.Fatal(err)
		}
		recovered := restarted.stories[sid]
		if recovered == nil {
			t.Fatal("checkpoint story missing after restart")
		}
		if len(recovered.ArticleIDs) != 2 {
			t.Fatalf("recovered membership = %v, want both articles from cs", recovered.ArticleIDs)
		}
		if restarted.resume[a2.Id] != sid {
			t.Fatalf("recovered article %s is not resumable", a2.Id)
		}
		metaEntry, err := liveKV.Get(ctx, live.StoryMetaKey(sid))
		if err != nil {
			t.Fatal(err)
		}
		var meta storyMeta
		if err := json.Unmarshal(metaEntry.Value(), &meta); err != nil {
			t.Fatal(err)
		}
		if len(meta.ArticleIDs) != 2 {
			t.Fatalf("derived storymeta was not republished from cs: %v", meta.ArticleIDs)
		}
		storyEntry, err := liveKV.Get(ctx, live.StoryKey(sid))
		if err != nil {
			t.Fatal(err)
		}
		var derived dorv1.Story
		if err := proto.Unmarshal(storyEntry.Value(), &derived); err != nil {
			t.Fatal(err)
		}
		if derived.UpdatedAt != recovered.Story.UpdatedAt {
			t.Fatalf("derived story not republished from cs: updated %d vs %d", derived.UpdatedAt, recovered.Story.UpdatedAt)
		}

		processGoldenJoin(t, ctx, restarted, a2.Id)
		if got := restarted.stories[sid].Story.Heat; got != expectedHeat {
			t.Fatalf("resume merged twice: heat %f -> %f", expectedHeat, got)
		}
		if ids := restarted.stories[sid].ArticleIDs; len(ids) != 2 {
			t.Fatalf("resumed membership = %v, want each article once", ids)
		}
	})

	t.Run("crash before checkpoint discards derived writes", func(t *testing.T) {
		b := durabilityBus(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		now := time.Now().UTC()
		c := fixedCluster(t, b, now)
		_, a2, sid := seedClusterPair(t, ctx, b, c, now)
		place := goldenPlace("capital", 40, -74)
		class := goldenClass(dorv1.Category_CATEGORY_DISASTER, "Transport Authority")

		old := c.stories[sid]
		mergeForCrash(c, old, a2, place, class.Category, now)
		liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
		if err != nil {
			t.Fatal(err)
		}
		// A crash before the authoritative cs write: only the derived keys got
		// the second article.
		sd, err := proto.Marshal(old.Story)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := liveKV.Put(ctx, live.StoryKey(sid), sd); err != nil {
			t.Fatal(err)
		}
		metaData, err := json.Marshal(storyMeta{ID: sid, ArticleIDs: old.ArticleIDs, SourceIDs: old.SourceIDs, Embedding: old.Embedding, Entities: old.Entities, FirstSeen: old.FirstSeen, UpdatedAt: old.UpdatedAt})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := liveKV.Put(ctx, live.StoryMetaKey(sid), metaData); err != nil {
			t.Fatal(err)
		}

		restarted := fixedCluster(t, b, now)
		if err := restarted.load(ctx); err != nil {
			t.Fatal(err)
		}
		recovered := restarted.stories[sid]
		if recovered == nil {
			t.Fatal("checkpoint story missing after restart")
		}
		if len(recovered.ArticleIDs) != 1 || recovered.ArticleIDs[0] != old.ArticleIDs[0] {
			t.Fatalf("derived pre-checkpoint write was not discarded: %v", recovered.ArticleIDs)
		}
		metaEntry, err := liveKV.Get(ctx, live.StoryMetaKey(sid))
		if err != nil {
			t.Fatal(err)
		}
		var meta storyMeta
		if err := json.Unmarshal(metaEntry.Value(), &meta); err != nil {
			t.Fatal(err)
		}
		if len(meta.ArticleIDs) != 1 {
			t.Fatalf("derived storymeta kept the pre-checkpoint article: %v", meta.ArticleIDs)
		}

		processGoldenJoin(t, ctx, restarted, a2.Id)
		if len(restarted.stories) != 1 {
			t.Fatalf("retry after discarded crash formed %d stories, want 1", len(restarted.stories))
		}
		if ids := restarted.stories[sid].ArticleIDs; len(ids) != 2 {
			t.Fatalf("retry membership = %v, want one merge of both articles", ids)
		}
	})
}

func TestClusterHazardRevisionsEndToEnd(t *testing.T) {
	t.Run("same event revisions form one story", func(t *testing.T) {
		b := durabilityBus(t)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		norm, err := NewNormalizer(b, NewMetrics(), testLogger(), NormalizerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		runner := startRunner(t, norm)
		defer stopRunner(t, runner)

		now := time.Now().Unix()
		lastSeq := uint64(0)
		for i, revision := range []string{"1", "2"} {
			raw := &dorv1.RawArticle{
				Id:          "haz-e2e-" + revision,
				SourceId:    "usgs",
				Url:         "https://example.test/hazard-e2e",
				Title:       "Magnitude 6.1 earthquake strikes the coastal region",
				Summary:     "A magnitude 6.1 earthquake struck the coastal region.",
				PublishedAt: now,
				FetchedAt:   now,
				Lang:        "en",
				Hints:       &dorv1.RawHints{Hazard: "earthquake", EventId: "eq-e2e-1", Revision: revision},
			}
			seq := publishRawArticle(t, ctx, b, "haz-e2e-msg-"+revision, raw)
			if seq > lastSeq {
				lastSeq = seq
			}
			_ = i
		}
		waitNormalizerAcked(t, ctx, b, lastSeq)
		cleans := collectCleanArticles(t, ctx, b)
		if len(cleans) != 2 {
			t.Fatalf("normalizer produced %d clean articles, want 2 hazard revisions", len(cleans))
		}

		cluster := fixedCluster(t, b, time.Now().UTC())
		for _, clean := range cleans {
			putJoinState(t, ctx, b, clean.Id, joinState{
				Article: clean,
				Place:   goldenPlace("Coastal Region", 40, -74),
				Class:   goldenClass(dorv1.Category_CATEGORY_DISASTER),
			})
			processGoldenJoin(t, ctx, cluster, clean.Id)
		}
		if got := len(cluster.stories); got != 1 {
			t.Fatalf("hazard revisions formed %d stories, want 1", got)
		}
		var storyID string
		for id, st := range cluster.stories {
			storyID = id
			if len(st.ArticleIDs) != 2 {
				t.Fatalf("story %s articles = %v, want both revisions", id, st.ArticleIDs)
			}
		}
		evKey := eventMapKey("usgs", "eq-e2e-1")
		liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := liveKV.Get(ctx, evKey)
		if err != nil {
			t.Fatalf("durable hazard event map missing: %v", err)
		}
		if string(entry.Value()) != storyID {
			t.Fatalf("event map = %q, want %q", entry.Value(), storyID)
		}
	})

	t.Run("different event ids stay apart", func(t *testing.T) {
		b := durabilityBus(t)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		norm, err := NewNormalizer(b, NewMetrics(), testLogger(), NormalizerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		runner := startRunner(t, norm)
		defer stopRunner(t, runner)

		now := time.Now().Unix()
		lastSeq := uint64(0)
		inputs := []struct {
			url, eventID, title string
		}{
			{"https://example.test/hazard-one", "eq-e2e-2", "Magnitude 6.1 earthquake strikes the coastal region"},
			{"https://example.test/hazard-two", "eq-e2e-3", "Wildfire evacuations ordered in the mountain county"},
		}
		for i, in := range inputs {
			raw := &dorv1.RawArticle{
				Id:          "haz-distinct-" + in.eventID,
				SourceId:    "usgs",
				Url:         in.url,
				Title:       in.title,
				Summary:     in.title + ".",
				PublishedAt: now,
				FetchedAt:   now,
				Lang:        "en",
				Hints:       &dorv1.RawHints{Hazard: "hazard", EventId: in.eventID, Revision: "1"},
			}
			seq := publishRawArticle(t, ctx, b, "haz-distinct-msg-"+in.eventID, raw)
			if seq > lastSeq {
				lastSeq = seq
			}
			_ = i
		}
		waitNormalizerAcked(t, ctx, b, lastSeq)
		cleans := collectCleanArticles(t, ctx, b)
		if len(cleans) != 2 {
			t.Fatalf("normalizer produced %d clean articles, want 2", len(cleans))
		}
		cluster := fixedCluster(t, b, time.Now().UTC())
		for _, clean := range cleans {
			putJoinState(t, ctx, b, clean.Id, joinState{
				Article: clean,
				Place:   goldenPlace("Somewhere", 40, -74),
				Class:   goldenClass(dorv1.Category_CATEGORY_DISASTER),
			})
			processGoldenJoin(t, ctx, cluster, clean.Id)
		}
		if got := len(cluster.stories); got != 2 {
			t.Fatalf("different hazard events formed %d stories, want 2", got)
		}
	})
}

func collectCleanArticles(t *testing.T, ctx context.Context, b *bus.Bus) []*dorv1.Article {
	t.Helper()
	bodies := collectSubject(t, b, bus.SubjectArticleClean)
	out := make([]*dorv1.Article, 0, len(bodies))
	for _, body := range bodies {
		var article dorv1.Article
		if err := proto.Unmarshal([]byte(body), &article); err != nil {
			t.Fatalf("decode clean article: %v", err)
		}
		out = append(out, &article)
	}
	return out
}
