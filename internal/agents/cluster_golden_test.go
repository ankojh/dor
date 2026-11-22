package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// goldenVec returns a 384-d unit vector whose cosine with goldenBase() is cos.
func goldenVec(cos float64) []float32 {
	v := make([]float32, embeddingDim)
	v[0] = float32(cos)
	v[1] = float32(math.Sqrt(math.Max(0, 1-cos*cos)))
	return v
}

func goldenBase() []float32 { return goldenVec(1) }

func goldenArticle(id, source, title, lede string, published int64) *dorv1.Article {
	return &dorv1.Article{
		Id:          id,
		SourceId:    source,
		Url:         "https://example.test/" + id,
		Title:       title,
		Lede:        lede,
		PublishedAt: published,
		Lang:        "en",
	}
}

func goldenPlace(name string, lat, lon float32) *dorv1.PlacedArticle {
	return &dorv1.PlacedArticle{
		Locations: []*dorv1.Location{{
			PlaceId:   name,
			Name:      name,
			Lat:       lat,
			Lon:       lon,
			Precision: 1,
			Role:      dorv1.Role_ROLE_ORIGIN,
			Weight:    1,
		}},
		Primary: 0,
	}
}

func goldenClass(category dorv1.Category, entities ...string) *dorv1.ClassifiedArticle {
	return &dorv1.ClassifiedArticle{Category: category, Entities: entities}
}

func putJoinState(t *testing.T, ctx context.Context, b *bus.Bus, id string, state joinState) {
	t.Helper()
	kv, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatalf("joins kv: %v", err)
	}
	if state.UpdatedAt == 0 {
		state.UpdatedAt = time.Now().UTC().UnixNano()
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("encode join: %v", err)
	}
	if _, err := kv.Put(ctx, "join."+id, data); err != nil {
		t.Fatalf("write join %s: %v", id, err)
	}
}

func newGoldenClusterer(t *testing.T, b *bus.Bus) *clusterer {
	t.Helper()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{})
	if err != nil {
		t.Fatalf("new clusterer: %v", err)
	}
	return agent.(*clusterer)
}

func processGoldenJoin(t *testing.T, ctx context.Context, c *clusterer, id string) {
	t.Helper()
	if err := c.process(ctx, id); err != nil {
		t.Fatalf("process %s: %v", id, err)
	}
}

func clusterStoryCount(c *clusterer) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.stories)
}

func clusterArticles(c *clusterer) map[string][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]string, len(c.stories))
	for id, st := range c.stories {
		out[id] = append([]string(nil), st.ArticleIDs...)
	}
	return out
}

// TestClustererGoldenNoTwinsNoWrongMerges pins the story-forming contract:
// reports of one event across sources merge, distinct events do not, and the
// deterministic (no-model) path stays conservative when embeddings are absent.
func TestClustererGoldenNoTwinsNoWrongMerges(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	published := time.Now().Add(-time.Minute).Unix()

	type goldenArticleCase struct {
		article  *dorv1.Article
		place    *dorv1.PlacedArticle
		class    *dorv1.ClassifiedArticle
		vector   []float32
		noVector bool
	}
	cases := []struct {
		name         string
		articles     []goldenArticleCase
		wantStories  int
		wantArticles []string
	}{
		{
			name: "same event across sources merges",
			articles: []goldenArticleCase{
				{
					article: goldenArticle("gold-a1", "wire-one", "Fire breaks out at central market", "A fire broke out at the central market on Tuesday.", published),
					place:   goldenPlace("Springfield", 39.8, -89.65),
					class:   goldenClass(dorv1.Category_CATEGORY_DISASTER, "Springfield Fire Department"),
					vector:  goldenBase(),
				},
				{
					article: goldenArticle("gold-a2", "wire-two", "Fire breaks out at central market", "Firefighters responded to a blaze at the central market.", published+60),
					place:   goldenPlace("Springfield", 39.8, -89.65),
					class:   goldenClass(dorv1.Category_CATEGORY_DISASTER, "Springfield Fire Department"),
					vector:  goldenVec(0.97),
				},
			},
			wantStories:  1,
			wantArticles: []string{"gold-a1", "gold-a2"},
		},
		{
			name: "same topic but different event does not merge",
			articles: []goldenArticleCase{
				{
					article: goldenArticle("gold-b1", "wire-one", "Central bank raises interest rates to six percent", "The central bank raised its benchmark rate.", published),
					place:   goldenPlace("New York", 40.71, -74.0),
					class:   goldenClass(dorv1.Category_CATEGORY_ECONOMY, "Central Bank"),
					vector:  goldenBase(),
				},
				{
					article: goldenArticle("gold-b2", "wire-two", "Central bank opens new branch in Osaka", "The central bank opened a regional branch in Osaka.", published+600),
					place:   goldenPlace("Osaka", 34.69, 135.5),
					class:   goldenClass(dorv1.Category_CATEGORY_ECONOMY, "Central Bank"),
					vector:  goldenVec(0.78),
				},
			},
			wantStories: 2,
		},
		{
			name: "no embedding still clusters by title and entity",
			articles: []goldenArticleCase{
				{
					article:  goldenArticle("gold-c1", "wire-one", "Flood warning issued for the coastal district", "Residents were told to prepare for flooding.", published),
					place:    goldenPlace("Coastal District", 29.76, -95.36),
					class:    goldenClass(dorv1.Category_CATEGORY_CLIMATE, "Coastal District"),
					noVector: true,
				},
				{
					article:  goldenArticle("gold-c2", "wire-two", "Flood warning issued for the coastal district", "A flood warning covers the coastal district overnight.", published+120),
					place:    goldenPlace("Coastal District", 29.76, -95.36),
					class:    goldenClass(dorv1.Category_CATEGORY_CLIMATE, "Coastal District"),
					noVector: true,
				},
			},
			wantStories:  1,
			wantArticles: []string{"gold-c1", "gold-c2"},
		},
		{
			name: "no embedding different events stay apart",
			articles: []goldenArticleCase{
				{
					article:  goldenArticle("gold-d1", "wire-one", "Flood warning issued for the coastal district", "Residents were told to prepare for flooding.", published),
					place:    goldenPlace("Coastal District", 29.76, -95.36),
					class:    goldenClass(dorv1.Category_CATEGORY_CLIMATE, "Coastal District"),
					noVector: true,
				},
				{
					article:  goldenArticle("gold-d2", "wire-two", "Wildfire evacuation ordered in the mountain county", "Officials ordered evacuations in the mountain county.", published+120),
					place:    goldenPlace("Mountain County", 39.55, -119.8),
					class:    goldenClass(dorv1.Category_CATEGORY_DISASTER, "Mountain County"),
					noVector: true,
				},
			},
			wantStories: 2,
		},
		{
			name: "same title in far apart places does not merge",
			articles: []goldenArticleCase{
				{
					article: goldenArticle("gold-e1", "wire-one", "Mayor announces new transit plan", "The mayor announced a transit plan for the city.", published),
					place:   goldenPlace("San Francisco", 37.77, -122.42),
					class:   goldenClass(dorv1.Category_CATEGORY_POLITICS, "Mayor"),
					vector:  goldenBase(),
				},
				{
					article: goldenArticle("gold-e2", "wire-two", "Mayor announces new transit plan", "The mayor announced a transit plan for the city.", published+60),
					place:   goldenPlace("Tokyo", 35.68, 139.69),
					class:   goldenClass(dorv1.Category_CATEGORY_POLITICS, "Mayor"),
					vector:  goldenVec(0.5),
				},
			},
			wantStories: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newGoldenClusterer(t, b)
			for _, articleCase := range tc.articles {
				id := articleCase.article.Id
				state := joinState{
					Article: articleCase.article,
					Place:   articleCase.place,
					Class:   articleCase.class,
				}
				if !articleCase.noVector {
					state.Vector = &dorv1.EmbeddedArticle{Article: articleCase.article, Vector: articleCase.vector}
				}
				putJoinState(t, ctx, b, id, state)
				processGoldenJoin(t, ctx, c, id)
			}
			if got := clusterStoryCount(c); got != tc.wantStories {
				t.Fatalf("stories = %d, want %d (%v)", got, tc.wantStories, clusterArticles(c))
			}
			if tc.wantArticles != nil {
				found := false
				for _, ids := range clusterArticles(c) {
					if len(ids) == len(tc.wantArticles) && ids[0] == tc.wantArticles[0] && ids[1] == tc.wantArticles[1] {
						found = true
					}
				}
				if !found {
					t.Fatalf("articles %v did not land in one story: %v", tc.wantArticles, clusterArticles(c))
				}
			}
		})
	}
}

// TestClustererConcurrentDuplicatesCannotCreateTwins drives the processor entry
// point from two goroutines. The single-writer mutex must serialize them so the
// second report merges into the first story instead of forming a twin.
func TestClustererConcurrentDuplicatesCannotCreateTwins(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newGoldenClusterer(t, b)

	published := time.Now().Add(-time.Minute).Unix()
	for i, id := range []string{"twin-a", "twin-b"} {
		article := goldenArticle(id, fmt.Sprintf("wire-%d", i), "Bridge collapse closes harbor road", "A bridge collapsed and closed the harbor road.", published)
		putJoinState(t, ctx, b, id, joinState{
			Article: article,
			Place:   goldenPlace("Harbor", 47.6, -122.33),
			Class:   goldenClass(dorv1.Category_CATEGORY_DISASTER, "Harbor Authority"),
			Vector:  &dorv1.EmbeddedArticle{Article: article, Vector: goldenVec(1 - 0.01*float64(i))},
		})
	}

	errCh := make(chan error, 2)
	for _, id := range []string{"twin-a", "twin-b"} {
		go func(id string) { errCh <- c.process(ctx, id) }(id)
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	if got := clusterStoryCount(c); got != 1 {
		t.Fatalf("twin stories formed: %d (%v)", got, clusterArticles(c))
	}
	for _, ids := range clusterArticles(c) {
		if len(ids) != 2 {
			t.Fatalf("story articles = %v, want both duplicates", ids)
		}
	}
}

// TestClustererRedeliveredJoinIsIdempotent re-writes a join after its story
// committed. The article must not be merged a second time and no twin story may
// appear.
func TestClustererRedeliveredJoinIsIdempotent(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newGoldenClusterer(t, b)

	published := time.Now().Add(-time.Minute).Unix()
	article := goldenArticle("idem-a", "wire-one", "Storm closes airport runways", "A storm closed the airport runways.", published)
	state := joinState{
		Article: article,
		Place:   goldenPlace("Airport", 51.47, -0.45),
		Class:   goldenClass(dorv1.Category_CATEGORY_DISASTER, "Airport Authority"),
		Vector:  &dorv1.EmbeddedArticle{Article: article, Vector: goldenBase()},
	}
	putJoinState(t, ctx, b, article.Id, state)
	processGoldenJoin(t, ctx, c, article.Id)
	first := clusterArticles(c)
	if len(first) != 1 {
		t.Fatalf("first pass stories = %v", first)
	}

	putJoinState(t, ctx, b, article.Id, state)
	processGoldenJoin(t, ctx, c, article.Id)

	if got := clusterStoryCount(c); got != 1 {
		t.Fatalf("redelivery created %d stories", got)
	}
	for id, ids := range clusterArticles(c) {
		if len(ids) != 1 || ids[0] != article.Id {
			t.Fatalf("story %s articles = %v, want exactly [%s]", id, ids, article.Id)
		}
	}
	c.mu.Lock()
	gotStory := c.articleStory[article.Id]
	c.mu.Unlock()
	if gotStory == "" {
		t.Fatal("article lost its story assignment")
	}
}

// TestClustererDurableJoinsSurviveRestart simulates a cluster stop: complete
// joins are already in the durable bucket and no story exists yet. On start the
// sweeper must drain every join into a story and leave no join records behind.
func TestClustererDurableJoinsSurviveRestart(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	published := time.Now().Add(-time.Minute).Unix()

	articles := []*dorv1.Article{
		goldenArticle("dur-a", "wire-one", "Ferry service resumes after storm", "Ferry service resumed after the storm passed.", published),
		goldenArticle("dur-b", "wire-two", "Ferry service resumes after storm", "The ferry resumed its normal schedule.", published+30),
	}
	for i, article := range articles {
		putJoinState(t, ctx, b, article.Id, joinState{
			Article: article,
			Place:   goldenPlace("Harbor", 47.6, -122.33),
			Class:   goldenClass(dorv1.Category_CATEGORY_CLIMATE, "Harbor Authority"),
			Vector:  &dorv1.EmbeddedArticle{Article: article, Vector: goldenVec(1 - 0.01*float64(i))},
		})
	}

	cluster := newGoldenClusterer(t, b)
	runner := startRunner(t, cluster)
	defer stopRunner(t, runner)

	waitFor(t, "all durable joins drained into stories", func() bool {
		kv, err := b.KV(ctx, bus.KVBucketJoins)
		if err != nil {
			return false
		}
		lister, err := kv.ListKeysFiltered(ctx, "join.*")
		if err != nil {
			return false
		}
		defer func() { _ = lister.Stop() }()
		for range lister.Keys() {
			return false
		}
		return clusterStoryCount(cluster) == 1
	})

	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := joins.Keys(ctx)
	if err != nil && !isKVNotFound(err) {
		t.Fatal(err)
	}
	for _, key := range keys {
		if len(key) >= 5 && key[:5] == "join." {
			t.Fatalf("join %s survived after its story committed", key)
		}
	}
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	for _, article := range articles {
		if _, err := liveKV.Get(ctx, "storymap."+article.Id); err != nil {
			t.Fatalf("article %s has no story: %v", article.Id, err)
		}
	}
}

// TestClustererResumesStoryMetaWithoutCompletion reproduces a crash between the
// storymeta write and the output commit: a durable join and storymeta exist with
// no completion record. The restart must re-emit for the same story, then delete
// the join, without merging the article twice or forming a twin.
func TestClustererResumesStoryMetaWithoutCompletion(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	now := time.Now().UTC()
	article := goldenArticle("resume-a", "wire-one", "Grid operator restores power", "Power was restored across the region.", now.Add(-time.Minute).Unix())
	story := &dorv1.Story{
		Id:          newStoryID(article.Id),
		Title:       article.Title,
		Category:    dorv1.Category_CATEGORY_ECONOMY,
		SourceCount: 1,
		Heat:        0.6,
		FirstSeen:   now.Add(-5 * time.Minute).Unix(),
		UpdatedAt:   now.Add(-time.Minute).Unix(),
		Locations: []*dorv1.Location{{
			PlaceId: "Grid", Name: "Grid", Lat: 40, Lon: -75, Role: dorv1.Role_ROLE_ORIGIN, Weight: 1,
		}},
	}
	meta := storyMeta{
		ID:         story.Id,
		ArticleIDs: []string{article.Id},
		SourceIDs:  []string{article.SourceId},
		Embedding:  goldenBase(),
		Entities:   []string{"Grid Operator"},
		FirstSeen:  story.FirstSeen,
		UpdatedAt:  story.UpdatedAt,
	}
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	storyData, err := proto.Marshal(story)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := liveKV.Put(ctx, "story."+story.Id, storyData); err != nil {
		t.Fatal(err)
	}
	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := liveKV.Put(ctx, "storymeta."+story.Id, metaData); err != nil {
		t.Fatal(err)
	}
	putJoinState(t, ctx, b, article.Id, joinState{
		Article: article,
		Place:   goldenPlace("Grid", 40, -75),
		Class:   goldenClass(dorv1.Category_CATEGORY_ECONOMY, "Grid Operator"),
		Vector:  &dorv1.EmbeddedArticle{Article: article, Vector: goldenBase()},
	})

	cluster := newGoldenClusterer(t, b)
	runner := startRunner(t, cluster)
	defer stopRunner(t, runner)

	waitFor(t, "uncommitted storymeta resume completes", func() bool {
		joins, err := b.KV(ctx, bus.KVBucketJoins)
		if err != nil {
			return false
		}
		if _, err := joins.Get(ctx, commitKey(story.Id)); err != nil {
			return false
		}
		if _, err := joins.Get(ctx, "join."+article.Id); err == nil {
			return false
		}
		return true
	})

	if got := clusterStoryCount(cluster); got != 1 {
		t.Fatalf("resume formed %d stories, want 1", got)
	}
	cluster.mu.Lock()
	st := cluster.stories[story.Id]
	cluster.mu.Unlock()
	if st == nil || st.Story.Id != story.Id {
		t.Fatalf("resumed story is missing; stories=%v", clusterArticles(cluster))
	}
	if len(st.ArticleIDs) != 1 || st.ArticleIDs[0] != article.Id {
		t.Fatalf("resumed story articles = %v, want one %s", st.ArticleIDs, article.Id)
	}
	entry, err := liveKV.Get(ctx, "storymeta."+story.Id)
	if err != nil {
		t.Fatal(err)
	}
	var gotMeta storyMeta
	if err := json.Unmarshal(entry.Value(), &gotMeta); err != nil {
		t.Fatal(err)
	}
	if len(gotMeta.ArticleIDs) != 1 {
		t.Fatalf("storymeta articles = %v, want no duplicate merge", gotMeta.ArticleIDs)
	}
	storyKeys, err := liveKV.Keys(ctx)
	if err != nil && !isKVNotFound(err) {
		t.Fatal(err)
	}
	for _, key := range storyKeys {
		if key == "story."+story.Id {
			continue
		}
		if len(key) > 6 && key[:6] == "story." {
			t.Fatalf("resume created a twin story key %s", key)
		}
	}
}

// TestClustererCoolingDropsHeatWithoutMovingExpiry pins the conservative cooling
// contract: Heat decays, but UpdatedAt (the expiry anchor) and ExpiresAt do not.
func TestClustererCoolingDropsHeatWithoutMovingExpiry(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fixed := time.Now().UTC()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{
		ClusterNow:          func() time.Time { return fixed },
		ClusterHeatHalfLife: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := agent.(*clusterer)
	st := &storyState{
		Story: &dorv1.Story{
			Id:        "cool-story",
			Title:     "Quiet story",
			Heat:      0.8,
			FirstSeen: fixed.Add(-4 * time.Hour).Unix(),
			UpdatedAt: fixed.Add(-2 * time.Hour).Unix(),
		},
		ArticleIDs: []string{"cool-article"},
		SourceIDs:  []string{"wire"},
		UpdatedAt:  fixed.Add(-2 * time.Hour).Unix(),
		ExpiresAt:  fixed.Add(46 * time.Hour),
		CooledAt:   fixed.Add(-2 * time.Hour).UnixNano(),
	}
	c.stories[st.Story.Id] = st

	c.coolStories(ctx)

	if st.Story.Heat >= 0.8 {
		t.Fatalf("heat did not cool: %v", st.Story.Heat)
	}
	if st.Story.Heat <= clusterHeatFloor {
		t.Fatalf("heat collapsed instead of fading: %v", st.Story.Heat)
	}
	if st.Story.UpdatedAt != fixed.Add(-2*time.Hour).Unix() {
		t.Fatalf("cooling moved UpdatedAt to %d", st.Story.UpdatedAt)
	}
	if !st.ExpiresAt.Equal(fixed.Add(46 * time.Hour)) {
		t.Fatalf("cooling moved ExpiresAt to %s", st.ExpiresAt)
	}
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := liveKV.Get(ctx, live.StoryKey(st.Story.Id))
	if err != nil {
		t.Fatalf("cooled story was not persisted: %v", err)
	}
	var persisted dorv1.Story
	if err := proto.Unmarshal(entry.Value(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Heat != st.Story.Heat {
		t.Fatalf("persisted heat = %v, want %v", persisted.Heat, st.Story.Heat)
	}
	if persisted.UpdatedAt != st.Story.UpdatedAt {
		t.Fatalf("persisted UpdatedAt = %d, want %d", persisted.UpdatedAt, st.Story.UpdatedAt)
	}
}

// TestClustererExpiryDeletesStateAndLinks covers the cache-consistency side of
// story expiry: the story, storymeta, completion record and touching links all
// leave live_state.
func TestClustererExpiryDeletesStateAndLinks(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UTC()
	agent, err := NewClusterer(b, NewMetrics(), testLogger(), Deps{ClusterNow: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	c := agent.(*clusterer)
	st := &storyState{
		Story:      &dorv1.Story{Id: "expired-story", Title: "Old", UpdatedAt: now.Add(-49 * time.Hour).Unix()},
		ArticleIDs: []string{"expired-article"},
		UpdatedAt:  now.Add(-49 * time.Hour).Unix(),
		ExpiresAt:  now.Add(-time.Hour),
	}
	c.stories[st.Story.Id] = st
	c.articleStory["expired-article"] = st.Story.Id
	c.resume["expired-article"] = st.Story.Id
	c.lastLinkSweep = now.Add(-2 * time.Minute)

	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	storyData, err := proto.Marshal(st.Story)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"story." + st.Story.Id, "storymeta." + st.Story.Id} {
		if _, err := liveKV.Put(ctx, key, storyData); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := joins.Put(ctx, commitKey(st.Story.Id), []byte(`{"story_id":"expired-story"}`)); err != nil {
		t.Fatal(err)
	}
	link := &dorv1.Link{A: "expired-story", B: "live-story", Strength: 0.7, Confidence: 0.8}
	if !live.PutLink(ctx, liveKV, link) {
		t.Fatal("seed link")
	}

	c.expireStories(ctx)

	if _, err := liveKV.Get(ctx, "story."+st.Story.Id); !isKVNotFound(err) {
		t.Fatalf("expired story remains: %v", err)
	}
	if _, err := liveKV.Get(ctx, "storymeta."+st.Story.Id); !isKVNotFound(err) {
		t.Fatalf("expired storymeta remains: %v", err)
	}
	if _, err := joins.Get(ctx, commitKey(st.Story.Id)); !isKVNotFound(err) {
		t.Fatalf("expired completion record remains: %v", err)
	}
	if _, ok := live.LoadLink(ctx, liveKV, link.A, link.B); ok {
		t.Fatal("link touching the expired story remains")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stories[st.Story.Id] != nil {
		t.Fatal("expired story remains in memory")
	}
	if c.articleStory["expired-article"] != "" || c.resume["expired-article"] != "" {
		t.Fatal("expired article membership remains in memory")
	}
}
