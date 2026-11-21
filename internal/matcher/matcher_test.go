package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func matcherTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func matcherTestService(t *testing.T, b *bus.Bus, store *alert.Store) *Service {
	t.Helper()
	return New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, embed.New("", 0), store, configForMatcherTest())
}

func configForMatcherTest() config.Thresholds { return config.DefaultThresholds() }

func TestSetClientPromptsImmediatelyUsesCanceledRegistrationContext(t *testing.T) {
	b := matcherTestBus(t)
	s := matcherTestService(t, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	s.SetClientPrompts(ctx, "client", []string{"  coastal wildfire  "})
	scores, approx := s.ArticleScores(context.Background(), &dorv1.Article{Id: "article", Title: "Coastal wildfire spreads"})
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("first match took %s, want <=2s", elapsed)
	}
	if scores["coastal wildfire"] < .99 {
		t.Fatalf("immediate match scores = %v, want lexical match", scores)
	}
	if len(approx) != 1 || approx[0] != "coastal wildfire" {
		t.Fatalf("approx prompts = %v, want [coastal wildfire]", approx)
	}
}

func TestBacktestScansWholeWindowAndCountsBeyondPreviewLimit(t *testing.T) {
	b := matcherTestBus(t)
	ctx := context.Background()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Unix()
	for i := 0; i < 315; i++ {
		story := &dorv1.Story{Id: fmt.Sprintf("match-%03d", i), Title: "Coastal wildfire destroys homes", SourceCount: 3, FirstSeen: now - int64(i)}
		data, _ := proto.Marshal(story)
		if _, err := kv.Put(ctx, live.StoryKey(story.Id), data); err != nil {
			t.Fatalf("put story %d: %v", i, err)
		}
	}
	for id, at := range map[string]int64{"outside-old": now - int64((24 * time.Hour).Seconds()) - 10, "outside-future": now + 10} {
		story := &dorv1.Story{Id: id, Title: "Coastal wildfire", SourceCount: 5, FirstSeen: at}
		data, _ := proto.Marshal(story)
		if _, err := kv.Put(ctx, live.StoryKey(id), data); err != nil {
			t.Fatal(err)
		}
	}
	unrelated, _ := proto.Marshal(&dorv1.Story{Id: "decoy", Title: "Coastal wildfire", SourceCount: 3, FirstSeen: now})
	if _, err := kv.Put(ctx, "notstory.decoy", unrelated); err != nil {
		t.Fatal(err)
	}

	s := matcherTestService(t, b, nil)
	s.now = func() time.Time { return time.Unix(now, 0).UTC() }
	got := s.Backtest(ctx, "coastal wildfire", 1, "", .80)
	if got.Partial {
		t.Fatal("backtest unexpectedly reported partial coverage")
	}
	if got.Scanned != 317 {
		t.Errorf("scanned = %d, want 317 story-prefix entries", got.Scanned)
	}
	if got.Eligible != 315 || got.Evaluated != 315 {
		t.Errorf("eligible/evaluated = %d/%d, want 315/315", got.Eligible, got.Evaluated)
	}
	if got.WouldFire != 315 {
		t.Errorf("would_fire = %d, want all 315 matches independent of preview", got.WouldFire)
	}
	if len(got.Stories) != 20 {
		t.Errorf("preview stories = %d, want capped at 20", len(got.Stories))
	}
	if !got.Approx {
		t.Error("no-key backtest should label its results approximate")
	}
}

func TestBackfillScansMoreThanThreeHundredRecentArticlesAndSkipsOldDecoys(t *testing.T) {
	b := matcherTestBus(t)
	ctx := context.Background()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Unix()
	for i := 0; i < 308; i++ {
		a := &dorv1.Article{Id: fmt.Sprintf("recent-%03d", i), Title: "Coastal wildfire spreads", PublishedAt: now - int64(i)}
		data, _ := proto.Marshal(a)
		if _, err := kv.Put(ctx, live.ArticleKey(a.Id), data); err != nil {
			t.Fatalf("put article %d: %v", i, err)
		}
	}
	for id, at := range map[string]int64{"old-article": now - int64(time.Hour.Seconds()) - 10, "future-article": now + 10} {
		a := &dorv1.Article{Id: id, Title: "Coastal wildfire", PublishedAt: at}
		data, _ := proto.Marshal(a)
		if _, err := kv.Put(ctx, live.ArticleKey(id), data); err != nil {
			t.Fatal(err)
		}
	}
	decoy, _ := proto.Marshal(&dorv1.Article{Id: "decoy", Title: "Coastal wildfire", PublishedAt: now})
	if _, err := kv.Put(ctx, "notarticle.decoy", decoy); err != nil {
		t.Fatal(err)
	}
	s := matcherTestService(t, b, nil)
	s.now = func() time.Time { return time.Unix(now, 0).UTC() }
	s.SetClientPrompts(ctx, "client", []string{"coastal wildfire"})
	s.backfillOnce(ctx)
	got := s.BackfillStatus()
	if got.Partial {
		t.Fatal("backfill unexpectedly reported partial coverage")
	}
	if got.Scanned != 310 {
		t.Errorf("scanned = %d, want 310 article-prefix entries", got.Scanned)
	}
	if got.Eligible != 308 || got.Evaluated != 308 {
		t.Errorf("eligible/evaluated = %d/%d, want 308/308", got.Eligible, got.Evaluated)
	}
}

func TestMockedTEIApproxPathWorksForBacktestAndStoryAlerts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Inputs []string `json:"inputs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode TEI request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		vectors := make([][]float32, len(req.Inputs))
		for i := range vectors {
			vectors[i] = []float32{1, 0, 0}
		}
		_ = json.NewEncoder(w).Encode(vectors)
	}))
	defer server.Close()
	b := matcherTestBus(t)
	ctx := context.Background()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Unix()
	stData, _ := proto.Marshal(&dorv1.Story{Id: "tei-story", Title: "Cyclone devastates coastal town", SourceCount: 2, FirstSeen: now})
	if _, err := kv.Put(ctx, live.StoryKey("tei-story"), stData); err != nil {
		t.Fatal(err)
	}
	s := New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, embed.New(server.URL, time.Second), nil, config.DefaultThresholds())
	s.now = func() time.Time { return time.Unix(now, 0).UTC() }
	result := s.Backtest(ctx, "city wildfire", 1, "", .8)
	if result.WouldFire != 1 || !result.Approx || len(result.Stories) != 1 || !result.Stories[0].Approx {
		t.Fatalf("TEI-backed backtest = %+v, want one approximate semantic match", result)
	}

	alertStore, err := alert.New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := alertStore.Create(ctx, alert.Alert{ID: "tei-alert", Prompt: "city wildfire", Enabled: true, Strictness: .8})
	if err != nil {
		t.Fatal(err)
	}
	s.alerts = alertStore
	s.alertsList = []alert.Alert{a}
	hits, err := s.StoryAlerts(ctx, &dorv1.Story{Id: "tei-live", Title: "Cyclone devastates coastal town", SourceCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || !hits[0].Approx {
		t.Fatalf("TEI-backed StoryAlerts = %+v, want one approximate semantic hit", hits)
	}
}

func TestBacktestCanceledWindowReportsPartialCoverage(t *testing.T) {
	b := matcherTestBus(t)
	s := matcherTestService(t, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := s.Backtest(ctx, "wildfire", 1, "", .8)
	if !got.Partial {
		t.Fatal("canceled scan should be reported as partial")
	}
	if got.Scanned != 0 || got.Eligible != 0 || got.Evaluated != 0 {
		t.Fatalf("canceled coverage counts = %+v, want all zero", got.WindowResult)
	}
}

func TestStoryAlertsPersistsGenerationAndAnchorsThresholdsToBaseSources(t *testing.T) {
	b := matcherTestBus(t)
	ctx := context.Background()
	store, err := alert.New(ctx, b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a, err := store.Create(ctx, alert.Alert{ID: "fire", Prompt: "coastal wildfire", Enabled: true, Strictness: .8, Channels: []string{"push"}})
	if err != nil {
		t.Fatal(err)
	}
	s := matcherTestService(t, b, store)
	s.alertsList = []alert.Alert{a}
	story := &dorv1.Story{Id: "story", Title: "Coastal wildfire spreads", SourceCount: 2}
	hits, err := s.StoryAlerts(ctx, story)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Generation != 0 || hits[0].BaseSources != 2 || !hits[0].Approx {
		t.Fatalf("initial hit = %+v, want approximate generation 0 anchored at 2", hits)
	}
	if err := s.RecordHit(ctx, hits[0]); err != nil {
		t.Fatal(err)
	}

	story.SourceCount = 5
	hits, err = s.StoryAlerts(ctx, story)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Generation != 1 || hits[0].BaseSources != 2 {
		t.Fatalf("2→5 hits = %+v, want generation 1 anchored at base 2", hits)
	}
	if err := s.RecordHit(ctx, hits[0]); err != nil {
		t.Fatal(err)
	}

	story.SourceCount = 9
	hits, err = s.StoryAlerts(ctx, story)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Generation != 2 || hits[0].BaseSources != 2 {
		t.Fatalf("2→5→9 hits = %+v, want generation 2 anchored at base 2", hits)
	}
	if err := s.RecordHit(ctx, hits[0]); err != nil {
		t.Fatal(err)
	}
	if all := store.Hits(ctx, a.ID); len(all) != 3 {
		t.Fatalf("persisted generations = %d, want 3", len(all))
	}
}

func TestStoryAlertsSkippedSourceJumpEmitsEveryMissedGeneration(t *testing.T) {
	b := matcherTestBus(t)
	ctx := context.Background()
	store, err := alert.New(ctx, b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a, err := store.Create(ctx, alert.Alert{ID: "jump", Prompt: "city wildfire", Enabled: true, Strictness: .8})
	if err != nil {
		t.Fatal(err)
	}
	s := matcherTestService(t, b, store)
	s.alertsList = []alert.Alert{a}
	first, err := s.StoryAlerts(ctx, &dorv1.Story{Id: "jump-story", Title: "City wildfire spreads", SourceCount: 2})
	if err != nil || len(first) != 1 {
		t.Fatalf("initial hit = %v, err %v", first, err)
	}
	if err := s.RecordHit(ctx, first[0]); err != nil {
		t.Fatal(err)
	}
	hits, err := s.StoryAlerts(ctx, &dorv1.Story{Id: "jump-story", Title: "City wildfire spreads", SourceCount: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Generation != 1 || hits[1].Generation != 2 {
		t.Fatalf("2→8 escalation = %+v, want generations 1 and 2", hits)
	}
	for _, h := range hits {
		if h.BaseSources != 2 {
			t.Errorf("generation %d base_sources = %d, want original 2", h.Generation, h.BaseSources)
		}
	}
}

type promptEditDecider struct {
	mu            sync.Mutex
	oldCalls      int
	oldBlocked    chan struct{}
	newPromptSeen chan struct{}
	blockOnce     sync.Once
	newOnce       sync.Once
}

func (d *promptEditDecider) Decide(ctx context.Context, req decide.Request) (decide.Response, error) {
	question := req.Questions["match"].Instructions
	if strings.Contains(question, `"wildfire"`) {
		d.mu.Lock()
		d.oldCalls++
		call := d.oldCalls
		d.mu.Unlock()
		if call == 2 {
			d.blockOnce.Do(func() { close(d.oldBlocked) })
			<-ctx.Done()
			return decide.Response{}, ctx.Err()
		}
	} else if strings.Contains(question, `"earthquake"`) {
		d.newOnce.Do(func() { close(d.newPromptSeen) })
	}
	return decide.Response{Answers: map[string]decide.Answer{"match": {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: .99}}}, nil
}

func TestPromptEditCancelsActiveBackfillAndRescoresAlreadyVisitedArticleQuickly(t *testing.T) {
	b := matcherTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Unix()
	for i, a := range []*dorv1.Article{
		{Id: "first", Title: "Wildfire and earthquake in valley", PublishedAt: now},
		{Id: "second", Title: "Wildfire and earthquake in valley", PublishedAt: now - 1},
	} {
		data, _ := proto.Marshal(a)
		if _, err := kv.Put(ctx, live.ArticleKey(a.Id), data); err != nil {
			t.Fatalf("put article %d: %v", i, err)
		}
	}
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(bus.SubjectMatchScore))
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: bus.SubjectMatchScore,
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	decider := &promptEditDecider{oldBlocked: make(chan struct{}), newPromptSeen: make(chan struct{})}
	s := New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), decider, embed.New("", 0), nil, config.DefaultThresholds())
	s.now = func() time.Time { return time.Unix(now, 0).UTC() }
	s.SetClientPrompts(ctx, "client", []string{"wildfire"})
	select {
	case <-decider.oldBlocked:
	case <-time.After(3 * time.Second):
		t.Fatal("old backfill did not reach its second article")
	}
	editAt := time.Now()
	s.SetClientPrompts(ctx, "client", []string{"earthquake"})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := consumer.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if err != nil {
			continue
		}
		var score dorv1.MatchScore
		if err := proto.Unmarshal(msg.Data(), &score); err != nil {
			t.Fatal(err)
		}
		if score.ArticleId == "first" {
			if value, ok := score.Scores["earthquake"]; ok && value >= .55 {
				if elapsed := time.Since(editAt); elapsed > 2*time.Second {
					t.Fatalf("new prompt score took %s, want <=2s", elapsed)
				}
				return
			}
		}
	}
	select {
	case <-decider.newPromptSeen:
	default:
	}
	t.Fatal("new prompt did not publish a score for the article already visited by the canceled backfill within 2s")
}

func TestCanceledPromptEmbeddingDoesNotTripProviderRetryCircuit(t *testing.T) {
	var calls sync.Mutex
	requestCount := 0
	oldStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Inputs []string `json:"inputs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		calls.Lock()
		requestCount++
		n := requestCount
		calls.Unlock()
		if n == 1 {
			close(oldStarted)
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode([][]float32{{1, 0, 0}})
	}))
	defer server.Close()
	s := New(matcherTestBus(t), slog.New(slog.NewTextHandler(io.Discard, nil)), nil, embed.New(server.URL, time.Second), nil, config.DefaultThresholds())
	ctx, cancel := context.WithCancel(context.Background())
	oldDone := make(chan struct{})
	go func() { defer close(oldDone); s.ensurePromptVectors(ctx, []string{"old prompt"}) }()
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old embedding request did not start")
	}
	cancel()
	<-oldDone
	if !s.embedRetryAfter.IsZero() {
		t.Fatalf("intentional cancellation set embed retry circuit until %s", s.embedRetryAfter)
	}
	s.ensurePromptVectors(context.Background(), []string{"new prompt"})
	calls.Lock()
	got := requestCount
	calls.Unlock()
	if got != 2 {
		t.Fatalf("embedding requests = %d, want canceled old + immediate new request", got)
	}
	s.mu.Lock()
	_, cached := s.promptVecs["new prompt"]
	s.mu.Unlock()
	if !cached {
		t.Fatal("new prompt embedding was not cached after immediate retry")
	}
}

func TestStoryAlertsKeepsRecoveredGenerationTwoQuietAtEightAndFiresAtSixteen(t *testing.T) {
	b := matcherTestBus(t)
	ctx := context.Background()
	store, err := alert.New(ctx, b, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a, err := store.Create(ctx, alert.Alert{ID: "recovered", Prompt: "city wildfire", Enabled: true, Strictness: .8})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHit(ctx, alert.Hit{AlertID: a.ID, StoryID: "recovered-story", Level: "escalated", Generation: 2, BaseSources: 2, SourceCount: 8, FiredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	s := matcherTestService(t, b, store)
	s.alertsList = []alert.Alert{a}
	story := &dorv1.Story{Id: "recovered-story", Title: "City wildfire spreads", SourceCount: 8}
	hits, err := s.StoryAlerts(ctx, story)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("unchanged source_count 8 emitted duplicate generation: %+v", hits)
	}
	story.SourceCount = 16
	hits, err = s.StoryAlerts(ctx, story)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Generation != 3 || hits[0].BaseSources != 2 {
		t.Fatalf("source_count 16 escalation = %+v, want gen3/base2", hits)
	}
}
