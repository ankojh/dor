package agents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
	"google.golang.org/protobuf/proto"
)

type publicationSample struct {
	at        time.Duration
	bytes     uint64
	keys      int
	baseBytes int
	journal   int
	entries   int
	retired   int
	staging   int
	stories   int
	links     int
	rows      int
	scores    int
	revisions int
	locations int
}

func designStoryID(i int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("design-story:%d", i)))
	return "s" + hex.EncodeToString(digest[:10])
}

func designArticleID(i int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("design-article:%d", i)))
	return hex.EncodeToString(digest[:])
}

func designStory(id string, i int, updatedAt int64) *dorv1.Story {
	return &dorv1.Story{
		Id: id, Title: fmt.Sprintf("Regional election protest and policy briefing %05d cycle %03d", i, updatedAt%1000),
		Category: dorv1.Category_CATEGORY_POLITICS, SourceCount: uint32(2 + i%38), Heat: float32(i%100) / 100,
		FirstSeen: updatedAt - 86400 - int64(i), UpdatedAt: updatedAt,
		Locations: []*dorv1.Location{{PlaceId: fmt.Sprintf("gn:%d", 100000+i), Name: fmt.Sprintf("City %05d", i), Lat: float32(i%180) - 90 + .12345, Lon: float32(i%360) - 180 + .54321, Weight: .8}},
	}
}

func designFixtureState() (*publication.State, []string, []string, [][2]string) {
	const (
		storyCount = 5000
		linkCount  = 20000
		wireCount  = 1500
	)
	state := publication.NewState(nil)
	storyIDs := make([]string, storyCount)
	seedNow := time.Now().Unix()
	for i := range storyIDs {
		id := designStoryID(i)
		storyIDs[i] = id
		state.Stories[id] = designStory(id, i, seedNow)
		state.Revisions["STORY/story:"+id] = 0 // real stream positions start above this seed watermark
	}
	links := make([][2]string, 0, linkCount)
	for i := 0; i < linkCount; i++ {
		a := storyIDs[i%storyCount]
		b := storyIDs[(i%storyCount+1+i/storyCount)%storyCount]
		if a > b {
			a, b = b, a
		}
		links = append(links, [2]string{a, b})
		state.Links[a+"|"+b] = &dorv1.Link{A: a, B: b, Confidence: .72, Strength: .45 + float32(i%50)/100, UpdatedAt: time.Now().Unix()}
		state.Revisions["STORY/link:"+a+"|"+b] = 0
	}
	wireIDs := make([]string, wireCount)
	for i := range wireIDs {
		id := designArticleID(i)
		wireIDs[i] = id
		row := &dorv1.WireRow{ArticleId: id, StoryId: storyIDs[i%storyCount], Title: fmt.Sprintf("Wire report from source %d", i%24), Source: fmt.Sprintf("source-%02d", i%24), Place: fmt.Sprintf("City %04d", i%500), At: time.Now().Unix(), Country: "US", Lang: "en", Lat: 37.7749, Lon: -122.4194, Located: true, PromptScores: map[string]float32{"election": .7, "protest": .4}}
		state.Rows[id] = row
		state.WireOrder = append(state.WireOrder, id)
		if i >= wireCount-500 {
			state.RecentWire = append(state.RecentWire, row)
		}
		score := &dorv1.MatchScore{ArticleId: id, Scores: map[string]float32{"election": .71, "protest": .43, "flood": .03, "court": .12}, Approx: []string{"election", "protest"}, UpdatedAt: time.Now().Unix()}
		state.Scores[id] = score
		state.ScoreOrder = append(state.ScoreOrder, id)
		for _, stage := range []string{"clean", "placed", "score"} {
			state.Revisions["ENRICH/"+stage+":"+id] = 0
		}
	}
	return state, storyIDs, wireIDs, links
}

func metricP95(reg *Metrics, familyName string, labels map[string]string) (float64, uint64) {
	families, err := reg.Registry().Gather()
	if err != nil {
		return 0, 0
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			match := true
			for _, label := range metric.GetLabel() {
				if want, ok := labels[label.GetName()]; ok && want != label.GetValue() {
					match = false
				}
			}
			for key := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == key {
						found = true
						break
					}
				}
				if !found {
					match = false
				}
			}
			if !match || metric.GetHistogram() == nil || metric.GetHistogram().GetSampleCount() == 0 {
				continue
			}
			h := metric.GetHistogram()
			want := uint64(math.Ceil(float64(h.GetSampleCount()) * .95))
			for _, bucket := range h.GetBucket() {
				if bucket.GetCumulativeCount() >= want {
					return bucket.GetUpperBound(), h.GetSampleCount()
				}
			}
			return math.Inf(1), h.GetSampleCount()
		}
	}
	return 0, 0
}

func TestPublisherDesignScaleSustainedStoragePlateau(t *testing.T) {
	if os.Getenv("DOR_TEST_PUBLISHER_SUSTAINED") != "1" {
		t.Skip("set DOR_TEST_PUBLISHER_SUSTAINED=1 to run the five-minute design-scale publisher workload")
	}
	const (
		articlesPerSecond = 50
		duration          = 5 * time.Minute
		articleCount      = 15000
		wireRows          = 1500
	)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	b := durabilityBus(t)
	state, storyIDs, _, links := designFixtureState()
	if len(state.Stories) != 5000 || len(state.Links) != 20000 || len(state.Rows) != wireRows || len(state.Revisions) != 29500 || len(state.Scores) != wireRows {
		t.Fatalf("design fixture stories=%d links=%d rows=%d revisions=%d scores=%d", len(state.Stories), len(state.Links), len(state.Rows), len(state.Revisions), len(state.Scores))
	}
	publisherKV, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	seedStore := publication.New(publisherKV, "design-fixture", publisherLease, b.NC().MaxPayload())
	if err := seedStore.Initialize(ctx, state, 0, nil); err != nil {
		t.Fatalf("install design-scale committed base: %v", err)
	}
	if err := seedStore.Release(ctx); err != nil {
		t.Fatal(err)
	}

	metrics := NewMetrics()
	agent, err := NewPublisher(b, metrics, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	publisher := agent.(*publisherAgent)
	// Install timing before any workload input; publisher Start restores the
	// seeded v2 state and launches the production consumers/flush/GC loops.
	if err := publisher.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer stopCancel()
			if err := publisher.Stop(stopCtx); err != nil {
				t.Errorf("stop publisher: %v", err)
			}
		}
	}()

	enrich, err := b.JS().Consumer(ctx, bus.StreamEnrich, "publisher-enrich")
	if err != nil {
		t.Fatal(err)
	}
	storyConsumer, err := b.JS().Consumer(ctx, bus.StreamStory, "publisher-story")
	if err != nil {
		t.Fatal(err)
	}
	readLag := func() (int, error) {
		ei, err := enrich.Info(ctx)
		if err != nil {
			return 0, err
		}
		si, err := storyConsumer.Info(ctx)
		if err != nil {
			return 0, err
		}
		return int(ei.NumPending) + int(ei.NumAckPending) + int(si.NumPending) + int(si.NumAckPending), nil
	}

	kvSamples := make([]publicationSample, 0, 40)
	sampleStorage := func(at time.Duration) {
		sampleCtx, stopSample := context.WithTimeout(ctx, 3*time.Second)
		defer stopSample()
		status, err := publisherKV.Status(sampleCtx)
		if err != nil {
			t.Errorf("sample publisher bucket status: %v", err)
			return
		}
		keys, err := publisherKV.Keys(sampleCtx)
		if err != nil {
			t.Errorf("sample publisher bucket keys: %v", err)
			return
		}
		root, _, err := publisher.core.storage.ReadRoot(sampleCtx)
		if err != nil {
			t.Errorf("sample publication root: %v", err)
			return
		}
		sample := publicationSample{at: at, bytes: status.Bytes(), keys: len(keys), baseBytes: root.Base.Blob.Size, entries: len(root.Entries), retired: len(root.Retired), staging: len(root.Staging)}
		for _, entry := range root.Entries {
			sample.journal += entry.Blob.Size
		}
		publisher.core.mu.Lock()
		sample.stories, sample.links, sample.rows = len(publisher.core.stories), len(publisher.core.links), len(publisher.core.wireRows)
		sample.scores, sample.revisions = len(publisher.core.scores), len(publisher.core.committedRevisions)
		for _, story := range publisher.core.stories {
			if story != nil {
				sample.locations += len(story.Locations)
			}
		}
		publisher.core.mu.Unlock()
		kvSamples = append(kvSamples, sample)
	}

	var commitMu sync.Mutex
	type startedCommit struct {
		started time.Time
		seq     uint64
	}
	pendingCommit := map[uint64]startedCommit{}
	var commitLatency []time.Duration
	publisher.core.publish = func(_ context.Context, data []byte, id string) error {
		var batch dorv1.DeltaBatch
		if err := proto.Unmarshal(data, &batch); err != nil {
			return err
		}
		if len(batch.Deltas) > 0 {
			seq := batch.Deltas[len(batch.Deltas)-1].Seq
			commitMu.Lock()
			pendingCommit[seq] = startedCommit{started: time.Now(), seq: seq}
			commitMu.Unlock()
		}
		return nil // the seam observes; publishBatch still performs the real JetStream publish
	}
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
			publisher.core.mu.Lock()
			committedSeq := publisher.core.seq
			publisher.core.mu.Unlock()
			commitMu.Lock()
			for seq, started := range pendingCommit {
				if seq <= committedSeq {
					commitLatency = append(commitLatency, time.Since(started.started))
					delete(pendingCommit, seq)
				}
			}
			commitMu.Unlock()
		}
	}()

	publish := func(subject, id string, message proto.Message) error {
		data, err := proto.Marshal(message)
		if err != nil {
			return err
		}
		_, err = b.Publish(ctx, subject, data, bus.WithMsgID(id))
		return err
	}
	initialStatus, err := publisherKV.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	initialKeys, err := publisherKV.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	nextSample := start.Add(10 * time.Second)
	nextLagSample := start.Add(5 * time.Second)
	maxLag := 0
	nextArticle := time.NewTicker(time.Second / articlesPerSecond)
	defer nextArticle.Stop()
	for i := 0; i < articleCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("producer stopped at article %d: %v", i, ctx.Err())
		case <-nextArticle.C:
		}
		id := designArticleID(100000 + i) // fresh 64-hex article identity; live rows remain capped at 1,500
		storyID := storyIDs[i%len(storyIDs)]
		link := links[i%len(links)]
		stamp := fmt.Sprintf("load-%05d", i)
		if err := publish(bus.SubjectArticleClean, stamp+"-clean", &dorv1.Article{Id: id, Title: "clean dispatch " + stamp, PublishedAt: time.Now().Unix()}); err != nil {
			t.Fatalf("publish clean input %d: %v", i, err)
		}
		if err := publish(bus.SubjectArticlePlaced, stamp+"-placed", &dorv1.Article{Id: id, StoryId: storyID, Title: "placed dispatch " + stamp, Dateline: "City", Join: dorv1.JoinKind_JOIN_KIND_JOINED, PublishedAt: time.Now().Unix()}); err != nil {
			t.Fatalf("publish placed input %d: %v", i, err)
		}
		if err := publish(bus.SubjectMatchScore, stamp+"-score", &dorv1.MatchScore{ArticleId: id, Scores: map[string]float32{"election": float32(i%100) / 100, "protest": .61}, UpdatedAt: time.Now().Unix()}); err != nil {
			t.Fatalf("publish score input %d: %v", i, err)
		}
		if err := publish(bus.SubjectStoryChangedPrefix+"design", stamp+"-story", designStory(storyID, i%len(storyIDs), time.Now().Unix())); err != nil {
			t.Fatalf("publish story input %d: %v", i, err)
		}
		if err := publish(bus.SubjectLinkChanged, stamp+"-link", &dorv1.Link{A: link[0], B: link[1], Strength: .61, Confidence: .8, UpdatedAt: time.Now().Unix()}); err != nil {
			t.Fatalf("publish link input %d: %v", i, err)
		}

		elapsed := time.Since(start)
		if time.Now().After(nextLagSample) {
			if lag, e := readLag(); e == nil && lag > maxLag {
				maxLag = lag
			}
			nextLagSample = time.Now().Add(5 * time.Second)
		}
		if time.Now().After(nextSample) {
			sampleStorage(elapsed)
			nextSample = time.Now().Add(10 * time.Second)
			s := kvSamples[len(kvSamples)-1]
			t.Logf("storage sample t=%s physical=%d keys=%d base=%d journal=%d entries=%d retired=%d staging=%d state=%d stories/%d links/%d rows/%d scores/%d revisions/%d locations lag-max=%d", elapsed.Round(time.Second), s.bytes, s.keys, s.baseBytes, s.journal, s.entries, s.retired, s.staging, s.stories, s.links, s.rows, s.scores, s.revisions, s.locations, maxLag)
		}
	}
	publishedDuration := time.Since(start)
	publishedRate := float64(articleCount) / publishedDuration.Seconds()
	if publishedDuration < duration || publishedRate < 49 {
		t.Fatalf("input workload lasted %s at %.2f articles/s, want >=%s and >=49/s", publishedDuration, publishedRate, duration)
	}
	if len(kvSamples) == 0 || kvSamples[len(kvSamples)-1].at < duration-10*time.Second {
		sampleStorage(publishedDuration)
	}

	deadline := time.Now().Add(45 * time.Second)
	lag := -1
	for time.Now().Before(deadline) {
		lag, err = readLag()
		if err == nil && lag == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lag != 0 {
		t.Fatalf("consumer backlog did not drain after workload: %d", lag)
	}
	if maxLag < lag {
		maxLag = lag
	}
	finalSampleAt := time.Since(start)
	sampleStorage(finalSampleAt)

	publisher.core.mu.Lock()
	stories, linkCount, rows, scores, revisions := len(publisher.core.stories), len(publisher.core.links), len(publisher.core.wireRows), len(publisher.core.scores), len(publisher.core.committedRevisions)
	lastSeq := publisher.core.seq
	publisher.core.mu.Unlock()
	if stories != 5000 || linkCount != 20000 || rows != wireRows || scores != wireRows || revisions < 29500 || revisions > 32000 {
		t.Fatalf("design occupancy after drain stories=%d links=%d rows=%d scores=%d revisions=%d", stories, linkCount, rows, scores, revisions)
	}
	if lastSeq == 0 {
		t.Fatal("publisher committed no output during workload")
	}

	stopMonitor()
	<-monitorDone
	commitMu.Lock()
	latencies := append([]time.Duration(nil), commitLatency...)
	commitMu.Unlock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	commitP95 := time.Duration(0)
	if len(latencies) > 0 {
		commitP95 = latencies[(len(latencies)-1)*95/100]
	}

	// Exclude initial seed/first compaction window, then require the physical
	// publisher bucket to stay within a bounded band across multiple GC cycles.
	plateau := make([]publicationSample, 0, len(kvSamples))
	for _, sample := range kvSamples {
		if sample.at >= 180*time.Second {
			if sample.stories != 5000 || sample.links != 20000 || sample.rows != wireRows || sample.scores != wireRows || sample.locations < 5000 {
				t.Fatalf("late occupancy at %s drifted stories=%d links=%d rows=%d scores=%d locations=%d", sample.at, sample.stories, sample.links, sample.rows, sample.scores, sample.locations)
			}
			plateau = append(plateau, sample)
		}
	}
	if len(plateau) < 8 {
		t.Fatalf("too few late physical storage samples for plateau check: %d", len(plateau))
	}
	minBytes, maxBytes := plateau[0].bytes, plateau[0].bytes
	minKeys, maxKeys := plateau[0].keys, plateau[0].keys
	for _, sample := range plateau[1:] {
		minBytes, maxBytes = min(minBytes, sample.bytes), max(maxBytes, sample.bytes)
		minKeys, maxKeys = min(minKeys, sample.keys), max(maxKeys, sample.keys)
	}
	if maxBytes-minBytes > max(minBytes/5, 2*1024*1024) {
		t.Fatalf("publisher physical bytes did not plateau in final window: min=%d max=%d", minBytes, maxBytes)
	}
	// Keys follow a bounded sawtooth while journal records accumulate and are
	// compacted/retired. Compare both the envelope and the means of adjacent
	// halves so normal compaction phase does not look like monotonic growth.
	if maxKeys-minKeys > 150 {
		t.Fatalf("publisher key count exceeded bounded compaction band: min=%d max=%d", minKeys, maxKeys)
	}
	mid := len(plateau) / 2
	var firstBytes, secondBytes, firstKeys, secondKeys float64
	for i, sample := range plateau {
		if i < mid {
			firstBytes += float64(sample.bytes)
			firstKeys += float64(sample.keys)
		} else {
			secondBytes += float64(sample.bytes)
			secondKeys += float64(sample.keys)
		}
	}
	firstCount, secondCount := float64(mid), float64(len(plateau)-mid)
	firstBytes, secondBytes = firstBytes/firstCount, secondBytes/secondCount
	firstKeys, secondKeys = firstKeys/firstCount, secondKeys/secondCount
	if secondBytes > firstBytes*1.15+1024*1024 {
		t.Fatalf("physical storage mean is still growing: first half %.0f bytes, second half %.0f bytes", firstBytes, secondBytes)
	}
	if secondKeys > firstKeys*1.15+25 {
		t.Fatalf("storage key mean is still growing: first half %.1f keys, second half %.1f keys", firstKeys, secondKeys)
	}
	if kvSamples[len(kvSamples)-1].entries > publication.MaxEntries/2 {
		t.Fatalf("journal entries remain high after compaction: %d", kvSamples[len(kvSamples)-1].entries)
	}

	storageP95 := map[string]float64{}
	for _, operation := range []string{"root_write", "lease", "journal_encode", "base_encode", "base_total", "cache_encode"} {
		bound, count := metricP95(metrics, "dor_publisher_storage_seconds", map[string]string{"operation": operation})
		if count > 0 {
			storageP95[operation] = bound
		}
	}
	handlerP95, handlerCount := metricP95(metrics, "dor_agent_handle_seconds", map[string]string{"agent": PublisherAgentName, "outcome": OutcomeOK})
	t.Logf("design run duration=%s article_rate=%.2f/s fanout=clean+placed+score/story/link commits=%d commit_latency_p95=%s handler_p95_bucket=%0.3fs (n=%d) storage_p95_buckets=%v samples=%d late_bytes=%d..%d late_keys=%d..%d max_lag=%d final_lag=%d seed_bytes=%d seed_keys=%d", publishedDuration, publishedRate, len(latencies), commitP95, handlerP95, handlerCount, storageP95, len(kvSamples), minBytes, maxBytes, minKeys, maxKeys, maxLag, lag, initialStatus.Bytes(), len(initialKeys))
	for _, sample := range kvSamples {
		t.Logf("storage t=%s physical=%d keys=%d base=%d journal=%d entries=%d retired=%d staging=%d state=%d stories/%d links/%d rows/%d scores/%d revisions/%d locations", sample.at.Round(time.Second), sample.bytes, sample.keys, sample.baseBytes, sample.journal, sample.entries, sample.retired, sample.staging, sample.stories, sample.links, sample.rows, sample.scores, sample.revisions, sample.locations)
	}

	stopCtx, stopPublisher := context.WithTimeout(ctx, 30*time.Second)
	if err := publisher.Stop(stopCtx); err != nil {
		stopPublisher()
		t.Fatal(err)
	}
	stopPublisher()
	stopped = true
	recovered := newPublisherCore(b, testLogger())
	if err := recovered.restore(ctx); err != nil {
		t.Fatalf("restore design state after drain: %v", err)
	}
	defer recovered.release(context.Background())
	if len(recovered.stories) != 5000 || len(recovered.links) != 20000 || len(recovered.wireRows) != wireRows || len(recovered.scores) != wireRows || len(recovered.revisions) < 29500 || len(recovered.revisions) > 32000 {
		t.Fatalf("recovered design state stories=%d links=%d rows=%d scores=%d revisions=%d", len(recovered.stories), len(recovered.links), len(recovered.wireRows), len(recovered.scores), len(recovered.revisions))
	}
	recoveredLocations := 0
	for _, story := range recovered.stories {
		recoveredLocations += len(story.Locations)
	}
	if recoveredLocations != 5000 {
		t.Fatalf("recovered design state locations=%d, want one per story", recoveredLocations)
	}
	if recovered.stories[storyIDs[0]] == nil || recovered.links[links[0][0]+"|"+links[0][1]] == nil {
		t.Fatal("recovery lost a representative story, link, or wire row")
	}
	var recoveredWireID string
	for id := range recovered.wireRows {
		if len(id) == 64 {
			recoveredWireID = id
			break
		}
	}
	if recoveredWireID == "" || recovered.scores[recoveredWireID] == nil {
		t.Fatal("recovery lost representative fresh article rows or scores")
	}
	if recovered.committedRevisions["STORY/story:"+storyIDs[0]] == 0 || recovered.committedRevisions["ENRICH/placed:"+recoveredWireID] == 0 {
		t.Fatal("recovery lost source revision or score state")
	}
}
