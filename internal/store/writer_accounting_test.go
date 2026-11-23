package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func accountingPrefix() string {
	return fmt.Sprintf("writer-accounting-%d-", time.Now().UnixNano())
}

func accountingWriter(t *testing.T, st *Store, batchSize int, interval time.Duration) *Writer {
	t.Helper()
	opts := DefaultWriterOptions()
	opts.BatchSize = batchSize
	opts.FlushInterval = interval
	w, err := NewWriter(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := w.Close(ctx); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	return w
}

func assertWriterSettled(t *testing.T, w *Writer, persisted func() int, want int, allowErrors bool) {
	t.Helper()
	waitStoreCondition(t, 10*time.Second, "persisted records and settled writer barrier", func() bool {
		b := w.MaintenanceBarrier()
		if b.Unsettled < 0 {
			t.Errorf("writer unsettled count went negative: %+v", b)
			return false
		}
		return persisted() == want && b.Queue == 0 && b.StoryQueue == 0 && b.AuxQueue == 0 &&
			b.Unsettled == 0 && b.PendingEnrichments == 0 && b.PendingAssignments == 0 && b.Flushing == 0
	})
	b := w.MaintenanceBarrier()
	if b.Unsettled != 0 || b.Queue != 0 || b.StoryQueue != 0 || b.AuxQueue != 0 || b.Flushing != 0 {
		t.Fatalf("writer did not settle: %+v", b)
	}
	if b.DroppedQueue != 0 || b.DroppedBatch != 0 || b.DroppedStories != 0 || (!allowErrors && b.Errors != 0) {
		t.Fatalf("successful writer work reported errors or drops: %+v", b)
	}
}

func accountingStory(id string) StoryRecord {
	now := time.Now().UTC()
	return StoryRecord{ID: id, Title: "Accounting story " + id, SourceCount: 1, FirstSeen: now, UpdatedAt: now, ExpiresAt: now.Add(48 * time.Hour)}
}

func countRows(t *testing.T, st *Store, table, prefix string) int {
	t.Helper()
	var count int
	query := "SELECT count(*) FROM " + table + " WHERE id LIKE $1"
	if err := st.Pool().QueryRow(context.Background(), query, prefix+"%").Scan(&count); err != nil {
		t.Fatalf("count %s rows: %v", table, err)
	}
	return count
}

func TestWriterArticleBatchAccounting(t *testing.T) {
	st := openTestStore(t)
	prefix := accountingPrefix()
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	w := accountingWriter(t, st, 100, time.Hour)
	for i := 0; i < 100; i++ {
		if !w.Enqueue(orderArticle(fmt.Sprintf("%sfull-%03d", prefix, i))) {
			t.Fatalf("enqueue article %d rejected", i)
		}
	}
	assertWriterSettled(t, w, func() int { return countRows(t, st, "articles", prefix) }, 100, false)
	if got := w.Stats().Batches; got != 1 {
		t.Fatalf("full article batch count = %d, want exactly one", got)
	}
	b := w.MaintenanceBarrier()
	if b.Unsettled != 0 || b.DroppedQueue != 0 || b.DroppedBatch != 0 {
		t.Fatalf("extra timer changed settled article state: %+v", b)
	}
}

func TestWriterArticleMultipleAndPartialBatchesSettle(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"multiple-full", 200}, {"timer-partial", 7}} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			prefix := accountingPrefix()
			t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
			interval := time.Hour
			if tc.n < 100 {
				interval = 30 * time.Millisecond
			}
			w := accountingWriter(t, st, 100, interval)
			for i := 0; i < tc.n; i++ {
				if !w.Enqueue(orderArticle(fmt.Sprintf("%s%s-%03d", prefix, tc.name, i))) {
					t.Fatalf("enqueue article %d rejected", i)
				}
			}
			assertWriterSettled(t, w, func() int { return countRows(t, st, "articles", prefix) }, tc.n, false)
			if tc.n == 200 && w.Stats().Batches != 2 {
				t.Fatalf("multiple article batches = %d, want 2 threshold batches", w.Stats().Batches)
			}
		})
	}
}

func TestWriterStoryBatchAccounting(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"one-full", 25}, {"multiple-full", 60}, {"timer-partial", 5}} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			prefix := accountingPrefix()
			t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
			w := accountingWriter(t, st, 100, 30*time.Millisecond)
			for i := 0; i < tc.n; i++ {
				story := accountingStory(fmt.Sprintf("%s%s-%03d", prefix, tc.name, i))
				if !w.EnqueueStory(story) {
					t.Fatalf("enqueue story %d rejected", i)
				}
			}
			assertWriterSettled(t, w, func() int { return countRows(t, st, "stories", prefix) }, tc.n, false)
			if w.Stats().StoryBatches == 0 {
				t.Fatal("successful story writes did not record a story batch")
			}
			if got := w.MaintenanceBarrier().DroppedStories; got != 0 {
				t.Fatalf("dropped stories = %d", got)
			}
		})
	}
}

func TestWriterCloseSettlesMixedQueues(t *testing.T) {
	st := openTestStore(t)
	prefix := accountingPrefix()
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	w := accountingWriter(t, st, 100, time.Hour)
	articleID, storyID := prefix+"close-article", prefix+"close-story"
	if !w.Enqueue(orderArticle(articleID)) || !w.EnqueueStory(accountingStory(storyID)) {
		t.Fatal("enqueue mixed article/story work rejected")
	}
	if !w.EnqueueLink(LinkRecord{A: prefix + "left", B: prefix + "right", Type: 1, Strength: .7}) ||
		!w.EnqueueStoryText(StoryTextRecord{ID: storyID, Brief: "updated brief", BriefSource: "generated", Model: "test"}) ||
		!w.EnqueueLinkText(LinkTextRecord{A: prefix + "left", B: prefix + "right", Why: "related"}) {
		t.Fatal("enqueue mixed auxiliary work rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if got := countRows(t, st, "articles", prefix); got != 1 {
		t.Fatalf("persisted articles = %d, want 1", got)
	}
	if got := countRows(t, st, "stories", prefix); got != 1 {
		t.Fatalf("persisted stories = %d, want 1", got)
	}
	b := w.MaintenanceBarrier()
	if b.Unsettled != 0 || b.Queue != 0 || b.StoryQueue != 0 || b.AuxQueue != 0 || b.Flushing != 0 || b.PendingEnrichments != 0 || b.PendingAssignments != 0 {
		t.Fatalf("closed writer has active work: %+v", b)
	}
	if b.DroppedQueue != 0 || b.DroppedBatch != 0 || b.DroppedStories != 0 {
		t.Fatalf("closed writer dropped accepted work: %+v", b)
	}
}

func TestWriterAuxiliaryMutationsSettle(t *testing.T) {
	st := openTestStore(t)
	prefix := accountingPrefix()
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	w := accountingWriter(t, st, 100, 25*time.Millisecond)
	articleID, storyID := prefix+"aux-article", prefix+"aux-story"
	left, right := prefix+"aux-left", prefix+"aux-right"
	if err := st.UpsertArticles(context.Background(), []ArticleRecord{orderArticle(articleID)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertStories(context.Background(), []StoryRecord{accountingStory(storyID)}); err != nil {
		t.Fatal(err)
	}
	if !w.EnqueueLink(LinkRecord{A: left, B: right, Type: 1, Strength: .5}) ||
		!w.EnqueueStoryText(StoryTextRecord{ID: storyID, Brief: "auxiliary story text", BriefSource: "generated", Model: "test"}) ||
		!w.EnqueueLinkText(LinkTextRecord{A: left, B: right, Why: "auxiliary link text"}) ||
		!w.EnqueueArticleEnrichment(ArticleEnrichmentRecord{ID: articleID, Category: 2, CategoryConfidence: .8, Lang: "en"}) {
		t.Fatal("enqueue auxiliary mutation rejected")
	}
	waitStoreCondition(t, 10*time.Second, "auxiliary mutations and enrichment to settle", func() bool {
		b := w.MaintenanceBarrier()
		if b.Unsettled < 0 {
			t.Errorf("writer unsettled count went negative: %+v", b)
			return false
		}
		return b.AuxQueue == 0 && b.Unsettled == 0 && b.PendingEnrichments == 0 && b.Flushing == 0
	})
	b := w.MaintenanceBarrier()
	if b.DroppedQueue != 0 || b.DroppedBatch != 0 || b.DroppedStories != 0 {
		t.Fatalf("auxiliary mutations dropped: %+v", b)
	}
	var why, brief string
	if err := st.Pool().QueryRow(context.Background(), "SELECT why FROM links WHERE a=$1 AND b=$2", left, right).Scan(&why); err != nil {
		t.Fatal(err)
	}
	if err := st.Pool().QueryRow(context.Background(), "SELECT brief FROM stories WHERE id=$1", storyID).Scan(&brief); err != nil {
		t.Fatal(err)
	}
	if why != "auxiliary link text" || brief != "auxiliary story text" {
		t.Fatalf("text mutations were not persisted: why=%q brief=%q", why, brief)
	}
	var category int16
	if err := st.Pool().QueryRow(context.Background(), "SELECT category FROM articles WHERE id=$1", articleID).Scan(&category); err != nil {
		t.Fatal(err)
	}
	if category != 2 {
		t.Fatalf("article enrichment category = %d, want 2", category)
	}
}

func installStoryFailureTrigger(t *testing.T, st *Store, prefix string, always bool) (trigger, function, sequence string) {
	t.Helper()
	base := strings.TrimSuffix(strings.NewReplacer("-", "_", ".", "_").Replace(prefix), "_")
	trigger, function, sequence = base+"_trigger", base+"_failure", base+"_seq"
	condition := "nextval('" + sequence + "') = 1"
	if always {
		condition = "true"
	}
	sql := fmt.Sprintf(`CREATE SEQUENCE %s;
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id LIKE '%s%%' AND %s THEN
  RAISE EXCEPTION 'injected writer failure' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER %s BEFORE INSERT ON stories FOR EACH ROW EXECUTE FUNCTION %s();`, sequence, function, prefix, condition, trigger, function)
	if _, err := st.Pool().Exec(context.Background(), sql); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = st.Pool().Exec(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON stories; DROP FUNCTION IF EXISTS %s(); DROP SEQUENCE IF EXISTS %s", trigger, function, sequence))
	})
	return trigger, function, sequence
}

func TestWriterRetriesTransientStoryFailure(t *testing.T) {
	st := openTestStore(t)
	prefix := accountingPrefix()
	installStoryFailureTrigger(t, st, prefix, false)
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	opts := DefaultWriterOptions()
	opts.BatchSize = 1
	opts.FlushInterval = 10 * time.Millisecond
	w, err := NewWriter(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := w.Close(ctx); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	if !w.EnqueueStory(accountingStory(prefix + "recover")) {
		t.Fatal("enqueue story rejected")
	}
	assertWriterSettled(t, w, func() int { return countRows(t, st, "stories", prefix) }, 1, true)
	stats := w.Stats()
	if stats.Errors < 1 || stats.DroppedStories != 0 || w.MaintenanceBarrier().DroppedBatch != 0 {
		t.Fatalf("transient failure did not recover cleanly: stats=%+v barrier=%+v", stats, w.MaintenanceBarrier())
	}
}

func TestWriterExhaustedTransientRetriesSettleAsDrop(t *testing.T) {
	st := openTestStore(t)
	prefix := accountingPrefix()
	installStoryFailureTrigger(t, st, prefix, true)
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	opts := DefaultWriterOptions()
	opts.BatchSize = 1
	opts.FlushInterval = 10 * time.Millisecond
	opts.MaxRetries = 1
	w, err := NewWriter(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := w.Close(ctx); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	if !w.EnqueueStory(accountingStory(prefix + "exhaust")) {
		t.Fatal("enqueue story rejected")
	}
	waitStoreCondition(t, 10*time.Second, "exhausted retries to become a terminal drop", func() bool {
		return w.Stats().DroppedStories == 1 && w.MaintenanceBarrier().Unsettled == 0
	})
	stats, barrier := w.Stats(), w.MaintenanceBarrier()
	if stats.Errors < 2 || stats.DroppedStories != 1 || barrier.Unsettled != 0 || barrier.Queue != 0 || barrier.StoryQueue != 0 || barrier.Flushing != 0 {
		t.Fatalf("unexpected exhausted retry state: stats=%+v barrier=%+v", stats, barrier)
	}
	if got := countRows(t, st, "stories", prefix); got != 0 {
		t.Fatalf("exhausted story persisted %d rows", got)
	}
}
