package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/store"
)

func TestProductionWriterRetriesWithDurableDefaults(t *testing.T) {
	url := os.Getenv("DOR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("DOR_TEST_DATABASE_URL is not set; skipping production writer database test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := store.Open(ctx, store.Config{URL: url, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("production-writer-%d-", time.Now().UnixNano())
	base := strings.TrimSuffix(strings.ReplaceAll(prefix, "-", "_"), "_")
	trigger, function, sequence := base+"_trigger", base+"_failure", base+"_seq"
	ddl := fmt.Sprintf(`CREATE SEQUENCE %s;
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id LIKE '%s%%' AND nextval('%s') = 1 THEN
  RAISE EXCEPTION 'injected one-time transient failure' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER %s BEFORE INSERT ON stories FOR EACH ROW EXECUTE FUNCTION %s();`, sequence, function, prefix, sequence, trigger, function)
	if _, err := st.Pool().Exec(ctx, ddl); err != nil {
		t.Fatalf("install one-time failure trigger: %v", err)
	}
	defer func() {
		_, _ = st.Pool().Exec(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON stories; DROP FUNCTION IF EXISTS %s(); DROP SEQUENCE IF EXISTS %s", trigger, function, sequence))
		_, _ = st.Pool().Exec(context.Background(), "DELETE FROM stories WHERE id LIKE $1", prefix+"%")
	}()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Keep this construction identical to the production runtime call site.
	w, err := store.NewWriter(st, newDurableWriterOptions(logger))
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := w.Close(closeCtx); err != nil {
			t.Errorf("close production writer: %v", err)
		}
	}()

	now := time.Now().UTC()
	first := store.StoryRecord{ID: prefix + "transient", Title: "Transient story", SourceCount: 1, FirstSeen: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if !w.EnqueueStory(first) {
		t.Fatal("enqueue transient story rejected")
	}
	waitWriter := func(id string) int {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			if err := st.Pool().QueryRow(context.Background(), "SELECT count(*) FROM stories WHERE id=$1", id).Scan(&n); err != nil {
				t.Fatal(err)
			}
			b := w.MaintenanceBarrier()
			if b.Unsettled < 0 {
				t.Fatalf("writer unsettled count went negative: %+v", b)
			}
			if n == 1 && b.StoryQueue == 0 && b.Unsettled == 0 && b.Flushing == 0 {
				return n
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for story %s and writer to settle: %+v", id, w.MaintenanceBarrier())
		return 0
	}
	if got := waitWriter(first.ID); got != 1 {
		t.Fatalf("transient story persisted rows = %d, want 1", got)
	}
	stats, barrier := w.Stats(), w.MaintenanceBarrier()
	if stats.Errors < 1 || stats.DroppedStories != 0 || barrier.DroppedStories != 0 || barrier.Unsettled != 0 || stats.StoryBatches < 1 {
		t.Fatalf("production retry did not recover cleanly: stats=%+v barrier=%+v", stats, barrier)
	}

	second := store.StoryRecord{ID: prefix + "healthy", Title: "Healthy follow-up story", SourceCount: 1, FirstSeen: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if !w.EnqueueStory(second) {
		t.Fatal("enqueue follow-up story rejected")
	}
	if got := waitWriter(second.ID); got != 1 {
		t.Fatalf("follow-up story persisted rows = %d, want 1", got)
	}
	stats, barrier = w.Stats(), w.MaintenanceBarrier()
	if stats.DroppedStories != 0 || barrier.DroppedQueue != 0 || barrier.DroppedBatch != 0 || barrier.DroppedStories != 0 || barrier.Unsettled != 0 {
		t.Fatalf("writer did not remain healthy after recovery: stats=%+v barrier=%+v", stats, barrier)
	}
}
