package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const assignmentFailureDDL = `
DROP TRIGGER IF EXISTS f22_assignment_failure ON articles;
DROP FUNCTION IF EXISTS f22_assignment_failure();
`

// TestWriterAssignmentInsertPreservesCategory is the cluster enqueueStore
// regression: an assignment-only article upsert must not reset enrichment that
// the story write already applied.
func TestWriterAssignmentInsertPreservesCategory(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("f2-2-cat-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })

	article := orderArticle(prefix + "-a")
	story := orderStory(prefix+"-s", article.ID)
	story.Category = 7

	if err := st.UpsertArticles(ctx, []ArticleRecord{article}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertStories(ctx, []StoryRecord{story}); err != nil {
		t.Fatal(err)
	}
	var assigned int16
	if err := st.Pool().QueryRow(ctx, `SELECT category FROM articles WHERE id = $1`, article.ID).Scan(&assigned); err != nil {
		t.Fatal(err)
	}
	if assigned != story.Category {
		t.Fatalf("story assignment category = %d, want %d", assigned, story.Category)
	}

	// The cluster's durable path carries story_id without enrichment fields.
	article.StoryID = story.ID
	article.Category = 0
	article.CategoryConfidence = 0
	if err := st.UpsertArticles(ctx, []ArticleRecord{article}); err != nil {
		t.Fatal(err)
	}
	var category int16
	if err := st.Pool().QueryRow(ctx, `SELECT category FROM articles WHERE id = $1`, article.ID).Scan(&category); err != nil {
		t.Fatal(err)
	}
	if category != story.Category {
		t.Fatalf("assignment-only upsert reset category %d -> %d", story.Category, category)
	}
}

// TestWriterRetriesTransientAssignmentFailure injects one failed assignment
// attempt. A later article flush must retry the pending assignment even though
// the failed article is not delivered again.
func TestWriterRetriesTransientAssignmentFailure(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("f2-2-retry-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	if _, err := st.Pool().Exec(ctx, assignmentFailureDDL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = st.Pool().Exec(context.Background(), assignmentFailureDDL)
	})

	opts := DefaultWriterOptions()
	opts.WriteTimeout = time.Second
	opts.MaxRetries = 1
	writer, err := NewWriter(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())

	article := orderArticle(prefix + "-a")
	story := orderStory(prefix+"-s", article.ID)
	writer.flushStories([]StoryRecord{story})

	if _, err := st.Pool().Exec(ctx, `
CREATE FUNCTION f22_assignment_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected transient assignment failure'; END $$;
CREATE TRIGGER f22_assignment_failure BEFORE UPDATE OF story_id ON articles
FOR EACH ROW EXECUTE FUNCTION f22_assignment_failure();`); err != nil {
		t.Fatal(err)
	}
	writer.flush([]ArticleRecord{article})
	if _, err := st.Pool().Exec(ctx, `DROP TRIGGER f22_assignment_failure ON articles`); err != nil {
		t.Fatal(err)
	}

	writer.flush([]ArticleRecord{orderArticle(prefix + "-unrelated")})

	got, assigned, exists := articleStoryID(t, st, article.ID)
	if !exists {
		t.Fatal("article insert did not commit")
	}
	if !assigned || got != story.ID {
		t.Fatalf("assignment after retry = %q (assigned=%v), want %q", got, assigned, story.ID)
	}
}
