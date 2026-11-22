package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("DOR_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("DOR_TEST_DATABASE_URL is not set; skipping real-database writer ordering tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := Open(ctx, Config{URL: url, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return st
}

func cleanupOrderRows(t *testing.T, st *Store, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pattern := prefix + "%"
	for _, query := range []string{
		`DELETE FROM articles WHERE id LIKE $1`,
		`DELETE FROM article_locations WHERE article_id LIKE $1`,
		`DELETE FROM story_locations WHERE story_id LIKE $1`,
		`DELETE FROM stories WHERE id LIKE $1`,
		`DELETE FROM links WHERE a LIKE $1 OR b LIKE $1`,
	} {
		if _, err := st.Pool().Exec(ctx, query, pattern); err != nil {
			t.Logf("cleanup %q: %v", query, err)
		}
	}
}

func orderStory(id, articleID string) StoryRecord {
	now := time.Now().UTC()
	return StoryRecord{
		ID:          id,
		Title:       "Ordering story " + id,
		Category:    1,
		SourceCount: 1,
		Heat:        0.5,
		FirstSeen:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(48 * time.Hour),
		ArticleIDs:  []string{articleID},
	}
}

func orderArticle(id string) ArticleRecord {
	now := time.Now().UTC()
	return ArticleRecord{
		ID:          id,
		SourceID:    "wire",
		URL:         "https://example.test/" + id,
		Title:       "Article " + id,
		Lede:        "A lede for " + id,
		PublishedAt: now,
		CreatedAt:   now,
		Lang:        "en",
	}
}

// articleStoryID returns (story id, story assigned, row exists).
func articleStoryID(t *testing.T, st *Store, articleID string) (string, bool, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var story sql.NullString
	err := st.Pool().QueryRow(ctx, `SELECT story_id FROM articles WHERE id = $1 ORDER BY created_at DESC LIMIT 1`, articleID).Scan(&story)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, false
	}
	if err != nil {
		t.Fatalf("read article %s: %v", articleID, err)
	}
	return story.String, story.Valid, true
}

// TestStoreStoryAssignmentIsOrderIndependent covers the two flushes in either
// order plus the writer's pending-assignment retry.
func TestStoreStoryAssignmentIsOrderIndependent(t *testing.T) {
	st := openTestStore(t)
	prefix := fmt.Sprintf("f2-2-order-%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupOrderRows(t, st, prefix) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("story before article", func(t *testing.T) {
		storyID := prefix + "-s1"
		articleID := prefix + "-a1"
		missing, err := st.UpsertStories(ctx, []StoryRecord{orderStory(storyID, articleID)})
		if err != nil {
			t.Fatalf("upsert story before article: %v", err)
		}
		if got := missing[articleID]; got != storyID {
			t.Fatalf("missing assignment for %s = %q, want %q", articleID, got, storyID)
		}
		if _, _, exists := articleStoryID(t, st, articleID); exists {
			t.Fatal("article row exists before it was inserted")
		}

		if err := st.UpsertArticles(ctx, []ArticleRecord{orderArticle(articleID)}); err != nil {
			t.Fatalf("insert article: %v", err)
		}
		if got, assigned, exists := articleStoryID(t, st, articleID); !exists || assigned {
			t.Fatalf("article story after insert = %q (assigned=%v, exists=%v), want a row with NULL story", got, assigned, exists)
		}

		missing, err = st.UpsertStories(ctx, []StoryRecord{orderStory(storyID, articleID)})
		if err != nil {
			t.Fatalf("retry story after article: %v", err)
		}
		if len(missing) != 0 {
			t.Fatalf("retry reported missing assignments: %v", missing)
		}
		if got, assigned, exists := articleStoryID(t, st, articleID); !exists || !assigned || got != storyID {
			t.Fatalf("article story = %q (assigned=%v, exists=%v), want %q", got, assigned, exists, storyID)
		}
	})

	t.Run("article before story", func(t *testing.T) {
		storyID := prefix + "-s2"
		articleID := prefix + "-a2"
		if err := st.UpsertArticles(ctx, []ArticleRecord{orderArticle(articleID)}); err != nil {
			t.Fatalf("insert article: %v", err)
		}
		missing, err := st.UpsertStories(ctx, []StoryRecord{orderStory(storyID, articleID)})
		if err != nil {
			t.Fatalf("upsert story after article: %v", err)
		}
		if len(missing) != 0 {
			t.Fatalf("story after article reported missing assignments: %v", missing)
		}
		if got, assigned, exists := articleStoryID(t, st, articleID); !exists || !assigned || got != storyID {
			t.Fatalf("article story = %q (assigned=%v, exists=%v), want %q", got, assigned, exists, storyID)
		}
	})

	t.Run("writer retries a pending assignment", func(t *testing.T) {
		storyID := prefix + "-s3"
		articleID := prefix + "-a3"
		opts := DefaultWriterOptions()
		opts.QueueSize = 32
		opts.BatchSize = 1
		opts.FlushInterval = 20 * time.Millisecond
		opts.MaxRetries = 1
		writer, err := NewWriter(st, opts)
		if err != nil {
			t.Fatal(err)
		}
		writer.Start()
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()
			if err := writer.Close(closeCtx); err != nil {
				t.Errorf("close writer: %v", err)
			}
		}()

		if !writer.EnqueueStory(orderStory(storyID, articleID)) {
			t.Fatal("enqueue story was rejected")
		}
		waitStoreCondition(t, 20*time.Second, "story flush records the missing assignment", func() bool {
			writer.assignMu.Lock()
			defer writer.assignMu.Unlock()
			_, ok := writer.pendingAssign[articleID]
			return ok
		})

		if !writer.Enqueue(orderArticle(articleID)) {
			t.Fatal("enqueue article was rejected")
		}
		waitStoreCondition(t, 20*time.Second, "writer retry assigns the article", func() bool {
			got, assigned, exists := articleStoryID(t, st, articleID)
			return exists && assigned && got == storyID
		})
	})
}

func waitStoreCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestNilWriterIsANoOp guards the nil *Writer receiver methods. A nil writer
// assigned to an interface (as cmd/dor does when Postgres is disabled) must not
// panic on the first enqueue.
func TestNilWriterIsANoOp(t *testing.T) {
	var w *Writer
	if w.Enqueue(ArticleRecord{ID: "a"}) {
		t.Fatal("nil writer accepted an article")
	}
	if w.EnqueueStory(StoryRecord{ID: "s"}) {
		t.Fatal("nil writer accepted a story")
	}
	if w.EnqueueLink(LinkRecord{A: "a", B: "b"}) {
		t.Fatal("nil writer accepted a link")
	}
	if w.EnqueueStoryText(StoryTextRecord{ID: "s"}) {
		t.Fatal("nil writer accepted story text")
	}
	if w.EnqueueLinkText(LinkTextRecord{A: "a", B: "b"}) {
		t.Fatal("nil writer accepted link text")
	}
	if stats := w.Stats(); stats != (WriterStats{}) {
		t.Fatalf("nil writer stats = %+v, want zero", stats)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("nil writer close: %v", err)
	}
}
