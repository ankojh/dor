package agents

import (
	"testing"

	"github.com/ankojh/dor/internal/store"
)

// TestNilStoreWriterSinkDoesNotPanic is the DB-disabled regression: cmd/dor
// leaves these sinks nil unless a writer exists, but a nil *store.Writer stored
// in an interface must also be harmless. Before the nil receiver guard the first
// story completion panicked here.
func TestNilStoreWriterSinkDoesNotPanic(t *testing.T) {
	var writer *store.Writer
	var storySink StorySink = writer
	if storySink == nil {
		t.Fatal("interface holding a typed nil writer is nil; the test would not exercise the guard")
	}
	if storySink.EnqueueStory(store.StoryRecord{ID: "s-nil"}) {
		t.Fatal("nil writer sink accepted a story")
	}

	var articleSink ArticleSink = writer
	if articleSink.Enqueue(store.ArticleRecord{ID: "a-nil"}) {
		t.Fatal("nil writer sink accepted an article")
	}

	var linkSink LinkSink = writer
	if linkSink.EnqueueLink(store.LinkRecord{A: "a", B: "b"}) {
		t.Fatal("nil writer sink accepted a link")
	}

	// The run wiring keeps the interface nil when no writer exists, so this is
	// the shape the pipeline actually uses without Postgres.
	var dbWriter *store.Writer
	var wired StorySink
	if dbWriter != nil {
		wired = dbWriter
	}
	if wired != nil {
		t.Fatal("nil *store.Writer leaked into a non-nil StorySink")
	}
}
