package agents

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// TestPublisherExpiryWithJournalCommit runs the real journal/restore path: a
// fresh acknowledged update must not be replaced by an expiry even when expiry
// runs before the flush, and the committed story must survive a publisher
// restart.
func TestPublisherExpiryWithJournalCommit(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	id := "journal-expiry-story"
	now := time.Now()

	core := newPublisherCore(b, testLogger())
	if err := core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = core.release(context.Background())
		}
	}()

	old := &dorv1.Story{Id: id, Title: "old", Heat: 0.5, FirstSeen: now.Add(-49 * time.Hour).Unix(), UpdatedAt: now.Add(-49 * time.Hour).Unix()}
	core.onStory(old)
	if err := core.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if core.committed.Stories[id] == nil {
		t.Fatal("old story was not committed")
	}

	fresh := proto.Clone(old).(*dorv1.Story)
	fresh.Title = "fresh acknowledged update"
	fresh.UpdatedAt = now.Unix()
	ack := make(chan error, 1)
	go func() {
		ack <- core.acceptInput(ctx, &Message{Subject: bus.StoryChangedSubject(id), StreamSequence: 41}, "story:"+id, func() { core.onStory(fresh) })
	}()
	waitFor(t, "pending fresh update", func() bool {
		core.mu.Lock()
		defer core.mu.Unlock()
		return core.pending["story:"+id].GetStoryUpsert() != nil
	})

	// Expiry runs while the fresh update is only pending, before any flush.
	core.expireStories(now)
	if err := core.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-ack; err != nil {
		t.Fatal(err)
	}

	if got := core.committed.Stories[id]; got == nil || got.Title != fresh.Title {
		t.Fatalf("committed story after expiry = %+v, want %q", got, fresh.Title)
	}
	if got := core.stories[id]; got == nil || got.Title != fresh.Title {
		t.Fatalf("in-memory story after expiry = %+v, want %q", got, fresh.Title)
	}

	if err := core.release(ctx); err != nil {
		t.Fatalf("release publisher: %v", err)
	}
	released = true

	restarted := newPublisherCore(b, testLogger())
	if err := restarted.restore(ctx); err != nil {
		t.Fatalf("restore publisher: %v", err)
	}
	defer restarted.release(context.Background())
	if got := restarted.committed.Stories[id]; got == nil || got.Title != fresh.Title {
		t.Fatalf("journal restore lost the fresh story: %+v", got)
	}
	if got := restarted.stories[id]; got == nil || got.Title != fresh.Title {
		t.Fatalf("restored live story = %+v, want %q", got, fresh.Title)
	}
}
