package agents

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/decide"
)

type acceptedCounter struct {
	mu    sync.Mutex
	calls int
}

func (d *acceptedCounter) Decide(context.Context, decide.Request) (decide.Response, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return acceptedRelationResponse(), nil
}

func (d *acceptedCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// Both endpoints updating must still produce one judgement for the resulting
// revision pair, not one per endpoint. A real content revision afterwards must
// judge again.
func TestRelationPairJudgedOnceForBothEndpointUpdates(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	decider := &acceptedCounter{}
	agent, err := NewRelationJudge(b, NewMetrics(), testLogger(), RelationConfig{
		Decider:  decider,
		Shards:   1,
		Shard:    -1,
		Debounce: time.Millisecond,
		Workers:  4,
	})
	if err != nil {
		t.Fatal(err)
	}
	core := agent.(*relationAgent).core
	now := time.Now().Unix()
	a, other := relationPairIDs(t)
	ea := &relationEntry{story: relationTestStory(a, now, 0.5, 40.7, -74), firstSeen: now - 60, updated: now, rev: 10}
	eb := &relationEntry{story: relationTestStory(other, now, 0.5, 40.7, -74), firstSeen: now - 60, updated: now, rev: 20}
	core.index[a] = ea
	core.index[other] = eb

	ctx := context.Background()
	core.judgePair(ctx, ea, eb)
	core.judgePair(ctx, eb, ea) // the other endpoint's trigger
	if got := decider.count(); got != 1 {
		t.Fatalf("judgements for one revision pair = %d, want 1", got)
	}

	// A real content change fences and judges again.
	eb.story.SourceCount++
	eb.rev++
	core.judgePair(ctx, eb, ea)
	if got := decider.count(); got != 2 {
		t.Fatalf("judgements after content change = %d, want 2", got)
	}

	// A heat-only update never schedules or advances the revision.
	heatOnly := relationTestStory(other, now, 0.1, 40.7, -74)
	heatOnly.SourceCount = eb.story.SourceCount
	core.observe(ctx, heatOnly)
	core.mu.Lock()
	gotRev := core.index[other].rev
	_, pending := core.pending[other]
	core.mu.Unlock()
	if gotRev != eb.rev {
		t.Fatalf("heat-only observe advanced revision %d -> %d", eb.rev, gotRev)
	}
	if pending {
		t.Fatal("heat-only observe scheduled a judgement")
	}
}
