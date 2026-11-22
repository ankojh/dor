package matcher

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

type countingDecider struct {
	calls atomic.Int64
}

func (d *countingDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	d.calls.Add(1)
	return decide.Response{Answers: map[string]decide.Answer{
		"match": {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: .99},
	}}, nil
}

func waitForBackfillIdle(t *testing.T, s *Service) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		busy := s.backfilling
		s.mu.Unlock()
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("backfill did not become idle")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRemovingClientPromptsStopsJevEvaluation(t *testing.T) {
	b := matcherTestBus(t)
	decider := &countingDecider{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(b, log, decider, embed.New("", 0), nil, configForMatcherTest())
	ctx := context.Background()

	s.SetClientPrompts(ctx, "client-a", []string{"wildfire"})
	// An empty embedded KV means the automatic backfill has no articles to
	// score; wait for it so the counter assertions below only see ArticleScores.
	waitForBackfillIdle(t, s)

	article := &dorv1.Article{Id: "article-1", Title: "Wildfire spreads", Lede: "Evacuations ordered."}
	scores, approx := s.ArticleScores(ctx, article)
	if len(scores) == 0 || scores["wildfire"] == 0 {
		t.Fatalf("active prompt scored %v / %v, want a wildfire score", scores, approx)
	}
	if got := decider.calls.Load(); got < 1 {
		t.Fatal("jev was not consulted for an active prompt")
	}

	before := decider.calls.Load()
	s.SetClientPrompts(ctx, "client-a", nil)
	scores, approx = s.ArticleScores(ctx, article)
	if scores != nil || approx != nil {
		t.Fatalf("scores after prompt removal = %v / %v, want nil", scores, approx)
	}
	if got := decider.calls.Load(); got != before {
		t.Fatalf("jev calls after prompt removal = %d, want %d", got, before)
	}
}
