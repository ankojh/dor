package textsvc

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func TestF12WithProvenance(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   live.StoryText
		want string
	}{
		{
			name: "available with model is generated",
			in:   live.StoryText{Available: true, Model: "claude-opus"},
			want: live.BriefGenerated,
		},
		{
			name: "available without model is a source snippet",
			in:   live.StoryText{Available: true},
			want: live.BriefSourceSnippet,
		},
		{
			name: "unavailable is none even with a model",
			in:   live.StoryText{Available: false, Model: "claude-opus"},
			want: live.BriefNone,
		},
		{
			name: "explicit provenance is preserved",
			in:   live.StoryText{Available: true, Provenance: live.BriefGenerated},
			want: live.BriefGenerated,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withProvenance(tc.in); got.Provenance != tc.want {
				t.Fatalf("provenance = %q, want %q", got.Provenance, tc.want)
			}
		})
	}
}

func TestF12FallbackStory(t *testing.T) {
	story := &dorv1.Story{Id: "story-f12-fallback", Title: "Working title"}
	got := fallbackStory(story, nil, 3, "2026-11-22T12:00:00Z")
	if got.Provenance != live.BriefNone || got.Brief != "" || got.Available {
		t.Fatalf("fallback = (%q,%q,%v), want none/empty/unavailable", got.Provenance, got.Brief, got.Available)
	}
	if got.Title != "Working title" {
		t.Fatalf("title = %q, want the story title", got.Title)
	}
	if got.SourceCount != 3 {
		t.Fatalf("source count = %d, want 3", got.SourceCount)
	}

	withSource := fallbackStory(story, []llm.Source{{Title: "First source headline"}}, 2, "now")
	if withSource.Title != "First source headline" {
		t.Fatalf("title = %q, want the first source headline", withSource.Title)
	}

	editorStory := &dorv1.Story{Id: story.Id, Title: "Neutral editor title", TitleSource: "editor"}
	fallback := fallbackStory(editorStory, []llm.Source{{Title: "Raw source headline"}}, 2, "now")
	if fallback.Title != "Neutral editor title" {
		t.Fatalf("fallback title = %q, want the applied editor title", fallback.Title)
	}
}

type f12Completer struct {
	res   llm.Result
	err   error
	jobs  []llm.Job
	calls int
}

func (c *f12Completer) Complete(_ context.Context, job llm.Job) (llm.Result, error) {
	c.calls++
	c.jobs = append(c.jobs, job)
	return c.res, c.err
}

func f12SeedStory(t *testing.T, ctx context.Context, kv jetstream.KeyValue, id string) {
	t.Helper()
	if !live.PutStory(ctx, kv, &dorv1.Story{
		Id: id, Title: "Working headline", TitleSource: "source",
		Category:    dorv1.Category_CATEGORY_DISASTER,
		SourceCount: 2, Heat: 0.5,
		FirstSeen: time.Now().Add(-time.Hour).Unix(),
		UpdatedAt: time.Now().Add(-time.Minute).Unix(),
		Locations: []*dorv1.Location{{PlaceId: "pahala", Name: "Pahala", Country: "US"}},
	}) {
		t.Fatal("seed live story")
	}
	meta, err := json.Marshal(live.StoryMeta{
		ID: id, ArticleIDs: []string{"a-f12-1", "a-f12-2"}, SourceIDs: []string{"wire-1", "wire-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryMetaKey(id), meta); err != nil {
		t.Fatalf("seed storymeta: %v", err)
	}
	for i, articleID := range []string{"a-f12-1", "a-f12-2"} {
		data, err := proto.Marshal(&dorv1.Article{
			Id: articleID, SourceId: []string{"wire-1", "wire-2"}[i],
			Title:       []string{"First source headline", "Second source headline"}[i],
			Lede:        "A lede with enough detail for the brief.",
			Url:         "https://example.test/f12/" + articleID,
			PublishedAt: time.Now().Add(-time.Hour).Unix(),
			Lang:        "en",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := kv.Put(ctx, live.ArticleKey(articleID), data); err != nil {
			t.Fatalf("seed article %s: %v", articleID, err)
		}
	}
}

func TestF12GenerateStoryWritesEditorTitleMarker(t *testing.T) {
	b := textsvcTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatalf("live_state kv: %v", err)
	}
	const id = "story-f12-generate"
	f12SeedStory(t, ctx, kv, id)

	fake := &f12Completer{res: llm.Result{
		Text:  `{"title":"Neutral editor headline","brief":"A brief from two sources."}`,
		Model: "test-opus",
	}}
	svc := New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), fake)
	fixed := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return fixed }

	out, ok := svc.GenerateStory(ctx, id)
	if !ok {
		t.Fatal("GenerateStory did not find the story")
	}
	if out.Provenance != live.BriefGenerated {
		t.Fatalf("provenance = %q, want generated", out.Provenance)
	}
	if out.Title != "Neutral editor headline" || out.Brief != "A brief from two sources." || !out.Available {
		t.Fatalf("generated text = %+v", out)
	}
	if fake.calls != 1 || len(fake.jobs) != 1 || fake.jobs[0].Name != "brief" {
		t.Fatalf("completer calls = %d jobs = %+v, want one brief job", fake.calls, fake.jobs)
	}

	stored, ok := live.LoadStory(ctx, kv, id)
	if !ok {
		t.Fatal("generated story missing from live_state")
	}
	// textsvc does not own story.<id>; the clusterer applies the marker.
	if stored.Title != "Working headline" || stored.TitleSource != "source" {
		t.Fatalf("live story title = (%q,%q), want the cluster-owned source title untouched", stored.Title, stored.TitleSource)
	}
	marker, ok := live.LoadStoryTitle(ctx, kv, id)
	if !ok || marker.Title != "Neutral editor headline" {
		t.Fatalf("storytitle marker = (%+v,%v), want the editor title", marker, ok)
	}

	// Marker-only: no story.changed from textsvc; the clusterer publishes.
	if got := f12SubjectMessages(t, ctx, b, bus.StoryChangedSubject(id)); got != 0 {
		t.Fatalf("story.changed messages = %d, want 0", got)
	}

	cached, ok := live.LoadStoryText(ctx, kv, id)
	if !ok {
		t.Fatal("generated brief was not cached")
	}
	if cached.Provenance != live.BriefGenerated || cached.Model != "test-opus" {
		t.Fatalf("cached brief = %+v", cached)
	}
}

// A cluster content update during the model call must not be reverted by the
// editor-title path. textsvc writes only the marker; story.<id> stays newer.
type f12RacingCompleter struct {
	kv jetstream.KeyValue
	id string
}

func (c *f12RacingCompleter) Complete(ctx context.Context, _ llm.Job) (llm.Result, error) {
	live.PutStory(ctx, c.kv, &dorv1.Story{
		Id: c.id, Title: "Working headline", TitleSource: "source",
		Category: dorv1.Category_CATEGORY_DISASTER, SourceCount: 3, Heat: 0.9,
		FirstSeen: time.Now().Add(-time.Hour).Unix(), UpdatedAt: time.Now().Unix(),
		Locations: []*dorv1.Location{{PlaceId: "pahala", Name: "Pahala", Country: "US"}, {PlaceId: "hilo", Name: "Hilo", Country: "US"}},
	})
	return llm.Result{Text: `{"title":"Neutral editor headline","brief":"A brief."}`, Model: "test-opus"}, nil
}

func TestF12EditorTitleDoesNotRevertNewerStory(t *testing.T) {
	b := textsvcTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatalf("live_state kv: %v", err)
	}
	const id = "story-f12-race"
	f12SeedStory(t, ctx, kv, id)
	svc := New(b, slog.New(slog.NewTextHandler(io.Discard, nil)), &f12RacingCompleter{kv: kv, id: id})
	if _, ok := svc.GenerateStory(ctx, id); !ok {
		t.Fatal("GenerateStory did not find the story")
	}
	got, ok := live.LoadStory(ctx, kv, id)
	if !ok {
		t.Fatal("story missing after editor title")
	}
	if got.SourceCount != 3 || len(got.Locations) != 2 || got.Heat != 0.9 {
		t.Fatalf("editor title reverted newer story: sources=%d locations=%d heat=%v", got.SourceCount, len(got.Locations), got.Heat)
	}
	marker, ok := live.LoadStoryTitle(ctx, kv, id)
	if !ok || marker.Title != "Neutral editor headline" {
		t.Fatalf("marker = (%+v,%v), want the editor title", marker, ok)
	}
}

func TestF12GenerateStoryWithoutModelFallsBack(t *testing.T) {
	b := textsvcTestBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatalf("live_state kv: %v", err)
	}
	const id = "story-f12-no-model"
	f12SeedStory(t, ctx, kv, id)

	svc := textsvcTestService(t, b, time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC))
	out, ok := svc.GenerateStory(ctx, id)
	if !ok {
		t.Fatal("GenerateStory did not find the story")
	}
	if out.Provenance != live.BriefNone || out.Brief != "" || out.Available {
		t.Fatalf("fallback = (%q,%q,%v), want none/empty/unavailable", out.Provenance, out.Brief, out.Available)
	}
	if out.Title != "First source headline" {
		t.Fatalf("fallback title = %q, want the first source headline", out.Title)
	}
}

func f12SubjectMessages(t *testing.T, ctx context.Context, b *bus.Bus, subject string) uint64 {
	t.Helper()
	stream, err := b.StreamNamed(ctx, bus.StreamForSubject(subject))
	if err != nil {
		t.Fatalf("stream for %s: %v", subject, err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatalf("stream info for %s: %v", subject, err)
	}
	return info.State.Subjects[subject]
}
