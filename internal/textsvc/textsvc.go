// Package textsvc serves story briefs and link "why" sentences. The editor
// writes briefs ahead of time; the gateway calls the service on GET /api/text
// and a miss generates lazily. Every failure (no key, budget, refusal, model
// error) becomes available:false with the best headline, never an error in the
// UI.
package textsvc

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
)

// Completer is the subset of *llm.Client used here.
type Completer interface {
	Complete(ctx context.Context, job llm.Job) (llm.Result, error)
}

// TextSink is the narrow durable mirror for the KV text cache. Nil keeps the
// service fully functional with KV only.
type TextSink interface {
	EnqueueStoryText(store.StoryTextRecord) bool
	EnqueueLinkText(store.LinkTextRecord) bool
}

// Service reads and fills the storytext/linktext caches in live_state.
type Service struct {
	bus  *bus.Bus
	log  *slog.Logger
	llm  Completer
	sink TextSink
	now  func() time.Time

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error

	sem    chan struct{}
	mu     sync.Mutex
	flight map[string]bool
}

// New builds the service. llm may be nil: every request returns the fallback.
func New(b *bus.Bus, log *slog.Logger, l Completer) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		bus:    b,
		log:    log,
		llm:    l,
		now:    time.Now,
		sem:    make(chan struct{}, 2),
		flight: make(map[string]bool),
	}
}

// WithSink attaches the optional durable mirror. Call it before the service
// starts serving; the sink itself must be non-blocking.
func (s *Service) WithSink(sink TextSink) *Service {
	s.sink = sink
	return s
}

func (s *Service) keyValue(ctx context.Context) jetstream.KeyValue {
	s.kvOnce.Do(func() {
		s.kv, s.kvErr = s.bus.KV(ctx, bus.KVBucketLiveState)
		if s.kvErr != nil {
			s.log.Warn("text: live_state unavailable; briefs disabled", "err", s.kvErr)
		}
	})
	return s.kv
}

func (s *Service) nowString() string { return s.now().UTC().Format(time.RFC3339) }

// Story returns the cached brief, or a fallback headline when none exists.
func (s *Service) Story(ctx context.Context, id string) (live.StoryText, bool) {
	kv := s.keyValue(ctx)
	if t, ok := live.LoadStoryText(ctx, kv, id); ok {
		return withProvenance(t), true
	}
	story, ok := live.LoadStory(ctx, kv, id)
	if !ok {
		return live.StoryText{}, false
	}
	return live.StoryText{
		Kind:        "story",
		ID:          id,
		Title:       story.Title,
		SourceCount: int(story.SourceCount),
		Available:   false,
		Provenance:  live.BriefNone,
	}, true
}

// GenerateStory returns a cached brief when it is newer than the story, and
// otherwise writes one (or the fallback). It never returns an error: the UI
// contract is available:false.
func (s *Service) GenerateStory(ctx context.Context, id string) (live.StoryText, bool) {
	kv := s.keyValue(ctx)
	story, ok := live.LoadStory(ctx, kv, id)
	if !ok {
		return live.StoryText{}, false
	}
	meta, _ := live.LoadStoryMeta(ctx, kv, id)
	updated := time.Unix(story.UpdatedAt, 0)
	if story.UpdatedAt == 0 {
		updated = time.Unix(story.FirstSeen, 0)
	}
	if cached, ok := live.LoadStoryText(ctx, kv, id); ok {
		if generated, err := time.Parse(time.RFC3339, cached.GeneratedAt); err == nil {
			// A successful brief is kept until the story changes; a fallback
			// is retried after 5 minutes or when the story updates.
			if cached.Available && !generated.Before(updated) {
				return withProvenance(cached), true
			}
			if !cached.Available && s.now().Sub(generated) < 5*time.Minute && !generated.Before(updated) {
				return withProvenance(cached), true
			}
		}
	}

	sources := s.sources(ctx, kv, meta)
	if story.SourceCount < 2 || s.llm == nil || len(sources) < 2 {
		out := fallbackStory(story, sources, int(story.SourceCount), s.nowString())
		s.putStoryText(ctx, kv, out)
		return out, true
	}

	job := llm.BriefJob(llm.BriefInput{
		Title:    story.Title,
		Category: categoryName(story.Category),
		Place:    primaryPlace(story),
		Sources:  sources,
	})
	res, err := s.llm.Complete(ctx, job)
	if err != nil {
		s.log.Debug("text: brief unavailable", "story", id, "err", err)
		out := fallbackStory(story, sources, int(story.SourceCount), s.nowString())
		s.putStoryText(ctx, kv, out)
		return out, true
	}
	var draft llm.Draft
	if err := res.Decode(&draft); err != nil {
		s.log.Debug("text: brief decode failed", "story", id, "err", err)
		out := fallbackStory(story, sources, int(story.SourceCount), s.nowString())
		s.putStoryText(ctx, kv, out)
		return out, true
	}
	title := strings.TrimSpace(draft.Title)
	if title == "" {
		title = story.Title
	}
	brief := strings.TrimSpace(draft.Brief)
	out := live.StoryText{
		Kind:        "story",
		ID:          id,
		Title:       title,
		Brief:       brief,
		Available:   brief != "",
		Provenance:  live.BriefGenerated,
		SourceCount: int(story.SourceCount),
		GeneratedAt: s.nowString(),
		Model:       res.Model,
	}
	s.applyEditorTitle(ctx, kv, story, title)
	s.putStoryText(ctx, kv, out)
	return out, true
}

// applyEditorTitle records the Editor's neutral title in the storytitle
// marker. The clusterer owns story.<id>; it overlays this marker on the latest
// story state before writing the checkpoint and publishing story.changed.
// Writing the story snapshot loaded before the model call here would revert
// newer sources, locations and heat.
func (s *Service) applyEditorTitle(ctx context.Context, kv jetstream.KeyValue, story *dorv1.Story, title string) {
	title = strings.TrimSpace(title)
	if story == nil || story.Id == "" || title == "" {
		return
	}
	if story.Title == title && story.TitleSource == "editor" {
		return
	}
	marker, err := json.Marshal(map[string]string{
		"title":        title,
		"generated_at": s.nowString(),
	})
	if err != nil {
		return
	}
	if _, err := kv.Put(ctx, live.StoryTitleKey(story.Id), marker); err != nil {
		s.log.Debug("text: editor title marker write failed", "story", story.Id, "err", err)
	}
}

// Link returns the cached "why", or an unavailable placeholder for a known
// link. Unknown links return ok=false.
func (s *Service) Link(ctx context.Context, a, b string) (live.LinkText, bool) {
	a, b = live.SortPair(a, b)
	kv := s.keyValue(ctx)
	link, ok := live.LoadLink(ctx, kv, a, b)
	if !ok || (link.Strength <= 0 && link.Confidence <= 0) {
		return live.LinkText{}, false
	}
	revision := live.LinkRevision(link)
	if t, ok := live.LoadLinkText(ctx, kv, a, b); ok {
		if t.Revision == revision {
			return t, true
		}
		// The relation changed (or was re-created) since this explanation was
		// written; do not serve text about the old link.
		return live.LinkText{Kind: "link", A: a, B: b, Available: false, Revision: revision}, true
	}
	return live.LinkText{Kind: "link", A: a, B: b, Available: false, Revision: revision}, true
}

// GenerateLink returns a cached "why" or writes one. The cache is kept for the
// life of the link; a failed generation is retried after a minute.
func (s *Service) GenerateLink(ctx context.Context, a, b string) (live.LinkText, bool) {
	a, b = live.SortPair(a, b)
	kv := s.keyValue(ctx)
	link, ok := live.LoadLink(ctx, kv, a, b)
	if !ok || (link.Strength <= 0 && link.Confidence <= 0) {
		return live.LinkText{}, false
	}
	revision := live.LinkRevision(link)
	if cached, ok := live.LoadLinkText(ctx, kv, a, b); ok {
		if cached.Available && cached.Revision == revision {
			return cached, true
		}
		if !cached.Available && cached.Revision == revision {
			if generated, err := time.Parse(time.RFC3339, cached.GeneratedAt); err == nil && s.now().Sub(generated) < time.Minute {
				return cached, true
			}
		}
		// A successful explanation for an older relation is stale and must be
		// regenerated.
	}
	fallback := live.LinkText{Kind: "link", A: a, B: b, Available: false, GeneratedAt: s.nowString(), Revision: revision}
	if s.llm == nil {
		s.putLinkText(ctx, kv, fallback)
		return fallback, true
	}
	endA := s.linkEnd(ctx, kv, a)
	endB := s.linkEnd(ctx, kv, b)
	job := llm.WhyJob(llm.WhyInput{
		A:         endA,
		B:         endB,
		Type:      linkTypeName(link.Type),
		Direction: directionName(link.Dir),
		Strength:  float64(link.Strength),
	})
	res, err := s.llm.Complete(ctx, job)
	if err != nil {
		s.log.Debug("text: why unavailable", "a", a, "b", b, "err", err)
		s.putLinkText(ctx, kv, fallback)
		return fallback, true
	}
	var exp llm.Explanation
	if err := res.Decode(&exp); err != nil {
		s.putLinkText(ctx, kv, fallback)
		return fallback, true
	}
	out := live.LinkText{
		Kind:        "link",
		A:           a,
		B:           b,
		Why:         strings.TrimSpace(exp.Why),
		Available:   strings.TrimSpace(exp.Why) != "",
		GeneratedAt: s.nowString(),
		Model:       res.Model,
		Revision:    revision,
	}
	s.putLinkText(ctx, kv, out)
	return out, true
}

func (s *Service) putStoryText(ctx context.Context, kv jetstream.KeyValue, t live.StoryText) {
	live.PutStoryText(ctx, kv, t)
	if s.sink != nil {
		titleSource := ""
		if t.Provenance == live.BriefGenerated {
			titleSource = "editor"
		}
		briefSource := t.Provenance
		if briefSource == "" {
			briefSource = live.BriefNone
		}
		s.sink.EnqueueStoryText(store.StoryTextRecord{
			ID: t.ID, Title: t.Title, TitleSource: titleSource,
			Brief: t.Brief, BriefSource: briefSource, Model: t.Model,
		})
	}
}

func (s *Service) putLinkText(ctx context.Context, kv jetstream.KeyValue, t live.LinkText) {
	live.PutLinkText(ctx, kv, t)
	if s.sink != nil {
		s.sink.EnqueueLinkText(store.LinkTextRecord{A: t.A, B: t.B, Why: t.Why})
	}
}

// PrefetchStory fills a brief in the background (focus / hover intent).
func (s *Service) PrefetchStory(id string) {
	if id == "" {
		return
	}
	s.prefetch("story:"+id, func(ctx context.Context) { _, _ = s.GenerateStory(ctx, id) })
}

// PrefetchLink fills a "why" in the background.
func (s *Service) PrefetchLink(a, b string) {
	a, b = live.SortPair(a, b)
	if a == "" || b == "" {
		return
	}
	s.prefetch("link:"+a+"."+b, func(ctx context.Context) { _, _ = s.GenerateLink(ctx, a, b) })
}

func (s *Service) prefetch(key string, fn func(context.Context)) {
	s.mu.Lock()
	if s.flight[key] {
		s.mu.Unlock()
		return
	}
	s.flight[key] = true
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.flight, key)
			s.mu.Unlock()
		}()
		select {
		case s.sem <- struct{}{}:
		case <-time.After(5 * time.Second):
			return
		}
		defer func() { <-s.sem }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(ctx)
	}()
}

func (s *Service) sources(ctx context.Context, kv jetstream.KeyValue, meta live.StoryMeta) []llm.Source {
	out := make([]llm.Source, 0, len(meta.ArticleIDs))
	for _, id := range meta.ArticleIDs {
		if len(out) >= 8 {
			break
		}
		article, ok := live.LoadArticle(ctx, kv, id)
		if !ok {
			continue
		}
		out = append(out, llm.Source{
			Name:  article.SourceId,
			Title: article.Title,
			Lede:  article.Lede,
			URL:   article.Url,
		})
	}
	return out
}

func (s *Service) linkEnd(ctx context.Context, kv jetstream.KeyValue, id string) llm.LinkEnd {
	end := llm.LinkEnd{}
	if story, ok := live.LoadStory(ctx, kv, id); ok {
		end.Title = story.Title
		end.Place = primaryPlace(story)
	}
	if brief, ok := live.LoadStoryText(ctx, kv, id); ok && brief.Available {
		end.Brief = brief.Brief
	}
	return end
}

func fallbackStory(story *dorv1.Story, sources []llm.Source, sourceCount int, generated string) live.StoryText {
	title := story.Title
	// An applied editor title outranks a source headline even when a later
	// brief generation falls back. The fallback must not change the heading.
	if story.GetTitleSource() != "editor" && len(sources) > 0 && strings.TrimSpace(sources[0].Title) != "" {
		title = sources[0].Title
	}
	return live.StoryText{
		Kind:        "story",
		ID:          story.Id,
		Title:       title,
		Brief:       "",
		Available:   false,
		Provenance:  live.BriefNone,
		SourceCount: sourceCount,
		GeneratedAt: generated,
	}
}

func primaryPlace(story *dorv1.Story) string {
	if story == nil || len(story.Locations) == 0 {
		return ""
	}
	idx := int(story.Primary)
	if idx < 0 || idx >= len(story.Locations) {
		idx = 0
	}
	return story.Locations[idx].Name
}

// withProvenance fills provenance for caches written before the field existed.
// A cached brief with a model name is a generated brief; any other available
// brief is legacy source text, never an agent brief.
func withProvenance(t live.StoryText) live.StoryText {
	if t.Provenance != "" {
		return t
	}
	switch {
	case !t.Available:
		t.Provenance = live.BriefNone
	case t.Model != "":
		t.Provenance = live.BriefGenerated
	default:
		t.Provenance = live.BriefSourceSnippet
	}
	return t
}

func categoryName(c dorv1.Category) string {
	return strings.ToLower(strings.TrimPrefix(c.String(), "CATEGORY_"))
}

func linkTypeName(t dorv1.LinkType) string {
	switch t {
	case dorv1.LinkType_LINK_TYPE_CAUSAL:
		return "causal"
	case dorv1.LinkType_LINK_TYPE_REACTION:
		return "reaction"
	case dorv1.LinkType_LINK_TYPE_THEMATIC:
		return "thematic"
	default:
		return "thematic"
	}
}

func directionName(d dorv1.Direction) string {
	switch d {
	case dorv1.Direction_DIRECTION_A_TO_B:
		return "a_to_b"
	case dorv1.Direction_DIRECTION_B_TO_A:
		return "b_to_a"
	default:
		return "unclear"
	}
}
