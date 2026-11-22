package agents

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/sources"
	"github.com/ankojh/dor/internal/store"
)

// F1·2 tests the enrichment provenance plumbing: language resolution, the
// hazard-fed rule classifier, cluster story provenance, publisher wire
// reconciliation and the editor-title overlay. Nothing here touches Postgres
// or a model; the sinks are in-memory fakes.

func TestF12ResolveLanguage(t *testing.T) {
	frenchTitle := "Le gouvernement annonce de nouvelles mesures"
	frenchLede := "Le gouvernement français a annoncé mardi de nouvelles mesures contre la hausse des prix de l'énergie."
	russian := "Правительство объявило о новых мерах по снижению цен на энергию"
	arabic := "أعلنت الحكومة عن إجراءات جديدة لخفض أسعار الطاقة"
	chinese := "政府宣布了应对能源价格上涨的新措施，并承诺加强市场监督"

	for _, tc := range []struct {
		name     string
		declared string
		title    string
		lede     string
		want     string
	}{
		{"declared region tag reduces to base", "fr-CA", "Whatever the headline says", "", "fr"},
		{"declared tag wins over text", "fr", "Storm closes airport runways", "", "fr"},
		{"french title and lede", "", frenchTitle, frenchLede, "fr"},
		{"french title only", "", frenchLede, "", "fr"},
		{"russian text", "", russian, "", "ru"},
		{"arabic text", "", arabic, "", "ar"},
		{"chinese text", "", chinese, "", "zh"},
		{"too short to decide", "", "M 4.1", "", "und"},
		{"blank text", "", "", "", "und"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveLanguage(tc.declared, tc.title, tc.lede); got != tc.want {
				t.Fatalf("resolveLanguage(%q, %q, %q) = %q, want %q", tc.declared, tc.title, tc.lede, got, tc.want)
			}
		})
	}
}

// The normalizer must store the detected language, never silently default to
// English. A source-declared tag is still passed through.
func TestF12NormalizerStoresDetectedLanguage(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	n := &normalizer{cfg: NormalizerConfig{Now: func() time.Time { return now }}}

	french := &dorv1.RawArticle{
		Id:          "raw-f12-fr",
		SourceId:    "wire-fr",
		Url:         "https://example.test/f12/french",
		Title:       "Le gouvernement annonce de nouvelles mesures",
		Summary:     "Le gouvernement français a annoncé mardi de nouvelles mesures contre la hausse des prix de l'énergie.",
		PublishedAt: now.Unix(),
		FetchedAt:   now.Unix(),
	}
	clean, record, reason, ok := n.normalize(french)
	if !ok {
		t.Fatalf("normalize rejected a valid article: %s", reason)
	}
	if clean.Lang != "fr" || record.Lang != "fr" {
		t.Fatalf("clean.Lang=%q record.Lang=%q, want fr for a French article", clean.Lang, record.Lang)
	}

	short := &dorv1.RawArticle{
		Id:          "raw-f12-und",
		SourceId:    "wire-und",
		Url:         "https://example.test/f12/short",
		Title:       "Hi",
		PublishedAt: now.Unix(),
		FetchedAt:   now.Unix(),
	}
	clean, record, reason, ok = n.normalize(short)
	if !ok {
		t.Fatalf("normalize rejected the short article: %s", reason)
	}
	if clean.Lang != "und" || record.Lang != "und" {
		t.Fatalf("clean.Lang=%q record.Lang=%q, want und; an unreadable fragment is not English", clean.Lang, record.Lang)
	}
}

func TestF12HazardFeedsDeclareEnglish(t *testing.T) {
	if got := hazardLanguage(sources.Source{}); got != "en" {
		t.Fatalf("hazardLanguage(no source lang) = %q, want en", got)
	}
	if got := hazardLanguage(sources.Source{Lang: "fr"}); got != "fr" {
		t.Fatalf("hazardLanguage(declared fr) = %q, want fr", got)
	}
	q := "M 2.2 - 19 km SW of Karluk, Alaska"
	if got := resolveLanguage("en", q, ""); got != "en" {
		t.Fatalf("resolveLanguage(declared en, quake) = %q, want en", got)
	}
}

func TestF12HazardClassificationShortCircuitsModel(t *testing.T) {
	article := &dorv1.Article{
		Id: "quake-hint", Title: "M 1.4 - 7 km SSW of Pahala, Hawaii",
		Hints: &dorv1.RawHints{Hazard: "earthquake"},
	}
	c := &classifier{log: testLogger()}
	got := c.classify(context.Background(), article)
	if got.GetCategory() != dorv1.Category_CATEGORY_DISASTER || got.GetBackend() != "hints" ||
		!got.GetAccepted() || got.GetDegraded() {
		t.Fatalf("hazard classify = %+v, want hints/disaster/accepted/not-degraded", got)
	}
}

func TestF12RuleCategoryHazardQuake(t *testing.T) {
	quake := &dorv1.Article{
		Id:    "quake-f12",
		Title: "M 1.4 - 7 km SSW of Pahala, Hawaii",
		Hints: &dorv1.RawHints{Hazard: "earthquake"},
	}
	got, conf := ruleCategory(quake)
	if got != "disaster" || conf <= 0 {
		t.Fatalf("ruleCategory(hazard quake) = (%q, %v), want disaster with positive confidence", got, conf)
	}

	plain := &dorv1.Article{Id: "plain-f12", Title: "Council meets on Tuesday"}
	got, conf = ruleCategory(plain)
	if got != "other" {
		t.Fatalf("ruleCategory(no hazard, no keywords) = %q, want other", got)
	}
	if conf < 0 {
		t.Fatalf("ruleCategory returned negative confidence %v", conf)
	}
}

func TestF12ApplyJoinProvenance(t *testing.T) {
	c := &clusterer{log: testLogger()}
	newStory := func() *storyState {
		return &storyState{Story: &dorv1.Story{Id: "story-f12"}}
	}

	t.Run("provenance follows the displayed category", func(t *testing.T) {
		st := newStory()
		st.Story.Category = dorv1.Category_CATEGORY_SPORTS
		c.applyJoinProvenance(st, nil, &dorv1.ClassifiedArticle{
			Backend: "claude", Category: dorv1.Category_CATEGORY_SPORTS, Confidence: 0.5,
		}, &dorv1.Article{})
		if st.CategoryBackend != "claude" || st.CategoryConfidence != 0.5 {
			t.Fatalf("first class = (%q, %v), want claude/0.5", st.CategoryBackend, st.CategoryConfidence)
		}
		c.applyJoinProvenance(st, nil, &dorv1.ClassifiedArticle{
			Backend: "rule", Category: dorv1.Category_CATEGORY_SPORTS, Confidence: 0.9,
		}, &dorv1.Article{})
		if st.CategoryBackend != "rule" || st.CategoryConfidence != 0.9 {
			t.Fatalf("stronger class = (%q, %v), want rule/0.9", st.CategoryBackend, st.CategoryConfidence)
		}
		c.applyJoinProvenance(st, nil, &dorv1.ClassifiedArticle{
			Backend: "jev", Category: dorv1.Category_CATEGORY_SPORTS, Confidence: 0.2,
		}, &dorv1.Article{})
		if st.CategoryBackend != "rule" || st.CategoryConfidence != 0.9 {
			t.Fatalf("weaker class overwrote provenance: (%q, %v)", st.CategoryBackend, st.CategoryConfidence)
		}
		// A higher-confidence classification for a different category cannot
		// relabel the category the story actually displays.
		c.applyJoinProvenance(st, nil, &dorv1.ClassifiedArticle{
			Backend: "jev", Category: dorv1.Category_CATEGORY_DISASTER, Confidence: 0.99,
		}, &dorv1.Article{})
		if st.CategoryBackend != "rule" || st.CategoryConfidence != 0.9 {
			t.Fatalf("mismatched class overwrote provenance: (%q, %v)", st.CategoryBackend, st.CategoryConfidence)
		}
		if st.Story.Category != dorv1.Category_CATEGORY_SPORTS {
			t.Fatalf("story category = %v, want SPORTS", st.Story.Category)
		}
	})

	t.Run("displayed category keeps its own provenance", func(t *testing.T) {
		now := time.Now()
		a1 := &dorv1.Article{Id: "a1", SourceId: "s1", Title: "Floods hit region"}
		place := &dorv1.PlacedArticle{Article: a1, Backend: "gazetteer"}
		class1 := &dorv1.ClassifiedArticle{Article: a1, Category: dorv1.Category_CATEGORY_DISASTER, Confidence: 0.6, Backend: "rule", Degraded: true, Fallback: true}
		st := c.buildStory(a1, place, class1.Category, nil, nil, now)
		c.applyJoinProvenance(st, place, class1, a1)

		a2 := &dorv1.Article{Id: "a2", SourceId: "s2", Title: "Army deployed"}
		class2 := &dorv1.ClassifiedArticle{Article: a2, Category: dorv1.Category_CATEGORY_CONFLICT, Confidence: 0.85, Backend: "jev", Accepted: true}
		st = c.mergeStory(st, a2, place, class2.Category, nil, nil, now)
		c.applyJoinProvenance(st, place, class2, a2)

		if st.Story.Category != dorv1.Category_CATEGORY_DISASTER {
			t.Fatalf("story category = %v, want the first non-OTHER DISASTER", st.Story.Category)
		}
		if st.CategoryBackend != "rule" || st.CategoryConfidence != 0.6 || !st.CategoryDegraded {
			t.Fatalf("displayed DISASTER provenance = (%q,%v,degraded=%v), want rule/0.6/degraded", st.CategoryBackend, st.CategoryConfidence, st.CategoryDegraded)
		}
		if !st.Story.Degraded {
			t.Fatal("story.Degraded = false; the displayed category is still a rule guess")
		}
	})

	t.Run("place backend rank prefers jev", func(t *testing.T) {
		st := newStory()
		c.applyJoinProvenance(st, &dorv1.PlacedArticle{Backend: "gazetteer"}, nil, &dorv1.Article{})
		if st.PlaceBackend != "gazetteer" {
			t.Fatalf("place backend = %q, want gazetteer", st.PlaceBackend)
		}
		c.applyJoinProvenance(st, &dorv1.PlacedArticle{Backend: "hints"}, nil, &dorv1.Article{})
		if st.PlaceBackend != "gazetteer" {
			t.Fatalf("weaker hint backend overwrote gazetteer: %q", st.PlaceBackend)
		}
		c.applyJoinProvenance(st, &dorv1.PlacedArticle{Backend: "jev"}, nil, &dorv1.Article{})
		if st.PlaceBackend != "jev" {
			t.Fatalf("jev did not win: %q", st.PlaceBackend)
		}
	})

	t.Run("degraded and fallback combine across class and place", func(t *testing.T) {
		st := newStory()
		c.applyJoinProvenance(st,
			&dorv1.PlacedArticle{Backend: "hints", Degraded: true},
			&dorv1.ClassifiedArticle{Backend: "claude", Confidence: 0.8, Fallback: true},
			&dorv1.Article{})
		if !st.Story.Degraded {
			t.Fatal("story.Degraded = false, want true because the place is degraded")
		}
		if !st.Story.Fallback {
			t.Fatal("story.Fallback = false, want true because the class fell back")
		}
		if !st.Story.PlaceDegraded || st.Story.PlaceFallback {
			t.Fatalf("place provenance not published: degraded=%v fallback=%v", st.Story.PlaceDegraded, st.Story.PlaceFallback)
		}
		if !st.CategoryFallback {
			t.Fatal("category fallback not folded into the process-local provenance")
		}
	})

	t.Run("hazard and magnitude survive into the story", func(t *testing.T) {
		st := newStory()
		c.applyJoinProvenance(st, nil, nil, &dorv1.Article{
			Hints: &dorv1.RawHints{Hazard: "earthquake", Magnitude: 5.2},
		})
		if st.Story.Hazard != "earthquake" || st.Story.Magnitude != 5.2 {
			t.Fatalf("story hazard=(%q,%v), want earthquake/5.2", st.Story.Hazard, st.Story.Magnitude)
		}
	})
}

type f12Sink struct {
	stories    []store.StoryRecord
	enrichment []store.ArticleEnrichmentRecord
}

func (s *f12Sink) EnqueueStory(rec store.StoryRecord) bool {
	s.stories = append(s.stories, rec)
	return true
}

func (s *f12Sink) EnqueueArticleEnrichment(rec store.ArticleEnrichmentRecord) bool {
	s.enrichment = append(s.enrichment, rec)
	return true
}

func TestF12EnqueueStoreRecordsProvenance(t *testing.T) {
	fixed := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	sink := &f12Sink{}
	c := &clusterer{
		deps: Deps{StorySink: sink},
		log:  testLogger(),
		now:  func() time.Time { return fixed },
	}
	st := &storyState{
		Story: &dorv1.Story{
			Id:          "story-f12-store",
			Title:       "Pahala quake",
			TitleSource: "source",
			Category:    dorv1.Category_CATEGORY_DISASTER,
			Primary:     0,
			SourceCount: 2,
			Heat:        0.6,
			Degraded:    true,
			Fallback:    true,
			Locations: []*dorv1.Location{{
				PlaceId: "pahala", Name: "Pahala", Country: "US",
				Lat: 19.2, Lon: -155.4, Role: dorv1.Role_ROLE_VENUE, Weight: 0.7,
			}},
		},
		CategoryBackend:    "jev",
		CategoryConfidence: 0.83,
		CategoryDegraded:   true,
		CategoryFallback:   true,
		PlaceBackend:       "gazetteer",
		PlaceDegraded:      true,
		PlaceFallback:      false,
		Embedding:          []float32{0.1, 0.2},
		Entities:           []string{"Pahala"},
		FirstSeen:          fixed.Unix(),
		UpdatedAt:          fixed.Unix(),
		ExpiresAt:          fixed.Add(48 * time.Hour),
	}
	article := &dorv1.Article{Id: "article-f12-store"}

	c.enqueueStore(st, article)
	if len(sink.stories) != 1 {
		t.Fatalf("enqueued %d story records, want 1", len(sink.stories))
	}
	rec := sink.stories[0]
	if rec.Brief != "" || rec.BriefSource != "none" {
		t.Fatalf("brief/brief_source = (%q,%q), want empty/none; the clusterer never writes a brief", rec.Brief, rec.BriefSource)
	}
	if rec.TitleSource != "source" {
		t.Fatalf("title_source = %q, want source", rec.TitleSource)
	}
	if rec.CategoryBackend != "jev" || rec.CategoryConfidence != 0.83 {
		t.Fatalf("category provenance = (%q,%v), want jev/0.83", rec.CategoryBackend, rec.CategoryConfidence)
	}
	if rec.PlaceBackend != "gazetteer" || !rec.PlaceDegraded || rec.PlaceFallback {
		t.Fatalf("place provenance = (%q,%v,%v)", rec.PlaceBackend, rec.PlaceDegraded, rec.PlaceFallback)
	}
	if !rec.Degraded || !rec.Fallback {
		t.Fatalf("degraded/fallback = (%v,%v), want true/true", rec.Degraded, rec.Fallback)
	}
	if rec.PrimaryLoc != "pahala" {
		t.Fatalf("primary loc = %q, want pahala", rec.PrimaryLoc)
	}
	if len(rec.Locations) != 1 || rec.Locations[0].Country != "US" || rec.Locations[0].Confidence != 0.7 {
		t.Fatalf("location record = %+v, want country US and confidence from weight", rec.Locations)
	}
	if len(rec.ArticleIDs) != 1 || rec.ArticleIDs[0] != article.Id {
		t.Fatalf("article ids = %v, want [%s]", rec.ArticleIDs, article.Id)
	}

	// An editor-owned title is carried by the same record.
	st.Story.TitleSource = "editor"
	st.Story.Title = "Editor-neutral headline"
	c.enqueueStore(st, article)
	if len(sink.stories) != 2 {
		t.Fatalf("enqueued %d story records, want 2", len(sink.stories))
	}
	if got := sink.stories[1]; got.TitleSource != "editor" || got.Title != "Editor-neutral headline" {
		t.Fatalf("editor title record = (%q,%q)", got.Title, got.TitleSource)
	}
}

func TestF12EnqueueEnrichment(t *testing.T) {
	sink := &f12Sink{}
	c := &clusterer{deps: Deps{StorySink: sink}, log: testLogger()}
	article := &dorv1.Article{Id: "article-f12-enrich", Lang: "en"}
	place := &dorv1.PlacedArticle{
		Primary: 0,
		Locations: []*dorv1.Location{{
			PlaceId: "paris", Name: "Paris", Country: "FR",
			Lat: 48.86, Lon: 2.35, Role: dorv1.Role_ROLE_VENUE, Weight: 0.9,
		}},
	}
	class := &dorv1.ClassifiedArticle{
		Category:   dorv1.Category_CATEGORY_SPORTS,
		Confidence: 0.61,
		Lang:       "fr",
		Backend:    "jev",
	}

	c.enqueueEnrichment(place, class, article)
	if len(sink.enrichment) != 1 {
		t.Fatalf("enqueued %d enrichment records, want 1", len(sink.enrichment))
	}
	rec := sink.enrichment[0]
	if rec.ID != article.Id {
		t.Fatalf("record id = %q, want %q", rec.ID, article.Id)
	}
	if rec.Category != int16(dorv1.Category_CATEGORY_SPORTS) || rec.CategoryConfidence != 0.61 || rec.Lang != "fr" {
		t.Fatalf("classification = (%d,%v,%q)", rec.Category, rec.CategoryConfidence, rec.Lang)
	}
	if len(rec.Locations) != 1 || rec.Locations[0].Country != "FR" || rec.Locations[0].Confidence != 0.9 {
		t.Fatalf("locations = %+v, want Paris/FR with confidence", rec.Locations)
	}

	// With no class, the article's language is the fallback.
	c.enqueueEnrichment(place, nil, article)
	if len(sink.enrichment) != 2 {
		t.Fatalf("enqueued %d enrichment records, want 2", len(sink.enrichment))
	}
	if got := sink.enrichment[1]; got.Lang != "en" {
		t.Fatalf("fallback lang = %q, want en", got.Lang)
	}

	// No place and no class: nothing to persist.
	c.enqueueEnrichment(nil, nil, article)
	if len(sink.enrichment) != 2 {
		t.Fatalf("enqueued %d enrichment records after nil join, want 2", len(sink.enrichment))
	}
}

func TestF12OverlayEditorTitle(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatalf("live_state kv: %v", err)
	}
	c := &clusterer{bus: b, log: testLogger()}

	t.Run("live editor story wins", func(t *testing.T) {
		const id = "story-f12-live-editor"
		if !live.PutStory(ctx, kv, &dorv1.Story{
			Id: id, Title: "Editor-neutral headline", TitleSource: "editor",
			UpdatedAt: time.Now().Unix(),
		}) {
			t.Fatal("seed live story")
		}
		st := &storyState{Story: &dorv1.Story{Id: id, Title: "Source headline", TitleSource: "source"}}
		c.overlayEditorTitle(ctx, kv, st)
		if st.Story.Title != "Editor-neutral headline" {
			t.Fatalf("title = %q, want the editor title", st.Story.Title)
		}
		if st.Story.TitleSource != "editor" {
			t.Fatalf("title_source = %q, want editor", st.Story.TitleSource)
		}
	})

	t.Run("storytitle marker survives a torn derived write", func(t *testing.T) {
		const id = "story-f12-marker"
		if _, err := kv.Put(ctx, live.StoryTitleKey(id), []byte(`{"title":"Marker headline","generated_at":"2026-11-22T12:00:00Z"}`)); err != nil {
			t.Fatalf("seed marker: %v", err)
		}
		st := &storyState{Story: &dorv1.Story{Id: id, Title: "Source headline", TitleSource: "source"}}
		c.overlayEditorTitle(ctx, kv, st)
		if st.Story.Title != "Marker headline" {
			t.Fatalf("title = %q, want the marker title", st.Story.Title)
		}
		if st.Story.TitleSource != "editor" {
			t.Fatalf("title_source = %q, want editor; the marker is editor-owned provenance", st.Story.TitleSource)
		}
	})

	t.Run("non-editor live story does not overwrite the derived title", func(t *testing.T) {
		const id = "story-f12-source"
		if !live.PutStory(ctx, kv, &dorv1.Story{
			Id: id, Title: "Older derived title", TitleSource: "source",
			UpdatedAt: time.Now().Add(-time.Hour).Unix(),
		}) {
			t.Fatal("seed live story")
		}
		st := &storyState{Story: &dorv1.Story{Id: id, Title: "Newer derived title", TitleSource: "source"}}
		c.overlayEditorTitle(ctx, kv, st)
		if st.Story.Title != "Newer derived title" {
			t.Fatalf("title = %q, want the derived title unchanged", st.Story.Title)
		}
	})
}

func TestF12WireWithStory(t *testing.T) {
	story := &dorv1.Story{
		Id: "story-f12-wire", Category: dorv1.Category_CATEGORY_DISASTER,
		Degraded: true, Fallback: false, CategoryBackend: "rule", CategoryConfidence: 0.6,
		PlaceBackend: "gazetteer", PlaceDegraded: false, PlaceFallback: true,
		Primary: 0,
		Locations: []*dorv1.Location{{
			PlaceId: "pahala", Name: "Pahala", Country: "US", Lat: 19.2, Lon: -155.4,
		}},
	}

	t.Run("fills missing row metadata and location", func(t *testing.T) {
		row := &dorv1.WireRow{ArticleId: "article-f12-wire", StoryId: story.Id, Category: dorv1.Category_CATEGORY_OTHER}
		next, changed := wireWithStory(row, story)
		if !changed {
			t.Fatal("wireWithStory reported no change for a row missing story metadata")
		}
		if next.Category != story.Category || !next.Degraded || next.Fallback != story.Fallback {
			t.Fatalf("category/degraded/fallback = (%v,%v,%v)", next.Category, next.Degraded, next.Fallback)
		}
		if next.CategoryBackend != "rule" || next.CategoryConfidence != 0.6 {
			t.Fatalf("category provenance = (%q,%v)", next.CategoryBackend, next.CategoryConfidence)
		}
		if next.PlaceBackend != "gazetteer" || next.PlaceDegraded || !next.PlaceFallback {
			t.Fatalf("place provenance = (%q,%v,%v)", next.PlaceBackend, next.PlaceDegraded, next.PlaceFallback)
		}
		if next.Country != "US" || next.Place != "Pahala" || !next.Located {
			t.Fatalf("location = country %q place %q located %v, want US/Pahala/true", next.Country, next.Place, next.Located)
		}
		if diff := next.Lat - 19.2; diff > 1e-4 || diff < -1e-4 {
			t.Fatalf("lat = %v, want 19.2", next.Lat)
		}
		if diff := next.Lon + 155.4; diff > 1e-4 || diff < -1e-4 {
			t.Fatalf("lon = %v, want -155.4", next.Lon)
		}
		// The original row is never mutated.
		if row.Category != dorv1.Category_CATEGORY_OTHER || row.Located {
			t.Fatalf("wireWithStory mutated the source row: %+v", row)
		}
	})

	t.Run("identical row is returned unchanged", func(t *testing.T) {
		row := &dorv1.WireRow{
			ArticleId: "article-f12-wire", StoryId: story.Id,
			Category: story.Category, Degraded: story.Degraded, Fallback: story.Fallback,
			CategoryBackend: story.CategoryBackend, CategoryConfidence: story.CategoryConfidence,
			PlaceBackend: story.PlaceBackend, PlaceDegraded: story.PlaceDegraded, PlaceFallback: story.PlaceFallback,
			Country: "US", Place: "Pahala", Located: true, Lat: 19.2, Lon: -155.4,
		}
		next, changed := wireWithStory(row, story)
		if changed {
			t.Fatal("wireWithStory reported a change for an identical row")
		}
		if next != row {
			t.Fatal("wireWithStory should return the original row when nothing changed")
		}
	})

	t.Run("existing row location is not overwritten", func(t *testing.T) {
		row := &dorv1.WireRow{
			ArticleId: "article-f12-wire", StoryId: story.Id,
			Category: story.Category, Degraded: story.Degraded, Fallback: story.Fallback,
			CategoryBackend: story.CategoryBackend, CategoryConfidence: story.CategoryConfidence,
			PlaceBackend: story.PlaceBackend, PlaceDegraded: story.PlaceDegraded, PlaceFallback: story.PlaceFallback,
			Country: "GB", Place: "London", Located: true, Lat: 51.5, Lon: -0.12,
		}
		next, changed := wireWithStory(row, story)
		if changed {
			t.Fatalf("wireWithStory changed a row whose metadata and location already differ only by value: %+v", next)
		}
	})
}

// The clusterer publishes story.changed before article.placed. The story is
// still pending (not in c.stories) when the placed article is handled, so the
// row must read the pending story instead of staying OTHER.
func TestF12PlacedAfterPendingStoryUsesPendingMetadata(t *testing.T) {
	core := newPublisherCore(nil, testLogger())
	story := &dorv1.Story{
		Id: "s-pending", Category: dorv1.Category_CATEGORY_DISASTER,
		CategoryBackend: "rule", CategoryConfidence: 0.6, Degraded: true,
		Locations: []*dorv1.Location{{PlaceId: "p", Name: "Pahala", Country: "US", Lat: 19.2, Lon: -155.4}},
	}
	core.onArticle(&dorv1.Article{Id: "a-pending", Title: "M 4.6 - Pahala", SourceId: "usgs"}, false)
	core.onStory(story)
	core.onArticle(&dorv1.Article{
		Id: "a-pending", Title: "M 4.6 - Pahala", SourceId: "usgs",
		StoryId: story.Id, Join: dorv1.JoinKind_JOIN_KIND_JOINED,
	}, true)
	row := core.wireRows["a-pending"]
	if row == nil {
		t.Fatal("wire row missing")
	}
	if row.Category != dorv1.Category_CATEGORY_DISASTER || row.CategoryBackend != "rule" ||
		row.CategoryConfidence != 0.6 || !row.Degraded {
		t.Fatalf("row = category %v backend %q conf %v degraded %v, want pending story metadata",
			row.Category, row.CategoryBackend, row.CategoryConfidence, row.Degraded)
	}
	if row.Country != "US" || row.Place != "Pahala" || !row.Located {
		t.Fatalf("row place = (%q,%q,%v), want the pending story's primary place", row.Country, row.Place, row.Located)
	}
}

func TestF12OnStoryReconcilesWireRows(t *testing.T) {
	core := newPublisherCore(nil, testLogger())
	story := &dorv1.Story{
		Id: "story-f12-reconcile", Category: dorv1.Category_CATEGORY_DISASTER,
		Degraded: true, CategoryBackend: "rule", CategoryConfidence: 0.55,
		PlaceBackend: "hints", PlaceFallback: true,
		Primary: 0,
		Locations: []*dorv1.Location{{
			PlaceId: "pahala", Name: "Pahala", Country: "US", Lat: 19.2, Lon: -155.4,
		}},
	}
	core.wireRows["article-f12-reconcile"] = &dorv1.WireRow{
		ArticleId: "article-f12-reconcile", StoryId: story.Id, Category: dorv1.Category_CATEGORY_OTHER,
	}
	core.wireOrder = []string{"article-f12-reconcile"}
	// A row for another story must be left alone.
	core.wireRows["article-f12-other"] = &dorv1.WireRow{
		ArticleId: "article-f12-other", StoryId: "story-other", Category: dorv1.Category_CATEGORY_OTHER,
	}
	core.wireOrder = append(core.wireOrder, "article-f12-other")

	core.onStory(story)

	row := core.wireRows["article-f12-reconcile"]
	if row == nil {
		t.Fatal("matching wire row disappeared")
	}
	if row.Category != dorv1.Category_CATEGORY_DISASTER || !row.Degraded || row.CategoryBackend != "rule" || row.CategoryConfidence != 0.55 {
		t.Fatalf("reconciled row metadata = %+v", row)
	}
	if row.Country != "US" || row.Place != "Pahala" || !row.Located {
		t.Fatalf("reconciled row location = (%q,%q,%v)", row.Country, row.Place, row.Located)
	}
	if got := core.wireRows["article-f12-other"]; got.Category != dorv1.Category_CATEGORY_OTHER {
		t.Fatalf("unrelated row was reconciled: %+v", got)
	}

	core.mu.Lock()
	defer core.mu.Unlock()
	storyDelta, ok := core.pending["story:"+story.Id]
	if !ok || storyDelta.GetStoryUpsert() == nil {
		t.Fatalf("story upsert was not enqueued: %+v", core.pending)
	}
	wireDelta, ok := core.pending["wire:article-f12-reconcile"]
	if !ok || wireDelta.GetWire() == nil {
		t.Fatalf("wire delta was not enqueued: %+v", core.pending)
	}
	if wireDelta.GetWire().Category != dorv1.Category_CATEGORY_DISASTER {
		t.Fatalf("enqueued wire delta = %+v", wireDelta.GetWire())
	}
}

// Guard against a regression where the provenance fields stop being cloneable
// through the wire path (proto.Clone preserves unknown fields as well).
func TestF12WireWithStoryPreservesPromptScores(t *testing.T) {
	row := &dorv1.WireRow{
		ArticleId: "article-f12-prompts", StoryId: "story-f12-prompts",
		Category:      dorv1.Category_CATEGORY_OTHER,
		PromptScores:  map[string]float32{"energy": 0.9},
		ApproxPrompts: []string{"energy"},
	}
	story := &dorv1.Story{Id: "story-f12-prompts", Category: dorv1.Category_CATEGORY_ECONOMY}
	next, changed := wireWithStory(row, story)
	if !changed {
		t.Fatal("expected a category change")
	}
	if next.PromptScores["energy"] != 0.9 || len(next.ApproxPrompts) != 1 {
		t.Fatalf("prompt scores lost in the reconciled copy: %+v", next)
	}
	cloned := proto.Clone(next).(*dorv1.WireRow)
	if cloned.GetCategory() != dorv1.Category_CATEGORY_ECONOMY {
		t.Fatalf("cloned category = %v", cloned.GetCategory())
	}
}
