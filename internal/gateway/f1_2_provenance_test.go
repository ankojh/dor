package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
)

// f12Text is the narrow TextService seam the story detail path reads. Only
// Story is exercised here; the write paths are covered in internal/textsvc.
type f12Text struct {
	text  live.StoryText
	found bool
}

func (f *f12Text) Story(context.Context, string) (live.StoryText, bool) {
	return f.text, f.found
}

func (f *f12Text) GenerateStory(context.Context, string) (live.StoryText, bool) {
	return f.text, f.found
}

func (f *f12Text) Link(context.Context, string, string) (live.LinkText, bool) {
	return live.LinkText{}, false
}

func (f *f12Text) GenerateLink(context.Context, string, string) (live.LinkText, bool) {
	return live.LinkText{}, false
}

func (f *f12Text) PrefetchStory(string) {}

func (f *f12Text) PrefetchLink(string, string) {}

func f12Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestF12StoryFromProtoMapsProvenance(t *testing.T) {
	story := &dorv1.Story{
		Id:                 "story-f12-proto",
		Title:              "Editor headline",
		TitleSource:        "editor",
		Category:           dorv1.Category_CATEGORY_DISASTER,
		CategoryBackend:    "rule",
		CategoryConfidence: 0.42,
		Degraded:           true,
		Fallback:           true,
		PlaceBackend:       "hints",
		PlaceDegraded:      true,
		PlaceFallback:      false,
		SourceCount:        3,
		Heat:               0.7,
		FirstSeen:          100,
		UpdatedAt:          200,
		Primary:            0,
		Locations: []*dorv1.Location{{
			PlaceId: "pahala", Name: "Pahala", Lat: 19.2, Lon: -155.4,
			Precision: dorv1.Precision_PRECISION_CITY, Role: dorv1.Role_ROLE_VENUE,
			Weight: 0.6, Country: "US",
		}},
	}
	got := storyFromProto(story, "a generated brief", true, live.BriefGenerated)
	if got.TitleSource != "editor" {
		t.Fatalf("title_source = %q, want editor", got.TitleSource)
	}
	if got.Brief != "a generated brief" || !got.BriefAvailable || got.BriefSource != live.BriefGenerated {
		t.Fatalf("brief = (%q,%v,%q)", got.Brief, got.BriefAvailable, got.BriefSource)
	}
	if got.Category != "disaster" || got.CategoryBackend != "rule" || got.CategoryConfidence != 0.42 {
		t.Fatalf("category = (%q,%q,%v)", got.Category, got.CategoryBackend, got.CategoryConfidence)
	}
	if !got.Degraded || !got.Fallback {
		t.Fatalf("degraded/fallback = (%v,%v)", got.Degraded, got.Fallback)
	}
	if got.PlaceBackend != "hints" || !got.PlaceDegraded || got.PlaceFallback {
		t.Fatalf("place provenance = (%q,%v,%v)", got.PlaceBackend, got.PlaceDegraded, got.PlaceFallback)
	}
	if len(got.Locations) != 1 || got.Locations[0].Country != "US" {
		t.Fatalf("locations = %+v, want one US location", got.Locations)
	}
}

func TestF12DetailFromStoreBriefProvenance(t *testing.T) {
	base := store.StoryDetail{
		ID: "story-f12-store", Title: "Stored title", TitleSource: "editor",
		Category: int16(dorv1.Category_CATEGORY_SPORTS), CategoryBackend: "jev",
		PlaceBackend: "gazetteer", Degraded: false, Fallback: false,
		SourceCount: 2, Heat: 0.4,
		FirstSeen: time.Unix(100, 0).UTC(), UpdatedAt: time.Unix(200, 0).UTC(),
		Locations: []store.LocationRecord{{
			PlaceID: "paris", Name: "Paris", Country: "FR", Lat: 48.86, Lon: 2.35,
			Precision: int16(dorv1.Precision_PRECISION_CITY), Role: int16(dorv1.Role_ROLE_VENUE), Weight: 0.8,
		}},
	}

	for _, tc := range []struct {
		name       string
		detail     store.StoryDetail
		text       *f12Text
		wantBrief  string
		wantOK     bool
		wantSource string
	}{
		{
			name:       "legacy DB brief without provenance is a source snippet",
			detail:     withBrief(base, "legacy excerpt", ""),
			wantBrief:  "legacy excerpt",
			wantOK:     true,
			wantSource: live.BriefSourceSnippet,
		},
		{
			name:       "no brief is none",
			detail:     withBrief(base, "", ""),
			wantBrief:  "",
			wantOK:     false,
			wantSource: live.BriefNone,
		},
		{
			name:       "explicit stored generated provenance is preserved",
			detail:     withBrief(base, "agent text", live.BriefGenerated),
			wantBrief:  "agent text",
			wantOK:     true,
			wantSource: live.BriefGenerated,
		},
		{
			name:       "generated provenance with no text degrades to none",
			detail:     withBrief(base, "", live.BriefGenerated),
			wantBrief:  "",
			wantOK:     false,
			wantSource: live.BriefNone,
		},
		{
			name:   "live generated text overrides the stored excerpt",
			detail: withBrief(base, "legacy excerpt", live.BriefSourceSnippet),
			text: &f12Text{found: true, text: live.StoryText{
				Brief: "fresh generated brief", Available: true, Provenance: live.BriefGenerated,
			}},
			wantBrief:  "fresh generated brief",
			wantOK:     true,
			wantSource: live.BriefGenerated,
		},
		{
			name:   "live text without provenance is treated as generated",
			detail: withBrief(base, "legacy excerpt", live.BriefSourceSnippet),
			text: &f12Text{found: true, text: live.StoryText{
				Brief: "fresh brief", Available: true,
			}},
			wantBrief:  "fresh brief",
			wantOK:     true,
			wantSource: live.BriefGenerated,
		},
		{
			name:   "unavailable live text does not clobber the stored excerpt",
			detail: withBrief(base, "legacy excerpt", live.BriefSourceSnippet),
			text: &f12Text{found: true, text: live.StoryText{
				Available: false, Provenance: live.BriefNone,
			}},
			wantBrief:  "legacy excerpt",
			wantOK:     true,
			wantSource: live.BriefSourceSnippet,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{}
			if tc.text != nil {
				cfg.Text = tc.text
			}
			s := &Server{cfg: cfg, log: f12Logger()}
			got := s.detailFromStore(context.Background(), tc.detail)
			if got.Story.Brief != tc.wantBrief || got.Story.BriefAvailable != tc.wantOK || got.Story.BriefSource != tc.wantSource {
				t.Fatalf("brief = (%q,%v,%q), want (%q,%v,%q)",
					got.Story.Brief, got.Story.BriefAvailable, got.Story.BriefSource,
					tc.wantBrief, tc.wantOK, tc.wantSource)
			}
			if got.Story.TitleSource != "editor" {
				t.Fatalf("title_source = %q, want the stored editor value", got.Story.TitleSource)
			}
			if len(got.Story.Locations) != 1 || got.Story.Locations[0].Country != "FR" {
				t.Fatalf("locations = %+v, want the stored FR location", got.Story.Locations)
			}
		})
	}
}

func withBrief(d store.StoryDetail, brief, source string) store.StoryDetail {
	d.Brief = brief
	d.BriefSource = source
	return d
}

// The live path is the primary detail source; the same provenance contract has
// to hold there without a durable store.
func TestF12CurrentStoryUsesLiveBriefProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: f12Logger()})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	defer b.Close()
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatalf("live_state kv: %v", err)
	}

	const storyID = "story-f12-live"
	if !live.PutStory(ctx, kv, &dorv1.Story{
		Id: storyID, Title: "Editor headline", TitleSource: "editor",
		Category: dorv1.Category_CATEGORY_DISASTER, CategoryBackend: "rule", CategoryConfidence: 0.5,
		Degraded: true, Fallback: false, PlaceBackend: "hints", PlaceDegraded: true,
		SourceCount: 2, Heat: 0.5, FirstSeen: 100, UpdatedAt: 200,
		Primary: 0,
		Locations: []*dorv1.Location{{
			PlaceId: "pahala", Name: "Pahala", Country: "US", Lat: 19.2, Lon: -155.4,
		}},
	}) {
		t.Fatal("seed live story")
	}
	meta, err := json.Marshal(live.StoryMeta{
		ID: storyID, ArticleIDs: []string{"article-f12-live"}, SourceIDs: []string{"wire"},
		FirstSeen: 100, UpdatedAt: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.StoryMetaKey(storyID), meta); err != nil {
		t.Fatalf("seed storymeta: %v", err)
	}
	articleData, err := proto.Marshal(&dorv1.Article{
		Id: "article-f12-live", SourceId: "wire", Title: "Source headline",
		Url: "https://example.test/f12/live", PublishedAt: 150, Lang: "en",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, live.ArticleKey("article-f12-live"), articleData); err != nil {
		t.Fatalf("seed article: %v", err)
	}

	s := &Server{
		cfg: Config{Bus: b, Text: &f12Text{found: true, text: live.StoryText{
			Brief: "generated from live sources", Available: true, Provenance: live.BriefGenerated,
		}}},
		log: f12Logger(),
	}
	detail, ok := s.currentStory(ctx, storyID)
	if !ok {
		t.Fatal("currentStory did not find the live story")
	}
	if detail.Story.Brief != "generated from live sources" || !detail.Story.BriefAvailable || detail.Story.BriefSource != live.BriefGenerated {
		t.Fatalf("brief = (%q,%v,%q)", detail.Story.Brief, detail.Story.BriefAvailable, detail.Story.BriefSource)
	}
	if detail.Story.TitleSource != "editor" || detail.Story.CategoryBackend != "rule" || !detail.Story.PlaceDegraded {
		t.Fatalf("story provenance = %+v", detail.Story)
	}
	if len(detail.Articles) != 1 || detail.Articles[0].ArticleID != "article-f12-live" {
		t.Fatalf("articles = %+v", detail.Articles)
	}
	if len(detail.Story.Locations) != 1 || detail.Story.Locations[0].Country != "US" {
		t.Fatalf("locations = %+v", detail.Story.Locations)
	}
}
