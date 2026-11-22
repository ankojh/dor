package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/store"
)

// StoryStore is the durable history fallback used by the panel API. Current
// stories still come from live_state first. *store.Store satisfies it.
type StoryStore interface {
	GetStoryDetail(ctx context.Context, id string, limit int) (store.StoryDetail, error)
	GetArticlesByIDs(ctx context.Context, ids []string) ([]store.StoryArticle, error)
	GetSources(ctx context.Context, ids []string) ([]store.SourceInfo, error)
	SearchStories(ctx context.Context, query string, limit int) ([]store.StorySummary, error)
	ArticleHistogram(ctx context.Context, start, end time.Time, bins int) (store.ArticleHistogram, error)
}

const storyArticleLimit = 20

type storyDetailJSON struct {
	Story             storyJSON     `json:"story"`
	Articles          []articleJSON `json:"articles"`
	Sources           []sourceJSON  `json:"sources"`
	ArticlesTruncated bool          `json:"articles_truncated"`
}

type storyJSON struct {
	ID                 string         `json:"id"`
	Title              string         `json:"title"`
	TitleSource        string         `json:"title_source"`
	Brief              string         `json:"brief"`
	BriefAvailable     bool           `json:"brief_available"`
	BriefSource        string         `json:"brief_source"`
	Category           string         `json:"category"`
	CategoryBackend    string         `json:"category_backend"`
	CategoryConfidence float32        `json:"category_confidence"`
	Degraded           bool           `json:"degraded"`
	Fallback           bool           `json:"fallback"`
	PlaceBackend       string         `json:"place_backend"`
	PlaceDegraded      bool           `json:"place_degraded"`
	PlaceFallback      bool           `json:"place_fallback"`
	SourceCount        int            `json:"source_count"`
	Heat               float64        `json:"heat"`
	FirstSeen          int64          `json:"first_seen"`
	UpdatedAt          int64          `json:"updated_at"`
	Primary            uint32         `json:"primary"`
	Locations          []locationJSON `json:"locations"`
}

type locationJSON struct {
	PlaceID   string  `json:"place_id"`
	Name      string  `json:"name"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Precision string  `json:"precision"`
	Role      string  `json:"role"`
	Weight    float32 `json:"weight"`
	Country   string  `json:"country"`
}

type articleJSON struct {
	ArticleID      string `json:"article_id"`
	SourceID       string `json:"source_id"`
	SourceName     string `json:"source_name"`
	URL            string `json:"url"`
	Title          string `json:"title"`
	PublishedAt    int64  `json:"published_at"`
	PublishedAtISO string `json:"published_at_iso"`
	Lang           string `json:"lang"`
}

type sourceJSON struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	ArticleCount int    `json:"article_count"`
	Health       string `json:"health"`
}

type storyListJSON struct {
	Stories          []storySummaryJSON `json:"stories"`
	Count            int                `json:"count"`
	HistoryAvailable bool               `json:"history_available"`
}

type storySummaryJSON struct {
	ID           string  `json:"id"`
	Title        string  `json:"title"`
	Category     string  `json:"category"`
	SourceCount  int     `json:"source_count"`
	Heat         float64 `json:"heat"`
	FirstSeen    int64   `json:"first_seen"`
	UpdatedAt    int64   `json:"updated_at"`
	PrimaryPlace string  `json:"primary_place"`
}

func (s *Server) handleStoryDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" || len(id) > 128 || strings.ContainsAny(id, "/ \t\n") {
		writeAPI(w, http.StatusBadRequest, map[string]any{"error": "invalid story id"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if detail, ok := s.currentStory(ctx, id); ok {
		writeAPI(w, http.StatusOK, detail)
		return
	}
	if s.cfg.Store != nil {
		d, err := s.cfg.Store.GetStoryDetail(ctx, id, storyArticleLimit)
		if err == nil {
			writeAPI(w, http.StatusOK, s.detailFromStore(ctx, d))
			return
		}
		if !errors.Is(err, store.ErrStoryNotFound) {
			s.log.Warn("gateway: story detail store fallback", "story", id, "err", err)
		}
	}
	writeAPI(w, http.StatusNotFound, map[string]any{"error": "unknown story", "id": id})
}

func (s *Server) handleStories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 50 {
			limit = n
		}
	}
	out := storyListJSON{Stories: []storySummaryJSON{}, HistoryAvailable: s.cfg.Store != nil}
	if s.cfg.Store == nil {
		writeAPI(w, http.StatusOK, out)
		return
	}
	rows, err := s.cfg.Store.SearchStories(ctx, r.URL.Query().Get("query"), limit)
	if err != nil {
		s.log.Warn("gateway: story search", "err", err)
		writeAPI(w, http.StatusOK, out)
		return
	}
	for _, row := range rows {
		out.Stories = append(out.Stories, storySummaryJSON{
			ID: row.ID, Title: row.Title, Category: categoryName(row.Category),
			SourceCount: row.SourceCount, Heat: row.Heat,
			FirstSeen: row.FirstSeen.Unix(), UpdatedAt: row.UpdatedAt.Unix(),
			PrimaryPlace: row.PrimaryPlace,
		})
	}
	out.Count = len(out.Stories)
	writeAPI(w, http.StatusOK, out)
}

func (s *Server) currentStory(ctx context.Context, id string) (storyDetailJSON, bool) {
	kv, err := s.cfg.Bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return storyDetailJSON{}, false
	}
	story, ok := live.LoadStory(ctx, kv, id)
	if !ok || story == nil {
		return storyDetailJSON{}, false
	}
	meta, _ := live.LoadStoryMeta(ctx, kv, id)
	articles := s.kvArticles(ctx, kv, meta.ArticleIDs)
	truncated := len(meta.ArticleIDs) > storyArticleLimit
	if len(articles) > storyArticleLimit {
		articles = articles[:storyArticleLimit]
		truncated = true
	}
	byID := make(map[string]store.StoryArticle, len(articles))
	for _, a := range articles {
		byID[a.ID] = a
	}
	// A live story can reference an article that has already aged out of KV.
	// Fill only the missing rows from the durable store.
	if s.cfg.Store != nil && len(articles) < min(len(meta.ArticleIDs), storyArticleLimit) {
		missing := make([]string, 0)
		for _, aid := range meta.ArticleIDs {
			if _, ok := byID[aid]; !ok {
				missing = append(missing, aid)
			}
			if len(missing) >= storyArticleLimit {
				break
			}
		}
		if len(missing) > 0 {
			extra, _ := s.cfg.Store.GetArticlesByIDs(ctx, missing)
			articles = append(articles, extra...)
			sort.SliceStable(articles, func(i, j int) bool { return articles[i].PublishedAt.After(articles[j].PublishedAt) })
		}
	}
	brief, briefOK, briefSource := "", false, live.BriefNone
	if s.cfg.Text != nil {
		if t, ok := s.cfg.Text.Story(ctx, id); ok {
			brief, briefOK = t.Brief, t.Available
			briefSource = t.Provenance
			if briefSource == "" {
				briefSource = live.BriefNone
			}
		}
	}
	return storyDetailJSON{
		Story:             storyFromProto(story, brief, briefOK, briefSource),
		Articles:          articlesJSON(articles, s.sourcesFor(ctx, articles)),
		Sources:           sourcesJSON(s.sourcesFor(ctx, articles)),
		ArticlesTruncated: truncated,
	}, true
}

func (s *Server) detailFromStore(ctx context.Context, d store.StoryDetail) storyDetailJSON {
	brief := d.Brief
	briefOK := strings.TrimSpace(brief) != ""
	briefSource := d.BriefSource
	if briefSource == "" {
		briefSource = live.BriefNone
		if briefOK {
			briefSource = live.BriefSourceSnippet
		}
	}
	if s.cfg.Text != nil {
		if t, ok := s.cfg.Text.Story(ctx, d.ID); ok && t.Available {
			brief, briefOK = t.Brief, true
			briefSource = t.Provenance
			if briefSource == "" {
				briefSource = live.BriefGenerated
			}
		}
	}
	if !briefOK && briefSource == live.BriefGenerated {
		briefSource = live.BriefNone
	}
	return storyDetailJSON{
		Story:             storyFromStore(d, brief, briefOK, briefSource),
		Articles:          articlesJSON(d.Articles, d.Sources),
		Sources:           sourcesJSON(d.Sources),
		ArticlesTruncated: d.ArticlesTruncated,
	}
}

func (s *Server) kvArticles(ctx context.Context, kv jetstream.KeyValue, ids []string) []store.StoryArticle {
	out := make([]store.StoryArticle, 0, min(len(ids), storyArticleLimit))
	for _, id := range ids {
		if len(out) >= storyArticleLimit {
			break
		}
		a, ok := live.LoadArticle(ctx, kv, id)
		if !ok || a == nil {
			continue
		}
		out = append(out, store.StoryArticle{
			ID: a.Id, SourceID: a.SourceId, Title: a.Title, URL: a.Url,
			PublishedAt: time.Unix(a.PublishedAt, 0).UTC(), Lang: a.Lang,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].PublishedAt.After(out[j].PublishedAt) })
	return out
}

func storyFromProto(story *dorv1.Story, brief string, briefOK bool, briefSource string) storyJSON {
	locations := make([]locationJSON, 0, len(story.Locations))
	for _, l := range story.Locations {
		if l == nil {
			continue
		}
		locations = append(locations, locationJSON{
			PlaceID: l.PlaceId, Name: l.Name, Lat: float64(l.Lat), Lon: float64(l.Lon),
			Precision: precisionName(l.Precision), Role: roleName(l.Role), Weight: l.Weight,
			Country: l.Country,
		})
	}
	primary := story.Primary
	deduped := dedupeLocationJSON(locations)
	if int(primary) < len(locations) {
		key := locationKey(locations[primary])
		for index, location := range deduped {
			if locationKey(location) == key {
				primary = uint32(index)
				break
			}
		}
	}
	return storyJSON{
		ID: story.Id, Title: story.Title, TitleSource: story.GetTitleSource(),
		Brief: brief, BriefAvailable: briefOK, BriefSource: briefSource,
		Category: categoryName(int16(story.Category)), CategoryBackend: story.GetCategoryBackend(),
		CategoryConfidence: story.GetCategoryConfidence(), Degraded: story.GetDegraded(),
		Fallback: story.GetFallback(), PlaceBackend: story.GetPlaceBackend(),
		PlaceDegraded: story.GetPlaceDegraded(), PlaceFallback: story.GetPlaceFallback(),
		SourceCount: int(story.SourceCount),
		Heat:        float64(story.Heat), FirstSeen: story.FirstSeen, UpdatedAt: story.UpdatedAt,
		Primary: primary, Locations: deduped,
	}
}

func storyFromStore(d store.StoryDetail, brief string, briefOK bool, briefSource string) storyJSON {
	locations := make([]locationJSON, 0, len(d.Locations))
	for _, l := range d.Locations {
		locations = append(locations, locationJSON{
			PlaceID: l.PlaceID, Name: l.Name, Lat: l.Lat, Lon: l.Lon,
			Precision: precisionName(dorv1.Precision(l.Precision)),
			Role:      roleName(dorv1.Role(l.Role)), Weight: l.Weight,
			Country: l.Country,
		})
	}
	primary := d.Primary
	deduped := dedupeLocationJSON(locations)
	if int(primary) < len(locations) {
		key := locationKey(locations[primary])
		for index, location := range deduped {
			if locationKey(location) == key {
				primary = uint32(index)
				break
			}
		}
	}
	return storyJSON{
		ID: d.ID, Title: d.Title, TitleSource: d.TitleSource,
		Brief: brief, BriefAvailable: briefOK, BriefSource: briefSource,
		Category: categoryName(d.Category), CategoryBackend: d.CategoryBackend,
		CategoryConfidence: d.CategoryConfidence, Degraded: d.Degraded, Fallback: d.Fallback,
		PlaceBackend: d.PlaceBackend, PlaceDegraded: d.PlaceDegraded, PlaceFallback: d.PlaceFallback,
		SourceCount: d.SourceCount, Heat: d.Heat,
		FirstSeen: d.FirstSeen.Unix(), UpdatedAt: d.UpdatedAt.Unix(),
		Primary: primary, Locations: deduped,
	}
}

// storyLocationLimit bounds one panel's place list. The first entry is the
// story's primary place and is never moved or dropped.
const storyLocationLimit = 12

// dedupeLocationJSON collapses repeated place rows from different source
// articles (the live review saw "Berlin, Berlin, Germany" three times) while
// keeping the most decisive role and the most precise coordinate for a place.
// Order is preserved so the primary stays first.
func dedupeLocationJSON(in []locationJSON) []locationJSON {
	if len(in) <= 1 {
		return in
	}
	out := make([]locationJSON, 0, len(in))
	seen := make(map[string]int, len(in))
	for _, location := range in {
		key := locationKey(location)
		if at, ok := seen[key]; ok {
			previous := &out[at]
			if rolePriority(location.Role) < rolePriority(previous.Role) {
				previous.Role = location.Role
			}
			if precisionPriority(location.Precision) > precisionPriority(previous.Precision) {
				previous.Precision = location.Precision
			}
			previous.Weight += location.Weight
			continue
		}
		seen[key] = len(out)
		out = append(out, location)
	}
	if len(out) > storyLocationLimit {
		out = out[:storyLocationLimit]
	}
	return out
}

func locationKey(location locationJSON) string {
	name := location.Name
	for i, r := range name {
		if r == ',' {
			name = name[:i]
			break
		}
	}
	name = strings.ToLower(strings.TrimSpace(name))
	// Coordinates are rounded to ~1 km so a city centroid from the gazetteer
	// and the same city from Photon collapse without moving the dot.
	return name + "|" +
		strconv.FormatFloat(location.Lat, 'f', 2, 64) + "," +
		strconv.FormatFloat(location.Lon, 'f', 2, 64)
}

func rolePriority(role string) int {
	switch role {
	case "venue":
		return 0
	case "origin":
		return 1
	case "party":
		return 2
	case "affected":
		return 3
	default:
		return 4
	}
}

func precisionPriority(precision string) int {
	switch precision {
	case "point":
		return 4
	case "street":
		return 3
	case "city":
		return 2
	case "region":
		return 1
	default:
		return 0
	}
}

func (s *Server) sourcesFor(ctx context.Context, articles []store.StoryArticle) []store.SourceInfo {
	ids := make([]string, 0, len(articles))
	seen := make(map[string]bool)
	for _, a := range articles {
		if a.SourceID != "" && !seen[a.SourceID] {
			seen[a.SourceID] = true
			ids = append(ids, a.SourceID)
		}
	}
	byID := make(map[string]store.SourceInfo, len(ids))
	if s.cfg.Store != nil && len(ids) > 0 {
		if infos, err := s.cfg.Store.GetSources(ctx, ids); err == nil {
			for _, info := range infos {
				byID[info.ID] = info
			}
		}
	}
	out := make([]store.SourceInfo, 0, len(ids))
	for _, id := range ids {
		info := byID[id]
		info.ID = id
		if info.Name == "" {
			info.Name = displaySourceName(id)
		}
		if info.Health == "" {
			info.Health = "unknown"
		}
		for _, a := range articles {
			if a.SourceID == id {
				info.ArticleCount++
			}
		}
		out = append(out, info)
	}
	return out
}

func articlesJSON(articles []store.StoryArticle, sources []store.SourceInfo) []articleJSON {
	names := make(map[string]string, len(sources))
	for _, src := range sources {
		names[src.ID] = src.Name
	}
	out := make([]articleJSON, 0, len(articles))
	for _, a := range articles {
		name := names[a.SourceID]
		if name == "" {
			name = displaySourceName(a.SourceID)
		}
		published := int64(0)
		iso := ""
		if !a.PublishedAt.IsZero() {
			published = a.PublishedAt.Unix()
			iso = a.PublishedAt.UTC().Format(time.RFC3339)
		}
		out = append(out, articleJSON{
			ArticleID: a.ID, SourceID: a.SourceID, SourceName: name, URL: a.URL,
			Title: a.Title, PublishedAt: published, PublishedAtISO: iso, Lang: a.Lang,
		})
	}
	return out
}

func sourcesJSON(sources []store.SourceInfo) []sourceJSON {
	out := make([]sourceJSON, 0, len(sources))
	for _, src := range sources {
		name := src.Name
		if name == "" {
			name = displaySourceName(src.ID)
		}
		health := src.Health
		if health == "" {
			health = "unknown"
		}
		out = append(out, sourceJSON{
			ID: src.ID, Name: name, URL: src.URL,
			ArticleCount: src.ArticleCount, Health: health,
		})
	}
	return out
}

func writeAPI(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func categoryName(c int16) string {
	switch dorv1.Category(c) {
	case dorv1.Category_CATEGORY_CONFLICT:
		return "conflict"
	case dorv1.Category_CATEGORY_POLITICS:
		return "politics"
	case dorv1.Category_CATEGORY_ECONOMY:
		return "economy"
	case dorv1.Category_CATEGORY_TECH:
		return "tech"
	case dorv1.Category_CATEGORY_CLIMATE:
		return "climate"
	case dorv1.Category_CATEGORY_DISASTER:
		return "disaster"
	case dorv1.Category_CATEGORY_HEALTH:
		return "health"
	case dorv1.Category_CATEGORY_SCIENCE:
		return "science"
	case dorv1.Category_CATEGORY_SPORTS:
		return "sports"
	case dorv1.Category_CATEGORY_CULTURE:
		return "culture"
	default:
		return "other"
	}
}

func precisionName(p dorv1.Precision) string {
	return strings.ToLower(strings.TrimPrefix(p.String(), "PRECISION_"))
}

func roleName(r dorv1.Role) string {
	return strings.ToLower(strings.TrimPrefix(r.String(), "ROLE_"))
}

func displaySourceName(id string) string {
	words := strings.Fields(strings.ReplaceAll(strings.TrimSpace(id), "-", " "))
	for i, word := range words {
		if word == "" {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
