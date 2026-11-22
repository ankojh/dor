package store

import (
	"time"
)

// ArticleRecord is one normalized article for the durable store. It contains no
// full body: title and lede are the only text.
type ArticleRecord struct {
	ID                 string
	SourceID           string
	URL                string
	Title              string
	Lede               string
	PublishedAt        time.Time
	CreatedAt          time.Time
	Lang               string
	Dateline           string
	Category           int16
	CategoryConfidence float32
	StoryID            string
	Locations          []LocationRecord
}

// LocationRecord is one article or story location vote.
type LocationRecord struct {
	PlaceID    string
	Name       string
	Lat        float64
	Lon        float64
	Precision  int16
	Role       int16
	Confidence float32
	Weight     float32
	// Country is the ISO 3166-1 alpha-2 code of the resolved place when known.
	Country string
}

// SourceRecord is one reviewed source row.
type SourceRecord struct {
	ID        string
	Name      string
	Kind      string
	URL       string
	Country   string
	Region    string
	Lang      string
	IntervalS int
	Enabled   bool
	TermsOK   bool
	TermsNote string
	Hazard    string
	Notes     string
	Health    []byte // JSON object; empty becomes {}
}

// StoryRecord is one clusterer story update for the durable store. Centroid is
// the 384-d article centroid; the writer formats it for pgvector.
//
// Brief is deliberately empty from the clusterer: only the Editor may write an
// agent brief, and its provenance is carried by BriefSource. TitleSource is
// "source" from the clusterer and "editor" once the neutral title is applied.
type StoryRecord struct {
	ID                 string
	Title              string
	TitleSource        string
	Brief              string
	BriefSource        string
	Category           int16
	CategoryBackend    string
	CategoryConfidence float32
	PrimaryLoc         string
	SourceCount        int
	Heat               float64
	Degraded           bool
	Fallback           bool
	PlaceBackend       string
	PlaceDegraded      bool
	PlaceFallback      bool
	Centroid           []float32
	Entities           []string
	FirstSeen          time.Time
	UpdatedAt          time.Time
	ExpiresAt          time.Time
	Locations          []LocationRecord
	ArticleIDs         []string
}

// ArticleEnrichmentRecord is the enrichment a joined article contributes to
// the durable article row. It never writes story_id: the clusterer's story
// record owns assignment. The writer applies it separately and retries while
// the article row is still in the insert queue.
type ArticleEnrichmentRecord struct {
	ID                 string
	Category           int16
	CategoryConfidence float32
	Lang               string
	Locations          []LocationRecord
}

// LinkRecord is one durable link upsert or removal. A record with Remove=true
// deletes the pair; a zero-strength tombstone is represented by Remove, not by
// storing a fake link in the durable table.
type LinkRecord struct {
	A          string
	B          string
	Type       int16
	Direction  int16
	Confidence float32
	Strength   float32
	Unverified bool
	UpdatedAt  time.Time
	DecaysAt   time.Time
	Remove     bool
}

// StoryTextRecord is the durable mirror of the storytext KV cache. An empty
// brief leaves the existing editor brief untouched. A non-empty Title with
// TitleSource "editor" applies the Editor's neutral title. BriefSource is
// "generated", "source_snippet" or "none".
type StoryTextRecord struct {
	ID          string
	Title       string
	TitleSource string
	Brief       string
	BriefSource string
}

// LinkTextRecord is the durable mirror of the linktext KV cache.
type LinkTextRecord struct {
	A   string
	B   string
	Why string
}
