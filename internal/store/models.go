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
}

// SourceRecord is one reviewed source row.
type SourceRecord struct {
	ID        string
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
type StoryRecord struct {
	ID          string
	Title       string
	Brief       string
	Category    int16
	PrimaryLoc  string
	SourceCount int
	Heat        float64
	Centroid    []float32
	Entities    []string
	FirstSeen   time.Time
	UpdatedAt   time.Time
	ExpiresAt   time.Time
	Locations   []LocationRecord
	ArticleIDs  []string
}
