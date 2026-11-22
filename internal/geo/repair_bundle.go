package geo

// LocationRepairBundle is a DB-location plan, not permission to mutate hot
// state. The maintenance planner must bind every relevant current KV revision
// and its own hot-state before hash under a verified quiesce/fence barrier.
// DB hashes cannot stand in for actual KV/root revision fences.
type LocationRepairBundle struct {
	DatasetSHA256   string                 `json:"dataset_sha256"`
	Version         int                    `json:"version"`
	RunID           string                 `json:"run_id"`
	RevisionBinding string                 `json:"revision_binding"`
	Stories         []StoryLocationPatch   `json:"stories"`
	Articles        []ArticleLocationPatch `json:"articles"`
}
type LocationVote struct {
	PlaceID    string  `json:"place_id"`
	Name       string  `json:"name"`
	Country    string  `json:"country"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	Precision  int16   `json:"precision"`
	Role       int16   `json:"role"`
	Weight     float32 `json:"weight"`
	Confidence float32 `json:"conf"`
}
type StoryLocationPatch struct {
	ID                   string         `json:"id"`
	DBBeforeHash         string         `json:"db_before_hash"`
	BeforePrimaryPlaceID string         `json:"before_primary_place_id"`
	Locations            []LocationVote `json:"locations"`
	Primary              uint32         `json:"primary"`
	PrimaryPlaceID       string         `json:"primary_place_id"`
	Unlocated            bool           `json:"unlocated"`
}
type EffectiveWireLocation struct {
	Place   string  `json:"place"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Located bool    `json:"located"`
}
type ArticleLocationPatch struct {
	ID               string                `json:"id"`
	StoryID          string                `json:"expected_story_id"`
	DBBeforeHash     string                `json:"db_before_hash"`
	Locations        []LocationVote        `json:"locations"`
	Unlocated        bool                  `json:"unlocated"`
	EventCandidate   EffectiveWireLocation `json:"event_candidate"`
	NeedsHotEvidence bool                  `json:"needs_hot_evidence"`
}
