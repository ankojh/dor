package dorv1

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

var updateGolden = flag.Bool("update", false, "rewrite proto/testdata golden binaries")

const goldenDir = "../../../proto/testdata"

type goldenCase struct {
	name string
	msg  proto.Message
}

func locationCountry() *Location {
	return &Location{
		PlaceId:   "place-fr",
		Name:      "France",
		Lat:       46.5,
		Lon:       2.25,
		Precision: Precision_PRECISION_COUNTRY,
		Role:      Role_ROLE_PARTY,
		Weight:    0.75,
	}
}

func locationCity() *Location {
	return &Location{
		PlaceId:   "place-paris",
		Name:      "Paris",
		Lat:       48.5,
		Lon:       2.25,
		Precision: Precision_PRECISION_CITY,
		Role:      Role_ROLE_VENUE,
		Weight:    1,
	}
}

func goldenStory() *Story {
	return &Story{
		Id:          "story-1",
		Title:       "Flooding in Paris",
		Category:    Category_CATEGORY_CLIMATE,
		Locations:   []*Location{locationCountry(), locationCity()},
		Primary:     1,
		SourceCount: 3,
		Heat:        0.875,
		FirstSeen:   1_700_000_000,
		UpdatedAt:   1_700_000_123,
	}
}

func goldenLink() *Link {
	return &Link{
		A:          "story-1",
		B:          "story-2",
		Type:       LinkType_LINK_TYPE_REACTION,
		Dir:        Direction_DIRECTION_B_TO_A,
		Confidence: 0.875,
		Strength:   0.625,
		UpdatedAt:  1_700_000_200,
	}
}

func goldenLinkKey() *LinkKey {
	return &LinkKey{A: "story-1", B: "story-2"}
}

func goldenAlertHit() *AlertHit {
	return &AlertHit{
		AlertId: "alert-1",
		StoryId: "story-1",
		Level:   "escalated",
		FiredAt: 1_700_000_300,
		Title:   "Flooding in Paris",
		Prompt:  "Tell me about floods",
	}
}

func goldenWireRow() *WireRow {
	return &WireRow{
		ArticleId: "art-1",
		StoryId:   "story-1",
		Title:     "Flooding «Paris»",
		Source:    "BBC",
		Place:     "Paris, France",
		Category:  Category_CATEGORY_ECONOMY,
		At:        1_700_000_400,
		Join:      JoinKind_JOIN_KIND_JOINED,
		PromptScores: map[string]float32{
			"prompt-a": 0.5,
			"prompt-b": 0.25,
		},
	}
}

func goldenDelta() *Delta {
	return &Delta{
		Seq: 42,
		Body: &Delta_StoryUpsert{
			StoryUpsert: goldenStory(),
		},
	}
}

func goldenArticle() *Article {
	return &Article{
		Id:          "art-1",
		SourceId:    "src-bbc",
		Url:         "https://example.com/a?utm_source=test",
		Title:       "Flooding «Paris»",
		Lede:        "Lede with emoji 🌍 and ünïcødé.",
		PublishedAt: 1_700_000_000,
		Lang:        "en",
		Dateline:    "PARIS, Nov 20",
	}
}

func goldenSnapshot() *Snapshot {
	return &Snapshot{
		Seq:     42,
		Stories: []*Story{goldenStory()},
		Links:   []*Link{goldenLink()},
		BuiltAt: 1_700_000_400,
	}
}

func goldenDeltaBatch() *DeltaBatch {
	return &DeltaBatch{Deltas: []*Delta{
		goldenDelta(),
		{Seq: 43, Body: &Delta_Wire{Wire: goldenWireRow()}},
	}}
}

func allGoldenCases() []goldenCase {
	hello := &Hello{LastSeq: 42, ClientVersion: "web-0.1.0-test"}
	filters := &SetFilters{
		Categories: []Category{Category_CATEGORY_CLIMATE, Category_CATEGORY_ECONOMY},
		Regions:    []string{"eu-west", "fr"},
		MinSources: 2,
		Lang:       []string{"en", "fr"},
	}
	prompts := &SetPrompts{Prompts: []string{"floods", "energy"}}
	focus := &Focus{StoryId: "story-1"}
	getText := &GetText{StoryIds: []string{"story-1", "story-2"}, LinkKeys: []*LinkKey{goldenLinkKey()}}
	ack := &Ack{Seq: 42}

	serverSnapshot := &ServerMessage{Body: &ServerMessage_Snapshot{Snapshot: goldenSnapshot()}}
	serverDeltas := &ServerMessage{Body: &ServerMessage_Deltas{Deltas: goldenDeltaBatch()}}
	serverResync := &ServerMessage{Body: &ServerMessage_Resync{Resync: &Resync{Seq: 43, Reason: "client behind"}}}

	clientHello := &ClientMessage{Body: &ClientMessage_Hello{Hello: hello}}
	clientFilters := &ClientMessage{Body: &ClientMessage_SetFilters{SetFilters: filters}}
	clientPrompts := &ClientMessage{Body: &ClientMessage_SetPrompts{SetPrompts: prompts}}
	clientFocus := &ClientMessage{Body: &ClientMessage_Focus{Focus: focus}}
	clientGetText := &ClientMessage{Body: &ClientMessage_GetText{GetText: getText}}
	clientAck := &ClientMessage{Body: &ClientMessage_Ack{Ack: ack}}

	return []goldenCase{
		{"article", goldenArticle()},
		{"story", goldenStory()},
		{"link", goldenLink()},
		{"link_key", goldenLinkKey()},
		{"alert_hit", goldenAlertHit()},
		{"wire_row", goldenWireRow()},
		{"delta_story", goldenDelta()},
		{"delta_story_expire", &Delta{Seq: 44, Body: &Delta_StoryExpire{StoryExpire: "story-9"}}},
		{"delta_link", &Delta{Seq: 45, Body: &Delta_LinkUpsert{LinkUpsert: goldenLink()}}},
		{"delta_link_remove", &Delta{Seq: 46, Body: &Delta_LinkRemove{LinkRemove: goldenLinkKey()}}},
		{"delta_wire", &Delta{Seq: 47, Body: &Delta_Wire{Wire: goldenWireRow()}}},
		{"delta_alert", &Delta{Seq: 48, Body: &Delta_Alert{Alert: goldenAlertHit()}}},
		{"snapshot", goldenSnapshot()},
		{"delta_batch", goldenDeltaBatch()},
		{"resync", &Resync{Seq: 43, Reason: "client behind"}},
		{"server_snapshot", serverSnapshot},
		{"server_deltas", serverDeltas},
		{"server_resync", serverResync},
		{"hello", hello},
		{"set_filters", filters},
		{"set_prompts", prompts},
		{"focus", focus},
		{"get_text", getText},
		{"ack", ack},
		{"client_hello", clientHello},
		{"client_set_filters", clientFilters},
		{"client_set_prompts", clientPrompts},
		{"client_focus", clientFocus},
		{"client_get_text", clientGetText},
		{"client_ack", clientAck},
	}
}

// TestWriteGolden writes one deterministic binary per message type. It only
// runs with -update (or make proto-golden); normal tests compare the checked-in
// files instead of rewriting them. The web Vitest suite reads these exact
// files and checks every field, so a wire incompatibility fails on both sides.
func TestWriteGolden(t *testing.T) {
	if !*updateGolden {
		t.Skip("golden update disabled; run go test ./internal/pb/dorv1 -update or make proto-golden")
	}
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		t.Fatalf("mkdir golden dir: %v", err)
	}
	opts := proto.MarshalOptions{Deterministic: true}
	for _, tc := range allGoldenCases() {
		data, err := opts.Marshal(tc.msg)
		if err != nil {
			t.Fatalf("marshal %s: %v", tc.name, err)
		}
		path := filepath.Join(goldenDir, tc.name+".bin")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
}

// TestGoldenRoundTrip makes sure every written file decodes back to the exact
// message that produced it.
func TestGoldenRoundTrip(t *testing.T) {
	for _, tc := range allGoldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(goldenDir, tc.name+".bin")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			got := tc.msg.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(data, got); err != nil {
				t.Fatalf("unmarshal golden: %v", err)
			}
			if !proto.Equal(tc.msg, got) {
				t.Fatalf("golden %s does not round-trip", tc.name)
			}
		})
	}
}
