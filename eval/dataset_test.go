package eval

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLines(t *testing.T, dir, name string, rows ...any) {
	t.Helper()
	var b strings.Builder
	for _, r := range rows {
		if s, ok := r.(string); ok {
			b.WriteString(s + "\n")
			continue
		}
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func baseDataset(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeLines(t, dir, "articles.candidates.jsonl",
		Article{ID: "a1", Title: "One", Candidates: []Candidate{{ID: "country:FR", Name: "France"}}},
		Article{ID: "a2", Title: "Two"})
	writeLines(t, dir, "pairs.candidates.jsonl", Pair{ID: "p1", A: "a1", B: "a2"})
	return dir
}

func aLabel(id, labeler string, provisional bool) ArticleLabel {
	return ArticleLabel{ArticleID: id, Category: "politics", PrimaryPlace: "country:FR", Precision: "country", Labeler: labeler, LabeledAt: "2026-11-20T18:00:00Z", Provisional: provisional}
}

func pLabel(id, labeler string, provisional bool) PairLabel {
	return PairLabel{PairID: id, Related: true, Type: "causal", Direction: "a_to_b", Strength: 2, Labeler: labeler, LabeledAt: "2026-11-20T18:00:00Z", Provisional: provisional}
}

func hasWarning(ds *Dataset, sub string) bool {
	for _, w := range ds.Warnings {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

func TestLabelValidation(t *testing.T) {
	dir := baseDataset(t)
	writeLines(t, dir, "articles.labels.jsonl",
		aLabel("a1", "agent:model-x", true),  // provisional agent: ok
		aLabel("a2", "agent:model-x", false), // final label by an agent: rejected
	)
	writeLines(t, dir, "pairs.labels.jsonl", pLabel("p1", "agent:model-x", false))
	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ds.ArticleLabels["a1"]; !ok {
		t.Error("a provisional agent label must load")
	}
	if _, ok := ds.ArticleLabels["a2"]; ok {
		t.Error("a non-provisional label whose labeler is not human: must be rejected")
	}
	if _, ok := ds.PairLabels["p1"]; ok {
		t.Error("a non-provisional agent pair label must be rejected")
	}
	if !hasWarning(ds, "non-provisional") {
		t.Errorf("warnings = %v", ds.Warnings)
	}

	dir = baseDataset(t)
	writeLines(t, dir, "articles.labels.jsonl", aLabel("a1", "human:ankit", false))
	writeLines(t, dir, "pairs.labels.jsonl", pLabel("p1", "human:ankit", false))
	ds, err = LoadDataset(dir)
	if err != nil || len(ds.ArticleLabels) != 1 || len(ds.PairLabels) != 1 || len(ds.Warnings) != 0 {
		t.Errorf("human labels: err=%v labels=%d/%d warnings=%v", err, len(ds.ArticleLabels), len(ds.PairLabels), ds.Warnings)
	}
}

func TestLabelFieldValidation(t *testing.T) {
	dir := baseDataset(t)
	missing := aLabel("a1", "human:x", false)
	missing.Category = ""
	noPlace := aLabel("a2", "human:x", false)
	noPlace.PrimaryPlace = ""
	writeLines(t, dir, "articles.labels.jsonl", missing, noPlace, ArticleLabel{}, aLabel("a1", "", true))
	badStrength := pLabel("p1", "human:x", false)
	badStrength.Strength = 5
	noType := pLabel("p2", "human:x", false)
	noType.Type = ""
	writeLines(t, dir, "pairs.labels.jsonl", badStrength, noType, PairLabel{})
	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.ArticleLabels) != 0 || len(ds.PairLabels) != 0 {
		t.Errorf("invalid labels loaded: %d/%d", len(ds.ArticleLabels), len(ds.PairLabels))
	}
	for _, want := range []string{"missing category", "missing primary_place", "no article_id", "missing labeler", "outside 0..3", "missing type", "no pair_id"} {
		if !hasWarning(ds, want) {
			t.Errorf("no warning containing %q in %v", want, ds.Warnings)
		}
	}
}

func TestHumanLabelsOverrideSeed(t *testing.T) {
	dir := baseDataset(t)
	seed := aLabel("a1", "agent:model-x", true)
	seed.Category = "tech"
	writeLines(t, dir, "articles.labels.jsonl", seed, aLabel("a2", "agent:model-x", true))
	human := aLabel("a1", "human:ankit", false)
	human.Category = "economy"
	writeLines(t, dir, "articles.labels.human.jsonl", human)
	seedPair := pLabel("p1", "agent:model-x", true)
	seedPair.Type = "thematic"
	writeLines(t, dir, "pairs.labels.jsonl", seedPair)
	humanPair := pLabel("p1", "human:ankit", false)
	humanPair.Type = "reaction"
	writeLines(t, dir, "pairs.labels.human.jsonl", humanPair)

	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := ds.ArticleLabels["a1"]; got.Category != "economy" || got.Labeler != "human:ankit" || got.Provisional {
		t.Errorf("human label did not override: %+v", got)
	}
	if got := ds.ArticleLabels["a2"]; got.Labeler != "agent:model-x" {
		t.Errorf("a2 keeps its seed label: %+v", got)
	}
	if got := ds.PairLabels["p1"]; got.Type != "reaction" || got.Labeler != "human:ankit" {
		t.Errorf("pair override: %+v", got)
	}
	// Sources are summarised after overrides, sorted by labeler.
	if len(ds.Sources) != 2 || ds.Sources[0].Labeler != "agent:model-x" || ds.Sources[0].Articles != 1 || ds.Sources[0].Pairs != 0 ||
		ds.Sources[1].Labeler != "human:ankit" || ds.Sources[1].Articles != 1 || ds.Sources[1].Pairs != 1 || ds.Sources[1].Provisional {
		t.Errorf("sources = %+v", ds.Sources)
	}
	if s := LabelSummary(ds.Sources); !strings.Contains(s, "agent:model-x, provisional (articles 1, pairs 0)") || !strings.Contains(s, "human:ankit (articles 1, pairs 1)") {
		t.Errorf("summary = %q", s)
	}
	if LabelSummary(nil) != "no labels" {
		t.Error("empty summary")
	}
}

func TestOrphanLabelsAreWarnings(t *testing.T) {
	dir := baseDataset(t)
	writeLines(t, dir, "articles.labels.jsonl", aLabel("a1", "human:x", false), aLabel("ghost", "human:x", false))
	writeLines(t, dir, "pairs.labels.jsonl", pLabel("p-ghost", "human:x", false))
	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatalf("orphans must not be fatal: %v", err)
	}
	if !hasWarning(ds, "article label for unknown id ghost") || !hasWarning(ds, "pair label for unknown id p-ghost") {
		t.Errorf("warnings = %v", ds.Warnings)
	}
}

func TestLoadDatasetErrors(t *testing.T) {
	if _, err := LoadDataset(t.TempDir()); err == nil {
		t.Error("an empty directory must fail")
	}
	dir := t.TempDir()
	writeLines(t, dir, "articles.candidates.jsonl")
	writeLines(t, dir, "pairs.candidates.jsonl", Pair{ID: "p"})
	if _, err := LoadDataset(dir); err == nil || !strings.Contains(err.Error(), "no articles") {
		t.Errorf("no articles: %v", err)
	}
	dir = t.TempDir()
	writeLines(t, dir, "articles.candidates.jsonl", Article{ID: "a"})
	writeLines(t, dir, "pairs.candidates.jsonl")
	if _, err := LoadDataset(dir); err == nil || !strings.Contains(err.Error(), "no pairs") {
		t.Errorf("no pairs: %v", err)
	}
	dir = baseDataset(t)
	writeLines(t, dir, "articles.labels.jsonl", "{not json")
	if _, err := LoadDataset(dir); err == nil || !strings.Contains(err.Error(), "articles.labels.jsonl:1") {
		t.Errorf("malformed JSONL must name file and line: %v", err)
	}
}

func TestLoadDatasetOptionalFilesAndBlankLines(t *testing.T) {
	dir := baseDataset(t)
	writeLines(t, dir, "articles.candidates.jsonl", "", `{"id":"a1","title":"One"}`, "   ", `{"id":"a2","title":"Two"}`)
	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.Articles) != 2 || len(ds.Matches) != 0 || len(ds.ArticleLabels) != 0 {
		t.Errorf("articles=%d matches=%d labels=%d", len(ds.Articles), len(ds.Matches), len(ds.ArticleLabels))
	}
	writeLines(t, dir, "match.candidates.jsonl", MatchCandidate{ID: "m1", ArticleID: "a1", Prompt: "p", Relevant: true, Derived: true})
	ds, _ = LoadDataset(dir)
	if len(ds.Matches) != 1 || !ds.Matches[0].Derived {
		t.Errorf("matches = %+v", ds.Matches)
	}
}

func TestWriteAndReadJSONLRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "x.jsonl")
	in := []Article{{ID: "a", Title: "T", Candidates: []Candidate{{ID: "c", Name: "N", Population: 5}}}, {ID: "b"}}
	if err := WriteJSONL(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadArticles(path)
	if err != nil || len(out) != 2 || out[0].Candidates[0].Population != 5 {
		t.Errorf("out=%+v err=%v", out, err)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Error("temp file left behind")
	}
	pairsPath := filepath.Join(t.TempDir(), "p.jsonl")
	if err := WriteJSONL(pairsPath, []Pair{{ID: "p", A: "a", B: "b", Shared: []string{"x"}}}); err != nil {
		t.Fatal(err)
	}
	if ps, err := ReadPairs(pairsPath); err != nil || len(ps) != 1 || ps[0].Shared[0] != "x" {
		t.Errorf("pairs = %+v %v", ps, err)
	}
	if _, err := ReadArticles(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("missing file")
	}
}

func TestCommittedDatasetLoads(t *testing.T) {
	ds, err := LoadDataset("data")
	if err != nil {
		t.Fatalf("the committed eval data must load: %v", err)
	}
	if len(ds.Articles) == 0 || len(ds.Pairs) == 0 || len(ds.Matches) == 0 {
		t.Errorf("articles=%d pairs=%d matches=%d", len(ds.Articles), len(ds.Pairs), len(ds.Matches))
	}
	if len(ds.ArticleLabels) == 0 || len(ds.PairLabels) == 0 {
		t.Errorf("labels = %d/%d", len(ds.ArticleLabels), len(ds.PairLabels))
	}
	if len(ds.Warnings) != 0 {
		t.Errorf("committed data has warnings: %v", ds.Warnings)
	}
	// Every seeded label is provisional agent work; none may claim to be human.
	for id, l := range ds.ArticleLabels {
		if !l.Provisional && !strings.HasPrefix(l.Labeler, "human:") {
			t.Errorf("article label %s breaks the labeler rule: %+v", id, l)
		}
	}
	dups := 0
	for _, l := range ds.PairLabels {
		if l.Duplicate {
			dups++
		}
	}
	if dups == 0 {
		t.Error("the seeded pair labels include duplicate pairs; the relation metrics rely on skipping them")
	}
	// Candidates reference known articles.
	byID := articleByID(ds.Articles)
	for _, p := range ds.Pairs {
		if _, ok := byID[p.A]; !ok {
			t.Errorf("pair %s references unknown article %s", p.ID, p.A)
		}
	}
	for _, m := range ds.Matches {
		if _, ok := byID[m.ArticleID]; !ok {
			t.Errorf("match %s references unknown article %s", m.ID, m.ArticleID)
		}
	}
}
