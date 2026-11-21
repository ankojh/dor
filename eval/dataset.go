// Package eval runs Dor's model evaluation: jev against Claude on labelled
// classify, plot, relate and match sets.
//
// The package is deliberately split from the command so the metrics and
// threshold selection are unit-testable with synthetic distributions, and so a
// report is reproducible from the committed JSONL files and config alone.
package eval

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Article is one candidate article for classify and plot.
type Article struct {
	ID          string      `json:"id"`
	Title       string      `json:"title"`
	Snippet     string      `json:"snippet"`
	URL         string      `json:"link"`
	Source      string      `json:"source"`
	PublishedAt string      `json:"published_at,omitempty"`
	Candidates  []Candidate `json:"candidates,omitempty"`
}

// Candidate is one gazetteer place. The eval gazetteer is a minimal matcher
// over GeoNames cities15000 plus countries; M2·1 replaces it with the real
// gazetteer.
type Candidate struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Admin      string `json:"admin,omitempty"`
	Country    string `json:"country,omitempty"`
	Population int64  `json:"population,omitempty"`
}

// Pair is one story-pair candidate.
type Pair struct {
	ID     string   `json:"id"`
	A      string   `json:"a"`
	B      string   `json:"b"`
	Shared []string `json:"shared,omitempty"`
}

// MatchCandidate is one alert-prompt candidate.
type MatchCandidate struct {
	ID        string `json:"id"`
	ArticleID string `json:"article_id"`
	Prompt    string `json:"prompt"`
	Relevant  bool   `json:"relevant"`
	// Derived is true when the label was derived from the article's category
	// rather than labelled independently.
	Derived bool `json:"derived"`
}

// ArticleLabel is one labelled article.
type ArticleLabel struct {
	ArticleID    string   `json:"article_id"`
	Category     string   `json:"category"`
	PrimaryPlace string   `json:"primary_place"` // candidate id or "none"
	PrimaryName  string   `json:"primary_name,omitempty"`
	Precision    string   `json:"precision"`
	Parties      []string `json:"parties,omitempty"`
	Labeler      string   `json:"labeler"`
	LabeledAt    string   `json:"labeled_at"`
	Provisional  bool     `json:"provisional"`
}

// PairLabel is one labelled pair.
type PairLabel struct {
	PairID    string `json:"pair_id"`
	Related   bool   `json:"related"`
	Type      string `json:"type"`
	Direction string `json:"direction"`
	Strength  int    `json:"strength"`
	// Duplicate marks coverage of the same event by two outlets. Clustering
	// handles it in production, so the eval excludes duplicate pairs from the
	// relation metrics.
	Duplicate   bool   `json:"duplicate,omitempty"`
	Labeler     string `json:"labeler"`
	LabeledAt   string `json:"labeled_at"`
	Provisional bool   `json:"provisional"`
}

// LabelSource summarises one labeler's contribution.
type LabelSource struct {
	Labeler     string `json:"labeler"`
	Provisional bool   `json:"provisional"`
	Articles    int    `json:"articles"`
	Pairs       int    `json:"pairs"`
}

// Dataset is everything one eval run reads.
type Dataset struct {
	Dir           string
	Articles      []Article
	Pairs         []Pair
	Matches       []MatchCandidate
	ArticleLabels map[string]ArticleLabel
	PairLabels    map[string]PairLabel
	// Sources counts labels by labeler after human overrides are applied.
	Sources []LabelSource
	// Warnings collects non-fatal data problems (orphan labels, missing
	// candidates, ...).
	Warnings []string
}

// LoadDataset reads candidates and labels from dir.
//
// A human label file overrides an agent label for the same id:
//
//	articles.labels.jsonl        seeded agent labels
//	articles.labels.human.jsonl  human labels (the follow-up target)
//	pairs.labels.jsonl
//	pairs.labels.human.jsonl
func LoadDataset(dir string) (*Dataset, error) {
	ds := &Dataset{
		Dir:           dir,
		ArticleLabels: make(map[string]ArticleLabel),
		PairLabels:    make(map[string]PairLabel),
	}
	if err := readJSONL(filepath.Join(dir, "articles.candidates.jsonl"), &ds.Articles); err != nil {
		return nil, err
	}
	if err := readJSONL(filepath.Join(dir, "pairs.candidates.jsonl"), &ds.Pairs); err != nil {
		return nil, err
	}
	matchesPath := filepath.Join(dir, "match.candidates.jsonl")
	if err := readJSONL(matchesPath, &ds.Matches); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	if len(ds.Articles) == 0 {
		return nil, fmt.Errorf("eval: %s has no articles", dir)
	}
	if len(ds.Pairs) == 0 {
		return nil, fmt.Errorf("eval: %s has no pairs", dir)
	}

	for _, name := range []string{"articles.labels.jsonl", "articles.labels.human.jsonl"} {
		var labels []ArticleLabel
		if err := readJSONL(filepath.Join(dir, name), &labels); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, l := range labels {
			if err := validateArticleLabel(l); err != nil {
				ds.Warnings = append(ds.Warnings, fmt.Sprintf("%s: %v", name, err))
				continue
			}
			ds.ArticleLabels[l.ArticleID] = l
		}
	}
	for _, name := range []string{"pairs.labels.jsonl", "pairs.labels.human.jsonl"} {
		var labels []PairLabel
		if err := readJSONL(filepath.Join(dir, name), &labels); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, l := range labels {
			if err := validatePairLabel(l); err != nil {
				ds.Warnings = append(ds.Warnings, fmt.Sprintf("%s: %v", name, err))
				continue
			}
			ds.PairLabels[l.PairID] = l
		}
	}

	articles := make(map[string]struct{}, len(ds.Articles))
	for _, a := range ds.Articles {
		articles[a.ID] = struct{}{}
	}
	pairs := make(map[string]struct{}, len(ds.Pairs))
	for _, p := range ds.Pairs {
		pairs[p.ID] = struct{}{}
	}
	for id := range ds.ArticleLabels {
		if _, ok := articles[id]; !ok {
			ds.Warnings = append(ds.Warnings, "article label for unknown id "+id)
		}
	}
	for id := range ds.PairLabels {
		if _, ok := pairs[id]; !ok {
			ds.Warnings = append(ds.Warnings, "pair label for unknown id "+id)
		}
	}
	ds.Sources = summarizeSources(ds.ArticleLabels, ds.PairLabels)
	return ds, nil
}

func validateArticleLabel(l ArticleLabel) error {
	if l.ArticleID == "" {
		return fmt.Errorf("label has no article_id")
	}
	if l.Category == "" || l.Precision == "" {
		return fmt.Errorf("label %s is missing category or precision", l.ArticleID)
	}
	if l.PrimaryPlace == "" {
		return fmt.Errorf("label %s is missing primary_place", l.ArticleID)
	}
	if l.Labeler == "" || l.LabeledAt == "" {
		return fmt.Errorf("label %s is missing labeler or labeled_at", l.ArticleID)
	}
	if !l.Provisional && !strings.HasPrefix(l.Labeler, "human:") {
		return fmt.Errorf("label %s is non-provisional but labeler %q is not human:", l.ArticleID, l.Labeler)
	}
	return nil
}

func validatePairLabel(l PairLabel) error {
	if l.PairID == "" {
		return fmt.Errorf("pair label has no pair_id")
	}
	if l.Type == "" || l.Direction == "" {
		return fmt.Errorf("pair label %s is missing type or direction", l.PairID)
	}
	if l.Strength < 0 || l.Strength > 3 {
		return fmt.Errorf("pair label %s strength %d outside 0..3", l.PairID, l.Strength)
	}
	if l.Labeler == "" || l.LabeledAt == "" {
		return fmt.Errorf("pair label %s is missing labeler or labeled_at", l.PairID)
	}
	if !l.Provisional && !strings.HasPrefix(l.Labeler, "human:") {
		return fmt.Errorf("pair label %s is non-provisional but labeler %q is not human:", l.PairID, l.Labeler)
	}
	return nil
}

func summarizeSources(articles map[string]ArticleLabel, pairs map[string]PairLabel) []LabelSource {
	index := make(map[string]*LabelSource)
	get := func(labeler string, provisional bool) *LabelSource {
		key := labeler
		if s, ok := index[key]; ok {
			return s
		}
		s := &LabelSource{Labeler: labeler, Provisional: provisional}
		index[key] = s
		return s
	}
	for _, l := range articles {
		get(l.Labeler, l.Provisional).Articles++
	}
	for _, l := range pairs {
		get(l.Labeler, l.Provisional).Pairs++
	}
	out := make([]LabelSource, 0, len(index))
	for _, s := range index {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labeler < out[j].Labeler })
	return out
}

// readJSONL decodes a JSON-lines file into out. Blank lines are skipped.
func readJSONL[T any](path string, out *[]T) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	line := 0
	for sc.Scan() {
		line++
		b := strings.TrimSpace(sc.Text())
		if b == "" {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(b), &v); err != nil {
			return fmt.Errorf("eval: %s:%d: %w", path, line, err)
		}
		*out = append(*out, v)
	}
	return sc.Err()
}

// writeJSONL writes one JSON object per line, creating the directory.
func writeJSONL[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// articleByID indexes articles.
func articleByID(articles []Article) map[string]Article {
	out := make(map[string]Article, len(articles))
	for _, a := range articles {
		out[a.ID] = a
	}
	return out
}

// WriteJSONL writes rows to path, one JSON object per line. It is the
// collector's and label CLI's writer; reads use LoadDataset/readJSONL.
func WriteJSONL[T any](path string, rows []T) error {
	return writeJSONL(path, rows)
}

// ReadArticles reads an article candidates file.
func ReadArticles(path string) ([]Article, error) {
	var out []Article
	if err := readJSONL(path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadPairs reads a pair candidates file.
func ReadPairs(path string) ([]Pair, error) {
	var out []Pair
	if err := readJSONL(path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Limit keeps up to n labelled articles, up to n labelled pairs (adding their
// endpoint articles when needed) and up to n match candidates. It is the CLI's
// -max-items test hook and a small smoke run; a zero or negative n leaves the
// dataset unchanged.
func (d *Dataset) Limit(n int) {
	if n <= 0 {
		return
	}
	original := make(map[string]Article, len(d.Articles))
	for _, a := range d.Articles {
		original[a.ID] = a
	}
	keptArticles := make([]Article, 0, n)
	articleIDs := make(map[string]struct{}, n)
	for _, a := range d.Articles {
		if _, labelled := d.ArticleLabels[a.ID]; !labelled {
			continue
		}
		keptArticles = append(keptArticles, a)
		articleIDs[a.ID] = struct{}{}
		if len(keptArticles) >= n {
			break
		}
	}
	keptPairs := make([]Pair, 0, n)
	pairIDs := make(map[string]struct{}, n)
	for _, p := range d.Pairs {
		if _, labelled := d.PairLabels[p.ID]; !labelled {
			continue
		}
		for _, endpoint := range []string{p.A, p.B} {
			if _, ok := articleIDs[endpoint]; ok {
				continue
			}
			a, exists := original[endpoint]
			if !exists {
				continue
			}
			keptArticles = append(keptArticles, a)
			articleIDs[endpoint] = struct{}{}
		}
		if _, okA := articleIDs[p.A]; !okA {
			continue
		}
		if _, okB := articleIDs[p.B]; !okB {
			continue
		}
		keptPairs = append(keptPairs, p)
		pairIDs[p.ID] = struct{}{}
		if len(keptPairs) >= n {
			break
		}
	}
	d.Articles = keptArticles
	d.Pairs = keptPairs

	keptMatches := make([]MatchCandidate, 0, n)
	for _, m := range d.Matches {
		if _, ok := articleIDs[m.ArticleID]; !ok {
			continue
		}
		keptMatches = append(keptMatches, m)
		if len(keptMatches) >= n {
			break
		}
	}
	d.Matches = keptMatches

	for id := range d.ArticleLabels {
		if _, ok := articleIDs[id]; !ok {
			delete(d.ArticleLabels, id)
		}
	}
	for id := range d.PairLabels {
		if _, ok := pairIDs[id]; !ok {
			delete(d.PairLabels, id)
		}
	}
	d.Sources = summarizeSources(d.ArticleLabels, d.PairLabels)
}
