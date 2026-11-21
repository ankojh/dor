// Command collect builds the eval's candidate sets: recent articles from a
// handful of public RSS feeds, gazetteer candidates for the plot question, and
// story-pair candidates. It stores headline, short snippet, link, source and
// time only — never full text.
//
// The committed JSONL is a snapshot. Re-running changes it as feeds move, so
// collect deliberately, review the diff, and commit. `make eval` never fetches
// the network.
package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ankojh/dor/eval"
	"github.com/ankojh/dor/eval/gazetteer"
)

type feed struct {
	Source string
	URL    string
}

var feeds = []feed{
	{"BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"},
	{"Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"},
	{"DW", "https://rss.dw.com/rdf/rss-en-world"},
	{"NPR World", "https://feeds.npr.org/1004/rss.xml"},
	{"The Guardian", "https://www.theguardian.com/world/rss"},
	{"ABC Australia", "https://www.abc.net.au/news/feed/51120/rss.xml"},
	{"France 24", "https://www.france24.com/en/rss"},
	{"CBC World", "https://www.cbc.ca/webfeed/rss/rss-world"},
	{"Sky News", "https://feeds.skynews.com/feeds/rss/world.xml"},
	{"EuroNews", "https://www.euronews.com/rss?level=theme&name=news"},
	{"UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml"},
	{"CNA Asia", "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml"},
	{"NYT World", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
	{"Washington Post", "https://feeds.washingtonpost.com/rss/world"},
	{"Japan Times", "https://www.japantimes.co.jp/feed/"},
	{"Africanews", "https://www.africanews.com/feed/rss"},
	{"SCMP", "https://www.scmp.com/rss/91/feed"},
	{"Times of India", "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms"},
	{"The Hindu", "https://www.thehindu.com/news/international/feeder/default.rss"},
}

type rssItem struct {
	Title       string `xml:"title"`
	Description string `xml:"description"`
	Link        string `xml:"link"`
	PubDate     string `xml:"pubDate"`
	Published   string `xml:"published"`
	Updated     string `xml:"updated"`
}

type atomEntry struct {
	Title     string `xml:"title"`
	Summary   string `xml:"summary"`
	Content   string `xml:"content"`
	Published string `xml:"published"`
	Updated   string `xml:"updated"`
	Links     []struct {
		Href string `xml:"href,attr"`
		Rel  string `xml:"rel,attr"`
	} `xml:"link"`
}

type feedDoc struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
	Items   []rssItem   `xml:"item"`  // RDF
	Entries []atomEntry `xml:"entry"` // Atom
}

type rawArticle struct {
	Title   string
	Snippet string
	URL     string
	Source  string
	At      time.Time
}

func main() {
	var (
		outDir      = flag.String("out", "eval/data", "output directory")
		limit       = flag.Int("limit", 350, "maximum articles")
		pairs       = flag.Int("pairs", 350, "maximum pair candidates")
		cacheDir    = flag.String("cache", "eval/data/.cache/geonames", "GeoNames cache (gitignored)")
		matchesOnly = flag.Bool("matches-only", false, "only regenerate match.candidates.jsonl from existing labels")
		timeout     = flag.Duration("timeout", 3*time.Minute, "overall fetch timeout")
	)
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fail(err)
	}
	if *matchesOnly {
		if err := writeMatches(*outDir); err != nil {
			fail(err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	articles, err := collect(ctx, *limit)
	if err != nil {
		fail(err)
	}
	gz, err := gazetteer.Load(ctx, *cacheDir)
	if err != nil {
		fail(err)
	}
	rows := make([]eval.Article, 0, len(articles))
	for _, a := range articles {
		cands := gz.Candidates(a.Title+" "+a.Snippet, a.Title, 8)
		row := eval.Article{
			ID: "a" + shortHash(a.URL), Title: a.Title, Snippet: a.Snippet,
			URL: a.URL, Source: a.Source, PublishedAt: a.At.Format(time.RFC3339),
		}
		for _, c := range cands {
			row.Candidates = append(row.Candidates, eval.Candidate{
				ID: c.ID, Name: c.Name, Admin: c.Admin, Country: c.Country, Population: c.Population,
			})
		}
		rows = append(rows, row)
	}
	if err := eval.WriteJSONL(filepath.Join(*outDir, "articles.candidates.jsonl"), rows); err != nil {
		fail(err)
	}
	fmt.Printf("articles: %d with %d gazetteer candidates\n", len(rows), countCandidates(rows))

	pairRows := buildPairs(rows, *pairs)
	if err := eval.WriteJSONL(filepath.Join(*outDir, "pairs.candidates.jsonl"), pairRows); err != nil {
		fail(err)
	}
	fmt.Printf("pairs: %d\n", len(pairRows))
	if err := writeMatches(*outDir); err != nil {
		fmt.Println("matches: skipped (" + err.Error() + ")")
	}
}

func collect(ctx context.Context, limit int) ([]rawArticle, error) {
	type feedResult struct {
		source string
		items  []rawArticle
		err    error
	}
	results := make(chan feedResult, len(feeds))
	for _, f := range feeds {
		go func(f feed) {
			items, err := fetchFeed(ctx, f)
			results <- feedResult{source: f.Source, items: items, err: err}
		}(f)
	}
	var byFeed [][]rawArticle
	seen := make(map[string]struct{})
	good := 0
	for range feeds {
		r := <-results
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "feed %s: %v\n", r.source, r.err)
			continue
		}
		var items []rawArticle
		for _, a := range r.items {
			if _, dup := seen[a.URL]; dup {
				continue
			}
			seen[a.URL] = struct{}{}
			items = append(items, a)
		}
		if len(items) > 0 {
			good++
			byFeed = append(byFeed, items)
			fmt.Fprintf(os.Stderr, "feed %s: %d items\n", r.source, len(items))
		}
	}
	if good < 3 {
		return nil, fmt.Errorf("only %d feeds returned items", good)
	}
	var out []rawArticle
	for len(out) < limit {
		progress := false
		for i := range byFeed {
			if len(byFeed[i]) == 0 {
				continue
			}
			out = append(out, byFeed[i][0])
			byFeed[i] = byFeed[i][1:]
			progress = true
			if len(out) >= limit {
				break
			}
		}
		if !progress {
			break
		}
	}
	if len(out) < 50 {
		return nil, fmt.Errorf("only %d articles collected", len(out))
	}
	return out, nil
}

func fetchFeed(ctx context.Context, f feed) ([]rawArticle, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "dor-eval-collector/0.1 (+https://github.com/ankojh/dor)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var doc feedDoc
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	cutoff := time.Now().Add(-21 * 24 * time.Hour)
	var out []rawArticle
	add := func(title, snippet, link, pub string) {
		title = clean(title)
		if len(title) < 20 {
			return
		}
		link = strings.TrimSpace(link)
		if link == "" {
			return
		}
		snippet = clean(snippet)
		if snippet == "" {
			return
		}
		at := parseTime(pub)
		if !at.IsZero() && at.Before(cutoff) {
			return
		}
		out = append(out, rawArticle{Title: title, Snippet: truncate(snippet, 400), URL: link, Source: f.Source, At: at})
	}
	for _, it := range append(doc.Channel.Items, doc.Items...) {
		add(it.Title, it.Description, it.Link, firstNonEmpty(it.PubDate, it.Published, it.Updated))
	}
	for _, it := range doc.Entries {
		link := ""
		for _, l := range it.Links {
			if l.Rel == "" || l.Rel == "alternate" {
				link = l.Href
				break
			}
		}
		add(it.Title, firstNonEmpty(it.Summary, it.Content), link, firstNonEmpty(it.Published, it.Updated))
	}
	return out, nil
}

var tagRE = regexp.MustCompile(`<[^>]*>`)

func clean(s string) string {
	s = html.UnescapeString(s)
	s = tagRE.ReplaceAllString(s, " ")
	return strings.Join(strings.Fields(s), " ")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return strings.TrimSpace(s[:n])
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		time.RFC3339, "Mon, 2 Jan 2006 15:04:05 -0700",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func shortHash(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

func countCandidates(rows []eval.Article) int {
	n := 0
	for _, r := range rows {
		n += len(r.Candidates)
	}
	return n
}

var pairStop = map[string]struct{}{
	"that": {}, "this": {}, "with": {}, "from": {}, "after": {}, "over": {},
	"into": {}, "says": {}, "said": {}, "will": {}, "amid": {}, "more": {},
	"than": {}, "about": {}, "their": {}, "there": {}, "have": {}, "been": {},
}

func buildPairs(rows []eval.Article, limit int) []eval.Pair {
	terms := make([]map[string]struct{}, len(rows))
	for i, a := range rows {
		terms[i] = titleTerms(a)
	}
	type scored struct {
		i, j   int
		shared []string
		score  int
	}
	var likely []scored
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			if rows[i].Source == rows[j].Source {
				continue
			}
			var shared []string
			for t := range terms[i] {
				if _, ok := terms[j][t]; ok {
					shared = append(shared, t)
				}
			}
			if len(shared) < 2 {
				continue
			}
			// Same-event coverage is a cluster, not a link. Drop pairs whose
			// titles are near-identical; the relation judge never sees them in
			// production.
			if jaccard(terms[i], terms[j]) > 0.5 {
				continue
			}
			sort.Strings(shared)
			if len(shared) > 5 {
				shared = shared[:5]
			}
			likely = append(likely, scored{i: i, j: j, shared: shared, score: len(shared)})
		}
	}
	sort.SliceStable(likely, func(a, b int) bool { return likely[a].score > likely[b].score })
	seen := make(map[string]struct{})
	var out []eval.Pair
	add := func(i, j int, shared []string) {
		if i > j {
			i, j = j, i
		}
		key := rows[i].ID + "|" + rows[j].ID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, eval.Pair{
			ID: "p-" + rows[i].ID + "-" + rows[j].ID, A: rows[i].ID, B: rows[j].ID, Shared: shared,
		})
	}
	half := limit / 2
	for _, c := range likely {
		if len(out) >= half {
			break
		}
		add(c.i, c.j, c.shared)
	}
	rng := rand.New(rand.NewSource(42))
	for len(out) < limit {
		i := rng.Intn(len(rows))
		j := rng.Intn(len(rows))
		if i == j {
			continue
		}
		add(i, j, nil)
		if len(seen) > limit*20 {
			break
		}
	}
	return out
}

var termStop = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "with": {}, "from": {}, "that": {}, "this": {},
	"after": {}, "over": {}, "into": {}, "says": {}, "said": {}, "will": {}, "amid": {},
	"more": {}, "than": {}, "about": {}, "their": {}, "there": {}, "have": {}, "been": {},
	"news": {}, "world": {}, "live": {}, "update": {}, "updates": {}, "new": {},
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if _, ok := b[k]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func titleTerms(a eval.Article) map[string]struct{} {
	out := make(map[string]struct{})
	add := func(s string) {
		for _, w := range strings.Fields(strings.ToLower(s)) {
			w = strings.Trim(w, ".,:;!?\"'()[]“”‘’-")
			if len(w) < 4 {
				continue
			}
			if _, stop := termStop[w]; stop {
				continue
			}
			if _, bad := pairStop[w]; bad {
				continue
			}
			out[w] = struct{}{}
		}
	}
	add(a.Title)
	for _, c := range a.Candidates {
		add(c.Name)
	}
	return out
}

// matchPrompts maps a category to an alert prompt. Prompts are deliberately
// broad; the match set is a tiny smoke set, not a tuned alert corpus.
var matchPrompts = map[string]string{
	"conflict": "armed conflict, war or terrorism",
	"politics": "elections, government or diplomacy",
	"economy":  "markets, trade or the economy",
	"tech":     "technology, AI or cyberattacks",
	"climate":  "climate change or the environment",
	"disaster": "natural disasters or humanitarian emergencies",
	"health":   "disease outbreaks or public health",
	"science":  "science or space",
	"sports":   "sport",
	"culture":  "arts, entertainment or religion",
	"other":    "major international news",
}

func writeMatches(dir string) error {
	labels, err := readArticleLabels(filepath.Join(dir, "articles.labels.jsonl"), filepath.Join(dir, "articles.labels.human.jsonl"))
	if err != nil {
		return err
	}
	if len(labels) < 5 {
		return fmt.Errorf("need at least 5 article labels, have %d", len(labels))
	}
	articles, err := readArticles(filepath.Join(dir, "articles.candidates.jsonl"))
	if err != nil {
		return err
	}
	byID := make(map[string]eval.Article, len(articles))
	for _, a := range articles {
		byID[a.ID] = a
	}
	// Spread the sample across categories: take up to two per category.
	perCategory := make(map[string]int)
	var chosen []eval.ArticleLabel
	keys := make([]string, 0, len(labels))
	for id := range labels {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		l := labels[id]
		if perCategory[l.Category] >= 2 {
			continue
		}
		if _, ok := byID[id]; !ok {
			continue
		}
		perCategory[l.Category]++
		chosen = append(chosen, l)
		if len(chosen) >= 10 {
			break
		}
	}
	var out []eval.MatchCandidate
	for _, l := range chosen {
		a := byID[l.ArticleID]
		rel := matchPrompts[l.Category]
		other := mismatchPrompts[l.Category]
		if other == "" {
			other = "sport"
		}
		out = append(out,
			eval.MatchCandidate{
				ID: "m-" + a.ID + "-rel", ArticleID: a.ID,
				Prompt: "Stories about " + rel, Relevant: true, Derived: true,
			},
			eval.MatchCandidate{
				ID: "m-" + a.ID + "-irr", ArticleID: a.ID,
				Prompt: "Stories about " + other, Relevant: false, Derived: true,
			},
		)
	}
	if err := eval.WriteJSONL(filepath.Join(dir, "match.candidates.jsonl"), out); err != nil {
		return err
	}
	fmt.Printf("matches: %d (derived from article categories)\n", len(out))
	return nil
}

// mismatchPrompts is a deliberately distant prompt per category, so the
// "irrelevant" arm is not accidentally a near-miss.
var mismatchPrompts = map[string]string{
	"conflict": "sport",
	"politics": "science or space",
	"economy":  "arts, entertainment or religion",
	"tech":     "sport",
	"climate":  "sport",
	"disaster": "arts, entertainment or religion",
	"health":   "sport",
	"science":  "sport",
	"sports":   "armed conflict, war or terrorism",
	"culture":  "technology, AI or cyberattacks",
	"other":    "sport",
}

func readArticleLabels(paths ...string) (map[string]eval.ArticleLabel, error) {
	out := make(map[string]eval.ArticleLabel)
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		dec := json.NewDecoder(f)
		for {
			var l eval.ArticleLabel
			if err := dec.Decode(&l); err != nil {
				if err == io.EOF {
					break
				}
				f.Close()
				return nil, err
			}
			out[l.ArticleID] = l
		}
		f.Close()
	}
	return out, nil
}

func readArticles(path string) ([]eval.Article, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []eval.Article
	dec := json.NewDecoder(f)
	for {
		var a eval.Article
		if err := dec.Decode(&a); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "collect:", err)
	os.Exit(1)
}
