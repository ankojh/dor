// Command label is the resumable labelling CLI for the eval sets.
//
// It is deliberately explicit about who labelled: --labeler is required and
// must start with "human:" or "agent:". Agent labels are written with
// provisional: true unless you override it. Nothing here ever claims a human
// label on its own.
//
//	make eval-label SET=articles LABELER=human:your-name
//	go run ./eval/cmd/label -set pairs -labeler agent:model -out eval/data/pairs.labels.jsonl
//
// The default output for a human labeler is eval/data/<set>.labels.human.jsonl,
// which the eval runner prefers over the agent seed file for the same id.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ankojh/dor/eval"
	"github.com/ankojh/dor/internal/decide"
)

var errQuit = errors.New("quit")

func main() {
	var (
		set         = flag.String("set", "articles", "articles or pairs")
		dataDir     = flag.String("data", "eval/data", "candidate directory")
		out         = flag.String("out", "", "output JSONL (default depends on labeler)")
		labeler     = flag.String("labeler", "", "required: human:<name> or agent:<model>")
		provisional = flag.Bool("provisional", false, "mark labels provisional (default: false for human:, true for agent:)")
		limit       = flag.Int("limit", 0, "stop after N new labels (0 = all)")
		redo        = flag.Bool("redo", false, "re-label items already in the output file")
	)
	flag.Parse()
	if err := run(*set, *dataDir, *out, *labeler, *provisional, *limit, *redo); err != nil {
		fmt.Fprintln(os.Stderr, "label:", err)
		os.Exit(1)
	}
}

func run(set, dataDir, out, labeler string, provisionalFlag bool, limit int, redo bool) error {
	if labeler == "" {
		return errors.New("--labeler is required (human:<name> or agent:<model>)")
	}
	if !strings.HasPrefix(labeler, "human:") && !strings.HasPrefix(labeler, "agent:") {
		return fmt.Errorf("--labeler %q must start with human: or agent:", labeler)
	}
	provisional := provisionalFlag
	if !flagWasSet("provisional") {
		provisional = !strings.HasPrefix(labeler, "human:")
	}
	if out == "" {
		if strings.HasPrefix(labeler, "human:") {
			out = filepath.Join(dataDir, set+".labels.human.jsonl")
		} else {
			out = filepath.Join(dataDir, set+".labels.jsonl")
		}
	}
	done := make(map[string]bool)
	if !redo {
		if err := readExisting(out, set, done); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	in := bufio.NewReader(os.Stdin)
	fmt.Printf("Dor eval labelling · set=%s · labeler=%s · provisional=%t\n", set, labeler, provisional)
	fmt.Printf("output: %s · %d already labelled\n", out, len(done))
	fmt.Println("Answer with the short key or number, Enter to accept a shown default, q to quit.")
	if !strings.HasPrefix(labeler, "human:") {
		fmt.Println("NOTE: this run writes agent labels. Human labelling is a separate run with -labeler human:<name>.")
	}
	fmt.Println()

	switch set {
	case "articles":
		articles, err := eval.ReadArticles(filepath.Join(dataDir, "articles.candidates.jsonl"))
		if err != nil {
			return err
		}
		return labelArticles(in, articles, done, out, labeler, provisional, limit)
	case "pairs":
		pairs, err := eval.ReadPairs(filepath.Join(dataDir, "pairs.candidates.jsonl"))
		if err != nil {
			return err
		}
		articles, err := eval.ReadArticles(filepath.Join(dataDir, "articles.candidates.jsonl"))
		if err != nil {
			return err
		}
		return labelPairs(in, pairs, articles, done, out, labeler, provisional, limit)
	default:
		return fmt.Errorf("unknown set %q (articles or pairs)", set)
	}
}

func flagWasSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func readExisting(path, set string, done map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		switch set {
		case "articles":
			var l eval.ArticleLabel
			if err := dec.Decode(&l); err != nil {
				return nilIfEOF(err)
			}
			done[l.ArticleID] = true
		case "pairs":
			var l eval.PairLabel
			if err := dec.Decode(&l); err != nil {
				return nilIfEOF(err)
			}
			done[l.PairID] = true
		}
	}
}

func nilIfEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func appendJSON(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = f.Write(b)
	return err
}

func labelArticles(in *bufio.Reader, articles []eval.Article, done map[string]bool, out, labeler string, provisional bool, limit int) error {
	fmt.Println("Categories:")
	for i, o := range decide.CategoryOptions() {
		fmt.Printf("  %2d %-9s %s\n", i, o.Key, o.Definition)
	}
	fmt.Println("Precision: c=country r=region t=city s=street p=point n=none")
	fmt.Println()
	written := 0
	total := 0
	for _, a := range articles {
		if !done[a.ID] {
			total++
		}
	}
	for _, a := range articles {
		if done[a.ID] {
			continue
		}
		if limit > 0 && written >= limit {
			break
		}
		fmt.Printf("[%d/%d] %s · %s\n%s\n%s\n", written+1, total, a.Source, a.PublishedAt, a.Title, wrap(a.Snippet, 100, "  "))
		for i, c := range a.Candidates {
			loc := c.Name
			if c.Admin != "" {
				loc += ", " + c.Admin
			}
			if c.Country != "" && c.Country != c.Name {
				loc += ", " + c.Country
			}
			if c.Population > 0 {
				loc += fmt.Sprintf(" (population %d)", c.Population)
			}
			fmt.Printf("  %d) %s\n", i+1, loc)
		}

		cat, skip, err := askCategory(in)
		if err != nil {
			return stop(err)
		}
		if skip {
			fmt.Println("  skipped")
			continue
		}
		primary, err := askPrimary(in, len(a.Candidates))
		if err != nil {
			return stop(err)
		}
		precision := "none"
		if primary != "none" {
			precision, err = askPrecision(in)
			if err != nil {
				return stop(err)
			}
		}
		parties, err := askParties(in, a.Candidates, primary)
		if err != nil {
			return stop(err)
		}
		label := eval.ArticleLabel{
			ArticleID:    a.ID,
			Category:     cat,
			PrimaryPlace: primary,
			Precision:    precision,
			Parties:      parties,
			Labeler:      labeler,
			LabeledAt:    time.Now().UTC().Format(time.RFC3339),
			Provisional:  provisional,
		}
		if primary != "none" {
			for i, c := range a.Candidates {
				if strconv.Itoa(i+1) == primary || c.ID == primary {
					label.PrimaryName = c.Name
					label.PrimaryPlace = c.ID
					break
				}
			}
		}
		if err := appendJSON(out, label); err != nil {
			return err
		}
		written++
		fmt.Printf("  ok: %s · %s · parties %v\n", label.Category, label.PrimaryName, label.Parties)
	}
	fmt.Printf("wrote %d article labels to %s\n", written, out)
	return nil
}

func labelPairs(in *bufio.Reader, pairs []eval.Pair, articles []eval.Article, done map[string]bool, out, labeler string, provisional bool, limit int) error {
	byID := make(map[string]eval.Article, len(articles))
	for _, a := range articles {
		byID[a.ID] = a
	}
	written := 0
	total := 0
	for _, p := range pairs {
		if !done[p.ID] {
			total++
		}
	}
	for _, p := range pairs {
		if done[p.ID] {
			continue
		}
		if limit > 0 && written >= limit {
			break
		}
		a, okA := byID[p.A]
		b, okB := byID[p.B]
		if !okA || !okB {
			continue
		}
		fmt.Printf("[%d/%d] pair %s\nA: %s\n   %s\nB: %s\n   %s\n", written+1, total, p.ID,
			a.Title, wrap(a.Snippet, 100, "   "), b.Title, wrap(b.Snippet, 100, "   "))
		if len(p.Shared) > 0 {
			fmt.Printf("   shared: %s\n", strings.Join(p.Shared, ", "))
		}
		rel, err := ask(in, "related? [y/n/d=same-event duplicate/q] ", "")
		if err != nil {
			return stop(err)
		}
		switch strings.ToLower(rel) {
		case "q":
			return stop(errQuit)
		case "d":
			label := eval.PairLabel{
				PairID: p.ID, Related: true, Type: "thematic", Direction: "unclear", Strength: 3,
				Duplicate: true,
				Labeler:   labeler, LabeledAt: time.Now().UTC().Format(time.RFC3339), Provisional: provisional,
			}
			if err := appendJSON(out, label); err != nil {
				return err
			}
			written++
			fmt.Println("  ok: same event (duplicate, excluded from relation metrics)")
			continue
		case "y", "yes":
		case "n", "no":
			label := eval.PairLabel{
				PairID: p.ID, Related: false, Type: "none", Direction: "unclear", Strength: 0,
				Labeler: labeler, LabeledAt: time.Now().UTC().Format(time.RFC3339), Provisional: provisional,
			}
			if err := appendJSON(out, label); err != nil {
				return err
			}
			written++
			fmt.Println("  ok: not related")
			continue
		default:
			fmt.Println("  skipped")
			continue
		}
		typ, err := ask(in, "type [c=causal r=reaction t=thematic/q] ", "")
		if err != nil {
			return stop(err)
		}
		typeKey := map[string]string{"c": "causal", "r": "reaction", "t": "thematic"}[strings.ToLower(typ)]
		if typeKey == "" {
			fmt.Println("  skipped")
			continue
		}
		dir, err := ask(in, "direction [a=A→B b=B→A u=unclear] (u) ", "u")
		if err != nil {
			return stop(err)
		}
		dirKey := map[string]string{"a": "a_to_b", "b": "b_to_a", "u": "unclear"}[strings.ToLower(dir)]
		if dirKey == "" {
			dirKey = "unclear"
		}
		str, err := ask(in, "strength 0-3 (0) ", "0")
		if err != nil {
			return stop(err)
		}
		strength, _ := strconv.Atoi(strings.TrimSpace(str))
		if strength < 0 || strength > 3 {
			strength = 0
		}
		label := eval.PairLabel{
			PairID: p.ID, Related: true, Type: typeKey, Direction: dirKey, Strength: strength,
			Labeler: labeler, LabeledAt: time.Now().UTC().Format(time.RFC3339), Provisional: provisional,
		}
		if err := appendJSON(out, label); err != nil {
			return err
		}
		written++
		fmt.Printf("  ok: %s · %s · strength %d\n", typeKey, dirKey, strength)
	}
	fmt.Printf("wrote %d pair labels to %s\n", written, out)
	return nil
}

func askCategory(in *bufio.Reader) (string, bool, error) {
	keys := decide.CategoryKeys()
	for {
		line, err := ask(in, "category [number or key, s=skip, q=quit] ", "")
		if err != nil {
			return "", false, err
		}
		switch strings.ToLower(line) {
		case "q":
			return "", false, errQuit
		case "s":
			return "", true, nil
		}
		if n, err := strconv.Atoi(line); err == nil && n >= 0 && n < len(keys) {
			return keys[n], false, nil
		}
		for _, k := range keys {
			if strings.EqualFold(line, k) {
				return k, false, nil
			}
		}
		fmt.Println("  choose a number 0-10 or a category key")
	}
}

func askPrimary(in *bufio.Reader, n int) (string, error) {
	for {
		prompt := "primary place [n=none, q=quit] "
		if n > 0 {
			prompt = fmt.Sprintf("primary place [1-%d, n=none, q=quit] ", n)
		}
		line, err := ask(in, prompt, "")
		if err != nil {
			return "", err
		}
		line = strings.ToLower(line)
		switch line {
		case "q":
			return "", errQuit
		case "n", "none", "":
			return "none", nil
		}
		if v, err := strconv.Atoi(line); err == nil && v >= 1 && v <= n {
			return strconv.Itoa(v), nil
		}
		fmt.Println("  choose a candidate number or n")
	}
}

func askPrecision(in *bufio.Reader) (string, error) {
	for {
		line, err := ask(in, "precision [c=country r=region t=city s=street p=point, q=quit] ", "")
		if err != nil {
			return "", err
		}
		switch strings.ToLower(line) {
		case "q":
			return "", errQuit
		case "c":
			return "country", nil
		case "r":
			return "region", nil
		case "t":
			return "city", nil
		case "s":
			return "street", nil
		case "p":
			return "point", nil
		}
		fmt.Println("  choose c, r, t, s or p")
	}
}

func askParties(in *bufio.Reader, candidates []eval.Candidate, primary string) ([]string, error) {
	n := len(candidates)
	if n == 0 {
		return nil, nil
	}
	line, err := ask(in, "parties [numbers comma-separated, none] (none) ", "none")
	if err != nil {
		return nil, err
	}
	line = strings.ToLower(strings.TrimSpace(line))
	if line == "q" {
		return nil, errQuit
	}
	if line == "" || line == "none" || line == "n" {
		return nil, nil
	}
	var out []string
	seen := make(map[string]struct{})
	for _, part := range strings.Split(line, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || v < 1 || v > n {
			continue
		}
		id := candidates[v-1].ID
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

// ask prints prompt, reads one line, and returns it trimmed. An empty line
// returns def.
func ask(in *bufio.Reader, prompt, def string) (string, error) {
	fmt.Print(prompt)
	line, err := in.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def, nil
	}
	return line, nil
}

func wrap(s string, width int, indent string) string {
	words := strings.Fields(s)
	var b strings.Builder
	lineLen := 0
	for i, w := range words {
		if i > 0 && lineLen+1+len(w) > width {
			b.WriteString("\n" + indent)
			lineLen = 0
		} else if i > 0 {
			b.WriteString(" ")
			lineLen++
		}
		b.WriteString(w)
		lineLen += len(w)
	}
	return b.String()
}

func stop(err error) error {
	if errors.Is(err, errQuit) {
		return nil
	}
	return err
}
