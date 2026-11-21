package eval

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MockMarker is the first line of a report produced with any mock backend.
const MockMarker = "MOCK BACKENDS — NOT A MEASUREMENT"

// ReportData is everything the report needs.
type ReportData struct {
	Run         RunResult
	Selections  Selections
	DS          *Dataset
	GeneratedAt time.Time
	Notes       []string
}

// Report is the JSON form of the report, written next to the markdown.
type Report struct {
	Mock        bool              `json:"mock"`
	Marker      string            `json:"marker,omitempty"`
	GeneratedAt string            `json:"generated_at"`
	Backends    map[string]string `json:"backends"`
	Labels      LabelReport       `json:"labels"`
	Thresholds  ThresholdReport   `json:"thresholds"`
	Tasks       []TaskResult      `json:"tasks"`
	Warnings    []string          `json:"warnings,omitempty"`
	Notes       []string          `json:"notes,omitempty"`
}

// LabelReport summarises the labelled set.
type LabelReport struct {
	Sources     []LabelSource `json:"sources"`
	Articles    int           `json:"labelled_articles"`
	Pairs       int           `json:"labelled_pairs"`
	Matches     int           `json:"labelled_matches"`
	Provisional bool          `json:"provisional"`
	Summary     string        `json:"summary"`
}

// ThresholdReport is the applied thresholds plus the selection outcome.
type ThresholdReport struct {
	Applied     map[string]float64 `json:"applied"`
	Provisional bool               `json:"provisional"`
	SelectedAt  string             `json:"selected_at"`
	Selection   Selections         `json:"selection"`
}

// WriteReport writes eval/report.md and eval/report.json.
func WriteReport(mdPath, jsonPath string, data ReportData) error {
	if data.GeneratedAt.IsZero() {
		data.GeneratedAt = time.Now().UTC()
	}
	md := renderMarkdown(data)
	if err := os.MkdirAll(filepath.Dir(mdPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return err
	}
	if jsonPath == "" {
		return nil
	}
	doc := buildReport(data)
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(jsonPath, b, 0o644)
}

func buildReport(data ReportData) Report {
	labelledArticles := len(data.DS.ArticleLabels)
	labelledPairs := len(data.DS.PairLabels)
	matches := len(data.DS.Matches)
	provisional := true
	for _, s := range data.DS.Sources {
		if !s.Provisional {
			provisional = false
		}
	}
	applied := map[string]float64{
		"classify.accept":  data.Selections.Applied.Classify.Accept,
		"plot.primary":     data.Selections.Applied.Plot.Primary,
		"plot.party":       data.Selections.Applied.Plot.Party,
		"relate.accept":    data.Selections.Applied.Relate.Accept,
		"match.strictness": data.Selections.Applied.Match.Strictness,
	}
	return Report{
		Mock:        data.Run.Mock(),
		Marker:      marker(data.Run),
		GeneratedAt: data.GeneratedAt.Format(time.RFC3339),
		Backends: map[string]string{
			"jev":    data.Run.JevKind,
			"claude": data.Run.ClaudeKind,
		},
		Labels: LabelReport{
			Sources:     data.DS.Sources,
			Articles:    labelledArticles,
			Pairs:       labelledPairs,
			Matches:     matches,
			Provisional: provisional,
			Summary:     LabelSummary(data.DS.Sources),
		},
		Thresholds: ThresholdReport{
			Applied:     applied,
			Provisional: data.Selections.Applied.Provisional,
			SelectedAt:  data.Selections.Applied.SelectedAt,
			Selection:   data.Selections,
		},
		Tasks:    data.Run.Tasks,
		Warnings: data.DS.Warnings,
		Notes:    data.Notes,
	}
}

func marker(run RunResult) string {
	if run.Mock() {
		return MockMarker
	}
	return ""
}

func renderMarkdown(data ReportData) string {
	var b strings.Builder
	if data.Run.Mock() {
		b.WriteString(MockMarker + "\n\n")
	}
	fmt.Fprintf(&b, "# Dor model eval — %s\n\n", data.GeneratedAt.Format("2006-01-02 15:04 UTC"))
	fmt.Fprintf(&b, "Backends: jev **%s**, Claude **%s**. Labels: %s.\n\n",
		data.Run.JevKind, data.Run.ClaudeKind, LabelSummary(data.DS.Sources))
	if models := servedModels(data.Run.Tasks); models != "" {
		fmt.Fprintf(&b, "Served models: %s.\n\n", models)
	}
	fmt.Fprintf(&b, "Labelled items: %d articles, %d pairs, %d alert prompts. ",
		len(data.DS.ArticleLabels), len(data.DS.PairLabels), len(data.DS.Matches))
	if data.Selections.Applied.Provisional {
		b.WriteString("Thresholds are **provisional**.\n\n")
	} else {
		b.WriteString("Thresholds are from a human-labelled run.\n\n")
	}
	if data.Run.Mock() {
		b.WriteString("> These numbers come from in-process mock backends that exercise the real client code. ")
		b.WriteString("They are **not a measurement of jev or Claude** and must not be used to choose a threshold.\n\n")
	}

	b.WriteString("## Results\n\n")
	b.WriteString("| Task | Backend | N | Calls | Answered | Accuracy | Accuracy (all) | ECE (event) | ECE (decision) | p50 ms | p95 ms | last p50 | last p95 | $/1k |\n")
	b.WriteString("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, t := range data.Run.Tasks {
		ece := "n/a"
		if len(t.Bins) > 0 {
			ece = fmt.Sprintf("%.3f", t.ECE)
		}
		dec := "n/a"
		if len(t.DecisionBins) > 0 {
			dec = fmt.Sprintf("%.3f", t.DecisionECE)
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %.3f | %.3f | %s | %s | %.1f | %.1f | %.1f | %.1f | $%.4f |\n",
			t.Task, t.Backend, t.N, t.Calls, t.Answered, t.Accuracy, t.AccuracyAll, ece, dec,
			t.LatencyP50MS, t.LatencyP95MS, t.LastLatencyP50MS, t.LastLatencyP95MS, t.CostPer1kUSD)
	}
	b.WriteString("\n")
	b.WriteString("`$/1k` divides total cost by **N labelled items**, not answered items, so refusals and errors are included. Latency is the total for the logical call including retries/backoff; `last` is the final attempt only. Results use the thresholds the run started with; the Thresholds section reports the separately selected values.\n\n")

	b.WriteString("## Fail-closed\n\n")
	b.WriteString("| Task | Backend | refusals | unanswered | errors |\n| --- | --- | ---: | ---: | ---: |\n")
	for _, t := range data.Run.Tasks {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d |\n", t.Task, t.Backend, t.Refusals, t.Unanswered, t.Errors)
	}
	b.WriteString("\n")

	b.WriteString("## Tokens and prompt cache\n\n")
	b.WriteString("| Task | Backend | input | output | cache read | cache write |\n| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, t := range data.Run.Tasks {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %d |\n",
			t.Task, t.Backend, t.InputTokens, t.OutputTokens, t.CacheReadTokens, t.CacheCreateTokens)
	}
	b.WriteString("\n")

	b.WriteString("## Per-task detail\n\n")
	b.WriteString("| Task | Backend | Metric | Value |\n| --- | --- | --- | ---: |\n")
	for _, t := range data.Run.Tasks {
		if len(t.Extra) == 0 {
			continue
		}
		keys := make([]string, 0, len(t.Extra))
		for k := range t.Extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "| %s | %s | %s | %.3f |\n", t.Task, t.Backend, k, t.Extra[k])
		}
	}
	b.WriteString("\n")

	b.WriteString("## Thresholds\n\n")
	fmt.Fprintf(&b, "| Question | Applied | Source |\n| --- | ---: | --- |\n")
	fmt.Fprintf(&b, "| classify.accept | %.2f | %s |\n", data.Selections.Applied.Classify.Accept, thresholdSource(data.Selections.Skipped))
	fmt.Fprintf(&b, "| plot.primary | %.2f | %s |\n", data.Selections.Applied.Plot.Primary, thresholdSource(data.Selections.Skipped))
	fmt.Fprintf(&b, "| plot.party | %.2f | %s |\n", data.Selections.Applied.Plot.Party, thresholdSource(data.Selections.Skipped))
	fmt.Fprintf(&b, "| relate.accept | %.2f | %s |\n", data.Selections.Applied.Relate.Accept, thresholdSource(data.Selections.Skipped))
	fmt.Fprintf(&b, "| match.strictness | %.2f | %s |\n\n", data.Selections.Applied.Match.Strictness, thresholdSource(data.Selections.Skipped))

	if data.Selections.Skipped {
		fmt.Fprintf(&b, "Selection skipped: %s.\n\n", data.Selections.Reason)
		if data.Selections.HasProjection {
			fmt.Fprintf(&b, "Projected Claude fallback rate at the configured thresholds: **%.1f%%** (target < %.0f%%), met: **%t**.\n\n",
				data.Selections.FallbackRate*100, data.Selections.Applied.Targets.ClaudeFallbackRate*100,
				data.Selections.FallbackMet)
		}
	} else {
		b.WriteString("| Selection | Target | Threshold | Accepted | Coverage | Accuracy | Met |\n")
		b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
		writeSelection := func(name string, s Selection) {
			fmt.Fprintf(&b, "| %s | %.2f | %.2f | %d | %.3f | %.3f | %t |\n",
				name, s.Target, s.Threshold, s.Accepted, s.Coverage, s.Accuracy, s.Met)
		}
		writeSelection("place pick", data.Selections.Plot)
		writeSelection("relation type", data.Selections.Relate)
		writeSelection("classify/fallback", data.Selections.Classify)
		b.WriteString("\n")
		fmt.Fprintf(&b, "Projected Claude fallback rate: **%.1f%%** (target < %.0f%%), met: **%t**.\n\n",
			data.Selections.FallbackRate*100, data.Selections.Applied.Targets.ClaudeFallbackRate*100,
			data.Selections.FallbackMet)
	}

	b.WriteString("## Calibration\n\n")
	b.WriteString("Event calibration is probability against observed truth. Decision calibration at a threshold is p for a predicted yes and 1-p for a predicted no. Choice tasks report selected-option calibration; binary tasks (match, relate) report both.\n\n")
	wrote := false
	for _, t := range data.Run.Tasks {
		if len(t.Bins) == 0 && len(t.DecisionBins) == 0 {
			continue
		}
		wrote = true
		if len(t.Bins) > 0 {
			fmt.Fprintf(&b, "**%s (%s), event** — ECE %.3f (n=%d)\n\n", t.Task, t.Backend, t.ECE, binN(t.Bins))
			writeBins(&b, t.Bins)
		}
		if len(t.DecisionBins) > 0 {
			if t.DecisionThreshold > 0 {
				fmt.Fprintf(&b, "**%s (%s), decision at %.2f** — ECE %.3f (n=%d)\n\n", t.Task, t.Backend, t.DecisionThreshold, t.DecisionECE, binN(t.DecisionBins))
			} else {
				fmt.Fprintf(&b, "**%s (%s), decision confidence** — ECE %.3f (n=%d)\n\n", t.Task, t.Backend, t.DecisionECE, binN(t.DecisionBins))
			}
			writeBins(&b, t.DecisionBins)
		}
	}
	if !wrote {
		b.WriteString("No backend returned probabilities.\n\n")
	}

	b.WriteString("## Accuracy vs coverage / strictness\n\n")
	wrote = false
	for _, t := range data.Run.Tasks {
		if len(t.Coverage) == 0 {
			continue
		}
		wrote = true
		if len(t.Binary) > 0 {
			fmt.Fprintf(&b, "**%s (%s)**\n\n| threshold | coverage | accepted | precision | recall | TP | FP | FN |\n| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n", t.Task, t.Backend)
			for _, c := range t.Coverage {
				fmt.Fprintf(&b, "| %.2f | %.3f | %d | %.3f | %.3f | %d | %d | %d |\n", c.Threshold, c.Coverage, c.Accepted, c.Precision, c.Recall, c.TP, c.FP, c.FN)
			}
		} else {
			fmt.Fprintf(&b, "**%s (%s)**\n\n| threshold | coverage | accepted | accuracy |\n| ---: | ---: | ---: | ---: |\n", t.Task, t.Backend)
			for _, c := range t.Coverage {
				fmt.Fprintf(&b, "| %.2f | %.3f | %d | %.3f |\n", c.Threshold, c.Coverage, c.Accepted, c.Accuracy)
			}
		}
		b.WriteString("\n")
	}
	if !wrote {
		b.WriteString("No backend returned probabilities.\n\n")
	}

	b.WriteString("## Labels\n\n")
	b.WriteString("| Labeler | Provisional | Articles | Pairs |\n| --- | --- | ---: | ---: |\n")
	for _, s := range data.DS.Sources {
		fmt.Fprintf(&b, "| %s | %t | %d | %d |\n", s.Labeler, s.Provisional, s.Articles, s.Pairs)
	}
	duplicates := 0
	for _, l := range data.DS.PairLabels {
		if l.Duplicate {
			duplicates++
		}
	}
	if duplicates > 0 {
		fmt.Fprintf(&b, "\n%d labelled pair(s) are same-event duplicates and are excluded from the relation metrics.\n", duplicates)
	}
	if len(data.DS.Warnings) > 0 {
		b.WriteString("\nData warnings:\n\n")
		for _, w := range data.DS.Warnings {
			fmt.Fprintf(&b, "- %s\n", w)
		}
	}
	b.WriteString("\n")

	b.WriteString("## Method and caveats\n\n")
	b.WriteString("- The eval sets are in `eval/data/`. The collector stores headline, short snippet, link, source and time only; never full text.\n")
	b.WriteString("- The gazetteer is a minimal matcher over GeoNames `cities15000` plus country names. M2·1 replaces it with the real gazetteer, so plot accuracy is bounded by candidate recall.\n")
	b.WriteString("- Claude's eval schemas ask for a self-reported confidence in [0,1]; it is a soft signal, not an API probability. Threshold selection uses jev only.\n")
	b.WriteString("- Claude cost is estimated from the price schedule in `config/models.yaml` (marked provisional); the API returns tokens, not money. jev cost comes from `usage.cost`.\n")
	b.WriteString("- Thresholds are model-specific. A jev version change requires a new eval run before using these values.\n")
	b.WriteString("- Labels marked provisional were produced by an agent, not a human. The full ~300 article and ~300 pair human labelling is a follow-up; see `eval/README.md`.\n")
	b.WriteString("- Pairs marked `duplicate` are same-event coverage by two outlets. Clustering handles those in production, so they are excluded from the relation metrics (the per-task detail shows how many were skipped).\n")
	b.WriteString("- jev's `confidence` is a normalized concentration, not P(selected); it is recorded as `mean_concentration` and never used as a gate. Gates use P(selected option).\n")
	b.WriteString("- The relation threshold is chosen from raw answers over all usable labelled pairs, including gold-unrelated pairs; an accepted false positive lowers accepted type accuracy.\n")
	b.WriteString("- The fallback projection has one record per labelled article. Errors, refusals, missing answers and missing ids count as fallback and stay in the denominator; it is evaluated at each candidate threshold.\n")
	b.WriteString("- Claude wire schemas are sanitized for the structured-outputs API; constraints are described on the wire and enforced by Go after decoding.\n")
	if data.Run.Mock() {
		b.WriteString("- **Mock run.** No API keys were present. The report exercises the real HTTP and SDK clients against fake endpoints; it measures nothing about either model.\n")
	}
	for _, n := range data.Notes {
		fmt.Fprintf(&b, "- %s\n", n)
	}
	return b.String()
}

// servedModels lists the dated snapshots the APIs actually served.
func servedModels(tasks []TaskResult) string {
	seen := make(map[string]map[string]struct{})
	for _, t := range tasks {
		for m := range t.ServedModels {
			if seen[t.Backend] == nil {
				seen[t.Backend] = make(map[string]struct{})
			}
			seen[t.Backend][m] = struct{}{}
		}
	}
	var parts []string
	for _, backend := range []string{"jev", "claude"} {
		var names []string
		for m := range seen[backend] {
			names = append(names, m)
		}
		sort.Strings(names)
		if len(names) > 0 {
			parts = append(parts, backend+" "+strings.Join(names, ", "))
		}
	}
	return strings.Join(parts, "; ")
}

func binN(bins []Bin) int {
	n := 0
	for _, b := range bins {
		n += b.N
	}
	return n
}

func writeBins(b *strings.Builder, bins []Bin) {
	b.WriteString("| bin | n | mean confidence | accuracy |\n| --- | ---: | ---: | ---: |\n")
	for _, bin := range bins {
		if bin.N == 0 {
			continue
		}
		fmt.Fprintf(b, "| %.2f–%.2f | %d | %.3f | %.3f |\n", bin.Lo, bin.Hi, bin.N, bin.MeanConf, bin.Accuracy)
	}
	b.WriteString("\n")
}

func thresholdSource(skipped bool) string {
	if skipped {
		return "plan starting value (provisional)"
	}
	return "selected from labelled set (provisional)"
}
