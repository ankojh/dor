package eval

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/config"
)

func reportFixture(t *testing.T, jevKind, claudeKind string) ReportData {
	t.Helper()
	dir := baseDataset(t)
	writeLines(t, dir, "articles.labels.jsonl", aLabel("a1", "agent:m", true))
	dup := pLabel("p1", "agent:m", true)
	dup.Duplicate = true
	writeLines(t, dir, "pairs.labels.jsonl", dup)
	ds, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	obs := concat(repeat("a", 10, 8, 0.85))
	bins, ece := Calibrate(obs)
	run := RunResult{
		JevKind: jevKind, ClaudeKind: claudeKind,
		Tasks: []TaskResult{
			{Task: "classify", Backend: "jev", Kind: jevKind, N: 10, Answered: 10, Accuracy: 0.8, ECE: ece, Bins: bins,
				Coverage: AccuracyAtCoverage(obs, ThresholdGrid()), Extra: map[string]float64{"accept_rate": 1}},
			{Task: "relate", Backend: "jev", Kind: jevKind, N: 5, Answered: 5, Extra: map[string]float64{"duplicate_pairs_skipped": 7}},
			{Task: "classify", Backend: "claude", Kind: claudeKind, N: 10, Answered: 10, CacheReadTokens: 900, CacheCreateTokens: 100},
		},
	}
	th := config.DefaultThresholds()
	sel := SelectThresholds(run, th, run.Mock())
	return ReportData{
		Run: run, Selections: sel, DS: ds,
		GeneratedAt: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC),
		Notes:       []string{"custom note about labels"},
	}
}

func writeAndRead(t *testing.T, data ReportData) (md string, js Report, dir string) {
	t.Helper()
	dir = t.TempDir()
	mdPath, jsonPath := filepath.Join(dir, "out", "report.md"), filepath.Join(dir, "out", "report.json")
	if err := WriteReport(mdPath, jsonPath, data); err != nil {
		t.Fatal(err)
	}
	rawMD, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatal(err)
	}
	rawJS, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawJS, &js); err != nil {
		t.Fatalf("report.json is not valid: %v", err)
	}
	return string(rawMD), js, dir
}

func TestMockReportMarkerIsFirstLine(t *testing.T) {
	for _, kinds := range [][2]string{{"mock", "mock"}, {"mock", "live"}, {"live", "mock"}} {
		md, js, _ := writeAndRead(t, reportFixture(t, kinds[0], kinds[1]))
		if first := strings.SplitN(md, "\n", 2)[0]; first != "MOCK BACKENDS — NOT A MEASUREMENT" {
			t.Errorf("%v: first line = %q", kinds, first)
		}
		if MockMarker != "MOCK BACKENDS — NOT A MEASUREMENT" {
			t.Error("marker text changed")
		}
		if !js.Mock || js.Marker != MockMarker {
			t.Errorf("%v: json mock=%v marker=%q", kinds, js.Mock, js.Marker)
		}
		if !strings.Contains(md, "not a measurement of jev or Claude") {
			t.Errorf("%v: the mock caveat is missing", kinds)
		}
		if !js.Thresholds.Selection.Skipped {
			t.Errorf("%v: mock selection must be skipped", kinds)
		}
	}
}

func TestLiveReportHasNoMarker(t *testing.T) {
	md, js, _ := writeAndRead(t, reportFixture(t, "live", "live"))
	if strings.Contains(md, "MOCK BACKENDS") || strings.Contains(md, "NOT A MEASUREMENT") {
		t.Error("a live report must not carry the mock marker")
	}
	if !strings.HasPrefix(md, "# Dor model eval — 2026-11-20 12:00 UTC") {
		t.Errorf("live report starts with %q", strings.SplitN(md, "\n", 2)[0])
	}
	if js.Mock || js.Marker != "" {
		t.Errorf("json mock=%v marker=%q", js.Mock, js.Marker)
	}
}

func TestReportJSONShape(t *testing.T) {
	_, js, dir := writeAndRead(t, reportFixture(t, "mock", "mock"))
	raw, _ := os.ReadFile(filepath.Join(dir, "out", "report.json"))
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"mock", "backends", "labels", "thresholds", "tasks", "generated_at"} {
		if _, ok := generic[key]; !ok {
			t.Errorf("report.json lacks %q", key)
		}
	}
	if js.Backends["jev"] != "mock" || js.Backends["claude"] != "mock" || js.GeneratedAt != "2026-11-20T12:00:00Z" {
		t.Errorf("backends=%v generated=%q", js.Backends, js.GeneratedAt)
	}
	if len(js.Tasks) != 3 || js.Tasks[0].Task != "classify" || js.Tasks[2].CacheReadTokens != 900 {
		t.Errorf("tasks = %+v", js.Tasks)
	}
	if js.Labels.Articles != 1 || js.Labels.Pairs != 1 || !js.Labels.Provisional || len(js.Labels.Sources) != 1 {
		t.Errorf("labels = %+v", js.Labels)
	}
	th := config.DefaultThresholds()
	want := map[string]float64{"classify.accept": th.Classify.Accept, "plot.primary": th.Plot.Primary, "plot.party": th.Plot.Party, "relate.accept": th.Relate.Accept, "match.strictness": th.Match.Strictness}
	for k, v := range want {
		if js.Thresholds.Applied[k] != v {
			t.Errorf("applied %s = %v, want %v", k, js.Thresholds.Applied[k], v)
		}
	}
	if !js.Thresholds.Provisional || len(js.Notes) != 1 {
		t.Errorf("thresholds=%+v notes=%v", js.Thresholds, js.Notes)
	}
}

func TestReportShowsDuplicatePairCount(t *testing.T) {
	md, js, _ := writeAndRead(t, reportFixture(t, "mock", "mock"))
	if !strings.Contains(md, "| relate | jev | duplicate_pairs_skipped | 7.000 |") {
		t.Errorf("markdown lacks the duplicate count:\n%s", md)
	}
	if js.Tasks[1].Extra["duplicate_pairs_skipped"] != 7 {
		t.Errorf("json extra = %v", js.Tasks[1].Extra)
	}
}

func TestReportSections(t *testing.T) {
	md, _, _ := writeAndRead(t, reportFixture(t, "mock", "mock"))
	for _, want := range []string{
		"## Results", "## Tokens and prompt cache", "| classify | claude | 10 | 0 | 10 |", "900 | 100 |",
		"## Thresholds", "Selection skipped: mock backends", "plan starting value (provisional)",
		"## Calibration", "## Accuracy vs coverage", "## Labels", "## Method and caveats", "custom note about labels",
		"**Mock run.**",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q", want)
		}
	}
	// Claude has no probabilities: ECE column is n/a.
	if !strings.Contains(md, "| classify | claude |") || !strings.Contains(md, "n/a") {
		t.Error("Claude's ECE must be n/a")
	}
}

func TestLiveReportRendersSelectionTable(t *testing.T) {
	data := reportFixture(t, "live", "live")
	data.Selections = Selections{
		Plot: Selection{Threshold: 0.6, Target: 0.9, Accepted: 10, Coverage: 0.5, Accuracy: 1, Met: true},
		Applied: func() config.Thresholds {
			th := config.DefaultThresholds()
			th.Plot.Primary = 0.6
			return th
		}(),
		FallbackRate: 0.1, FallbackMet: true,
	}
	md, js, _ := writeAndRead(t, data)
	for _, want := range []string{"| place pick | 0.90 | 0.60 | 10 |", "Projected Claude fallback rate: **10.0%**", "selected from labelled set"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q", want)
		}
	}
	if js.Thresholds.Applied["plot.primary"] != 0.6 || js.Thresholds.Selection.Skipped {
		t.Errorf("thresholds = %+v", js.Thresholds)
	}
}

func TestWriteReportWithoutJSONPath(t *testing.T) {
	dir := t.TempDir()
	if err := WriteReport(filepath.Join(dir, "r.md"), "", reportFixture(t, "mock", "mock")); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "r.md" {
		t.Errorf("files = %v", entries)
	}
	// A zero GeneratedAt is filled in.
	data := reportFixture(t, "mock", "mock")
	data.GeneratedAt = time.Time{}
	md, js, _ := writeAndRead(t, data)
	if !strings.Contains(md, "# Dor model eval — 20") || js.GeneratedAt == "" {
		t.Error("generated time not filled")
	}
}

func TestMockDetection(t *testing.T) {
	cases := map[[2]string]bool{{"mock", "mock"}: true, {"mock", "live"}: true, {"live", "mock"}: true, {"live", "live"}: false, {"unavailable", "live"}: false}
	for k, want := range cases {
		if got := (RunResult{JevKind: k[0], ClaudeKind: k[1]}).Mock(); got != want {
			t.Errorf("%v: Mock = %v", k, got)
		}
	}
	r := RunResult{Tasks: []TaskResult{{Task: "plot", Backend: "jev"}}}
	if _, ok := r.Find("plot", "jev"); !ok {
		t.Error("Find")
	}
	if _, ok := r.Find("plot", "claude"); ok {
		t.Error("Find must match both fields")
	}
}
