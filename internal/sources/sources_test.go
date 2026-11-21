package sources

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const header = "version: 1\nreviewed_at: \"2026-11-20\"\nsources:\n"

func entry(overrides map[string]string) string {
	fields := map[string]string{
		"id":         "bbc-world",
		"kind":       "rss",
		"url":        `"https://feeds.bbci.co.uk/news/world/rss.xml"`,
		"country":    "GB",
		"region":     "global",
		"lang":       "en",
		"interval_s": "120",
		"enabled":    "true",
		"terms_ok":   "true",
		"terms_note": `"public feed"`,
	}
	for k, v := range overrides {
		fields[k] = v
	}
	order := []string{"id", "kind", "url", "country", "region", "lang", "interval_s", "enabled", "terms_ok", "terms_note"}
	var b strings.Builder
	for i, k := range order {
		if v, ok := fields[k]; ok && v != "-" {
			prefix := "    "
			if i == 0 {
				prefix = "  - "
			}
			b.WriteString(prefix + k + ": " + v + "\n")
		}
	}
	return b.String()
}

func TestParseValid(t *testing.T) {
	doc := header + entry(nil) + entry(map[string]string{
		"id": "usgs-hour", "kind": "usgs", "url": `"https://earthquake.usgs.gov/feed.geojson"`,
		"country": "US", "region": "-", "lang": "-", "interval_s": "300",
	}) + entry(map[string]string{"id": "off-feed", "enabled": "false", "terms_ok": "false", "terms_note": "-"})
	f, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.All()) != 3 {
		t.Fatalf("sources = %d", len(f.All()))
	}
	if rss := f.ForKind(KindRSS); len(rss) != 1 || rss[0].ID != "bbc-world" {
		t.Errorf("ForKind(rss) = %+v (disabled sources must be excluded)", rss)
	}
	if usgs := f.ForKind(KindUSGS); len(usgs) != 1 {
		t.Errorf("ForKind(usgs) = %+v", usgs)
	}
	c := f.Count()
	if c.Total != 3 || c.Enabled != 2 || c.Disabled != 1 || c.ByKind["rss"] != 2 || c.ByRegion["global"] != 1 {
		t.Errorf("counts = %+v", c)
	}
	if got := f.Sources[0].Interval(); got != 2*time.Minute {
		t.Errorf("interval = %v", got)
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"duplicate id", header + entry(nil) + entry(nil), "duplicate id"},
		{"http url", header + entry(map[string]string{"url": `"http://feeds.bbci.co.uk/rss.xml"`}), "https"},
		{"relative url", header + entry(map[string]string{"url": `"/rss.xml"`}), "https"},
		{"enabled without terms_ok", header + entry(map[string]string{"terms_ok": "false"}), "terms_ok"},
		{"enabled without terms_note", header + entry(map[string]string{"terms_note": "-"}), "terms_note"},
		{"missing country", header + entry(map[string]string{"country": "-"}), "country"},
		{"missing region for rss", header + entry(map[string]string{"region": "-"}), "region"},
		{"non-English lang", header + entry(map[string]string{"lang": "de"}), "English"},
		{"missing lang", header + entry(map[string]string{"lang": "-"}), "English"},
		{"unknown kind", header + entry(map[string]string{"kind": "twitter"}), "unknown kind"},
		{"missing id", header + entry(map[string]string{"id": `""`}), "id is required"},
		{"interval too small", header + entry(map[string]string{"interval_s": "5"}), "interval_s"},
		{"interval missing", header + entry(map[string]string{"interval_s": "-"}), "interval_s"},
		{"empty list", "version: 1\nsources: []\n", "empty"},
		{"wrong version", strings.Replace(header, "version: 1", "version: 2", 1) + entry(nil), "version"},
		{"bad yaml", "version: [1\n", "parse yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Parse([]byte(tt.doc))
			if err == nil {
				t.Fatalf("expected an error, loaded %+v", f)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestDisabledSourceNeedsNoTermsReview(t *testing.T) {
	doc := header + entry(map[string]string{"enabled": "false", "terms_ok": "false", "terms_note": "-"})
	f, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("a disabled source with no terms review is allowed: %v", err)
	}
	if len(f.ForKind(KindRSS)) != 0 {
		t.Error("disabled source must not be returned")
	}
}

func TestLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.yaml")
	if err := os.WriteFile(path, []byte(header+entry(nil)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("missing file must be an error")
	}
}

func TestIntervalDefaults(t *testing.T) {
	tests := map[Kind]time.Duration{
		KindRSS: 3 * time.Minute, KindGDELT: 15 * time.Minute, KindReliefWeb: time.Hour,
		KindUSGS: 5 * time.Minute, KindGDACS: 5 * time.Minute, KindEONET: 5 * time.Minute,
	}
	for kind, want := range tests {
		if got := (Source{Kind: kind}).Interval(); got != want {
			t.Errorf("%s default interval = %v, want %v", kind, got, want)
		}
	}
	if got := (Source{Kind: KindRSS, IntervalS: 90}).Interval(); got != 90*time.Second {
		t.Errorf("explicit interval = %v", got)
	}
}

func TestShippedSeedList(t *testing.T) {
	f, err := Load(filepath.Join("..", "..", "deploy", "sources", "sources.yaml"))
	if err != nil {
		t.Fatalf("the shipped seed list must load and validate: %v", err)
	}
	var rss, enabledRSS int
	for _, s := range f.All() {
		if s.Kind != KindRSS {
			continue
		}
		rss++
		if s.Enabled {
			enabledRSS++
		}
	}
	if rss < 150 || rss > 300 {
		t.Errorf("rss entries = %d, want 150..300", rss)
	}
	if enabledRSS < 150 {
		t.Errorf("enabled rss = %d, want >= 150", enabledRSS)
	}
	if got := len(f.ForKind(KindRSS)); got != enabledRSS {
		t.Errorf("ForKind(rss) = %d, enabled = %d", got, enabledRSS)
	}
	// Every RSS seed uses the 60 s politeness floor unless a publisher
	// documents a slower minimum; there are no documented exceptions, so an
	// unannotated seed above 60 fails this test.
	for _, s := range f.All() {
		if s.Kind == KindRSS && s.IntervalS != 60 {
			t.Errorf("%s: rss interval_s = %d, want the 60 s floor", s.ID, s.IntervalS)
		}
	}
	for _, kind := range []Kind{KindGDELT, KindUSGS, KindGDACS, KindEONET} {
		if len(f.ForKind(kind)) == 0 {
			t.Errorf("no enabled %s source in the seed list", kind)
		}
	}
	for _, s := range f.ForKind(KindReliefWeb) {
		t.Errorf("reliefweb must ship disabled (no approved appname), %s is enabled", s.ID)
	}
	regions := f.Count().ByRegion
	if len(regions) < 3 {
		t.Errorf("rss regions = %v, want a spread", regions)
	}
}
