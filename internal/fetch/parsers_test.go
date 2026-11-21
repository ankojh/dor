package fetch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func parseFeedFixture(t *testing.T, name string) (*ParsedFeed, error) {
	t.Helper()
	return ParseFeed(strings.NewReader(string(fixture(t, name))), "https://example.test/feed")
}

func TestParseFeedFixtures(t *testing.T) {
	tests := []struct {
		file      string
		title     string
		lang      string
		items     int
		firstLink string
		firstPub  time.Time
	}{
		{"rss_bbc.xml", "BBC News", "en", 5, "https://www.bbc.co.uk/news/articles/c6808ym10vzgo?at_medium=RSS&at_campaign=rss", time.Date(2026, 11, 20, 22, 42, 47, 0, time.UTC)},
		{"atom_verge.xml", "The Verge", "en", 5, "https://www.theverge.com/streaming/1039096/youtube-social-media-addiction-school-districts-settlement", time.Date(2026, 11, 20, 20, 43, 52, 0, time.UTC)},
		{"rdf_dw.xml", "Deutsche Welle", "", 5, "https://www.dw.com/en/brazil-ex-president-jair-bolsonaro-taken-to-hospital-over-abdominal-pain/a-80007648?maca=en-rss-en-all-1573-rdf", time.Date(2026, 11, 21, 3, 16, 0, 0, time.UTC)},
		{"rss_utf16_vietnam.xml", "home - Vietnamenws.vn RSS", "en", 1, "https://vietnamnews.vn/", time.Date(2026, 11, 20, 5, 53, 59, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			feed, err := parseFeedFixture(t, tt.file)
			if err != nil {
				t.Fatalf("ParseFeed: %v", err)
			}
			if feed.Title != tt.title || feed.Lang != tt.lang {
				t.Errorf("title/lang = %q/%q, want %q/%q", feed.Title, feed.Lang, tt.title, tt.lang)
			}
			if len(feed.Items) != tt.items {
				t.Fatalf("items = %d, want %d", len(feed.Items), tt.items)
			}
			first := feed.Items[0]
			if first.Link != tt.firstLink {
				t.Errorf("first link = %q, want %q", first.Link, tt.firstLink)
			}
			if !first.Published.Equal(tt.firstPub) {
				t.Errorf("first published = %v, want %v", first.Published, tt.firstPub)
			}
			for i, item := range feed.Items {
				if item.Link == "" || !strings.HasPrefix(item.Link, "http") {
					t.Errorf("item %d has no absolute link: %+v", i, item)
				}
				if strings.ContainsAny(item.Title, "<>") || strings.ContainsAny(item.Summary, "<>") {
					t.Errorf("item %d keeps markup: %+v", i, item)
				}
				if n := len([]rune(item.Summary)); n > 401 {
					t.Errorf("item %d summary has %d runes", i, n)
				}
			}
		})
	}
}

func TestParseFeedTitlesAndSummaries(t *testing.T) {
	bbc, err := parseFeedFixture(t, "rss_bbc.xml")
	if err != nil {
		t.Fatal(err)
	}
	if got := bbc.Items[0].Title; got != "Tech stock sell-off drags markets down as Anthropic shares slide" {
		t.Errorf("bbc title = %q", got)
	}
	// The UTF-16 item has no title; the empty slug of "/" yields no invented one.
	vn, err := parseFeedFixture(t, "rss_utf16_vietnam.xml")
	if err != nil {
		t.Fatal(err)
	}
	if vn.Items[0].Title != "" {
		t.Errorf("utf16 title = %q, want empty", vn.Items[0].Title)
	}
}

func TestParseFeedErrors(t *testing.T) {
	for _, name := range []string{"rss_truncated.xml", "not_a_feed.html"} {
		t.Run(name, func(t *testing.T) {
			feed, err := parseFeedFixture(t, name)
			if err == nil {
				t.Fatalf("expected an error, got %d items", len(feed.Items))
			}
		})
	}
	t.Run("unknown root", func(t *testing.T) {
		if _, err := ParseFeed(strings.NewReader(`<?xml version="1.0"?><html><body/></html>`), ""); err == nil || !strings.Contains(err.Error(), "not a feed") {
			t.Fatalf("err = %v, want not-a-feed", err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		if _, err := ParseFeed(strings.NewReader(""), ""); err == nil {
			t.Fatal("empty body must be an error")
		}
	})
}

func TestParseFeedRelativeLinksAndFallbacks(t *testing.T) {
	const doc = `<?xml version="1.0"?><rss version="2.0"><channel><title>T</title><language>en-GB</language>
<item><title><![CDATA[<b>Bold</b> &amp; plain]]></title><link>/news/one</link><pubDate>Mon, 02 Jan 2006 15:04:05 GMT</pubDate></item>
<item><link>mailto:x</link></item>
<item><guid>https://example.test/a-long-story-slug-here</guid></item>
</channel></rss>`
	feed, err := ParseFeed(strings.NewReader(doc), "https://example.test/feed.xml")
	if err != nil {
		t.Fatal(err)
	}
	if feed.Lang != "en" {
		t.Errorf("lang = %q", feed.Lang)
	}
	if len(feed.Items) != 2 {
		t.Fatalf("items = %d, want 2 (link-less item dropped): %+v", len(feed.Items), feed.Items)
	}
	if feed.Items[0].Link != "https://example.test/news/one" || feed.Items[0].Title != "Bold & plain" {
		t.Errorf("item 0 = %+v", feed.Items[0])
	}
	if feed.Items[1].Title != "A long story slug here" {
		t.Errorf("slug title = %q", feed.Items[1].Title)
	}
}

func TestTitleFromURL(t *testing.T) {
	tests := []struct{ url, want string }{
		{"https://www.theverge.com/streaming/1039096/youtube-social-media-addiction", "Youtube social media addiction"},
		{"https://example.com/news/some_story_title.html", "Some story title"},
		{"https://example.com/a/b/c/", ""},
		{"https://example.com/article/74496311", ""},
		{"https://example.com/", ""},
		{"https://example.com/story/2641453/fcc-rejects-jui-fs-reserved-seat-claim", "Fcc rejects jui fs reserved seat claim"},
		{"::bad", ""},
	}
	for _, tt := range tests {
		if got := TitleFromURL(tt.url); got != tt.want {
			t.Errorf("TitleFromURL(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
	long := "https://example.com/" + strings.Repeat("word-", 80)
	if n := len([]rune(TitleFromURL(long))); n > 181 {
		t.Errorf("title not truncated: %d runes", n)
	}
}

func TestParseGDELTGKGFixture(t *testing.T) {
	items, err := ParseGDELTGKG(strings.NewReader(string(fixture(t, "gdelt_gkg.csv"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 4 {
		t.Fatalf("items = %d, want 4", len(items))
	}
	byDomain := map[string]GDELTItem{}
	for _, it := range items {
		byDomain[it.Domain] = it
		if it.URL == "" || it.RecordID == "" {
			t.Errorf("missing url/record id: %+v", it)
		}
		if want := time.Date(2026, 11, 21, 5, 0, 0, 0, time.UTC); !it.Published.Equal(want) {
			t.Errorf("%s published = %v", it.Domain, it.Published)
		}
		seen := map[string]bool{}
		for _, th := range it.Themes {
			if seen[th] || strings.Contains(th, ",") {
				t.Errorf("%s theme %q duplicated or has an offset", it.Domain, th)
			}
			seen[th] = true
		}
		if len(it.Themes) > 32 {
			t.Errorf("%s has %d themes", it.Domain, len(it.Themes))
		}
	}
	tribune := byDomain["tribune.com.pk"]
	if !tribune.Located || tribune.Country != "PK" || tribune.Place != "Islamabad, Islamabad, Pakistan" || tribune.Precision != PrecisionCity {
		t.Errorf("tribune location = %+v", tribune)
	}
	if tribune.Lat != 33.7 || tribune.Lon != 73.1667 {
		t.Errorf("tribune coords = %v,%v", tribune.Lat, tribune.Lon)
	}
	if !containsString(tribune.Themes, "ELECTION") {
		t.Errorf("tribune themes lack ELECTION: %v", tribune.Themes)
	}
	if jp := byDomain["jpost.com"]; !jp.Located || jp.Precision != PrecisionCountry || jp.Place != "Portugal" {
		t.Errorf("jpost should be located at country level: %+v", jp)
	}
	wlwt := byDomain["wlwt.com"]
	if !wlwt.Located || wlwt.Place != "Waynesville, Ohio, United States" || wlwt.Country != "US" {
		t.Errorf("wlwt location = %+v", wlwt)
	}
	if len(wlwt.Themes) != 0 {
		t.Errorf("wlwt should have no themes: %v", wlwt.Themes)
	}
}

func TestParseGDELTGKGEdgeCases(t *testing.T) {
	row := func(url, domain, locations string) string {
		cols := make([]string, 11)
		cols[0], cols[1], cols[3], cols[4], cols[10] = "id-1", "20261121050000", domain, url, locations
		return strings.Join(cols, "\t")
	}
	input := strings.Join([]string{
		"too\tshort",
		row("", "x.com", ""),
		row("https://news.example.org/a/long-story-slug-goes-here", "", "1#Country#NO#NO#60.0#10.0#NO;4#Oslo, Norway#NO#NO12#59.9#10.7#-1;3#Bad#XX#XX#0#0#1"),
	}, "\n")
	items, err := ParseGDELTGKG(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	it := items[0]
	if it.Domain != "news.example.org" {
		t.Errorf("domain not derived from URL: %q", it.Domain)
	}
	if it.Place != "Oslo, Norway" || it.Precision != PrecisionCity {
		t.Errorf("most precise valid location expected, got %+v", it)
	}
}

func TestParseGDELTExportFixture(t *testing.T) {
	items, err := ParseGDELTExport(strings.NewReader(string(fixture(t, "gdelt_export.csv"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 20 {
		t.Fatalf("items = %d, want 20", len(items))
	}
	first := items[0]
	if first.RecordID != "1334316871" || first.Domain != "foxwilmington.com" || first.EventCode != "172" {
		t.Errorf("first = %+v", first)
	}
	if want := []string{"MILITARY", "CAMEO_ROOT_17", "CAMEO_172"}; !equalStrings(first.Themes, want) {
		t.Errorf("themes = %v, want %v", first.Themes, want)
	}
	if first.Actor1 != "Brunswick" || first.Actor2 != "Business" || first.Located {
		t.Errorf("first actor/location = %+v", first)
	}
	second := items[1]
	if !second.Located || second.Lat != 24.75 || second.Lon != 90.4 || second.Precision != PrecisionCity || second.Actor2 != "Mymensingh" {
		t.Errorf("second = %+v", second)
	}
	if second.Domain != "www.thedailystar.net" {
		t.Errorf("export domain = %q", second.Domain)
	}
}

func TestGDELTLastFileAndUnzip(t *testing.T) {
	body := []byte("100 aaa http://data.gdeltproject.org/gdeltv2/20261121050000.export.CSV.zip\n" +
		"200 bbb http://data.gdeltproject.org/gdeltv2/20261121050000.mentions.CSV.zip\n" +
		"300 ccc http://data.gdeltproject.org/gdeltv2/20261121050000.gkg.csv.zip\n")
	got, err := GDELTLastFile(body, "gkg")
	if err != nil || got != "https://data.gdeltproject.org/gdeltv2/20261121050000.gkg.csv.zip" {
		t.Errorf("gkg = %q, %v", got, err)
	}
	got, err = GDELTLastFile(body, "export")
	if err != nil || !strings.HasSuffix(got, ".export.CSV.zip") || !strings.HasPrefix(got, "https://") {
		t.Errorf("export = %q, %v", got, err)
	}
	if _, err := GDELTLastFile([]byte("nothing here"), "gkg"); err == nil {
		t.Error("missing file must be an error")
	}
	if _, err := UnzipCSV([]byte("not a zip"), 0); err == nil {
		t.Error("bad zip must be an error")
	}
}

func TestDomainAllowed(t *testing.T) {
	allow := []string{"bbc.co.uk", " Reuters.com ", ""}
	tests := []struct {
		domain string
		want   bool
	}{
		{"bbc.co.uk", true},
		{"www.bbc.co.uk", true},
		{"WWW.REUTERS.COM", true},
		{"notbbc.co.uk", false},
		{"bbc.co.uk.evil.com", false},
		{"", false},
		{"example.com", false},
	}
	for _, tt := range tests {
		if got := DomainAllowed(tt.domain, allow); got != tt.want {
			t.Errorf("DomainAllowed(%q) = %v, want %v", tt.domain, got, tt.want)
		}
	}
}

func TestParseUSGSFixture(t *testing.T) {
	items, err := ParseUSGS(strings.NewReader(string(fixture(t, "usgs_earthquakes.json"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("items = %d", len(items))
	}
	big := items[2]
	if big.ID != "us6000uchd" || big.Hazard != "earthquake" || !big.Located || big.Precision != PrecisionPoint {
		t.Errorf("big = %+v", big)
	}
	if big.Magnitude != 5 || big.MagnitudeUnit != "mb" || big.Lat != -10.913 || big.Lon != -76.0308 {
		t.Errorf("big magnitude/coords = %+v", big)
	}
	if big.Severity != 0.5 { // (5-2)/6
		t.Errorf("big severity = %v", big.Severity)
	}
	if items[0].Severity >= big.Severity {
		t.Errorf("a M1.3 quake must rank below M5: %v vs %v", items[0].Severity, big.Severity)
	}
	if !big.Published.Equal(time.UnixMilli(big.Published.UnixMilli()).UTC()) || big.Published.IsZero() || big.Updated.Before(big.Published) {
		t.Errorf("times = %v / %v", big.Published, big.Updated)
	}
	if !equalStrings(big.Themes, []string{"EARTHQUAKE"}) || big.URL == "" || big.Title == "" {
		t.Errorf("themes/url/title = %+v", big)
	}
}

func TestParseUSGSPagerAlertAndErrors(t *testing.T) {
	items, err := ParseUSGS(strings.NewReader(`{"features":[{"id":"a","properties":{"mag":6.1,"alert":"red","place":"Somewhere","title":""},"geometry":{"coordinates":[0,0]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if items[0].Severity != 1 || items[0].Located {
		t.Errorf("pager alert/0,0 handling = %+v", items[0])
	}
	if items[0].Title != "Somewhere · M6.1" {
		t.Errorf("fallback title = %q", items[0].Title)
	}
	if _, err := ParseUSGS(strings.NewReader("<html>")); err == nil {
		t.Error("non-JSON must be an error")
	}
}

func TestParseGDACSFixture(t *testing.T) {
	items, err := ParseGDACS(strings.NewReader(string(fixture(t, "gdacs_alerts.xml"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("items = %d", len(items))
	}
	first := items[0]
	if first.ID != "1578488" || first.Hazard != "earthquake" || !first.Located || first.Lat != 58.5457 || first.Lon != -31.857 {
		t.Errorf("first = %+v", first)
	}
	if !equalStrings(first.Themes, []string{"GDACS_EQ", "ALERT_GREEN"}) {
		t.Errorf("themes = %v", first.Themes)
	}
	if first.Magnitude != 4.9 || first.MagnitudeUnit != "M" {
		t.Errorf("magnitude = %v %s", first.Magnitude, first.MagnitudeUnit)
	}
	if items[2].Country != "CHL" {
		t.Errorf("iso3 country = %q", items[2].Country)
	}
	if first.Published.IsZero() {
		t.Error("published missing")
	}
	if _, err := ParseGDACS(strings.NewReader("<rss><channel>")); err == nil {
		t.Error("truncated GDACS must be an error")
	}
}

func TestParseEONETFixture(t *testing.T) {
	items, err := ParseEONET(strings.NewReader(string(fixture(t, "eonet_events.json"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("items = %d", len(items))
	}
	first := items[0]
	if first.ID != "EONET_26568" || first.Hazard != "wildfire" || !equalStrings(first.Themes, []string{"EONET_WILDFIRES"}) {
		t.Errorf("first = %+v", first)
	}
	if first.Magnitude != 500 || first.MagnitudeUnit != "acres" || first.Severity <= 0 || first.Severity >= 1 {
		t.Errorf("magnitude/severity = %+v", first)
	}
	if !first.Located || first.Lat != 36.36301667 || first.Lon != -82.7985 {
		t.Errorf("coords = %v,%v", first.Lat, first.Lon)
	}
	if first.URL != "https://eonet.gsfc.nasa.gov/api/v3/events/EONET_26568" {
		t.Errorf("url = %q", first.URL)
	}
	if first.Published.IsZero() {
		t.Error("published from geometry date missing")
	}
}

func TestParseEONETLatestGeometryAndClosed(t *testing.T) {
	const doc = `{"events":[{"id":"E1","title":"Storm","link":"","closed":"2026-01-02T00:00:00Z",
"categories":[{"id":"severeStorms","title":"Severe Storms"}],"sources":[{"id":"x","url":"https://src.example/e1"}],
"geometry":[{"date":"2026-01-01T00:00:00Z","coordinates":[10,20],"magnitudeValue":60,"magnitudeUnit":"kts"},
{"date":"2026-01-02T00:00:00Z","coordinates":[11,21],"magnitudeValue":134,"magnitudeUnit":"knots"}]}]}`
	items, err := ParseEONET(strings.NewReader(doc))
	if err != nil {
		t.Fatal(err)
	}
	it := items[0]
	if it.Lat != 21 || it.Lon != 11 {
		t.Errorf("latest point expected, got %v,%v", it.Lat, it.Lon)
	}
	if it.URL != "https://src.example/e1" || it.Hazard != "storm" || it.Severity != 1 {
		t.Errorf("item = %+v", it)
	}
	if !containsString(it.Themes, "CLOSED") {
		t.Errorf("closed theme missing: %v", it.Themes)
	}
}

func TestParseReliefWeb(t *testing.T) {
	items, err := ParseReliefWeb(strings.NewReader(string(fixture(t, "reliefweb_reports.json"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d", len(items))
	}
	first := items[0]
	if first.ID != "1234567" || first.Hazard != "flood" || first.Country != "NGA" || first.Precision != PrecisionCountry {
		t.Errorf("first = %+v", first)
	}
	if !first.Located || first.Lat != 9.082 || first.Lon != 8.6753 {
		t.Errorf("coords = %v,%v", first.Lat, first.Lon)
	}
	if !equalStrings(first.Themes, []string{"RELIEFWEB_FLOOD"}) || first.Summary != "Nigeria · OCHA · Flood" {
		t.Errorf("themes/summary = %v / %q", first.Themes, first.Summary)
	}
	if want := time.Date(2026, 11, 20, 9, 15, 0, 0, time.UTC); !first.Published.Equal(want) {
		t.Errorf("published = %v", first.Published)
	}
	if items[1].URL != "https://reliefweb.int/node/7654321" {
		t.Errorf("missing url must fall back to node id, got %q", items[1].URL)
	}
}

// The real anonymous-access response is an error object, not a report list.
// It must never yield items; the HTTP client already turns the 403 itself into
// a non-retryable StatusError (see TestClientReliefWeb403).
func TestParseReliefWeb403BodyYieldsNothing(t *testing.T) {
	items, err := ParseReliefWeb(strings.NewReader(string(fixture(t, "reliefweb_403.json"))))
	if err == nil && len(items) != 0 {
		t.Fatalf("403 body produced items: %+v", items)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
