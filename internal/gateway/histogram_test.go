package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/store"
)

// histogramStore is a StoryStore whose only interesting method is
// ArticleHistogram. The detail/search methods exist so the gateway's full
// interface is satisfied without a database.
type histogramStore struct {
	histogram store.ArticleHistogram
	err       error
	gotStart  time.Time
	gotEnd    time.Time
	gotBins   int
	calls     int
}

func (f *histogramStore) GetStoryDetail(context.Context, string, int) (store.StoryDetail, error) {
	return store.StoryDetail{}, store.ErrStoryNotFound
}

func (f *histogramStore) GetArticlesByIDs(context.Context, []string) ([]store.StoryArticle, error) {
	return nil, nil
}

func (f *histogramStore) GetSources(context.Context, []string) ([]store.SourceInfo, error) {
	return nil, nil
}

func (f *histogramStore) SearchStories(context.Context, string, int) ([]store.StorySummary, error) {
	return nil, nil
}

func (f *histogramStore) ArticleHistogram(
	_ context.Context,
	start, end time.Time,
	bins int,
) (store.ArticleHistogram, error) {
	f.calls++
	f.gotStart, f.gotEnd, f.gotBins = start, end, bins
	if f.err != nil {
		return store.ArticleHistogram{}, f.err
	}
	out := f.histogram
	out.Start, out.End = start, end
	if len(out.Bins) != bins {
		out.Bins = make([]int, bins)
	}
	out.Total = 0
	for _, count := range out.Bins {
		out.Total += count
	}
	return out, nil
}

func histogramServer(history StoryStore) *Server {
	return &Server{
		cfg: Config{Store: history},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func getHistogram(t *testing.T, s *Server, query string) (int, histogramJSON) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleHistogram(rec, httptest.NewRequest(http.MethodGet, "/api/histogram"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body histogramJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode histogram response: %v", err)
	}
	return rec.Code, body
}

func knownHistogram(bins int) store.ArticleHistogram {
	counts := make([]int, bins)
	for i := range counts {
		counts[i] = i % 4
	}
	return store.ArticleHistogram{Bins: counts, Bucket: time.Hour}
}

func TestHandleHistogramServesBoundedBins(t *testing.T) {
	history := &histogramStore{histogram: knownHistogram(defaultHistogramBins)}
	s := histogramServer(history)
	before := time.Now().UTC()

	_, body := getHistogram(t, s, "?hours=24&bins=36")

	after := time.Now().UTC()
	if !body.Available {
		t.Fatalf("available = false, error = %q", body.Error)
	}
	if body.Hours != 24 {
		t.Errorf("hours = %d, want 24", body.Hours)
	}
	if len(body.Bins) != 36 {
		t.Fatalf("len(bins) = %d, want 36", len(body.Bins))
	}
	wantTotal := 0
	for i := range body.Bins {
		wantTotal += i % 4
	}
	if body.Count != wantTotal {
		t.Errorf("count = %d, want %d", body.Count, wantTotal)
	}
	if body.Count == 0 {
		t.Error("fixture produced an empty histogram")
	}
	start := time.Unix(body.Start, 0)
	end := time.Unix(body.End, 0)
	if end.Sub(start) != 24*time.Hour {
		t.Errorf("window = %s, want 24h", end.Sub(start))
	}
	if end.Before(before.Add(-time.Second)) || end.After(after.Add(time.Second)) {
		t.Errorf("end = %s, want between %s and %s", end, before, after)
	}
	if history.calls != 1 {
		t.Fatalf("ArticleHistogram calls = %d, want 1", history.calls)
	}
	if history.gotBins != 36 {
		t.Errorf("store bins = %d, want 36", history.gotBins)
	}
	// The handler keeps sub-second precision for the store call while the
	// response carries whole seconds, so compare the truncated instants.
	if !time.Unix(history.gotStart.Unix(), 0).Equal(start) ||
		!time.Unix(history.gotEnd.Unix(), 0).Equal(end) {
		t.Errorf("store window = [%s, %s), response = [%s, %s)",
			history.gotStart, history.gotEnd, start, end)
	}
}

func TestHandleHistogramHonorsHoursAndBins(t *testing.T) {
	history := &histogramStore{histogram: knownHistogram(10)}
	s := histogramServer(history)

	_, body := getHistogram(t, s, "?hours=12&bins=10")

	if !body.Available {
		t.Fatalf("available = false, error = %q", body.Error)
	}
	if body.Hours != 12 {
		t.Errorf("hours = %d, want 12", body.Hours)
	}
	if len(body.Bins) != 10 {
		t.Errorf("len(bins) = %d, want 10", len(body.Bins))
	}
	if got := time.Unix(body.End, 0).Sub(time.Unix(body.Start, 0)); got != 12*time.Hour {
		t.Errorf("window = %s, want 12h", got)
	}
	if history.gotBins != 10 {
		t.Errorf("store bins = %d, want 10", history.gotBins)
	}
	if got := history.gotEnd.Sub(history.gotStart); got != 12*time.Hour {
		t.Errorf("store window = %s, want 12h", got)
	}
}

func TestHandleHistogramClampsBadParameters(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantHours int
		wantBins  int
	}{
		{name: "zero hours", query: "?hours=0", wantHours: 24, wantBins: 36},
		{name: "huge hours", query: "?hours=999", wantHours: 24, wantBins: 36},
		{name: "negative hours", query: "?hours=-24", wantHours: 24, wantBins: 36},
		{name: "non-numeric hours", query: "?hours=all", wantHours: 24, wantBins: 36},
		{name: "zero bins", query: "?bins=0", wantHours: 24, wantBins: 36},
		{name: "huge bins", query: "?bins=999", wantHours: 24, wantBins: 36},
		{name: "negative bins", query: "?bins=-8", wantHours: 24, wantBins: 36},
		{name: "non-numeric bins", query: "?bins=lots", wantHours: 24, wantBins: 36},
		{name: "both huge", query: "?hours=999&bins=999", wantHours: 24, wantBins: 36},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			history := &histogramStore{histogram: knownHistogram(tc.wantBins)}
			s := histogramServer(history)
			_, body := getHistogram(t, s, tc.query)
			if body.Hours != tc.wantHours {
				t.Errorf("hours = %d, want %d", body.Hours, tc.wantHours)
			}
			if len(body.Bins) != tc.wantBins {
				t.Errorf("len(bins) = %d, want %d", len(body.Bins), tc.wantBins)
			}
			if history.gotBins != tc.wantBins {
				t.Errorf("store bins = %d, want %d", history.gotBins, tc.wantBins)
			}
			if !body.Available {
				t.Errorf("available = false, error = %q", body.Error)
			}
		})
	}
}

func TestHandleHistogramStoreErrorDegrades(t *testing.T) {
	history := &histogramStore{err: errors.New("database unavailable")}
	s := histogramServer(history)

	code, body := getHistogram(t, s, "")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body.Available {
		t.Error("available = true after store error")
	}
	if body.Error == "" {
		t.Error("error = empty after store error")
	}
	if body.Count != 0 {
		t.Errorf("count = %d, want 0", body.Count)
	}
	if len(body.Bins) != defaultHistogramBins {
		t.Errorf("len(bins) = %d, want %d", len(body.Bins), defaultHistogramBins)
	}
	for i, count := range body.Bins {
		if count != 0 {
			t.Errorf("bins[%d] = %d, want 0", i, count)
		}
	}
}

func TestHandleHistogramNilStoreDegrades(t *testing.T) {
	s := histogramServer(nil)

	code, body := getHistogram(t, s, "?hours=24&bins=36")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body.Available {
		t.Error("available = true with a nil store")
	}
	if body.Error == "" {
		t.Error("error = empty with a nil store")
	}
	if body.Hours != 24 {
		t.Errorf("hours = %d, want 24", body.Hours)
	}
	if len(body.Bins) != 36 {
		t.Errorf("len(bins) = %d, want 36", len(body.Bins))
	}
	if body.Count != 0 {
		t.Errorf("count = %d, want 0", body.Count)
	}
}
