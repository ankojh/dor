package agents

import (
	"testing"
	"time"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

// A source that reports local time as UTC can put PublishedAt hours ahead of
// the moment the fetcher saw it. The normalizer clamps once at ingest so the
// store, wire row, story panel and worker LAG agree on a real past time.
func TestNormalizerClampsFuturePublishAtIngest(t *testing.T) {
	now := time.Date(2026, 11, 22, 14, 58, 0, 0, time.UTC)
	fetched := now.Add(-5 * time.Minute).Unix()
	published := now.Add(8 * time.Hour).Unix()

	cases := []struct {
		name       string
		published  int64
		fetched    int64
		want       int64
		wantRecord int64
	}{
		{
			name:       "future publish clamps to fetch time",
			published:  published,
			fetched:    fetched,
			want:       fetched,
			wantRecord: fetched,
		},
		{
			name:       "future publish without fetch time clamps to now",
			published:  published,
			fetched:    0,
			want:       now.Unix(),
			wantRecord: now.Unix(),
		},
		{
			name:       "a past publish time is left alone",
			published:  fetched - 600,
			fetched:    fetched,
			want:       fetched - 600,
			wantRecord: fetched - 600,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := &normalizer{cfg: NormalizerConfig{Now: func() time.Time { return now }}}
			raw := &dorv1.RawArticle{
				Id:          "raw-1",
				SourceId:    "economictimes",
				Url:         "https://example.com/news/story",
				Title:       "A source clock that disagrees with UTC",
				PublishedAt: tc.published,
				FetchedAt:   tc.fetched,
			}
			clean, record, reason, ok := n.normalize(raw)
			if !ok {
				t.Fatalf("normalize rejected the article: %s", reason)
			}
			if got := clean.GetPublishedAt(); got != tc.want {
				t.Fatalf("clean published_at = %d, want %d", got, tc.want)
			}
			if got := record.PublishedAt.Unix(); got != tc.wantRecord {
				t.Fatalf("record published_at = %d, want %d", got, tc.wantRecord)
			}
			if !record.CreatedAt.Equal(record.PublishedAt) {
				t.Fatalf("created_at = %s, want published_at %s", record.CreatedAt, record.PublishedAt)
			}
		})
	}
}
