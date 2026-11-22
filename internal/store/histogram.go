package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ArticleHistogram is an evenly spaced count of articles by publish time. Bins
// cover [Start, End) and every bin is present (zero counts included), so the
// API and the UI never have to infer gaps.
type ArticleHistogram struct {
	Start  time.Time
	End    time.Time
	Bins   []int
	Total  int
	Bucket time.Duration
}

// ArticleHistogram counts articles whose published_at falls inside the window.
// The articles table is partitioned by ingested day; published_at has its own
// index, and the 24 h window is small enough for a bounded index scan. Rows
// outside the window (including future-dated feed rows) are excluded.
func (s *Store) ArticleHistogram(
	ctx context.Context,
	start, end time.Time,
	bins int,
) (ArticleHistogram, error) {
	if bins < 1 || bins > 200 {
		return ArticleHistogram{}, errors.New("store: histogram bins must be 1..200")
	}
	if !end.After(start) {
		return ArticleHistogram{}, errors.New("store: histogram end must be after start")
	}
	start = start.UTC()
	end = end.UTC()
	bucket := end.Sub(start) / time.Duration(bins)
	if bucket <= 0 {
		bucket = time.Second
	}
	out := ArticleHistogram{
		Start:  start,
		End:    end,
		Bins:   make([]int, bins),
		Bucket: bucket,
	}
	rows, err := s.pool.Query(ctx, `
SELECT LEAST($3::int - 1,
             GREATEST(0,
               floor(extract(epoch FROM (published_at - $1::timestamptz)) / $4::float8)::int
             )) AS bin,
       count(*)
FROM articles
WHERE published_at >= $1::timestamptz
  AND published_at < $2::timestamptz
GROUP BY bin
ORDER BY bin`,
		start, end, bins, bucket.Seconds())
	if err != nil {
		return ArticleHistogram{}, fmt.Errorf("store: article histogram: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var bin, count int
		if err := rows.Scan(&bin, &count); err != nil {
			return ArticleHistogram{}, fmt.Errorf("store: article histogram scan: %w", err)
		}
		if bin < 0 || bin >= bins {
			continue
		}
		out.Bins[bin] = count
		out.Total += count
	}
	if err := rows.Err(); err != nil {
		return ArticleHistogram{}, fmt.Errorf("store: article histogram rows: %w", err)
	}
	return out, nil
}
