package gateway

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

const (
	defaultHistogramHours = 24
	maxHistogramHours     = 72
	defaultHistogramBins  = 36
	maxHistogramBins      = 96
)

type histogramJSON struct {
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Hours     int    `json:"hours"`
	Bins      []int  `json:"bins"`
	Count     int    `json:"count"`
	Available bool   `json:"available"`
	Error     string `json:"error,omitempty"`
}

// handleHistogram serves the ambient "Last 24h" strip from durable article
// history. It is deliberately bounded: the UI never asks for more than three
// days, and the response always has exactly `bins` counts so the caller can
// render it without gap-filling.
func (s *Server) handleHistogram(w http.ResponseWriter, r *http.Request) {
	hours := defaultHistogramHours
	if raw := r.URL.Query().Get("hours"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= maxHistogramHours {
			hours = n
		}
	}
	bins := defaultHistogramBins
	if raw := r.URL.Query().Get("bins"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= maxHistogramBins {
			bins = n
		}
	}
	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-time.Duration(hours) * time.Hour)
	out := histogramJSON{
		Start: start.Unix(),
		End:   end.Unix(),
		Hours: hours,
		Bins:  make([]int, bins),
	}
	if s.cfg.Store == nil {
		out.Error = "story history unavailable"
		writeAPI(w, http.StatusOK, out)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	histogram, err := s.cfg.Store.ArticleHistogram(ctx, start, end, bins)
	if err != nil {
		s.log.Warn("gateway: article histogram", "err", err)
		out.Error = "story history unavailable"
		writeAPI(w, http.StatusOK, out)
		return
	}
	out.Available = true
	out.Bins = histogram.Bins
	out.Count = histogram.Total
	writeAPI(w, http.StatusOK, out)
}
