package maintenance

import (
	"strings"
	"testing"
)

func TestWriterWorkCountsUnsettledAndTerminalOutcomes(t *testing.T) {
	// These shapes pair with the store accounting tests and the cmd/dor test of
	// the actual production constructor: recovered retries keep cumulative
	// Errors but have no active work, while terminal drops continue to block.
	base := WriterCounters{Verified: true, Enqueued: 1, Errors: 7}
	tests := []struct {
		name   string
		change func(*WriterCounters)
		want   bool
	}{
		{name: "recovered retry diagnostics are quiescent", want: false},
		{name: "dropped queue", change: func(w *WriterCounters) { w.DroppedQueue = 1 }, want: true},
		{name: "dropped batch", change: func(w *WriterCounters) { w.DroppedBatch = 1 }, want: true},
		{name: "dropped story", change: func(w *WriterCounters) { w.DroppedStories = 1 }, want: true},
		{name: "pending article", change: func(w *WriterCounters) { w.Pending = 1 }, want: true},
		{name: "pending story", change: func(w *WriterCounters) { w.PendingStories = 1 }, want: true},
		{name: "auxiliary queue", change: func(w *WriterCounters) { w.AuxQueue = 1 }, want: true},
		{name: "unsettled", change: func(w *WriterCounters) { w.Unsettled = 1 }, want: true},
		{name: "pending enrichment", change: func(w *WriterCounters) { w.PendingEnrich = 1 }, want: true},
		{name: "pending assignment", change: func(w *WriterCounters) { w.PendingAssign = 1 }, want: true},
		{name: "flushing", change: func(w *WriterCounters) { w.Flushing = 1 }, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counters := base
			if tc.change != nil {
				tc.change(&counters)
			}
			got := writerWork(counters) > 0
			if got != tc.want {
				t.Fatalf("writerWork(%+v) > 0 = %v, want %v", counters, got, tc.want)
			}
		})
	}
}

func TestQuiesceReasonsOnlyReportsTerminalWriterDrops(t *testing.T) {
	for _, tc := range []struct {
		name       string
		counters   WriterCounters
		wantReason bool
	}{
		{name: "recovered retry", counters: WriterCounters{Verified: true, Enqueued: 1, Errors: 7}},
		{name: "terminal drop", counters: WriterCounters{Verified: true, Enqueued: 1, Errors: 7, DroppedStories: 1}, wantReason: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := &DrainReport{Writer: &tc.counters, WriterVerified: true}
			reasons := strings.Join(quiesceReasons(report), "\n")
			got := strings.Contains(reasons, "DB writer reported terminal drops")
			if got != tc.wantReason {
				t.Fatalf("terminal drop reason present=%v, want %v; reasons: %s", got, tc.wantReason, reasons)
			}
		})
	}
}
