package fetch

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func newTestHealth(t *testing.T, kv *fakeKV, clock *fakeClock) (*HealthStore, *testMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	var store *HealthStore
	if kv == nil {
		store = NewHealthStore(nil, m, nil)
	} else {
		store = NewHealthStore(kv, m, nil)
	}
	store.now = clock.Now
	return store, &testMetrics{Metrics: m, reg: reg}
}

func TestHealthRecordUpdatesKVAndMetrics(t *testing.T) {
	kv := newFakeKV()
	clock := newFakeClock()
	h, m := newTestHealth(t, kv, clock)
	ctx := context.Background()

	rec := h.Record(ctx, Update{
		SourceID: "rss-bbc", Kind: "rss", State: StateOK, Success: true, Items: 5,
		ETag: `"e1"`, LastModified: "lm1", NextAttempt: clock.Now().Add(time.Minute),
	})
	if rec.ItemsHour != 5 || rec.ItemsTotal != 5 || rec.FetchesHour != 1 || rec.ErrorsHour != 0 || rec.ErrorRate != 0 {
		t.Fatalf("after success: %+v", rec)
	}
	if rec.LastSuccess != clock.Now().Unix() || rec.NextAttempt != clock.Now().Add(time.Minute).Unix() {
		t.Errorf("timestamps: %+v", rec)
	}

	clock.Advance(10 * time.Second)
	rec = h.Record(ctx, Update{
		SourceID: "rss-bbc", Kind: "rss", State: StateBackoff, Err: "boom",
		ConsecutiveFailures: 1, QuarantinedUntil: clock.Now().Add(time.Hour),
	})
	if rec.ErrorsHour != 1 || rec.FetchesHour != 2 || rec.ErrorRate != 0.5 || rec.State != "backoff" {
		t.Fatalf("after failure: %+v", rec)
	}
	if rec.ItemsHour != 5 || rec.ItemsTotal != 5 {
		t.Errorf("a failure must not add items: %+v", rec)
	}
	if want := clock.Now().Add(-10 * time.Second).Unix(); rec.LastSuccess != want {
		t.Errorf("last success = %d, want the earlier %d", rec.LastSuccess, want)
	}
	if rec.LastError != "boom" || rec.QuarantinedUntil == 0 {
		t.Errorf("error fields: %+v", rec)
	}

	// KV: one JSON record per source, keyed by the source id.
	raw, ok := kv.raw("rss-bbc")
	if !ok {
		t.Fatal("no KV entry under the source id")
	}
	var stored HealthRecord
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if stored != rec {
		t.Errorf("stored %+v != returned %+v", stored, rec)
	}
	loaded, ok := h.Load(ctx, "rss-bbc")
	if !ok || loaded != rec {
		t.Errorf("Load = %+v %v", loaded, ok)
	}
	if _, ok := h.Load(ctx, "missing"); ok {
		t.Error("Load of an unknown source must report false")
	}

	// Metrics.
	if got := m.value(t, "dor_source_state", "rss-bbc"); got != float64(StateBackoff) {
		t.Errorf("dor_source_state = %v", got)
	}
	if got := m.value(t, "dor_source_items_hour", "rss-bbc"); got != 5 {
		t.Errorf("dor_source_items_hour = %v", got)
	}
	if got := m.value(t, "dor_source_error_rate", "rss-bbc"); got != 0.5 {
		t.Errorf("dor_source_error_rate = %v", got)
	}
	if got := m.value(t, "dor_source_items_total", "rss-bbc"); got != 5 {
		t.Errorf("dor_source_items_total = %v", got)
	}
	if got := m.value(t, "dor_source_errors_total", "rss-bbc"); got != 1 {
		t.Errorf("dor_source_errors_total = %v", got)
	}
	if got := m.value(t, "dor_source_fetches_total", "rss-bbc"); got != 2 {
		t.Errorf("dor_source_fetches_total = %v", got)
	}
	if got := m.value(t, "dor_source_last_success_timestamp_seconds", "rss-bbc"); got != float64(clock.Now().Add(-10*time.Second).Unix()) {
		t.Errorf("dor_source_last_success_timestamp_seconds = %v", got)
	}
}

func TestHealthKVKeyPerSource(t *testing.T) {
	kv := newFakeKV()
	h, _ := newTestHealth(t, kv, newFakeClock())
	ctx := context.Background()
	h.Record(ctx, Update{SourceID: "a", Kind: "rss", Success: true, Items: 1})
	h.Record(ctx, Update{SourceID: "b", Kind: "usgs", Success: true, Items: 2})
	h.Record(ctx, Update{SourceID: "a", Kind: "rss", Success: true, Items: 3})
	if len(kv.data) != 2 {
		t.Fatalf("keys = %d, want one per source", len(kv.data))
	}
	a, _ := h.Load(ctx, "a")
	b, _ := h.Load(ctx, "b")
	if a.ItemsTotal != 4 || b.ItemsTotal != 2 || a.Kind != "rss" || b.Kind != "usgs" {
		t.Errorf("a = %+v b = %+v", a, b)
	}
}

func TestHealthHourRollover(t *testing.T) {
	clock := newFakeClock()
	h, m := newTestHealth(t, nil, clock)
	ctx := context.Background()
	h.Record(ctx, Update{SourceID: "s", Success: true, Items: 7})
	h.Record(ctx, Update{SourceID: "s", Err: "x"})
	clock.Advance(time.Hour)
	rec := h.Record(ctx, Update{SourceID: "s", Success: true, Items: 2})
	if rec.ItemsHour != 2 || rec.FetchesHour != 1 || rec.ErrorsHour != 0 || rec.ErrorRate != 0 {
		t.Errorf("hour counters did not reset: %+v", rec)
	}
	if rec.ItemsTotal != 9 {
		t.Errorf("items total must survive the hour: %d", rec.ItemsTotal)
	}
	if got := m.value(t, "dor_source_items_hour", "s"); got != 2 {
		t.Errorf("items_hour gauge = %v", got)
	}
	if got := m.value(t, "dor_source_error_rate", "s"); got != 0 {
		t.Errorf("error_rate gauge = %v", got)
	}
}

func TestHealthWithoutKVOrMetrics(t *testing.T) {
	h := NewHealthStore(nil, nil, nil)
	rec := h.Record(context.Background(), Update{SourceID: "s", Success: true, Items: 1})
	if rec.ItemsHour != 1 {
		t.Errorf("rec = %+v", rec)
	}
	if _, ok := h.Load(context.Background(), "s"); ok {
		t.Error("no KV means nothing to load")
	}
}

func TestMetricsRecordMode(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := &testMetrics{Metrics: NewMetrics(reg), reg: reg}
	m.RecordWritten(100)
	m.RecordWritten(50)
	m.RecordQueueDrop()
	m.RecordWriteError()
	m.RecordQueueDepth(7)
	for name, want := range map[string]float64{
		"dor_record_frames_total":       2,
		"dor_record_bytes_total":        150,
		"dor_record_dropped_total":      1,
		"dor_record_write_errors_total": 1,
		"dor_record_queue_depth":        7,
	} {
		if got := m.value(t, name, ""); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// testMetrics reads values back from the registry, so the tests assert on the
// exported metric names and not on the Go fields.
type testMetrics struct {
	*Metrics
	reg *prometheus.Registry
}

func (m *testMetrics) value(t *testing.T, name, source string) float64 {
	t.Helper()
	families, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			label := ""
			for _, l := range metric.GetLabel() {
				if l.GetName() == "source" {
					label = l.GetValue()
				}
			}
			if label != source {
				continue
			}
			switch {
			case metric.Gauge != nil:
				return metric.GetGauge().GetValue()
			case metric.Counter != nil:
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s{source=%q} not found", name, source)
	return 0
}

// TestHealthRestoreSeedsCounters checks that a restart keeps last-success,
// item totals and the current-hour counters instead of starting from zero.
func TestHealthRestoreSeedsCounters(t *testing.T) {
	kv := newFakeKV()
	reg := prometheus.NewRegistry()
	h := NewHealthStore(kv, NewMetrics(reg), nil)
	now := time.Date(2026, 11, 20, 12, 30, 0, 0, time.UTC)
	h.now = func() time.Time { return now }
	first := h.Record(context.Background(), Update{SourceID: "s", Kind: "rss", State: StateOK, Success: true, Items: 5})
	if first.ItemsTotal != 5 || first.LastSuccess == 0 {
		t.Fatalf("first = %+v", first)
	}
	loaded, ok := h.Load(context.Background(), "s")
	if !ok {
		t.Fatal("record not persisted")
	}

	reg2 := prometheus.NewRegistry()
	h2 := NewHealthStore(kv, NewMetrics(reg2), nil)
	h2.now = func() time.Time { return now.Add(time.Minute) }
	h2.Restore(loaded)
	second := h2.Record(context.Background(), Update{SourceID: "s", Kind: "rss", State: StateOK, Success: true, Items: 2})
	if second.ItemsTotal != 7 {
		t.Errorf("items_total after restore = %d, want 7", second.ItemsTotal)
	}
	if second.ItemsHour != 7 || second.LastSuccess < loaded.LastSuccess {
		t.Errorf("restored counters = %+v, want last_success >= %d and 7 items this hour", second, loaded.LastSuccess)
	}
	if got := metricValue(t, reg2, "dor_source_items_total", "s"); got != 7 {
		t.Errorf("dor_source_items_total after restore = %v, want 7", got)
	}
}

// metricValue reads one exported metric value from a registry.
func metricValue(t *testing.T, reg *prometheus.Registry, name, source string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			label := ""
			for _, l := range metric.GetLabel() {
				if l.GetName() == "source" {
					label = l.GetValue()
				}
			}
			if label != source {
				continue
			}
			if metric.Counter != nil {
				return metric.GetCounter().GetValue()
			}
			if metric.Gauge != nil {
				return metric.GetGauge().GetValue()
			}
		}
	}
	return -1
}
