package record

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

type countingMetrics struct {
	written, dropped, writeErr, bytes atomic.Int64
	depth                             atomic.Int64
}

func (m *countingMetrics) RecordWritten(n int)    { m.written.Add(1); m.bytes.Add(int64(n)) }
func (m *countingMetrics) RecordQueueDrop()       { m.dropped.Add(1) }
func (m *countingMetrics) RecordWriteError()      { m.writeErr.Add(1) }
func (m *countingMetrics) RecordQueueDepth(n int) { m.depth.Store(int64(n)) }

func newWriter(t *testing.T, dir string, clock *testClock, opts ...func(*Options)) *Writer {
	t.Helper()
	o := Options{Dir: dir, Now: clock.Now}
	for _, f := range opts {
		f(&o)
	}
	w, err := NewWriter(o)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func readAll(t *testing.T, dir string) []Frame {
	t.Helper()
	var frames []Frame
	if err := NewReader(dir).ReadAll(func(f Frame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return frames
}

func fileNames(t *testing.T, dir, day string) []string {
	t.Helper()
	files, err := NewReader(dir).Files(day)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = filepath.Base(f)
	}
	return names
}

func TestWriterRoundTripAndRotation(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 11, 20, 10, 59, 58, 0, time.UTC)}
	w := newWriter(t, dir, clock)

	type rec struct {
		at      time.Time
		subject string
		id      string
	}
	recs := []rec{
		{time.Date(2026, 11, 20, 10, 59, 58, 0, time.UTC), "article.raw", "raw:a"},
		{time.Date(2026, 11, 20, 10, 59, 59, 0, time.UTC), "article.raw.priority", "raw:b"},
		{time.Date(2026, 11, 20, 11, 0, 0, 0, time.UTC), "article.raw", "raw:c"},
		{time.Date(2026, 11, 20, 23, 59, 59, 0, time.UTC), "article.raw", "raw:d"},
		{time.Date(2026, 11, 21, 0, 0, 1, 0, time.UTC), "article.raw", "raw:e"},
	}
	for _, r := range recs {
		clock.Set(r.at)
		w.Record(r.subject, r.id, []byte("payload-"+r.id))
		// Rotation is decided by the writer goroutine in queue order; wait for
		// it so the next Record cannot race the injected clock.
		waitFor(t, func() bool { return w.Written() == int64(indexOf(recs, r)+1) })
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Dropped() != 0 || w.Written() != 5 {
		t.Fatalf("written %d dropped %d", w.Written(), w.Dropped())
	}

	if got, want := fileNames(t, dir, "2026-11-20"), []string{"10.pb.gz", "11.pb.gz", "23.pb.gz"}; !reflect.DeepEqual(got, want) {
		t.Errorf("day 1 files = %v, want %v", got, want)
	}
	if got, want := fileNames(t, dir, "2026-11-21"), []string{"00.pb.gz"}; !reflect.DeepEqual(got, want) {
		t.Errorf("day 2 files = %v, want %v", got, want)
	}
	days, err := NewReader(dir).Days()
	if err != nil || !reflect.DeepEqual(days, []string{"2026-11-20", "2026-11-21"}) {
		t.Errorf("days = %v %v", days, err)
	}

	frames := readAll(t, dir)
	if len(frames) != len(recs) {
		t.Fatalf("frames = %d", len(frames))
	}
	for i, r := range recs {
		f := frames[i]
		if f.Subject != r.subject || f.MsgID != r.id || string(f.Payload) != "payload-"+r.id || !f.ReceivedAt.Equal(r.at) {
			t.Errorf("frame %d = %+v, want %+v", i, f, r)
		}
	}

	stats, err := NewReader(dir).Stat("2026-11-20")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Frames != 4 || stats.ByHours["10"] != 2 || stats.ByHours["11"] != 1 || stats.ByHours["23"] != 1 {
		t.Errorf("stats = %+v", stats)
	}
	if !stats.First.Equal(recs[0].at) || !stats.Last.Equal(recs[3].at) {
		t.Errorf("first/last = %v / %v", stats.First, stats.Last)
	}
	if want := int64(4 * len("payload-raw:a")); stats.Bytes != want {
		t.Errorf("bytes = %d, want %d", stats.Bytes, want)
	}
}

func TestWriterRestartOpensNextSegmentInOrder(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 20, 7, 30, 0, 0, time.UTC)
	clock := &testClock{now: at}

	for run := 0; run < 3; run++ {
		w := newWriter(t, dir, clock)
		w.Record("article.raw", fmt.Sprintf("run%d-1", run), []byte("x"))
		w.Record("article.raw", fmt.Sprintf("run%d-2", run), []byte("y"))
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		clock.Set(clock.Now().Add(time.Second))
	}

	names := fileNames(t, dir, "2026-11-20")
	if len(names) != 3 {
		t.Fatalf("segments = %v", names)
	}
	for _, want := range []string{"07.pb.gz", "07-2.pb.gz", "07-3.pb.gz"} {
		if _, err := os.Stat(filepath.Join(dir, "2026-11-20", want)); err != nil {
			t.Errorf("segment %s missing: %v", want, err)
		}
	}
	var ids []string
	for _, f := range readAll(t, dir) {
		ids = append(ids, f.MsgID)
	}
	want := []string{"run0-1", "run0-2", "run1-1", "run1-2", "run2-1", "run2-2"}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("frames must replay in write order across restarts:\n got %v\nwant %v", ids, want)
	}
}

func TestSegmentOrderNumeric(t *testing.T) {
	dir := t.TempDir()
	day := filepath.Join(dir, "2026-11-20")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"07.pb.gz", "07-2.pb.gz", "07-10.pb.gz", "07-3.pb.gz", "08.pb.gz", "ignored.txt"} {
		if err := os.WriteFile(filepath.Join(day, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"07.pb.gz", "07-2.pb.gz", "07-3.pb.gz", "07-10.pb.gz", "08.pb.gz"}
	if got := fileNames(t, dir, "2026-11-20"); !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
}

func frameBytes(t *testing.T, id string) []byte {
	t.Helper()
	data, err := proto.Marshal(&dorv1.RecordFrame{Subject: "article.raw", MsgId: id, ReceivedAtMs: 1000, Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 4, 4+len(data))
	binary.BigEndian.PutUint32(out, uint32(len(data)))
	return append(out, data...)
}

func writeGz(t *testing.T, path string, parts ...[]byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for _, p := range parts {
		if _, err := gz.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReaderToleratesTruncatedTail(t *testing.T) {
	good := frameBytes(t, "good")
	partial := frameBytes(t, "partial")
	tests := []struct {
		name  string
		parts [][]byte
		want  []string
	}{
		{"frame cut mid-body", [][]byte{[]byte(Magic), good, partial[:len(partial)-3]}, []string{"good"}},
		{"length prefix cut", [][]byte{[]byte(Magic), good, partial[:2]}, []string{"good"}},
		{"only a header prefix", [][]byte{[]byte(Magic)[:4]}, nil},
		{"empty stream", nil, nil},
		{"complete", [][]byte{[]byte(Magic), good, frameBytes(t, "two")}, []string{"good", "two"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeGz(t, filepath.Join(dir, "2026-11-20", "07.pb.gz"), tt.parts...)
			var ids []string
			err := NewReader(dir).ReadDay("2026-11-20", func(f Frame) error {
				ids = append(ids, f.MsgID)
				return nil
			})
			if err != nil {
				t.Fatalf("truncated tail must not be an error: %v", err)
			}
			if !reflect.DeepEqual(ids, tt.want) {
				t.Errorf("frames = %v, want %v", ids, tt.want)
			}
		})
	}
}

// A real crash cuts the gzip stream itself, not a clean frame boundary.
func TestReaderToleratesCrashTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)}
	w := newWriter(t, dir, clock, func(o *Options) { o.FlushEvery = 1 })
	for i := 0; i < 5; i++ {
		w.Record("article.raw", fmt.Sprintf("m%d", i), bytes.Repeat([]byte("z"), 200))
	}
	waitFor(t, func() bool { return w.Written() == 5 })
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "2026-11-20", "07.pb.gz")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-30], 0o644); err != nil {
		t.Fatal(err)
	}
	frames := readAll(t, dir)
	if len(frames) < 3 || len(frames) > 5 {
		t.Fatalf("frames after truncation = %d", len(frames))
	}
	for i, f := range frames {
		if f.MsgID != fmt.Sprintf("m%d", i) {
			t.Errorf("frame %d = %q", i, f.MsgID)
		}
	}
}

func TestReaderRejectsBadMagicAndLength(t *testing.T) {
	dir := t.TempDir()
	writeGz(t, filepath.Join(dir, "2026-11-20", "07.pb.gz"), []byte("NOTAREC\x00rest"))
	if err := NewReader(dir).ReadDay("2026-11-20", func(Frame) error { return nil }); err == nil {
		t.Error("bad magic must be an error")
	}
	dir = t.TempDir()
	huge := make([]byte, 4)
	binary.BigEndian.PutUint32(huge, 64<<20)
	writeGz(t, filepath.Join(dir, "2026-11-20", "07.pb.gz"), []byte(Magic), huge)
	if err := NewReader(dir).ReadDay("2026-11-20", func(Frame) error { return nil }); err == nil {
		t.Error("implausible frame length must be an error")
	}
}

func TestReaderMissingDirAndCallbackError(t *testing.T) {
	r := NewReader(filepath.Join(t.TempDir(), "nope"))
	if days, err := r.Days(); err != nil || len(days) != 0 {
		t.Errorf("Days = %v %v", days, err)
	}
	if files, err := r.Files("2026-11-20"); err != nil || len(files) != 0 {
		t.Errorf("Files = %v %v", files, err)
	}
	stats, err := r.Stat("2026-11-20")
	if err != nil || stats.Frames != 0 {
		t.Errorf("Stat = %+v %v", stats, err)
	}

	dir := t.TempDir()
	writeGz(t, filepath.Join(dir, "2026-11-20", "07.pb.gz"), []byte(Magic), frameBytes(t, "a"), frameBytes(t, "b"))
	stop := fmt.Errorf("stop")
	calls := 0
	err = NewReader(dir).ReadAll(func(Frame) error { calls++; return stop })
	if err != stop || calls != 1 {
		t.Errorf("callback error = %v after %d calls", err, calls)
	}
}

func TestWriterQueueFullDropsWithoutBlocking(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)}
	metrics := &countingMetrics{}
	// Build a writer with no running goroutine: the queue stays full and the
	// test observes the drop path without relying on a stalled disk.
	w := &Writer{
		dir:           t.TempDir(),
		queue:         make(chan Frame, 1),
		now:           clock.Now,
		metrics:       metrics,
		flushEvery:    64,
		flushInterval: time.Hour,
		done:          make(chan struct{}),
	}
	close(w.done)

	w.Record("article.raw", "first", []byte("1")) // fills the queue
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			w.Record("article.raw", fmt.Sprintf("extra-%d", i), []byte("x"))
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked on a full queue")
	}
	if w.Dropped() != 50 || metrics.dropped.Load() != 50 {
		t.Fatalf("dropped = %d (metric %d), want 50", w.Dropped(), metrics.dropped.Load())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// A Record after Close is a no-op, not a panic on a closed channel.
	w.Record("article.raw", "after-close", []byte("x"))
	if w.Dropped() != 50 {
		t.Fatalf("Record after Close counted a drop: %d", w.Dropped())
	}
}

func TestWriterCloseIdempotentAndNilSafe(t *testing.T) {
	var nilWriter *Writer
	nilWriter.Record("s", "id", nil)
	if err := nilWriter.Close(); err != nil {
		t.Errorf("nil Close = %v", err)
	}
	w := newWriter(t, t.TempDir(), &testClock{now: time.Now()})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
}

func TestNewWriterValidation(t *testing.T) {
	if _, err := NewWriter(Options{Dir: "  "}); err == nil {
		t.Error("empty dir must be rejected")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWriter(Options{Dir: filepath.Join(file, "sub")}); err == nil {
		t.Error("uncreatable dir must be rejected")
	}
}

func TestRecordCopiesPayload(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)}
	w := newWriter(t, dir, clock)
	payload := []byte("original")
	w.Record("article.raw", "id", payload)
	copy(payload, "MUTATED!")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	frames := readAll(t, dir)
	if len(frames) != 1 || string(frames[0].Payload) != "original" {
		t.Errorf("frames = %+v", frames)
	}
}

func indexOf[T comparable](list []T, v T) int {
	for i, x := range list {
		if x == v {
			return i
		}
	}
	return -1
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestWriterFlushesOnInterval checks the time-based flush: a frame is readable
// from the open segment within the flush interval even though the frame
// threshold has not been reached.
func TestWriterFlushesOnInterval(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)}
	w := newWriter(t, dir, clock, func(o *Options) {
		o.FlushEvery = 1000
		o.FlushInterval = 20 * time.Millisecond
	})
	defer func() { _ = w.Close() }()
	w.Record("article.raw", "flush-me", []byte("payload"))

	deadline := time.Now().Add(2 * time.Second)
	for {
		if frames := readAll(t, dir); len(frames) == 1 && frames[0].MsgID == "flush-me" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("frame was not flushed within the interval")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
