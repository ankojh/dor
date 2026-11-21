// Package record implements record mode: the raw stream (article.raw and
// article.raw.priority) is written to rotating gzip files so a recorded day can
// be replayed later (M6). The format is documented in docs/record-format.md.
package record

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Magic is the eight-byte header at the start of every record file.
const Magic = "DORREC\x00\x01"

// DefaultQueueSize bounds the in-memory queue between the fetchers and the
// writer goroutine. A full queue drops records and counts them; it never
// blocks a fetcher.
const DefaultQueueSize = 4096

// DefaultFlushInterval bounds how long a written frame can sit in the gzip
// buffer. Together with FlushEvery it keeps a crash loss to a few seconds.
const DefaultFlushInterval = 5 * time.Second

// Frame is one raw message to record.
type Frame struct {
	Subject    string
	MsgID      string
	ReceivedAt time.Time
	Payload    []byte
}

// Metrics receives writer counters. It is an interface so this package does
// not depend on Prometheus; fetch.Metrics satisfies it in the server.
type Metrics interface {
	RecordWritten(bytes int)
	RecordQueueDrop()
	RecordWriteError()
	RecordQueueDepth(n int)
}

// Options configure a Writer.
type Options struct {
	// Dir is the root directory; files go to <dir>/YYYY-MM-DD/HH.pb.gz.
	Dir string
	// QueueSize bounds the pending frames. Default DefaultQueueSize.
	QueueSize int
	// Now is the clock used for the file name and segment choice. Tests
	// inject a fake clock to cross hour and day boundaries.
	Now func() time.Time
	// Metrics is optional.
	Metrics Metrics
	// FlushEvery forces a gzip flush after this many frames. Default 64.
	FlushEvery int
	// FlushInterval forces a gzip flush after this much wall time. Default 5 s.
	FlushInterval time.Duration
}

// Writer is the non-blocking recorder. Record only enqueues; one goroutine
// owns the files.
type Writer struct {
	dir           string
	queue         chan Frame
	now           func() time.Time
	metrics       Metrics
	flushEvery    int
	flushInterval time.Duration

	done      chan struct{}
	closeOnce sync.Once
	dropped   atomic.Int64
	writeErrs atomic.Int64
	written   atomic.Int64

	// admitMu is short-lived: it guards closed, stamps ReceivedAt in
	// admission order and performs the nonblocking channel send. It is never
	// held while touching the disk, so a slow write cannot block Record.
	admitMu  sync.Mutex
	closed   bool
	closeErr error

	// mu guards the file, gzip writer and flush state only.
	mu       sync.Mutex
	file     *os.File
	gz       *gzip.Writer
	openKey  string // "YYYY-MM-DD/HH"
	openPath string
	pending  int
}

// NewWriter validates the directory and starts the writer goroutine.
func NewWriter(opts Options) (*Writer, error) {
	if strings.TrimSpace(opts.Dir) == "" {
		return nil, errors.New("record: dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("record: create %s: %w", opts.Dir, err)
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 64
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	w := &Writer{
		dir:           opts.Dir,
		queue:         make(chan Frame, opts.QueueSize),
		now:           opts.Now,
		metrics:       opts.Metrics,
		flushEvery:    opts.FlushEvery,
		flushInterval: opts.FlushInterval,
		done:          make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// Record enqueues one raw message. It never blocks: when the queue is full the
// frame is dropped and counted. A call after Close is a no-op. The writer
// stamps ReceivedAt and copies the payload, so the caller may reuse its buffer.
func (w *Writer) Record(subject, msgID string, payload []byte) {
	if w == nil {
		return
	}
	frame := Frame{
		Subject:    subject,
		MsgID:      msgID,
		ReceivedAt: w.now().UTC(),
		Payload:    append([]byte(nil), payload...),
	}
	w.admitMu.Lock()
	defer w.admitMu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.queue <- frame:
		if w.metrics != nil {
			w.metrics.RecordQueueDepth(len(w.queue))
		}
	default:
		w.dropped.Add(1)
		if w.metrics != nil {
			w.metrics.RecordQueueDrop()
		}
	}
}

// Dropped returns the number of frames dropped because the queue was full.
func (w *Writer) Dropped() int64 { return w.dropped.Load() }

// WriteErrors returns the number of frames lost to a disk error.
func (w *Writer) WriteErrors() int64 { return w.writeErrs.Load() }

// Written returns the number of frames written.
func (w *Writer) Written() int64 { return w.written.Load() }

// Close drains the queue, flushes and closes the current file. It is safe to
// call more than once.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		// Closing the channel is synchronised with admission only; the writer
		// goroutine is waited for outside that critical section, and the file
		// is closed under the disk mutex afterwards.
		w.admitMu.Lock()
		w.closed = true
		close(w.queue)
		w.admitMu.Unlock()
		<-w.done
		w.mu.Lock()
		w.closeErr = w.closeFileLocked()
		w.mu.Unlock()
	})
	return w.closeErr
}

func (w *Writer) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	flush := func() {
		w.mu.Lock()
		_ = w.flushLocked()
		w.mu.Unlock()
		w.pending = 0
	}
	for {
		select {
		case frame, ok := <-w.queue:
			if !ok {
				return
			}
			if err := w.writeFrame(frame); err != nil {
				// A disk error must not stop ingest. Count the frame as a
				// write error and keep the writer alive for the next frame.
				w.writeErrs.Add(1)
				if w.metrics != nil {
					w.metrics.RecordWriteError()
				}
			}
			w.pending++
			if w.pending >= w.flushEvery {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (w *Writer) writeFrame(frame Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureFileLocked(frame.ReceivedAt); err != nil {
		return err
	}
	msg := &dorv1.RecordFrame{
		Subject:      frame.Subject,
		MsgId:        frame.MsgID,
		ReceivedAtMs: frame.ReceivedAt.UnixMilli(),
		Payload:      frame.Payload,
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	if _, err := w.gz.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.gz.Write(data); err != nil {
		return err
	}
	w.written.Add(1)
	if w.metrics != nil {
		w.metrics.RecordWritten(len(data) + 4)
	}
	return nil
}

// ensureFileLocked opens the file for the frame's hour, rotating when the hour
// or day changed. A restart inside the same hour opens a new segment
// (HH-2.pb.gz) instead of appending, so a truncated crash tail can never
// corrupt later data.
func (w *Writer) ensureFileLocked(at time.Time) error {
	day := at.Format("2006-01-02")
	hour := at.Format("15")
	key := day + "/" + hour
	if w.file != nil && key == w.openKey {
		return nil
	}
	if err := w.closeFileLocked(); err != nil {
		return err
	}
	dir := filepath.Join(w.dir, day)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, hour+".pb.gz")
	for n := 2; ; n++ {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			break
		}
		path = filepath.Join(dir, fmt.Sprintf("%s-%d.pb.gz", hour, n))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	gz, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return err
	}
	if _, err := gz.Write([]byte(Magic)); err != nil {
		_ = gz.Close()
		_ = f.Close()
		return err
	}
	w.file = f
	w.gz = gz
	w.openKey = key
	w.openPath = path
	return nil
}

func (w *Writer) flushLocked() error {
	if w.gz == nil {
		return nil
	}
	return w.gz.Flush()
}

func (w *Writer) closeFileLocked() error {
	if w.file == nil {
		return nil
	}
	var errs []error
	if err := w.gz.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := w.file.Close(); err != nil {
		errs = append(errs, err)
	}
	w.file, w.gz, w.openKey, w.openPath = nil, nil, "", ""
	return errors.Join(errs...)
}

// Reader reads recorded files. It is the M6 replayer's input.
type Reader struct {
	dir string
}

// NewReader builds a reader rooted at dir.
func NewReader(dir string) *Reader { return &Reader{dir: dir} }

// Days lists recorded days (YYYY-MM-DD) in ascending order.
func (r *Reader) Days() ([]string, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var days []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 10 {
			days = append(days, e.Name())
		}
	}
	sort.Strings(days)
	return days, nil
}

// Files lists one day's record files in name order, which is hour order.
func (r *Reader) Files(day string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(r.dir, day))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".pb.gz") {
			files = append(files, filepath.Join(r.dir, day, e.Name()))
		}
	}
	// Plain name order puts "07-2.pb.gz" before "07.pb.gz" ('-' < '.'), which
	// would replay a restarted segment ahead of the original. Order by hour,
	// then segment number.
	sort.Slice(files, func(i, j int) bool {
		hi, si := segmentKey(files[i])
		hj, sj := segmentKey(files[j])
		if hi != hj {
			return hi < hj
		}
		if si != sj {
			return si < sj
		}
		return files[i] < files[j]
	})
	return files, nil
}

// segmentKey splits "HH.pb.gz" or "HH-N.pb.gz" into the hour and the segment
// number (1 for the first segment).
func segmentKey(path string) (hour string, seq int) {
	name := strings.TrimSuffix(filepath.Base(path), ".pb.gz")
	hour, rest, found := strings.Cut(name, "-")
	if !found {
		return hour, 1
	}
	n, err := strconv.Atoi(rest)
	if err != nil {
		return hour, 1
	}
	return hour, n
}

// ReadDay calls fn for every frame of one day in file order.
func (r *Reader) ReadDay(day string, fn func(Frame) error) error {
	files, err := r.Files(day)
	if err != nil {
		return err
	}
	for _, path := range files {
		if err := readFile(path, fn); err != nil {
			return err
		}
	}
	return nil
}

// ReadAll calls fn for every recorded frame, day by day.
func (r *Reader) ReadAll(fn func(Frame) error) error {
	days, err := r.Days()
	if err != nil {
		return err
	}
	for _, day := range days {
		if err := r.ReadDay(day, fn); err != nil {
			return err
		}
	}
	return nil
}

// Stats summarises a recording.
type Stats struct {
	Frames  int
	Bytes   int64
	First   time.Time
	Last    time.Time
	ByHours map[string]int
}

// Stat reads the recording and returns counters. It is what the acceptance
// evidence and the M6 replayer's dry run use.
func (r *Reader) Stat(day string) (Stats, error) {
	stats := Stats{ByHours: map[string]int{}}
	err := r.ReadDay(day, func(f Frame) error {
		stats.Frames++
		stats.Bytes += int64(len(f.Payload))
		if stats.First.IsZero() || f.ReceivedAt.Before(stats.First) {
			stats.First = f.ReceivedAt
		}
		if f.ReceivedAt.After(stats.Last) {
			stats.Last = f.ReceivedAt
		}
		stats.ByHours[f.ReceivedAt.Format("15")]++
		return nil
	})
	return stats, err
}

func readFile(path string, fn func(Frame) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("record: open %s: %w", path, err)
	}
	defer func() { _ = gz.Close() }()
	gz.Multistream(true)

	header := make([]byte, len(Magic))
	if _, err := io.ReadFull(gz, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil // empty or truncated first write
		}
		return fmt.Errorf("record: read %s header: %w", path, err)
	}
	if !bytes.Equal(header, []byte(Magic)) {
		return fmt.Errorf("record: %s is not a record file (bad magic)", path)
	}
	var length [4]byte
	for {
		if _, err := io.ReadFull(gz, length[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// A crash can truncate the final frame of the hour. Everything
				// read so far is valid; the tail is dropped.
				return nil
			}
			return fmt.Errorf("record: read %s: %w", path, err)
		}
		n := binary.BigEndian.Uint32(length[:])
		if n == 0 || n > 32<<20 {
			return fmt.Errorf("record: %s has an implausible frame length %d", path, n)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(gz, buf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("record: read %s frame: %w", path, err)
		}
		var msg dorv1.RecordFrame
		if err := proto.Unmarshal(buf, &msg); err != nil {
			return fmt.Errorf("record: decode %s frame: %w", path, err)
		}
		frame := Frame{
			Subject: msg.Subject,
			MsgID:   msg.MsgId,
			Payload: msg.Payload,
		}
		if msg.ReceivedAtMs != 0 {
			frame.ReceivedAt = time.UnixMilli(msg.ReceivedAtMs).UTC()
		}
		if err := fn(frame); err != nil {
			return err
		}
	}
}
