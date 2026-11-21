package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// WriterOptions configure the asynchronous batch writer.
type WriterOptions struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	// WriteTimeout bounds one batch attempt. Default 10 s.
	WriteTimeout time.Duration
	Logger       *slog.Logger
}

// DefaultWriterOptions returns production defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		QueueSize:     4096,
		BatchSize:     100,
		FlushInterval: 500 * time.Millisecond,
		MaxRetries:    3,
		WriteTimeout:  10 * time.Second,
	}
}

// WriterStats is a snapshot of the writer counters.
type WriterStats struct {
	Enqueued     int64
	DroppedQueue int64
	DroppedBatch int64
	Batches      int64
	Errors       int64
	Pending      int
}

// Writer is the non-blocking path from agents to Postgres. Enqueue only sends
// on a bounded channel; it never touches the database. A full queue drops the
// record and increments DroppedQueue.
type Writer struct {
	store *Store
	opts  WriterOptions
	log   *slog.Logger
	queue chan ArticleRecord
	done  chan struct{}
	ready chan struct{}

	mu     sync.RWMutex
	closed bool

	startOnce sync.Once
	closeOnce sync.Once

	started      atomic.Bool
	enqueued     atomic.Int64
	droppedQueue atomic.Int64
	droppedBatch atomic.Int64
	batches      atomic.Int64
	errors       atomic.Int64
}

// NewWriter builds a writer over an open store.
func NewWriter(s *Store, opts WriterOptions) (*Writer, error) {
	if s == nil {
		return nil, errors.New("store: writer requires a store")
	}
	def := DefaultWriterOptions()
	if opts.QueueSize <= 0 {
		opts.QueueSize = def.QueueSize
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = def.BatchSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = def.FlushInterval
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = def.WriteTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Writer{
		store: s,
		opts:  opts,
		log:   opts.Logger,
		queue: make(chan ArticleRecord, opts.QueueSize),
		done:  make(chan struct{}),
		ready: make(chan struct{}),
	}, nil
}

// Start launches the batch loop. It is safe to call once.
func (w *Writer) Start() {
	w.startOnce.Do(func() {
		w.started.Store(true)
		close(w.ready)
		go w.loop()
	})
}

// Enqueue offers one record. A full queue or a closed writer returns false.
func (w *Writer) Enqueue(rec ArticleRecord) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return false
	}
	select {
	case w.queue <- rec:
		w.enqueued.Add(1)
		return true
	default:
		w.droppedQueue.Add(1)
		return false
	}
}

// Close stops accepting records, drains the queue and waits for the final
// batch. It is bounded by ctx; a slow database does not hold shutdown forever.
func (w *Writer) Close(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
	})
	if !w.started.Load() {
		return nil
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a snapshot of the counters and current queue depth.
func (w *Writer) Stats() WriterStats {
	return WriterStats{
		Enqueued:     w.enqueued.Load(),
		DroppedQueue: w.droppedQueue.Load(),
		DroppedBatch: w.droppedBatch.Load(),
		Batches:      w.batches.Load(),
		Errors:       w.errors.Load(),
		Pending:      len(w.queue),
	}
}

func (w *Writer) loop() {
	defer close(w.done)
	select {
	case <-w.ready:
	default:
	}
	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()
	buffer := make([]ArticleRecord, 0, w.opts.BatchSize)
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		w.flush(buffer)
		buffer = buffer[:0]
	}
	for {
		select {
		case rec, ok := <-w.queue:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, rec)
			if len(buffer) >= w.opts.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (w *Writer) flush(batch []ArticleRecord) {
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		err := w.store.UpsertArticles(ctx, batch)
		cancel()
		if err == nil {
			w.batches.Add(1)
			return
		}
		w.errors.Add(1)
		w.log.Warn("store batch failed", "attempt", attempt+1, "rows", len(batch), "err", err)
		if attempt < w.opts.MaxRetries {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}
	w.droppedBatch.Add(int64(len(batch)))
	w.log.Error("store batch dropped after retries", "rows", len(batch))
}
