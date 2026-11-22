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
	Enqueued        int64
	DroppedQueue    int64
	DroppedBatch    int64
	Batches         int64
	Errors          int64
	Pending         int
	EnqueuedStories int64
	DroppedStories  int64
	StoryBatches    int64
	PendingStories  int
}

// Writer is the non-blocking path from agents to Postgres. Enqueue only sends
// on a bounded channel; it never touches the database. A full queue drops the
// record and increments DroppedQueue.
// auxMutation is a small durable side record (link/text) queued on the same
// non-blocking writer as articles and stories.
type auxMutation struct {
	kind      uint8
	link      LinkRecord
	storyText StoryTextRecord
	linkText  LinkTextRecord
}

// pendingAssignment is a story assignment whose article row was not inserted
// yet when the story batch was flushed. It is retried when that article arrives.
type pendingAssignment struct {
	storyID  string
	category int16
}

type Writer struct {
	store      *Store
	opts       WriterOptions
	log        *slog.Logger
	queue      chan ArticleRecord
	storyQueue chan StoryRecord
	auxQueue   chan auxMutation
	done       chan struct{}
	ready      chan struct{}

	mu     sync.RWMutex
	closed bool

	assignMu      sync.Mutex
	pendingAssign map[string]pendingAssignment
	pendingOrder  []string

	startOnce sync.Once
	closeOnce sync.Once

	started         atomic.Bool
	enqueued        atomic.Int64
	droppedQueue    atomic.Int64
	droppedBatch    atomic.Int64
	batches         atomic.Int64
	errors          atomic.Int64
	enqueuedStories atomic.Int64
	droppedStories  atomic.Int64
	storyBatches    atomic.Int64
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
		store:         s,
		opts:          opts,
		log:           opts.Logger,
		queue:         make(chan ArticleRecord, opts.QueueSize),
		storyQueue:    make(chan StoryRecord, max(64, opts.QueueSize/4)),
		auxQueue:      make(chan auxMutation, max(64, opts.QueueSize/4)),
		pendingAssign: make(map[string]pendingAssignment),
		done:          make(chan struct{}),
		ready:         make(chan struct{}),
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
	if w == nil {
		return false
	}
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

// EnqueueStory offers one story update. Like Enqueue it never blocks.
func (w *Writer) EnqueueStory(rec StoryRecord) bool {
	if w == nil {
		return false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return false
	}
	select {
	case w.storyQueue <- rec:
		w.enqueuedStories.Add(1)
		return true
	default:
		w.droppedStories.Add(1)
		return false
	}
}

// EnqueueLink mirrors one link upsert/removal. It never blocks.
func (w *Writer) EnqueueLink(rec LinkRecord) bool {
	return w.enqueueAux(auxMutation{kind: 1, link: rec})
}

// EnqueueStoryText mirrors a story brief.
func (w *Writer) EnqueueStoryText(rec StoryTextRecord) bool {
	return w.enqueueAux(auxMutation{kind: 2, storyText: rec})
}

// EnqueueLinkText mirrors a link "why".
func (w *Writer) EnqueueLinkText(rec LinkTextRecord) bool {
	return w.enqueueAux(auxMutation{kind: 3, linkText: rec})
}

func (w *Writer) enqueueAux(m auxMutation) bool {
	if w == nil {
		return false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return false
	}
	select {
	case w.auxQueue <- m:
		return true
	default:
		w.droppedQueue.Add(1)
		return false
	}
}

// Close stops accepting records, drains all queues and waits for the final
// batch. It is bounded by ctx; a slow database does not hold shutdown forever.
func (w *Writer) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		close(w.storyQueue)
		close(w.auxQueue)
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
	if w == nil {
		return WriterStats{}
	}
	return WriterStats{
		Enqueued:        w.enqueued.Load(),
		DroppedQueue:    w.droppedQueue.Load(),
		DroppedBatch:    w.droppedBatch.Load(),
		Batches:         w.batches.Load(),
		Errors:          w.errors.Load(),
		Pending:         len(w.queue),
		EnqueuedStories: w.enqueuedStories.Load(),
		DroppedStories:  w.droppedStories.Load(),
		StoryBatches:    w.storyBatches.Load(),
		PendingStories:  len(w.storyQueue),
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
	stories := make([]StoryRecord, 0, max(8, w.opts.BatchSize/4))
	var links []LinkRecord
	var storyTexts []StoryTextRecord
	var linkTexts []LinkTextRecord
	flush := func() {
		if len(buffer) > 0 {
			w.flush(buffer)
			buffer = buffer[:0]
		}
		if len(stories) > 0 {
			w.flushStories(stories)
			stories = stories[:0]
		}
		if len(links) > 0 {
			w.flushLinks(links)
			links = nil
		}
		if len(storyTexts) > 0 {
			w.flushStoryTexts(storyTexts)
			storyTexts = nil
		}
		if len(linkTexts) > 0 {
			w.flushLinkTexts(linkTexts)
			linkTexts = nil
		}
	}
	articlesOpen, storiesOpen, auxOpen := true, true, true
	articles := w.queue
	storyUpdates := w.storyQueue
	aux := w.auxQueue
	for articlesOpen || storiesOpen || auxOpen {
		select {
		case rec, ok := <-articles:
			if !ok {
				articlesOpen = false
				articles = nil
				continue
			}
			buffer = append(buffer, rec)
			if len(buffer) >= w.opts.BatchSize {
				w.flush(buffer)
				buffer = buffer[:0]
			}
		case rec, ok := <-storyUpdates:
			if !ok {
				storiesOpen = false
				storyUpdates = nil
				continue
			}
			stories = append(stories, rec)
			if len(stories) >= max(8, w.opts.BatchSize/4) {
				w.flushStories(stories)
				stories = stories[:0]
			}
		case m, ok := <-aux:
			if !ok {
				auxOpen = false
				aux = nil
				continue
			}
			switch m.kind {
			case 1:
				links = append(links, m.link)
			case 2:
				storyTexts = append(storyTexts, m.storyText)
			case 3:
				linkTexts = append(linkTexts, m.linkText)
			}
			if len(links)+len(storyTexts)+len(linkTexts) >= w.opts.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
	flush()
}

func (w *Writer) flushLinks(batch []LinkRecord) {
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		err := w.store.UpsertLinks(ctx, batch)
		cancel()
		if err == nil {
			return
		}
		w.errors.Add(1)
		w.log.Warn("store link batch failed", "attempt", attempt+1, "rows", len(batch), "err", err)
		if attempt < w.opts.MaxRetries {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}
	w.droppedBatch.Add(int64(len(batch)))
}

func (w *Writer) flushStoryTexts(batch []StoryTextRecord) {
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		err := w.store.UpsertStoryTexts(ctx, batch)
		cancel()
		if err == nil {
			return
		}
		w.errors.Add(1)
		w.log.Warn("store story text batch failed", "attempt", attempt+1, "rows", len(batch), "err", err)
		if attempt < w.opts.MaxRetries {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}
	w.droppedBatch.Add(int64(len(batch)))
}

func (w *Writer) flushLinkTexts(batch []LinkTextRecord) {
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		err := w.store.UpsertLinkTexts(ctx, batch)
		cancel()
		if err == nil {
			return
		}
		w.errors.Add(1)
		w.log.Warn("store link text batch failed", "attempt", attempt+1, "rows", len(batch), "err", err)
		if attempt < w.opts.MaxRetries {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}
	w.droppedBatch.Add(int64(len(batch)))
}

func (w *Writer) flushStories(batch []StoryRecord) {
	for attempt := 0; attempt <= w.opts.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		missing, err := w.store.UpsertStories(ctx, batch)
		cancel()
		if err == nil {
			w.storyBatches.Add(1)
			if len(missing) > 0 {
				w.trackAssignments(batch, missing)
			}
			return
		}
		w.errors.Add(1)
		w.log.Warn("store story batch failed", "attempt", attempt+1, "rows", len(batch), "err", err)
		if attempt < w.opts.MaxRetries {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
	}
	w.droppedStories.Add(int64(len(batch)))
	w.log.Error("store story batch dropped after retries", "rows", len(batch))
}

const maxPendingAssignments = 8192

// trackAssignments remembers story assignments whose article rows did not
// exist yet. The assignment is retried after the article insert, so the result
// is independent of which async queue the writer happens to flush first.
func (w *Writer) trackAssignments(batch []StoryRecord, missing map[string]string) {
	byStory := make(map[string]StoryRecord, len(batch))
	for _, rec := range batch {
		byStory[rec.ID] = rec
	}
	w.assignMu.Lock()
	for articleID, storyID := range missing {
		rec := byStory[storyID]
		if _, exists := w.pendingAssign[articleID]; !exists {
			w.pendingOrder = append(w.pendingOrder, articleID)
		}
		w.pendingAssign[articleID] = pendingAssignment{storyID: storyID, category: rec.Category}
	}
	for len(w.pendingOrder) > maxPendingAssignments {
		old := w.pendingOrder[0]
		w.pendingOrder = w.pendingOrder[1:]
		delete(w.pendingAssign, old)
	}
	w.assignMu.Unlock()
}

// retryAssignments applies any remembered assignments for article ids that
// just landed. Cleared assignments are retried on the next story batch.
func (w *Writer) retryAssignments(records []ArticleRecord) {
	w.assignMu.Lock()
	if len(w.pendingAssign) == 0 {
		w.assignMu.Unlock()
		return
	}
	pending := make(map[string]pendingAssignment, len(records))
	for _, rec := range records {
		if a, ok := w.pendingAssign[rec.ID]; ok {
			pending[rec.ID] = a
		}
	}
	w.assignMu.Unlock()
	if len(pending) == 0 {
		return
	}
	for articleID, a := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		ok, err := w.store.AssignStory(ctx, articleID, a.storyID, a.category)
		cancel()
		if err != nil {
			w.errors.Add(1)
			w.log.Warn("store assignment retry failed", "article_id", articleID, "story_id", a.storyID, "err", err)
			continue
		}
		if ok {
			w.assignMu.Lock()
			delete(w.pendingAssign, articleID)
			w.assignMu.Unlock()
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
			w.retryAssignments(batch)
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
