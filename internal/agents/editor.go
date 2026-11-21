package agents

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/textsvc"
)

// EditorAgentName is the registry name of the story editor.
const EditorAgentName = "editor"

// EditorConfig configures the editor.
type EditorConfig struct {
	Text     *textsvc.Service
	Debounce time.Duration
	Now      func() time.Time
}

// NewEditor builds the editor. It debounces story changes and asks the text
// service for a brief once a story has at least two sources. It never blocks
// the story stream: generation happens in the debounce worker.
func NewEditor(b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg EditorConfig) (Agent, error) {
	if cfg.Debounce <= 0 {
		cfg.Debounce = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	core := &editorCore{log: log, cfg: cfg, now: cfg.Now, pending: make(map[string]time.Time), work: make(chan string, 32)}
	base, err := NewBase(b, metrics, log, Spec{
		Name:           EditorAgentName,
		Durable:        EditorAgentName,
		FilterSubjects: []string{bus.SubjectStoryChangedPrefix + "*"},
		Ordered:        true,
		MaxAckPending:  1,
		AckWait:        60 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var story dorv1.Story
			if err := proto.Unmarshal(msg.Data, &story); err != nil {
				return Terminal(fmt.Errorf("editor: story: %w", err))
			}
			if story.SourceCount < 2 || story.Id == "" {
				return nil
			}
			core.schedule(story.Id)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &editorAgent{name: EditorAgentName, core: core, base: base}, nil
}

type editorCore struct {
	log *slog.Logger
	cfg EditorConfig
	now func() time.Time

	mu      sync.Mutex
	pending map[string]time.Time
	work    chan string
}

func (c *editorCore) schedule(id string) {
	c.mu.Lock()
	c.pending[id] = c.now().Add(c.cfg.Debounce)
	c.mu.Unlock()
}

func (c *editorCore) loop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := c.now()
			var due []string
			c.mu.Lock()
			for id, at := range c.pending {
				if !now.Before(at) {
					due = append(due, id)
				}
			}
			c.mu.Unlock()
			for _, id := range due {
				select {
				case c.work <- id:
					c.mu.Lock()
					delete(c.pending, id)
					c.mu.Unlock()
				default:
					c.mu.Lock()
					c.pending[id] = now.Add(time.Second)
					c.mu.Unlock()
				}
			}
		}
	}
}

func (c *editorCore) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-c.work:
			if c.cfg.Text == nil {
				continue
			}
			if _, ok := c.cfg.Text.GenerateStory(ctx, id); !ok {
				c.log.Debug("editor: story not in live state", "story", id)
			}
		}
	}
}

type editorAgent struct {
	name string
	core *editorCore
	base Agent

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (e *editorAgent) Name() string { return e.name }

func (e *editorAgent) Start(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: %s already started", e.name)
	}
	if e.stopped.Load() {
		return fmt.Errorf("agents: %s is stopped", e.name)
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.cancel = cancel
	e.mu.Unlock()
	if err := e.base.Start(ctx); err != nil {
		cancel()
		return err
	}
	e.running.Store(true)
	e.wg.Add(2)
	go func() {
		defer e.wg.Done()
		e.core.loop(runCtx)
	}()
	go func() {
		defer e.wg.Done()
		e.core.worker(runCtx)
	}()
	e.core.log.Info("editor started")
	return nil
}

func (e *editorAgent) Consume(ctx context.Context) error {
	if err := e.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (e *editorAgent) Stop(ctx context.Context) error {
	if e.stopped.Swap(true) {
		return nil
	}
	e.running.Store(false)
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.wg.Wait()
	return e.base.Stop(ctx)
}

func (e *editorAgent) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	return e.base.Publish(ctx, subject, data, opts...)
}

func (e *editorAgent) Running() bool {
	return e.running.Load() && !e.stopped.Load() && e.base.(HealthReporter).Running()
}

func (e *editorAgent) LastError() error { return e.base.(HealthReporter).LastError() }
func (e *editorAgent) TerminalCount() int64 {
	return e.base.(HealthReporter).TerminalCount()
}
