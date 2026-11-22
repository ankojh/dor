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
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// MatcherAgentName is the registry name of the prompt/alert matcher.
const MatcherAgentName = "matcher"

// NewMatcher builds the matcher agent. It consumes clean articles (prompt
// scores) and story changes (alert hits), plus a periodic prompt/alert
// refresh. Provider calls happen here, never in the gateway path.
func NewMatcher(b *bus.Bus, metrics *Metrics, log *slog.Logger, svc *matcher.Service) (Agent, error) {
	if svc == nil {
		return nil, fmt.Errorf("matcher: service is required")
	}
	articles, err := NewBase(b, metrics, log, Spec{
		Name:          "matcher-articles",
		Durable:       "matcher-articles",
		FilterSubject: bus.SubjectArticleClean,
		MaxAckPending: 128,
		AckWait:       60 * time.Second,
		Concurrency:   2,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var article dorv1.Article
			if err := proto.Unmarshal(msg.Data, &article); err != nil {
				return Terminal(fmt.Errorf("matcher: article: %w", err))
			}
			scores, approx := svc.ArticleScores(ctx, &article)
			return svc.PublishScores(ctx, article.Id, scores, approx)
		},
	})
	if err != nil {
		return nil, err
	}
	stories, err := NewBase(b, metrics, log, Spec{
		Name:           "matcher-stories",
		Durable:        "matcher-stories",
		FilterSubjects: []string{bus.SubjectStoryChangedPrefix + "*"},
		Ordered:        true,
		MaxAckPending:  1,
		AckWait:        60 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			var story dorv1.Story
			if err := proto.Unmarshal(msg.Data, &story); err != nil {
				return Terminal(fmt.Errorf("matcher: story: %w", err))
			}
			hits, err := svc.StoryAlerts(ctx, &story)
			if err != nil {
				return fmt.Errorf("matcher: story alerts: %w", err)
			}
			for _, hit := range hits {
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&dorv1.AlertHit{
					AlertId:     hit.AlertID,
					StoryId:     hit.StoryID,
					Level:       hit.Level,
					FiredAt:     hit.FiredAt.Unix(),
					Title:       hit.Title,
					Prompt:      hit.Prompt,
					Generation:  hit.Generation,
					Approx:      hit.Approx,
					SourceCount: uint32(hit.SourceCount),
				})
				if err != nil {
					continue
				}
				pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err = b.Publish(pubCtx, bus.SubjectMatchAlert, data,
					bus.WithMsgID(fmt.Sprintf("alert:%s:%s:%s:%d", hit.AlertID, hit.StoryID, hit.Level, hit.Generation)))
				cancel()
				if err != nil {
					return fmt.Errorf("matcher: publish alert: %w", err)
				}
				if err := svc.RecordHit(ctx, hit); err != nil {
					return fmt.Errorf("matcher: persist hit: %w", err)
				}
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &matcherAgent{name: MatcherAgentName, svc: svc, articles: articles, stories: stories}, nil
}

type matcherAgent struct {
	name     string
	svc      *matcher.Service
	articles Agent
	stories  Agent

	started atomic.Bool
	stopped atomic.Bool
	running atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (m *matcherAgent) Name() string { return m.name }

func (m *matcherAgent) Start(ctx context.Context) error {
	if !m.started.CompareAndSwap(false, true) {
		return fmt.Errorf("agents: %s already started", m.name)
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	m.svc.Refresh(runCtx)
	if err := m.articles.Start(ctx); err != nil {
		cancel()
		return err
	}
	if err := m.stories.Start(ctx); err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = m.articles.Stop(stopCtx)
		stopCancel()
		cancel()
		return err
	}
	m.running.Store(true)
	m.startRuntime(runCtx)
	return nil
}

// startRuntime starts the prompt-refresh ticker and prompt watch for one run
// context. Start and Restart share it so a restart restores both background
// loops, not only the durable consumers.
func (m *matcherAgent) startRuntime(runCtx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				m.svc.Refresh(runCtx)
			}
		}
	}()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for runCtx.Err() == nil {
			if err := m.svc.WatchPrompts(runCtx); err != nil && runCtx.Err() == nil {
				timer := time.NewTimer(time.Second)
				select {
				case <-runCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}()
}

func (m *matcherAgent) Consume(ctx context.Context) error {
	if err := m.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (m *matcherAgent) Stop(ctx context.Context) error {
	if m.stopped.Swap(true) {
		return nil
	}
	m.running.Store(false)
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	var errs []error
	if err := m.stories.Stop(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := m.articles.Stop(ctx); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (m *matcherAgent) Publish(ctx context.Context, subject string, data []byte, opts ...bus.PublishOption) error {
	return m.articles.Publish(ctx, subject, data, opts...)
}

func (m *matcherAgent) Running() bool {
	return m.running.Load() && !m.stopped.Load() && m.articles.(HealthReporter).Running() && m.stories.(HealthReporter).Running()
}

func (m *matcherAgent) LastError() error { return m.articles.(HealthReporter).LastError() }
func (m *matcherAgent) TerminalCount() int64 {
	return m.articles.(HealthReporter).TerminalCount() + m.stories.(HealthReporter).TerminalCount()
}
