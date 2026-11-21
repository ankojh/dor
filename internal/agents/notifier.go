package agents

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// NotifierAgentName is the registry name of the external notification sender.
const NotifierAgentName = "notifier"

// PushSender delivers a Browser Push notification. A nil sender means push is
// not configured; the notifier reports that clearly and does not claim
// delivery.
type PushSender interface {
	Send(ctx context.Context, a alert.Alert, h alert.Hit) error
}

// NotifierConfig configures the notifier.
type NotifierConfig struct {
	Alerts      *alert.Store
	NtfyBaseURL string
	Push        PushSender
	HTTP        *http.Client
	Now         func() time.Time
	// MinInterval is the per-alert external delivery rate limit.
	MinInterval time.Duration
}

// NewNotifier builds the notifier.
func NewNotifier(b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg NotifierConfig) (Agent, error) {
	if cfg.Alerts == nil {
		return nil, fmt.Errorf("notifier: alert store is required")
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 15 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = time.Minute
	}
	n := &notifier{cfg: cfg, log: log}
	return NewBase(b, metrics, log, Spec{
		Name:          NotifierAgentName,
		Durable:       NotifierAgentName,
		FilterSubject: bus.SubjectMatchAlert,
		MaxAckPending: 64,
		AckWait:       30 * time.Second,
		Concurrency:   2,
		Handle:        n.handle,
	})
}

type notifier struct {
	cfg NotifierConfig
	log *slog.Logger

	pushWarned atomic.Bool
	ntfyWarned atomic.Bool
}

func (n *notifier) handle(ctx context.Context, msg *Message, out *Emitter) error {
	var hit dorv1.AlertHit
	if err := proto.Unmarshal(msg.Data, &hit); err != nil {
		return Terminal(fmt.Errorf("notifier: alert hit: %w", err))
	}
	a, ok := n.cfg.Alerts.Get(ctx, hit.AlertId)
	if !ok {
		// The alert was deleted; the hit is not deliverable.
		return nil
	}
	if n.cfg.Alerts.IsMuted(ctx, a.ID, hit.StoryId) {
		return nil
	}
	var external []string
	for _, ch := range a.Channels {
		if ch == "ntfy" || ch == "push" {
			external = append(external, ch)
		}
	}
	if len(external) == 0 {
		return nil
	}
	// Rate limit new firings only; a redelivery after an external failure
	// must be allowed to retry immediately.
	if n.cfg.Alerts.Attempts(ctx, a.ID, hit.StoryId) == 0 &&
		!n.cfg.Alerts.AllowRate(ctx, a.ID, n.cfg.MinInterval) {
		return nil
	}
	internal := alert.Hit{
		AlertID: hit.AlertId,
		StoryID: hit.StoryId,
		Level:   hit.Level,
		FiredAt: time.Unix(hit.FiredAt, 0).UTC(),
		Title:   hit.Title,
		Prompt:  hit.Prompt,
	}
	var errs []error
	for _, ch := range external {
		switch ch {
		case "ntfy":
			if n.cfg.NtfyBaseURL == "" {
				if n.ntfyWarned.CompareAndSwap(false, true) {
					n.log.Warn("notifier: ntfy is not configured (DOR_NTFY_URL empty); no external delivery")
				}
				continue
			}
			if err := n.sendNtfy(ctx, a, internal); err != nil {
				errs = append(errs, err)
			}
		case "push":
			if n.cfg.Push == nil {
				if n.pushWarned.CompareAndSwap(false, true) {
					n.log.Warn("notifier: web push sender is not configured; no push delivery")
				}
				continue
			}
			if err := n.cfg.Push.Send(ctx, a, internal); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		if attempts := n.cfg.Alerts.BumpAttempt(ctx, a.ID, hit.StoryId); attempts >= 5 {
			return Terminal(fmt.Errorf("notifier: delivery failed %d times: %v", attempts, errs[0]))
		}
		return errs[0]
	}
	n.cfg.Alerts.ClearAttempt(ctx, a.ID, hit.StoryId)
	return nil
}

func (n *notifier) sendNtfy(ctx context.Context, a alert.Alert, h alert.Hit) error {
	if n.cfg.NtfyBaseURL == "" || a.NtfyTopic == "" {
		return nil
	}
	body := h.Title + "\n" + h.Prompt
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		n.cfg.NtfyBaseURL+"/"+a.NtfyTopic, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Title", h.Title)
	req.Header.Set("Priority", "high")
	req.Header.Set("Tags", "rotating_light")
	resp, err := n.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("notifier: ntfy HTTP %d", resp.StatusCode)
	}
	return nil
}
