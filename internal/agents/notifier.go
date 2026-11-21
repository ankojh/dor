package agents

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

const NotifierAgentName = "notifier"

type PushSender interface {
	Send(context.Context, alert.Alert, alert.Hit) error
}

type NotifierConfig struct {
	Alerts      *alert.Store
	NtfyBaseURL string
	Push        PushSender
	HTTP        *http.Client
	Now         func() time.Time
	MinInterval time.Duration
}

// NewNotifier acknowledges input only after a non-expiring queue write. A
// separate worker drains delayed hits and frozen digests, including on restart.
func NewNotifier(b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg NotifierConfig) (Agent, error) {
	if cfg.Alerts == nil {
		return nil, fmt.Errorf("notifier: alert store is required")
	}
	if log == nil {
		log = slog.Default()
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
	consumer, err := NewBase(b, metrics, log, Spec{Name: NotifierAgentName, Durable: NotifierAgentName,
		FilterSubject: bus.SubjectMatchAlert, MaxAckPending: 64, AckWait: 30 * time.Second, Concurrency: 2, Handle: n.handle})
	if err != nil {
		return nil, err
	}
	worker, err := NewBase(b, metrics, log, Spec{Name: "notifier-deliveries", Producer: true, Produce: n.run})
	if err != nil {
		return nil, err
	}
	return &notifierAgent{Base: consumer, worker: worker}, nil
}

type notifierAgent struct {
	*Base
	worker *Base
}

func (n *notifierAgent) Start(ctx context.Context) error {
	if err := n.Base.Start(ctx); err != nil {
		return err
	}
	if err := n.worker.Start(ctx); err != nil {
		_ = n.Base.Stop(ctx)
		return err
	}
	return nil
}
func (n *notifierAgent) Stop(ctx context.Context) error {
	return errors.Join(n.worker.Stop(ctx), n.Base.Stop(ctx))
}
func (n *notifierAgent) Consume(ctx context.Context) error {
	if err := n.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
func (n *notifierAgent) Running() bool { return n.Base.Running() && n.worker.Running() }

type notifier struct {
	cfg NotifierConfig
	log *slog.Logger
}

func (n *notifier) handle(ctx context.Context, msg *Message, _ *Emitter) error {
	var h dorv1.AlertHit
	if err := proto.Unmarshal(msg.Data, &h); err != nil {
		return Terminal(fmt.Errorf("notifier: alert hit: %w", err))
	}
	if h.AlertId == "" || h.StoryId == "" {
		return Terminal(fmt.Errorf("notifier: missing hit identity"))
	}
	a, _, err := n.cfg.Alerts.Lookup(ctx, h.AlertId)
	if err != nil {
		return err
	}
	generation := h.Generation
	if h.Level == "escalated" && generation == 0 {
		generation = 1
	} // pre-migration stream records
	return n.cfg.Alerts.Enqueue(ctx, a, alert.Hit{AlertID: h.AlertId, StoryID: h.StoryId, Level: h.Level,
		Generation: generation, Approx: h.Approx, SourceCount: int(h.SourceCount),
		FiredAt: time.Unix(h.FiredAt, 0).UTC(), Title: h.Title, Prompt: h.Prompt})
}

func (n *notifier) run(ctx context.Context, _ *Emitter) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.drain(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (n *notifier) drain(ctx context.Context) {
	scanCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	pending, err := n.cfg.Alerts.Pending(scanCtx)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			n.log.Warn("notifier: pending scan failed", "err", err)
		}
		return
	}
	visited := map[string]bool{}
	for _, d := range pending {
		if ctx.Err() != nil {
			return
		}
		if visited[d.Hit.AlertID] || n.cfg.Now().Before(d.DueAt) {
			continue
		}
		visited[d.Hit.AlertID] = true
		if err := n.processAlert(ctx, d.Hit.AlertID); err != nil && ctx.Err() == nil {
			n.log.Warn("notifier: delivery retained for retry", "alert", d.Hit.AlertID, "err", err)
		}
	}
}

func (n *notifier) processAlert(ctx context.Context, id string) error {
	// All external calls and receipt writes finish before the 45-second lease.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	release, claimed, err := n.cfg.Alerts.ClaimAlert(ctx, id, n.cfg.Now(), 45*time.Second)
	if err != nil || !claimed {
		return err
	}
	defer release()
	// Re-read after claiming: another replica may have delayed/completed a task.
	pending, err := n.cfg.Alerts.Pending(ctx)
	if err != nil {
		return err
	}
	a, exists, err := n.cfg.Alerts.Lookup(ctx, id)
	if err != nil {
		return err
	}
	var ready []alert.Delivery
	for _, d := range pending {
		if d.Hit.AlertID != id || n.cfg.Now().Before(d.DueAt) {
			continue
		}
		done, err := n.cfg.Alerts.Completed(ctx, d.ID)
		if err != nil {
			return err
		}
		if done {
			if err := n.cfg.Alerts.CompleteDelivery(ctx, d); err != nil {
				return err
			}
			continue
		}
		if !exists || !a.Enabled || n.cfg.Alerts.IsMuted(ctx, id, d.Hit.StoryID) {
			if err := n.cfg.Alerts.CompleteDelivery(ctx, d); err != nil {
				return err
			}
			continue
		}
		ready = append(ready, d)
	}
	if len(ready) == 0 {
		return nil
	}
	d := ready[0]
	if d.Digest {
		var group []alert.Delivery
		for _, p := range ready {
			if p.Digest && p.DigestAt.Equal(d.DigestAt) {
				group = append(group, p)
			}
		}
		d, err = n.cfg.Alerts.FreezeDigest(ctx, a, d.DigestAt, group)
		if err != nil {
			return err
		}
	}
	if n.cfg.Now().Before(d.DueAt) {
		return nil
	}
	return n.deliver(ctx, a, d)
}

func (n *notifier) deliver(ctx context.Context, a alert.Alert, d alert.Delivery) error {
	var remaining []string
	for _, ch := range d.Channels {
		done, err := n.cfg.Alerts.ChannelDelivered(ctx, d.ID, ch)
		if err != nil {
			return err
		}
		if !done {
			remaining = append(remaining, ch)
		}
	}
	if len(remaining) == 0 {
		return n.cfg.Alerts.CompleteDelivery(ctx, d)
	}
	if !d.Reserved {
		allowed, next, err := n.cfg.Alerts.ReserveRate(ctx, a.ID, d.ID, n.cfg.Now(), n.cfg.MinInterval)
		if err != nil {
			return err
		}
		if !allowed {
			d.DueAt = next
			// Digest's scheduled day is its stable identity; use a retry time without
			// changing which input set belongs to this frozen batch.
			return n.delay(ctx, d, next)
		}
		d.Reserved = true
		if err := n.cfg.Alerts.SaveDelivery(ctx, d); err != nil {
			return err
		}
	}
	var errs []error
	for _, ch := range remaining {
		var err error
		switch ch {
		case "ntfy":
			err = n.sendNtfy(ctx, a, d.Hit)
		case "push":
			if n.cfg.Push == nil {
				err = errors.New("notifier: web push is not configured")
			} else {
				err = n.cfg.Push.Send(ctx, a, d.Hit)
			}
		}
		if err == nil {
			err = n.cfg.Alerts.MarkChannelDelivered(ctx, d.ID, ch)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		d.Attempts++
		d.LastError = errors.Join(errs...).Error()
		delay := time.Second * time.Duration(1<<min(d.Attempts, 10))
		if err := n.delay(ctx, d, n.cfg.Now().Add(delay)); err != nil {
			return err
		}
		return errors.Join(errs...)
	}
	return n.cfg.Alerts.CompleteDelivery(ctx, d)
}

func (n *notifier) delay(ctx context.Context, d alert.Delivery, next time.Time) error {
	if len(d.Members) == 0 {
		d.DueAt = next
		return n.cfg.Alerts.SaveDelivery(ctx, d)
	}
	// Persist batch retries independently; retain the day in the digest record.
	if err := n.cfg.Alerts.SaveDelivery(ctx, d); err != nil {
		return err
	}
	// Member due times are scheduling hints. Keep their digest day for grouping;
	// the frozen batch has the actual retry deadline.
	return n.cfg.Alerts.DelayDigest(ctx, d, next)
}

func (n *notifier) sendNtfy(ctx context.Context, a alert.Alert, h alert.Hit) error {
	if n.cfg.NtfyBaseURL == "" || a.NtfyTopic == "" {
		return errors.New("notifier: ntfy is not configured")
	}
	body := h.Title + "\n" + h.Prompt
	if h.Approx {
		body = "Approximate match\n" + body
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(n.cfg.NtfyBaseURL, "/")+"/"+a.NtfyTopic, bytes.NewBufferString(body))
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
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("notifier: ntfy HTTP %d", resp.StatusCode)
	}
	return nil
}
