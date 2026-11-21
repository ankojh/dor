package alert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
)

// Delivery is a durable pending notification. Digest batches freeze their input
// IDs before the first external send, so retries produce the same summary.
type Delivery struct {
	ID        string    `json:"id"`
	Hit       Hit       `json:"hit"`
	Channels  []string  `json:"channels"`
	DueAt     time.Time `json:"due_at"`
	DigestAt  time.Time `json:"digest_at,omitempty"`
	Digest    bool      `json:"digest"`
	Members   []string  `json:"members,omitempty"`
	Attempts  int       `json:"attempts"`
	Reserved  bool      `json:"reserved"`
	LastError string    `json:"last_error,omitempty"`
}

// NextDigest is the next configured UTC hour strictly after the firing.
func NextDigest(at time.Time, hour int) time.Time {
	at = at.UTC()
	due := time.Date(at.Year(), at.Month(), at.Day(), hour, 0, 0, 0, time.UTC)
	if !due.After(at) {
		due = due.AddDate(0, 0, 1)
	}
	return due
}

// Enqueue is idempotent per hit generation. Only acknowledge match.alert after
// this succeeds. The bucket has no TTL: a provider outage cannot expire work.
func (s *Store) Enqueue(ctx context.Context, a Alert, h Hit) error {
	channels := make([]string, 0, 2)
	for _, ch := range a.Channels {
		if ch == "push" || ch == "ntfy" {
			channels = append(channels, ch)
		}
	}
	d := Delivery{ID: h.Identity(), Hit: h, Channels: channels, DueAt: h.FiredAt, Digest: a.DailyDigest}
	if d.Digest {
		d.DueAt = NextDigest(h.FiredAt, a.DigestHourUTC)
		d.DigestAt = d.DueAt
	}
	// A hit keeps its first routing choice and date across duplicate input,
	// preference changes, and a matcher retry that regenerates FiredAt.
	done, err := s.Completed(ctx, d.ID)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	routeKey := "route." + d.ID
	if _, err = s.delivery.Create(ctx, routeKey, data); errors.Is(err, jetstream.ErrKeyExists) {
		e, readErr := s.delivery.Get(ctx, routeKey)
		if readErr != nil {
			return readErr
		}
		data = e.Value()
	} else if err != nil {
		return err
	}
	_, err = s.delivery.Create(ctx, "pending."+d.ID, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return nil
	}
	return err
}

func (s *Store) walkDelivery(ctx context.Context, pattern string, visit func(jetstream.KeyValueEntry) error) error {
	w, err := s.delivery.Watch(ctx, pattern, jetstream.IgnoreDeletes())
	if err != nil {
		return err
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				return errors.New("alert: delivery scan interrupted")
			}
			if e == nil {
				return nil
			}
			if err := visit(e); err != nil {
				return err
			}
		}
	}
}

func (s *Store) Pending(ctx context.Context) ([]Delivery, error) {
	var out []Delivery
	err := s.walkDelivery(ctx, "pending.>", func(e jetstream.KeyValueEntry) error {
		var d Delivery
		if err := json.Unmarshal(e.Value(), &d); err != nil {
			return fmt.Errorf("alert: invalid delivery %s: %w", e.Key(), err)
		}
		out = append(out, d)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].DueAt.Equal(out[j].DueAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].DueAt.Before(out[j].DueAt)
	})
	return out, err
}

func (s *Store) SaveDelivery(ctx context.Context, d Delivery) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	prefix := "pending."
	if len(d.Members) > 0 {
		prefix = "digest."
	}
	_, err = s.delivery.Put(ctx, prefix+d.ID, data)
	return err
}

func (s *Store) CompleteDelivery(ctx context.Context, d Delivery) error {
	ids := d.Members
	if len(ids) == 0 {
		ids = []string{d.ID}
	}
	for _, id := range ids {
		if _, err := s.delivery.Put(ctx, "complete."+id, []byte("true")); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if err := s.delivery.Delete(ctx, "pending."+id); err != nil {
			return err
		}
	}
	return nil
}

// Completed retains a per-hit tombstone independent of mode/day/channel routing.
func (s *Store) Completed(ctx context.Context, id string) (bool, error) {
	_, err := s.delivery.Get(ctx, "complete."+id)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// FreezeDigest creates at most one immutable batch per alert/UTC delivery day.
// A late arrival after a batch was frozen is rescheduled to the next day.
func (s *Store) FreezeDigest(ctx context.Context, a Alert, due time.Time, pending []Delivery) (Delivery, error) {
	id := identity(a.ID, "digest", due.UTC().Format("2006-01-02"))
	key := "digest." + id
	var batch Delivery
	e, err := s.delivery.Get(ctx, key)
	if err == nil {
		if err = json.Unmarshal(e.Value(), &batch); err != nil {
			return Delivery{}, err
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return Delivery{}, err
	}
	if batch.ID == "" {
		sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })
		batch = Delivery{ID: id, DueAt: due, DigestAt: due, Hit: Hit{AlertID: a.ID, StoryID: "digest-" + due.UTC().Format("2006-01-02"), Level: "digest", FiredAt: due}, Channels: []string{}}
		var body strings.Builder
		shown := 0
		seen := map[string]bool{}
		for _, d := range pending {
			batch.Members = append(batch.Members, d.ID)
			for _, ch := range d.Channels {
				if !seen[ch] {
					seen[ch] = true
					batch.Channels = append(batch.Channels, ch)
				}
			}
			batch.Hit.Approx = batch.Hit.Approx || d.Hit.Approx
			label := ""
			if d.Hit.Approx {
				label = " (approx)"
			}
			title := d.Hit.Title
			if len(title) > 160 {
				title = title[:160]
				for !utf8.ValidString(title) {
					title = title[:len(title)-1]
				}
				title += "…"
			}
			if body.Len()+len(title)+len(label) < 1600 {
				body.WriteString(title + label + "\n")
				shown++
			}
		}
		batch.Hit.Title = fmt.Sprintf("Dor daily digest · %d alerts", len(pending))
		if shown < len(pending) {
			fmt.Fprintf(&body, "… and %d more", len(pending)-shown)
		}
		batch.Hit.Prompt = body.String()
		data, err := json.Marshal(batch)
		if err != nil {
			return Delivery{}, err
		}
		if _, err = s.delivery.Create(ctx, key, data); err != nil {
			return Delivery{}, err
		}
	}
	included := map[string]bool{}
	for _, id := range batch.Members {
		included[id] = true
	}
	for _, d := range pending {
		if !included[d.ID] {
			d.DueAt = due.AddDate(0, 0, 1)
			d.DigestAt = d.DueAt
			if err := s.SaveDelivery(ctx, d); err != nil {
				return Delivery{}, err
			}
		}
	}
	return batch, nil
}

// ChannelDelivered fails closed on receipt-store errors to avoid duplicate sends.
func (s *Store) ChannelDelivered(ctx context.Context, id, channel string) (bool, error) {
	_, err := s.delivery.Get(ctx, "receipt."+identity(id, channel))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}
func (s *Store) MarkChannelDelivered(ctx context.Context, id, channel string) error {
	_, err := s.delivery.Put(ctx, "receipt."+identity(id, channel), []byte("true"))
	return err
}

type rateReservation struct {
	At    time.Time `json:"at"`
	Owner string    `json:"owner"`
}

// ReserveRate atomically reserves an alert's external delivery slot. All
// notifier replicas contend on the same KV revision; errors never grant a slot.
func (s *Store) ReserveRate(ctx context.Context, alertID, owner string, now time.Time, min time.Duration) (bool, time.Time, error) {
	key := "rate." + identity(alertID)
	data, _ := json.Marshal(rateReservation{At: now, Owner: owner})
	for {
		e, err := s.delivery.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			_, err = s.delivery.Create(ctx, key, data)
		} else if err == nil {
			var last rateReservation
			if err = json.Unmarshal(e.Value(), &last); err != nil {
				return false, time.Time{}, err
			}
			if last.Owner == owner {
				return true, now, nil
			}
			if due := last.At.Add(min); now.Before(due) {
				return false, due, nil
			}
			_, err = s.delivery.Update(ctx, key, data, e.Revision())
		}
		if err == nil {
			return true, now, nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return false, time.Time{}, err
		}
		if ctx.Err() != nil {
			return false, time.Time{}, ctx.Err()
		}
	}
}

type deliveryLease struct {
	Until time.Time `json:"until"`
	Token string    `json:"token"`
}

// ClaimAlert serializes sends and digest freezing across replicas. The caller
// must finish within hold (including receipt writes), using a shorter deadline.
func (s *Store) ClaimAlert(ctx context.Context, alertID string, now time.Time, hold time.Duration) (func(), bool, error) {
	key := "lease." + identity(alertID)
	data, _ := json.Marshal(deliveryLease{Until: now.Add(hold), Token: randomHex(16)})
	e, err := s.delivery.Get(ctx, key)
	var revision uint64
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		revision, err = s.delivery.Create(ctx, key, data)
	} else if err == nil {
		var lease deliveryLease
		if err = json.Unmarshal(e.Value(), &lease); err != nil {
			return nil, false, err
		}
		if now.Before(lease.Until) {
			return nil, false, nil
		}
		revision, err = s.delivery.Update(ctx, key, data, e.Revision())
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.delivery.Delete(releaseCtx, key, jetstream.LastRevision(revision))
	}, true, nil
}

// DelayDigest moves the batch and all still-pending members to a retry deadline,
// while DigestAt continues to identify the frozen UTC day.
func (s *Store) DelayDigest(ctx context.Context, d Delivery, next time.Time) error {
	d.DueAt = next
	if err := s.SaveDelivery(ctx, d); err != nil {
		return err
	}
	for _, id := range d.Members {
		e, err := s.delivery.Get(ctx, "pending."+id)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var member Delivery
		if err = json.Unmarshal(e.Value(), &member); err != nil {
			return err
		}
		member.DueAt = next
		if err = s.SaveDelivery(ctx, member); err != nil {
			return err
		}
	}
	return nil
}
