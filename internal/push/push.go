// Package push sends Web Push notifications for alert hits. Subscriptions are
// stored AES-GCM encrypted by the gateway and only decrypted here, immediately
// before a send; VAPID private keys never leave the notifier process.
package push

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"

	"github.com/ankojh/dor/internal/alert"
)

// Subscription is the browser PushSubscription JSON shape. Keys are the
// base64 values from PushSubscription.toJSON().
type Subscription struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		Auth   string `json:"auth"`
		P256DH string `json:"p256dh"`
	} `json:"keys"`
}

// Config configures the VAPID sender.
type Config struct {
	Alerts        *alert.Store
	EncryptionKey []byte
	VAPIDPublic   string
	VAPIDPrivate  string
	// Subject is the VAPID contact. Either an email-ish value
	// (ops@example.com) or an https URL; a leading mailto: is accepted and
	// stripped because the Web Push library adds it.
	Subject    string
	TTL        int
	Timeout    time.Duration
	HTTPClient *http.Client
	Logger     *slog.Logger
	Now        func() time.Time
}

// Sender fans one alert hit out to every stored subscription.
type Sender struct {
	alerts     *alert.Store
	key        []byte
	publicKey  string
	privateKey string
	subject    string
	ttl        int
	timeout    time.Duration
	http       *http.Client
	log        *slog.Logger
	now        func() time.Time
}

// NewSender validates the VAPID and encryption material. Missing or malformed
// material is an error so the caller can leave push explicitly unavailable.
func NewSender(cfg Config) (*Sender, error) {
	if cfg.Alerts == nil {
		return nil, errors.New("push: alert store is required")
	}
	if len(cfg.EncryptionKey) != 32 {
		return nil, errors.New("push: encryption key must be 32 bytes")
	}
	publicKey := strings.TrimSpace(cfg.VAPIDPublic)
	privateKey := strings.TrimSpace(cfg.VAPIDPrivate)
	subject := normalizeSubject(cfg.Subject)
	if publicKey == "" || privateKey == "" {
		return nil, errors.New("push: VAPID public and private keys are required")
	}
	if subject == "" {
		return nil, errors.New("push: VAPID subject is required")
	}
	publicBytes, err := decodeKey(publicKey)
	if err != nil || len(publicBytes) != 65 {
		return nil, errors.New("push: VAPID public key is not a 65-byte P-256 key")
	}
	privateBytes, err := decodeKey(privateKey)
	if err != nil || len(privateBytes) != 32 {
		return nil, errors.New("push: VAPID private key is not a 32-byte P-256 key")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 20 * time.Second
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 3600
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	return &Sender{
		alerts:     cfg.Alerts,
		key:        append([]byte(nil), cfg.EncryptionKey...),
		publicKey:  publicKey,
		privateKey: privateKey,
		subject:    subject,
		ttl:        cfg.TTL,
		timeout:    cfg.Timeout,
		http:       cfg.HTTPClient,
		log:        cfg.Logger,
		now:        cfg.Now,
	}, nil
}

// Send delivers one alert hit to every subscription that has not already
// received this exact (alert, story, level). It returns an error when at least
// one transient delivery failed, which lets the notifier's retry/backoff/DLQ
// path take over. Expired subscriptions (404/410) are removed immediately.
func (s *Sender) Send(ctx context.Context, a alert.Alert, h alert.Hit) error {
	records := s.alerts.PushSubscriptions(ctx)
	if len(records) == 0 {
		s.log.Debug("push: no subscriptions", "alert", a.ID)
		return nil
	}
	payload, err := json.Marshal(buildNotificationPayload(a, h))
	if err != nil {
		return fmt.Errorf("push: marshal payload: %w", err)
	}
	var errs []error
	sent := 0
	for _, record := range records {
		if s.alerts.PushDelivered(ctx, h.AlertID, h.StoryID, h.Level, record.ID) {
			continue
		}
		sub, err := DecryptSubscription(s.key, record.Blob)
		if err != nil {
			// A corrupt blob can never become valid; drop it rather than
			// retrying every alert forever.
			s.log.Warn("push: dropping unreadable subscription", "id", record.ID, "err", err)
			s.alerts.DeletePush(ctx, record.ID)
			continue
		}
		err = s.sendOne(ctx, sub, payload)
		if errors.Is(err, errSubscriptionGone) {
			s.log.Info("push: subscription expired; removed", "id", record.ID)
			s.alerts.DeletePush(ctx, record.ID)
			s.alerts.MarkPushDelivered(ctx, h.AlertID, h.StoryID, h.Level, record.ID)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("push: subscription %s: %w", record.ID, err))
			continue
		}
		s.alerts.MarkPushDelivered(ctx, h.AlertID, h.StoryID, h.Level, record.ID)
		sent++
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if sent > 0 {
		s.log.Info("push: delivered", "alert", a.ID, "story", h.StoryID, "level", h.Level, "subscriptions", sent)
	}
	return nil
}

var errSubscriptionGone = errors.New("push: subscription expired")

func (s *Sender) sendOne(ctx context.Context, sub Subscription, payload []byte) error {
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	resp, err := webpush.SendNotificationWithContext(requestCtx,
		payload,
		&webpush.Subscription{
			Endpoint: sub.Endpoint,
			Keys: webpush.Keys{
				Auth:   sub.Keys.Auth,
				P256dh: sub.Keys.P256DH,
			},
		},
		&webpush.Options{
			HTTPClient:      s.http,
			Subscriber:      s.subject,
			TTL:             s.ttl,
			Urgency:         webpush.UrgencyHigh,
			VAPIDPublicKey:  s.publicKey,
			VAPIDPrivateKey: s.privateKey,
		})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("push: empty response")
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 16<<10))
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		return errSubscriptionGone
	case resp.StatusCode >= 200 && resp.StatusCode <= 299:
		return nil
	default:
		return fmt.Errorf("push: HTTP %d", resp.StatusCode)
	}
}

// notificationPayload is the JSON the service worker receives. Keep this in
// sync with the frontend push contract.
type notificationPayload struct {
	Title   string `json:"title"`
	Body    string `json:"body"`
	AlertID string `json:"alert_id"`
	StoryID string `json:"story_id"`
	Prompt  string `json:"prompt"`
	Level   string `json:"level"`
	URL     string `json:"url"`
	Tag     string `json:"tag"`
	FiredAt int64  `json:"fired_at"`
}

func buildNotificationPayload(a alert.Alert, h alert.Hit) notificationPayload {
	if h.AlertID == "" {
		h.AlertID = a.ID
	}
	title := strings.TrimSpace(h.Title)
	if title == "" {
		title = "Dor alert"
	}
	body := strings.TrimSpace(h.Prompt)
	if h.Level == "escalated" {
		body = "Escalated — " + body
	}
	return notificationPayload{
		Title:   title,
		Body:    body,
		AlertID: h.AlertID,
		StoryID: h.StoryID,
		Prompt:  h.Prompt,
		Level:   h.Level,
		URL:     "/?story=" + url.QueryEscape(h.StoryID) + "&alert=" + url.QueryEscape(h.AlertID),
		Tag:     "dor-alert:" + h.AlertID + ":" + h.StoryID,
		FiredAt: h.FiredAt.Unix(),
	}
}

// EncryptSubscription seals the subscription JSON with AES-GCM:
// nonce || ciphertext.
func EncryptSubscription(key []byte, sub Subscription) ([]byte, error) {
	plain, err := json.Marshal(sub)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plain, nil), nil
}

// DecryptSubscription opens a blob produced by EncryptSubscription.
func DecryptSubscription(key, blob []byte) (Subscription, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return Subscription{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return Subscription{}, err
	}
	if len(blob) < gcm.NonceSize() {
		return Subscription{}, errors.New("push: subscription blob is truncated")
	}
	plain, err := gcm.Open(nil, blob[:gcm.NonceSize()], blob[gcm.NonceSize():], nil)
	if err != nil {
		return Subscription{}, err
	}
	var sub Subscription
	if err := json.Unmarshal(plain, &sub); err != nil {
		return Subscription{}, err
	}
	if sub.Endpoint == "" || sub.Keys.Auth == "" || sub.Keys.P256DH == "" {
		return Subscription{}, errors.New("push: subscription is incomplete")
	}
	return sub, nil
}

func normalizeSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if len(subject) >= len("mailto:") && strings.EqualFold(subject[:len("mailto:")], "mailto:") {
		subject = strings.TrimSpace(subject[len("mailto:"):])
	}
	if strings.HasPrefix(subject, "https://") {
		return subject
	}
	if strings.Contains(subject, "@") {
		return subject
	}
	return ""
}

func decodeKey(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	encodings := []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	}
	var lastErr error
	for _, encoding := range encodings {
		decoded, err := encoding.DecodeString(raw)
		if err == nil {
			return decoded, nil
		}
		lastErr = err
	}
	return nil, lastErr
}
