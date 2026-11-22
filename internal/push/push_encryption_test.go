package push

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/ankojh/dor/internal/alert"
)

func pushTestSubscription() Subscription {
	sub := Subscription{Endpoint: "https://push.example/subscription"}
	sub.Keys.Auth = "auth-key"
	sub.Keys.P256DH = "p256dh-key"
	return sub
}

func pushTestEncryptionKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("read key: %v", err)
	}
	return key
}

func TestSubscriptionEncryptDecryptRoundTrip(t *testing.T) {
	key := pushTestEncryptionKey(t)
	want := pushTestSubscription()
	blob, err := EncryptSubscription(key, want)
	if err != nil {
		t.Fatalf("EncryptSubscription: %v", err)
	}
	if len(blob) == 0 {
		t.Fatal("EncryptSubscription returned an empty blob")
	}

	// Two encryptions of the same subscription must differ: a fresh nonce is
	// part of the sealed format.
	other, err := EncryptSubscription(key, want)
	if err != nil {
		t.Fatalf("EncryptSubscription second call: %v", err)
	}
	if string(blob) == string(other) {
		t.Fatal("encryption reused a nonce")
	}

	got, err := DecryptSubscription(key, blob)
	if err != nil {
		t.Fatalf("DecryptSubscription: %v", err)
	}
	if got.Endpoint != want.Endpoint || got.Keys.Auth != want.Keys.Auth || got.Keys.P256DH != want.Keys.P256DH {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestDecryptSubscriptionRejectsWrongKeyAndMalformedBlob(t *testing.T) {
	key := pushTestEncryptionKey(t)
	blob, err := EncryptSubscription(key, pushTestSubscription())
	if err != nil {
		t.Fatalf("EncryptSubscription: %v", err)
	}

	t.Run("wrong key", func(t *testing.T) {
		if _, err := DecryptSubscription(pushTestEncryptionKey(t), blob); err == nil {
			t.Fatal("DecryptSubscription accepted a different key")
		}
	})
	t.Run("short key", func(t *testing.T) {
		if _, err := DecryptSubscription([]byte("short"), blob); err == nil {
			t.Fatal("DecryptSubscription accepted a short key")
		}
	})
	t.Run("truncated blob", func(t *testing.T) {
		if _, err := DecryptSubscription(key, []byte{1, 2, 3}); err == nil {
			t.Fatal("DecryptSubscription accepted a truncated blob")
		}
	})
	t.Run("tampered ciphertext", func(t *testing.T) {
		tampered := append([]byte(nil), blob...)
		tampered[len(tampered)-1] ^= 0xff
		if _, err := DecryptSubscription(key, tampered); err == nil {
			t.Fatal("DecryptSubscription accepted a tampered blob")
		}
	})
	t.Run("incomplete subscription", func(t *testing.T) {
		incomplete, err := EncryptSubscription(key, Subscription{Endpoint: "https://push.example/subscription"})
		if err != nil {
			t.Fatalf("EncryptSubscription incomplete: %v", err)
		}
		if _, err := DecryptSubscription(key, incomplete); err == nil {
			t.Fatal("DecryptSubscription accepted a subscription without keys")
		}
	})
}

func TestEncryptSubscriptionRejectsBadKey(t *testing.T) {
	if _, err := EncryptSubscription([]byte("too-short"), pushTestSubscription()); err == nil {
		t.Fatal("EncryptSubscription accepted a non-AES key length")
	}
}

func validVAPIDKeys(t *testing.T) (publicKey, privateKey string) {
	t.Helper()
	public := make([]byte, 65)
	private := make([]byte, 32)
	if _, err := rand.Read(public); err != nil {
		t.Fatalf("read public key: %v", err)
	}
	if _, err := rand.Read(private); err != nil {
		t.Fatalf("read private key: %v", err)
	}
	// 0x04 is the uncompressed P-256 point prefix the webpush library expects.
	public[0] = 0x04
	return base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(private)
}

func TestNewSenderValidation(t *testing.T) {
	_, st := pushTestStore(t)
	publicKey, privateKey := validVAPIDKeys(t)
	valid := Config{
		Alerts:        st,
		EncryptionKey: pushTestEncryptionKey(t),
		VAPIDPublic:   publicKey,
		VAPIDPrivate:  privateKey,
		Subject:       "mailto:ops@example.com",
	}

	if _, err := NewSender(valid); err != nil {
		t.Fatalf("NewSender(valid): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing alert store", mutate: func(c *Config) { c.Alerts = nil }},
		{name: "missing encryption key", mutate: func(c *Config) { c.EncryptionKey = nil }},
		{name: "short encryption key", mutate: func(c *Config) { c.EncryptionKey = make([]byte, 16) }},
		{name: "missing public key", mutate: func(c *Config) { c.VAPIDPublic = "" }},
		{name: "missing private key", mutate: func(c *Config) { c.VAPIDPrivate = "" }},
		{name: "malformed public key size", mutate: func(c *Config) {
			c.VAPIDPublic = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
		}},
		{name: "malformed private key size", mutate: func(c *Config) {
			c.VAPIDPrivate = base64.RawURLEncoding.EncodeToString(make([]byte, 31))
		}},
		{name: "unparseable public key", mutate: func(c *Config) { c.VAPIDPublic = "not base64 !!" }},
		{name: "missing subject", mutate: func(c *Config) { c.Subject = "" }},
		{name: "invalid subject", mutate: func(c *Config) { c.Subject = "not-a-contact" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := NewSender(cfg); err == nil {
				t.Fatal("NewSender accepted an invalid config")
			}
		})
	}
}

func TestNewSenderAcceptsHTTPSAndBareEmailSubjects(t *testing.T) {
	_, st := pushTestStore(t)
	publicKey, privateKey := validVAPIDKeys(t)
	for _, subject := range []string{"https://example.com/contact", "ops@example.com", "MAILTO:ops@example.com"} {
		if _, err := NewSender(Config{
			Alerts:        st,
			EncryptionKey: pushTestEncryptionKey(t),
			VAPIDPublic:   publicKey,
			VAPIDPrivate:  privateKey,
			Subject:       subject,
		}); err != nil {
			t.Errorf("NewSender subject %q: %v", subject, err)
		}
	}
}

func TestSendWithNoSubscriptionsMakesNoHTTPCall(t *testing.T) {
	_, st := pushTestStore(t)
	transport := &countRoundTripper{}
	sender := &Sender{
		alerts: st,
		key:    pushTestEncryptionKey(t),
		http:   &http.Client{Transport: transport},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	a, err := st.Create(context.Background(), alert.Alert{ID: "alert", Prompt: "wildfire", Enabled: true, Channels: []string{"push"}})
	if err != nil {
		t.Fatal(err)
	}
	hit := alert.Hit{AlertID: a.ID, StoryID: "story", Level: "new", Title: "Wildfire", Prompt: "wildfire"}
	if err := sender.Send(context.Background(), a, hit); err != nil {
		t.Fatalf("Send with no subscriptions: %v", err)
	}
	if got := transport.calls.Load(); got != 0 {
		t.Fatalf("HTTP calls = %d, want none with no subscriptions", got)
	}
}
