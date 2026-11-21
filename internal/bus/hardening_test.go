package bus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

func TestOrdinaryAttachPreservesProvisionedPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, err := bus.Connect(ctx, bus.Options{
		ListenAddr:      "127.0.0.1:0",
		StoreDir:        t.TempDir(),
		DuplicateWindow: 10 * time.Minute,
		StreamMaxAge:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	stream, err := owner.StreamNamed(ctx, bus.StreamEcho)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxBytes = 1024 * 1024
	if _, err := owner.JS().UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	attached, err := bus.Connect(ctx, bus.Options{URL: owner.URL()})
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	if attached.Owner() {
		t.Error("ordinary client reports owner")
	}

	after, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Config.Duplicates != 10*time.Minute || after.Config.MaxAge != time.Hour || after.Config.MaxBytes != 1024*1024 {
		t.Errorf("attach rewrote policy: dup=%s age=%s bytes=%d",
			after.Config.Duplicates, after.Config.MaxAge, after.Config.MaxBytes)
	}
}

func TestExternalAttachRequiresProvisionedStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	for _, name := range bus.StreamOrder() {
		if err := owner.JS().DeleteStream(ctx, name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}
	if _, err := bus.Connect(ctx, bus.Options{URL: owner.URL()}); err == nil {
		t.Fatal("attaching client accepted an unprovisioned bus")
	}
}

func TestProvisionExternalClient(t *testing.T) {
	// An embedded owner starts the server, but an external client with
	// Provision set is also allowed to create the topology.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	for _, name := range bus.StreamOrder() {
		if err := owner.JS().DeleteStream(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	external, err := bus.Connect(ctx, bus.Options{URL: owner.URL(), Provision: true})
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	if !external.Owner() {
		t.Error("provisioning external client does not report owner")
	}
	if _, err := external.StreamNamed(ctx, bus.StreamIngest); err != nil {
		t.Fatal(err)
	}
}

func TestRedactedURL(t *testing.T) {
	cases := map[string]string{
		"nats://user:pass@example.com:4222":                "nats://example.com:4222",
		"nats://token@example.com:4222, nats://b:2@h:4222": "nats://example.com:4222,nats://h:4222",
		"nats://example.com:4222":                          "nats://example.com:4222",
		"user:pass@example.com:4222":                       "example.com:4222",
		"":                                                 "",
	}
	for in, want := range cases {
		if got := bus.RedactedURL(in); got != want {
			t.Errorf("RedactedURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStreamConfigsHaveBoundedRetention(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	policies := bus.DefaultStreamPolicies()
	for _, name := range bus.StreamOrder() {
		stream, err := b.StreamNamed(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := policies[name]
		if info.Config.MaxAge <= 0 || info.Config.MaxBytes <= 0 {
			t.Errorf("%s is unbounded: age=%s bytes=%d", name, info.Config.MaxAge, info.Config.MaxBytes)
		}
		if info.Config.MaxAge != want.MaxAge || info.Config.MaxBytes != want.MaxBytes {
			t.Errorf("%s policy mismatch: age=%s/%s bytes=%d/%d", name,
				info.Config.MaxAge, want.MaxAge, info.Config.MaxBytes, want.MaxBytes)
		}
		if name == bus.StreamMatch && info.Config.Storage != jetstream.MemoryStorage {
			t.Errorf("MATCH storage = %v, want memory", info.Config.Storage)
		}
	}
}

func TestConnectErrorRedactsUserinfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := bus.Connect(ctx, bus.Options{URL: "nats://user:super-secret@127.0.0.1:1"})
	if err == nil {
		t.Fatal("connect unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("connect error leaks credentials: %v", err)
	}
}

func TestConnectErrorRedactsEveryComponentURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := bus.Connect(ctx, bus.Options{
		URL: "nats://review-user:dummy-review-secret@127.0.0.1:bad,nats://127.0.0.1:1",
	})
	if err == nil {
		t.Fatal("connect unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "dummy-review-secret") {
		t.Fatalf("component URL parse error leaks credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "nats://127.0.0.1:bad") {
		t.Fatalf("redacted error lost the component URL context: %v", err)
	}
}

func TestConnectErrorRedactsTrickyPasswords(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		also   []string
	}{
		{"apostrophe", "dummy'apostrophe-secret", nil},
		{"quote", `dummy"quote-secret`, nil},
		{"space", "dummy space-secret", nil},
		{"percent40", "dummy%40secret", []string{"dummy@secret"}},
		{"unicode", "dummy-秘密", nil},
		{"comma-component", "comma-secret", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "nats://user:" + tt.secret + "@127.0.0.1:bad,nats://127.0.0.1:1"
			if tt.name == "comma-component" {
				url = "nats://user:" + tt.secret + "@127.0.0.1:bad,nats://127.0.0.1:1"
			}
			_, err := bus.Connect(context.Background(), bus.Options{URL: url})
			if err == nil {
				t.Fatal("expected a connection/parse error")
			}
			msg := err.Error()
			if strings.Contains(msg, tt.secret) {
				t.Fatalf("Connect error leaked %q: %s", tt.secret, msg)
			}
			for _, secret := range tt.also {
				if strings.Contains(msg, secret) {
					t.Fatalf("Connect error leaked normalized secret %q: %s", secret, msg)
				}
			}
		})
	}
}
