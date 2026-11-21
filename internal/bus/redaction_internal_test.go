package bus

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestRedactTextKeepsAtInQuery(t *testing.T) {
	const text = "wss://gateway.example/ws?contact=ops@example.org"
	if got := redactText(text); got != text {
		t.Fatalf("redactText(%q) = %q; an @ outside the authority must be preserved", text, got)
	}
}

func TestRedactErrorHandlesConfiguredComponentForms(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		secret string
	}{
		{"apostrophe", "nats://user:dummy'apostrophe-secret@127.0.0.1:bad", "dummy'apostrophe-secret"},
		{"quote", `nats://user:dummy"quote-secret@127.0.0.1:bad`, `dummy"quote-secret`},
		{"space", "nats://user:dummy space-secret@127.0.0.1:bad", "dummy space-secret"},
		{"percent40", "nats://user:dummy%40secret@127.0.0.1:bad", "dummy%40secret"},
		{"unicode", "nats://user:dummy-秘密@127.0.0.1:bad", "dummy-秘密"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Exercise both the raw form and the quoted form nats.Connect emits.
			for _, text := range []string{
				fmt.Sprintf("parse %q: invalid port", tt.raw),
				fmt.Sprintf("parse %s: invalid port", tt.raw),
			} {
				got := redactError(errors.New(text), tt.raw).Error()
				if strings.Contains(got, tt.secret) {
					t.Fatalf("redacted error still contains %q: %s", tt.secret, got)
				}
				if !strings.Contains(got, "nats://127.0.0.1:bad") {
					t.Fatalf("redacted error lost the safe URL: %s", got)
				}
			}
		})
	}
}

func TestRedactErrorHandlesMalformedCommaComponent(t *testing.T) {
	rawURL := "nats://user:comma-secret@127.0.0.1:bad,nats://127.0.0.1:1"
	err := fmt.Errorf(`parse "nats://user:comma-secret@127.0.0.1:bad": invalid port ":bad" after host`)
	got := redactError(err, rawURL).Error()
	if strings.Contains(got, "comma-secret") {
		t.Fatalf("redacted comma-list error still contains credential: %s", got)
	}
}

func TestRedactErrorHandlesURLNormalizedForm(t *testing.T) {
	raw := "nats://user:norm-secret@127.0.0.1:4222/path with space"
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	normalized := parsed.String()
	if normalized == raw {
		t.Fatalf("test URL did not normalize: %q", raw)
	}
	got := redactError(fmt.Errorf("parse %q: invalid URL", normalized), raw).Error()
	if strings.Contains(got, "norm-secret") {
		t.Fatalf("redacted normalized URL still contains credential: %s", got)
	}
}
