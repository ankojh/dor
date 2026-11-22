// Package session issues a signed anonymous browser session. Dor has no
// accounts in v1; a session is an opaque owner id for saved alerts, carried
// in an HttpOnly cookie and signed so a client cannot choose someone else's
// id. The same id also keys per-client REST admission.
package session

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

// CookieName is the browser session cookie.
const CookieName = "dor_session"

// TTL is long-lived: the id owns saved alerts and should survive a browser
// restart. The HMAC gives it integrity, not confidentiality; it carries no
// personal data.
const TTL = 365 * 24 * time.Hour

type contextKey struct{}

type contextInfo struct {
	id     string
	issued bool
}

// Signer issues and verifies session tokens.
type Signer struct {
	secret [32]byte
}

// NewSigner creates a signer with a fresh process secret. Use it for unit
// tests; production uses LoadOrCreate so sessions survive restarts.
func NewSigner() (*Signer, error) {
	s := &Signer{}
	if _, err := rand.Read(s.secret[:]); err != nil {
		return nil, err
	}
	return s, nil
}

// LoadOrCreate reads the signing secret from the durable session bucket, or
// creates one atomically. A shared secret lets every gateway replica verify a
// browser session and keeps saved alerts reachable after a restart.
func LoadOrCreate(ctx context.Context, b *bus.Bus) (*Signer, error) {
	if b == nil {
		return nil, errors.New("session: bus is required")
	}
	kv, err := b.KV(ctx, bus.KVBucketSession)
	if err != nil {
		return nil, fmt.Errorf("session: bucket: %w", err)
	}
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	secret, err := readSecret(opCtx, kv)
	if err == nil {
		s := &Signer{}
		copy(s.secret[:], secret)
		return s, nil
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, fmt.Errorf("session: read secret: %w", err)
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	if _, err := kv.Create(opCtx, "secret", raw[:]); err != nil {
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, fmt.Errorf("session: create secret: %w", err)
		}
		secret, err = readSecret(opCtx, kv)
		if err != nil {
			return nil, fmt.Errorf("session: read competing secret: %w", err)
		}
		copy(raw[:], secret)
	}
	s := &Signer{}
	copy(s.secret[:], raw[:])
	return s, nil
}

func readSecret(ctx context.Context, kv jetstream.KeyValue) ([]byte, error) {
	entry, err := kv.Get(ctx, "secret")
	if err != nil {
		return nil, err
	}
	if len(entry.Value()) != 32 {
		return nil, errors.New("session: secret has wrong length")
	}
	return append([]byte(nil), entry.Value()...), nil
}

// Issue returns a fresh session id and its signed cookie token.
func (s *Signer) Issue() (id, token string, err error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", "", err
	}
	id = hex.EncodeToString(raw[:])
	return id, id + "." + s.sign(id), nil
}

// Verify returns the session id when token has a valid signature.
func (s *Signer) Verify(token string) (string, bool) {
	id, signature, ok := strings.Cut(strings.TrimSpace(token), ".")
	if !ok || len(id) != 64 || len(signature) != 64 {
		return "", false
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(signature), []byte(s.sign(id))) != 1 {
		return "", false
	}
	return id, true
}

func (s *Signer) sign(id string) string {
	mac := hmac.New(sha256.New, s.secret[:])
	_, _ = mac.Write([]byte(id))
	return hex.EncodeToString(mac.Sum(nil))
}

// FromRequest returns the verified session id and cookie token, if present.
func (s *Signer) FromRequest(r *http.Request) (id, token string, ok bool) {
	cookie, err := r.Cookie(CookieName)
	if err != nil {
		return "", "", false
	}
	id, ok = s.Verify(cookie.Value)
	if !ok {
		return "", "", false
	}
	return id, cookie.Value, true
}

// Cookie builds the HttpOnly session cookie.
func (s *Signer) Cookie(token string, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(TTL.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
	}
}

// Middleware ensures every request has a valid anonymous session and puts its
// id in the request context. A missing or tampered cookie gets a fresh id.
func (s *Signer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, token, ok := s.FromRequest(r)
		issued := !ok
		if issued {
			var err error
			id, token, err = s.Issue()
			if err != nil {
				http.Error(w, "session unavailable", http.StatusInternalServerError)
				return
			}
			http.SetCookie(w, s.Cookie(token, r.TLS != nil))
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, contextInfo{id: id, issued: issued})))
	})
}

// FromContext returns the verified session id placed by Middleware.
func FromContext(ctx context.Context) string {
	info, _ := ctx.Value(contextKey{}).(contextInfo)
	return info.id
}

// Issued reports whether Middleware created the session on this request. A
// first-time browser request is keyed by address for admission; its later
// requests use the stable session id.
func Issued(r *http.Request) bool {
	info, _ := r.Context().Value(contextKey{}).(contextInfo)
	return info.issued
}

// IDFromRequest is the handler shorthand for FromContext(r.Context()).
func IDFromRequest(r *http.Request) string {
	return FromContext(r.Context())
}
