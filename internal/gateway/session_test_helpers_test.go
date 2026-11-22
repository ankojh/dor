package gateway

import (
	"net/http"
	"testing"

	"github.com/ankojh/dor/internal/session"
)

// issueSession returns a signed session cookie and its owner id without going
// through an HTTP round trip.
func issueSession(t *testing.T, srv *Server) (*http.Cookie, string) {
	t.Helper()
	if srv.sessions == nil {
		t.Fatal("server has no session signer")
	}
	id, token, err := srv.sessions.Issue()
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	return &http.Cookie{Name: session.CookieName, Value: token, Path: "/"}, id
}
