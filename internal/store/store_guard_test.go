package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestOpenRefusesLiveDatabaseFromTestBinary proves the guard fires before any
// pool is created or pinged. The live host is never contacted: Open returns at
// the database-name check, which is also why every case must finish quickly.
func TestOpenRefusesLiveDatabaseFromTestBinary(t *testing.T) {
	t.Setenv(AllowTestLiveDBEnv, "")
	t.Setenv("PGDATABASE", "")

	tests := []struct {
		name string
		url  string
	}{
		{
			name: "query string present",
			url:  "postgres://dor:dor@127.0.0.1:5433/dor?sslmode=disable",
		},
		{
			name: "no query string",
			url:  "postgres://dor:dor@127.0.0.1:5433/dor",
		},
		{
			// pgx leaves the database empty and the server then defaults it to
			// the user name, so this DSN also resolves to the live "dor"
			// database. Use TEST-NET-1 so a missed guard can never reach a
			// real Postgres.
			name: "database omitted defaults to user",
			url:  "postgres://dor:dor@192.0.2.1:5433",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			_, err := Open(context.Background(), Config{URL: tc.url, ConnectTimeout: 100 * time.Millisecond})
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("Open accepted the live database from a test binary")
			}
			msg := err.Error()
			if !strings.Contains(msg, `refusing live database "dor"`) {
				t.Fatalf("error = %q, want the live-database refusal", msg)
			}
			if !strings.Contains(msg, AllowTestLiveDBEnv) {
				t.Fatalf("error = %q, want the %s opt-in mentioned", msg, AllowTestLiveDBEnv)
			}
			if elapsed > 2*time.Second {
				t.Fatalf("guard blocked for %s; it must refuse before the network ping", elapsed)
			}
		})
	}
}

// TestOpenAllowsNonLiveDatabaseNameFromTestBinary uses the scratch database
// only when DOR_TEST_DATABASE_URL is set and does not name the live database.
func TestOpenAllowsNonLiveDatabaseNameFromTestBinary(t *testing.T) {
	url := strings.TrimSpace(os.Getenv(testDatabaseEnv))
	if url == "" {
		t.Skipf("set %s to a scratch database to run this test", testDatabaseEnv)
	}
	pc, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse %s: %v", testDatabaseEnv, err)
	}
	if pc.ConnConfig.Database == liveDatabase {
		t.Skipf("%s names the live %q database; refusing to open it", testDatabaseEnv, liveDatabase)
	}

	t.Setenv(AllowTestLiveDBEnv, "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := Open(ctx, Config{URL: url, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open(scratch database %q): %v", pc.ConnConfig.Database, err)
	}
	defer s.Close()
	if got := s.Pool(); got == nil {
		t.Fatal("Open returned a store without a pool")
	}
}

func TestIsTestBinaryAndLiveDatabaseGuardHelpers(t *testing.T) {
	if !isTestBinary() {
		t.Fatal("isTestBinary() = false inside a go test process")
	}
	if liveDatabase != "dor" {
		t.Fatalf("liveDatabase = %q, want dor", liveDatabase)
	}
	if AllowTestLiveDBEnv != "DOR_ALLOW_TEST_LIVE_DB" {
		t.Fatalf("AllowTestLiveDBEnv = %q", AllowTestLiveDBEnv)
	}

	// Parsing must expose the explicit database name the guard compares. The
	// omitted-database hole is covered end-to-end by the refusal test above.
	pc, err := pgxpool.ParseConfig("postgres://dor:dor@127.0.0.1:5433/dor?sslmode=disable")
	if err != nil {
		t.Fatalf("parse live URL: %v", err)
	}
	if pc.ConnConfig.Database != liveDatabase {
		t.Fatalf("parsed database = %q, want %q", pc.ConnConfig.Database, liveDatabase)
	}
	if pc.ConnConfig.User != "dor" {
		t.Fatalf("parsed user = %q, want dor", pc.ConnConfig.User)
	}
	omitted, err := pgxpool.ParseConfig("postgres://dor:dor@192.0.2.1:5433")
	if err != nil {
		t.Fatalf("parse omitted-database URL: %v", err)
	}
	// pgx leaves this empty; Postgres resolves it to the user name. Open must
	// normalise it before comparing against liveDatabase.
	if omitted.ConnConfig.Database != "" || omitted.ConnConfig.User != "dor" {
		t.Fatalf("omitted DSN parsed as database %q user %q", omitted.ConnConfig.Database, omitted.ConnConfig.User)
	}
}
