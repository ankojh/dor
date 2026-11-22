package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary re-executes itself as `dor run` so the real signal handling
// and shutdown path run in a separate process without a separate build step.
func TestMain(m *testing.M) {
	if os.Getenv("DOR_TEST_AS_MAIN") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

func spawnDor(t *testing.T, args ...string) (*exec.Cmd, <-chan map[string]any, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"run"}, args...)...)
	cmd.Env = append(os.Environ(), "DOR_TEST_AS_MAIN=1")
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	lines := make(chan map[string]any, 256)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var rec map[string]any
			if json.Unmarshal(sc.Bytes(), &rec) == nil {
				lines <- rec
			}
		}
	}()
	return cmd, lines, stderr
}

func waitLog(t *testing.T, lines <-chan map[string]any, msg string) map[string]any {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case rec, ok := <-lines:
			if !ok {
				t.Fatalf("process output ended before log %q", msg)
			}
			if rec["msg"] == msg {
				return rec
			}
		case <-deadline:
			t.Fatalf("timed out waiting for log %q", msg)
		}
	}
}

func TestSignalsShutDownCleanly(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			cmd, lines, stderr := spawnDor(t,
				"--nats-listen", "127.0.0.1:0",
				"--nats-store", t.TempDir(),
				"--admin-addr", "",
				"--agents", "echo",
			)
			running := waitLog(t, lines, "dor running")
			if running["mode"] != "embedded" {
				t.Errorf("mode = %v, want embedded", running["mode"])
			}
			if url, _ := running["nats_url"].(string); strings.HasSuffix(url, ":4222") {
				t.Errorf("nats_url %q fell back to the default port", url)
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			shutting := waitLog(t, lines, "shutting down")
			if shutting["reason"] != "signal" {
				t.Errorf("reason = %v", shutting["reason"])
			}
			waitLog(t, lines, "stopped")

			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("exit not clean: %v\nstderr: %s", err, stderr.String())
				}
			case <-time.After(15 * time.Second):
				t.Fatal("process did not exit after signal")
			}
		})
	}
}

func TestBadAgentSelectionExitsNonZero(t *testing.T) {
	cmd, _, stderr := spawnDor(t,
		"--nats-listen", "127.0.0.1:0", "--nats-store", t.TempDir(),
		"--admin-addr", "", "--agents", "nope",
	)
	err := cmd.Wait()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr.String(), "unknown agent") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

// writeTestGeonames creates the three files geo.Load reads so a gateway-only
// process can load a tiny gazetteer without downloading GeoNames in a test.
func writeTestGeonames(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"countryInfo.txt":      "# test fixture\nFR\tFRA\t250\tEU\tFrance\tParis\n",
		"admin1CodesASCII.txt": "FR.11\tIle-de-France\tIle-de-France\t0\n",
		"cities15000.txt":      "2988507\tParis\tParis\t\t48.85341\t2.34880\tP\tPPL\tFR\t\t11\tIle-de-France\t11\t75000\t2138551\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestGatewayOnlyProcessStartsWithoutPublisher(t *testing.T) {
	cmd, lines, stderr := spawnDor(t,
		"--nats-listen", "127.0.0.1:0", "--nats-store", t.TempDir(),
		"--admin-addr", "", "--agents", "gateway",
		"--gateway-addr", "127.0.0.1:0", "--db-url", "off", "--tei-url", "",
		"--geonames-dir", writeTestGeonames(t),
		"--supervisor=false",
	)
	running := waitLog(t, lines, "dor running")
	if running["mode"] != "embedded" {
		t.Fatalf("mode=%v, want embedded", running["mode"])
	}
	names, ok := running["agents"].([]any)
	if !ok || len(names) != 0 {
		t.Fatalf("gateway-only selected agents=%v, want empty", running["agents"])
	}
	opsURL, _ := running["ops_url"].(string)
	if !strings.HasPrefix(opsURL, "http://127.0.0.1:") {
		t.Fatalf("gateway ops URL=%q, want the ephemeral gateway listener", opsURL)
	}
	base := strings.TrimSuffix(opsURL, "/ops")
	places := struct {
		Places    []struct{ Name string } `json:"places"`
		Available bool                    `json:"available"`
	}{}
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get(base + "/api/places?query=Paris&limit=3")
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				err = json.NewDecoder(resp.Body).Decode(&places)
			} else {
				err = fmt.Errorf("status %d", resp.StatusCode)
			}
			resp.Body.Close()
		}
		if err == nil && places.Available {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/api/places on a gateway-only process: available=%v places=%v err=%v", places.Available, places.Places, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(places.Places) == 0 || places.Places[0].Name != "Paris" {
		t.Fatalf("/api/places places=%v, want the test gazetteer's Paris", places.Places)
	}
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	waitLog(t, lines, "shutting down")
	waitLog(t, lines, "stopped")
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gateway-only process failed: %v; stderr: %s", err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("gateway-only process did not stop after signal")
	}
}
