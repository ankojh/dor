package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"go.yaml.in/yaml/v4"

	decidemock "github.com/ankojh/dor/internal/decide/mockapi"
	llmmock "github.com/ankojh/dor/internal/llm/mockapi"
)

const mockMarker = "MOCK BACKENDS — NOT A MEASUREMENT"

var envNames = []string{
	"OPENROUTER_API_KEY", "TYPESAFE_API_KEY", "TYPESAFE_BASE_URL", "ANTHROPIC_API_KEY",
	"DOR_JEV_BASE_URL", "DOR_JEV_PATH", "DOR_JEV_REQUEST_SHAPE", "DOR_CLAUDE_BASE_URL",
}

type fakeServer struct {
	*httptest.Server
	hits atomic.Int32
}

func (f *fakeServer) Hits() int { return int(f.hits.Load()) }

func serve(t *testing.T, h http.Handler) *fakeServer {
	t.Helper()
	f := &fakeServer{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

func jevFake(t *testing.T) *fakeServer {
	o := decidemock.DefaultOptions()
	o.Latency = 0
	return serve(t, decidemock.NewHandler(o))
}

func claudeFake(t *testing.T) *fakeServer {
	o := llmmock.DefaultOptions()
	o.Latency = 0
	return serve(t, llmmock.NewHandler(o))
}

func failing(t *testing.T, status int, body string) *fakeServer {
	return serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

type env struct {
	args   []string
	cfgDir string
	report string
	json   string
}

// setup copies the committed config into a temp dir (the CLI may rewrite
// thresholds.yaml on a live run), clears every credential variable and returns
// the base arguments. Nothing in the repo is written.
func setup(t *testing.T) env {
	t.Helper()
	for _, n := range envNames {
		t.Setenv(n, "")
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	if err := os.MkdirAll(cfg, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"models.yaml", "thresholds.yaml", "budget.yaml"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "config", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cfg, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	e := env{cfgDir: cfg, report: filepath.Join(dir, "report.md"), json: filepath.Join(dir, "report.json")}
	e.args = []string{
		"-data", filepath.Join("..", "..", "data"), "-config", cfg, "-report", e.report, "-json", e.json,
		"-max-items", "3", "-timeout", "60s",
	}
	return e
}

func (e env) setEvalUSD(t *testing.T, v string) {
	t.Helper()
	p := filepath.Join(e.cfgDir, "budget.yaml")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	out := strings.Replace(string(raw), "eval_usd: 2.00", "eval_usd: "+v, 1)
	if out == string(raw) {
		t.Fatal("eval_usd line not found in budget.yaml")
	}
	if err := os.WriteFile(p, []byte(out), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (e env) firstLine(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(e.report)
	if err != nil {
		t.Fatalf("report not written: %v", err)
	}
	return strings.SplitN(string(raw), "\n", 2)[0]
}

func (e env) hasReport() bool {
	_, err := os.Stat(e.report)
	return err == nil
}

func TestFullyMock(t *testing.T) {
	e := setup(t)
	jev, claude := jevFake(t), claudeFake(t)
	// Keys and URLs are ignored under -mock.
	t.Setenv("OPENROUTER_API_KEY", "k")
	t.Setenv("ANTHROPIC_API_KEY", "k")
	t.Setenv("DOR_JEV_BASE_URL", jev.URL)
	t.Setenv("DOR_CLAUDE_BASE_URL", claude.URL)
	if err := run(append(e.args, "-mock")); err != nil {
		t.Fatal(err)
	}
	if first := e.firstLine(t); first != mockMarker {
		t.Errorf("first line = %q", first)
	}
	if jev.Hits() != 0 || claude.Hits() != 0 {
		t.Errorf("-mock must not touch the configured servers: jev %d claude %d", jev.Hits(), claude.Hits())
	}
	if _, err := os.Stat(e.json); err != nil {
		t.Errorf("json report: %v", err)
	}
}

func TestNoKeysIsMock(t *testing.T) {
	e := setup(t)
	if err := run(e.args); err != nil {
		t.Fatal(err)
	}
	if e.firstLine(t) != mockMarker {
		t.Error("without keys the run is a mock run")
	}
	// A mock run never rewrites thresholds.
	raw, _ := os.ReadFile(filepath.Join(e.cfgDir, "thresholds.yaml"))
	orig, _ := os.ReadFile(filepath.Join("..", "..", "..", "config", "thresholds.yaml"))
	if string(raw) != string(orig) {
		t.Error("a mock run must not rewrite thresholds.yaml")
	}
}

func TestEvalUSDZeroDeniesLiveBeforeAnyCall(t *testing.T) {
	for _, which := range []string{"jev", "claude"} {
		e := setup(t)
		e.setEvalUSD(t, "0")
		jev, claude := jevFake(t), claudeFake(t)
		if which == "jev" {
			t.Setenv("OPENROUTER_API_KEY", "k")
			t.Setenv("DOR_JEV_BASE_URL", jev.URL)
		} else {
			t.Setenv("ANTHROPIC_API_KEY", "k")
			t.Setenv("DOR_CLAUDE_BASE_URL", claude.URL)
		}
		err := run(e.args)
		if err == nil || !strings.Contains(err.Error(), "eval_usd is 0") {
			t.Errorf("%s: err = %v, want a clear eval_usd error", which, err)
		}
		if jev.Hits() != 0 || claude.Hits() != 0 {
			t.Errorf("%s: zero server hits expected, got jev %d claude %d", which, jev.Hits(), claude.Hits())
		}
		if e.hasReport() {
			t.Errorf("%s: no report when the run is denied", which)
		}
	}
	// eval_usd 0 does not block a pure mock run.
	e := setup(t)
	e.setEvalUSD(t, "0")
	if err := run(append(e.args, "-mock")); err != nil {
		t.Errorf("mock with eval_usd 0: %v", err)
	}
}

func TestOverCapIsRefusedBeforeAnyCall(t *testing.T) {
	e := setup(t)
	e.setEvalUSD(t, "0.0000001")
	jev, claude := jevFake(t), claudeFake(t)
	t.Setenv("OPENROUTER_API_KEY", "k")
	t.Setenv("DOR_JEV_BASE_URL", jev.URL)
	t.Setenv("ANTHROPIC_API_KEY", "k")
	t.Setenv("DOR_CLAUDE_BASE_URL", claude.URL)
	err := run(e.args)
	if err == nil || !strings.Contains(err.Error(), "exceeds eval_usd") {
		t.Fatalf("err = %v", err)
	}
	if jev.Hits() != 0 || claude.Hits() != 0 {
		t.Errorf("preflight must stop before any call: jev %d claude %d", jev.Hits(), claude.Hits())
	}
	// -allow-over-cap passes the preflight, but the limiter still enforces the cap:
	// every call is denied, so nothing reaches a server.
	err = run(append(e.args, "-allow-over-cap"))
	if err == nil || strings.Contains(err.Error(), "exceeds eval_usd") {
		t.Errorf("with -allow-over-cap: err = %v, want a backend failure rather than the preflight error", err)
	}
	if jev.Hits() != 0 || claude.Hits() != 0 {
		t.Errorf("the hard cap must hold even with -allow-over-cap: jev %d claude %d", jev.Hits(), claude.Hits())
	}
}

func TestUnderCapMixedJevLiveClaudeMock(t *testing.T) {
	e := setup(t)
	jev, claude := jevFake(t), claudeFake(t)
	t.Setenv("OPENROUTER_API_KEY", "k")
	t.Setenv("DOR_JEV_BASE_URL", jev.URL)
	if err := run(e.args); err != nil {
		t.Fatal(err)
	}
	if jev.Hits() == 0 {
		t.Error("the live jev backend was never called")
	}
	if claude.Hits() != 0 {
		t.Errorf("Claude is a mock here (no key): the DOR_CLAUDE_BASE_URL-less run must not hit %d", claude.Hits())
	}
	if first := e.firstLine(t); first != mockMarker {
		t.Errorf("a mixed run is still marked as mock: %q", first)
	}
	raw, _ := os.ReadFile(e.report)
	if !strings.Contains(string(raw), "Mixed run: jev live, Claude mock") {
		t.Error("the report must say it is a mixed run")
	}
	// A mixed run never selects thresholds.
	cur, _ := os.ReadFile(filepath.Join(e.cfgDir, "thresholds.yaml"))
	orig, _ := os.ReadFile(filepath.Join("..", "..", "..", "config", "thresholds.yaml"))
	if string(cur) != string(orig) {
		t.Error("a mixed run must not rewrite thresholds.yaml")
	}
}

func TestUnderCapMixedClaudeLiveJevMock(t *testing.T) {
	e := setup(t)
	jev, claude := jevFake(t), claudeFake(t)
	t.Setenv("ANTHROPIC_API_KEY", "k")
	t.Setenv("DOR_CLAUDE_BASE_URL", claude.URL)
	t.Setenv("DOR_JEV_BASE_URL", jev.URL) // ignored: no jev key
	if err := run(e.args); err != nil {
		t.Fatal(err)
	}
	if claude.Hits() == 0 {
		t.Error("the live Claude backend was never called")
	}
	if jev.Hits() != 0 {
		t.Errorf("jev is a mock here: server hit %d times", jev.Hits())
	}
	if e.firstLine(t) != mockMarker {
		t.Error("a mixed run is marked as mock")
	}
	raw, _ := os.ReadFile(e.report)
	if !strings.Contains(string(raw), "Mixed run: jev mock, Claude live") {
		t.Error("the report must say it is a mixed run")
	}
}

func TestFullyLive(t *testing.T) {
	e := setup(t)
	jev, claude := jevFake(t), claudeFake(t)
	t.Setenv("OPENROUTER_API_KEY", "k")
	t.Setenv("DOR_JEV_BASE_URL", jev.URL)
	t.Setenv("ANTHROPIC_API_KEY", "k")
	t.Setenv("DOR_CLAUDE_BASE_URL", claude.URL)
	if err := run(append(e.args, "-write-thresholds=false")); err != nil {
		t.Fatal(err)
	}
	if jev.Hits() == 0 || claude.Hits() == 0 {
		t.Errorf("both backends must be called: jev %d claude %d", jev.Hits(), claude.Hits())
	}
	if first := e.firstLine(t); strings.Contains(first, "MOCK") {
		t.Errorf("a fully live report must not carry the mock marker: %q", first)
	}
	raw, _ := os.ReadFile(e.report)
	if !strings.Contains(string(raw), "jev **live**, Claude **live**") {
		t.Error("the report must name both live backends")
	}
	// -write-thresholds=false leaves the file alone even on a live run.
	cur, _ := os.ReadFile(filepath.Join(e.cfgDir, "thresholds.yaml"))
	orig, _ := os.ReadFile(filepath.Join("..", "..", "..", "config", "thresholds.yaml"))
	if string(cur) != string(orig) {
		t.Error("-write-thresholds=false must not write")
	}
}

func TestBackendFailingEntirelyStillWritesReportThenErrors(t *testing.T) {
	cases := map[string]func(t *testing.T, e env) (*fakeServer, string){
		"jev": func(t *testing.T, e env) (*fakeServer, string) {
			bad := failing(t, 400, `{"error":{"code":400,"message":"bad request"}}`)
			t.Setenv("OPENROUTER_API_KEY", "k")
			t.Setenv("DOR_JEV_BASE_URL", bad.URL)
			return bad, "jev"
		},
		"claude": func(t *testing.T, e env) (*fakeServer, string) {
			bad := failing(t, 400, `{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`)
			t.Setenv("ANTHROPIC_API_KEY", "k")
			t.Setenv("DOR_CLAUDE_BASE_URL", bad.URL)
			return bad, "claude"
		},
	}
	for name, mk := range cases {
		e := setup(t)
		bad, who := mk(t, e)
		err := run(e.args)
		if err == nil || !strings.Contains(err.Error(), "backend failed entirely") || !strings.Contains(err.Error(), who) {
			t.Errorf("%s: err = %v", name, err)
		}
		if !e.hasReport() {
			t.Errorf("%s: the report must be written before the error is returned", name)
		}
		if _, statErr := os.Stat(e.json); statErr != nil {
			t.Errorf("%s: json report missing", name)
		}
		if bad.Hits() == 0 {
			t.Errorf("%s: the backend was never called", name)
		}
		// 400 is permanent: every logical call is exactly one hit.
		raw, _ := os.ReadFile(e.report)
		if !strings.Contains(string(raw), "| errors |") && !strings.Contains(string(raw), "## Fail-closed") {
			t.Errorf("%s: the fail-closed table is missing", name)
		}
	}
}

func TestTypeSafeNeedsExplicitConfiguration(t *testing.T) {
	cases := map[string]map[string]string{
		"key alone":         {"TYPESAFE_API_KEY": "k"},
		"no path":           {"TYPESAFE_API_KEY": "k", "DOR_JEV_BASE_URL": "http://x", "DOR_JEV_REQUEST_SHAPE": "openrouter-decisions"},
		"no base":           {"TYPESAFE_API_KEY": "k", "DOR_JEV_PATH": "/p", "DOR_JEV_REQUEST_SHAPE": "openrouter-decisions"},
		"no shape":          {"TYPESAFE_API_KEY": "k", "DOR_JEV_BASE_URL": "http://x", "DOR_JEV_PATH": "/p"},
		"unsupported shape": {"TYPESAFE_API_KEY": "k", "DOR_JEV_BASE_URL": "http://x", "DOR_JEV_PATH": "/p", "DOR_JEV_REQUEST_SHAPE": "systemone"},
	}
	for name, vars := range cases {
		e := setup(t)
		jev, claude := jevFake(t), claudeFake(t)
		for k, v := range vars {
			if k == "DOR_JEV_BASE_URL" {
				v = jev.URL
			}
			t.Setenv(k, v)
		}
		err := run(e.args)
		if err == nil {
			t.Errorf("%s: expected an error", name)
			continue
		}
		if name == "unsupported shape" && !strings.Contains(err.Error(), "unsupported DOR_JEV_REQUEST_SHAPE") {
			t.Errorf("%s: err = %v", name, err)
		}
		if name != "unsupported shape" && !strings.Contains(err.Error(), "TYPESAFE_API_KEY needs") {
			t.Errorf("%s: err = %v", name, err)
		}
		if jev.Hits() != 0 || claude.Hits() != 0 || e.hasReport() {
			t.Errorf("%s: must fail before any call and write no report (hits %d/%d)", name, jev.Hits(), claude.Hits())
		}
	}
}

func TestTypeSafeWithAllThreeEnvsRuns(t *testing.T) {
	e := setup(t)
	jev := jevFake(t)
	t.Setenv("TYPESAFE_API_KEY", "k")
	t.Setenv("DOR_JEV_BASE_URL", jev.URL)
	t.Setenv("DOR_JEV_PATH", "/api/alpha/decisions")
	t.Setenv("DOR_JEV_REQUEST_SHAPE", "openrouter-decisions")
	if err := run(e.args); err != nil {
		t.Fatal(err)
	}
	if jev.Hits() == 0 {
		t.Error("the configured TypeSafe-compatible endpoint was not called")
	}
}

func TestMockIgnoresIncompleteTypeSafeKey(t *testing.T) {
	e := setup(t)
	t.Setenv("TYPESAFE_API_KEY", "k")
	if err := run(append(e.args, "-mock")); err != nil {
		t.Fatalf("-mock must ignore keys, got %v", err)
	}
	if e.firstLine(t) != mockMarker {
		t.Error("a -mock run carries the marker")
	}
}

func TestBadFlagsAndData(t *testing.T) {
	e := setup(t)
	if err := run([]string{"-no-such-flag"}); err == nil {
		t.Error("an unknown flag must be an error")
	}
	if err := run(append(append([]string{}, e.args...), "-data", t.TempDir())); err == nil {
		t.Error("an empty data directory must be an error")
	}
}

func TestWorkflowIsSecretFreeOnPush(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "eval.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		On   map[string]any    `yaml:"on"`
		Env  map[string]string `yaml:"env"`
		Jobs map[string]struct {
			Env   map[string]string `yaml:"env"`
			Steps []struct {
				Name string            `yaml:"name"`
				If   string            `yaml:"if"`
				Run  string            `yaml:"run"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("eval.yml is not valid YAML: %v", err)
	}
	if _, ok := wf.On["push"]; !ok {
		t.Error("eval.yml has no push trigger")
	}
	if _, ok := wf.On["workflow_dispatch"]; !ok {
		t.Error("eval.yml has no workflow_dispatch trigger")
	}
	if containsSecret(wf.Env) {
		t.Error("workflow-level env must not carry secrets")
	}
	job, ok := wf.Jobs["eval"]
	if !ok {
		t.Fatal("eval.yml has no eval job")
	}
	if containsSecret(job.Env) {
		t.Error("job-level env must not carry secrets")
	}
	var push, manual bool
	for _, st := range job.Steps {
		if strings.Contains(st.If, "github.event_name == 'push'") {
			push = true
			if !strings.Contains(st.Run, "-mock") {
				t.Errorf("push step must run mock: %q", st.Run)
			}
			if containsSecret(st.Env) {
				t.Error("push step must not see secrets")
			}
		}
		if strings.Contains(st.If, "github.event_name == 'workflow_dispatch'") {
			manual = true
			if !strings.Contains(st.Run, "make eval") {
				t.Errorf("manual step must run make eval: %q", st.Run)
			}
			if st.Env["OPENROUTER_API_KEY"] == "" || st.Env["ANTHROPIC_API_KEY"] == "" {
				t.Errorf("manual step needs both secret keys: %v", st.Env)
			}
		}
	}
	if !push || !manual {
		t.Errorf("push step=%v manual step=%v", push, manual)
	}
	var ci map[string]any
	ciRaw, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(ciRaw, &ci); err != nil {
		t.Fatalf("ci.yml is not valid YAML: %v", err)
	}
}

func containsSecret(env map[string]string) bool {
	for k, v := range env {
		if strings.Contains(k, "API_KEY") || strings.Contains(v, "secrets.") {
			return true
		}
	}
	return false
}
