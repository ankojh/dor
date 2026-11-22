package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// gatewayCountingDecider counts jev calls from the real matcher so a prompt
// clear can be checked end to end instead of only at the PromptSink seam.
type gatewayCountingDecider struct {
	calls atomic.Int64
}

func (d *gatewayCountingDecider) Decide(context.Context, decide.Request) (decide.Response, error) {
	d.calls.Add(1)
	return decide.Response{Answers: map[string]decide.Answer{
		"match": {Type: decide.TypeNoul, Present: true, HasNoul: true, Noul: .99},
	}}, nil
}

type recordingPromptSink struct {
	inner PromptSink

	mu    sync.Mutex
	calls [][]string
}

func (s *recordingPromptSink) SetClientPrompts(ctx context.Context, clientID string, prompts []string) {
	s.mu.Lock()
	s.calls = append(s.calls, append([]string(nil), prompts...))
	s.mu.Unlock()
	if s.inner != nil {
		s.inner.SetClientPrompts(ctx, clientID, prompts)
	}
}

func (s *recordingPromptSink) ClearClientPrompts(ctx context.Context, clientID string) {
	s.mu.Lock()
	s.calls = append(s.calls, nil)
	s.mu.Unlock()
	if s.inner != nil {
		s.inner.ClearClientPrompts(ctx, clientID)
	}
}

func (s *recordingPromptSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *recordingPromptSink) last() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return nil
	}
	return s.calls[len(s.calls)-1]
}

func (s *recordingPromptSink) since(index int) [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= len(s.calls) {
		return nil
	}
	return append([][]string(nil), s.calls[index:]...)
}

func TestPromptClearBypassesThrottleAndStopsMatcherEvaluation(t *testing.T) {
	srv, _ := newGuardTestServer(t, nil)
	log := projectionLogger()
	decider := &gatewayCountingDecider{}
	svc := matcher.New(srv.cfg.Bus, log, decider, embed.New("", 0), nil, config.DefaultThresholds())
	sink := &recordingPromptSink{inner: svc}
	h := newHub(srv.cfg.Bus, log)
	c := newClient(h, nil, sink, nil, log)
	h.addClient(c)
	h.mu.Lock()
	c.ready = true
	h.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spend every inbound token. These updates apply immediately.
	for i := 0; i < int(inboundBurst); i++ {
		c.handle(ctx, setPromptsMessage([]string{"wildfire"}))
	}
	if got := sink.last(); len(got) != 1 || got[0] != "wildfire" {
		t.Fatalf("last applied prompts = %v, want [wildfire]", got)
	}

	// The active prompt really reaches the matcher's paid path.
	article := &dorv1.Article{Id: "article-1", Title: "Wildfire spreads", Lede: "Evacuations ordered."}
	scores, _ := svc.ArticleScores(ctx, article)
	if scores["wildfire"] == 0 {
		t.Fatalf("active prompt did not score the article: %v", scores)
	}
	if decider.calls.Load() == 0 {
		t.Fatal("jev was not consulted for an active prompt")
	}

	// Force the bucket empty so both of the next updates are rate limited.
	c.mu.Lock()
	c.inTokens = 0
	c.inRefill = time.Now()
	c.mu.Unlock()

	c.handle(ctx, setPromptsMessage([]string{"pending"}))
	if got := sink.last(); len(got) != 1 || got[0] != "wildfire" {
		t.Fatalf("throttled update reached the matcher before a token returned: %v", got)
	}

	// Removing the last prompt bypasses admission entirely.
	c.handle(ctx, setPromptsMessage(nil))
	if got := sink.last(); len(got) != 0 {
		t.Fatalf("clear was not applied immediately: %v", got)
	}
	afterClear := sink.count()

	// The coalesced loop must not resurrect the superseded nonempty state.
	promptCtx, promptCancel := context.WithCancel(ctx)
	go c.promptLoop(promptCtx)
	time.Sleep(300 * time.Millisecond)
	if extra := sink.since(afterClear); len(extra) != 0 {
		t.Fatalf("prompt loop applied a superseded update after the clear: %v", extra)
	}
	promptCancel()
	select {
	case <-c.promptDone:
	case <-time.After(time.Second):
		t.Fatal("prompt loop did not stop")
	}

	before := decider.calls.Load()
	if scores, approx := svc.ArticleScores(ctx, article); scores != nil || approx != nil {
		t.Fatalf("scores after prompt clear = %v / %v, want nil", scores, approx)
	}
	if got := decider.calls.Load(); got != before {
		t.Fatalf("jev calls after prompt clear = %d, want %d", got, before)
	}
}

func TestThrottledPromptUpdateAppliesWhenBudgetReturns(t *testing.T) {
	h := newHub(nil, projectionLogger())
	sink := &projectionSink{}
	c := newAdmissionClient(h, nil, sink)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < int(inboundBurst); i++ {
		c.handle(ctx, setPromptsMessage([]string{"warmup"}))
	}
	c.mu.Lock()
	c.inTokens = 0
	c.inRefill = time.Now()
	c.mu.Unlock()

	c.handle(ctx, setPromptsMessage([]string{"later"}))
	if got := sink.last(); len(got) != 1 || got[0] != "warmup" {
		t.Fatalf("throttled update applied immediately: %v", got)
	}

	go c.promptLoop(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := sink.last(); len(got) == 1 && got[0] == "later" {
			cancel()
			select {
			case <-c.promptDone:
			case <-time.After(time.Second):
				t.Fatal("prompt loop did not stop")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.mu.Lock()
	tokens, refill := c.inTokens, c.inRefill
	c.mu.Unlock()
	t.Fatalf("coalesced update never applied; last = %v, calls = %v, tokens = %v since %v", sink.last(), sink.callsSnapshot(), tokens, time.Since(refill))
}

func TestUnknownStoryDoesNotSpendPaidTextBudget(t *testing.T) {
	text := &admissionText{}
	srv, _ := newGuardTestServer(t, func(cfg *Config) { cfg.Text = text })
	cookie, _ := issueSession(t, srv)

	get := func(storyID string) int {
		req := httptest.NewRequest(http.MethodGet, "http://localhost/api/text?story_id="+storyID, nil)
		req.AddCookie(cookie)
		req.RemoteAddr = "192.0.2.71:4000"
		w := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(w, req)
		return w.Code
	}

	// Below the lookup burst, so a 404 cannot be mistaken for a lookup
	// throttle; the paid limiter must be untouched.
	for i := 0; i < 5; i++ {
		if got := get("missing"); got != http.StatusNotFound {
			t.Fatalf("unknown story request %d status = %d, want 404", i, got)
		}
	}
	for i := 0; i < 5; i++ {
		if got := get("known"); got != http.StatusOK {
			t.Fatalf("known story request %d after unknown 404s status = %d, want 200 (paid budget spent on 404s?)", i, got)
		}
	}
}

func TestResumeFromWarmCacheKeepsOwnPromptScores(t *testing.T) {
	h := newHub(nil, projectionLogger())
	h.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{
			Seq: 0,
			RecentWire: []*dorv1.WireRow{{
				StoryId:       "story-1",
				Title:         "Some story",
				PromptScores:  map[string]float32{"secret": 0.9, "mine": 0.5},
				ApproxPrompts: []string{"secret"},
			}},
		}, nil
	}
	c := newProjectionClient(h, nil)
	ctx := context.Background()

	// The worker sends the prompt set first, then Hello(0).
	c.handle(ctx, setPromptsMessage([]string{"secret"}))
	c.handle(ctx, &dorv1.ClientMessage{Body: &dorv1.ClientMessage_Hello{Hello: &dorv1.Hello{LastSeq: 0}}})

	var snapshot *dorv1.Snapshot
	deadline := time.Now().Add(2 * time.Second)
	for snapshot == nil && time.Now().Before(deadline) {
		select {
		case data := <-c.send:
			var msg dorv1.ServerMessage
			if err := proto.Unmarshal(data, &msg); err != nil {
				t.Fatalf("decode resume message: %v", err)
			}
			if snap := msg.GetSnapshot(); snap != nil {
				snapshot = snap
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	if snapshot == nil {
		t.Fatal("resumed snapshot never arrived")
	}
	if len(snapshot.RecentWire) != 1 {
		t.Fatalf("resumed recent wire rows = %d, want 1", len(snapshot.RecentWire))
	}
	row := snapshot.RecentWire[0]
	if len(row.PromptScores) != 1 {
		t.Fatalf("resumed prompt scores = %v, want only this client's prompt", row.PromptScores)
	}
	if score := row.PromptScores["secret"]; score < 0.89 || score > 0.91 {
		t.Fatalf("resumed secret score = %v, want ~0.9", score)
	}
	if _, ok := row.PromptScores["mine"]; ok {
		t.Fatalf("resumed snapshot leaked another client's prompt: %v", row.PromptScores)
	}
	if len(row.ApproxPrompts) != 1 || row.ApproxPrompts[0] != "secret" {
		t.Fatalf("resumed approx prompts = %v, want [secret]", row.ApproxPrompts)
	}
}

func TestGatewayWithoutMatcherSurvivesEmptySetPrompts(t *testing.T) {
	srv, _ := newGuardTestServer(t, func(cfg *Config) { cfg.Matcher = nil })
	srv.hub.readSnapshot = func(context.Context) (*dorv1.Snapshot, error) {
		return &dorv1.Snapshot{Seq: 0}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = srv.Stop(stopCtx)
	})

	conn, _, err := websocket.Dial(ctx, "ws://"+srv.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer conn.CloseNow()

	write := func(msg *dorv1.ClientMessage) {
		t.Helper()
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal client message: %v", err)
		}
		writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
		defer writeCancel()
		if err := conn.Write(writeCtx, websocket.MessageBinary, data); err != nil {
			t.Fatalf("write client message: %v", err)
		}
	}
	write(&dorv1.ClientMessage{Body: &dorv1.ClientMessage_SetPrompts{SetPrompts: &dorv1.SetPrompts{}}})
	write(&dorv1.ClientMessage{Body: &dorv1.ClientMessage_Hello{Hello: &dorv1.Hello{LastSeq: 0}}})

	readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
	defer readCancel()
	for {
		typ, data, err := conn.Read(readCtx)
		if err != nil {
			t.Fatalf("read server message: %v", err)
		}
		if typ != websocket.MessageBinary {
			continue
		}
		var msg dorv1.ServerMessage
		if err := proto.Unmarshal(data, &msg); err != nil {
			t.Fatalf("decode server message: %v", err)
		}
		if msg.GetSnapshot() != nil {
			break
		}
	}
}
