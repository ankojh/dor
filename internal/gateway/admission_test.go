package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

type admissionText struct {
	storyPrefetch atomic.Int64
	linkPrefetch  atomic.Int64

	mu        sync.Mutex
	storyIDs  []string
	linkPairs [][2]string

	generateStory func(ctx context.Context, id string) (live.StoryText, bool)
}

func (f *admissionText) Story(_ context.Context, id string) (live.StoryText, bool) {
	if id == "missing" {
		return live.StoryText{}, false
	}
	return live.StoryText{Kind: "story", ID: id, Available: false}, true
}

func (f *admissionText) GenerateStory(ctx context.Context, id string) (live.StoryText, bool) {
	if f.generateStory != nil {
		return f.generateStory(ctx, id)
	}
	return live.StoryText{Kind: "story", ID: id, Available: true}, true
}

func (f *admissionText) Link(context.Context, string, string) (live.LinkText, bool) {
	return live.LinkText{}, false
}

func (f *admissionText) GenerateLink(_ context.Context, a, b string) (live.LinkText, bool) {
	return live.LinkText{Kind: "link", A: a, B: b, Available: true}, true
}

func (f *admissionText) PrefetchStory(id string) {
	f.storyPrefetch.Add(1)
	f.mu.Lock()
	f.storyIDs = append(f.storyIDs, id)
	f.mu.Unlock()
}

func (f *admissionText) PrefetchLink(a, b string) {
	f.linkPrefetch.Add(1)
	f.mu.Lock()
	f.linkPairs = append(f.linkPairs, [2]string{a, b})
	f.mu.Unlock()
}

func (f *admissionText) recordedIDs() ([]string, [][2]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.storyIDs...), append([][2]string(nil), f.linkPairs...)
}

func newAdmissionClient(h *hub, text TextService, sink PromptSink) *client {
	return newClient(h, text, sink, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSetPromptsAdmissionIsPerClientAndBounded(t *testing.T) {
	h := newHub(nil, projectionLogger())
	sink := &projectionSink{}
	c := newAdmissionClient(h, nil, sink)

	for i := 0; i < 50; i++ {
		c.handle(context.Background(), setPromptsMessage([]string{"news"}))
	}
	if got := sink.count(); got < 1 || got > int(inboundBurst) {
		t.Fatalf("forwarded SetPrompts calls = %d, want 1..%d", got, int(inboundBurst))
	}

	// A second client has its own inbound bucket.
	otherSink := &projectionSink{}
	other := newAdmissionClient(h, nil, otherSink)
	other.handle(context.Background(), setPromptsMessage([]string{"wildfire"}))
	if got := otherSink.count(); got != 1 {
		t.Fatalf("independent client forwarded calls = %d, want 1", got)
	}

	// One accepted list is trimmed, deduplicated and capped in both count and
	// rune length.
	boundSink := &projectionSink{}
	bound := newAdmissionClient(h, nil, boundSink)
	long := strings.Repeat("x", 400)
	longDuplicate := strings.Repeat("x", 400) + "y"
	prompts := []string{"  a  ", "a", "", long, longDuplicate}
	for i := 0; i < 10; i++ {
		prompts = append(prompts, fmt.Sprintf("p%d", i))
	}
	bound.handle(context.Background(), setPromptsMessage(prompts))

	last := boundSink.last()
	if len(last) != maxClientPrompts {
		t.Fatalf("bounded prompt count = %d, want %d (%v)", len(last), maxClientPrompts, last)
	}
	seen := make(map[string]bool, len(last))
	truncated := 0
	for _, prompt := range last {
		if prompt != strings.TrimSpace(prompt) {
			t.Fatalf("prompt %q was not trimmed", prompt)
		}
		if runes := utf8.RuneCountInString(prompt); runes > maxPromptRunes {
			t.Fatalf("prompt has %d runes, want <=%d", runes, maxPromptRunes)
		}
		if seen[prompt] {
			t.Fatalf("duplicate prompt %q in %v", prompt, last)
		}
		seen[prompt] = true
		if utf8.RuneCountInString(prompt) == maxPromptRunes {
			truncated++
		}
	}
	if truncated != 1 {
		t.Fatalf("truncated prompt variants retained = %d, want 1 (%v)", truncated, last)
	}
}

func TestEmptySetPromptsIsForwarded(t *testing.T) {
	h := newHub(nil, projectionLogger())
	sink := &projectionSink{}
	c := newAdmissionClient(h, nil, sink)

	c.handle(context.Background(), setPromptsMessage(nil))
	if sink.count() != 1 {
		t.Fatalf("empty SetPrompts forwarded %d times, want 1", sink.count())
	}
	if got := sink.last(); len(got) != 0 {
		t.Fatalf("empty SetPrompts forwarded %v, want an empty list", got)
	}
	if got := c.filterState(); len(got.prompts) != 0 {
		t.Fatalf("client retained prompts after removal: %v", got.prompts)
	}
}

func TestFocusAndGetTextPrefetchIsBounded(t *testing.T) {
	h := newHub(nil, projectionLogger())
	text := &admissionText{}
	focus := newAdmissionClient(h, text, nil)
	focusMsg := &dorv1.ClientMessage{Body: &dorv1.ClientMessage_Focus{
		Focus: &dorv1.Focus{StoryId: "story-1"},
	}}
	for i := 0; i < 10; i++ {
		focus.handle(context.Background(), focusMsg)
	}
	if got := text.storyPrefetch.Load(); got > int64(textBurst) {
		t.Fatalf("Focus prefetches = %d, want <=%d", got, int64(textBurst))
	}

	getText := &admissionText{}
	reader := newAdmissionClient(h, getText, nil)
	ids := []string{"", strings.Repeat("x", maxRequestIDLen+1)}
	for i := 0; i < 100; i++ {
		ids = append(ids, fmt.Sprintf("story-%03d", i))
	}
	ids = append(ids, "story-000")
	links := []*dorv1.LinkKey{
		{A: "", B: "story-b"},
		{A: "story-a", B: ""},
		{A: strings.Repeat("x", maxRequestIDLen+1), B: "story-b"},
	}
	for i := 0; i < 100; i++ {
		links = append(links, &dorv1.LinkKey{
			A: fmt.Sprintf("story-a-%03d", i),
			B: fmt.Sprintf("story-b-%03d", i),
		})
	}
	reader.handle(context.Background(), &dorv1.ClientMessage{Body: &dorv1.ClientMessage_GetText{
		GetText: &dorv1.GetText{StoryIds: ids, LinkKeys: links},
	}})

	if got := getText.storyPrefetch.Load(); got != int64(maxGetTextIDs) {
		t.Fatalf("GetText story prefetches = %d, want %d", got, maxGetTextIDs)
	}
	if got := getText.linkPrefetch.Load(); got != int64(maxGetTextIDs) {
		t.Fatalf("GetText link prefetches = %d, want %d", got, maxGetTextIDs)
	}
	storyIDs, linkPairs := getText.recordedIDs()
	seen := make(map[string]bool, len(storyIDs))
	for _, id := range storyIDs {
		if id == "" || len(id) > maxRequestIDLen {
			t.Fatalf("prefetched invalid story id %q", id)
		}
		if seen[id] {
			t.Fatalf("prefetched duplicate story id %q", id)
		}
		seen[id] = true
	}
	for _, pair := range linkPairs {
		if pair[0] == "" || pair[1] == "" {
			t.Fatalf("prefetched a link with empty endpoints: %v", pair)
		}
		if len(pair[0]) > maxRequestIDLen || len(pair[1]) > maxRequestIDLen {
			t.Fatalf("prefetched oversized link endpoints: %v", pair)
		}
	}
}

func TestRESTAlertAndTextLimitsArePerSession(t *testing.T) {
	text := &admissionText{}
	srv, _ := newGuardTestServer(t, func(cfg *Config) { cfg.Text = text })
	cookieA, _ := issueSession(t, srv)
	cookieB, _ := issueSession(t, srv)

	postAlert := func(cookie *http.Cookie, addr string) int {
		req := httptest.NewRequest(http.MethodPost, "http://localhost/api/alerts", strings.NewReader(`{"prompt":"wildfire"}`))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(cookie)
		req.RemoteAddr = addr
		w := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(w, req)
		return w.Code
	}

	codes := map[int]int{}
	for i := 0; i < 40; i++ {
		codes[postAlert(cookieA, "192.0.2.10:4000")]++
	}
	if codes[http.StatusTooManyRequests] == 0 {
		t.Fatalf("hammering POST /api/alerts produced no 429: %v", codes)
	}
	if codes[http.StatusCreated] == 0 {
		t.Fatalf("hammering POST /api/alerts produced no 201: %v", codes)
	}
	// A different session on the same loopback/proxy address is unaffected.
	if got := postAlert(cookieB, "192.0.2.10:4000"); got == http.StatusTooManyRequests {
		t.Fatal("a second session inherited the first session's alert limit")
	}

	getText := func(cookie *http.Cookie, addr string) int {
		req := httptest.NewRequest(http.MethodGet, "http://localhost/api/text?story_id=s1", nil)
		req.AddCookie(cookie)
		req.RemoteAddr = addr
		w := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(w, req)
		return w.Code
	}
	textCodes := map[int]int{}
	for i := 0; i < 20; i++ {
		textCodes[getText(cookieA, "192.0.2.12:4000")]++
	}
	if textCodes[http.StatusTooManyRequests] == 0 {
		t.Fatalf("hammering GET /api/text produced no 429: %v", textCodes)
	}
	if textCodes[http.StatusOK] == 0 {
		t.Fatalf("hammering GET /api/text produced no 200: %v", textCodes)
	}
	if got := getText(cookieB, "192.0.2.12:4000"); got == http.StatusTooManyRequests {
		t.Fatal("a second session inherited the first session's text limit")
	}
}

func TestTextGenerationConcurrencyCapsAtTwoAndRecovers(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	text := &admissionText{generateStory: func(_ context.Context, id string) (live.StoryText, bool) {
		entered <- struct{}{}
		<-release
		return live.StoryText{Kind: "story", ID: id, Available: true}, true
	}}
	srv, _ := newGuardTestServer(t, func(cfg *Config) { cfg.Text = text })

	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost/api/text?story_id=s%d", i), nil)
			req.RemoteAddr = "192.0.2.20:4000"
			w := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(w, req)
			codes <- w.Code
		}(i)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("two generations did not start")
		}
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/text?story_id=third", nil)
	req.RemoteAddr = "192.0.2.20:4000"
	w := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("third concurrent generation status = %d, want 429", w.Code)
	}

	close(release)
	wg.Wait()
	close(codes)
	for code := range codes {
		if code != http.StatusOK {
			t.Fatalf("blocked generation status = %d, want 200", code)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "http://localhost/api/text?story_id=after", nil)
	req.RemoteAddr = "192.0.2.20:4000"
	w = httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("generation slot did not recover: status = %d", w.Code)
	}
}
