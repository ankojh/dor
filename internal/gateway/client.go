package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/pb/dorv1"
)

const (
	// clientQueue bounds one client's unsent messages. The publisher batches
	// at 250 ms, so 64 messages is ~16 s of backlog; a client further behind
	// drops its backlog and receives Resync followed by a fresh snapshot.
	clientQueue = 64
	// clientRate and clientBurst limit outgoing messages per client. Normal
	// traffic is ~4 batches/s, so this only trips on abuse or a pathological
	// burst.
	clientRate  = 100.0
	clientBurst = 50.0
	// inboundRate and inboundBurst limit Hello, SetPrompts, Focus and GetText
	// from one connection. Acks and filter updates are cheap and exempt.
	inboundRate  = 8.0
	inboundBurst = 16.0
	// textRate and textBurst apply an additional, tighter budget to requests
	// that can reach a paid text generator (Focus prefetch and GetText).
	textRate  = 2.0
	textBurst = 4.0
	// maxGetTextIDs bounds one GetText message, and maxRequestIDLen bounds a
	// story id or link endpoint in a request.
	maxGetTextIDs   = 32
	maxRequestIDLen = 200
)

// PromptSink is the matcher surface the gateway needs. *matcher.Service
// implements it; tests use a recorder.
type PromptSink interface {
	SetClientPrompts(ctx context.Context, clientID string, prompts []string)
	ClearClientPrompts(ctx context.Context, clientID string)
}

// client is one WebSocket connection with a bounded send queue and a write
// goroutine. Reads and writes are independent so a slow reader cannot stall
// the hub, and a slow writer is disconnected rather than buffered forever.
type client struct {
	hub     *hub
	text    TextService
	matcher PromptSink
	id      string
	conn    *websocket.Conn
	log     *slog.Logger

	send         chan []byte
	closed       atomic.Bool
	cancel       context.CancelFunc
	ctx          context.Context
	resumeCancel context.CancelFunc // guarded by hub.mu
	ready        bool               // guarded by hub.mu
	lastSeq      atomic.Uint64

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	inTokens   float64
	inRefill   time.Time
	textTokens float64
	textRefill time.Time
	focus      string
	filters    *dorv1.SetFilters
	prompts    *dorv1.SetPrompts
}

func newClient(h *hub, text TextService, prompts PromptSink, conn *websocket.Conn, log *slog.Logger) *client {
	now := time.Now()
	return &client{
		hub:        h,
		text:       text,
		matcher:    prompts,
		id:         newClientID(),
		conn:       conn,
		log:        log,
		send:       make(chan []byte, clientQueue),
		tokens:     clientBurst,
		lastRefill: now,
		inTokens:   inboundBurst,
		inRefill:   now,
		textTokens: textBurst,
		textRefill: now,
	}
}

// newClientID is the per-connection prompt key in live_state. It is random so
// one tab cannot read another tab's prompt set.
func newClientID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "client-" + hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b)
}

// run starts the writer, handles client messages, and removes the client when
// either side ends.
func (c *client) run(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.ctx = ctx
	defer func() {
		cancel()
		c.hub.removeClient(c)
		if c.matcher != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
			c.matcher.ClearClientPrompts(cleanupCtx, c.id)
			cleanupCancel()
		}
		c.conn.CloseNow()
	}()

	go c.writeLoop(ctx)
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			if !c.closed.Load() {
				c.log.Debug("gateway: client read ended", "err", err)
			}
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		var msg dorv1.ClientMessage
		if err := proto.Unmarshal(data, &msg); err != nil {
			c.log.Debug("gateway: bad client message", "err", err)
			continue
		}
		c.handle(ctx, &msg)
	}
}

func (c *client) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-c.send:
			if !ok {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.conn.Write(writeCtx, websocket.MessageBinary, data)
			cancel()
			if err != nil {
				c.kick("write failed")
				return
			}
		}
	}
}

func (c *client) handle(ctx context.Context, msg *dorv1.ClientMessage) {
	switch body := msg.Body.(type) {
	case *dorv1.ClientMessage_Hello:
		if !c.take(&c.inTokens, &c.inRefill, inboundRate, inboundBurst) {
			c.log.Debug("gateway: hello throttled", "client", c.id)
			return
		}
		last := uint64(0)
		if body.Hello != nil {
			last = body.Hello.LastSeq
		}
		c.lastSeq.Store(last)
		c.hub.startResume(ctx, c, last, "")
	case *dorv1.ClientMessage_Ack:
		if body.Ack != nil {
			c.lastSeq.Store(body.Ack.Seq)
		}
	case *dorv1.ClientMessage_SetFilters:
		c.mu.Lock()
		c.filters = body.SetFilters
		c.mu.Unlock()
	case *dorv1.ClientMessage_SetPrompts:
		if !c.take(&c.inTokens, &c.inRefill, inboundRate, inboundBurst) {
			c.log.Debug("gateway: set_prompts throttled", "client", c.id)
			return
		}
		prompts := []string(nil)
		if body.SetPrompts != nil {
			prompts = boundPrompts(body.SetPrompts.Prompts)
		}
		c.mu.Lock()
		c.prompts = &dorv1.SetPrompts{Prompts: prompts}
		c.mu.Unlock()
		if c.matcher != nil {
			c.matcher.SetClientPrompts(ctx, c.id, prompts)
		}
	case *dorv1.ClientMessage_Focus:
		if !c.take(&c.inTokens, &c.inRefill, inboundRate, inboundBurst) ||
			!c.take(&c.textTokens, &c.textRefill, textRate, textBurst) {
			c.log.Debug("gateway: focus throttled", "client", c.id)
			return
		}
		if id := strings.TrimSpace(body.Focus.GetStoryId()); id != "" && len(id) <= maxRequestIDLen {
			c.mu.Lock()
			c.focus = id
			c.mu.Unlock()
			if c.text != nil {
				c.text.PrefetchStory(id)
			}
		}
	case *dorv1.ClientMessage_GetText:
		if !c.take(&c.inTokens, &c.inRefill, inboundRate, inboundBurst) ||
			!c.take(&c.textTokens, &c.textRefill, textRate, textBurst) {
			c.log.Debug("gateway: get_text throttled", "client", c.id)
			return
		}
		if body.GetText != nil && c.text != nil {
			seen := make(map[string]bool, maxGetTextIDs)
			for _, id := range body.GetText.StoryIds {
				if len(seen) >= maxGetTextIDs {
					break
				}
				id = strings.TrimSpace(id)
				if id == "" || len(id) > maxRequestIDLen || seen[id] {
					continue
				}
				seen[id] = true
				c.text.PrefetchStory(id)
			}
			links := 0
			for _, key := range body.GetText.LinkKeys {
				if links >= maxGetTextIDs || key == nil {
					break
				}
				a, b := strings.TrimSpace(key.A), strings.TrimSpace(key.B)
				if a == "" || b == "" || len(a) > maxRequestIDLen || len(b) > maxRequestIDLen {
					continue
				}
				links++
				c.text.PrefetchLink(a, b)
			}
		}
	}
}

// take refills one token bucket and consumes a token. The bucket fields are
// guarded by c.mu, which is also held briefly by filter snapshots.
func (c *client) take(tokens *float64, refill *time.Time, rate, burst float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if elapsed := now.Sub(*refill).Seconds(); elapsed > 0 {
		*tokens += elapsed * rate
		if *tokens > burst {
			*tokens = burst
		}
		*refill = now
	}
	if *tokens < 1 {
		return false
	}
	*tokens--
	return true
}

// enqueue is nonblocking. It is called under hub.mu for broadcasts and resume.
func (c *client) enqueue(data []byte) bool {
	if c.closed.Load() {
		return false
	}
	if !c.allow() {
		return false
	}
	select {
	case c.send <- data:
		return true
	default:
		return false
	}
}

// allow applies the per-client rate limit.
func (c *client) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if elapsed := now.Sub(c.lastRefill).Seconds(); elapsed > 0 {
		c.tokens += elapsed * clientRate
		if c.tokens > clientBurst {
			c.tokens = clientBurst
		}
		c.lastRefill = now
	}
	if c.tokens < 1 {
		return false
	}
	c.tokens--
	return true
}

// kick disconnects a client. It is non-blocking so it can be called while the
// hub lock is held; the client cleans itself up in run's deferred path.
func (c *client) kick(reason string) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	if c.conn != nil {
		c.conn.CloseNow()
	}
	c.log.Debug("gateway: client disconnected", "reason", reason)
}

// resetQueueLocked discards stale unsent messages. A write already in flight
// finishes first; the following Resync resets the client's baseline.
func (c *client) resetQueueLocked() {
	for {
		select {
		case <-c.send:
		default:
			return
		}
	}
}

func (c *client) enqueueRecovery(data []byte) bool {
	if c.closed.Load() {
		return false
	}
	select {
	case c.send <- data:
		return true
	default:
		return false
	}
}

func (c *client) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}
