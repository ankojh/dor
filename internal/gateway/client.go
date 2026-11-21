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

	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

const (
	// clientQueue bounds one client's unsent messages. The publisher batches
	// at 250 ms, so 64 messages is ~16 s of backlog; a client further behind
	// is disconnected and resyncs on reconnect.
	clientQueue = 64
	// clientRate and clientBurst limit outgoing messages per client. Normal
	// traffic is ~4 batches/s, so this only trips on abuse or a pathological
	// burst.
	clientRate  = 100.0
	clientBurst = 50.0
)

// client is one WebSocket connection with a bounded send queue and a write
// goroutine. Reads and writes are independent so a slow reader cannot stall
// the hub, and a slow writer is disconnected rather than buffered forever.
type client struct {
	hub     *hub
	text    TextService
	matcher *matcher.Service
	id      string
	conn    *websocket.Conn
	log     *slog.Logger

	send    chan []byte
	closed  atomic.Bool
	cancel  context.CancelFunc
	ready   bool // guarded by hub.mu
	lastSeq atomic.Uint64

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	focus      string
	filters    *dorv1.SetFilters
	prompts    *dorv1.SetPrompts
}

func newClient(h *hub, text TextService, matcherSvc *matcher.Service, conn *websocket.Conn, log *slog.Logger) *client {
	return &client{
		hub:        h,
		text:       text,
		matcher:    matcherSvc,
		id:         newClientID(),
		conn:       conn,
		log:        log,
		send:       make(chan []byte, clientQueue),
		tokens:     clientBurst,
		lastRefill: time.Now(),
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
		last := uint64(0)
		if body.Hello != nil {
			last = body.Hello.LastSeq
		}
		c.lastSeq.Store(last)
		c.hub.resume(c, last)
	case *dorv1.ClientMessage_Ack:
		if body.Ack != nil {
			c.lastSeq.Store(body.Ack.Seq)
		}
	case *dorv1.ClientMessage_SetFilters:
		c.mu.Lock()
		c.filters = body.SetFilters
		c.mu.Unlock()
	case *dorv1.ClientMessage_SetPrompts:
		c.mu.Lock()
		c.prompts = body.SetPrompts
		c.mu.Unlock()
		if c.matcher != nil {
			prompts := []string(nil)
			if body.SetPrompts != nil {
				for _, prompt := range body.SetPrompts.Prompts {
					if prompt = strings.TrimSpace(prompt); prompt != "" {
						prompts = append(prompts, prompt)
					}
				}
			}
			c.matcher.SetClientPrompts(ctx, c.id, prompts)
		}
	case *dorv1.ClientMessage_Focus:
		if body.Focus != nil {
			c.mu.Lock()
			c.focus = body.Focus.StoryId
			c.mu.Unlock()
			if c.text != nil {
				c.text.PrefetchStory(body.Focus.StoryId)
			}
		}
	case *dorv1.ClientMessage_GetText:
		if body.GetText != nil && c.text != nil {
			for _, id := range body.GetText.StoryIds {
				c.text.PrefetchStory(id)
			}
			for _, key := range body.GetText.LinkKeys {
				if key != nil {
					c.text.PrefetchLink(key.A, key.B)
				}
			}
		}
	}
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
	if c.cancel != nil {
		c.cancel()
	}
	c.conn.CloseNow()
	c.log.Debug("gateway: client disconnected", "reason", reason)
}
