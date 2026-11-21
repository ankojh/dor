package agents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankojh/dor/internal/bus"
)

// EchoDurable is the durable consumer name of the sample echo agent.
const EchoDurable = "echo"

// NewEcho builds the development sample agent: it consumes echo.in, publishes
// the same payload to echo.out and derives the output Nats-Msg-Id from the
// input message id. A redelivery after a crash therefore republishes the same
// id, and JetStream drops the duplicate.
func NewEcho(b *bus.Bus, metrics *Metrics, log *slog.Logger) (Agent, error) {
	return NewBase(b, metrics, log, echoSpec())
}

func echoSpec() Spec {
	return Spec{
		Name:          EchoDurable,
		Durable:       EchoDurable,
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 64,
		AckWait:       5 * time.Second,
		Handle: func(ctx context.Context, msg *Message, out *Emitter) error {
			id := EchoOutputMsgID(msg.ID())
			if err := out.Publish(ctx, bus.SubjectEchoOut, msg.Data, bus.WithMsgID(id)); err != nil {
				return fmt.Errorf("publish echo.out: %w", err)
			}
			return nil
		},
	}
}

// EchoOutputMsgID is deterministic in the input message id and namespaced by
// stage. It is deliberately different from the input id so the input message
// and its echo can coexist in the same domain without being mistaken for
// duplicates of each other.
func EchoOutputMsgID(inputID string) string {
	sum := sha256.Sum256([]byte("echo:" + inputID))
	return "echo:" + hex.EncodeToString(sum[:16])
}

// Builtins registers every agent compiled into this binary. Fetchers are
// always registered; --agents selects what runs.
func Builtins(ctx context.Context, b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg Config) (*Registry, error) {
	registry := NewRegistry()
	echo, err := NewEcho(b, metrics, log)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(echo); err != nil {
		return nil, err
	}

	runtime, err := newFetcherRuntime(ctx, b, metrics, log, cfg)
	if err != nil {
		return nil, err
	}
	for _, build := range []func(*fetcherRuntime) (Agent, error){
		newRSSFetcher,
		newGDELTFetcher,
		newHazardFetcher,
	} {
		agent, err := build(runtime)
		if err != nil {
			return nil, err
		}
		if err := registry.Register(agent); err != nil {
			return nil, err
		}
	}
	return registry, nil
}
