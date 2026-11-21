package agents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/matcher"
	"github.com/ankojh/dor/internal/textsvc"
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
type Deps struct {
	ArticleSink ArticleSink
	StorySink   StorySink

	// Gazetteer and Photon are optional; the plotter marks a deterministic
	// fallback when either is absent.
	Gazetteer *geo.Gazetteer
	Photon    *geo.Photon

	// Decider is shared by the plotter, classifier and relation judge. Nil
	// means the deterministic fallbacks (never a fake model answer).
	Decider  decide.Decider
	Enricher *llm.Client
	// Text is the brief/why service. Nil gets a fallback-only instance.
	Text       *textsvc.Service
	Thresholds config.Thresholds

	// Embedding is the remote TEI client; nil or an error produces a degraded
	// deterministic vector.
	Embedding EmbeddingClient

	// StoryIndex is the optional pgvector ANN candidate source. Nil uses the
	// loaded in-memory 48 h story map.
	StoryIndex StoryIndex

	// LinkSink is the durable link mirror; nil keeps links in KV only.
	LinkSink LinkSink

	// RelationShards and RelationShard partition story shards across replicas.
	// Shard -1 runs every shard in one process.
	RelationShards   int
	RelationShard    int
	RelationDebounce time.Duration
	EditorDebounce   time.Duration

	// M4 prompt filters and alerts. Matcher and Alerts are shared with the
	// gateway so REST backtests and the live matcher see the same state.
	Alerts      *alert.Store
	Matcher     *matcher.Service
	NtfyBaseURL string
	Push        PushSender
	AlertRate   time.Duration
}

func Builtins(ctx context.Context, b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg Config) (*Registry, error) {
	return BuiltinsWithDeps(ctx, b, metrics, log, cfg, Deps{})
}

func BuiltinsWithDeps(ctx context.Context, b *bus.Bus, metrics *Metrics, log *slog.Logger, cfg Config, deps Deps) (*Registry, error) {
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
	normalize, err := NewNormalizer(b, metrics, log, NormalizerConfig{Sink: deps.ArticleSink})
	if err != nil {
		return nil, err
	}
	if err := registry.Register(normalize); err != nil {

		return nil, err
	}

	plot, err := NewPlotter(b, metrics, log, deps)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(plot); err != nil {
		return nil, err
	}
	classify, err := NewClassifier(b, metrics, log, deps)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(classify); err != nil {
		return nil, err
	}
	embed, err := NewEmbedder(b, metrics, log, deps)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(embed); err != nil {
		return nil, err
	}
	cluster, err := NewClusterer(b, metrics, log, deps)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(cluster); err != nil {
		return nil, err
	}

	publisher, err := NewPublisher(b, metrics, log)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(publisher); err != nil {
		return nil, err
	}

	thresholds := deps.Thresholds
	if thresholds.Relate.Accept == 0 {
		thresholds = config.DefaultThresholds()
	}
	relation, err := NewRelationJudge(b, metrics, log, RelationConfig{
		Decider:    deps.Decider,
		Thresholds: thresholds,
		Persist:    deps.LinkSink,
		Shards:     deps.RelationShards,
		Shard:      deps.RelationShard,
		Debounce:   deps.RelationDebounce,
	})
	if err != nil {
		return nil, err
	}
	if err := registry.Register(relation); err != nil {
		return nil, err
	}

	text := deps.Text
	if text == nil {
		text = textsvc.New(b, log, nil)
	}
	editor, err := NewEditor(b, metrics, log, EditorConfig{
		Text:     text,
		Debounce: deps.EditorDebounce,
	})
	if err != nil {
		return nil, err
	}
	if err := registry.Register(editor); err != nil {
		return nil, err
	}

	if deps.Matcher != nil {
		matcherAgent, err := NewMatcher(b, metrics, log, deps.Matcher)
		if err != nil {
			return nil, err
		}
		if err := registry.Register(matcherAgent); err != nil {
			return nil, err
		}
	}
	if deps.Alerts != nil {
		notifierAgent, err := NewNotifier(b, metrics, log, NotifierConfig{
			Alerts:      deps.Alerts,
			NtfyBaseURL: deps.NtfyBaseURL,
			Push:        deps.Push,
			MinInterval: deps.AlertRate,
		})
		if err != nil {
			return nil, err
		}
		if err := registry.Register(notifierAgent); err != nil {
			return nil, err
		}
	}
	return registry, nil
}
