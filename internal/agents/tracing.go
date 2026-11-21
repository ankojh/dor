package agents

import (
	"context"
	"os"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/ankojh/dor/internal/bus"
)

const tracerName = "github.com/ankojh/dor/internal/agents"

type articleIDKey struct{}

// WithArticleID tags a context so every publish made from it carries the
// article id. The agent framework does this automatically for handled
// messages.
func WithArticleID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, articleIDKey{}, id)
}

// ArticleIDFrom returns the article id carried by ctx, if any.
func ArticleIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(articleIDKey{}).(string)
	return v
}

// SetupTracing installs W3C trace context propagation. With no OTLP endpoint
// the tracer stays a noop; setting OTEL_EXPORTER_OTLP_ENDPOINT switches on
// OTLP/HTTP export. The returned function flushes and shuts the provider down.
func SetupTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// natsHeaderCarrier adapts nats.Header to propagation.TextMapCarrier.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string { return nats.Header(c).Get(key) }

func (c natsHeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func extractTrace(ctx context.Context, h nats.Header) context.Context {
	if h == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, natsHeaderCarrier(h))
}

func injectTrace(ctx context.Context, h nats.Header) {
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(h))
}

func startMessageSpan(ctx context.Context, agent, subject, msgID string, seq uint64, header nats.Header) (context.Context, trace.Span) {
	ctx = extractTrace(ctx, header)
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.operation", "process"),
		attribute.String("messaging.destination.name", subject),
		attribute.String("messaging.message.id", msgID),
		attribute.String("dor.agent", agent),
		attribute.Int64("dor.stream_sequence", int64(seq)),
	}
	if id := header.Get(bus.HeaderArticleID); id != "" {
		attrs = append(attrs, attribute.String("dor.article_id", id))
	}
	return otel.Tracer(tracerName).Start(ctx, "agent.handle",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
}

func failSpan(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
