// Package tracing provides OpenTelemetry initialization and W3C TraceContext
// propagation through Kafka message headers.
//
// Sleipnir sits in the middle of a trace that spans huginn → sleipnir →
// exchange → sleipnir → huginn. The intent leg arrives from huginn with a
// `traceparent` header; sleipnir extracts the parent, opens child spans for
// risk/limiter/submit/publish-fill, and re-injects a `traceparent` on the
// outbound fill so huginn can resume the same trace on the consumer side.
//
// # Wire format
//
// Headers travel as Kafka message headers (segmentio/kafka-go `kafka.Header`),
// not in the JSON payload. The W3C standard names — `traceparent` and
// `tracestate` — are reserved; downstream consumers across the stack agree on
// these. See docs/CONTRACTS.md.
//
// # No collector configured?
//
// When `OTEL_EXPORTER_OTLP_ENDPOINT` is empty the package installs a no-op
// tracer provider. Dev paths, tests, and the simulator can run without an
// OTel collector deployed.
package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	tracerName  = "sleipnir"
	serviceName = "sleipnir"
)

// Init wires up the global tracer provider. Returns a shutdown function the
// caller must defer to flush pending spans on graceful exit. When the
// `OTEL_EXPORTER_OTLP_ENDPOINT` env var is empty the function returns a
// shutdown that does nothing — the global provider stays no-op.
func Init(ctx context.Context, version string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	// W3C TraceContext propagator is set unconditionally so Extract/Inject
	// work even when no exporter is configured (in-process tracing still
	// flows; spans just go nowhere).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	return func(ctx context.Context) error {
		// Shutdown is bounded so a misconfigured collector can't hang the
		// process. Drop the deadline if it conflicts.
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(shutdownCtx)
	}, nil
}

// Tracer returns sleipnir's tracer. Always safe to call; falls back to the
// no-op tracer when the provider hasn't been initialized.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// kafkaHeadersCarrier adapts a slice of kafka.Header to the propagation
// TextMapCarrier interface so Inject/Extract can read & write trace headers.
type kafkaHeadersCarrier []kafka.Header

// Compile-time check.
var _ propagation.TextMapCarrier = (*kafkaHeadersCarrier)(nil)

func (c *kafkaHeadersCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeadersCarrier) Set(key, value string) {
	// Replace if already present.
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, kafka.Header{Key: key, Value: []byte(value)})
}

func (c *kafkaHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(*c))
	for _, h := range *c {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectKafkaHeaders writes the current trace context into a fresh slice of
// kafka.Header entries derived from `existing`. The Kafka producer call site
// uses the returned slice on its outbound message.
func InjectKafkaHeaders(ctx context.Context, existing []kafka.Header) []kafka.Header {
	carrier := kafkaHeadersCarrier(append([]kafka.Header(nil), existing...))
	otel.GetTextMapPropagator().Inject(ctx, &carrier)
	return []kafka.Header(carrier)
}

// ExtractKafkaContext reads trace headers off a consumed message and returns
// a context that, when used as the parent of new spans, links them into the
// upstream trace.
func ExtractKafkaContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := kafkaHeadersCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, &carrier)
}

// StartSpan is a thin wrapper that pins the tracer name and provides a place
// to attach service-wide attributes if we ever need them. Keep call sites
// short.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}
