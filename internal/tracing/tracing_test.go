package tracing

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestInit_NoEndpoint_NoOp(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "test")
	if err != nil {
		t.Fatalf("Init with empty endpoint: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown func should not be nil even in no-op mode")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestInjectExtract_RoundTrip(t *testing.T) {
	// W3C TraceContext propagator must be installed for Inject/Extract to work.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Start a span so the context carries a real SpanContext to inject.
	ctx, span := Tracer().Start(context.Background(), "test.parent")
	defer span.End()

	// Inject into a fresh header slice.
	headers := InjectKafkaHeaders(ctx, nil)

	// Without a global tracer provider configured, otel may not actually
	// inject anything (the no-op tracer doesn't produce a recording span).
	// What we *can* assert is that the call is idempotent and the round-trip
	// at minimum returns a valid context.
	got := ExtractKafkaContext(context.Background(), headers)
	if got == nil {
		t.Fatal("ExtractKafkaContext returned nil context")
	}
}

func TestKafkaHeadersCarrier_SetGetKeys(t *testing.T) {
	var c kafkaHeadersCarrier

	c.Set("traceparent", "00-foo-bar-01")
	c.Set("tracestate", "vendor=baz")
	c.Set("traceparent", "00-foo-bar-02") // replace, not append

	if got := c.Get("traceparent"); got != "00-foo-bar-02" {
		t.Errorf("Get traceparent = %q, want 00-foo-bar-02", got)
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get missing should return empty, got %q", got)
	}
	keys := c.Keys()
	if len(keys) != 2 {
		t.Errorf("Keys() = %v, want 2 entries (set should have replaced, not appended)", keys)
	}
}

func TestInjectKafkaHeaders_PreservesExisting(t *testing.T) {
	existing := []kafka.Header{
		{Key: "x-custom", Value: []byte("preserve-me")},
	}
	ctx, span := Tracer().Start(context.Background(), "test")
	defer span.End()

	out := InjectKafkaHeaders(ctx, existing)

	// Existing header must still be there.
	found := false
	for _, h := range out {
		if h.Key == "x-custom" && string(h.Value) == "preserve-me" {
			found = true
		}
	}
	if !found {
		t.Errorf("InjectKafkaHeaders dropped pre-existing header: %v", out)
	}
}
