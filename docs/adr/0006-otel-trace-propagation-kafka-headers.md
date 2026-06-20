# ADR-0006: OTel trace propagation through Kafka headers

**Status:** Accepted

## Context

An order's lifecycle spans repositories: Huginn publishes an intent → sleipnir
runs risk/limit/submit → the exchange fills → sleipnir publishes a fill →
Huginn applies it to the portfolio. To debug latency and failures we need a
**single distributed trace** that follows one order across all those hops, not
disconnected per-service spans. The hops are Kafka topics, so the trace context
has to ride along on the Kafka messages — and it must work even when no OTel
collector is deployed (dev, tests, the simulator).

## Decision

Propagate W3C TraceContext (`traceparent` / `tracestate`) through **Kafka
message headers**, not the JSON payload. The mechanism lives in
`internal/tracing/tracing.go`:

- `Init` (`tracing.go:52`) sets a composite TraceContext + Baggage propagator
  **unconditionally**, before checking for an exporter, so Extract/Inject always
  work. When `OTEL_EXPORTER_OTLP_ENDPOINT` is empty it installs a no-op tracer
  provider (`tracing.go:63`) — in-process spans flow but go nowhere, so tests
  and sim mode need no collector.
- `kafkaHeadersCarrier` (`tracing.go:112`) adapts `[]kafka.Header` to OTel's
  `TextMapCarrier`, with a compile-time interface check (`tracing.go:115`).
- **Consume side:** `ExtractKafkaContext` (`tracing.go:157`) reads the headers
  off the consumed intent and returns a context parented to Huginn's upstream
  span. The gateway calls it at the top of intent handling (`gateway.go:207`),
  then opens child spans `gateway.handle_intent`, `gateway.risk_check`,
  `gateway.limiter_wait`, `exchange.submit_order`, and `gateway.publish_fill`
  off that context.
- **Produce side:** `InjectKafkaHeaders` (`tracing.go:148`) writes the current
  trace context into fresh headers; the fill producer attaches them to the
  outbound message (`internal/kafka/producer.go:53`) so Huginn resumes the same
  trace on the fill leg.

The header names are a cross-stack contract (`docs/CONTRACTS.md`): every service
agrees on the reserved W3C names.

## Consequences

- One order yields **one end-to-end trace** across Huginn → sleipnir → exchange
  → sleipnir → Huginn, with sleipnir's risk/limit/submit/publish phases visible
  as child spans.
- Trace context travels in headers, leaving the message payload clean and
  unchanged — payload consumers don't see tracing fields.
- The gateway degrades gracefully: with no collector configured everything still
  runs (no-op provider), so tracing is zero-config in dev and CI and opt-in in
  prod via one env var.
- Correctness depends on the whole stack using the same propagator and the same
  reserved header names; this is pinned by the shared contract doc, not by code
  in any single repo.
- A human-readable `correlation_id` is assigned per intent for logs
  (`gateway.go:219`), kept distinct from the trace id, so log-grep and
  trace-tooling stay independently useful.
