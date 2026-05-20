package gateway

import (
	"context"

	"github.com/segmentio/kafka-go"

	"sleipnir/internal/exchange"
)

// IntentConsumer abstracts the inbound Kafka consumer the gateway depends on.
// The real implementation lives in internal/kafka (Consumer); the test suite
// in gateway_e2e_test.go uses a hand-rolled in-memory implementation so the
// full intent → submit → fill → publish loop can be exercised without a
// real broker.
//
// Note on kafka.Message: leaking the segmentio type at the interface boundary
// is the pragmatic choice — wrapping it in a sleipnir-owned Message would
// double the surface for no real isolation gain since both implementations
// already speak this type. See docs/ROADMAP.md Phase 3.
type IntentConsumer interface {
	// FetchIntent retrieves the next execution intent. It does NOT commit the
	// offset; the gateway calls Commit after a successful submit (or risk
	// rejection that should not be retried).
	FetchIntent(ctx context.Context) (exchange.Order, kafka.Message, error)

	// Commit marks one or more messages as processed.
	Commit(ctx context.Context, msgs ...kafka.Message) error

	// Close releases the underlying connection.
	Close() error
}

// FillPublisher abstracts the outbound Kafka producer the gateway uses to
// broadcast verified fills onto executions.fills.v1.
type FillPublisher interface {
	PublishFill(ctx context.Context, fill exchange.ExecutionFill) error
	Close() error
}
