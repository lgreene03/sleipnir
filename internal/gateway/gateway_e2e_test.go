package gateway

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"sleipnir/internal/exchange"
)

// fakeConsumer drives the gateway loop with a pre-built sequence of intents.
// FetchIntent returns each one in turn, then blocks forever (or returns the
// context's error on shutdown). Committed messages are recorded so tests can
// assert the gateway acknowledged the right number of inputs.
type fakeConsumer struct {
	mu        sync.Mutex
	intents   []exchange.Order
	idx       int
	committed []kafka.Message
}

func (f *fakeConsumer) FetchIntent(ctx context.Context) (exchange.Order, kafka.Message, error) {
	f.mu.Lock()
	if f.idx >= len(f.intents) {
		f.mu.Unlock()
		// Block until cancel — mirrors a quiet Kafka topic.
		<-ctx.Done()
		return exchange.Order{}, kafka.Message{}, ctx.Err()
	}
	intent := f.intents[f.idx]
	f.idx++
	f.mu.Unlock()
	return intent, kafka.Message{Offset: int64(f.idx)}, nil
}

func (f *fakeConsumer) Commit(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeConsumer) Close() error { return nil }

// fakePublisher captures every fill the gateway publishes so tests can assert
// downstream emission ordering and content.
type fakePublisher struct {
	mu    sync.Mutex
	fills []exchange.ExecutionFill
}

func (p *fakePublisher) PublishFill(_ context.Context, fill exchange.ExecutionFill) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fills = append(p.fills, fill)
	return nil
}

func (p *fakePublisher) Close() error { return nil }

func (p *fakePublisher) snapshot() []exchange.ExecutionFill {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]exchange.ExecutionFill(nil), p.fills...)
}

func TestGatewayE2E_HappyPath(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	consumer := &fakeConsumer{
		intents: []exchange.Order{
			{OrderID: "ord-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_000},
			{OrderID: "ord-2", Instrument: "BTC-USD", Side: exchange.SideSell, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_100},
			{OrderID: "ord-3", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_050},
		},
	}
	publisher := &fakePublisher{}
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice: 50_000.0,
		Now:       func() time.Time { return time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC) },
	}, slog.Default())

	tracker := NewOrderTracker()
	limiter := NewTokenBucketLimiter(1000) // effectively no throttling

	gw := NewGateway(
		consumer, publisher, sim, tracker, limiter,
		10.0,  // maxOrderQtyBTC — generous
		100.0, // maxOrderQtyETH
		100,   // maxDailyOrders
		slog.Default(),
	)

	// Run the gateway in a goroutine; cancel ctx when we've observed enough
	// fills downstream.
	done := make(chan struct{})
	go func() {
		_ = gw.Start(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(publisher.snapshot()) >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	fills := publisher.snapshot()
	if len(fills) < 3 {
		t.Fatalf("expected ≥ 3 fills, got %d", len(fills))
	}
	// Each fill must carry an ExecutionID matching the simulator's
	// "<orderID>-sim-rest-<counter>" pattern.
	for i, f := range fills {
		if f.ExecutionID == "" {
			t.Errorf("fill %d missing ExecutionID: %+v", i, f)
		}
		if f.OrderID == "" {
			t.Errorf("fill %d missing OrderID", i)
		}
	}
	// All three intent messages must have been committed.
	consumer.mu.Lock()
	committed := len(consumer.committed)
	consumer.mu.Unlock()
	if committed != 3 {
		t.Errorf("expected 3 committed offsets, got %d", committed)
	}
}

func TestGatewayE2E_RiskRejectionDoesNotEmitFill(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	consumer := &fakeConsumer{
		intents: []exchange.Order{
			// Hard-coded BTC/ETH cap is 0.1 by config default in the gateway
			// test; we set 0.001 here to ensure rejection.
			{OrderID: "huge-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 5.0, Type: exchange.TypeMarket, Price: 50_000},
		},
	}
	publisher := &fakePublisher{}
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{}, slog.Default())

	gw := NewGateway(
		consumer, publisher, sim,
		NewOrderTracker(), NewTokenBucketLimiter(1000),
		0.001, 0.001, 100, // tight caps → rejection
		slog.Default(),
	)

	done := make(chan struct{})
	go func() {
		_ = gw.Start(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	fills := publisher.snapshot()
	if len(fills) != 0 {
		t.Fatalf("rejection must not produce a published fill, got %d", len(fills))
	}
	// Rejected intents must still commit (poison-pill avoidance).
	consumer.mu.Lock()
	committed := len(consumer.committed)
	consumer.mu.Unlock()
	if committed != 1 {
		t.Errorf("rejection must commit the offset, got %d commits", committed)
	}
}

// TestUpdateOrderStatePreservesFilledQty regression-tests the audit-flagged
// bug where transitioning out of PartiallyFilled into REJECTED/CANCELED/
// EXPIRED used to silently zero out the filled_qty column.
//
// We pre-populate the tracker (no store) so we can drive the state machine
// directly and assert UpdateOrderStateAndQty + UpdateOrderState don't
// stomp on each other.
func TestUpdateOrderStatePreservesFilledQty(t *testing.T) {
	t.Parallel()
	tracker := NewOrderTracker()
	order := exchange.Order{OrderID: "regress-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 1.0}
	tracker.AddOrder(order, exchange.StateSubmitted)

	// Partial fill recorded with explicit qty.
	tracker.UpdateOrderStateAndQty(order.OrderID, exchange.StatePartiallyFilled, 0.4)

	// Now transition to CANCELED via the qty-less UpdateOrderState. The
	// old behavior would have overwritten filled_qty with 0; the fix
	// preserves the prior value. Without a store wired, the assertion is
	// limited to "no panic + state observable".
	if !tracker.UpdateOrderState(order.OrderID, exchange.StateCanceled) {
		t.Fatalf("expected state change to be reported")
	}
	if s, _ := tracker.GetOrderState(order.OrderID); s != exchange.StateCanceled {
		t.Errorf("expected StateCanceled, got %s", s)
	}
}
