package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"sleipnir/internal/exchange"
)

// flakyPublisher fails the first failUntil PublishFill calls (returning a
// transient error) and succeeds thereafter. It records every fill that was
// ultimately accepted plus the total number of attempts, so tests can assert
// the gateway retried and only committed once the broker confirmed.
type flakyPublisher struct {
	mu        sync.Mutex
	failUntil int // number of leading attempts that should fail
	attempts  int
	accepted  []exchange.ExecutionFill
}

func (p *flakyPublisher) PublishFill(_ context.Context, fill exchange.ExecutionFill) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.attempts <= p.failUntil {
		return errors.New("transient broker error")
	}
	p.accepted = append(p.accepted, fill)
	return nil
}

func (p *flakyPublisher) Close() error { return nil }

func (p *flakyPublisher) snapshot() (attempts int, accepted []exchange.ExecutionFill) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts, append([]exchange.ExecutionFill(nil), p.accepted...)
}

// TestCommitWaitsForPublishSuccess (sre-resilience-10) asserts that a transient
// publish failure is retried and the offset is committed only after the broker
// confirms the fill — never before.
func TestCommitWaitsForPublishSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	consumer := &fakeConsumer{
		intents: []exchange.Order{
			{OrderID: "ord-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_000},
		},
	}
	// First two attempts fail, the third succeeds (publishRetries default = 3
	// → up to 4 attempts).
	publisher := &flakyPublisher{failUntil: 2}
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice: 50_000.0,
		Now:       func() time.Time { return time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC) },
	}, slog.Default())

	gw := NewGateway(
		consumer, publisher, sim,
		NewOrderTracker(), NewTokenBucketLimiter(1000),
		NewLegacyRiskPolicy(10.0, 100.0), NewHalt(),
		100, slog.Default(),
	).WithPublishRetry(3, time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = gw.Start(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, accepted := publisher.snapshot()
		if len(accepted) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	attempts, accepted := publisher.snapshot()
	if len(accepted) != 1 {
		t.Fatalf("expected exactly 1 accepted fill, got %d", len(accepted))
	}
	if attempts < 3 {
		t.Errorf("expected the publish to be retried (>=3 attempts), got %d", attempts)
	}
	// The offset MUST be committed only after the publish succeeded.
	consumer.mu.Lock()
	committed := len(consumer.committed)
	consumer.mu.Unlock()
	if committed != 1 {
		t.Fatalf("expected exactly 1 committed offset after publish success, got %d", committed)
	}
}

// hardDownPublisher always fails. Used to assert that an unrecoverable publish
// leaves the offset UNcommitted so the intent is redelivered rather than
// silently desyncing huginn.
type hardDownPublisher struct {
	mu       sync.Mutex
	attempts int
}

func (p *hardDownPublisher) PublishFill(_ context.Context, _ exchange.ExecutionFill) error {
	p.mu.Lock()
	p.attempts++
	p.mu.Unlock()
	return errors.New("broker hard down")
}

func (p *hardDownPublisher) Close() error { return nil }

// TestNoCommitWhenPublishExhausted (sre-resilience-10) asserts that when every
// publish attempt fails, the gateway does NOT commit the offset. A committed
// offset on a lost fill is exactly the desync the fix prevents.
func TestNoCommitWhenPublishExhausted(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	consumer := &fakeConsumer{
		intents: []exchange.Order{
			{OrderID: "ord-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_000},
		},
	}
	publisher := &hardDownPublisher{}
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice: 50_000.0,
		Now:       func() time.Time { return time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC) },
	}, slog.Default())

	gw := NewGateway(
		consumer, publisher, sim,
		NewOrderTracker(), NewTokenBucketLimiter(1000),
		NewLegacyRiskPolicy(10.0, 100.0), NewHalt(),
		100, slog.Default(),
	).WithPublishRetry(2, time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = gw.Start(ctx)
		close(done)
	}()

	// Give the loop time to exhaust its retries on the single intent.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		publisher.mu.Lock()
		a := publisher.attempts
		publisher.mu.Unlock()
		if a >= 3 { // retries=2 → 3 attempts
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	publisher.mu.Lock()
	attempts := publisher.attempts
	publisher.mu.Unlock()
	if attempts < 3 {
		t.Errorf("expected 3 publish attempts (retries=2), got %d", attempts)
	}
	consumer.mu.Lock()
	committed := len(consumer.committed)
	consumer.mu.Unlock()
	if committed != 0 {
		t.Fatalf("offset must NOT be committed when publish is exhausted, got %d commits", committed)
	}
}

// slowConnector blocks SubmitOrder until either the supplied delay elapses or
// the context is canceled, letting us assert the submit timeout fires.
type slowConnector struct {
	delay    time.Duration
	canceled atomic.Bool
}

func (c *slowConnector) SubmitOrder(ctx context.Context, order exchange.Order) (exchange.ExecutionFill, error) {
	select {
	case <-time.After(c.delay):
		return exchange.ExecutionFill{
			OrderID: order.OrderID, ExecutionID: order.OrderID + "-x",
			Instrument: order.Instrument, Side: order.Side,
			OrderStatus: exchange.StateFilled, Quantity: order.Quantity,
			FillPrice: order.Price,
		}, nil
	case <-ctx.Done():
		c.canceled.Store(true)
		return exchange.ExecutionFill{}, ctx.Err()
	}
}

func (c *slowConnector) StartUserStream(_ context.Context, _ chan<- exchange.ExecutionFill) error {
	return nil
}

func (c *slowConnector) CancelOrder(_ context.Context, _, _ string) error { return nil }

func (c *slowConnector) GetOrderState(_ context.Context, _, _ string) (exchange.OrderStatusResult, error) {
	return exchange.OrderStatusResult{}, nil
}

// TestSubmitTimeoutBoundsSlowExchange (sre-resilience-9) asserts that a slow
// exchange call is cut off by the configured submit timeout instead of blocking
// the serial intent loop indefinitely.
func TestSubmitTimeoutBoundsSlowExchange(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	consumer := &fakeConsumer{
		intents: []exchange.Order{
			{OrderID: "slow-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_000},
		},
	}
	publisher := &fakePublisher{}
	// SubmitOrder would block for 10s; the 50ms timeout must cut it off.
	conn := &slowConnector{delay: 10 * time.Second}

	gw := NewGateway(
		consumer, publisher, conn,
		NewOrderTracker(), NewTokenBucketLimiter(1000),
		NewLegacyRiskPolicy(10.0, 100.0), NewHalt(),
		100, slog.Default(),
	).WithSubmitTimeout(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = gw.Start(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn.canceled.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if !conn.canceled.Load() {
		t.Fatal("expected submit timeout to cancel the slow exchange call, but SubmitOrder was never canceled")
	}
	// The failed submission must commit its offset (rejection path) and emit no fill.
	if got := len(publisher.snapshot()); got != 0 {
		t.Errorf("timed-out submission must not produce a fill, got %d", got)
	}
}

// abnormalStreamConnector signals (via an atomic flag) once the gateway has
// handed it the fill channel, so the test can then close that channel to
// simulate the user-data stream dying while the gateway is still running.
type abnormalStreamConnector struct {
	started atomic.Bool
}

func (c *abnormalStreamConnector) SubmitOrder(_ context.Context, order exchange.Order) (exchange.ExecutionFill, error) {
	return exchange.ExecutionFill{OrderID: order.OrderID}, nil
}

func (c *abnormalStreamConnector) StartUserStream(_ context.Context, _ chan<- exchange.ExecutionFill) error {
	c.started.Store(true)
	return nil
}

func (c *abnormalStreamConnector) CancelOrder(_ context.Context, _, _ string) error { return nil }

func (c *abnormalStreamConnector) GetOrderState(_ context.Context, _, _ string) (exchange.OrderStatusResult, error) {
	return exchange.OrderStatusResult{}, nil
}

// quietConsumer never produces an intent; it just blocks until ctx is done so
// only the fills worker is interesting in the abnormal-exit test.
type quietConsumer struct{}

func (quietConsumer) FetchIntent(ctx context.Context) (exchange.Order, kafka.Message, error) {
	<-ctx.Done()
	return exchange.Order{}, kafka.Message{}, ctx.Err()
}
func (quietConsumer) Commit(_ context.Context, _ ...kafka.Message) error { return nil }
func (quietConsumer) Close() error                                       { return nil }

// TestStartReturnsErrorOnAbnormalConsumerExit (sre-resilience-15) closes the
// fills channel while the gateway is running (ctx NOT canceled) and asserts
// that Start returns a non-nil error instead of nil.
func TestStartReturnsErrorOnAbnormalConsumerExit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	conn := &abnormalStreamConnector{}
	gw := NewGateway(
		quietConsumer{}, &fakePublisher{}, conn,
		NewOrderTracker(), NewTokenBucketLimiter(1000),
		NewLegacyRiskPolicy(10.0, 100.0), NewHalt(),
		100, slog.Default(),
	)

	errCh := make(chan error, 1)
	go func() { errCh <- gw.Start(ctx) }()

	// Wait for StartUserStream to have run, then close the gateway's fill
	// channel to simulate the stream dying abnormally.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !conn.started.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if !conn.started.Load() {
		t.Fatal("connector StartUserStream never ran")
	}
	close(gw.fillChan)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected non-nil error after abnormal fills-worker exit, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after abnormal exit")
	}
}
