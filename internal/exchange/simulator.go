package exchange

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// SimulatorConnector is a fully in-memory ExchangeConnector implementation.
//
// It's the substrate Phase 4's testcontainer integration test will run against
// (no Binance testnet credentials required), and the production paper-mode
// equivalent of "what would happen if Binance always fills cleanly".
//
// Behavior — kept deliberately minimal in this first cut:
//
//   - SubmitOrder always returns a synchronous fill at the requested price
//     (MARKET orders fill at order.Price if non-zero, otherwise at the
//     configured SimulatorConfig.FillPrice). The fill carries a deterministic
//     ExecutionID = "<orderID>-sim-rest-<monotonic counter>".
//   - StartUserStream optionally re-publishes the same fill on the WS channel
//     (SimulatorConfig.AlsoEmitOnWS = true). Disabled by default so callers
//     don't double-count.
//   - CancelOrder is a no-op acknowledgement.
//   - GetOrderState reports StateFilled for any order ever seen by SubmitOrder,
//     and StatePending otherwise.
//
// Configurable knobs exist for the things tests want to control deterministically:
// fill price, transaction cost, latency, and whether to also emit on the WS
// channel. The clock is injectable via Now().
//
// Not yet implemented (deferred to Phase 4/5 simulator extensions):
//   - Partial-fill emission across multiple WS events
//   - Rejection probability
//   - In-memory order-book matching against quoted bid/ask
//   - Time-in-force handling (GTC/IOC/FOK)
//
// These are tracked in docs/ROADMAP.md.
type SimulatorConnector struct {
	cfg    SimulatorConfig
	logger *slog.Logger

	mu       sync.Mutex
	counter  int64
	known    map[string]OrderState
	fillSubs []chan<- ExecutionFill
}

// SimulatorConfig is the knob set tests adjust to drive specific scenarios.
type SimulatorConfig struct {
	// FillPrice is used for MARKET orders when order.Price == 0.
	FillPrice float64

	// TransactionCost in quote-currency units. 0 disables.
	// Ignored when TransactionCostBps > 0.
	TransactionCost float64

	// TransactionCostBps is the fee in basis points (e.g. 10 = 0.10%).
	// When > 0, overrides flat TransactionCost with: price * qty * bps/10000.
	TransactionCostBps float64

	// Latency is the wall-clock delay sleep applied before SubmitOrder returns.
	// Zero is "no sleep". Production tests should use small values (≤ 1 ms).
	Latency time.Duration

	// AlsoEmitOnWS controls whether SubmitOrder also pushes the fill onto the
	// WS fillChan registered by StartUserStream. Off by default so a single
	// SubmitOrder produces one downstream fill, mirroring how a typical
	// MARKET fill arrives only on the REST response.
	AlsoEmitOnWS bool

	// Now is the clock source; defaults to time.Now when nil. Tests inject
	// a fixed clock so timestamps are deterministic.
	Now func() time.Time
}

// NewSimulatorConnector builds a SimulatorConnector with the given config.
func NewSimulatorConnector(cfg SimulatorConfig, logger *slog.Logger) *SimulatorConnector {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.FillPrice == 0 {
		cfg.FillPrice = 1.0
	}
	return &SimulatorConnector{
		cfg:    cfg,
		logger: logger.With("module", "exchange_simulator"),
		known:  make(map[string]OrderState),
	}
}

// SubmitOrder produces a synchronous fill at the requested price (or the
// configured FillPrice for MARKET orders without a price hint).
func (s *SimulatorConnector) SubmitOrder(ctx context.Context, order Order) (ExecutionFill, error) {
	if s.cfg.Latency > 0 {
		select {
		case <-ctx.Done():
			return ExecutionFill{}, ctx.Err()
		case <-time.After(s.cfg.Latency):
		}
	}

	price := order.Price
	if price == 0 {
		price = s.cfg.FillPrice
	}

	s.mu.Lock()
	s.counter++
	executionID := fmt.Sprintf("%s-sim-rest-%d", order.OrderID, s.counter)
	s.known[order.OrderID] = StateFilled
	subs := append([]chan<- ExecutionFill(nil), s.fillSubs...)
	emitWS := s.cfg.AlsoEmitOnWS
	s.mu.Unlock()

	txCost := s.cfg.TransactionCost
	if s.cfg.TransactionCostBps > 0 {
		txCost = price * order.Quantity * (s.cfg.TransactionCostBps / 10_000.0)
	}

	fill := ExecutionFill{
		OrderID:         order.OrderID,
		ExecutionID:     executionID,
		Instrument:      order.Instrument,
		Side:            order.Side,
		OrderStatus:     StateFilled, // simulator fully fills synchronously
		Quantity:        order.Quantity,
		FillPrice:       price,
		TransactionCost: txCost,
		Timestamp:       s.cfg.Now(),
	}
	s.logger.Debug("Simulated REST fill", "orderID", order.OrderID, "qty", order.Quantity, "price", price)

	if emitWS {
		// Asynchronous emit; subscribers must drain fast enough.
		for _, ch := range subs {
			select {
			case ch <- fill:
			case <-ctx.Done():
				return fill, nil
			default:
				// Non-blocking: drop if the subscriber is slow. Mirrors how a
				// downstream consumer would react to a full channel.
				s.logger.Warn("Dropping WS fill — subscriber full", "orderID", order.OrderID)
			}
		}
	}

	return fill, nil
}

// CancelOrder records the cancellation and returns nil.
func (s *SimulatorConnector) CancelOrder(_ context.Context, orderID, instrument string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.known[orderID]; ok && state == StateFilled {
		s.logger.Warn("Cancel called on already-filled order", "orderID", orderID, "instrument", instrument)
	}
	s.known[orderID] = StateCanceled
	return nil
}

// GetOrderState returns whatever state the simulator last recorded for the
// order. The simulator doesn't carry per-order qty (the caller already has
// it) — ExecutedQty is always 0 here.
func (s *SimulatorConnector) GetOrderState(_ context.Context, orderID, _ string) (OrderStatusResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.known[orderID]
	if !ok {
		return OrderStatusResult{State: StatePending, TransactTime: s.cfg.Now()}, nil
	}
	return OrderStatusResult{
		State:        state,
		ExecutedQty:  0,
		FillPrice:    s.cfg.FillPrice,
		TransactTime: s.cfg.Now(),
	}, nil
}

// StartUserStream subscribes the provided channel to subsequent simulator
// fills. Multiple calls fan out to multiple subscribers. The goroutine that
// runs the ctx.Done watch exits cleanly on context cancel.
func (s *SimulatorConnector) StartUserStream(ctx context.Context, fillChan chan<- ExecutionFill) error {
	s.mu.Lock()
	s.fillSubs = append(s.fillSubs, fillChan)
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		// Drop the subscriber on cancel so we don't keep pushing into a
		// channel whose consumer has gone away.
		for i, ch := range s.fillSubs {
			if ch == fillChan {
				s.fillSubs = append(s.fillSubs[:i], s.fillSubs[i+1:]...)
				break
			}
		}
	}()
	return nil
}
