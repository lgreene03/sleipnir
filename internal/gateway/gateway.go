// Package gateway is sleipnir's core coordinator: it consumes execution
// intents from Kafka, runs pre-trade risk checks (rate limit + per-instrument
// size + daily count), submits via the ExchangeConnector, tracks order
// lifecycle in SQLite (OrderStore + OrderTracker), and forwards
// WS-discovered fills back to the producer. See docs/ARCHITECTURE.md.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"sleipnir/internal/exchange"
	"sleipnir/internal/telemetry"
)

// Gateway coordinates the order ingestion, submission, tracking, and fills broadcast loops.
type Gateway struct {
	consumer  IntentConsumer
	producer  FillPublisher
	connector exchange.ExchangeConnector
	tracker   *OrderTracker
	limiter   *TokenBucketLimiter
	risk      *RiskPolicy
	halt      *Halt
	logger    *slog.Logger
	fillChan  chan exchange.ExecutionFill

	maxDailyOrders int
	ready          atomic.Bool // flipped true once we've consumed our first message
}

// NewGateway creates a new core Gateway.
func NewGateway(
	consumer IntentConsumer,
	producer FillPublisher,
	connector exchange.ExchangeConnector,
	tracker *OrderTracker,
	limiter *TokenBucketLimiter,
	risk *RiskPolicy,
	halt *Halt,
	maxDailyOrders int,
	logger *slog.Logger,
) *Gateway {
	if logger == nil {
		logger = slog.Default()
	}
	if halt == nil {
		halt = NewHalt()
	}
	return &Gateway{
		consumer:       consumer,
		producer:       producer,
		connector:      connector,
		tracker:        tracker,
		limiter:        limiter,
		risk:           risk,
		halt:           halt,
		logger:         logger.With("module", "gateway"),
		fillChan:       make(chan exchange.ExecutionFill, 1000),
		maxDailyOrders: maxDailyOrders,
	}
}

// Halt returns the Halt switch the operator HTTP endpoints flip.
func (gw *Gateway) Halt() *Halt { return gw.halt }

// IsReady reports whether the gateway has consumed at least one intent
// successfully. /readyz uses this.
func (gw *Gateway) IsReady() bool { return gw.ready.Load() }

// Start launches the background loops of the gateway and blocks until the context is canceled.
func (gw *Gateway) Start(ctx context.Context) error {
	gw.logger.Info("Starting Sleipnir Gateway coordination loops...")

	// 1. Start the live WebSockets user data stream
	if err := gw.connector.StartUserStream(ctx, gw.fillChan); err != nil {
		return fmt.Errorf("failed to start exchange websocket user stream: %w", err)
	}

	var wg sync.WaitGroup

	// 2. Start WebSocket Fills handler loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		gw.logger.Info("Fills processing worker started")
		for {
			select {
			case <-ctx.Done():
				gw.logger.Info("Fills processing worker shutting down")
				return
			case fill, ok := <-gw.fillChan:
				if !ok {
					return
				}
				gw.logger.Info("Gateway received real-time fill",
					"orderID", fill.OrderID,
					"instrument", fill.Instrument,
					"qty", fill.Quantity,
					"price", fill.FillPrice,
					"order_status", string(fill.OrderStatus),
				)

				// Track filled orders metric
				telemetry.OrdersFilled.WithLabelValues(fill.Instrument, string(fill.Side)).Inc()

				// Phase 5 fix: use the exchange-reported order status from the
				// fill, not blanket StateFilled. Partial fills now transition
				// to StatePartiallyFilled and stay active in the store; only
				// the terminal FILLED event moves the order off the active
				// list. UpdateOrderStateAndQty so we don't clobber filled_qty.
				targetState := fill.OrderStatus
				if targetState == "" {
					// Defensive: empty status from an old producer means we
					// treat it as fully filled (legacy behaviour).
					targetState = exchange.StateFilled
				}
				gw.tracker.UpdateOrderStateAndQty(fill.OrderID, targetState, fill.Quantity)

				// Broadcast fill back to the downstream tracking layer (Kafka)
				if err := gw.producer.PublishFill(ctx, fill); err != nil {
					gw.logger.Error("Failed to broadcast execution fill downstream", "orderID", fill.OrderID, "error", err)
				}
			}
		}
	}()

	// 3. Start Kafka Intent Ingestion Loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		gw.logger.Info("Intents ingestion consumer worker started")
		for {
			select {
			case <-ctx.Done():
				gw.logger.Info("Intents consumer worker shutting down")
				return
			default:
			}

			// Ingest next execution intent from Kafka
			intent, msg, err := gw.consumer.FetchIntent(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				gw.logger.Error("Failed to consume intent message from Kafka", "error", err)
				time.Sleep(1 * time.Second) // Throttling fallback on connection drop
				continue
			}

			gw.logger.Info("Ingested new execution intent", "orderID", intent.OrderID, "instrument", intent.Instrument, "qty", intent.Quantity, "side", intent.Side)
			gw.ready.Store(true) // first successful consume → /readyz returns 200

			// Operator kill-switch trumps everything else.
			if gw.halt.IsHalted() {
				gw.logger.Warn("Operator halt active — rejecting intent",
					"orderID", intent.OrderID, "reason", gw.halt.Reason())
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, "operator_halt").Inc()
				gw.tracker.AddOrder(intent, exchange.StateRejected)
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after halt rejection", "orderID", intent.OrderID, "error", commitErr)
				}
				continue
			}

			// Pre-trade risk limit validation checks
			if allowed, reason := gw.checkRiskLimits(ctx, intent); !allowed {
				gw.logger.Error("Pre-trade risk limit check failed. Rejecting order.",
					"orderID", intent.OrderID,
					"instrument", intent.Instrument,
					"qty", intent.Quantity,
					"reason", reason,
				)

				// Increment telemetry risk rejections count
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, reason).Inc()

				// Commit state in memory and database stores as REJECTED
				gw.tracker.AddOrder(intent, exchange.StateRejected)

				// Commit Kafka offset to acknowledge message consumption
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after risk rejection", "orderID", intent.OrderID, "error", commitErr)
				}
				continue
			}

			// Throttling outbound submissions with Token Bucket rate limiter
			if err := gw.limiter.Wait(ctx); err != nil {
				gw.logger.Error("Rate limiter wait cancelled", "error", err)
				continue
			}

			// Register inside thread-safe tracker
			gw.tracker.AddOrder(intent, exchange.StatePending)

			// Send to concrete exchange connector
			fill, err := gw.connector.SubmitOrder(ctx, intent)
			if err != nil {
				gw.logger.Error("Exchange submission failed", "orderID", intent.OrderID, "error", err)
				gw.tracker.UpdateOrderState(intent.OrderID, exchange.StateRejected)

				// Commit offset even on rejection to prevent poisonous message loops
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after rejection", "orderID", intent.OrderID, "error", commitErr)
				}
				continue
			}

			gw.logger.Info("Exchange submission accepted", "orderID", intent.OrderID, "state", exchange.StateSubmitted)
			gw.tracker.UpdateOrderState(intent.OrderID, exchange.StateSubmitted)

			// Track submitted orders metric
			telemetry.OrdersSubmitted.WithLabelValues(intent.Instrument, string(intent.Side), string(intent.Type)).Inc()

			// If the submission immediately returned filled units (e.g. filled MARKET orders), broadcast them.
			// Fills received via WebSocket will be deduplicated downstream by sequence timestamps/IDs,
			// but we also submit it here if filled quantity is positive.
			if fill.Quantity > 0 {
				gw.logger.Info("Immediate fill detected on submission", "orderID", fill.OrderID, "qty", fill.Quantity, "price", fill.FillPrice)

				// Track filled orders metric
				telemetry.OrdersFilled.WithLabelValues(fill.Instrument, string(fill.Side)).Inc()

				gw.tracker.UpdateOrderState(fill.OrderID, exchange.StateFilled)
				if prodErr := gw.producer.PublishFill(ctx, fill); prodErr != nil {
					gw.logger.Error("Failed to broadcast immediate execution fill", "orderID", fill.OrderID, "error", prodErr)
				}
			}

			// Commit Kafka offset now that submission is complete and recorded
			if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
				gw.logger.Error("Failed to commit consumer offset", "orderID", intent.OrderID, "error", commitErr)
			}
		}
	}()

	wg.Wait()
	return nil
}

// checkRiskLimits validates if the intent complies with pre-trade risk
// thresholds. Phase 6 replaced the pre-existing hardcoded BTC/ETH branches
// with a RiskPolicy lookup (audit finding C3); the daily-count check is
// unchanged.
func (gw *Gateway) checkRiskLimits(ctx context.Context, intent exchange.Order) (bool, string) {
	if ok, reason := gw.risk.CheckIntent(intent); !ok {
		return false, reason
	}

	// Daily count limit check.
	if gw.tracker.store != nil {
		count, err := gw.tracker.store.GetDailyOrderCount(ctx)
		if err != nil {
			gw.logger.Error("Failed to check daily order count from store for risk limits", "error", err)
			// Fail-safe by rejecting order if database is unreachable.
			return false, "db_unreachable"
		}
		if count >= gw.maxDailyOrders {
			return false, "daily_count_exceeded"
		}
	}

	return true, ""
}
