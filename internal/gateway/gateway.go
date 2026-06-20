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

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"sleipnir/internal/algo"
	"sleipnir/internal/exchange"
	"sleipnir/internal/telemetry"
	"sleipnir/internal/tracing"
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

	algoExec   *algo.Executor
	algoCfg    *algo.Config
	maxDailyOrders int
	maxDailyBuys   int         // 0 = no per-side cap
	maxDailySells  int         // 0 = no per-side cap
	ready          atomic.Bool // flipped true once we've consumed our first message

	// submitTimeout bounds a single exchange submission (sre-resilience-9). The
	// intent loop is serial, so an unbounded slow exchange call or multi-minute
	// TWAP would block every following instrument. Zero disables the bound.
	submitTimeout time.Duration

	// publishRetries bounds the in-loop retry of an immediate fill publish
	// before the offset is committed (sre-resilience-10). After exhausting
	// these, the offset is NOT committed so the message is redelivered rather
	// than silently desyncing huginn's portfolio.
	publishRetries  int
	publishBackoff  time.Duration
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
		// Conservative defaults; main.go overrides via WithSubmitTimeout.
		// submitTimeout 0 = no bound (legacy behaviour preserved for callers
		// that don't opt in). publishRetries 3 with 100ms→ exponential backoff
		// gives a transient broker blip a few chances before we refuse to
		// commit the offset (sre-resilience-10).
		submitTimeout:  0,
		publishRetries: 3,
		publishBackoff: 100 * time.Millisecond,
	}
}

// WithSubmitTimeout bounds every exchange submission with a deadline
// (sre-resilience-9). Zero disables the bound (legacy behaviour). Loaded from
// SUBMIT_TIMEOUT in main.go.
func (gw *Gateway) WithSubmitTimeout(d time.Duration) *Gateway {
	gw.submitTimeout = d
	return gw
}

// WithPublishRetry configures the bounded retry applied to a fill publish
// before the consumer offset is committed (sre-resilience-10). retries < 0 is
// clamped to 0 (a single attempt). A non-positive backoff disables sleeping
// between attempts.
func (gw *Gateway) WithPublishRetry(retries int, backoff time.Duration) *Gateway {
	if retries < 0 {
		retries = 0
	}
	gw.publishRetries = retries
	gw.publishBackoff = backoff
	return gw
}

// publishFillWithRetry publishes a fill, retrying on transient errors with
// bounded exponential backoff. It returns nil only when the broker has
// confirmed the publish. Callers MUST NOT commit the consumer offset until
// this returns nil — committing first would let a transient broker error
// permanently desync huginn's portfolio (sre-resilience-10).
//
// The retry loop respects ctx cancellation so shutdown is not delayed by a
// hard-down broker.
func (gw *Gateway) publishFillWithRetry(ctx context.Context, fill exchange.ExecutionFill) error {
	backoff := gw.publishBackoff
	var lastErr error
	for attempt := 0; attempt <= gw.publishRetries; attempt++ {
		if attempt > 0 {
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return fmt.Errorf("publish retry aborted: %w", ctx.Err())
				case <-time.After(backoff):
				}
				backoff *= 2
			} else if ctx.Err() != nil {
				return fmt.Errorf("publish retry aborted: %w", ctx.Err())
			}
		}
		lastErr = gw.producer.PublishFill(ctx, fill)
		if lastErr == nil {
			return nil
		}
		gw.logger.Warn("Fill publish attempt failed",
			"orderID", fill.OrderID, "attempt", attempt+1,
			"max_attempts", gw.publishRetries+1, "error", lastErr)
	}
	return fmt.Errorf("publish failed after %d attempts: %w", gw.publishRetries+1, lastErr)
}

// Halt returns the Halt switch the operator HTTP endpoints flip.
func (gw *Gateway) Halt() *Halt { return gw.halt }

// IsReady reports whether the gateway has consumed at least one intent
// successfully. /readyz uses this.
func (gw *Gateway) IsReady() bool { return gw.ready.Load() }

// WithAlgo configures TWAP/VWAP execution for all orders routed through this gateway.
func (gw *Gateway) WithAlgo(exec *algo.Executor, cfg *algo.Config) *Gateway {
	gw.algoExec = exec
	gw.algoCfg = cfg
	return gw
}

// WithDailySideLimits configures the per-side daily order caps. Zero on either
// side means no cap for that side (the combined maxDailyOrders still applies).
// Loaded from MAX_DAILY_BUYS / MAX_DAILY_SELLS env vars in main.go.
func (gw *Gateway) WithDailySideLimits(maxBuys, maxSells int) *Gateway {
	gw.maxDailyBuys = maxBuys
	gw.maxDailySells = maxSells
	return gw
}

// Start launches the background loops of the gateway and blocks until the context is canceled.
func (gw *Gateway) Start(ctx context.Context) error {
	gw.logger.Info("Starting Sleipnir Gateway coordination loops...")

	// Derive a cancelable context so that if one worker exits abnormally we can
	// wind the other down and let Start return promptly (sre-resilience-15)
	// rather than blocking in wg.Wait until the operator cancels.
	ctx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()

	// 1. Start the live WebSockets user data stream
	if err := gw.connector.StartUserStream(ctx, gw.fillChan); err != nil {
		return fmt.Errorf("failed to start exchange websocket user stream: %w", err)
	}

	var wg sync.WaitGroup

	// sre-resilience-15: capture the first abnormal consumer-loop exit so Start
	// returns a non-nil error (instead of nil after wg.Wait) and main can act —
	// e.g. exit non-zero and let the orchestrator restart us. A clean shutdown
	// (ctx canceled) leaves this nil. Guarded by once so the first cause wins
	// and concurrent writes are safe.
	var (
		exitOnce sync.Once
		exitErr  error
	)
	recordAbnormalExit := func(err error) {
		exitOnce.Do(func() { exitErr = err })
		cancelLoops() // wind down the sibling worker so wg.Wait returns
	}

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
					// The exchange closed the fills channel while we were NOT
					// asked to shut down — the user-data stream died. This is
					// abnormal: surface it so Start returns non-nil.
					if ctx.Err() == nil {
						gw.logger.Error("Fills channel closed unexpectedly while running; user-data stream terminated")
						recordAbnormalExit(errors.New("fills worker exited: user-data stream closed unexpectedly"))
					}
					return
				}
				fillReceivedAt := time.Now()
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
				gw.tracker.UpdateOrderStateAndQty(ctx, fill.OrderID, targetState, fill.Quantity)

				// Decrement active orders on terminal state transitions.
				if targetState == exchange.StateFilled || targetState == exchange.StateCanceled ||
					targetState == exchange.StateRejected {
					telemetry.ActiveOrders.Dec()
				}

				// Persist commission and slippage so downstream research can
				// reconstruct realized transaction costs per order. Slippage is
				// (fill_price − intent_price): positive on a buy means filled
				// above the limit; negative means better-than-limit execution.
				if gw.tracker.store != nil {
					slippage := fill.FillPrice
					if intent, ok := gw.tracker.GetOrder(fill.OrderID); ok {
						slippage = fill.FillPrice - intent.Price
					}
					if costErr := gw.tracker.store.RecordFillCosts(ctx, fill.OrderID, fill.TransactionCost, slippage); costErr != nil {
						gw.logger.Warn("Failed to persist fill costs", "orderID", fill.OrderID, "error", costErr)
					}
				}

				// Broadcast fill back to the downstream tracking layer (Kafka).
				// Bounded retry (sre-resilience-10): a WS fill carries no Kafka
				// offset to redeliver it, so a swallowed publish error here
				// permanently desyncs huginn's portfolio. Retry transient broker
				// errors before giving up and logging loudly.
				if err := gw.publishFillWithRetry(ctx, fill); err != nil {
					gw.logger.Error("Failed to broadcast execution fill downstream after retries", "orderID", fill.OrderID, "error", err)
				}
				telemetry.FillToPublishSeconds.Observe(time.Since(fillReceivedAt).Seconds())
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

			// Resume the trace huginn began on the producer side. From this
			// point until span.End() below, every otel-aware call we make is
			// a child of huginn's `PublishIntent` span.
			intentCtx := tracing.ExtractKafkaContext(ctx, msg.Headers)
			intentCtx, span := tracing.StartSpan(intentCtx, "gateway.handle_intent",
				attribute.String("order_id", intent.OrderID),
				attribute.String("instrument", intent.Instrument),
				attribute.String("side", string(intent.Side)),
				attribute.Float64("quantity", intent.Quantity),
			)

			// Assign a correlation ID at consume time to thread through all
			// log lines touching this intent's lifecycle. Distinct from the
			// trace ID (a correlation_id is human-typeable; the trace ID lives
			// in the otel span attributes).
			correlationID := uuid.New().String()
			intentIngestedAt := time.Now()

			gw.logger.Info("Ingested new execution intent",
				"orderID", intent.OrderID,
				"correlation_id", correlationID,
				"instrument", intent.Instrument,
				"qty", intent.Quantity,
				"side", intent.Side,
			)
			gw.ready.Store(true) // first successful consume → /readyz returns 200

			// Operator kill-switch trumps everything else.
			if gw.halt.IsHalted() {
				gw.logger.Warn("Operator halt active — rejecting intent",
					"orderID", intent.OrderID,
					"correlation_id", correlationID,
					"reason", gw.halt.Reason(),
				)
				span.SetAttributes(attribute.String("reject_reason", "operator_halt"))
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, "operator_halt").Inc()
				gw.tracker.AddOrder(ctx, intent, exchange.StateRejected)
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after halt rejection",
						"orderID", intent.OrderID, "correlation_id", correlationID, "error", commitErr)
				}
				span.End()
				continue
			}

			// OrderID validation (closes audit H4). Rejects empty / over-long /
			// disallowed-character IDs before they reach the signed Binance
			// request or overwrite an in-memory tracker slot. Also rejects
			// duplicates against any OrderID the tracker already knows — the
			// gateway is the sole writer to `newClientOrderId`, so a collision
			// here either means a buggy producer or a malicious replay.
			if err := ValidateOrderID(intent.OrderID); err != nil {
				reason := err.Error()
				gw.logger.Warn("Rejecting intent: invalid OrderID",
					"orderID", intent.OrderID,
					"correlation_id", correlationID,
					"reason", reason,
				)
				span.SetAttributes(attribute.String("reject_reason", reason))
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, reason).Inc()
				// Do NOT call tracker.AddOrder — an invalid OrderID is the one
				// case where writing into the tracker is itself the attack.
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after orderID validation rejection",
						"orderID", intent.OrderID, "correlation_id", correlationID, "error", commitErr)
				}
				span.End()
				continue
			}
			if _, dup := gw.tracker.GetOrderState(intent.OrderID); dup {
				gw.logger.Warn("Rejecting intent: duplicate OrderID",
					"orderID", intent.OrderID,
					"correlation_id", correlationID,
				)
				span.SetAttributes(attribute.String("reject_reason", ReasonOrderIDDuplicate))
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, ReasonOrderIDDuplicate).Inc()
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after duplicate OrderID rejection",
						"orderID", intent.OrderID, "correlation_id", correlationID, "error", commitErr)
				}
				span.End()
				continue
			}

			// Pre-trade risk limit validation checks
			riskCtx, riskSpan := tracing.StartSpan(intentCtx, "gateway.risk_check")
			allowed, reason := gw.checkRiskLimits(riskCtx, intent)
			riskSpan.SetAttributes(attribute.Bool("allowed", allowed), attribute.String("reason", reason))
			riskSpan.End()
			if !allowed {
				gw.logger.Error("Pre-trade risk limit check failed. Rejecting order.",
					"orderID", intent.OrderID,
					"correlation_id", correlationID,
					"instrument", intent.Instrument,
					"qty", intent.Quantity,
					"reason", reason,
				)
				span.SetAttributes(attribute.String("reject_reason", reason))
				telemetry.RiskRejections.WithLabelValues(intent.Instrument, reason).Inc()
				gw.tracker.AddOrder(ctx, intent, exchange.StateRejected)
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after risk rejection",
						"orderID", intent.OrderID, "correlation_id", correlationID, "error", commitErr)
				}
				span.End()
				continue
			}

			// Throttling outbound submissions with Token Bucket rate limiter
			limiterCtx, limiterSpan := tracing.StartSpan(intentCtx, "gateway.limiter_wait")
			err = gw.limiter.Wait(limiterCtx)
			limiterSpan.End()
			if err != nil {
				gw.logger.Error("Rate limiter wait cancelled",
					"orderID", intent.OrderID, "correlation_id", correlationID, "error", err)
				span.End()
				continue
			}

			// Register inside thread-safe tracker
			gw.tracker.AddOrder(ctx, intent, exchange.StatePending)
			telemetry.ActiveOrders.Inc()

			// Send to exchange — through algo executor (TWAP/VWAP) if configured,
			// otherwise direct single-shot submission.
			//
			// sre-resilience-9: the intent loop is serial, so bound each
			// submission with a deadline. Without it a slow exchange call or a
			// multi-minute TWAP blocks every following instrument. A worker
			// pool would parallelise this but is out of scope; the timeout is
			// the correctness floor. submitTimeout 0 keeps the legacy unbounded
			// behaviour.
			submitCtx, submitSpan := tracing.StartSpan(intentCtx, "exchange.submit_order")
			var submitCancel context.CancelFunc
			if gw.submitTimeout > 0 {
				submitCtx, submitCancel = context.WithTimeout(submitCtx, gw.submitTimeout)
			}
			var fill exchange.ExecutionFill
			if gw.algoExec != nil && gw.algoCfg != nil {
				result, algoErr := gw.algoExec.Execute(submitCtx, intent, *gw.algoCfg)
				if algoErr != nil {
					err = algoErr
				} else if len(result.Fills) > 0 {
					fill = result.Fills[len(result.Fills)-1]
					fill.Quantity = result.TotalQty
					fill.FillPrice = result.AvgPrice
				}
			} else {
				fill, err = gw.connector.SubmitOrder(submitCtx, intent)
			}
			if submitCancel != nil {
				submitCancel()
			}
			submitSpan.End()
			telemetry.IntentToSubmitSeconds.Observe(time.Since(intentIngestedAt).Seconds())
			if err != nil {
				gw.logger.Error("Exchange submission failed",
					"orderID", intent.OrderID, "correlation_id", correlationID, "error", err)
				span.SetAttributes(attribute.String("reject_reason", "exchange_error"))
				gw.tracker.UpdateOrderState(ctx, intent.OrderID, exchange.StateRejected)
				telemetry.ActiveOrders.Dec()

				// Commit offset even on rejection to prevent poisonous message loops
				if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
					gw.logger.Error("Failed to commit offset after rejection",
						"orderID", intent.OrderID, "correlation_id", correlationID, "error", commitErr)
				}
				span.End()
				continue
			}

			gw.logger.Info("Exchange submission accepted",
				"orderID", intent.OrderID, "correlation_id", correlationID, "state", exchange.StateSubmitted)
			gw.tracker.UpdateOrderState(ctx, intent.OrderID, exchange.StateSubmitted)

			// Track submitted orders metric
			telemetry.OrdersSubmitted.WithLabelValues(intent.Instrument, string(intent.Side), string(intent.Type)).Inc()

			// If the submission immediately returned filled units (e.g. filled MARKET orders), broadcast them.
			// Fills received via WebSocket will be deduplicated downstream by sequence timestamps/IDs,
			// but we also submit it here if filled quantity is positive.
			if fill.Quantity > 0 {
				gw.logger.Info("Immediate fill detected on submission",
					"orderID", fill.OrderID, "correlation_id", correlationID,
					"qty", fill.Quantity, "price", fill.FillPrice)

				// Track filled orders metric
				telemetry.OrdersFilled.WithLabelValues(fill.Instrument, string(fill.Side)).Inc()
				telemetry.ActiveOrders.Dec()

				gw.tracker.UpdateOrderState(ctx, fill.OrderID, exchange.StateFilled)

				// Record commission and slippage for the immediately-returned fill.
				if gw.tracker.store != nil {
					slippage := fill.FillPrice - intent.Price
					if costErr := gw.tracker.store.RecordFillCosts(ctx, fill.OrderID, fill.TransactionCost, slippage); costErr != nil {
						gw.logger.Warn("Failed to persist immediate fill costs",
							"orderID", fill.OrderID, "correlation_id", correlationID, "error", costErr)
					}
				}

				// sre-resilience-10: confirm the fill is on the broker BEFORE we
				// commit the intent offset. Use the loop's parent ctx (not the
				// per-submit deadline ctx, which may already be cancelled) so the
				// retry has its own lifetime. If the publish ultimately fails we
				// skip the commit so the intent is redelivered rather than
				// silently dropping the fill and desyncing huginn's portfolio.
				publishCtx, publishSpan := tracing.StartSpan(ctx, "gateway.publish_fill",
					attribute.String("execution_id", fill.ExecutionID),
				)
				prodErr := gw.publishFillWithRetry(publishCtx, fill)
				publishSpan.End()
				if prodErr != nil {
					gw.logger.Error("Failed to broadcast immediate execution fill after retries; NOT committing offset so the intent is redelivered",
						"orderID", fill.OrderID, "correlation_id", correlationID, "error", prodErr)
					span.SetAttributes(attribute.String("publish_error", prodErr.Error()))
					span.End()
					continue
				}
			}

			// Commit Kafka offset now that submission is complete, recorded, and
			// any immediate fill has been confirmed on the broker (sre-resilience-10).
			if commitErr := gw.consumer.Commit(ctx, msg); commitErr != nil {
				gw.logger.Error("Failed to commit consumer offset", "orderID", intent.OrderID, "error", commitErr)
			}
			span.End()
		}
	}()

	wg.Wait()

	// sre-resilience-15: if a worker exited abnormally (not a clean ctx
	// cancellation), surface it. A clean operator/SIGTERM shutdown returns nil.
	if exitErr != nil {
		return exitErr
	}
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

		// Per-side daily caps (Phase 6). A zero cap means "no limit for this side".
		if intent.Side == exchange.SideBuy && gw.maxDailyBuys > 0 {
			buyCount, err := gw.tracker.store.GetDailyOrderCountBySide(ctx, exchange.SideBuy)
			if err != nil {
				gw.logger.Error("Failed to check daily buy count", "error", err)
				return false, "db_unreachable"
			}
			if buyCount >= gw.maxDailyBuys {
				return false, "daily_buy_count_exceeded"
			}
		}
		if intent.Side == exchange.SideSell && gw.maxDailySells > 0 {
			sellCount, err := gw.tracker.store.GetDailyOrderCountBySide(ctx, exchange.SideSell)
			if err != nil {
				gw.logger.Error("Failed to check daily sell count", "error", err)
				return false, "db_unreachable"
			}
			if sellCount >= gw.maxDailySells {
				return false, "daily_sell_count_exceeded"
			}
		}
	}

	return true, ""
}
