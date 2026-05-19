package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sleipnir/internal/exchange"
	"sleipnir/internal/kafka"
)

// Gateway coordinates the order ingestion, submission, tracking, and fills broadcast loops.
type Gateway struct {
	consumer  *kafka.Consumer
	producer  *kafka.Producer
	connector exchange.ExchangeConnector
	tracker   *OrderTracker
	limiter   *TokenBucketLimiter
	logger    *slog.Logger
	fillChan  chan exchange.ExecutionFill
}

// NewGateway creates a new core Gateway.
func NewGateway(
	consumer *kafka.Consumer,
	producer *kafka.Producer,
	connector exchange.ExchangeConnector,
	tracker *OrderTracker,
	limiter *TokenBucketLimiter,
	logger *slog.Logger,
) *Gateway {
	if logger == nil {
		logger = slog.Default()
	}
	return &Gateway{
		consumer:  consumer,
		producer:  producer,
		connector: connector,
		tracker:   tracker,
		limiter:   limiter,
		logger:    logger.With("module", "gateway"),
		fillChan:  make(chan exchange.ExecutionFill, 1000),
	}
}

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
				gw.logger.Info("Gateway received real-time fill", "orderID", fill.OrderID, "instrument", fill.Instrument, "qty", fill.Quantity, "price", fill.FillPrice)

				// Update memory tracker state
				gw.tracker.UpdateOrderState(fill.OrderID, exchange.StateFilled)

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

			// If the submission immediately returned filled units (e.g. filled MARKET orders), broadcast them.
			// Fills received via WebSocket will be deduplicated downstream by sequence timestamps/IDs,
			// but we also submit it here if filled quantity is positive.
			if fill.Quantity > 0 {
				gw.logger.Info("Immediate fill detected on submission", "orderID", fill.OrderID, "qty", fill.Quantity, "price", fill.FillPrice)
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
