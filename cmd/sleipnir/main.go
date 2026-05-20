package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sleipnir/internal/config"
	"sleipnir/internal/exchange"
	"sleipnir/internal/gateway"
	"sleipnir/internal/kafka"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 1. Initialize structured JSON logging (stdout)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting Sleipnir Order Execution Gateway...")

	// 2. Load environment configurations
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Failed to initialize configuration. Ensure API credentials are set.", "error", err)
		os.Exit(1)
	}

	logger.Info("Configuration loaded successfully",
		"kafka_brokers", cfg.KafkaBrokers,
		"intents_topic", cfg.KafkaIntentsTopic,
		"fills_topic", cfg.KafkaFillsTopic,
		"rate_limit_rps", cfg.RateLimitRPS,
	)

	// 3. Set up signal catching for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 4. Initialize components (Dependency Injection)
	// Database path volume-mounted or default
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/app/data/sleipnir.db"
	}
	
	logger.Info("Initializing persistent SQLite database...", "path", dbPath)
	store, err := gateway.NewSQLiteOrderStore(dbPath)
	if err != nil {
		logger.Error("Failed to initialize SQLite order store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Initialize the tracker and preload non-terminal active orders
	tracker := gateway.NewOrderTracker().WithStore(store)
	limiter := gateway.NewTokenBucketLimiter(cfg.RateLimitRPS)
	
	connector := exchange.NewBinanceConnector(
		cfg.BinanceAPIKey,
		cfg.BinanceAPISecret,
		cfg.BinanceRESTURL,
		cfg.BinanceWSURL,
		logger,
	)

	consumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaIntentsTopic,
		cfg.KafkaConsumerGroup,
		logger,
	)

	producer := kafka.NewProducer(
		cfg.KafkaBrokers,
		cfg.KafkaFillsTopic,
		logger,
	)

	// 5. Active Boot-Time Reconciliation Loop
	logger.Info("Performing boot-time order reconciliation against live exchange status...")
	reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 30*time.Second)
	
	activeOrders, activeStates, filledQtys, err := store.GetActiveOrders(reconcileCtx)
	if err != nil {
		logger.Error("Failed to query active orders for reconciliation", "error", err)
	} else {
		logger.Info("Found outstanding active orders in local store to reconcile", "count", len(activeOrders))
		for _, order := range activeOrders {
			logger.Info("Querying live status for order", "orderID", order.OrderID, "instrument", order.Instrument)
			
			exchState, exchFilledQty, exchPrice, err := connector.GetOrderState(reconcileCtx, order.OrderID, order.Instrument)
			if err != nil {
				logger.Error("Failed to query order state on exchange during boot reconciliation", "orderID", order.OrderID, "error", err)
				continue
			}

			prevFilledQty := filledQtys[order.OrderID]
			deltaQty := exchFilledQty - prevFilledQty
			
			logger.Info("Reconciliation details", 
				"orderID", order.OrderID, 
				"local_state", activeStates[order.OrderID], 
				"exchange_state", exchState, 
				"prev_filled_qty", prevFilledQty, 
				"exchange_filled_qty", exchFilledQty,
				"delta_qty", deltaQty,
			)

			if deltaQty > 0 {
				logger.Info("Detected missed fills. Backfilling fill message to Kafka...", "orderID", order.OrderID, "qty", deltaQty)
				
				fill := exchange.ExecutionFill{
					OrderID: order.OrderID,
					// Stable across restarts: same (orderID, deltaQty) → same ExecutionID.
					// Huginn's LRU dedup uses this to drop the event if it has already
					// applied the WS fill that this reconciliation is backfilling.
					ExecutionID:     fmt.Sprintf("%s-reconcile-%g", order.OrderID, deltaQty),
					Instrument:      order.Instrument,
					Side:            order.Side,
					Quantity:        deltaQty,
					FillPrice:       exchPrice,
					TransactionCost: 0.0,
					Timestamp:       time.Now(),
				}

				if err := producer.PublishFill(reconcileCtx, fill); err != nil {
					logger.Error("Failed to publish backfilled execution fill to Kafka", "orderID", order.OrderID, "error", err)
				} else {
					logger.Info("Successfully backfilled execution fill to Kafka", "orderID", order.OrderID)
				}
			}

			// Synchronize status in tracking memory & persistent DB
			tracker.UpdateOrderStateAndQty(order.OrderID, exchState, exchFilledQty)
		}
		logger.Info("Active boot-time reconciliation completed successfully.")
	}
	reconcileCancel()

	gw := gateway.NewGateway(
		consumer,
		producer,
		connector,
		tracker,
		limiter,
		cfg.MaxOrderQtyBTC,
		cfg.MaxOrderQtyETH,
		cfg.MaxDailyOrders,
		logger,
	)

	// 6. Spin up a production-grade health check probe HTTP server (with Prometheus /metrics)
	healthServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           healthHandler(tracker),
		ReadHeaderTimeout: 10 * time.Second, // mitigates Slowloris (gosec G112)
	}

	go func() {
		logger.Info("Starting HTTP API (health-check & telemetry) server", "port", cfg.Port)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP API server encountered an error", "error", err)
		}
	}()

	// 7. Handle signals and coordinate graceful teardown
	go func() {
		sig := <-sigChan
		logger.Info("System signal captured, triggering shutdown sequence...", "signal", sig.String())
		cancel() // Propagate cancellation context

		// Shut down health server
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Error during health-check server shutdown", "error", err)
		}

		// Close Kafka resources safely
		if err := consumer.Close(); err != nil {
			logger.Error("Error closing Kafka consumer connection", "error", err)
		}
		if err := producer.Close(); err != nil {
			logger.Error("Error closing Kafka producer connection", "error", err)
		}
		logger.Info("Sleipnir resources torn down cleanly. Exiting.")
		os.Exit(0)
	}()

	// 8. Run the Gateway Loop
	if err := gw.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Gateway loops terminated via context completion.")
		} else {
			logger.Error("Fatal runtime failure in Gateway loop", "error", err)
			os.Exit(1)
		}
	}
}

// healthHandler serves liveness probes, active trading telemetry, and Prometheus /metrics.
func healthHandler(tracker *gateway.OrderTracker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"OK","service":"sleipnir"}`))
	})
	mux.HandleFunc("/telemetry", func(w http.ResponseWriter, r *http.Request) {
		activeOrders := tracker.GetAllActiveOrders()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"HEALTHY","active_tracked_orders":%d}`, len(activeOrders))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
