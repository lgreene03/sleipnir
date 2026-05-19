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
	tracker := gateway.NewOrderTracker()
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

	gw := gateway.NewGateway(
		consumer,
		producer,
		connector,
		tracker,
		limiter,
		logger,
	)

	// 5. Spin up a production-grade health check probe HTTP server
	healthServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: healthHandler(tracker),
	}

	go func() {
		logger.Info("Starting health-check server", "port", cfg.Port)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Health-check server encountered an error", "error", err)
		}
	}()

	// 6. Handle signals and coordinate graceful teardown
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

	// 7. Run the Gateway Loop
	if err := gw.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Gateway loops terminated via context completion.")
		} else {
			logger.Error("Fatal runtime failure in Gateway loop", "error", err)
			os.Exit(1)
		}
	}
}

// healthHandler serves liveness probes and active trading telemetry.
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
	return mux
}
