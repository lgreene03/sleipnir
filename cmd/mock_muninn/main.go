package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"sleipnir/internal/exchange"
)

type PortfolioTracker struct {
	mu          sync.Mutex
	totalFilled map[string]float64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting Mock Muninn Downstream Portfolio Tracker...")

	// 1. Get configurations from environment
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	topic := os.Getenv("KAFKA_FILLS_TOPIC")
	if topic == "" {
		topic = "executions.fills.v1"
	}

	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	if groupID == "" {
		groupID = "mock-muninn-tracker"
	}

	logger.Info("Configuration loaded", 
		"kafka_brokers", brokers, 
		"fills_topic", topic,
		"consumer_group", groupID,
	)

	// 2. Initialize Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Shutdown signal captured. Halting portfolio tracker...", "signal", sig.String())
		cancel()
	}()

	tracker := &PortfolioTracker{
		totalFilled: make(map[string]float64),
	}

	logger.Info("Downstream portfolio fill consumption loop active.")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("Consumer stream closed cleanly.")
				return
			}
			logger.Error("Failed to fetch fill message from Kafka", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var fill exchange.ExecutionFill
		if err := json.Unmarshal(msg.Value, &fill); err != nil {
			logger.Error("Failed to deserialize execution fill payload", "error", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Update portfolio total filled quantities
		tracker.mu.Lock()
		tracker.totalFilled[fill.Instrument] += fill.Quantity
		totalInstrumentFilled := tracker.totalFilled[fill.Instrument]
		tracker.mu.Unlock()

		// Pretty print details with slog
		logger.Info("🔔 DOWNSTREAM EXECUTION FILL RECEIVED",
			"orderID", fill.OrderID,
			"instrument", fill.Instrument,
			"side", fill.Side,
			"qty_filled", fill.Quantity,
			"fill_price", fill.FillPrice,
			"cumulative_instrument_filled", totalInstrumentFilled,
			"transaction_cost", fill.TransactionCost,
			"timestamp", fill.Timestamp.Format(time.RFC3339Nano),
		)

		// Print a pretty console summary
		fmt.Printf("[Muninn Portfolio Tracker] %s | Fill Event: %s %s %s @ %.4f | Cumulative Vol: %.4f | Slippage Cost: $%.4f\n",
			time.Now().Format("15:04:05"),
			fill.Side,
			fill.Instrument,
			fmt.Sprintf("%.4f", fill.Quantity),
			fill.FillPrice,
			totalInstrumentFilled,
			fill.TransactionCost,
		)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Error("Failed to commit message offset", "error", err)
		}
	}
}
