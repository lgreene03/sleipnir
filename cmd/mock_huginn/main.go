package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"sleipnir/internal/exchange"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting Mock Huginn Quantitative Strategy Engine...")

	// 1. Get configurations from environment
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	topic := os.Getenv("KAFKA_INTENTS_TOPIC")
	if topic == "" {
		topic = "executions.intents.v1"
	}

	logger.Info("Configuration loaded", "kafka_brokers", brokers, "intents_topic", topic)

	// 2. Initialize Kafka Writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Shutdown signal captured. Halting mock strategy...", "signal", sig.String())
		cancel()
	}()

	// 3. Ticker Loop sending simulated order intents
	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()

	instruments := []string{"BTC-USD", "ETH-USD"}
	sides := []exchange.OrderSide{exchange.SideBuy, exchange.SideSell}
	types := []exchange.OrderType{exchange.TypeLimit, exchange.TypeMarket}

	logger.Info("Order intent generation loop active. Operating at 7-second intervals.")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Mock Huginn halted successfully.")
			return
		case <-ticker.C:
			// Generate realistic order intent
			inst := instruments[rand.Intn(len(instruments))]
			side := sides[rand.Intn(len(sides))]
			orderType := types[rand.Intn(len(types))]

			var qty float64
			var price float64

			if inst == "BTC-USD" {
				qty = 0.001 + rand.Float64()*0.004      // 0.001 - 0.005 BTC
				price = 62000.0 + rand.Float64()*3000.0 // 62k - 65k
			} else {
				qty = 0.01 + rand.Float64()*0.09       // 0.01 - 0.1 ETH
				price = 31000.0 + rand.Float64()*150.0 // 3.1k - 3.25k ETH / wait ETH price is around 3k, but let's make it 3100.0 + rand.Float64()*150
			}

			// Format floating values
			qty = floatRound(qty, 4)
			price = floatRound(price, 2)

			// Market orders don't need a price limit
			if orderType == exchange.TypeMarket {
				price = 0.0
			}

			orderID := "huginn-" + uuid.New().String()[:8]

			intent := exchange.Order{
				OrderID:    orderID,
				Instrument: inst,
				Side:       side,
				Quantity:   qty,
				Price:      price,
				Type:       orderType,
			}

			payload, err := json.Marshal(intent)
			if err != nil {
				logger.Error("Failed to marshal mock intent", "error", err)
				continue
			}

			logger.Info(
				"Generating trading intent",
				"orderID", intent.OrderID,
				"instrument", intent.Instrument,
				"side", intent.Side,
				"type", intent.Type,
				"qty", intent.Quantity,
				"price", intent.Price,
			)

			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(intent.OrderID),
				Value: payload,
			})
			if err != nil {
				logger.Error("Failed to publish intent to Kafka", "error", err)
			} else {
				logger.Info("Intent dispatched to Kafka topic", "orderID", intent.OrderID, "topic", topic)
			}
		}
	}
}

func floatRound(val float64, precision int) float64 {
	ratio := 1.0
	for i := 0; i < precision; i++ {
		ratio *= 10
	}
	return float64(int(val*ratio+0.5)) / ratio
}
