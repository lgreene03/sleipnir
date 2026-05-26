//go:build integration

// Package gateway integration tests exercise the full intent → submit → fill →
// publish loop against a real Redpanda broker spun up by Testcontainers-Go.
// The exchange leg uses the in-memory SimulatorConnector so the suite needs no
// Binance testnet credentials. See docs/ROADMAP.md Phase 4.
//
// These tests are gated by the `integration` build tag and only run under the
// integration-tests.yml CI workflow.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"sleipnir/internal/exchange"
	"sleipnir/internal/kafka"
)

const (
	intentsTopic = "executions.intents.v1"
	fillsTopic   = "executions.fills.v1"
	groupID      = "sleipnir-gateway-integration"
)

// TestGatewayIntegration_RoundTrip wires the gateway with a real Redpanda
// broker on the consume/produce sides and the SimulatorConnector on the
// exchange side. It seeds three execution intents onto the intents topic and
// asserts the matching three fills arrive on the fills topic with non-empty
// ExecutionIDs.
func TestGatewayIntegration_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	broker := startRedpanda(ctx, t)
	createTopic(ctx, t, broker, intentsTopic)
	createTopic(ctx, t, broker, fillsTopic)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	consumer := kafka.NewConsumer([]string{broker}, intentsTopic, groupID, logger)
	t.Cleanup(func() { _ = consumer.Close() })

	producer := kafka.NewProducer([]string{broker}, fillsTopic, logger)
	t.Cleanup(func() { _ = producer.Close() })

	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice: 50_000.0,
		Now:       func() time.Time { return time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC) },
	}, logger)

	gw := NewGateway(
		consumer, producer, sim,
		NewOrderTracker(),
		NewTokenBucketLimiter(1000),
		NewLegacyRiskPolicy(10.0, 100.0),
		NewHalt(),
		100,
		logger,
	)

	gwCtx, gwCancel := context.WithCancel(ctx)
	gwDone := make(chan error, 1)
	go func() { gwDone <- gw.Start(gwCtx) }()
	t.Cleanup(func() {
		gwCancel()
		select {
		case <-gwDone:
		case <-time.After(5 * time.Second):
			t.Logf("gateway did not shut down within 5s")
		}
	})

	intents := []exchange.Order{
		{OrderID: "int-1", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_000},
		{OrderID: "int-2", Instrument: "BTC-USD", Side: exchange.SideSell, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_100},
		{OrderID: "int-3", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01, Type: exchange.TypeMarket, Price: 50_050},
	}
	produceIntents(ctx, t, broker, intents)

	got := readFills(ctx, t, broker, len(intents), 90*time.Second)
	if len(got) != len(intents) {
		t.Fatalf("expected %d fills round-tripped through Kafka, got %d", len(intents), len(got))
	}
	seen := make(map[string]bool, len(got))
	for i, f := range got {
		if f.ExecutionID == "" {
			t.Errorf("fill %d missing ExecutionID: %+v", i, f)
		}
		if f.OrderID == "" {
			t.Errorf("fill %d missing OrderID", i)
		}
		if seen[f.ExecutionID] {
			t.Errorf("duplicate ExecutionID %q in published fills", f.ExecutionID)
		}
		seen[f.ExecutionID] = true
	}
}

func startRedpanda(ctx context.Context, t *testing.T) string {
	t.Helper()
	container, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v24.3.1",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		t.Fatalf("failed to start redpanda container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(container); termErr != nil {
			t.Logf("failed to terminate redpanda container: %v", termErr)
		}
	})

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("failed to resolve redpanda seed broker: %v", err)
	}
	return broker
}

func createTopic(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := segmentio.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

func produceIntents(ctx context.Context, t *testing.T, broker string, intents []exchange.Order) {
	t.Helper()
	writer := &segmentio.Writer{
		Addr:         segmentio.TCP(broker),
		Topic:        intentsTopic,
		Balancer:     &segmentio.LeastBytes{},
		RequiredAcks: segmentio.RequireAll,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	msgs := make([]segmentio.Message, len(intents))
	for i, intent := range intents {
		payload, err := json.Marshal(intent)
		if err != nil {
			t.Fatalf("marshal intent %s: %v", intent.OrderID, err)
		}
		msgs[i] = segmentio.Message{Key: []byte(intent.OrderID), Value: payload}
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("write intents: %v", err)
	}
}

func readFills(ctx context.Context, t *testing.T, broker string, count int, timeout time.Duration) []exchange.ExecutionFill {
	t.Helper()
	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       fillsTopic,
		GroupID:     fmt.Sprintf("integration-assert-%d", time.Now().UnixNano()),
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: segmentio.FirstOffset,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fills := make([]exchange.ExecutionFill, 0, count)
	for len(fills) < count {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read fill (got %d/%d): %v", len(fills), count, err)
		}
		var fill exchange.ExecutionFill
		if err := json.Unmarshal(msg.Value, &fill); err != nil {
			t.Fatalf("unmarshal fill: %v", err)
		}
		fills = append(fills, fill)
	}
	return fills
}
