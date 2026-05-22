// Package kafka wraps segmentio/kafka-go with the consumer and producer
// roles sleipnir needs: pulling execution intents off
// `executions.intents.v1` and publishing verified fills onto
// `executions.fills.v1`. See docs/CONTRACTS.md for the wire spec.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"sleipnir/internal/exchange"
	"sleipnir/internal/telemetry"
)

// Consumer wraps a segmentio kafka.Reader to consume execution intents.
type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
	topic  string
}

// NewConsumer creates a new Kafka consumer wrapper.
func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
	})
	return &Consumer{
		reader: r,
		logger: logger.With("module", "kafka_consumer", "topic", topic),
		topic:  topic,
	}
}

// FetchIntent retrieves the next execution intent. It does not commit the
// offset. The returned kafka.Message carries the upstream trace context in
// its Headers; the gateway loop runs tracing.ExtractKafkaContext on it to
// resume the trace huginn began on the producer side.
func (c *Consumer) FetchIntent(ctx context.Context) (exchange.Order, kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() == nil {
			telemetry.KafkaMessagesProcessed.WithLabelValues(c.topic, "consume", "error").Inc()
		}
		return exchange.Order{}, kafka.Message{}, err
	}

	var intent exchange.Order
	if err := json.Unmarshal(msg.Value, &intent); err != nil {
		telemetry.KafkaMessagesProcessed.WithLabelValues(c.topic, "consume", "error").Inc()
		c.logger.Error("Failed to deserialize execution intent payload", "error", err, "offset", msg.Offset)
		return exchange.Order{}, msg, fmt.Errorf("failed to deserialize intent: %w", err)
	}

	telemetry.KafkaMessagesProcessed.WithLabelValues(c.topic, "consume", "success").Inc()
	return intent, msg, nil
}

// Commit marks messages as successfully processed by committing their offsets.
func (c *Consumer) Commit(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Close gracefully closes the consumer connection.
func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer connection")
	return c.reader.Close()
}
