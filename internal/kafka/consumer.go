package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"sleipnir/internal/exchange"
)

// Consumer wraps a segmentio kafka.Reader to consume execution intents.
type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
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
	}
}

// FetchIntent retrieves the next execution intent. It does not commit the offset.
func (c *Consumer) FetchIntent(ctx context.Context) (exchange.Order, kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return exchange.Order{}, kafka.Message{}, err
	}

	var intent exchange.Order
	if err := json.Unmarshal(msg.Value, &intent); err != nil {
		c.logger.Error("Failed to deserialize execution intent payload", "error", err, "offset", msg.Offset)
		return exchange.Order{}, msg, fmt.Errorf("failed to deserialize intent: %w", err)
	}

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
