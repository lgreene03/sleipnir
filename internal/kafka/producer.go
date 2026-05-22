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
	"sleipnir/internal/tracing"
)

// Producer wraps a segmentio kafka.Writer to publish fills back to the tracking layer.
type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewProducer creates a new Kafka fills producer wrapper.
func NewProducer(brokers []string, topic string, logger *slog.Logger) *Producer {
	if logger == nil {
		logger = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, // Ensure maximum durability for fills
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	return &Producer{
		writer: w,
		logger: logger.With("module", "kafka_producer", "topic", topic),
	}
}

// PublishFill serializes and writes an ExecutionFill transaction event to Kafka.
func (p *Producer) PublishFill(ctx context.Context, fill exchange.ExecutionFill) error {
	payload, err := json.Marshal(fill)
	if err != nil {
		return fmt.Errorf("failed to marshal execution fill: %w", err)
	}

	p.logger.Info("Publishing fill to downstream tracking layer", "orderID", fill.OrderID, "instrument", fill.Instrument, "qty", fill.Quantity, "price", fill.FillPrice)

	// Inject the current trace context so huginn's FillsConsumer can attach
	// its OnExecutionFill span to the same trace tree the original intent
	// began on huginn's producer side.
	headers := tracing.InjectKafkaHeaders(ctx, nil)

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(fill.OrderID),
		Value:   payload,
		Time:    fill.Timestamp,
		Headers: headers,
	})
	if err != nil {
		telemetry.KafkaMessagesProcessed.WithLabelValues(p.writer.Topic, "produce", "error").Inc()
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	telemetry.KafkaMessagesProcessed.WithLabelValues(p.writer.Topic, "produce", "success").Inc()
	return nil
}

// Close gracefully closes the producer connection.
func (p *Producer) Close() error {
	p.logger.Info("Closing Kafka producer connection")
	return p.writer.Close()
}
