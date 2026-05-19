package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// OrdersSubmitted tracks total orders submitted by the gateway.
	OrdersSubmitted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sleipnir_orders_submitted_total",
			Help: "Total number of orders submitted to the exchange.",
		},
		[]string{"instrument", "side", "type"},
	)

	// OrdersFilled tracks total order fills processed.
	OrdersFilled = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sleipnir_orders_filled_total",
			Help: "Total number of execution fills successfully processed.",
		},
		[]string{"instrument", "side"},
	)

	// OrderLatency tracks exchange submission/cancellation REST API latency in seconds.
	OrderLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sleipnir_order_latency_seconds",
			Help:    "Latency of exchange REST API operations (submit/cancel) in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "instrument", "status"},
	)

	// RateLimitDelay tracks the cumulative time spent waiting for rate limit tokens.
	RateLimitDelay = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sleipnir_rate_limit_delay_seconds_total",
			Help: "Cumulative rate limiter bucket delay in seconds.",
		},
	)

	// KafkaMessagesProcessed tracks Kafka messages consumed/produced.
	KafkaMessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sleipnir_kafka_messages_processed_total",
			Help: "Total number of Kafka messages processed.",
		},
		[]string{"topic", "operation", "status"},
	)
)

func init() {
	prometheus.MustRegister(OrdersSubmitted)
	prometheus.MustRegister(OrdersFilled)
	prometheus.MustRegister(OrderLatency)
	prometheus.MustRegister(RateLimitDelay)
	prometheus.MustRegister(KafkaMessagesProcessed)
}
