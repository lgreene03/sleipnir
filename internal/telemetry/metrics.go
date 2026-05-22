// Package telemetry defines the Prometheus metrics sleipnir exposes on /metrics.
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

	// WSConnectionDrops tracks total exchange WebSocket connection drops.
	WSConnectionDrops = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sleipnir_ws_connection_drops_total",
			Help: "Total number of live exchange WebSocket user stream connection drops.",
		},
	)

	// RiskRejections tracks total pre-trade risk failures.
	RiskRejections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sleipnir_risk_rejections_total",
			Help: "Total number of orders rejected by pre-trade risk filters.",
		},
		[]string{"instrument", "reason"},
	)

	// ActiveOrders is the current count of orders in a non-terminal state
	// (pending, submitted). Incremented on accept, decremented on fill/reject.
	ActiveOrders = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sleipnir_active_orders",
		Help: "Current number of orders in a non-terminal state (pending or submitted).",
	})

	// IntentToSubmitSeconds measures the latency from intent ingestion to
	// exchange submission call, covering risk check + rate-limiter wait.
	IntentToSubmitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sleipnir_intent_to_submit_seconds",
		Help:    "Latency from Kafka intent consumed to exchange submission call, in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})

	// FillToPublishSeconds measures the latency from a WS fill event being
	// received to the fill being published to Kafka.
	FillToPublishSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sleipnir_fill_to_publish_seconds",
		Help:    "Latency from WebSocket fill received to fill published to Kafka, in seconds.",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	// WSConnected is 1 when the exchange WebSocket user stream is subscribed and
	// receiving messages, 0 otherwise. Used by the WSDisconnectedFor1Min alert.
	WSConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sleipnir_ws_connected",
		Help: "1 if the exchange WebSocket user data stream is currently subscribed, 0 if disconnected or reconnecting.",
	})
)

func init() {
	prometheus.MustRegister(OrdersSubmitted)
	prometheus.MustRegister(OrdersFilled)
	prometheus.MustRegister(OrderLatency)
	prometheus.MustRegister(RateLimitDelay)
	prometheus.MustRegister(KafkaMessagesProcessed)
	prometheus.MustRegister(WSConnectionDrops)
	prometheus.MustRegister(RiskRejections)
	prometheus.MustRegister(ActiveOrders)
	prometheus.MustRegister(IntentToSubmitSeconds)
	prometheus.MustRegister(FillToPublishSeconds)
	prometheus.MustRegister(WSConnected)
}
