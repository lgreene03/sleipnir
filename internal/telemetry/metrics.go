// Package telemetry defines the Prometheus metrics sleipnir exposes on
// /metrics. Counters for orders submitted/cancelled/rejected, histograms
// for REST latency by endpoint, and gauges for the rate-limit token
// budget. Phase 7 of the roadmap will extend this with active-order
// gauges, intent-to-submit latency, and fill-to-publish latency.
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
)

func init() {
	prometheus.MustRegister(OrdersSubmitted)
	prometheus.MustRegister(OrdersFilled)
	prometheus.MustRegister(OrderLatency)
	prometheus.MustRegister(RateLimitDelay)
	prometheus.MustRegister(KafkaMessagesProcessed)
	prometheus.MustRegister(WSConnectionDrops)
	prometheus.MustRegister(RiskRejections)
}
