// Package config loads sleipnir's runtime configuration from environment
// variables (via envconfig). All settings have sensible testnet defaults;
// the only required values are BINANCE_API_KEY and BINANCE_API_SECRET.
package config

import (
	"fmt"
	"os"

	"github.com/kelseyhightower/envconfig"
)

// Config holds the service configuration for Sleipnir.
type Config struct {
	KafkaBrokers       []string `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	KafkaIntentsTopic  string   `envconfig:"KAFKA_INTENTS_TOPIC" default:"executions.intents.v1"`
	KafkaFillsTopic    string   `envconfig:"KAFKA_FILLS_TOPIC" default:"executions.fills.v1"`
	KafkaConsumerGroup string   `envconfig:"KAFKA_CONSUMER_GROUP" default:"sleipnir-gateway"`

	BinanceAPIKey    string `envconfig:"BINANCE_API_KEY"`
	BinanceAPISecret string `envconfig:"BINANCE_API_SECRET"`
	BinanceRESTURL   string `envconfig:"BINANCE_REST_URL" default:"https://testnet.binance.vision"`
	BinanceWSURL     string `envconfig:"BINANCE_WS_URL" default:"wss://ws-api.testnet.binance.vision/ws-api/v3"`

	RateLimitRPS float64 `envconfig:"RATE_LIMIT_RPS" default:"10.0"`
	Port         string  `envconfig:"PORT" default:"8080"`

	MaxOrderQtyBTC float64 `envconfig:"MAX_ORDER_QTY_BTC" default:"0.1"`
	MaxOrderQtyETH float64 `envconfig:"MAX_ORDER_QTY_ETH" default:"2.0"`
	MaxDailyOrders int     `envconfig:"MAX_DAILY_ORDERS" default:"500"`
	// Per-side caps enforced on top of MAX_DAILY_ORDERS. Zero means no per-side cap.
	MaxDailyBuys  int `envconfig:"MAX_DAILY_BUYS" default:"0"`
	MaxDailySells int `envconfig:"MAX_DAILY_SELLS" default:"0"`

	SimFillPrice float64 `envconfig:"SIM_FILL_PRICE" default:"50000"`
	SimTxCostBps float64 `envconfig:"SIM_TX_COST_BPS" default:"0"`

	// Execution algorithm: "" (direct), "TWAP", or "VWAP".
	AlgoType     string `envconfig:"ALGO_TYPE" default:""`
	AlgoSlices   int    `envconfig:"ALGO_SLICES" default:"5"`
	AlgoDuration string `envconfig:"ALGO_DURATION" default:"5m"`

	// Phase 9 — Muninn SSE feature stream (ADR-0009). Disabled by default: the
	// transport is opt-in until proven in paper. When enabled, sleipnir tails
	// muninn's /api/v1/features/stream and surfaces the latest value per
	// feature at /feature/latest (read-only operator visibility; the feed does
	// not drive trading).
	FeatureStreamEnabled bool   `envconfig:"FEATURE_STREAM_ENABLED" default:"false"`
	MuninnStreamURL      string `envconfig:"MUNINN_STREAM_URL" default:"http://localhost:8080"`
	MuninnStreamFeature  string `envconfig:"MUNINN_STREAM_FEATURE" default:""`
}

// LoadConfig reads configuration from the environment and validates required fields.
func LoadConfig() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to process environment config: %w", err)
	}

	// Validate required exchange parameters — but only when the live Binance
	// backend is selected. EXCHANGE_BACKEND=sim runs without any credentials
	// (useful for local e2e tests and demos).
	if os.Getenv("EXCHANGE_BACKEND") != "sim" {
		if cfg.BinanceAPIKey == "" {
			return nil, fmt.Errorf("missing BINANCE_API_KEY environment variable")
		}
		if cfg.BinanceAPISecret == "" {
			return nil, fmt.Errorf("missing BINANCE_API_SECRET environment variable")
		}
	}

	return &cfg, nil
}
