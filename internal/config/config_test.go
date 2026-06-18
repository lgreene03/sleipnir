package config

import (
	"os"
	"testing"
)

func TestLoadConfigSimMode(t *testing.T) {
	t.Setenv("EXCHANGE_BACKEND", "sim")
	t.Setenv("BINANCE_API_KEY", "")
	t.Setenv("BINANCE_API_SECRET", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() in sim mode error: %v", err)
	}

	if cfg.Port != "8080" {
		t.Errorf("default Port = %q, want %q", cfg.Port, "8080")
	}
	if cfg.KafkaIntentsTopic != "executions.intents.v1" {
		t.Errorf("default IntentsTopic = %q, want %q", cfg.KafkaIntentsTopic, "executions.intents.v1")
	}
	if cfg.KafkaFillsTopic != "executions.fills.v1" {
		t.Errorf("default FillsTopic = %q, want %q", cfg.KafkaFillsTopic, "executions.fills.v1")
	}
	if cfg.KafkaConsumerGroup != "sleipnir-gateway" {
		t.Errorf("default ConsumerGroup = %q, want %q", cfg.KafkaConsumerGroup, "sleipnir-gateway")
	}
	if cfg.RateLimitRPS != 10.0 {
		t.Errorf("default RateLimitRPS = %v, want 10.0", cfg.RateLimitRPS)
	}
	if cfg.MaxOrderQtyBTC != 0.1 {
		t.Errorf("default MaxOrderQtyBTC = %v, want 0.1", cfg.MaxOrderQtyBTC)
	}
	if cfg.MaxDailyOrders != 500 {
		t.Errorf("default MaxDailyOrders = %d, want 500", cfg.MaxDailyOrders)
	}
}

func TestLoadConfigRequiresBinanceKeys(t *testing.T) {
	t.Setenv("EXCHANGE_BACKEND", "binance")
	t.Setenv("BINANCE_API_KEY", "")
	t.Setenv("BINANCE_API_SECRET", "")

	_, err := LoadConfig()
	if err == nil {
		t.Error("expected error when BINANCE_API_KEY is empty in binance mode, got nil")
	}
}

func TestLoadConfigAcceptsBinanceKeys(t *testing.T) {
	t.Setenv("EXCHANGE_BACKEND", "binance")
	t.Setenv("BINANCE_API_KEY", "test-key")
	t.Setenv("BINANCE_API_SECRET", "test-secret")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with valid keys error: %v", err)
	}
	if cfg.BinanceAPIKey != "test-key" {
		t.Errorf("BinanceAPIKey = %q, want %q", cfg.BinanceAPIKey, "test-key")
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("EXCHANGE_BACKEND", "sim")
	t.Setenv("PORT", "9090")
	t.Setenv("RATE_LIMIT_RPS", "5.0")
	t.Setenv("MAX_DAILY_ORDERS", "100")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}

	if cfg.Port != "9090" {
		t.Errorf("Port = %q, want %q", cfg.Port, "9090")
	}
	if cfg.RateLimitRPS != 5.0 {
		t.Errorf("RateLimitRPS = %v, want 5.0", cfg.RateLimitRPS)
	}
	if cfg.MaxDailyOrders != 100 {
		t.Errorf("MaxDailyOrders = %d, want 100", cfg.MaxDailyOrders)
	}
}

func TestLoadConfigMissingSecretOnly(t *testing.T) {
	// Clear EXCHANGE_BACKEND to force binance mode (default)
	os.Unsetenv("EXCHANGE_BACKEND")
	t.Setenv("BINANCE_API_KEY", "test-key")
	t.Setenv("BINANCE_API_SECRET", "")

	_, err := LoadConfig()
	if err == nil {
		t.Error("expected error when BINANCE_API_SECRET is empty, got nil")
	}
}
