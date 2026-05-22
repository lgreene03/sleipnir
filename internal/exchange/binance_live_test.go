//go:build binance_live

package exchange

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestBinanceLive_SubmitAndCancelLimitOrder submits a tiny GTC LIMIT BUY at an
// impossible price against Binance Spot Testnet, asserts the order is accepted
// (StateSubmitted), then cancels it. Runs only with the "binance_live" build tag.
//
// Required env vars (skipped silently when absent):
//
//	BINANCE_API_KEY    — testnet API key
//	BINANCE_API_SECRET — testnet API secret
//
// The Binance nightly CI job injects these from GitHub secrets; local devs can
// export them from their testnet account at https://testnet.binance.vision.
func TestBinanceLive_SubmitAndCancelLimitOrder(t *testing.T) {
	apiKey := os.Getenv("BINANCE_API_KEY")
	apiSecret := os.Getenv("BINANCE_API_SECRET")
	if apiKey == "" || apiSecret == "" {
		t.Skip("BINANCE_API_KEY / BINANCE_API_SECRET not set — skipping live test")
	}

	const (
		restURL = "https://testnet.binance.vision"
		wsURL   = "wss://ws-api.testnet.binance.vision/ws-api/v3"
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn := NewBinanceConnector(apiKey, apiSecret, restURL, wsURL, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique order ID per run; avoid collision with concurrent test runs.
	orderID := fmt.Sprintf("sleipnir-live-test-%d", time.Now().UnixMilli())

	order := Order{
		OrderID:    orderID,
		Instrument: "BTC-USD",
		Side:       SideBuy,
		Type:       TypeLimit,
		Quantity:   0.001,
		// Impossibly low price — GTC LIMIT will be accepted but never matched.
		Price: 1.00,
	}

	// Phase 1: submit.
	fill, err := conn.SubmitOrder(ctx, order)
	if err != nil {
		t.Fatalf("SubmitOrder failed: %v", err)
	}
	if fill.OrderID != orderID {
		t.Errorf("fill.OrderID = %q, want %q", fill.OrderID, orderID)
	}
	if fill.OrderStatus != StateSubmitted {
		t.Errorf("fill.OrderStatus = %q after LIMIT submit, want %q", fill.OrderStatus, StateSubmitted)
	}

	// Phase 2: poll GetOrderState — exchange confirms the order is live.
	result, err := conn.GetOrderState(ctx, orderID, order.Instrument)
	if err != nil {
		t.Fatalf("GetOrderState failed: %v", err)
	}
	if result.State != StateSubmitted {
		t.Errorf("GetOrderState.State = %q, want %q", result.State, StateSubmitted)
	}

	// Phase 3: cancel to avoid leaving orphaned orders on the testnet account.
	if err := conn.CancelOrder(ctx, orderID, order.Instrument); err != nil {
		t.Fatalf("CancelOrder failed: %v", err)
	}

	// Phase 4: verify the cancel took effect.
	result, err = conn.GetOrderState(ctx, orderID, order.Instrument)
	if err != nil {
		t.Fatalf("GetOrderState after cancel failed: %v", err)
	}
	if result.State != StateCanceled {
		t.Errorf("GetOrderState.State = %q after cancel, want %q", result.State, StateCanceled)
	}
}
