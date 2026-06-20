package algo

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"sleipnir/internal/exchange"
)

func TestTWAP_SplitsEvenly(t *testing.T) {
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice:          100.0,
		TransactionCostBps: 10,
	}, slog.Default())

	exec := NewExecutor(sim, slog.Default())

	parent := exchange.Order{
		OrderID:    "test-twap-1",
		Instrument: "BTC-USDT",
		Side:       exchange.SideBuy,
		Quantity:   1.0,
		Type:       exchange.TypeMarket,
	}

	result, err := exec.Execute(context.Background(), parent, Config{
		Algorithm: AlgoTWAP,
		Duration:  100 * time.Millisecond,
		Slices:    5,
	})
	if err != nil {
		t.Fatalf("TWAP failed: %v", err)
	}

	if result.SlicesExec != 5 {
		t.Errorf("expected 5 slices executed, got %d", result.SlicesExec)
	}
	if result.SlicesFailed != 0 {
		t.Errorf("expected 0 failed slices, got %d", result.SlicesFailed)
	}

	// Total quantity should be ~1.0 (5 * 0.2)
	if result.TotalQty < 0.99 || result.TotalQty > 1.01 {
		t.Errorf("expected total qty ~1.0, got %f", result.TotalQty)
	}

	if result.AvgPrice != 100.0 {
		t.Errorf("expected avg price 100.0, got %f", result.AvgPrice)
	}
}

func TestVWAP_UsesProfile(t *testing.T) {
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice:          50.0,
		TransactionCostBps: 10,
	}, slog.Default())

	exec := NewExecutor(sim, slog.Default())

	parent := exchange.Order{
		OrderID:    "test-vwap-1",
		Instrument: "ETH-USDT",
		Side:       exchange.SideSell,
		Quantity:   10.0,
		Type:       exchange.TypeMarket,
	}

	// Custom weights: 3-1-1-1-3 (heavier at start/end)
	result, err := exec.Execute(context.Background(), parent, Config{
		Algorithm:   AlgoVWAP,
		Duration:    100 * time.Millisecond,
		Slices:      5,
		VWAPWeights: []float64{3, 1, 1, 1, 3},
	})
	if err != nil {
		t.Fatalf("VWAP failed: %v", err)
	}

	if result.SlicesExec != 5 {
		t.Errorf("expected 5 slices, got %d", result.SlicesExec)
	}

	// First and last slices should be 3/9 of total = 3.33 each
	firstQty := result.Fills[0].Quantity
	midQty := result.Fills[2].Quantity
	if firstQty <= midQty {
		t.Errorf("VWAP first slice (%.4f) should be larger than mid slice (%.4f)", firstQty, midQty)
	}
}

func TestTWAP_ContextCancel(t *testing.T) {
	sim := exchange.NewSimulatorConnector(exchange.SimulatorConfig{
		FillPrice: 100.0,
	}, slog.Default())

	exec := NewExecutor(sim, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	parent := exchange.Order{
		OrderID:    "test-cancel-1",
		Instrument: "BTC-USDT",
		Side:       exchange.SideBuy,
		Quantity:   1.0,
		Type:       exchange.TypeMarket,
	}

	result, err := exec.Execute(ctx, parent, Config{
		Algorithm: AlgoTWAP,
		Duration:  10 * time.Second,
		Slices:    100,
	})

	// Should have partial fills due to context cancellation
	if err == nil && result.SlicesExec == 100 {
		t.Error("expected partial execution due to context cancel")
	}
	if result.SlicesExec == 0 {
		t.Error("expected at least one slice before cancel")
	}
}

func TestDefaultVWAPProfile(t *testing.T) {
	profile := defaultVWAPProfile(10)
	if len(profile) != 10 {
		t.Fatalf("expected 10 weights, got %d", len(profile))
	}

	// U-shape: first and last should be largest
	mid := profile[5]
	if profile[0] <= mid {
		t.Errorf("first weight (%.4f) should be > mid (%.4f)", profile[0], mid)
	}
	if profile[9] <= mid {
		t.Errorf("last weight (%.4f) should be > mid (%.4f)", profile[9], mid)
	}
}
