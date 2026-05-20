package exchange

import (
	"context"
	"testing"
	"time"
)

func TestSimulator_SubmitFillsSynchronously(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	sim := NewSimulatorConnector(SimulatorConfig{
		FillPrice: 50_000,
		Now:       func() time.Time { return fixed },
	}, nil)

	fill, err := sim.SubmitOrder(context.Background(), Order{
		OrderID: "ord-1", Instrument: "BTC-USD", Side: SideBuy, Quantity: 0.01, Type: TypeMarket,
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	if fill.ExecutionID != "ord-1-sim-rest-1" {
		t.Errorf("expected ExecutionID=ord-1-sim-rest-1, got %q", fill.ExecutionID)
	}
	if fill.FillPrice != 50_000 {
		t.Errorf("expected price=50000, got %v", fill.FillPrice)
	}
	if !fill.Timestamp.Equal(fixed) {
		t.Errorf("expected fixed timestamp, got %v", fill.Timestamp)
	}
	if fill.Quantity != 0.01 {
		t.Errorf("expected qty=0.01, got %v", fill.Quantity)
	}
}

func TestSimulator_GetOrderState(t *testing.T) {
	t.Parallel()
	sim := NewSimulatorConnector(SimulatorConfig{}, nil)
	if r, _ := sim.GetOrderState(context.Background(), "missing", "BTC-USD"); r.State != StatePending {
		t.Errorf("expected StatePending for unseen order, got %s", r.State)
	}
	_, _ = sim.SubmitOrder(context.Background(), Order{
		OrderID: "ord-x", Instrument: "BTC-USD", Side: SideBuy, Quantity: 0.01, Price: 100,
	})
	if r, _ := sim.GetOrderState(context.Background(), "ord-x", "BTC-USD"); r.State != StateFilled {
		t.Errorf("expected StateFilled after Submit, got %s", r.State)
	}
	if err := sim.CancelOrder(context.Background(), "ord-x", "BTC-USD"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
}

func TestSimulator_ExecutionIDsAreMonotonic(t *testing.T) {
	t.Parallel()
	sim := NewSimulatorConnector(SimulatorConfig{FillPrice: 1}, nil)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		fill, err := sim.SubmitOrder(context.Background(), Order{
			OrderID: "ord", Instrument: "BTC-USD", Side: SideBuy, Quantity: 1, Price: 1,
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if seen[fill.ExecutionID] {
			t.Fatalf("duplicate ExecutionID %q at iteration %d", fill.ExecutionID, i)
		}
		seen[fill.ExecutionID] = true
	}
}

func TestSimulator_AlsoEmitOnWSPushesToSubscriber(t *testing.T) {
	t.Parallel()
	sim := NewSimulatorConnector(SimulatorConfig{
		FillPrice:    100,
		AlsoEmitOnWS: true,
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := make(chan ExecutionFill, 4)
	if err := sim.StartUserStream(ctx, ch); err != nil {
		t.Fatalf("StartUserStream: %v", err)
	}

	_, _ = sim.SubmitOrder(ctx, Order{OrderID: "ord", Instrument: "BTC-USD", Side: SideBuy, Quantity: 1, Price: 100})

	select {
	case got := <-ch:
		if got.OrderID != "ord" {
			t.Errorf("WS fill OrderID = %q, want ord", got.OrderID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected fill on WS channel within 200ms")
	}
}
