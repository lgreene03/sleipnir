package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"sleipnir/internal/exchange"
)

func TestOrderTrackerAddAndUpdate(t *testing.T) {
	tracker := NewOrderTracker()

	order := exchange.Order{
		OrderID:    "ord-101",
		Instrument: "BTC-USD",
		Side:       exchange.SideBuy,
		Quantity:   1.5,
		Type:       exchange.TypeMarket,
	}

	// 1. Test registration
	tracker.AddOrder(order, exchange.StatePending)

	retrievedOrder, exists := tracker.GetOrder("ord-101")
	if !exists {
		t.Fatal("Order expected to exist in tracker but not found")
	}
	if retrievedOrder.Quantity != 1.5 {
		t.Errorf("Expected quantity 1.5, got %v", retrievedOrder.Quantity)
	}

	state, exists := tracker.GetOrderState("ord-101")
	if !exists {
		t.Fatal("Order state expected to exist but not found")
	}
	if state != exchange.StatePending {
		t.Errorf("Expected state to be StatePending, got %v", state)
	}

	// 2. Test status mutations
	changed := tracker.UpdateOrderState("ord-101", exchange.StateSubmitted)
	if !changed {
		t.Error("Expected UpdateOrderState to indicate state change (true), got false")
	}

	state, _ = tracker.GetOrderState("ord-101")
	if state != exchange.StateSubmitted {
		t.Errorf("Expected state to be StateSubmitted, got %v", state)
	}

	// Re-updating with same state should return false
	changed = tracker.UpdateOrderState("ord-101", exchange.StateSubmitted)
	if changed {
		t.Error("Expected UpdateOrderState to indicate no change (false), got true")
	}
}

func TestOrderTrackerActiveFilters(t *testing.T) {
	tracker := NewOrderTracker()

	o1 := exchange.Order{OrderID: "1", Instrument: "BTC-USD"}
	o2 := exchange.Order{OrderID: "2", Instrument: "ETH-USD"}
	o3 := exchange.Order{OrderID: "3", Instrument: "SOL-USD"}

	tracker.AddOrder(o1, exchange.StateSubmitted)
	tracker.AddOrder(o2, exchange.StateFilled)
	tracker.AddOrder(o3, exchange.StateCanceled)

	actives := tracker.GetAllActiveOrders()
	if len(actives) != 1 {
		t.Fatalf("Expected 1 active order, got %d", len(actives))
	}
	if actives[0].OrderID != "1" {
		t.Errorf("Expected active order to be ID '1', got %s", actives[0].OrderID)
	}
}

func TestTokenBucketLimiterPacing(t *testing.T) {
	// A bucket that refills at 5 tokens per second. Capacity is 2.
	limiter := NewTokenBucketLimiter(5.0)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Consume capacity immediately (5 tokens)
	for i := 0; i < 5; i++ {
		err := limiter.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait failed at iteration %d: %v", i, err)
		}
	}

	// The 6th request must trigger pacing since bucket is empty
	startTime := time.Now()
	err := limiter.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	elapsed := time.Since(startTime)

	// Since refill rate is 5/sec (1 token every 200ms), the delay must be at least 150ms
	if elapsed < 150*time.Millisecond {
		t.Errorf("Rate limiter didn't slow request down enough. Elapsed: %v", elapsed)
	}
}

func TestOrderTrackerConcurrency(t *testing.T) {
	tracker := NewOrderTracker()
	var wg sync.WaitGroup

	// Concurrently write and update multiple states to assert safety under load
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			orderID := string(rune(idx))
			order := exchange.Order{OrderID: orderID, Instrument: "BTC-USD"}
			tracker.AddOrder(order, exchange.StatePending)
			tracker.UpdateOrderState(orderID, exchange.StateSubmitted)
			_, _ = tracker.GetOrder(orderID)
			_ = tracker.GetAllActiveOrders()
		}(i)
	}

	wg.Wait()
}
