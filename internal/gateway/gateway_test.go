package gateway

import (
	"context"
	"path/filepath"
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

func TestSQLiteOrderStorePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_sleipnir.db")

	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer store.Close()

	order := exchange.Order{
		OrderID:    "test-123",
		Instrument: "BTC-USD",
		Side:       exchange.SideBuy,
		Quantity:   0.5,
		Price:      60000.0,
		Type:       exchange.TypeLimit,
	}

	ctx := context.Background()

	// 1. Save
	err = store.SaveOrder(ctx, order, exchange.StateSubmitted)
	if err != nil {
		t.Fatalf("failed to save order: %v", err)
	}

	// 2. Fetch Active
	orders, states, filledQtys, err := store.GetActiveOrders(ctx)
	if err != nil {
		t.Fatalf("failed to get active orders: %v", err)
	}

	if len(orders) != 1 {
		t.Fatalf("expected 1 active order, got %d", len(orders))
	}
	if orders[0].OrderID != "test-123" {
		t.Errorf("expected ID 'test-123', got %s", orders[0].OrderID)
	}
	if states["test-123"] != exchange.StateSubmitted {
		t.Errorf("expected state 'SUBMITTED', got %s", states["test-123"])
	}
	if filledQtys["test-123"] != 0.0 {
		t.Errorf("expected filled qty 0.0, got %f", filledQtys["test-123"])
	}

	// 3. Update
	err = store.UpdateOrderState(ctx, "test-123", exchange.StatePartiallyFilled, 0.2)
	if err != nil {
		t.Fatalf("failed to update order state: %v", err)
	}

	// Verify update
	orders, states, filledQtys, err = store.GetActiveOrders(ctx)
	if err != nil {
		t.Fatalf("failed to get active orders: %v", err)
	}
	if states["test-123"] != exchange.StatePartiallyFilled {
		t.Errorf("expected state 'PARTIALLY_FILLED', got %s", states["test-123"])
	}
	if filledQtys["test-123"] != 0.2 {
		t.Errorf("expected filled qty 0.2, got %f", filledQtys["test-123"])
	}

	// 4. Update to Terminal State (should not be in active orders)
	err = store.UpdateOrderState(ctx, "test-123", exchange.StateFilled, 0.5)
	if err != nil {
		t.Fatalf("failed to update to terminal state: %v", err)
	}

	orders, _, _, err = store.GetActiveOrders(ctx)
	if err != nil {
		t.Fatalf("failed to get active orders: %v", err)
	}
	if len(orders) != 0 {
		t.Errorf("expected 0 active orders after setting terminal state, got %d", len(orders))
	}
}
