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

	// Verify update — `orders` slice itself isn't asserted on this round,
	// only the maps. Use _ to keep the linter happy without dropping the call.
	_, states, filledQtys, err = store.GetActiveOrders(ctx)
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

func TestSQLiteOrderStoreMigrations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_migrations.db")

	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer store.Close()

	// 1. Verify schema_migrations table contains the 3 migrations
	var count int
	err = store.db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query schema_migrations: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 applied migrations, got %d", count)
	}

	// 2. Verify that orders table indeed contains commission and slippage columns
	_, err = store.db.Exec("INSERT INTO orders (order_id, instrument, side, quantity, price, type, state, filled_qty, commission, slippage, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"mig-test-1", "BTC-USD", "BUY", 0.05, 50000.0, "LIMIT", "SUBMITTED", 0.0, 0.001, 0.002, time.Now(), time.Now())
	if err != nil {
		t.Errorf("failed to insert order with commission and slippage (migration test failed): %v", err)
	}

	var commission, slippage float64
	err = store.db.QueryRow("SELECT commission, slippage FROM orders WHERE order_id = ?", "mig-test-1").Scan(&commission, &slippage)
	if err != nil {
		t.Fatalf("failed to select commission and slippage: %v", err)
	}

	if commission != 0.001 || slippage != 0.002 {
		t.Errorf("expected commission 0.001 and slippage 0.002, got commission %f and slippage %f", commission, slippage)
	}
}

func TestGatewayPreTradeRiskLimits(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_risk_limits.db")

	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	tracker := NewOrderTracker().WithStore(store)

	// Create a gateway with:
	// MaxOrderQtyBTC = 0.1
	// MaxOrderQtyETH = 2.0
	// MaxDailyOrders = 2
	gw := NewGateway(nil, nil, nil, tracker, nil, NewLegacyRiskPolicy(0.1, 2.0), NewHalt(), 2, nil)

	ctx := context.Background()

	// Case 1: Valid BTC order (qty <= 0.1)
	oValidBTC := exchange.Order{
		OrderID:    "btc-valid",
		Instrument: "BTC-USD",
		Quantity:   0.05,
	}
	allowed, reason := gw.checkRiskLimits(ctx, oValidBTC)
	if !allowed {
		t.Errorf("expected valid BTC order to pass risk limits, got rejected with reason %s", reason)
	}

	// Case 2: Invalid BTC order (qty > 0.1)
	oInvalidBTC := exchange.Order{
		OrderID:    "btc-invalid",
		Instrument: "BTC-USD",
		Quantity:   0.15,
	}
	allowed, reason = gw.checkRiskLimits(ctx, oInvalidBTC)
	if allowed {
		t.Error("expected invalid BTC order to be rejected, but it passed risk limits")
	}
	if reason != "qty_limit_exceeded" {
		t.Errorf("expected rejection reason 'qty_limit_exceeded', got %s", reason)
	}

	// Case 3: Valid ETH order (qty <= 2.0)
	oValidETH := exchange.Order{
		OrderID:    "eth-valid",
		Instrument: "ETH-USD",
		Quantity:   1.9,
	}
	allowed, reason = gw.checkRiskLimits(ctx, oValidETH)
	if !allowed {
		t.Errorf("expected valid ETH order to pass risk limits, got rejected with reason %s", reason)
	}

	// Case 4: Invalid ETH order (qty > 2.0)
	oInvalidETH := exchange.Order{
		OrderID:    "eth-invalid",
		Instrument: "ETH-USD",
		Quantity:   2.1,
	}
	allowed, reason = gw.checkRiskLimits(ctx, oInvalidETH)
	if allowed {
		t.Error("expected invalid ETH order to be rejected, but it passed risk limits")
	}
	if reason != "qty_limit_exceeded" {
		t.Errorf("expected rejection reason 'qty_limit_exceeded', got %s", reason)
	}

	// Case 5: Daily frequency limit checks.
	o1 := exchange.Order{
		OrderID:    "o1",
		Instrument: "BTC-USD",
		Quantity:   0.01,
	}
	o2 := exchange.Order{
		OrderID:    "o2",
		Instrument: "BTC-USD",
		Quantity:   0.01,
	}
	o3 := exchange.Order{
		OrderID:    "o3",
		Instrument: "BTC-USD",
		Quantity:   0.01,
	}

	err = store.SaveOrder(ctx, o1, exchange.StateSubmitted)
	if err != nil {
		t.Fatalf("failed to save o1: %v", err)
	}
	err = store.SaveOrder(ctx, o2, exchange.StateSubmitted)
	if err != nil {
		t.Fatalf("failed to save o2: %v", err)
	}

	count, err := store.GetDailyOrderCount(ctx)
	if err != nil {
		t.Fatalf("failed to get daily count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected daily order count 2, got %d", count)
	}

	allowed, reason = gw.checkRiskLimits(ctx, o3)
	if allowed {
		t.Error("expected third order to be rejected due to frequency caps, but it passed")
	}
	if reason != "daily_count_exceeded" {
		t.Errorf("expected rejection reason 'daily_count_exceeded', got %s", reason)
	}
}
