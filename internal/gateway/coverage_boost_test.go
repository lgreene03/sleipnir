package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sleipnir/internal/exchange"
)

// --- Halt tests ---

func TestHaltSetAndClear(t *testing.T) {
	t.Parallel()
	h := NewHalt()

	if h.IsHalted() {
		t.Fatal("new Halt should not be halted")
	}
	if h.Reason() != "" {
		t.Fatalf("reason should be empty, got %q", h.Reason())
	}

	h.Set("emergency")
	if !h.IsHalted() {
		t.Fatal("expected halted after Set")
	}
	if h.Reason() != "emergency" {
		t.Fatalf("reason = %q, want emergency", h.Reason())
	}

	h.Clear()
	if h.IsHalted() {
		t.Fatal("expected not halted after Clear")
	}
	if h.Reason() != "" {
		t.Fatalf("reason should be empty after Clear, got %q", h.Reason())
	}
}

func TestHaltSetEmptyReasonDefaults(t *testing.T) {
	t.Parallel()
	h := NewHalt()
	h.Set("")
	if h.Reason() != "operator_halt" {
		t.Fatalf("empty reason should default to operator_halt, got %q", h.Reason())
	}
}

// --- Gateway accessor tests ---

func TestGatewayHaltAccessor(t *testing.T) {
	t.Parallel()
	halt := NewHalt()
	gw := NewGateway(nil, nil, nil, NewOrderTracker(), nil, nil, halt, 0, nil)
	if gw.Halt() != halt {
		t.Fatal("Halt() should return the halt passed to NewGateway")
	}
}

func TestGatewayIsReady(t *testing.T) {
	t.Parallel()
	gw := NewGateway(nil, nil, nil, NewOrderTracker(), nil, nil, nil, 0, nil)
	if gw.IsReady() {
		t.Fatal("new gateway should not be ready")
	}
	gw.ready.Store(true)
	if !gw.IsReady() {
		t.Fatal("expected ready after storing true")
	}
}

// --- LoadRiskPolicy tests ---

func TestLoadRiskPolicyFromFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "risk.yaml")

	yaml := `default_max_qty: 0.5
default_max_notional: 10000.0
instruments:
  btc-usd:
    max_qty: 0.1
    max_notional: 50000.0
    min_qty: 0.001
  ETH-USD:
    max_qty: 2.0
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	p, err := LoadRiskPolicy(path)
	if err != nil {
		t.Fatalf("LoadRiskPolicy: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil policy")
	}
	if p.DefaultMaxQty != 0.5 {
		t.Errorf("DefaultMaxQty = %f, want 0.5", p.DefaultMaxQty)
	}
	if p.DefaultMaxNotional != 10000.0 {
		t.Errorf("DefaultMaxNotional = %f, want 10000.0", p.DefaultMaxNotional)
	}
	// Keys should be normalized to uppercase.
	if _, ok := p.Instruments["BTC-USD"]; !ok {
		t.Error("expected BTC-USD key (uppercased), not found")
	}
	if _, ok := p.Instruments["ETH-USD"]; !ok {
		t.Error("expected ETH-USD key, not found")
	}
	btc := p.Instruments["BTC-USD"]
	if btc.MaxQty != 0.1 {
		t.Errorf("BTC MaxQty = %f, want 0.1", btc.MaxQty)
	}
	if btc.MinQty != 0.001 {
		t.Errorf("BTC MinQty = %f, want 0.001", btc.MinQty)
	}
}

func TestLoadRiskPolicyEmptyPath(t *testing.T) {
	t.Parallel()
	p, err := LoadRiskPolicy("")
	if err != nil {
		t.Fatalf("LoadRiskPolicy empty path: %v", err)
	}
	if p != nil {
		t.Fatal("expected nil policy for empty path")
	}
}

func TestLoadRiskPolicyBadFile(t *testing.T) {
	t.Parallel()
	_, err := LoadRiskPolicy("/nonexistent/risk.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoadRiskPolicyBadYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{{not yaml"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadRiskPolicy(path)
	if err == nil {
		t.Fatal("expected error for invalid yaml")
	}
}

// --- CheckIntent edge cases ---

func TestCheckIntentNilPolicy(t *testing.T) {
	t.Parallel()
	var p *RiskPolicy
	ok, reason := p.CheckIntent(exchange.Order{Instrument: "BTC-USD", Quantity: 100})
	if !ok {
		t.Fatalf("nil policy should allow all, got rejection: %s", reason)
	}
}

func TestCheckIntentMinQtyRejection(t *testing.T) {
	t.Parallel()
	p := &RiskPolicy{
		Instruments: map[string]InstrumentLimits{
			"BTC-USD": {MaxQty: 1.0, MinQty: 0.01},
		},
	}
	ok, reason := p.CheckIntent(exchange.Order{Instrument: "BTC-USD", Quantity: 0.001})
	if ok {
		t.Fatal("expected rejection for qty below minimum")
	}
	if reason != "qty_below_minimum" {
		t.Errorf("reason = %q, want qty_below_minimum", reason)
	}
}

func TestCheckIntentNotionalRejection(t *testing.T) {
	t.Parallel()
	p := &RiskPolicy{
		Instruments: map[string]InstrumentLimits{
			"BTC-USD": {MaxQty: 10, MaxNotional: 100000.0},
		},
	}
	// 2.0 * 60000 = 120000 > 100000
	ok, reason := p.CheckIntent(exchange.Order{Instrument: "BTC-USD", Quantity: 2.0, Price: 60000.0})
	if ok {
		t.Fatal("expected rejection for notional limit")
	}
	if reason != "notional_limit_exceeded" {
		t.Errorf("reason = %q, want notional_limit_exceeded", reason)
	}
}

func TestCheckIntentUnknownInstrumentWithDefaults(t *testing.T) {
	t.Parallel()
	p := &RiskPolicy{
		DefaultMaxQty:      0.5,
		DefaultMaxNotional: 10000.0,
		Instruments:        map[string]InstrumentLimits{},
	}
	// Unknown instrument, qty within default
	ok, _ := p.CheckIntent(exchange.Order{Instrument: "SOL-USD", Quantity: 0.1})
	if !ok {
		t.Fatal("expected unknown instrument within default to pass")
	}
	// Unknown instrument, qty exceeds default
	ok, reason := p.CheckIntent(exchange.Order{Instrument: "SOL-USD", Quantity: 1.0})
	if ok {
		t.Fatal("expected unknown instrument exceeding default to be rejected")
	}
	if reason != "qty_limit_exceeded" {
		t.Errorf("reason = %q, want qty_limit_exceeded", reason)
	}
}

// --- lookupFilledQtyLocked via UpdateOrderState with store ---

func TestUpdateOrderStateWithStorePreservesFilledQty(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "lookup.db")
	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	tracker := NewOrderTracker().WithStore(store)

	order := exchange.Order{
		OrderID:    "lookup-1",
		Instrument: "BTC-USD",
		Side:       exchange.SideBuy,
		Quantity:   1.0,
		Price:      50000.0,
		Type:       exchange.TypeLimit,
	}
	tracker.AddOrder(ctx, order, exchange.StateSubmitted)

	// Simulate a partial fill via the store (as the WS path would).
	tracker.UpdateOrderStateAndQty(ctx, order.OrderID, exchange.StatePartiallyFilled, 0.3)

	// Now do a state-only transition (e.g. cancel). This exercises
	// lookupFilledQtyLocked to preserve the 0.3 filled_qty.
	changed := tracker.UpdateOrderState(ctx, order.OrderID, exchange.StateCanceled)
	if !changed {
		t.Fatal("expected state change")
	}

	// Verify the filled_qty was preserved in the DB.
	var gotQty float64
	row := store.db.QueryRowContext(ctx, `SELECT filled_qty FROM orders WHERE order_id = ?`, order.OrderID)
	if err := row.Scan(&gotQty); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gotQty != 0.3 {
		t.Errorf("filled_qty = %f, want 0.3 (lookupFilledQtyLocked should preserve it)", gotQty)
	}
}

// --- TokenBucketLimiter context cancellation ---

func TestTokenBucketLimiterContextCancel(t *testing.T) {
	t.Parallel()
	limiter := NewTokenBucketLimiter(1.0) // capacity must be >= 1.0 for Wait to succeed

	// Drain the bucket.
	ctx := context.Background()
	if err := limiter.Wait(ctx); err != nil {
		t.Fatalf("initial wait: %v", err)
	}

	// Now cancel immediately — should return context error.
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := limiter.Wait(cancelCtx)
	if err == nil {
		t.Fatal("expected context cancel error from limiter.Wait")
	}
}

// --- checkRiskLimits with DB error paths ---

func TestCheckRiskLimitsDBUnreachable(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "risk_db.db")
	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tracker := NewOrderTracker().WithStore(store)
	gw := NewGateway(nil, nil, nil, tracker, nil, NewLegacyRiskPolicy(10.0, 100.0), NewHalt(), 100, nil)

	ctx := context.Background()
	intent := exchange.Order{OrderID: "db-err-1", Instrument: "BTC-USD", Quantity: 0.01}

	// Verify it works first.
	ok, _ := gw.checkRiskLimits(ctx, intent)
	if !ok {
		t.Fatal("expected pass before DB close")
	}

	// Close the DB to simulate unreachable store.
	store.Close()

	ok, reason := gw.checkRiskLimits(ctx, intent)
	if ok {
		t.Fatal("expected rejection when DB is unreachable")
	}
	if reason != "db_unreachable" {
		t.Errorf("reason = %q, want db_unreachable", reason)
	}
}

// --- checkRiskLimits per-side with DB error on side query ---

func TestCheckRiskLimitsBuySideDBError(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "side_err.db")
	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tracker := NewOrderTracker().WithStore(store)
	gw := NewGateway(nil, nil, nil, tracker, nil, NewLegacyRiskPolicy(10.0, 100.0), NewHalt(), 100, nil).
		WithDailySideLimits(5, 5)

	ctx := context.Background()
	buyIntent := exchange.Order{OrderID: "buy-side-err", Instrument: "BTC-USD", Side: exchange.SideBuy, Quantity: 0.01}

	// Verify it works first.
	ok, _ := gw.checkRiskLimits(ctx, buyIntent)
	if !ok {
		t.Fatal("expected pass before DB close")
	}

	// Close DB to trigger error path on per-side check.
	store.Close()
	ok, reason := gw.checkRiskLimits(ctx, buyIntent)
	if ok {
		t.Fatal("expected rejection on DB error for buy side check")
	}
	if reason != "db_unreachable" {
		t.Errorf("reason = %q, want db_unreachable", reason)
	}
}

func TestCheckRiskLimitsSellSideDBError(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "sell_err.db")
	store, err := NewSQLiteOrderStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tracker := NewOrderTracker().WithStore(store)
	gw := NewGateway(nil, nil, nil, tracker, nil, NewLegacyRiskPolicy(10.0, 100.0), NewHalt(), 100, nil).
		WithDailySideLimits(5, 5)

	ctx := context.Background()
	sellIntent := exchange.Order{OrderID: "sell-side-err", Instrument: "BTC-USD", Side: exchange.SideSell, Quantity: 0.01}

	// Verify it works first.
	ok, _ := gw.checkRiskLimits(ctx, sellIntent)
	if !ok {
		t.Fatal("expected pass before DB close")
	}

	// Close DB to trigger error path on per-side check.
	store.Close()
	ok, reason := gw.checkRiskLimits(ctx, sellIntent)
	if ok {
		t.Fatal("expected rejection on DB error for sell side check")
	}
	if reason != "db_unreachable" {
		t.Errorf("reason = %q, want db_unreachable", reason)
	}
}
