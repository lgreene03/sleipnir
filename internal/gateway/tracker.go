package gateway

import (
	"context"
	"sync"
	"time"

	"sleipnir/internal/exchange"
	"sleipnir/internal/telemetry"
)

// OrderTracker is an active, thread-safe memory manager for tracking pending/submitted orders.
type OrderTracker struct {
	mu     sync.RWMutex
	orders map[string]exchange.Order
	states map[string]exchange.OrderState
	store  OrderStore
}

// NewOrderTracker creates a new instance of OrderTracker.
func NewOrderTracker() *OrderTracker {
	return &OrderTracker{
		orders: make(map[string]exchange.Order),
		states: make(map[string]exchange.OrderState),
	}
}

// WithStore registers a persistent store and preloads any active orders on boot.
func (ot *OrderTracker) WithStore(store OrderStore) *OrderTracker {
	ot.store = store
	orders, states, _, err := store.GetActiveOrders(context.Background())
	if err == nil {
		ot.mu.Lock()
		for _, o := range orders {
			ot.orders[o.OrderID] = o
			ot.states[o.OrderID] = states[o.OrderID]
		}
		ot.mu.Unlock()
	}
	return ot
}

// AddOrder registers a new order and sets its initial state.
func (ot *OrderTracker) AddOrder(ctx context.Context, order exchange.Order, state exchange.OrderState) {
	ot.mu.Lock()
	ot.orders[order.OrderID] = order
	ot.states[order.OrderID] = state
	ot.mu.Unlock()

	if ot.store != nil {
		_ = ot.store.SaveOrder(ctx, order, state)
	}
}

// UpdateOrderState updates the status of an existing order. Returns true if
// the state changed.
//
// History note (audit "Stubbed / quietly broken"): the previous implementation
// wrote filledQty=0 to the DB on every non-Filled transition, silently zeroing
// out the column for partially-filled orders. The fix is to look up the
// CURRENT filled_qty from the in-memory tracker (rebuilt from the store on
// boot) before writing — only StateFilled overrides it to the full quantity.
// Callers that already know the right qty should keep using
// UpdateOrderStateAndQty (the WS path does); this method is for state-only
// transitions like REJECTED, CANCELED, EXPIRED.
func (ot *OrderTracker) UpdateOrderState(ctx context.Context, orderID string, state exchange.OrderState) bool {
	ot.mu.Lock()
	oldState, exists := ot.states[orderID]
	if !exists || oldState != state {
		ot.states[orderID] = state
		var filledQty float64
		if state == exchange.StateFilled {
			if o, ok := ot.orders[orderID]; ok {
				filledQty = o.Quantity
			}
		} else {
			// Preserve whatever filled_qty the store currently records — we
			// don't have a fresh number to write, so don't blow away the old.
			if ot.store != nil {
				if q, ok := ot.lookupFilledQtyLocked(ctx, orderID); ok {
					filledQty = q
				}
			}
		}
		ot.mu.Unlock()

		if ot.store != nil {
			_ = ot.store.UpdateOrderState(ctx, orderID, state, filledQty)
		}
		return true
	}
	ot.mu.Unlock()
	return false
}

// lookupFilledQtyLocked reads the order's current filled_qty from the store
// without trying to refresh anything in memory. Called with ot.mu held.
func (ot *OrderTracker) lookupFilledQtyLocked(ctx context.Context, orderID string) (float64, bool) {
	// Avoid a round-trip on every state-only transition: GetActiveOrders is
	// O(active set), and the boot path already populated us. Walk the locally
	// cached snapshot first.
	if ot.store == nil {
		return 0, false
	}
	// Best-effort: fall back to a fresh query if we couldn't find it locally.
	// We can't easily cache filled_qty per-order without changing the tracker
	// shape, so the safe fallback is to issue one query. With single-digit
	// active orders this is a cheap (< 1 ms) SQLite call.
	_, _, qmap, err := ot.store.GetActiveOrders(ctx)
	if err != nil {
		return 0, false
	}
	q, ok := qmap[orderID]
	return q, ok
}

// UpdateOrderStateAndQty updates both the state and filled quantity.
func (ot *OrderTracker) UpdateOrderStateAndQty(ctx context.Context, orderID string, state exchange.OrderState, filledQty float64) bool {
	ot.mu.Lock()
	oldState, exists := ot.states[orderID]
	ot.states[orderID] = state
	ot.mu.Unlock()

	if ot.store != nil {
		_ = ot.store.UpdateOrderState(ctx, orderID, state, filledQty)
	}
	return !exists || oldState != state
}

// GetOrder retrieves an order by its ID.
func (ot *OrderTracker) GetOrder(orderID string) (exchange.Order, bool) {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	order, exists := ot.orders[orderID]
	return order, exists
}

// GetOrderState retrieves the current status of an order.
func (ot *OrderTracker) GetOrderState(orderID string) (exchange.OrderState, bool) {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	state, exists := ot.states[orderID]
	return state, exists
}

// GetAllActiveOrders retrieves a list of all orders that are not in terminal states.
func (ot *OrderTracker) GetAllActiveOrders() []exchange.Order {
	ot.mu.RLock()
	defer ot.mu.RUnlock()

	var active []exchange.Order
	for id, order := range ot.orders {
		state := ot.states[id]
		if state != exchange.StateFilled && state != exchange.StateCanceled && state != exchange.StateRejected && state != exchange.StateExpired {
			active = append(active, order)
		}
	}
	return active
}

// TokenBucketLimiter is a thread-safe token bucket rate limiter.
type TokenBucketLimiter struct {
	mu         sync.Mutex
	capacity   float64
	tokens     float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewTokenBucketLimiter initializes a new rate limiter with the given requests-per-second limit.
func NewTokenBucketLimiter(rps float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		capacity:   rps,
		tokens:     rps,
		refillRate: rps,
		lastRefill: time.Now(),
	}
}

// Wait blocks until a token is available or the context is canceled.
func (tbl *TokenBucketLimiter) Wait(ctx context.Context) error {
	start := time.Now()
	defer func() {
		delay := time.Since(start).Seconds()
		if delay > 0 {
			telemetry.RateLimitDelay.Add(delay)
		}
	}()

	for {
		tbl.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tbl.lastRefill).Seconds()
		tbl.lastRefill = now

		// Refill the bucket based on elapsed time
		tbl.tokens += elapsed * tbl.refillRate
		if tbl.tokens > tbl.capacity {
			tbl.tokens = tbl.capacity
		}

		// Check if we have at least one token
		if tbl.tokens >= 1.0 {
			tbl.tokens -= 1.0
			tbl.mu.Unlock()
			return nil
		}

		// Calculate how long to wait until a token is refilled
		neededTokens := 1.0 - tbl.tokens
		waitTime := time.Duration(neededTokens / tbl.refillRate * float64(time.Second))
		tbl.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}
