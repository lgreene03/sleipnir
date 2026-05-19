package gateway

import (
	"context"
	"sync"
	"time"

	"sleipnir/internal/exchange"
)

// OrderTracker is an active, thread-safe memory manager for tracking pending/submitted orders.
type OrderTracker struct {
	mu     sync.RWMutex
	orders map[string]exchange.Order
	states map[string]exchange.OrderState
}

// NewOrderTracker creates a new instance of OrderTracker.
func NewOrderTracker() *OrderTracker {
	return &OrderTracker{
		orders: make(map[string]exchange.Order),
		states: make(map[string]exchange.OrderState),
	}
}

// AddOrder registers a new order and sets its initial state.
func (ot *OrderTracker) AddOrder(order exchange.Order, state exchange.OrderState) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.orders[order.OrderID] = order
	ot.states[order.OrderID] = state
}

// UpdateOrderState updates the status of an existing order. Returns true if the state changed.
func (ot *OrderTracker) UpdateOrderState(orderID string, state exchange.OrderState) bool {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	oldState, exists := ot.states[orderID]
	if !exists || oldState != state {
		ot.states[orderID] = state
		return true
	}
	return false
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
