// Package exchange defines the venue-agnostic ExchangeConnector interface
// — SubmitOrder, CancelOrder, GetOrderState, StartUserStream — along with
// the canonical Order / ExecutionFill types and the production Binance
// Spot implementation. Sleipnir is single-venue today; multi-venue routing
// is explicitly Phase F / deferred per docs/ROADMAP.md.
package exchange

import (
	"context"
	"time"
)

// OrderSide represents whether an order is a buy or sell.
type OrderSide string

const (
	SideBuy  OrderSide = "BUY"
	SideSell OrderSide = "SELL"
)

// OrderType represents the execution type of an order.
type OrderType string

const (
	TypeLimit  OrderType = "LIMIT"
	TypeMarket OrderType = "MARKET"
)

// OrderState represents the current state of an order in the gateway or on the exchange.
type OrderState string

const (
	StatePending         OrderState = "PENDING"
	StateSubmitted       OrderState = "SUBMITTED"
	StatePartiallyFilled OrderState = "PARTIALLY_FILLED"
	StateFilled          OrderState = "FILLED"
	StateCanceled        OrderState = "CANCELED"
	StateRejected        OrderState = "REJECTED"
	StateExpired         OrderState = "EXPIRED"
)

// Order represents an execution intent mapped to exchange-facing fields.
type Order struct {
	OrderID    string    `json:"order_id"`
	Instrument string    `json:"instrument"` // e.g., BTC-USD
	Side       OrderSide `json:"side"`
	Quantity   float64   `json:"quantity"`
	Price      float64   `json:"price"` // Ignore for MARKET orders
	Type       OrderType `json:"order_type"`
}

// ExecutionFill represents a verified execution transaction block from the exchange.
//
// ExecutionID provides a deterministic per-fill identity so downstream consumers
// (notably huginn's executor) can drop duplicate events on restart-or-replay paths.
// Construction follows three patterns, all collision-free across restarts:
//
//   - REST submit response:   "<clientOrderID>-rest-<binance order id>"
//   - WebSocket execution:    "<clientOrderID>-ws-<binance trade id>"
//   - Boot reconciliation:    "<clientOrderID>-reconcile-<filledQty>"
//
// OrderStatus carries the lifecycle state the exchange reports alongside the
// fill (FILLED for terminal, PARTIALLY_FILLED for partial). Prior to Phase 5
// the gateway treated every WS-delivered fill as terminal, silently
// collapsing partial fills into one StateFilled transition; carrying the
// status explicitly fixes that.
//
// See docs/CONTRACTS.md for the cross-repo contract with huginn.
type ExecutionFill struct {
	OrderID         string     `json:"order_id"`
	ExecutionID     string     `json:"execution_id"`
	Instrument      string     `json:"instrument"`
	Side            OrderSide  `json:"side"`
	OrderStatus     OrderState `json:"order_status"`     // Phase 5 addition
	Quantity        float64    `json:"quantity"`
	FillPrice       float64    `json:"fill_price"`
	TransactionCost float64    `json:"transaction_cost"` // Fee incurred
	Timestamp       time.Time  `json:"timestamp"`
}

// OrderStatusResult is what GetOrderState returns. Carrying a typed struct
// (instead of four scalar returns) keeps the signature stable as we add fields
// — e.g. Phase 5's TransactTime, planned future TimeInForce.
type OrderStatusResult struct {
	State        OrderState
	ExecutedQty  float64
	FillPrice    float64
	TransactTime time.Time // exchange-reported, used by boot reconciliation
}

// ExchangeConnector represents a pluggable exchange API handler.
type ExchangeConnector interface {
	// SubmitOrder submits an order to the live exchange.
	SubmitOrder(ctx context.Context, order Order) (ExecutionFill, error)

	// CancelOrder cancels an open order on the live exchange.
	CancelOrder(ctx context.Context, orderID string, instrument string) error

	// GetOrderState queries the live exchange for the state of an order.
	// The boot reconciliation path uses the returned TransactTime to stamp
	// the synthesized backfill fill, replacing the previous time.Now() hack
	// (audit finding L6).
	GetOrderState(ctx context.Context, orderID string, instrument string) (OrderStatusResult, error)

	// StartUserStream opens a real-time stream (e.g. WebSockets) to consume execution reports.
	StartUserStream(ctx context.Context, fillChan chan<- ExecutionFill) error
}
