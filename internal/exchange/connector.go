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
type ExecutionFill struct {
	OrderID         string    `json:"order_id"`
	Instrument      string    `json:"instrument"`
	Side            OrderSide `json:"side"`
	Quantity        float64   `json:"quantity"`
	FillPrice       float64   `json:"fill_price"`
	TransactionCost float64   `json:"transaction_cost"` // Fee incurred
	Timestamp       time.Time `json:"timestamp"`
}

// ExchangeConnector represents a pluggable exchange API handler.
type ExchangeConnector interface {
	// SubmitOrder submits an order to the live exchange.
	SubmitOrder(ctx context.Context, order Order) (ExecutionFill, error)

	// CancelOrder cancels an open order on the live exchange.
	CancelOrder(ctx context.Context, orderID string, instrument string) error

	// GetOrderState queries the live exchange for the state of an order.
	GetOrderState(ctx context.Context, orderID string, instrument string) (OrderState, float64, float64, error)

	// StartUserStream opens a real-time stream (e.g. WebSockets) to consume execution reports.
	StartUserStream(ctx context.Context, fillChan chan<- ExecutionFill) error
}
