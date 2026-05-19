package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sleipnir/internal/exchange"

	_ "modernc.org/sqlite"
)

// OrderStore defines the persistence interface for gateway order tracking.
type OrderStore interface {
	SaveOrder(ctx context.Context, order exchange.Order, state exchange.OrderState) error
	UpdateOrderState(ctx context.Context, orderID string, state exchange.OrderState, filledQty float64) error
	GetActiveOrders(ctx context.Context) ([]exchange.Order, map[string]exchange.OrderState, map[string]float64, error)
}

// SQLiteOrderStore implements OrderStore using SQLite (CGO-free).
type SQLiteOrderStore struct {
	db *sql.DB
}

// NewSQLiteOrderStore creates a new SQLite-backed order store.
func NewSQLiteOrderStore(dbPath string) (*SQLiteOrderStore, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Create table schema
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		order_id TEXT PRIMARY KEY,
		instrument TEXT NOT NULL,
		side TEXT NOT NULL,
		quantity REAL NOT NULL,
		price REAL NOT NULL,
		type TEXT NOT NULL,
		state TEXT NOT NULL,
		filled_qty REAL NOT NULL DEFAULT 0.0,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);`
	if _, err := db.Exec(query); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create orders table: %w", err)
	}

	return &SQLiteOrderStore{db: db}, nil
}

// Close closes the underlying database connection.
func (s *SQLiteOrderStore) Close() error {
	return s.db.Close()
}

// SaveOrder transactionally persists a new order into the database.
func (s *SQLiteOrderStore) SaveOrder(ctx context.Context, order exchange.Order, state exchange.OrderState) error {
	query := `
	INSERT INTO orders (order_id, instrument, side, quantity, price, type, state, filled_qty, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(order_id) DO UPDATE SET
		state = excluded.state,
		updated_at = excluded.updated_at;`

	now := time.Now()
	_, err := s.db.ExecContext(ctx, query,
		order.OrderID,
		order.Instrument,
		string(order.Side),
		order.Quantity,
		order.Price,
		string(order.Type),
		string(state),
		0.0,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("failed to save order to sqlite: %w", err)
	}
	return nil
}

// UpdateOrderState updates the state and filled quantity of an existing order.
func (s *SQLiteOrderStore) UpdateOrderState(ctx context.Context, orderID string, state exchange.OrderState, filledQty float64) error {
	query := `
	UPDATE orders
	SET state = ?, filled_qty = ?, updated_at = ?
	WHERE order_id = ?;`

	_, err := s.db.ExecContext(ctx, query, string(state), filledQty, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update order state in sqlite: %w", err)
	}
	return nil
}

// GetActiveOrders fetches all orders in non-terminal states.
func (s *SQLiteOrderStore) GetActiveOrders(ctx context.Context) ([]exchange.Order, map[string]exchange.OrderState, map[string]float64, error) {
	query := `
	SELECT order_id, instrument, side, quantity, price, type, state, filled_qty
	FROM orders
	WHERE state NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED');`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query active orders from sqlite: %w", err)
	}
	defer rows.Close()

	var orders []exchange.Order
	states := make(map[string]exchange.OrderState)
	filledQtys := make(map[string]float64)

	for rows.Next() {
		var o exchange.Order
		var sideStr, typeStr, stateStr string
		var filledQty float64

		err := rows.Scan(
			&o.OrderID,
			&o.Instrument,
			&sideStr,
			&o.Quantity,
			&o.Price,
			&typeStr,
			&stateStr,
			&filledQty,
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan active order row: %w", err)
		}

		o.Side = exchange.OrderSide(sideStr)
		o.Type = exchange.OrderType(typeStr)
		orders = append(orders, o)
		states[o.OrderID] = exchange.OrderState(stateStr)
		filledQtys[o.OrderID] = filledQty
	}

	return orders, states, filledQtys, nil
}
