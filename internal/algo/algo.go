// Package algo implements execution algorithms (TWAP, VWAP) that slice large
// parent orders into smaller child orders executed over time. This is the
// standard approach at institutional trading desks to minimise market impact.
//
// Usage: wrap an ExchangeConnector with an algorithm, then submit the parent
// order. The algorithm handles slicing, pacing, and fill aggregation.
package algo

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"sleipnir/internal/exchange"
)

// Algorithm is the execution algorithm type.
type Algorithm string

const (
	AlgoTWAP Algorithm = "TWAP"
	AlgoVWAP Algorithm = "VWAP"
)

// Config controls how orders are sliced and paced.
type Config struct {
	Algorithm Algorithm
	Duration  time.Duration // total execution window
	Slices    int           // number of child orders
	// VWAPWeights is an optional volume profile for VWAP. If nil, a U-shaped
	// intraday curve is used (heavier at open/close). Length must match Slices.
	VWAPWeights []float64
}

// Result is the aggregated outcome of an algorithmic execution.
type Result struct {
	ParentOrderID string
	Fills         []exchange.ExecutionFill
	TotalQty      float64
	AvgPrice      float64
	TotalCost     float64
	SlicesExec    int
	SlicesFailed  int
	Duration      time.Duration
}

// Executor runs algorithmic order execution over an ExchangeConnector.
type Executor struct {
	connector exchange.ExchangeConnector
	logger    *slog.Logger
}

func NewExecutor(connector exchange.ExchangeConnector, logger *slog.Logger) *Executor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Executor{
		connector: connector,
		logger:    logger.With("module", "algo"),
	}
}

// Execute runs the configured algorithm. Blocks until all slices are submitted
// or the context is cancelled.
func (e *Executor) Execute(ctx context.Context, order exchange.Order, cfg Config) (*Result, error) {
	if cfg.Slices < 1 {
		cfg.Slices = 5
	}
	if cfg.Duration == 0 {
		cfg.Duration = 5 * time.Minute
	}

	switch cfg.Algorithm {
	case AlgoTWAP:
		return e.executeTWAP(ctx, order, cfg)
	case AlgoVWAP:
		return e.executeVWAP(ctx, order, cfg)
	default:
		return nil, fmt.Errorf("unknown algorithm: %s", cfg.Algorithm)
	}
}

// executeTWAP splits the order into equal-sized slices at uniform time intervals.
func (e *Executor) executeTWAP(ctx context.Context, parent exchange.Order, cfg Config) (*Result, error) {
	sliceQty := parent.Quantity / float64(cfg.Slices)
	interval := cfg.Duration / time.Duration(cfg.Slices)

	e.logger.Info("TWAP execution starting",
		"parent_id", parent.OrderID,
		"total_qty", parent.Quantity,
		"slices", cfg.Slices,
		"slice_qty", sliceQty,
		"interval", interval,
	)

	return e.executeSlices(ctx, parent, cfg.Slices, interval, func(i int) float64 {
		return sliceQty
	})
}

// executeVWAP splits the order proportional to a volume profile. Heavier
// execution during high-volume periods reduces market impact.
func (e *Executor) executeVWAP(ctx context.Context, parent exchange.Order, cfg Config) (*Result, error) {
	weights := cfg.VWAPWeights
	if weights == nil || len(weights) != cfg.Slices {
		weights = defaultVWAPProfile(cfg.Slices)
	}

	// Normalise weights
	totalWeight := 0.0
	for _, w := range weights {
		totalWeight += w
	}
	for i := range weights {
		weights[i] /= totalWeight
	}

	interval := cfg.Duration / time.Duration(cfg.Slices)

	e.logger.Info("VWAP execution starting",
		"parent_id", parent.OrderID,
		"total_qty", parent.Quantity,
		"slices", cfg.Slices,
		"interval", interval,
	)

	return e.executeSlices(ctx, parent, cfg.Slices, interval, func(i int) float64 {
		return parent.Quantity * weights[i]
	})
}

func (e *Executor) executeSlices(
	ctx context.Context,
	parent exchange.Order,
	slices int,
	interval time.Duration,
	qtyFn func(int) float64,
) (*Result, error) {
	result := &Result{
		ParentOrderID: parent.OrderID,
		Fills:         make([]exchange.ExecutionFill, 0, slices),
	}

	start := time.Now()
	var mu sync.Mutex

	for i := 0; i < slices; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				result.Duration = time.Since(start)
				return result, ctx.Err()
			case <-time.After(interval):
			}
		}

		childQty := qtyFn(i)
		if childQty <= 0 {
			continue
		}

		child := exchange.Order{
			OrderID:    fmt.Sprintf("%s-slice-%d", parent.OrderID, i+1),
			Instrument: parent.Instrument,
			Side:       parent.Side,
			Quantity:   childQty,
			Price:      parent.Price,
			Type:       parent.Type,
		}

		fill, err := e.connector.SubmitOrder(ctx, child)
		if err != nil {
			e.logger.Warn("Slice failed",
				"slice", i+1,
				"error", err,
			)
			mu.Lock()
			result.SlicesFailed++
			mu.Unlock()
			continue
		}

		mu.Lock()
		result.Fills = append(result.Fills, fill)
		result.TotalQty += fill.Quantity
		result.TotalCost += fill.TransactionCost
		result.SlicesExec++
		mu.Unlock()

		e.logger.Debug("Slice filled",
			"slice", i+1,
			"qty", fill.Quantity,
			"price", fill.FillPrice,
		)
	}

	result.Duration = time.Since(start)

	// Compute VWAP
	if result.TotalQty > 0 {
		totalNotional := 0.0
		for _, f := range result.Fills {
			totalNotional += f.FillPrice * f.Quantity
		}
		result.AvgPrice = totalNotional / result.TotalQty
	}

	e.logger.Info("Algo execution complete",
		"parent_id", parent.OrderID,
		"algo", "TWAP/VWAP",
		"filled", result.SlicesExec,
		"failed", result.SlicesFailed,
		"avg_price", fmt.Sprintf("%.4f", result.AvgPrice),
		"total_qty", result.TotalQty,
		"duration", result.Duration,
	)

	return result, nil
}

// defaultVWAPProfile generates a U-shaped intraday volume curve:
// heavier at the start and end (mimicking real market open/close volume spikes).
func defaultVWAPProfile(n int) []float64 {
	weights := make([]float64, n)
	mid := float64(n-1) / 2.0
	for i := 0; i < n; i++ {
		dist := math.Abs(float64(i)-mid) / mid // 0 at centre, 1 at edges
		weights[i] = 0.5 + 0.5*dist            // 0.5 at centre, 1.0 at edges
	}
	return weights
}
