package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sleipnir/internal/config"
	"sleipnir/internal/exchange"
	"sleipnir/internal/gateway"
	"sleipnir/internal/kafka"
	"sleipnir/internal/tracing"
	"sleipnir/internal/version"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 1. Initialize structured JSON logging (stdout)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	v := version.Get()
	logger.Info("Starting Sleipnir Order Execution Gateway",
		"version", v.Version, "git_sha", v.GitSHA, "build_time", v.BuildTime)

	// 2. Load environment configurations
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Failed to initialize configuration. Ensure API credentials are set.", "error", err)
		os.Exit(1)
	}

	logger.Info("Configuration loaded successfully",
		"kafka_brokers", cfg.KafkaBrokers,
		"intents_topic", cfg.KafkaIntentsTopic,
		"fills_topic", cfg.KafkaFillsTopic,
		"rate_limit_rps", cfg.RateLimitRPS,
	)

	// 3. Set up signal catching for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// OpenTelemetry tracing: if OTEL_EXPORTER_OTLP_ENDPOINT is set, spans are
	// exported via OTLP/gRPC. Unset → no-op (still propagates W3C TraceContext
	// in-process so headers flow through Kafka end-to-end).
	tracingShutdown, err := tracing.Init(ctx, v.Version)
	if err != nil {
		logger.Error("Failed to initialize OTel tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := tracingShutdown(shutdownCtx); err != nil {
			logger.Error("OTel tracing shutdown error", "error", err)
		}
	}()

	// 4. Initialize components (Dependency Injection)
	// Database path volume-mounted or default
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/app/data/sleipnir.db"
	}

	logger.Info("Initializing persistent SQLite database...", "path", dbPath)
	store, err := gateway.NewSQLiteOrderStore(dbPath)
	if err != nil {
		logger.Error("Failed to initialize SQLite order store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Initialize the tracker and preload non-terminal active orders
	tracker := gateway.NewOrderTracker().WithStore(store)
	limiter := gateway.NewTokenBucketLimiter(cfg.RateLimitRPS)

	// Exchange backend selector. EXCHANGE_BACKEND=sim swaps the live Binance
	// connector for the in-memory simulator — same ExchangeConnector
	// interface, no credentials required. The default ("binance") preserves
	// the production path.
	var connector exchange.ExchangeConnector
	switch backend := os.Getenv("EXCHANGE_BACKEND"); backend {
	case "", "binance":
		connector = exchange.NewBinanceConnector(
			cfg.BinanceAPIKey,
			cfg.BinanceAPISecret,
			cfg.BinanceRESTURL,
			cfg.BinanceWSURL,
			logger,
		)
	case "sim":
		logger.Warn("Using in-memory simulator backend; orders are NOT submitted to a real exchange")
		connector = exchange.NewSimulatorConnector(exchange.SimulatorConfig{
			FillPrice:       50_000.0,
			TransactionCost: 0.0,
			AlsoEmitOnWS:    false,
		}, logger)
	default:
		logger.Error("Unknown EXCHANGE_BACKEND", "backend", backend, "valid", "binance|sim")
		os.Exit(1)
	}

	consumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaIntentsTopic,
		cfg.KafkaConsumerGroup,
		logger,
	)

	producer := kafka.NewProducer(
		cfg.KafkaBrokers,
		cfg.KafkaFillsTopic,
		logger,
	)

	// 5. Active Boot-Time Reconciliation Loop
	logger.Info("Performing boot-time order reconciliation against live exchange status...")
	reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 30*time.Second)

	activeOrders, activeStates, filledQtys, err := store.GetActiveOrders(reconcileCtx)
	if err != nil {
		logger.Error("Failed to query active orders for reconciliation", "error", err)
	} else {
		logger.Info("Found outstanding active orders in local store to reconcile", "count", len(activeOrders))
		for _, order := range activeOrders {
			logger.Info("Querying live status for order", "orderID", order.OrderID, "instrument", order.Instrument)

			res, err := connector.GetOrderState(reconcileCtx, order.OrderID, order.Instrument)
			if err != nil {
				logger.Error("Failed to query order state on exchange during boot reconciliation", "orderID", order.OrderID, "error", err)
				continue
			}

			prevFilledQty := filledQtys[order.OrderID]
			deltaQty := res.ExecutedQty - prevFilledQty

			logger.Info("Reconciliation details",
				"orderID", order.OrderID,
				"local_state", activeStates[order.OrderID],
				"exchange_state", res.State,
				"prev_filled_qty", prevFilledQty,
				"exchange_filled_qty", res.ExecutedQty,
				"delta_qty", deltaQty,
				"transact_time", res.TransactTime,
			)

			if deltaQty > 0 {
				logger.Info("Detected missed fills. Backfilling fill message to Kafka...", "orderID", order.OrderID, "qty", deltaQty)

				// Phase 5 fix: timestamp the synthesized backfill with the
				// exchange-reported transaction time, not time.Now() (audit
				// finding L6). Falls back to now() only when the exchange
				// returned a zero TransactTime (shouldn't happen, but safe).
				ts := res.TransactTime
				if ts.IsZero() {
					ts = time.Now()
				}
				fill := exchange.ExecutionFill{
					OrderID: order.OrderID,
					// Stable across restarts: same (orderID, deltaQty) → same ExecutionID.
					// Huginn's LRU dedup uses this to drop the event if it has already
					// applied the WS fill that this reconciliation is backfilling.
					ExecutionID:     fmt.Sprintf("%s-reconcile-%g", order.OrderID, deltaQty),
					Instrument:      order.Instrument,
					Side:            order.Side,
					OrderStatus:     res.State,
					Quantity:        deltaQty,
					FillPrice:       res.FillPrice,
					TransactionCost: 0.0,
					Timestamp:       ts,
				}

				if err := producer.PublishFill(reconcileCtx, fill); err != nil {
					logger.Error("Failed to publish backfilled execution fill to Kafka", "orderID", order.OrderID, "error", err)
				} else {
					logger.Info("Successfully backfilled execution fill to Kafka", "orderID", order.OrderID)
				}
			}

			// Synchronize status in tracking memory & persistent DB
			tracker.UpdateOrderStateAndQty(order.OrderID, res.State, res.ExecutedQty)
		}
		logger.Info("Active boot-time reconciliation completed successfully.")
	}
	reconcileCancel()

	// Phase 6 risk policy: prefer the operator's risk.yaml when configured;
	// fall back to the legacy hardcoded BTC/ETH caps so existing deployments
	// don't break. See audit C3 — operators should land a risk.yaml ASAP.
	riskPolicy, err := gateway.LoadRiskPolicy(os.Getenv("RISK_CONFIG_PATH"))
	if err != nil {
		logger.Error("Failed to load risk policy", "error", err)
		os.Exit(1)
	}
	if riskPolicy == nil {
		logger.Warn("RISK_CONFIG_PATH not set — using legacy hardcoded BTC/ETH-only caps. Non-BTC/ETH instruments will pass without a size cap. See docs/SECURITY_AUDIT.md C3.")
		riskPolicy = gateway.NewLegacyRiskPolicy(cfg.MaxOrderQtyBTC, cfg.MaxOrderQtyETH)
	}
	halt := gateway.NewHalt()

	gw := gateway.NewGateway(
		consumer,
		producer,
		connector,
		tracker,
		limiter,
		riskPolicy,
		halt,
		cfg.MaxDailyOrders,
		logger,
	).WithDailySideLimits(cfg.MaxDailyBuys, cfg.MaxDailySells)

	// 6. Spin up a production-grade health check probe HTTP server (with Prometheus /metrics)
	healthServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           healthHandler(tracker, gw, halt),
		ReadHeaderTimeout: 10 * time.Second, // mitigates Slowloris (gosec G112)
	}

	go func() {
		logger.Info("Starting HTTP API (health-check & telemetry) server", "port", cfg.Port)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP API server encountered an error", "error", err)
		}
	}()

	// 7. Handle signals and coordinate graceful teardown
	go func() {
		sig := <-sigChan
		logger.Info("System signal captured, triggering shutdown sequence...", "signal", sig.String())
		cancel() // Propagate cancellation context

		// Shut down health server
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Error during health-check server shutdown", "error", err)
		}

		// Close Kafka resources safely
		if err := consumer.Close(); err != nil {
			logger.Error("Error closing Kafka consumer connection", "error", err)
		}
		if err := producer.Close(); err != nil {
			logger.Error("Error closing Kafka producer connection", "error", err)
		}
		logger.Info("Sleipnir resources torn down cleanly. Exiting.")
		os.Exit(0)
	}()

	// 8. Run the Gateway Loop
	if err := gw.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Gateway loops terminated via context completion.")
		} else {
			logger.Error("Fatal runtime failure in Gateway loop", "error", err)
			os.Exit(1)
		}
	}
}

// healthHandler serves liveness probes, /readyz, active trading telemetry,
// the operator kill switch, and Prometheus /metrics.
//
// Endpoint contract:
//
//	GET  /healthz      always 200 if the process is up
//	GET  /readyz       200 once the gateway has consumed ≥ 1 intent; 503 otherwise
//	GET  /telemetry    active-order count snapshot
//	POST /admin/halt   flip the in-memory kill switch; body: {"reason":"…"}
//	POST /admin/resume clear the kill switch
//	GET  /admin/halt   current halt status JSON
//	GET  /metrics      Prometheus
func healthHandler(tracker *gateway.OrderTracker, gw *gateway.Gateway, halt *gateway.Halt) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"OK","service":"sleipnir"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if gw.IsReady() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"READY"}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"NOT_READY"}`))
	})
	mux.HandleFunc("/telemetry", func(w http.ResponseWriter, r *http.Request) {
		activeOrders := tracker.GetAllActiveOrders()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"HEALTHY","active_tracked_orders":%d,"halted":%t}`, len(activeOrders), halt.IsHalted())
	})
	mux.HandleFunc("/admin/halt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprintf(w, `{"halted":%t,"reason":%q}`, halt.IsHalted(), halt.Reason())
		case http.MethodPost:
			var body struct {
				Reason string `json:"reason"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			halt.Set(body.Reason)
			slog.Warn("Operator kill switch engaged", "reason", halt.Reason())
			fmt.Fprintf(w, `{"halted":true,"reason":%q}`, halt.Reason())
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/admin/resume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		halt.Clear()
		slog.Info("Operator kill switch cleared")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"halted":false}`))
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(version.Get()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
