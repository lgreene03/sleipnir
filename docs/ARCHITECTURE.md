# Sleipnir — Architecture

One page. Companion to [`ROADMAP.md`](ROADMAP.md) and [`CONTRACTS.md`](CONTRACTS.md).

## Service position

Sleipnir is the execution leg of a four-service flow:

```
                          ┌────────────────┐
                          │     muninn     │   (feature engine, Java)
                          └────────┬───────┘
                                   │ features.*
                                   ▼
                          ┌────────────────┐
                          │     huginn     │   (strategy engine, Go)
                          └─┬────────────┬─┘
        executions.intents.v1│            │executions.fills.v1
                             ▼            │
                    ┌────────────────┐    │
                    │   sleipnir     │────┘
                    │  (this repo)   │
                    └────────┬───────┘
                             │  signed REST / WS
                             ▼
                    ┌────────────────┐
                    │ Binance Spot   │   (testnet only)
                    └────────────────┘
```

## Internal flow

```
┌──────────────────────────────────────────────────────────────────┐
│  cmd/sleipnir/main.go                                            │
│  - load config, init slog, open SQLite store                     │
│  - active boot reconciliation (publishes missed fills)           │
│  - graceful shutdown on SIGINT/SIGTERM                           │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  internal/gateway/gateway.go                                     │
│                                                                  │
│   Kafka intents loop                Kafka fills loop             │
│   ─────────────────                ─────────────────             │
│   FetchIntent                       drain fillChan               │
│       │                                  │                       │
│       ▼                                  ▼                       │
│   checkRiskLimits(intent)           PublishFill(fill)            │
│       │                                                          │
│       ▼                                                          │
│   TokenBucketLimiter.Wait                                        │
│       │                                                          │
│       ▼                                                          │
│   exchange.SubmitOrder(intent) ─────┐                            │
│       │                             │                            │
│       ▼                             ▼                            │
│   tracker.AddOrder()        immediate REST fill ──▶ fillChan     │
│       │                                                          │
│       └─▶ Commit Kafka offset                                    │
└────┬────────────────────────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────────────────────────────┐
│  internal/exchange/binance.go                                    │
│                                                                  │
│  REST: SubmitOrder, CancelOrder, GetOrderState                   │
│         (signed HMAC-SHA256, query-string canonicalization)      │
│                                                                  │
│  WS:  StartUserStream(ctx, fillChan)                             │
│         - exponential backoff + jitter                           │
│         - 30s stable-connection reset                            │
│         - subscribes executionReport events                      │
└────┬────────────────────────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────────────────────────────┐
│  internal/gateway/store.go + tracker.go                          │
│                                                                  │
│  SQLite via modernc.org/sqlite (CGO-free)                        │
│  Embedded versioned migrations, transactional apply              │
│  OrderTracker preloaded on boot via WithStore                    │
│  TokenBucketLimiter telemetry                                    │
└──────────────────────────────────────────────────────────────────┘
```

## Boot reconciliation

On startup, before consuming new intents:

1. Load all orders in `StateSubmitted` / `StatePartiallyFilled` from SQLite.
2. For each, call `exchange.GetOrderState(orderID)` to get the current status.
3. If the order is now `StateFilled` and the locally tracked `filledQty < exchange filledQty`, synthesize an `ExecutionFill` for the delta and publish it on `executions.fills.v1`.

This path lives in `cmd/sleipnir/main.go:92-147`. **It has known correctness bugs** documented in the roadmap and the audit — synthesized timestamp is `time.Now()` not the exchange transaction time, and there's no idempotency key so huginn will double-count on certain restart sequences. Fixed in Phase 5 via the cross-repo `ExecutionID` contract.

## Operational surfaces

| Surface | Where | Notes |
|---|---|---|
| HTTP health | `:8080/healthz` | Always 200 today (Phase 6 splits `/readyz`) |
| HTTP metrics | `:8080/metrics` | Prometheus, no auth |
| HTTP telemetry | `:8080/telemetry` | Active order counts, rate-limit delays |
| Prom alert rules | `telemetry/alerts.yml` | 3 rules today |
| Grafana dashboard | `telemetry/grafana/provisioning/` | Single dashboard |

## State of play, May 2026

The diagram describes the **target** flow. Today's implementation has the gaps catalogued in [`ROADMAP.md` § Current state assessment](ROADMAP.md#current-state-assessment) — notably the missing `ExecutionID`, the BTC/ETH-only risk check, and the dead `commission`/`slippage` SQLite columns. The roadmap orders the fixes.
