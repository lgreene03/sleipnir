# Changelog

All notable changes to Sleipnir are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

---

## [0.7.0] — 2026-06-19

### Added
- `sleipnir_active_orders` Prometheus gauge — current count of non-terminal orders.
- `sleipnir_intent_to_submit_seconds` histogram — latency from Kafka intent consumed to exchange submission.
- `sleipnir_fill_to_publish_seconds` histogram — latency from WS fill received to Kafka publish.
- Correlation ID (`correlation_id` UUID) generated at intent-consume time; threaded through all log lines that touch the same order lifecycle.
- **Standalone smoke test** (`scripts/smoke.sh`) — boots the stack in sim mode via `docker-compose.smoke.yml` overlay, verifies intent → fill → mock-portfolio round-trip, checks `/metrics` and `/readyz`. Auto-teardown on exit.
- **Sim exchange overlay** (`docker-compose.smoke.yml`) — sets `EXCHANGE_BACKEND=sim` for the gateway, enabling end-to-end testing without Binance credentials.
- Intent `OrderID` validation at the gateway boundary (security audit finding H4).
- `bodyclose` and `contextcheck` linters enabled in golangci-lint.
- `context.Context` threaded through OrderTracker methods for proper context propagation.
- Gateway coverage boost: store persistence, token bucket cancellation, risk DB error paths, duplicate order rejection tests.

### Changed
- All `orderID` log fields now carry a sibling `correlation_id` field for cross-process tracing.

---

## [0.6.0] — 2026-05-14

### Added
- Phase 6 — Risk and ops controls: per-instrument notional hard cap, daily order/buy/sell caps, operator halt/resume via `POST /halt` and `POST /resume`. `RiskPolicy` reads limits from `configs/default.yaml`.
- `sleipnir_risk_rejections_total{instrument,reason}` counter for every pre-trade rejection path.

---

## [0.5.0] — 2026-05-12

### Added
- Phase 5 — Correctness hardening: idempotent fill processing (dedup by `ExecutionID`), partial-fill state machine (`PARTIALLY_FILLED` → `FILLED`), commission + slippage recorded per order in SQLite (`fill_costs` table), per-side daily caps (`max_daily_buys`, `max_daily_sells`).

### Fixed
- `UpdateOrderStateAndQty` now accumulates `filled_qty` rather than overwriting it on each partial fill.

---

## [0.4.0] — 2026-05-08

### Added
- Phase 4 foundation: `gateway_e2e_test.go` cross-repo contract test asserting the wire format between Huginn's intent producer and Sleipnir's Kafka consumer.

---

## [0.3.0] — 2026-05-05

### Added
- Phase 3 — Testability: `ExchangeConnector` interface + `SimulatedConnector` (fully synchronous, no network). `IntentConsumer` and `FillPublisher` interfaces for dependency injection. End-to-end gateway test running purely in-memory.

---

## [0.2.0] — 2026-04-30

### Added
- Phase 2 — CI, lint, test hygiene: `golangci-lint` with `staticcheck`, `errcheck`, `gosec`, `revive`. GitHub Actions matrix (Go 1.25 on ubuntu-latest). `CONTRIBUTING.md` and `SECURITY.md`.

---

## [0.1.0] — 2026-04-25

### Added
- Phase 1 — Project foundations: `cmd/sleipnir/main.go`, `BinanceConnector` (REST submit/cancel/status + WS user-stream fills), Kafka intent consumer + fills producer, `OrderTracker` + SQLite `OrderStore`, token-bucket rate limiter, Prometheus `/metrics`, `/healthz` + `/readyz`.
