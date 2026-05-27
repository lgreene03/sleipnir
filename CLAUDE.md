# CLAUDE.md

## What Is Sleipnir

Sleipnir is the order execution gateway in the Norse stack. It bridges Huginn's order intents to exchange APIs (Binance Spot) and reports fills back. Named after Odin's eight-legged steed.

## Commands

```bash
# Build
make build

# Run tests
make test

# Run tests with race detector
make test-race

# Lint
make lint

# Docker Compose (Sleipnir + Redpanda + mocks + Prometheus + Grafana)
docker compose up -d

# Smoke test (sim mode, no Binance credentials needed)
bash scripts/smoke.sh
```

## Architecture

```
Kafka (executions.intents.v1) → Intent Consumer → Risk Check → Rate Limiter
                                                                    ↓
                                                          Binance REST (submit)
                                                                    ↓
                                                          Binance WS (fills)
                                                                    ↓
                                                   Kafka (executions.fills.v1)
```

**Exchange backends:**
- `binance` (default) — real Binance Spot testnet REST + WebSocket
- `sim` — in-memory simulated exchange for testing (no credentials needed)

## Key Packages

- `cmd/sleipnir/` — Main entry point. Wires Kafka consumer, exchange connector, fill publisher.
- `internal/gateway/` — Core orchestration: intent consumer → risk → rate limit → submit → fill → publish.
- `internal/exchange/` — `ExchangeConnector` interface with `BinanceConnector` and `SimulatedConnector`.
- `internal/kafka/` — Kafka intent consumer and fill publisher.
- `internal/risk/` — Pre-trade risk policy: per-instrument size caps, daily order count, operator halt.
- `internal/store/` — SQLite order lifecycle persistence + reconciliation.
- `internal/ratelimit/` — Token-bucket rate limiter.

## Configuration

All via environment variables:

- `KAFKA_BROKERS`, `KAFKA_INTENTS_TOPIC`, `KAFKA_FILLS_TOPIC`, `KAFKA_CONSUMER_GROUP`
- `EXCHANGE_BACKEND` (`binance` or `sim`)
- `BINANCE_API_KEY`, `BINANCE_API_SECRET` (required for binance backend)
- `RATE_LIMIT_RPS` (default `10.0`)
- `MAX_ORDER_QTY_BTC`, `MAX_ORDER_QTY_ETH`, `MAX_DAILY_ORDERS`
- `PORT` (default `8080`), `DB_PATH` (default `/app/data/sleipnir.db`)

## Norse Stack Context

```
Huginn (intents) → Sleipnir (execution) → Exchange → Sleipnir (fills) → Huginn (portfolio)
```

- Sleipnir consumes: `executions.intents.v1`
- Sleipnir produces: `executions.fills.v1`
- Wire contracts: `docs/CONTRACTS.md` (shared with Huginn)

## Testing

- Unit tests: `make test` (no Docker needed)
- Smoke test: `bash scripts/smoke.sh` (boots Docker in sim mode, auto-teardown)
- Cross-stack: `bash ../muninn/scripts/smoke-stack.sh` (full Norse pipeline)
