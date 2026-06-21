# Sleipnir — Order Execution Gateway

[![CI](https://github.com/lgreene03/sleipnir/actions/workflows/ci.yml/badge.svg)](https://github.com/lgreene03/sleipnir/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

> *Named after Odin's eight-legged steed, who carries riders across worlds.*
> Sleipnir carries order intents from [Huginn](https://github.com/lgreene03/huginn) to the exchange, and fills back.
> Part of the **[Norse Stack](https://github.com/lgreene03/norse-stack)**.

Sleipnir is the **execution layer** of the four-service Norse stack:

| Service | Role | Repo |
|---|---|---|
| [muninn](https://github.com/lgreene03/muninn) | Deterministic feature engine (Java/Spring Boot) | server-side compute |
| [muninn-py](https://github.com/lgreene03/muninn-py) | Research SDK + CLI (Python) | client library |
| [huginn](https://github.com/lgreene03/huginn) | Strategy execution engine (Go) | reads features, emits intents |
| **sleipnir** (this repo) | Order execution gateway (Go) | submits intents, reports fills |

```
huginn  ──▶  executions.intents.v1  ──▶  sleipnir  ──▶  Binance Spot
                                            │
                                            ▼
huginn  ◀──  executions.fills.v1    ◀── sleipnir
```

## What Sleipnir does

- Consumes order intents from Kafka topic `executions.intents.v1`
- Runs pre-trade size + rate-limit + daily-count checks
- Signs and submits REST orders to Binance Spot (testnet by default)
- Listens on the Binance User Data WebSocket API for fill events
- Republishes verified `ExecutionFill` events on `executions.fills.v1`
- Persists order lifecycle to SQLite, reconciles missed fills on boot
- Exposes Prometheus metrics + an alert ruleset + a Grafana dashboard

See **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** for the diagram and the boot-reconciliation path.

## What Sleipnir is not

See **[`docs/ROADMAP.md`](docs/ROADMAP.md)** for the full non-goals list. Headlines:

- **Not a strategy engine.** Sleipnir does not decide what to trade. Huginn does.
- **Not a portfolio tracker.** Huginn owns positions and PnL.
- **Not a market data feed.** Muninn handles features. Sleipnir handles execution.
- **Not multi-venue.** One Sleipnir, one venue. Multi-venue is Phase F / deferred.
- **Not a smart order router.** No iceberg / VWAP / TWAP algos.
- **Not mainnet.** Sleipnir is a research artifact. Testnet only.

## Quick start

```bash
# Set Binance Spot Testnet credentials (https://testnet.binance.vision)
export BINANCE_API_KEY=...
export BINANCE_API_SECRET=...

# Bring up sleipnir + Redpanda + mocks + Prometheus + Grafana
docker compose up -d

# Watch the gateway logs
docker compose logs -f sleipnir

# Health
curl http://localhost:8085/healthz

# Prometheus
open http://localhost:9095

# Grafana (anonymous Admin)
open http://localhost:3005
```

> ⚠️ **Do not commit `.env`.** See **[`SECURITY.md`](SECURITY.md)**. The `.dockerignore` excludes it from build context, but it must not enter the repo at any layer.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_INTENTS_TOPIC` | `executions.intents.v1` | Inbound order intents |
| `KAFKA_FILLS_TOPIC` | `executions.fills.v1` | Outbound fill reports |
| `KAFKA_CONSUMER_GROUP` | `sleipnir-gateway` | Consumer group |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | _(required)_ | Spot Testnet credentials |
| `BINANCE_REST_URL` | `https://testnet.binance.vision` | Testnet REST host |
| `BINANCE_WS_URL` | `wss://ws-api.testnet.binance.vision/ws-api/v3` | Testnet WS host |
| `RATE_LIMIT_RPS` | `10.0` | Token-bucket request budget |
| `SUBMIT_TIMEOUT` | `6m` | Deadline on a single exchange submission (incl. a full TWAP/VWAP schedule) so a slow venue can't block the serial intent loop; `0` disables |
| `MAX_ORDER_QTY_BTC` | `0.1` | Per-order size cap for BTC |
| `MAX_ORDER_QTY_ETH` | `2.0` | Per-order size cap for ETH |
| `MAX_DAILY_ORDERS` | `500` | Daily count cap |
| `PORT` | `8080` | Health + metrics port |
| `DB_PATH` | `/app/data/sleipnir.db` | SQLite store path |
| `FEATURE_STREAM_ENABLED` | `false` | Tail Muninn's SSE feature stream ([ADR-0009](https://github.com/lgreene03/muninn/blob/main/docs/adr/0009-streaming-features-sse.md)); surfaces latest values at `/feature/latest` (read-only, does not drive trading) |
| `MUNINN_STREAM_URL` | `http://localhost:8080` | Muninn base URL for the SSE feature stream |
| `MUNINN_STREAM_FEATURE` | _(empty)_ | Restrict the stream to one feature name (`?feature=`); empty streams all |

> The hardcoded BTC/ETH per-instrument caps are a known limitation. Any non-BTC/ETH instrument falls through with no size cap — see [`docs/SECURITY_AUDIT.md`](docs/SECURITY_AUDIT.md) finding **C3**. Phase 5/6 replaces this with a config-driven `risk.yaml`.

## HTTP API

The control-plane and observability endpoints (`/healthz`, `/readyz`, `/telemetry`, `/metrics`, `/version`, `/feature/latest`, and the bearer-gated `POST /admin/halt` & `POST /admin/resume`) are specified in **[`api/openapi.yaml`](api/openapi.yaml)** (OpenAPI 3). This is the HTTP control surface only — order intents and fills flow over Kafka, not HTTP.

## Topic contracts

Frozen wire definitions live in **[`docs/CONTRACTS.md`](docs/CONTRACTS.md)**. The contract is shared with huginn (`huginn/internal/kafka/{producer.go,fills_consumer.go}`); changes are a coordinated cross-repo PR pair.

## Development

```bash
make build          # build all binaries
make test           # go test ./...
make test-race      # go test -race -coverprofile=cover.out
make lint           # golangci-lint
make ci             # mirror what GitHub Actions runs on every PR
make compose-up     # docker compose up -d
make compose-down   # docker compose down -v
```

CI lives in [`.github/workflows/ci.yml`](.github/workflows/ci.yml): Go 1.25 build + vet + `-race` + coverage upload, `golangci-lint`, and a non-pushing Docker build verification. Dependabot is wired in [`.github/dependabot.yml`](.github/dependabot.yml).

## Project documents

- **[`docs/ROADMAP.md`](docs/ROADMAP.md)** — phased plan, exit criteria, non-goals
- **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — single-page service diagram + boot reconciliation path
- **[`docs/CONTRACTS.md`](docs/CONTRACTS.md)** — Kafka wire contracts, cross-linked to huginn
- **[`api/openapi.yaml`](api/openapi.yaml)** — OpenAPI 3 spec for the HTTP control + observability endpoints
- **[`docs/SECURITY_AUDIT.md`](docs/SECURITY_AUDIT.md)** — focused security review (May 2026)
- **[`SECURITY.md`](SECURITY.md)** — how to report a vulnerability, secret-handling rules
- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — branching, commit style, review expectations
- **[`LICENSE`](LICENSE)** — Apache 2.0

## License

Apache 2.0. See [`LICENSE`](LICENSE).
