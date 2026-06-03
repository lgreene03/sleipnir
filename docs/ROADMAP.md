# Sleipnir — Roadmap

Sleipnir is the **Order Execution Gateway** in the four-service Norse stack. It consumes execution intents from huginn off Redpanda topic `executions.intents.v1`, submits them to Binance Spot (testnet today), tracks order lifecycles in SQLite, listens on the Binance User Data WS API for fills, and republishes verified `ExecutionFill` events on `executions.fills.v1` for huginn to apply to its portfolio. Pre-trade risk checks, a token-bucket rate limiter, boot-time reconciliation, and Prometheus telemetry are already wired.

Phased delivery mirrors the discipline of the muninn server roadmap and the muninn-py SDK roadmap: ergonomics before hardening, verification before publishing, deferred buckets never on their own schedule.

---

## Current state assessment

**Built and exercised by tests.**
- `internal/exchange/binance.go` — signed REST `SubmitOrder` / `CancelOrder` / `GetOrderState`, WS API User Data stream with exponential backoff + jitter and a 30s "stable connection" reset rule (lines 287–494). Symbol translation `BTC-USD ↔ BTCUSDT` (lines 52–69). Status mapping at `mapBinanceStatus` (line 499).
- `internal/gateway/gateway.go` — two-loop coordinator: fills worker drains `fillChan`, intents worker pulls from Kafka, runs `checkRiskLimits` (line 204), waits for `TokenBucketLimiter`, submits, commits offsets. Pre-trade size limits hardcoded per-instrument for BTC/ETH (lines 206–214) — non-extensible.
- `internal/gateway/store.go` — SQLite via `modernc.org/sqlite` (CGO-free), embedded versioned migrations (`dbMigrations`, 3 versions), `schema_migrations` ledger, transactional apply.
- `internal/gateway/tracker.go` — in-memory state with persistent backing, `WithStore` preload on boot, token bucket limiter with telemetry.
- `cmd/sleipnir/main.go` — slog JSON logger, active boot-time reconciliation that backfills missed fills via `producer.PublishFill` (lines 92–147), graceful shutdown on SIGINT/SIGTERM, health/telemetry/metrics HTTP server.
- `cmd/mock_huginn`, `cmd/mock_portfolio` — local end-to-end loop simulators.
- `telemetry/` — Prometheus scrape config, 3 alert rules (`HighRESTLatency` p95 > 1.5s, `WebsocketDisconnectSpike`, `RiskLimitsViolationSpike`), Grafana provisioning.
- 9 unit tests across `binance_test.go` and `gateway_test.go` (tracker, limiter pacing, sqlite persistence, migrations, risk limits, concurrency).

**Stubbed / quietly broken.**
- `ExecutionFill.TransactionCost` is hardcoded `0.0` in REST submit response parsing (`binance.go:179`) and in the reconciliation backfill path (`main.go:131`). Only the WS path populates fees.
- Migrations v2 and v3 add `commission` and `slippage` columns that **are never read or written** by any code in `store.go`. Dead columns.
- `OrderTracker.UpdateOrderState` writes `filledQty=0` to the DB on every non-`StateFilled` transition (`tracker.go:64–70`) — partial fills will silently zero out `filled_qty`.
- `OrderTracker.WithStore` swallows preload errors (`tracker.go:32`).
- The boot reconciliation backfill emits a synthetic `Timestamp: time.Now()` (`main.go:132`) rather than the exchange's transaction time, polluting downstream PnL timing.
- `TokenBucketLimiter.Wait` re-enters the `for` loop without bounded sleeps — fine, but `RateLimitDelay` is incremented unconditionally even when zero tokens were needed (no real wait).
- `gateway.checkRiskLimits` only knows about literal strings `"BTC-USD"`/`"ETH-USD"` and their `…USDT` exchange forms. Anything else gets unlimited size.
- WebSocket subscription uses `userDataStream.subscribe.signature` and never sends a keepalive PING. The Binance WS API will drop idle sockets at the 24h server timeout boundary; the reconnect loop handles it but there's no proactive ping.
- No deduplication of fills between the immediate REST `SubmitOrder` response (`gateway.go:180–190`) and the WS stream — the gateway publishes both and relies on a never-existing downstream dedup layer. Huginn applies every fill it receives (`OnExecutionFill` in `internal/executor/executor.go:146`) with no idempotency key.
- `Producer.PublishFill` uses `RequiredAcks: RequireAll` (good) but the gateway's Consumer commits offsets **after** producing — except on risk rejection it commits without producing, which is fine. There is no transactional consume-process-produce; a crash between produce and commit will re-deliver the intent and double-submit. Idempotency relies on Binance accepting `newClientOrderId` collision (it does, with `400` — but the gateway then marks the order rejected).

**Missing entirely.**
- No `README.md`, no `LICENSE`, no `CONTRIBUTING.md`, no `SECURITY.md`, no `docs/`, no `Makefile`, no `.github/workflows/`, no `.golangci.yml`, no `CHANGELOG.md`.
- No CI of any kind. `go test ./...` has never run in CI.
- No structured tracing (OpenTelemetry).
- No request/correlation IDs through the gateway.
- No paper-trading / simulated exchange connector — every test that wants to exercise the gateway loop end-to-end has to hit Binance testnet.
- No metrics for: order state distribution (active count gauge), Kafka consumer lag, DB query latency, fill-to-publish latency, intent ingestion → submission latency, WS subscription confirmation success rate.
- No `/readyz` distinct from `/healthz`. `/healthz` always returns OK regardless of Kafka/DB/WS state.
- The Grafana dashboard JSON (`telemetry/grafana/provisioning/dashboards/dashboard.json`) is tracked but its content has not been audited here.

**Integration gaps with huginn / muninn.**
- Wire formats **match exactly**. Sleipnir's `exchange.Order` JSON and huginn's `kafka.GatewayOrder` (`/Users/lgreene/huginn/internal/kafka/producer.go:14–22`) align field-for-field. Same for `exchange.ExecutionFill` ↔ `kafka.GatewayFill` (`/Users/lgreene/huginn/internal/kafka/fills_consumer.go:14–22`). Topics `executions.intents.v1` / `executions.fills.v1` and default group `sleipnir-gateway` line up.
- ✅ **Naming hazard resolved.** Formerly `cmd/mock_muninn` — renamed to `cmd/mock_portfolio` so future readers don't conclude muninn (the JVM feature engine) ingests execution fills. The downstream consumer of fills is huginn; the mock represents huginn's portfolio-tracking side. Compose service is `mock-portfolio`; consumer group is `mock-portfolio-tracker`. Existing deployments must migrate the Kafka consumer-group offset (or accept a one-time replay from earliest).
- No schema versioning headers on Kafka messages. Topic name has `v1` but the payloads carry no `schema_version` field, so a future v2 must be a new topic.
- No idempotency key on `ExecutionFill`. Huginn's `OnExecutionFill` will double-count if a fill is republished after a sleipnir restart. The boot reconciliation backfill at `main.go:122–139` is exactly that scenario — partial fills observed before the crash will be re-emitted as a single `delta_qty` event that huginn cannot reconcile against what it already applied. **This is a real bug**, not a hypothetical.
- Huginn has no health probe contract with sleipnir. There is no way for huginn to know sleipnir is connected to the exchange before it starts publishing intents.

**Security concern that wants flagging.**
- `/Users/lgreene/sleipnir/.env` contains live-looking API key + secret strings. They are gitignored and **not** in the git history (verified). But they sit on disk in plaintext. Even if testnet-only, the SECURITY.md (Phase 2) should document `BINANCE_API_KEY`/`BINANCE_API_SECRET` handling, and the README should redirect first-time users to `export` them or use a secrets manager rather than committing an `.env`.

---

## Non-goals (declared up front)

To prevent scope drift:

- **Sleipnir does not pick which trades to make.** Strategy belongs to huginn. Sleipnir validates, throttles, executes, and reports.
- **Sleipnir does not aggregate PnL or maintain positions.** That is huginn's portfolio module.
- **Sleipnir is not a market data feed.** It does not publish ticks, book updates, or quotes. Muninn handles features; sleipnir handles execution. (The naming makes this worth saying out loud.)
- **Sleipnir does not run multiple exchanges concurrently in v1.** The `ExchangeConnector` interface keeps that door open; multi-venue routing is explicitly Phase F / deferred.
- **Sleipnir is not a smart order router.** No iceberg slicing, no VWAP/TWAP execution algorithms, no maker-taker preference logic. Single-venue passthrough only.
- **No live mainnet production trading.** Sleipnir is a research artifact. Mainnet credentials must never be checked in, and Phase 2 will state this in `SECURITY.md`.

---

## Phase 1 — Project foundations ✅

**Goal.** Give sleipnir the same baseline governance every other service in the stack has.

**Deliverables.**
- `README.md` — what sleipnir is, where it sits in the four-service stack, quickstart with `docker-compose up`, env vars, topic contracts, link to ROADMAP.
- `docs/ROADMAP.md` — this document.
- `docs/ARCHITECTURE.md` — single-page diagram of intent → risk → limiter → connector → exchange → WS fill → producer → huginn, plus the boot-reconciliation path.
- `docs/CONTRACTS.md` — frozen wire definitions for `Order` and `ExecutionFill`, cross-linked to `huginn/internal/kafka/producer.go` and `huginn/internal/kafka/fills_consumer.go` so a contract change in either repo is a coordinated event.
- `LICENSE` (Apache 2.0 to match muninn).
- `CONTRIBUTING.md`, `SECURITY.md` (with explicit "no mainnet keys, no committed `.env`" rule).
- `Makefile` — `make build`, `make test`, `make lint`, `make integration`, `make compose-up`, `make compose-down`.
- `.golangci.yml` enabling `govet`, `staticcheck`, `errcheck`, `gosec`, `revive`, `gofumpt`, `bodyclose`, `contextcheck`, `errorlint`.
- Pin `go.mod` to a real released Go (`1.25.7` is fine; just verify Dockerfile alignment).

**Exit criteria.** A first-time reader opens the repo, follows the README, and has a running gateway + mocks within 5 minutes. `make lint` and `make test` both pass with no warnings.

**Risks / open questions.**
- Apache 2.0 vs MIT — confirm with the rest of the stack.
- Should `docs/CONTRACTS.md` live in sleipnir, in huginn, or in a shared `docs-stack` repo? Recommend: in sleipnir (sleipnir owns the topic), with huginn's README linking to it.

---

## Phase 2 — CI, lint, and test hygiene ✅

**Goal.** Make every PR provably green before merge.

**Deliverables.**
- `.github/workflows/ci.yml` — matrix on `ubuntu-latest`, Go `1.25.x`. Steps: `go vet`, `golangci-lint`, `go test -race -coverprofile=cover.out ./...`, upload coverage.
- `.github/workflows/docker.yml` — build the multi-stage Dockerfile on every PR, push to GHCR on `main`.
- `gosec` step targeting `internal/exchange` (HMAC, secret handling).
- Coverage gate: fail PR if `internal/gateway` falls below 70 % or `internal/exchange` below 60 % (current connector code is hard to test without mocks — Phase 3 fixes that).
- Pre-existing tests must run with `-race` cleanly. The `OrderTracker` test at `gateway_test.go:109` is the canary.
- A Dependabot config covering `gomod` and `github-actions`.

**Exit criteria.** A red PR is impossible to merge. Coverage badge in README.

**Risks / open questions.**
- `kafka-go` test isolation — likely needs a kafka-go-backed Testcontainer or a stub Reader/Writer interface (Phase 4 problem).

---

## Phase 3 — Testability and the simulated connector ✅

**Goal.** Decouple the gateway loop from Binance so the full intent→fill cycle can be exercised in unit tests.

**Deliverables.**
- Promote the consumer/producer from concrete `*kafka.Consumer` / `*kafka.Producer` to small interfaces (`IntentConsumer`, `FillPublisher`) in a new `internal/gateway/ports.go`. The gateway loop already depends only on `FetchIntent` / `Commit` / `PublishFill`; this is a four-line refactor.
- `internal/exchange/simulator.go` — an `ExchangeConnector` implementation backed by an in-memory order book matcher and a `Now()` clock injection. Latency, slippage bps, and rejection probability are configurable. This is what end-to-end gateway tests and the bundled demo should use, **not** Binance testnet.
- `cmd/sleipnir` gains a `--exchange={binance,sim}` flag (env `EXCHANGE_BACKEND`).
- A new `gateway_e2e_test.go` that wires the gateway with mock consumer, mock producer, and the simulator, asserts the full lifecycle including a forced partial fill and a forced rejection.
- Fix the `UpdateOrderState → filled_qty=0` regression noted in the assessment by routing all state-with-qty transitions through `UpdateOrderStateAndQty`.
- Fix `Producer.PublishFill` and `gateway.go:180–190` dedup: introduce a `fill.ExecutionID` field derived from `(orderID, exchangeTradeID)` and skip republishing fills already seen by `OrderTracker` (Phase 5 finalizes the cross-service idempotency contract).

**Exit criteria.** A new contributor can `go test ./...` with no env vars, no docker, no network, and see the full gateway path covered.

**Risks / open questions.**
- The interface promotion may pull in segmentio's `kafka.Message` type at the interface boundary — acceptable, but consider a thin `Message{Offset, Key, Value}` wrapper.

---

## Phase 4 — Integration tests against real Redpanda and Binance testnet ✅

**Goal.** Verify that the wire actually works, not just that the code compiles.

**Deliverables.**
- ✅ `internal/gateway/integration_test.go` (build tag `integration`) — uses Testcontainers-Go to spin a Redpanda container, wires the gateway with the real `kafka.Consumer`/`kafka.Producer` against the simulator connector, seeds intents on `executions.intents.v1` and asserts matching fills round-trip on `executions.fills.v1` with non-empty `ExecutionID`s.
- ✅ `internal/exchange/binance_live_test.go` (build tag `binance_live`) — submits a tiny LIMIT order with an impossible price against Binance testnet, asserts `StateSubmitted`, then cancels. Reads creds from env, skips silently if absent.
- ✅ A separate CI job `integration-tests.yml` runs only the `integration` build tag (not `binance_live`). The `binance_live` tag runs on a nightly schedule with secrets injected from GitHub.
- ✅ Add a contract test that decodes a recorded huginn `GatewayOrder` JSON blob (committed under `testdata/huginn_intent_v1.json`) into `exchange.Order` and back. Catches accidental field renames in either repo.

**Exit criteria.** Nightly green run against testnet. A field rename in `huginn/internal/kafka/producer.go:GatewayOrder` breaks sleipnir's CI within 24 hours.

**Risks / open questions.**
- Binance testnet rate limits and rotating maintenance windows — pin the nightly job to retry-with-backoff before failing.
- API key rotation hygiene for CI — document in SECURITY.md.

---

## Phase 5 — Correctness hardening: idempotency, partial fills, reconciliation ✅

**Goal.** Eliminate the double-counting fill bug and make the gateway crash-safe at message granularity.

**Deliverables.**
- Add `ExecutionID string` to `exchange.ExecutionFill` and the Kafka payload. For Binance: `executionID = clientOrderID + "-" + tradeID` from the WS `t` field (trade id) and the REST response's `fills[].tradeId` array. For the simulator: deterministic counter.
- Coordinate with huginn: `huginn/internal/kafka/fills_consumer.go:GatewayFill` adds `ExecutionID`, and `executor.OnExecutionFill` keeps an LRU of seen IDs to drop dupes. This is a cross-repo PR pair that must land together — document the lockstep in `docs/CONTRACTS.md`.
- Rewrite the boot reconciliation path in `cmd/sleipnir/main.go:92–146` so the synthesized backfill fill uses the exchange-reported transaction time and a stable `ExecutionID = orderID + "-reconcile-" + filledQty`, not `time.Now()`.
- Fix partial fills: `gateway.go:91` currently transitions to `StateFilled` on **any** WS fill. Use the WS `X` (order status) field and only mark `StateFilled` when Binance says `FILLED`; otherwise `StatePartiallyFilled`. The fill event is published regardless.
- Persist filled quantity correctly through `UpdateOrderStateAndQty`; drop the buggy zero-write branch in `tracker.go:60–71`.
- Either remove the dead `commission` / `slippage` migration columns or actually write to them (recommend: write them — partial-fill realized slippage is useful for huginn's research path).
- Add a state-transition guard: `StateFilled → *` and `StateCanceled → *` are illegal and logged at WARN level.

**Exit criteria.** A partial-fill scenario in the simulator produces N distinct fill events with distinct `ExecutionID`s, all visible to a mock huginn consumer, none double-applied. Killing sleipnir mid-stream and restarting it re-issues only fills that have `ExecutionID`s the consumer has not seen.

**Risks / open questions.**
- Binance trade IDs are int64 per-symbol — confirm uniqueness assumptions.
- LRU cache eviction window on the huginn side — needs sizing.

---

## Phase 6 — Risk and ops controls ✅

**Goal.** Move from "demo-grade" risk to "would survive an unsupervised weekend on testnet".

**Deliverables.**
- Replace the hardcoded BTC/ETH instrument check in `gateway.go:206–214` with a config-driven `map[instrument]InstrumentLimits` loaded from a `risk.yaml` file (max qty, max notional, min qty, price collar bps vs. last fill).
- Notional limit (price × qty) in addition to size — catches a fat-finger 10000 USDT MARKET BUY of a sub-dollar token.
- Per-side daily count limits (`MAX_DAILY_BUYS`, `MAX_DAILY_SELLS`) on top of the existing `MAX_DAILY_ORDERS`.
- A kill-switch endpoint `POST /admin/halt` that flips an in-memory flag — gateway rejects all new intents with reason `halted`. _Halt state is intentionally reset on restart_: operators who restart a process in a halted state must re-issue the halt explicitly. The original deliverable specified a `halt_flag` SQLite table, but persistent halt was determined to be operationally confusing — a restarted process that auto-halts silently would be harder to diagnose than one that starts clean and immediately receives a new halt command. This design decision is documented in `internal/gateway/risk.go`.
- `GET /readyz` distinct from `/healthz`. Returns 200 only when: SQLite reachable, Kafka brokers reachable, WS user stream subscribed within the last 60 s. The mock containers and any future huginn health-check use `/readyz`, not `/healthz`.
- Token-bucket fix: only increment `telemetry.RateLimitDelay` when the actual wait > 0.

**Exit criteria.** A pathological intent stream (oversize, fat-finger, repeated rejections, broker disconnect) does not produce a single un-tracked submission to the exchange.

**Risks / open questions.**
- Halt-state semantics during boot reconciliation — should reconciliation honor the halt flag? Recommend: yes, log loudly, do not auto-resume.

---

## Phase 7 — Observability completeness ✅

**Goal.** Be able to debug a problem at 3 AM without a debugger.

**Deliverables.**
- ✅ **OpenTelemetry trace spans.** `intent.consume → risk.check → limiter.wait → exchange.submit → fill.publish` wired in `internal/gateway/gateway.go` via `internal/tracing`. W3C TraceContext extracted from Kafka intent headers so spans link to huginn's `PublishIntent` span end-to-end. Local Tempo collector available under `--profile tracing` in docker-compose.
- ✅ **New metrics.** `sleipnir_active_orders` (gauge, incremented on accept/decremented on terminal state), `sleipnir_intent_to_submit_seconds` (histogram covering risk + rate-limiter + submit), `sleipnir_fill_to_publish_seconds` (histogram from WS receive to Kafka publish), `sleipnir_ws_connected` (gauge, 1=subscribed/0=disconnected). `sleipnir_kafka_consumer_lag` and `sleipnir_db_query_seconds` deferred (require polling infrastructure).
- ✅ **Correlation ID.** `correlation_id` UUID generated at intent-consume time; threaded through every `slog` log line that touches the same order lifecycle.
- ✅ **Operational alerts.** `WSDisconnectedFor1Min` (fires when `sleipnir_ws_connected == 0` for > 1 min) and `NoIntentsConsumed30Min` (fires when no intents consumed in a 30-minute window). `KafkaConsumerLagHigh` deferred — depends on `sleipnir_kafka_consumer_lag` which requires polling infrastructure not yet wired.
- ✅ **Grafana dashboard panels.** Added "Operational Health" row (Active Orders gauge, WS Connection status, Daily Orders Consumed 24 h) and "Pipeline Latency" row (Intent-to-Submit p50/p95, Fill-to-Publish p50/p95) backed by the new Phase 7 metrics.
- ✅ **Jsonnet source for Grafana dashboard.** `telemetry/dashboards/src/sleipnir.jsonnet` is the human-readable source of truth. `make dashboard-regen` (requires `brew install jsonnet`) compiles it to the provisioning JSON. Dashboard diffs are now reviewable — edit the `.jsonnet`, not the raw JSON.

**Exit criteria.** A simulated WS outage produces an alert within 1 minute. A trace in any compatible viewer (Jaeger, Tempo) shows a single intent traversing huginn → sleipnir → Binance → huginn end-to-end.

**Risks / open questions.**
- OTel collector deployment in the compose stack — adds a service. Acceptable.

---

## Phase 8 — Release engineering and stable v0.1.0 ✅

**Goal.** Make sleipnir installable, versioned, and reproducibly buildable.

**Deliverables.**
- ✅ **`CHANGELOG.md`** following Keep a Changelog, covering v0.1.0 through current.
- ✅ **`release.yml`** — tag push (v*.*.*) triggers multi-arch Docker build (`linux/amd64` + `linux/arm64`), pushes to GHCR, creates a GitHub Release with image pull instructions. Semver tagging starts at `v0.1.0`.
- ✅ **`.goreleaser.yaml`** for binary archives — builds `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`; attaches tar.gz + checksums to the GitHub Release. `release.yml` runs goreleaser after the Docker push.
- ✅ **Pin all top-level dependencies to exact versions.** All direct deps use semver tags; `go mod verify` passes clean; `go mod tidy` updated `go.yaml.in/yaml/v2` from indirect→direct.
- ✅ **Dockerfile base images locked by digest.** `golang:1.26-alpine@sha256:...` and `alpine:3.23@sha256:...` pinned; `VERSION`/`GIT_SHA`/`BUILD_TIME` build-args inject version via `-ldflags`. `internal/version` package exposes them at `/version` endpoint and in the startup log.
- ✅ **`docs/RUNBOOK.md`** — WS reconnecting, Kafka lag climbing, SQLite growth, fills not arriving, key metrics table, graceful restart, config reload guidance.

**Exit criteria.** `docker pull ghcr.io/lgreene/sleipnir:v0.1.0` works. A blank machine can reproduce the binary byte-for-byte from a tag.

**Risks / open questions.**
- cosign keyless via OIDC — match whatever muninn uses.

---

## Phase F — Deferred (no schedule)

These belong on the roadmap so they stop showing up as "should we do this now?" in PR reviews. They are not on a clock — each is gated by an **observable trigger** (never a date) catalogued in [TRIGGERS.md](TRIGGERS.md). When a trigger trips, the item moves out of Phase F into the next numbered phase, marked 🟢 with the trigger ID.

- **Second venue.** A Coinbase Advanced Trade or Kraken connector. Forces honest tests of the `ExchangeConnector` interface and exposes the multi-venue order-id collision problem. _Gated by **T4** (a needed instrument is unavailable on the current venue, or a 2nd-venue testnet key is provisioned)._
- **Smart order routing.** Iceberg slicing, child-order management, parent/child tracking schema. _Gated by **T5** (orders start moving the market — slippage > ~5–10 bps or heavy partial-fill fragmentation)._
- **gRPC admin API** for huginn to query order status synchronously (today, huginn only learns about state via the fills topic). _Gated by **T6** (a huginn strategy decision needs synchronous order status)._
- **Postgres backend** as an alternative to SQLite, behind the existing `OrderStore` interface. Only worth doing if multi-instance sleipnir becomes necessary. _Gated by **T7** (single-instance sleipnir becomes a throughput/HA bottleneck)._
- **Replay mode** — re-emit historical fills from SQLite onto a `executions.fills.replay.v1` topic for huginn backtests. Would tie nicely into muninn's deterministic replay strategy doc. _Gated by **T8** (a huginn backtest fidelity gap is traced to synthetic fills)._
- **WS consumer for muninn streaming features** when the server adds a streaming endpoint. _Gated by **T3** (muninn ships a streaming features endpoint)._
- **Mainnet operation.** Explicitly out of scope until a separate security review phase. _Gated by **T9** (the go-live gate: ≥8 weeks clean paper trading + named human sign-off); opens a new **Phase 9 — Mainnet readiness** rather than promoting a single line._

---

## Phase ordering rationale

- **Phase 1 first** because every other service in this stack has README + ROADMAP + LICENSE + CONTRIBUTING + SECURITY and sleipnir does not. Ergonomics for the next contributor outranks any new feature.
- **Phases 2–3 before 4–5** because hardening the contract with huginn (Phase 5) is a cross-repo PR pair, and you do not want to land that on top of an untested codebase. Lint + simulator come first.
- **Phase 5 (correctness) before Phase 6 (risk)** because the double-counted-fill bug is a real defect today; adding more risk controls on a wrong substrate is sandcastle work.
- **Phase 7 (observability) before Phase 8 (release)** because v0.1.0 should not ship without the operational metrics needed to know if it's healthy.
- **Phase F is deferred** by name. It does not get scheduled until Phase 8 ships.
