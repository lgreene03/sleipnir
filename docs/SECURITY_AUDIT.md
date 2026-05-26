# Sleipnir Binance Connector — Security Audit

_Date: 2026-05-19. Scope: `internal/exchange/binance.go`, `internal/gateway/`, `internal/config/`, `cmd/sleipnir/`, `Dockerfile`, `.env` handling._

## Critical

**C1. `.env` leaks into Docker builder image layer**
`Dockerfile:14` (`COPY . .`) + missing `.dockerignore`. `.env` containing `BINANCE_API_SECRET` is baked into the builder stage. Even though the final `alpine` stage only copies binaries, anyone with access to the build cache, intermediate layers, or a registry that retains the builder tag recovers plaintext API secrets.
*Fix:* add `.dockerignore` with `.env`, `*.env`, `data/`, `.git`. Inject secrets at runtime only (Docker secrets / K8s `Secret`).

**C2. Plaintext secrets on disk in `.env`**
`.env` is gitignored but lives unencrypted on developer/host filesystems. Any local RCE, stolen laptop, or backup snapshot exfiltrates spot-trading keys. Binance Spot Testnet keys are low-impact, but the same code path will run with mainnet keys.
*Fix:* prefer Vault/AWS Secrets Manager/Doppler. At minimum, document the threat and enforce file mode 0600.

**C3. Risk check is effectively bypassed for any non-BTC/ETH instrument**
`internal/gateway/gateway.go:206-214`. The size cap is hardcoded to two symbols; any other instrument (e.g. `SOL-USD`, `LTCUSDT`, or an attacker-crafted `BTCUSD` which doesn't match either branch literally) falls through with no quantity limit. Combined with `MaxDailyOrders=500` per day, an attacker with Kafka publish rights to `executions.intents.v1` can drain the account by submitting one massive `SOLUSDT` order. Note also: `BTC-USD` and `BTCUSDT` are both checked, but `intent.Instrument` is whatever the upstream chose to send — `"btc-usd"` (lowercase) or `"BTC/USD"` slip past.
*Fix:* deny-by-default — require an explicit per-symbol cap in config, reject unknown instruments. Normalize case before compare. (Tracked in Phase 6 of the roadmap; promote to Phase 5 given severity.)

## High

**H1. Raw exchange payload echoed to logs may include `apiKey`**
`internal/exchange/binance.go:428` logs the full WS response with `"raw", string(msg)`. Binance WS-API subscription acknowledgements often echo the submitted `params` (including `apiKey`). The log line runs at `Debug`, but if log level is ever raised the **API key** lands in JSON logs / SIEM / OTel collector. Same risk on line 443 for `executionReport` (less sensitive but still account-identifying).
*Fix:* never log raw exchange payloads; whitelist fields.

**H2. HMAC signing is correct but fragile**
`internal/exchange/binance.go:86` uses `params.Encode()` which sorts alphabetically (Go stdlib guarantee) — OK. However `signature` is appended *after* `params.Encode()` with raw `%s` interpolation (line 88); if a future change adds a param via `params.Set` after signing, signature breaks silently. Also: `recvWindow=5000` is hardcoded; no clock-skew detection, no retry on `-1021` (timestamp outside recvWindow) — under NTP drift this becomes a DoS, and aggressive retry could trigger Binance IP ban.

**H3. Time-of-check / time-of-use between risk check and submit**
`gateway.go:128` → `gateway.go:159`. Between `checkRiskLimits` (which reads `GetDailyOrderCount`) and `SubmitOrder`, no lock is held. Concurrent intents (the loop is single-goroutine today but `wg.Add` patterns + future parallelism break this) can both pass the `count >= maxDailyOrders` check and exceed the cap. Also `tracker.AddOrder(... StatePending)` happens *after* the rate limiter — count isn't incremented atomically with check.
*Fix:* atomic check-and-increment inside the store (SQL `INSERT ... WHERE (SELECT count) < N`).

**H4. ~~`newClientOrderId` uses caller-supplied `OrderID` with no validation~~ — RESOLVED 2026-05-26**
`binance.go:119`. Whatever Kafka publishes lands directly in the signed request. An attacker who controls the topic can inject collisions to overwrite tracker state (`tracker.AddOrder` keyed by OrderID at `gateway.go:156`), or inject Binance-reserved characters causing the order to be rejected after the rate-limit token is spent (cheap DoS / fee burn).
*Fix:* validate against `^[A-Za-z0-9_-]{1,36}$`, reject collisions in tracker before submit.
*Resolution.* `gateway.ValidateOrderID` (`internal/gateway/risk.go`) rejects empty / over-length (>64 char) / disallowed-character OrderIDs with stable telemetry-friendly reason strings (`orderid_empty`, `orderid_too_long`, `orderid_invalid_char`). A duplicate-OrderID check in the gateway dispatch path (`internal/gateway/gateway.go`) rejects any OrderID already present in the tracker, blocking the state-overwrite vector. The 36 → 64 length cap is a deliberate accommodation: huginn's `huginn-live-order-<nanos>-<n>` form runs ~39 chars today, well within Binance's enforced limit; tightening further would have broken the working producer. Unit tests in `gateway_test.go::TestValidateOrderID` cover every branch; e2e tests in `gateway_e2e_test.go::TestGatewayE2E_OrderIDValidation` and `TestGatewayE2E_DuplicateOrderID` confirm rejected intents never reach the connector and never overwrite tracker state.

## Medium

**M1. WebSocket has no read size limit, no read deadline, no ping/pong handling**
`binance.go:309`. `websocket.DefaultDialer` + raw `conn.ReadMessage` with no `conn.SetReadLimit(...)`, `SetReadDeadline`, or pong handler. A malicious/MITM endpoint (or a Binance bug) can send a multi-GB frame → OOM. A silent half-open TCP connection will never reconnect because no read deadline ever fires. `wss://` scheme is enforced via config default only — not validated.
*Fix:* `conn.SetReadLimit(1<<20)`, `SetReadDeadline` refreshed in pong handler, send periodic pings, and assert `strings.HasPrefix(wsURL, "wss://")` at boot.

**M2. JSON unmarshal into `map[string]interface{}` without size guard** (`binance.go:421`). Combined with M1, hostile server can OOM. Type assertions with the `, _` pattern silently drop mismatches at line 429.

**M3. `math/rand` for jitter — process-global, unseeded** (`binance.go:313, 362, 399`). Not a vuln, but flag.

**M4. SQLite path defaults to `/app/data/sleipnir.db` with no file-mode hardening** (`main.go:54-57`). Order history is world-readable in container.

**M5. Health/metrics server has no auth** (`main.go:163`). `/metrics` exposes Prometheus counters that include the `instrument` label; `/telemetry` exposes active order counts.

## Low

- **L1–L4** Dependencies (`gorilla/websocket v1.5.3`, `segmentio/kafka-go v0.4.51`, `modernc.org/sqlite v1.50.1`, `envconfig v1.4.0`) — clean, no known open CVEs at audit date.
- **L5** `recvWindow=5000ms` is conservative-correct, but no logging when Binance clock-skew rejections happen.
- **L6** Reconciliation backfill (`main.go:122-140`) synthesizes a fill with `Timestamp: time.Now()` and `TransactionCost: 0.0`. An attacker who induces a crash window can manipulate downstream PnL by timing it across a real fill. Use exchange-reported timestamp. (Also called out in Phase 5 of the roadmap.)

## Info / Out of scope

- **Cert pinning** for `testnet.binance.vision` — not standard practice for public exchange APIs; rely on system CA bundle (Dockerfile correctly installs `ca-certificates`).
- **No HMAC timing-safe compare** — N/A; signature is *generated* not verified locally.
- **`crypto/hmac` + `crypto/sha256`** — correct, constant-time internally for signing.
- **Goroutine leak surface**: WS reader goroutine in `StartUserStream` correctly checks `ctx.Done()` on every loop. Clean.
- **Panic safety**: `defer recover()` is absent in the WS goroutine. A panic from any hostile-input edge case crashes the whole process. Add a top-level `defer recover()` that re-enters the reconnect loop.

## Things checked and found clean

- `crypto/hmac` usage (line 73) — correct construction
- Query param ordering — Go `url.Values.Encode()` sorts alphabetically, matches Binance spec
- `signature` appended *after* encoded query (line 88) — won't get re-encoded
- `wss://` default in config (`config.go:19`)
- Static binary build with stripped symbols (`Dockerfile:18`)
- Graceful shutdown propagation via context (`main.go:46, 175-196`)
- `defer resp.Body.Close()` is present on all three REST paths
- No `InsecureSkipVerify` anywhere
- No use of `fmt.Errorf` that interpolates `apiSecret`
- Test `TestBinanceSignature` uses Binance's published reference vector — signing is byte-correct

## Key untested paths

- Error paths in `SubmitOrder` / `CancelOrder` / `GetOrderState` (non-200, malformed JSON, network error)
- `StartUserStream` reconnect / backoff logic
- Hostile WS payload handling (oversize, wrong types, missing fields)
- Concurrent `SubmitOrder` calls (mutex `bc.mu` is declared but never used — dead field)
- Risk-limit bypass for non-BTC/ETH symbols (no test in `gateway_test.go` either)
