# Sleipnir — Operations Runbook

Day-two operational procedures for a running Sleipnir instance. For architecture, see `docs/ARCHITECTURE.md`. For deployment, see `docker-compose.yml`.

---

## Health and readiness

```bash
# Liveness probe — always returns 200 if the process is running
curl http://localhost:8082/healthz

# Readiness probe — returns 200 only after the first Kafka intent has been consumed
curl http://localhost:8082/readyz
```

---

## Operator halt/resume

Halt stops new order submissions immediately. In-flight WS fills continue to be broadcast.

```bash
# Halt — rejects all incoming intents
curl -X POST http://localhost:8082/halt

# Resume
curl -X POST http://localhost:8082/resume

# Check halt status via /healthz
curl http://localhost:8082/healthz | jq .halted
```

The halt is in-memory and resets on process restart.

---

## When the WebSocket keeps reconnecting

Symptoms: `sleipnir_ws_connection_drops_total` climbing; Grafana "WS uptime ratio" dropping; Binance returns 451 or 418.

1. Check Binance API status at `status.binance.com`.
2. Check for IP bans: `curl https://api.binance.com/api/v3/ping` — a 418 means your IP is banned (violated request-weight limits). Wait 30 s then retry.
3. If reconnecting too fast, increase `RECONNECT_BACKOFF_SECONDS` in `configs/default.yaml` (default: 5 s, capped at 60 s by exponential backoff).
4. Check `BINANCE_API_KEY` and `BINANCE_SECRET_KEY` are set and unexpired. A 401 on the listen-key `POST /api/v3/userDataStream` means the key is invalid or hasn't been granted trading permissions on the API management page.

---

## When Kafka consumer lag climbs

Symptoms: `sleipnir_kafka_messages_processed_total` rate falling; Huginn logs `"failed to publish live order intent"`.

1. Verify Redpanda/Kafka is healthy: `rpk topic describe executions.intents.v1`.
2. Check `sleipnir_intent_to_submit_seconds` p99 — if it's high, the Binance REST endpoint is slow. The token-bucket rate limiter may be backing up.
3. Restart Sleipnir if the consumer has stalled on a poison message (`go run . 2>&1 | grep "error"` to check). The offset will advance after restart because Sleipnir commits on every message.
4. If the topic has grown unbounded, check the retention policy on `executions.intents.v1` (default: 7 days).

---

## When the SQLite database grows

Sleipnir writes every order and fill to `data/orders.db`. On a high-order-rate deployment this file grows linearly.

```bash
# Current size
du -sh data/orders.db

# Trim completed orders older than 30 days (adjust as needed)
sqlite3 data/orders.db \
  "DELETE FROM orders WHERE state IN ('FILLED','CANCELED','REJECTED') \
   AND created_at < datetime('now', '-30 days');"

# Reclaim disk space after bulk delete
sqlite3 data/orders.db "VACUUM;"
```

Alternatively, rotate by stopping Sleipnir, renaming `data/orders.db`, and restarting with a fresh file. Historical orders are preserved in the archived database.

---

## When no fills are arriving

Symptoms: `sleipnir_orders_filled_total` flat; Huginn portfolio stalled.

1. Is the WS connected? Check `sleipnir_ws_connection_drops_total` rate and `/healthz`.
2. Are orders actually submitted? Check `sleipnir_orders_submitted_total` — if zero, Sleipnir isn't receiving intents (see Kafka lag section).
3. Are orders being rejected by risk? Check `sleipnir_risk_rejections_total{reason="*"}`. A sudden spike in `position_limit` or `daily_count` is normal near EOD.
4. Is the operator halt active? `curl /healthz | jq .halted`.

---

## Key metrics

| Metric | Alert threshold | What it means |
|--------|-----------------|---------------|
| `sleipnir_ws_connection_drops_total` rate | > 2/min for > 5 min | WS is cycling — investigate API key or IP ban |
| `sleipnir_active_orders` | > 50 for > 10 min | Orders stuck — WS may be disconnected |
| `sleipnir_intent_to_submit_seconds` p99 | > 1 s | Rate limiter or Binance REST is slow |
| `sleipnir_fill_to_publish_seconds` p99 | > 0.5 s | Kafka producer backpressure |
| `sleipnir_risk_rejections_total` rate | > 10/min | Risk limits hit — review position or daily count |

---

## Graceful restart

```bash
# SIGTERM triggers a graceful drain before exit
docker-compose restart sleipnir
```

On `SIGTERM`/`SIGINT`, Sleipnir performs a bounded **drain**: it stops pulling new
intents from Kafka, then waits up to `SHUTDOWN_DRAIN_TIMEOUT` (default `8s`) for
every in-flight order to reach a terminal state so late fills are published
before teardown. It logs `"starting graceful drain..."`, then either
`"Drain complete: all in-flight orders settled"` or, if the deadline is hit,
`"Drain deadline reached ..."` with the remaining count. Only then does it cancel
the workers, close Kafka, and close the SQLite store (the teardown now runs its
deferred cleanup rather than hard-exiting). Anything still in flight at the
deadline is recovered by the **boot-time reconciliation** on the next start,
which replays the SQLite store to rebuild the tracker and backfills any missed
fills. If you raise `SHUTDOWN_DRAIN_TIMEOUT`, raise the compose
`stop_grace_period` to match, so the orchestrator does not `SIGKILL` mid-drain.

---

## Config hot-reload

There is no hot-reload. To change limits (`max_daily_orders`, rate-limit tokens, instrument caps), update `configs/default.yaml` and restart. Changing `BINANCE_API_KEY` always requires a restart.
