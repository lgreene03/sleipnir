# ADR-0003: SQLite order store + boot-time reconciliation

**Status:** Accepted

## Context

The gateway tracks order lifecycle in an in-memory `OrderTracker`
(`internal/gateway/tracker.go:13`). Memory alone is not enough:

- A crash or restart loses every in-flight order, so we'd have no record of what
  was submitted and no basis for the daily-count risk check.
- While the process was down, the exchange may have **filled** orders we never
  saw the WebSocket event for. On restart we must discover and account for those
  missed fills, not silently lose them.

We wanted durable state without a database server to operate, and without CGO
(the binary ships in a small container — see `Dockerfile`).

## Decision

Persist order lifecycle in an embedded **SQLite** database via
`SQLiteOrderStore` (`internal/gateway/store.go:26`), using the pure-Go,
CGO-free `modernc.org/sqlite` driver (`store.go:13`). Schema is created and
evolved by a small forward-only migration runner (`runMigrations`,
`store.go:66`) keyed on a `schema_migrations` table; each migration runs in its
own transaction. `DB_PATH` defaults to `/app/data/sleipnir.db`.

The store is the source of truth for:

- Active orders (`GetActiveOrders`, `store.go:178` — everything not in a
  terminal state).
- The daily-count risk inputs (`GetDailyOrderCount` /
  `GetDailyOrderCountBySide`, `store.go:224`/`238`).
- Realized transaction costs (`RecordFillCosts`, `store.go:256`).

On boot, two things happen in order:

1. `OrderTracker.WithStore` (`tracker.go:29`) preloads active orders from the
   store into memory so the tracker's view survives restarts.
2. A **boot-time reconciliation loop** (`cmd/sleipnir/main.go:133`) queries the
   live exchange via `GetOrderState` for every locally-active order, compares
   the exchange's filled quantity against the last persisted quantity, and (when
   the exchange is ahead) backfills the missed delta as a fill — see ADR-0004
   for the idempotency mechanism. It then syncs each order's state and quantity
   back into the tracker and store (`main.go:198`).

Reconciliation runs under a 30-second bounded context (`main.go:135`) so a slow
exchange can't wedge startup.

## Consequences

- State is durable across restarts with **zero operational overhead** — no DB
  server, no CGO toolchain.
- Restarts are self-healing for fills: anything the exchange completed while we
  were down is detected and emitted on boot, so downstream portfolio state
  converges.
- The store is **single-writer, single-node**. SQLite on a local volume does not
  support two sleipnir instances sharing one DB file. This reinforces the
  single-instance-per-account posture already implied by ADR-0002.
- Migrations are forward-only and append-only: schema changes are new entries in
  `dbMigrations` (`store.go:36`), never edits to shipped ones.
- The daily-count check reads from SQLite on the hot path, and a DB error is
  treated as **fail-closed** (`gateway.go:425` returns `db_unreachable` and
  rejects) — durability is bought with the rule that an unreachable store stops
  trading rather than trading blind.
