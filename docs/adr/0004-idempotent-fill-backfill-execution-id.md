# ADR-0004: Idempotent fill backfill with a stable `ExecutionID`

**Status:** Accepted

## Context

The same economic fill can reach the gateway — and therefore be published to
`executions.fills.v1` — through more than one path:

- the REST submit response (immediate fills on a MARKET order),
- the WebSocket user-data stream (real-time execution reports),
- the boot-time reconciliation loop (ADR-0003), which re-emits fills that
  happened while the process was down.

These paths overlap. After a restart, reconciliation may re-publish a fill that
the WS stream had already delivered before the crash. Downstream (Huginn's
executor) must be able to **drop the duplicate** rather than double-count it
against the portfolio. That requires a per-fill identity that is identical no
matter which path produced it and stable across restarts.

## Decision

Give every `ExecutionFill` a deterministic `ExecutionID`
(`internal/exchange/connector.go:69`), constructed from the client order ID plus
a path-specific, collision-free suffix:

- **REST submit:** `"<clientOrderID>-rest-<binance order id>"`
  (`internal/exchange/binance.go:197`).
- **WebSocket execution:** `"<clientOrderID>-ws-<binance trade id>"`
  (`binance.go:524`), with a `-ws-<transactTime>-<lastFilledQty>` fallback when
  the venue omits a trade id (`binance.go:528`).
- **Boot reconciliation:** `"<orderID>-reconcile-<deltaQty>"`
  (`cmd/sleipnir/main.go:180`).
- **Simulator:** `"<orderID>-sim-rest-<counter>"`
  (`internal/exchange/simulator.go:115`).

The reconciliation suffix is keyed on the **delta quantity** being backfilled,
so the same (order, missed-quantity) pair always yields the same `ExecutionID` —
re-running reconciliation after another restart produces a byte-identical event.
Downstream dedups on this id (an LRU keyed by `ExecutionID`), so a re-emitted
fill is a no-op. The cross-repo contract for this field is in
`docs/CONTRACTS.md`.

Partial fills are handled honestly alongside this: `ExecutionFill.OrderStatus`
(`connector.go:74`, a Phase 5 addition) carries the exchange-reported lifecycle
state, so a `PARTIALLY_FILLED` event keeps the order active instead of
collapsing into a single terminal `FILLED` (`gateway.go:143`). The reconciliation
backfill also stamps the exchange-reported `TransactTime` rather than
`time.Now()` (`main.go:171`, audit finding L6).

## Consequences

- The fill stream is **at-least-once with downstream idempotency**: publishing a
  fill twice is safe because consumers key on `ExecutionID`. This is what lets
  reconciliation re-emit freely without bespoke "have I sent this?" bookkeeping
  in sleipnir.
- Correctness depends on every producing path constructing the id by the
  documented rules. The patterns are centralized in the call sites above and
  spelled out in the `ExecutionFill` doc comment so they don't drift.
- The reconciliation id is keyed on *delta* quantity, which assumes the backfill
  delta is the right dedup key against the WS event for the same missed fill.
  Multiple distinct missed fills that happen to sum to the same delta would
  share an id; in practice reconciliation emits a single net delta per order per
  boot, so this is the intended granularity, not a per-trade ledger.
