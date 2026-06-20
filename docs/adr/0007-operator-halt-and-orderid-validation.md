# ADR-0007: In-process operator halt + OrderID validation

**Status:** Accepted

## Context

Two distinct safety needs sit on the intent hot path:

1. **A kill switch.** An operator watching a misbehaving strategy needs to stop
   *all* outbound submissions immediately, without redeploying or draining
   Kafka.
2. **Untrusted intent identifiers.** The gateway is the sole writer of Binance's
   `newClientOrderId`. Audit **finding H4** showed that an actor with publish
   rights to the intents topic could inject Binance-reserved characters
   (causing a post-rate-limit exchange rejection — a cheap fee-burn / DoS) or
   collide on an existing OrderID to overwrite in-memory tracker state.

Both must be cheap to evaluate and must short-circuit *before* a token is spent
or a signed request is built.

## Decision

**Operator halt.** A small in-memory `Halt` (`internal/gateway/risk.go:146`)
guarded by an RWMutex, flipped via `/admin/halt` HTTP endpoints. The gateway
checks `IsHalted()` first thing on every intent (`gateway.go:232`), before
validation, risk, or rate limiting — a halted gateway records the intent as
`StateRejected`, commits the offset, and moves on. The halt is **process-scoped
and resets to `false` on restart** by design (`risk.go:144`): a restart should
be visible to operators, not silently re-arm a halt.

**OrderID validation.** `ValidateOrderID` (`risk.go:217`) enforces a
conservative `[A-Za-z0-9_-]` character class and a 64-char bound
(`MaxOrderIDLen`, `risk.go:204`) before the id reaches the signed request or the
tracker. It runs right after the halt check (`gateway.go:255`). An invalid id is
the one rejection path that **deliberately does not** call `tracker.AddOrder`
(`gateway.go:264`) — writing the attacker-controlled id into the tracker is
itself the attack. A separate duplicate-active-OrderID check
(`gateway.go:273`) rejects collisions against ids the tracker already knows.

Order of the hot-path guards is fixed: halt → OrderID validation → duplicate
check → risk policy → rate limiter → submit. Each rejection commits the Kafka
offset to avoid poison-message loops and emits a stable
`sleipnir_risk_rejections_total{reason=...}` label.

## Consequences

- Operators get an instant, redeploy-free stop on all submissions; the cost is
  that the halt is per-process and not persisted, so a multi-replica or
  restart-during-incident scenario requires re-issuing the halt.
- H4 is closed: malformed or hostile OrderIDs are rejected fast, before a rate
  limiter token or a signed Binance request is spent, and never mutate tracker
  state.
- Every reject path commits the offset, so a single bad message can't wedge the
  consumer — at the cost that a rejected intent is *acknowledged*, not retried.
- The guards are ordered cheapest-and-most-decisive first (a halt or a bad id
  shouldn't pay for a DB-backed risk check), keeping the rejection path cheap.
- Reject reasons are stable, bounded-cardinality strings (`risk.go:191`) shared
  by metrics and trace attributes.
