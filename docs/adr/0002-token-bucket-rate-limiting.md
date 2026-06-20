# ADR-0002: Token-bucket outbound rate limiting

**Status:** Accepted

## Context

Binance Spot enforces request-weight limits and will reject (or temporarily ban)
a client that submits too fast. Intents arrive from Kafka in bursts — Huginn can
publish a batch of order intents back-to-back — but our outbound submission rate
must stay smooth and bounded regardless of input shape. We also wanted the
throttle to be observable (how long are we waiting?) and cancellable (a
shutdown shouldn't block on a token).

## Decision

Throttle outbound submissions with an in-process **token bucket**,
`TokenBucketLimiter` (`internal/gateway/tracker.go:162`). One token equals one
permitted submission. The bucket refills continuously at `RATE_LIMIT_RPS` tokens
per second (`RATE_LIMIT_RPS`, default `10.0`, `internal/config/config.go:25`),
capped at the same value so bursts can't exceed one second's worth of capacity.

`Wait(ctx)` (`tracker.go:181`) refills based on elapsed wall-clock time, consumes
a token when one is available, and otherwise sleeps exactly long enough for the
next token to accrue — looping until success or `ctx.Done()`. It records the
incurred delay into the `RateLimitDelay` telemetry counter via a deferred
closure (`tracker.go:183`).

The gateway calls the limiter on the hot path **after** risk checks pass but
**before** submitting (`gateway.go:313`), inside its own
`gateway.limiter_wait` span so the throttle is visible in traces.

## Consequences

- Outbound submission rate is bounded by a single tunable, independent of how
  bursty the intent stream is.
- The limiter is **per-process and in-memory**: it does not coordinate across
  multiple sleipnir replicas. Running N replicas against one Binance account
  means the effective rate is up to N×`RATE_LIMIT_RPS`. Today the gateway runs
  as a single instance per account, which keeps this correct; horizontal
  scaling would require a shared limiter (not built).
- Ordering of risk-then-limit is deliberate: a rejected intent must never burn a
  token, so risk and OrderID validation run first (`gateway.go:255`–`310`) and
  only survivors reach `limiter.Wait`.
- `Wait` honours context cancellation, so the throttle never blocks graceful
  shutdown.
- The bucket models a smooth rate, not Binance's exact weighted-endpoint
  accounting. It is a conservative client-side guard, not a mirror of the
  server's limiter — set `RATE_LIMIT_RPS` below the real ceiling.
