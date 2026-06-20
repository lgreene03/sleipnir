# ADR-0005: `risk.yaml` policy vs. legacy hardcoded caps

**Status:** Accepted

## Context

Pre-trade risk originally lived as hardcoded BTC/ETH branches: per-instrument
quantity caps wired straight from `MAX_ORDER_QTY_BTC` / `MAX_ORDER_QTY_ETH`. A
security audit flagged this as **finding C3 (Critical)**: any instrument that
wasn't BTC or ETH fell through *with no size cap at all*. A typo'd or malicious
intent on an unlisted symbol could submit an arbitrary quantity.

We needed a policy that (a) closes that hole by defaulting unknown instruments
to *deny*, (b) supports per-instrument quantity, notional, and minimum-size
limits without a code change per symbol, and (c) does **not** break existing
deployments that have no policy file yet.

## Decision

Introduce a YAML-driven `RiskPolicy` (`internal/gateway/risk.go:41`), loaded from
the path in `RISK_CONFIG_PATH` by `LoadRiskPolicy` (`risk.go:51`). It carries
per-instrument `InstrumentLimits` (`max_qty`, `max_notional`, `min_qty`) plus
`default_max_qty` / `default_max_notional` applied to instruments not in the
map. Instrument keys are normalized to uppercase on load (`risk.go:67`) so
`btc-usd` can't bypass `BTC-USD`. `CheckIntent` (`risk.go:113`) evaluates min
qty, max qty, then notional, returning a stable reason string used as a
Prometheus label and a span attribute.

In the YAML path, a zero `default_max_qty` means **deny unknown instruments** —
the safe default that closes C3.

Backwards compatibility is preserved with an explicit, gated fallback. When
`RISK_CONFIG_PATH` is empty, `LoadRiskPolicy` returns `(nil, nil)` and `main.go`
falls back to `NewLegacyRiskPolicy` (`risk.go:80`), which reconstructs exactly
the old BTC/ETH-only behaviour from the `MAX_ORDER_QTY_*` env vars — **including
the C3 bug** (unknown instruments unconstrained). This is deliberate
(`risk.go:82`): flipping the legacy path to deny-by-default would break operators
who haven't authored a `risk.yaml` yet. The new capability defaults to current
behaviour; the fix is opt-in by setting `RISK_CONFIG_PATH`.

The daily-count and per-side caps (`gateway.go:417`) are unchanged and layer on
top of the instrument policy.

## Consequences

- The risk model is now **data-driven and auditable**: adding or tightening an
  instrument is a config change, not a deploy. Notional and min-size limits
  exist that the legacy path never had.
- The C3 critical is *fixable* but only *fixed* once an operator points
  `RISK_CONFIG_PATH` at a real file. Until then the legacy fallback still has the
  hole — a conscious migration risk, documented in `risk.go` and the roadmap, in
  exchange for non-breaking rollout.
- Reason strings are stable and bounded-cardinality so they're safe as metric
  labels (`sleipnir_risk_rejections_total{reason=...}`).
- A `nil` policy is treated as "allow" inside `CheckIntent` (`risk.go:114`), so
  the daily-count check still runs; the legacy fallback is constructed precisely
  so the policy is never silently nil in production wiring.
