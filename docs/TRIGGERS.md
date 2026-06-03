# Norse stack — Trigger catalog

This document is the **canonical, cross-repo catalog of promotion triggers** for the four Norse
repos ([muninn](https://github.com/lgreene03/muninn),
[muninn-py](https://github.com/lgreene03/muninn-py),
[sleipnir](https://github.com/lgreene03/sleipnir),
[huginn](https://github.com/lgreene03/huginn)). It lives alongside
[CONTRACTS.md](CONTRACTS.md) because, like the wire contract, it is a shared agreement the four
repos coordinate against.

## Why this exists

Every repo has a **Phase F — Deferred** bucket. Deferred items are tracked so they stop resurfacing
as "should we do this now?" in PR review — but "deferred" with no exit condition is just
procrastination with a label. A trigger is an **observable real-world event**, never a date. When a
trigger trips, the item it gates moves out of Phase F into the next numbered phase in its repo,
marked `🟢` with the trigger ID on the deliverable line (e.g.
`🟢 Second venue — promoted by T4 (2026-07-xx)`). That makes it visible to `whats-next.sh` and
eligible for scheduled pickup — which, by design, it is **not** while it sits in Phase F.

Triggers are deliberately graded: presentation triggers (T1–T2) are free and trip on first external
eyes; the go-live gate (T9) is the hardest to trip and guards everything that risks real capital.

---

## Tier 0 — Presentation & distribution

### T1 · A persistent demo instance is stood up
**Signal.** The full stack runs continuously for ≥ 24 h (a cheap VPS via
`muninn/docker-compose.cloud-cheap.yml`, or an always-on local host) so the observability stack
accumulates real panel data.
**Promotes.**
- **muninn** Phase 7 — capture Grafana panel screenshots, replace the README placeholders.
- Provides the live backdrop required to record the screencast (T2).

### T2 · First external audience
**Signal.** A talk is accepted, a blog post is cross-posted to a community, the doc-site URL is
shared in a job application, or a second person needs to install the SDK.
**Promotes.**
- **muninn** Phase 7 — record the 5-minute screencast from `docs/demo/screencast-outline.md` →
  **Phase 7 closes**.
- **muninn-py** Phase E — one-time pypi.org Trusted-Publisher registration (`docs/RELEASING.md`),
  then `git tag v0.1.0 && git push origin v0.1.0` → **Phase E closes**.

> T1 + T2 are the only things blocking *full completion* of muninn and muninn-py. Both trip on
> "someone who isn't the author is about to look at this."

---

## Tier 1 — Cross-repo capability triggers

### T3 · muninn ships a streaming features endpoint
**Signal.** A server-side ADR lands and a WS/SSE features endpoint goes live.
**Promotes (cascade).**
- **muninn-py** Phase F — WebSocket streaming client.
- **huginn** Phase F — WS consumer for streaming features (replaces polling).
- **sleipnir** Phase F — WS consumer for muninn streaming features.

Highest-leverage trigger in the catalog: one server feature promotes an item in three downstream
repos at once.

### T4 · A needed instrument is unavailable on the current venue (or a 2nd-venue testnet key is provisioned)
**Signal.** A strategy targets a symbol the current venue's testnet doesn't list, **or** Coinbase
Advanced Trade / Kraken testnet credentials are provisioned to stress the interface.
**Promotes.**
- **sleipnir** Phase F — second `ExchangeConnector` → forces honest interface tests, surfaces the
  multi-venue order-id collision problem.
- Then **huginn** Phase F multi-venue support becomes reachable (depends on this).

### T8 · A huginn backtest fidelity gap is traced to synthetic fills
**Signal.** Phase 4 backtest parity diverges from live and the root cause is "we replay synthetic
fills, not real ones."
**Promotes.**
- **sleipnir** Phase F — replay mode: re-emit historical SQLite fills onto
  `executions.fills.replay.v1`.
- **huginn** Phase F — consume that topic for high-fidelity backtests.

---

## Tier 2 — Scaling triggers (trip on a measured limit, not a guess)

### T5 · Orders start moving the market
**Signal (measurable).** A single parent order's notional exceeds top-of-book depth, **or** observed
slippage on large orders exceeds ~5–10 bps, **or** orders routinely fragment into many partials.
**Promotes.** **sleipnir** Phase F — smart order routing (iceberg slicing, parent/child tracking
schema).

### T6 · huginn needs *synchronous* order status
**Signal.** A strategy decision depends on current order state at decision time, not
eventually-via-the-fills-topic.
**Promotes.** **sleipnir** Phase F — gRPC admin API for synchronous order-status queries.

### T7 · Single-instance sleipnir becomes the bottleneck
**Signal (measurable).** Sustained queue depth / throughput ceiling on one instance, **or** an
HA/failover requirement appears.
**Promotes.** **sleipnir** Phase F — Postgres backend behind the existing `OrderStore` interface
(multi-instance).

---

## Tier 3 — The go-live gate

### T9 · Decision to trade real capital
The hardest trigger to trip. It requires a **measurable precondition** *and* a **human commitment**.

**Precondition (all must hold):**
- ≥ 8 consecutive weeks of paper trading within configured risk limits;
- max drawdown stayed within the daily-loss / drawdown limits the whole window;
- zero unhandled risk halts (staleness / circuit-breaker fired only when it should);
- replay divergence = 0 over the window.

**Plus:** a named human signs off on risking real money.

**Promotes — opens new numbered phases (real work, not one-line items):**
- **sleipnir → new "Phase 9 — Mainnet readiness":** per-instrument kill-switches, real-money risk
  limits, security review, `SECURITY_AUDIT.md` mainnet section.
- **huginn → new "Phase 8 — Live trading":** two-person operational consent, real-money risk limits,
  dedicated incident-response runbook.
- Mainnet operation in both repos flips on **only after** both new phases are complete.

### T10 · ≥ 2 strategies run live simultaneously and manual capital split is the bottleneck
**Promotes.** **huginn** Phase F — cross-strategy meta-allocator (rolling-Sharpe capital allocation).
*Gated behind T9 — pointless before live.*

### T11 · Any nonzero live-vs-replay divergence is observed
**Promotes.** **huginn** Phase F — replay-divergence diagnostics (deterministic replay of a fills
journal + features, surface the delta). Also wanted the moment T9 fires, to audit live vs backtest.

---

## Tier 4 — Long-tail (real triggers, low probability)

| Trigger | Signal | Promotes |
|---|---|---|
| **T12** | Strategy iteration cadence high enough that restart downtime hurts | huginn — strategy hot-reload (`.so` plugin). *Likely never; low payoff over restart.* |
| **T13** | A browser dashboard is built that calls muninn directly | muninn-py — TypeScript client. |
| **T14** | Server is fronted by reverse-proxy auth in a shared/multi-user deployment | muninn-py — typed `MuninnClient(auth=...)` helpers. |
| **T15** | A researcher has a concrete analysis that *requires* per-exchange feature slicing | muninn-py — `source` filter. |

---

## Promotion mechanics

When a trigger trips:

1. Move the gated item out of its repo's **Phase F** into the next available numbered phase.
2. Mark the deliverable line `🟢` and annotate it with the trigger ID and the date it tripped, e.g.
   `🟢 Second venue (Kraken connector) — promoted by T4 (2026-07-14)`.
3. For T9, open the named new phases (`sleipnir` Phase 9, `huginn` Phase 8) rather than promoting a
   single line — the work is large enough to warrant its own phase with its own exit criteria.
4. Once promoted, the item is eligible for `whats-next.sh` and the nightly roadmap-advance task.
   While it remains in Phase F, it is intentionally invisible to scheduled work.

## Dependency spine

- **T1 + T2** are free and close two repos completely.
- **T3** (server streaming endpoint) is the single highest-leverage move — it lights up three repos.
- **T4** unblocks sleipnir's second venue, which in turn unblocks huginn multi-venue.
- **T9** is the gate everything risky hides behind, and it is correctly the hardest to trip.
