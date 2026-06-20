# ADR-0001: Pluggable `ExchangeConnector` (binance vs sim)

**Status:** Accepted

## Context

Sleipnir's core loop submits orders to a venue and consumes fills from it. We
needed two things at once:

- A production path against **Binance Spot** (signed REST submit + a WebSocket
  user-data stream for fills).
- A way to run the gateway — including `make test`, `scripts/smoke.sh`, and the
  cross-stack pipeline — with **no exchange credentials and no network**, so
  CI and local demos don't depend on Binance testnet availability.

If the gateway called the Binance client directly, every test would need keys
and a reachable testnet, and there would be no clean seam for a future second
venue.

## Decision

Define a single venue-agnostic interface, `exchange.ExchangeConnector`
(`internal/exchange/connector.go:91`), with four methods: `SubmitOrder`,
`CancelOrder`, `GetOrderState`, and `StartUserStream`. All venue-facing types
(`Order`, `ExecutionFill`, `OrderStatusResult`) live in the same package and are
shared by every implementation.

Two implementations satisfy it:

- `BinanceConnector` (`internal/exchange/binance.go`) — the production path.
- `SimulatedConnector` (`internal/exchange/simulator.go`) — an in-memory
  exchange that synthesizes fills.

Selection is a single env var resolved once at startup. `main.go:99` switches on
`EXCHANGE_BACKEND`: `""`/`binance` builds the Binance connector, `sim` builds
the simulator (and logs a loud warning that orders are not real), and anything
else exits non-zero. The default preserves the production path. The simulator
branch in `config.LoadConfig` (`internal/config/config.go:63`) is the only place
that relaxes the "API key required" check — sim mode needs no credentials.

## Consequences

- The gateway (`internal/gateway/gateway.go`) depends only on the interface; it
  never imports the Binance client. Tests inject fakes or the simulator freely.
- `EXCHANGE_BACKEND=sim` gives a credential-free, deterministic path used by the
  smoke test and e2e tests — Binance outages can't redden CI.
- The interface is deliberately **single-venue today**. The package doc
  (`connector.go:1`) and the roadmap mark multi-venue *routing* as deferred
  (Phase F): one process talks to one venue, chosen at boot. Adding a venue
  means a new implementation, not a routing layer.
- The interface is the cross-cutting contract: widening it (e.g. the Phase 5
  `TransactTime` on `OrderStatusResult`, `connector.go:84`) forces every
  implementation to keep up, which is the intended pressure.
