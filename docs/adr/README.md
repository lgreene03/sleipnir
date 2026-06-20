# Architecture Decision Records

Short, append-only records of the load-bearing decisions behind sleipnir's
execution gateway. Each ADR captures the **Context** (the forces in play), the
**Decision** (what we chose), and the **Consequences** (what we now live with —
good and bad). They are grounded in the code as it stands; file/line references
point at the implementation.

ADRs are immutable once accepted. To revisit a decision, add a new ADR that
supersedes the old one rather than editing history.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-pluggable-exchange-connector.md) | Pluggable `ExchangeConnector` (binance vs sim) | Accepted |
| [0002](0002-token-bucket-rate-limiting.md) | Token-bucket outbound rate limiting | Accepted |
| [0003](0003-sqlite-order-store-and-reconciliation.md) | SQLite order store + boot-time reconciliation | Accepted |
| [0004](0004-idempotent-fill-backfill-execution-id.md) | Idempotent fill backfill with a stable `ExecutionID` | Accepted |
| [0005](0005-risk-yaml-policy-vs-legacy-caps.md) | `risk.yaml` policy vs. legacy hardcoded caps | Accepted |
| [0006](0006-otel-trace-propagation-kafka-headers.md) | OTel trace propagation through Kafka headers | Accepted |
| [0007](0007-operator-halt-and-orderid-validation.md) | In-process operator halt + OrderID validation | Accepted |

> Note: sleipnir's roadmap and config also reference **ADR-0009**. That record
> lives in the **muninn** repo (`muninn/docs/adr/0009-streaming-features-sse.md`)
> and governs the SSE feature stream sleipnir *consumes*; it is not a sleipnir
> ADR. The numbering here is local to this repo.
