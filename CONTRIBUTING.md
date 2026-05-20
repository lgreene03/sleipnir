# Contributing to Sleipnir

Thanks for your interest. Sleipnir is small, opinionated, and disciplined. Please read this short page before opening a PR.

## Before you start

- **Read [`docs/ROADMAP.md`](docs/ROADMAP.md).** It defines the order of work and the non-goals. New features outside the roadmap need a discussion issue first.
- **Read [`docs/SECURITY_AUDIT.md`](docs/SECURITY_AUDIT.md)** before touching anything in `internal/exchange/`, `internal/gateway/`, or `internal/config/`. Fixes that reference an audit finding ID (e.g. `Fixes C3`) are easier to review.
- **Read [`docs/CONTRACTS.md`](docs/CONTRACTS.md)** before changing any Kafka payload field. Wire-format changes are coordinated cross-repo PR pairs with huginn.

## How to work

1. **One concern per PR.** A roadmap-phase deliverable, an audit finding, a bug fix, a doc update — one of those, not several.
2. **No new tracked binaries, secrets, or large data files.** The `.gitignore` and `.dockerignore` are deliberate.
3. **`go test -race ./...` is green before pushing.** If you change `internal/exchange`, add tests. If you can't (because of network / credentials), say so in the PR.
4. **Lint locally with `make lint`.** Don't let CI find a `golangci-lint` issue you could have seen first.
5. **Logs are structured.** Use `slog`, not `log.Printf`. Never log raw exchange payloads (see SECURITY.md).
6. **Errors carry context.** Wrap with `fmt.Errorf("...: %w", err)`. Don't swallow.

## Commit style

- Subject under 72 characters, imperative mood (`feat(gateway): add ExecutionID`).
- Body explains *why*. The diff explains *what*.
- Reference roadmap phases and audit findings by ID where relevant.
- Co-author trailers are welcome.

Mirror the patterns you see in the existing log: `git log --oneline -20`.

## PR review expectations

- A reviewer will look at: correctness, test coverage, security posture, observability impact, and roadmap alignment.
- Expect questions about non-goals — they are real, not decorative.
- Two approvals for any change to `internal/exchange/binance.go` (signs orders) or `internal/gateway/gateway.go` (validates intents).

## Areas where help is welcome

These are roadmap items where a focused PR would land cleanly:

- **Phase 3.** Promote consumer/producer to interfaces; add `internal/exchange/simulator.go`.
- **Phase 5.** Add `ExecutionID` end-to-end (cross-repo coordination required).
- **Phase 6.** Replace hardcoded BTC/ETH risk caps with a config-driven `risk.yaml`. Addresses audit C3.
- **Phase 7.** Add OTel trace propagation through Kafka headers.

## Style references

- Effective Go: <https://go.dev/doc/effective_go>
- Uber Go style guide: <https://github.com/uber-go/guide/blob/master/style.md>
- The existing code beats both of the above when there's a conflict.

## Questions?

Open a Discussion, not an Issue, for design questions. Open an Issue for bugs you can reproduce. Reach out privately for security concerns (see SECURITY.md).
