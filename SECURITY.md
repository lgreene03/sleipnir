# Security Policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security reports. Email the maintainer at `security@<domain>` (replace with your contact) with:

- a description of the issue,
- the affected file path(s) and commit SHA,
- steps to reproduce, and
- the impact you believe it has.

You can expect an initial acknowledgement within 72 hours. Fixes for confirmed issues will be coordinated with you before public disclosure.

## Threat model

Sleipnir handles **exchange credentials** and **signs financial orders**. Treat every code path that touches the API key/secret, signs an HTTP request, or submits a Kafka intent as security-sensitive.

The full standing audit lives in [`docs/SECURITY_AUDIT.md`](docs/SECURITY_AUDIT.md). New issues should be triaged against that document; fixes should reference its finding IDs (e.g. `Fixes C3`).

## Secret-handling rules

These rules are not optional. They cover the most common ways exchange credentials leak.

1. **Never commit `.env` or any file containing `BINANCE_API_KEY` / `BINANCE_API_SECRET`** to this repo or any fork. `.env` is in `.gitignore` *and* `.dockerignore`. If you find a secret in git history, treat it as compromised — rotate immediately.
2. **Never paste credentials into a PR description, a CI log, or a Grafana panel screenshot.** CI logs are public on this repo.
3. **Never log raw exchange payloads.** Binance WS API responses can echo your `apiKey` in subscription acks. Whitelist fields before logging. See finding **H1** in the audit.
4. **Mainnet credentials are out of scope.** Sleipnir is testnet-only. The roadmap is explicit about this. Do not configure `BINANCE_REST_URL` to a mainnet host.
5. **`.env` permissions should be `0600`** on developer machines. The codebase does not enforce this; you do.
6. **Secrets in containers must come from runtime injection** (Docker secrets, Kubernetes `Secret`, env vars set by the orchestrator) — not baked into the image. The `.dockerignore` excludes `.env` from build context to prevent C1-style leaks into builder layers.

## Dependency hygiene

- Dependabot is configured under `.github/dependabot.yml` (Phase 2 of the roadmap) for `gomod` and `github-actions`.
- `gosec` runs in CI against `internal/exchange` (Phase 2) — HMAC and credential paths.
- Pinned image digests for the final `alpine` stage are a Phase 8 deliverable. Track CVEs against the current floating tag in the meantime.

## What we do not promise

- We do not provide a bug bounty.
- We do not promise CVE coordination for low-severity issues.
- Sleipnir is a research artifact. **Do not operate mainnet with it.** See `docs/ROADMAP.md` Phase F.
