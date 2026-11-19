# Roadmap

The current baseline is locally tested against a pinned contract and synthetic
responses. Live interoperability remains unverified. This file is a development
plan, not a claim that the following features are already supported.

## Completed: read-only REST coverage (0.1.0a2)

Instrument conditions, candle history and existing operation status now complete
the nine read-only REST operations in the pinned contract, plus discovery.
Both SDKs share response fixtures and a signing vector. The upstream candle oneOf
ambiguity and the request-selected validation approach are documented in
`contracts/README.md`. Live interoperability remains a separate gate.

## Completed: generated inputs and local HTTPS verification

- Hypothesis tests cover decimal precision, key formats, HTTPS origins, URL encoding
  and cursor/pagination invariants using bounded reproducible examples.
- Real local TLS connections cover header/body timeouts, truncated responses,
  redirects, signature verification, connection reuse and closure.
- Python decimals preserve exact numeric values, while Go also retains original
  spelling. Both SDKs test explicit cancellation through their async/context APIs.

## Completed: asyncio REST (0.1.0a3)

- `AsyncExnessClient` covers the same read-only methods and lazy history iteration.
- Signing, parameter validation, response models and cursor rules are shared.
- Tests cover concurrent discovery, cancelled callers, cache refresh, async generator
  termination and real TLS timeout/cancellation/connection cleanup.

## Completed: contract maintenance

- An explicit check command compares the official YAML or a local candidate with
  the snapshot, reports structural changes and writes optional review artifacts.
- A manual GitHub workflow runs the same comparison. Snapshot adoption stays an
  explicit reviewed edit; changes require assessment in both SDKs.

## Completed: asyncio live ticks (0.1.0a4)

- Signed WSS handshakes and tick subscriptions through the account Access Point.
- Typed tick/error models, bounded buffering, cancellation and connection cleanup.
- Opt-in lifetime reconnect budgets, resubscription and explicit gap notifications.
- Local TLS tests cover disconnects, slow consumers, protocol failures and redirects.

## Completed: transactions stream (0.1.0a5)

- Typed snapshots and transaction deltas reuse the signed tick transport.
- Require a fresh snapshot before transactions on every connection; emit gaps on
  reconnect and document caller-owned cache replacement/history reconciliation.
- Handle balance changes, partial closes and rejected/failed processing results.
- Share contract fixtures with Go and verify recovery boundaries on local TLS servers.

## Next: remaining server events

- Account/instrument/HMR events with subscription-specific recovery semantics.
- Reuse bounded reconnect policy, cancellation, resubscription and observable gaps.
- Fake-server tests for malformed messages, disconnects and slow consumers.

## Release gates

- CI must pass on supported Python versions; published metadata must match the SDK.
- A future opt-in read-only live smoke test must redact credentials and be disabled
  by default. Lack of live testing must stay visible in release notes.
- Trading writes require a separate design for idempotency, ACK versus execution
  results and uncertain outcomes; do not add them as routine REST wrappers.
- Package registry publication and stable-version claims are separate steps from
  publishing source on GitHub.
