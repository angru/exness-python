# Roadmap

The current baseline is locally tested against a pinned contract and synthetic
responses. Live interoperability remains unverified. This file is a development
plan, not a claim that the following features are already supported.

## Next: finish read-only REST coverage

- Instrument conditions: typed models and validation for precision, volume limits,
  execution settings and sessions.
- Candle history: documented timeframes, range/count rules and response models.
- Operation status: read existing operation results without submitting trades.
- Add shared response fixtures with the Go SDK for each supported operation.

Acceptance: documented operation IDs, fixtures checked against the pinned OpenAPI,
tests covering invalid inputs and actual transmitted paths, strict mypy, Ruff,
offline tests and installed-wheel checks all pass.

## Strengthen offline verification

- Property-based tests for decimal precision, URL encoding, key formats and cursors.
- A local HTTP test server to exercise transport failures and timeouts beyond mocks.
- Record cross-language behavior differences explicitly and add compatibility cases.
- An explicit contract-update command that reports schema changes for review; no
  silent specification replacement during builds.

## Async and streaming

- An async REST client sharing signing, validation and models with the sync client.
- WebSocket ticks first, then trading events, with bounded reconnect policy,
  cancellation, resubscription and observable gaps.
- Fake-server tests for malformed messages, disconnects and slow consumers.

## Release gates

- CI must pass on supported Python versions; published metadata must match the SDK.
- A future opt-in read-only live smoke test must redact credentials and be disabled
  by default. Lack of live testing must stay visible in release notes.
- Trading writes require a separate design for idempotency, ACK versus execution
  results and uncertain outcomes; do not add them as routine REST wrappers.
- Package registry publication and stable-version claims are separate steps from
  publishing source on GitHub.
