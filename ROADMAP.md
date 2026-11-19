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
- Go additionally tests explicit context cancellation. Python remains synchronous;
  decimals preserve exact numeric values, while Go also retains original spelling.

## Next: async REST and contract maintenance

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
