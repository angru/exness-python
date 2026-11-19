# Changelog

## 0.1.0a5 — 2026-11-19

- Add asyncio transactions streaming with typed snapshots, deltas and processing
  results. Require a fresh snapshot before transactions on each connection.
- Reuse signed tick transport, bounded reconnects, explicit gaps and cleanup;
  distinguish operation rejections from subscription errors.
- Add shared Go fixtures, offline contract checks, TLS recovery/cancellation tests,
  and a runnable example with caller-owned state-recovery guidance.
- Live API interoperability remains unverified. Account/instrument/HMR streams
  and trading writes are not implemented.

## 0.1.0a4 — 2026-11-19

- Add asyncio WebSocket ticks with signed GET handshakes, typed market-data models,
  subscription errors and terminal close errors. Add websockets 15.x as transport.
- Add explicit finite reconnect budgets, resubscription and gap notifications;
  default to no reconnects. Refuse redirects, disable environment proxies and
  transport header logging, bound messages/queues and propagate cancellation.
- Add shared tick/subscription/error fixtures and local TLS tests for disconnects,
  slow consumers, ping/pong, oversized messages and cancellation. Live API remains
  unverified; server-event streaming and trading writes are not implemented.

- Add a typed repository maintenance command for official or offline OpenAPI
  comparison, with JSON reports, byte hashes, operation summaries and explicit
  review status. Never overwrite the pinned snapshot or existing output files.
- Add offline regression tests and a manual contract-review workflow. Runtime
  dependencies and SDK package version remain unchanged.

## 0.1.0a3 — 2026-11-19

- Add `AsyncExnessClient` for asyncio with all read-only REST methods, async
  context management and lazy async history generators.
- Share parameter validation, response decoding, discovery validation and cursor
  rules with the synchronous client; preserve its public method signatures.
- Serialize concurrent discovery, propagate task cancellation unchanged, and
  release the discovery lock after cancellation or errors. Cached reads continue
  during explicit refresh; unsuccessful refresh preserves the previous host.
- Exercise async wire signatures, concurrent calls, cancellation, transport cleanup
  and real TLS failures. Add a TaskGroup-based account/history example.

- Add reproducible Hypothesis tests for exact decimal values, key formats, origins,
  cursor encoding/signing and pagination termination.
- Test real loopback TLS connections for header/body timeouts, truncated responses,
  redirects, signatures, connection reuse and closure, without live credentials.
- Source release only; live interoperability and PyPI publication remain unverified.

## 0.1.0a2 — 2026-11-19

- Complete read-only REST coverage with instrument conditions, candle history and
  operation status. No automatic polling or trading writes.
- Validate candle query combinations and preserve bid/ask/both response shapes;
  market data uses numeric prices, instrument monetary fields use Decimal.
- Shared Python/Go fixtures for all new operations; document the upstream candle
  oneOf ambiguity and validate request-selected schema branches explicitly.
- Add a runnable market-data example. Live interoperability remains unverified.

## 0.1.0a1 — 2026-11-19

- Initial synchronous read-only Trading API client: discovery, account, instruments,
  snapshot, limits and order/deal history with cursor iteration.
- Ed25519 signing, typed validated responses and precise Decimal fields.
- uv lockfile, Ruff, strict mypy, offline contract/signature tests and CI.
- Local alpha only; no live API verification or package publication yet.
