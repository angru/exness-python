# Changelog

## Unreleased

- Verify current submission/wait APIs from an installed Python wheel and an
  independent Go module, including negative typing checks and synthetic requests.
- Update the current-main API review to 18 paired Python entry points, submission
  request types and operation errors while preserving historical alpha-7 scope.

- Add six-cycle local TLS stress checks for concurrent REST cancellation, bounded
  WebSocket reconnects and concurrent stream shutdown, with resource accounting.
- Add reproducible snapshot/tick parsing and GET/POST signing benchmarks, smoke
  checks without speed thresholds, and documented local measurements.

- Add an optional pinned cross-SDK parity check: 46 shared files, 170 decoder
  outcomes and 49 recovery checkpoints, with explicit drift diagnostics.

- Add bounded REST/WebSocket decoder fuzzing and generated snapshot-state sequences,
  with shared malformed seeds and deterministic regression traces.
- Convert Python JSON nesting recursion failures into ProtocolError across all
  five WebSocket decoders; preserve normal cancellation/error handling.
- Run three new Go decoder fuzz targets in CI with explicit duration/worker budgets.

- Enforce uint64 response identifier bounds consistently, preserving zero and leading
  zeroes where the response contract permits them; validate critical entity states.
- Reject explicit null in audited non-nullable ACK/status/transaction fields and
  snapshot arrays while preserving omission and nullable transaction caller IDs.
- Add 170 shared positive/negative response cases and transport-facing regressions.
  See RESPONSE_VALIDATION.md for contract evidence, scope and compatibility notes.

- Add runnable offline account-recovery examples with eight shared scenarios and
  49 matching checkpoints, requiring no credentials or network connection.
- Demonstrate snapshot replacement, partial/full closure, rejected/failed execution,
  bounded duplicate handling, stream gaps, lost ACKs and late/conflicting results.
- Test example checkpoints, pinned-contract messages and capacity/conflict boundaries;
  document application policy, incomplete deal history and reconnect limitations.

- Add explicit bounded operation-status waiting and account-scoped OperationReference
  correlation for ACKs, REST status and transaction events. Preserve terminal
  rejection/failure details and timeout identity without retrying trading writes.
- Verify shared correlation cases, pending/terminal/error paths, discovery contention,
  cancellation and local TLS deadlines. Document cooperative sync Python deadlines.

- Implement all five trading submission methods with explicit idempotency keys,
  exact signed bodies/queries and no automatic write retries.
- Add limit/stop/SLTP, modification and closure request types; preserve PATCH
  omission versus null and validate close-by zero-volume semantics before networking.
- Report lost/invalid ACKs and ambiguous HTTP outcomes as UnknownOutcomeError;
  preserve cancellation semantics and distinguish preflight/discovery failures.
- Verify shared write scenarios, real TLS bytes, rejection, redirect, truncation,
  timeout and cancellation boundaries; live trading remains unverified.

- Document the staged trading-write plan and implement its first foundation:
  validated opening-position/ACK models and exact-body mutation signing with
  explicit idempotency keys. Client submission methods remain planned.
- Share OpenSSL-generated POST/PATCH/DELETE signing vectors with Go and verify
  signed method, path/query and body bytes over local TLS, including Unicode.
- Limit sync and async REST response bodies to 16 MiB after decompression,
  including error responses; close oversized responses without draining them.
- Interrupt WebSocket discovery, handshake and reconnect waits on client closure,
  without cancelling consumer tasks. Preserve caller cancellation and clean up
  sockets opened concurrently with shutdown.
- Add offline response-boundary and TLS lifecycle regression tests.

## 0.1.0a7 — 2026-11-19

- Review and document the public API, cross-language differences and alpha limits.
- Add neutral StreamOptions/StreamGap aliases without removing legacy names.
- Refuse new asyncio operations immediately when client closure starts; cover the
  socket-cleanup race and sync/async REST signatures with regression tests.
- Prepare the first tagged GitHub alpha with wheel/sdist and installation checks.
  Live interoperability and PyPI publication remain outside this release.

## 0.1.0a6 — 2026-11-19

- Add typed asyncio account-state, instrument and HMR subscriptions, reusing the
  signed WebSocket transport, bounded reconnects and explicit gap events.
- Validate copied instrument selections, support instrument-only ["all"], preserve
  HMR schedule strings and require a fresh HMR snapshot before incremental updates.
- Add shared Go fixtures, pinned-contract validation, local TLS recovery/cleanup
  tests and a runnable example with subscription-specific cache recovery guidance.
- Live interoperability remains unverified; trading mutations remain unsupported.

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
