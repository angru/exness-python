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

## Completed: account, instrument and HMR events

- Dedicated typed subscriptions share transport limits, signed handshakes, opt-in
  reconnects and explicit gaps. Instrument lists are copied and validated locally.
- Instrument selection supports exactly ["all"]; HMR uses explicit names only.
- HMR requires a fresh snapshot on every connection and supports empty/replacement
  snapshots and deltas. Schedule strings remain unchanged without inferred timezone.
- Account and instrument streams have no initial snapshot; recovery limitations
  and caller-owned cache reconciliation are explicit in README.md.
- Shared fixtures, pinned-schema checks and local TLS tests cover all event kinds,
  validation, lifecycle actions, reconnection boundaries and cleanup.

## Completed: alpha API review

- Reviewed the REST/stream surface, model locations, error contracts and lifecycle.
- Added neutral stream aliases without removing legacy names; fixed Python close
  admission and added signature/external-consumer regression checks.
- Prepared alpha 7 tags, release notes and reproducible install instructions.
  See API_REVIEW.md and RELEASE.md.

## Implemented offline: trading operations

See [TRADING_WRITES.md](TRADING_WRITES.md) for the shared design and acceptance criteria.

- [x] Stage 1: exact-byte mutation signing, explicit keys, opening-position and ACK models.
- [x] Stage 2: five submission methods, remaining request types and unknown-outcome handling.
- [x] Stage 3: bounded result tracking and event correlation.
- [x] Stage 4: offline recovery examples and cross-SDK scenarios (8 scenarios, 49 checkpoints).
- [ ] Stage 5: explicitly authorized live interoperability when an account is available.

Stages 1–4 do not require live access. Registry publication remains deferred.

<a id="next-offline-robustness-and-consumer-verification"></a>

## Completed: offline robustness and consumer verification

All five follow-up work packages are implemented and verified offline in both
SDKs. The linked issues record implementation and check evidence. No live requests,
automatic write retries or releases were part of this work. The table retains
the agreed acceptance criteria; new work should be tracked separately.

| Order | Work package | Acceptance criteria |
| --- | --- | --- |
| 1 | Response validation audit | Review critical REST/transaction/HMR identifiers, required fields, nulls and states against the pinned contract; fix demonstrated inconsistencies; share positive and negative fixtures; document intentional language differences |
| 2 | Decoder fuzzing and generated sequences | Bounded malformed REST/WebSocket inputs and snapshot/event sequences; no panics, unbounded processing or invalid state transitions; save discovered regressions and run deterministic seeds in normal CI |
| 3 | Automated cross-SDK comparison | Detect drift in shared fixture copies and compare normalized results for agreed scenarios; explicitly pin the peer revision for reproducibility; keep each SDK independently buildable |
| 4 | Local lifecycle stress and benchmarks | Repeat reconnect, concurrent request and cancellation scenarios against local servers; verify owned resource cleanup; record reproducible parsing/signing measurements without flaky wall-time performance gates |
| 5 | Installed-package consumer checks | Exercise current exports, request/response types and trading/wait entry points from an installed Python artifact and an independent Go module; update API review tables/counts; run without live credentials |

- [x] 1. [Response validation audit and shared regression corpus](https://github.com/angru/exness-python/issues/1).
- [x] 2. [Decoder fuzzing and generated event sequences](https://github.com/angru/exness-python/issues/2).
- [x] 3. [Automated fixture and behavioral parity checks](https://github.com/angru/exness-python/issues/3).
- [x] 4. [Local lifecycle stress and reproducible benchmarks](https://github.com/angru/exness-python/issues/4).
- [x] 5. [Installed-package checks and current public API documentation](https://github.com/angru/exness-python/issues/5).

Cross-SDK comparison now covers 46 shared files, 170 decoder outcomes and 49
recovery checkpoints, with a pinned-peer CI job in each repository. See
[PARITY.md](PARITY.md) and the [implementation](https://github.com/angru/exness-python/commit/bb9181393e2c5422dbea8e3f9b9c11b8ea0b2076).

Six-cycle TLS lifecycle checks and four parsing/signing benchmarks are now
implemented in both SDKs. See [PERFORMANCE.md](PERFORMANCE.md) for resource
accounting, finite test budgets, commands and initial measurements.

Isolated Python-wheel and independent Go-module consumers now exercise the
current submission/wait APIs and typing. See [CONSUMERS.md](CONSUMERS.md) and the
updated [API_REVIEW.md](API_REVIEW.md); historical alpha-7 tags are unchanged.

The first audit fixed uint64 overflow and related critical-field inconsistencies.
See [RESPONSE_VALIDATION.md](RESPONSE_VALIDATION.md) for the implemented rules and
the 170-case shared regression corpus. Do not tighten unknown
fields or optional values merely to make the languages identical; changes require
contract evidence and a regression case. Treat the pinned contract as the offline
source of truth, recording ambiguities explicitly.

## Completed: candidate pair parity

- [x] [Validate explicit Python and Go candidate revisions before updating peer pins](https://github.com/angru/exness-python/issues/8).
- Run the canonical offline candidate-pair command on two clean local checkouts;
  review its parity evidence and proposed counterpart pins before adoption.

## Completed: public API compatibility gate

- [x] [Guard public API compatibility against a pinned baseline](https://github.com/angru/exness-python/issues/6).
- The gate checks the reviewed public surface at `1cedf7d12b9977e2d7a4204b9e12ba0d9733d1c2` as its baseline. Historical alpha-7 tags omit
  later submission/wait APIs and are not this baseline.
- Detect incompatible public export, signature and request/response type changes;
  distinguish compatible additions and explain language-specific exclusions.
- Provide an offline command with symbol-level diagnostics, breaking-change
  regressions and a CI gate. Include a break that existing sync/async parity would miss.
- Keep baseline replacement an explicit reviewed edit; retain installed-consumer
  checks and document that API shape checks do not prove behavioral compatibility.

Worker Board completed implementation, verification and independent review for each SDK.

## Deferred: live interoperability and user feedback

- Add an explicitly enabled read-only live smoke workflow when credentials and a
  test account are available; retain the unverified-live disclosure until then.

## Release gates

- CI must pass on supported Python versions; published metadata must match the SDK.
- A future opt-in read-only live smoke test must redact credentials and be disabled
  by default. Lack of live testing must stay visible in release notes.
- Trading writes require a separate design for idempotency, ACK versus execution
  results and uncertain outcomes; do not add them as routine REST wrappers.
- Package registry publication and stable-version claims are separate steps from
  publishing source on GitHub.

## Completed: contract impact triage

- [x] [Review SDK impact of OpenAPI drift](https://github.com/angru/exness-python/issues/10).

## Cross-SDK transport check

- The Python parity job compares both public clients against shared local TLS
  fixtures. See the [offline command](PARITY.md#compare-public-clients-over-loopback-tls)
  and [issue #14](https://github.com/angru/exness-python/issues/14).
- [x] [Compare transaction-stream recovery and missing-snapshot rejection](https://github.com/angru/exness-python/issues/16).
- [x] [Compare write receipt and unknown-outcome identity after a lost acknowledgement](https://github.com/angru/exness-python/issues/18).
- [x] [Verify lost-ACK recovery by matching a transaction without replaying the write](https://github.com/angru/exness-python/issues/20).
- [x] [Compare acknowledged open-position submission and bounded status polling](https://github.com/angru/exness-python/issues/22).
- [x] [Compare HMR reconnect with a fresh empty snapshot and rejection of updates before a snapshot](https://github.com/angru/exness-python/issues/24).
- [x] [Compare signed order history pagination and repeated-cursor termination](https://github.com/angru/exness-python/issues/26).
- [x] [Compare rate-limited status reads after an acknowledged open-position submission](https://github.com/angru/exness-python/issues/28).
- [x] [Compare instrument-event resubscription with an explicit EURUSD selection and no initial snapshot](https://github.com/angru/exness-python/issues/30).
