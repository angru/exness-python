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

## Next: offline robustness and consumer verification

The initial feature plan is complete offline; live access does not block this
follow-up work. Complete the following work packages in order in both SDKs. Track
each package with a repository issue and link its verified implementation before
marking it complete. No live requests, automatic write retries or releases are
part of this plan.

| Order | Work package | Acceptance criteria |
| --- | --- | --- |
| 1 | Response validation audit | Review critical REST/transaction/HMR identifiers, required fields, nulls and states against the pinned contract; fix demonstrated inconsistencies; share positive and negative fixtures; document intentional language differences |
| 2 | Decoder fuzzing and generated sequences | Bounded malformed REST/WebSocket inputs and snapshot/event sequences; no panics, unbounded processing or invalid state transitions; save discovered regressions and run deterministic seeds in normal CI |
| 3 | Automated cross-SDK comparison | Detect drift in shared fixture copies and compare normalized results for agreed scenarios; explicitly pin the peer revision for reproducibility; keep each SDK independently buildable |
| 4 | Local lifecycle stress and benchmarks | Repeat reconnect, concurrent request and cancellation scenarios against local servers; verify owned resource cleanup; record reproducible parsing/signing measurements without flaky wall-time performance gates |
| 5 | Installed-package consumer checks | Exercise current exports, request/response types and trading/wait entry points from an installed Python artifact and an independent Go module; update API review tables/counts; run without live credentials |

- [x] 1. [Response validation audit and shared regression corpus](https://github.com/angru/exness-python/issues/1).
- [ ] 2. [Decoder fuzzing and generated event sequences](https://github.com/angru/exness-python/issues/2).
- [ ] 3. [Automated fixture and behavioral parity checks](https://github.com/angru/exness-python/issues/3).
- [ ] 4. [Local lifecycle stress and reproducible benchmarks](https://github.com/angru/exness-python/issues/4).
- [ ] 5. [Installed-package checks and current public API documentation](https://github.com/angru/exness-python/issues/5).

The first audit fixed uint64 overflow and related critical-field inconsistencies.
See [RESPONSE_VALIDATION.md](RESPONSE_VALIDATION.md) for the implemented rules and
the 170-case shared regression corpus. Do not tighten unknown
fields or optional values merely to make the languages identical; changes require
contract evidence and a regression case. Treat the pinned contract as the offline
source of truth, recording ambiguities explicitly.

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
