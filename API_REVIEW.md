# Public API review — current main

Reviewed 2026-11-26 (Asia/Nicosia) against the locally pinned Trading API 1.0.2
contract. This describes current main, including submission and operation waiting;
it does not claim a new upstream contract review or live interoperability. The
historical alpha 7 tags remain read-only. RELEASE.md retains their release scope;
this review creates no new tag, package release or stable-API promise.

## Entry points and parity

| Capability | Python (sync and asyncio REST) | Go |
| --- | --- | --- |
| Credentials/client | `Credentials`, `ExnessClient`, `AsyncExnessClient` | `NewCredentials`, `NewClient(Config)` |
| Discovery | `discover(refresh=False)` | `Discover(ctx, refresh)` |
| Account | `get_account()` | `GetAccount(ctx)` |
| Instrument list | `get_instruments()` | `GetInstruments(ctx)` |
| Trading snapshot | `get_snapshot()` | `GetSnapshot(ctx)` |
| Limits | `get_limits()` | `GetLimits(ctx)` |
| Conditions | `get_instrument_conditions(instrument)` | `GetInstrumentConditions(ctx, instrument)` |
| Candle history | `get_candle_history(...)` | `GetCandleHistory(ctx, CandleQuery)` |
| Existing operation | `get_operation_status(operation_id)` | `GetOperationStatus(ctx, operationID)` |
| Order/deal pages | `get_orders_history`, `get_deals_history` | `GetOrdersHistory`, `GetDealsHistory` |
| History iterators | `iter_orders_history`, `iter_deals_history` | `IterOrdersHistory`, `IterDealsHistory` |
| Open position | `open_position(OpenPositionRequest, idempotency_key=...)` | `OpenPosition(ctx, OpenPositionRequest, key)` |
| Place limit/stop/SLTP | `place_pending_order(PendingOrderRequest, idempotency_key=...)` | `PlacePendingOrder(ctx, PendingOrderRequest, key)` |
| Modify order | `modify_order(id, ModifyOrderRequest, idempotency_key=...)` | `ModifyOrder(ctx, id, ModifyOrderRequest, key)` |
| Cancel pending order | `cancel_pending_order(id, idempotency_key=...)` | `CancelPendingOrder(ctx, id, key)` |
| Close position | `close_position(id, ClosePositionRequest, idempotency_key=...)` | `ClosePosition(ctx, id, ClosePositionRequest, key)` |
| Bounded result waiting | `wait_operation(reference, timeout=..., poll_interval=...)` | `WaitOperation(ctx, reference, OperationWaitOptions)` |
| Operation correlation | `OperationReference.from_ack`, `matches_status`, `matches_event` | `OperationReferenceFromAck`, `MatchesStatus`, `MatchesEvent` |
| Ticks | `AsyncExnessClient.stream_ticks(names)` | `StreamTicks(ctx, names, options)` |
| Transactions | `stream_trading()` | `StreamTrading(ctx, options)` |
| Floating account state | `stream_account_state()` | `StreamAccountState(ctx, options)` |
| Instrument events | `stream_instruments(names)` | `StreamInstruments(ctx, names, options)` |
| HMR | `stream_hmr(names)` | `StreamHMR(ctx, names, options)` |
| Close | `close()` / `await aclose()`; context managers | `Close()`; usually `defer client.Close()` |

Python's 18 paired entry points have a signature parity regression test: discovery,
nine read methods, two history iterators, five submissions and one waiter. Parameter
names, defaults and types must match, with iterator yield types aligned. This count
excludes closure/context-manager methods and the five asyncio-only streams. Go
provides the corresponding operations and five streams. Examples remain in type/build
checks; installed consumers now exercise submissions, waiting, models and exports
from outside the repository (see CONSUMERS.md).

## Names, models and errors

Use `StreamOptions` and `StreamGap` for any stream. Existing `TickStream*`,
`TradingStream*` and `ServerStream*` names remain identical aliases, without a
migration requirement. Python retains the historical runtime class names so
pickling, identity checks and existing imports keep working.

Python package-root exports cover clients, credentials, errors, OperationReference
and stream options/gaps.
Response models are public in `exness.models`, transaction models in
`exness.trading`, and account/instrument/HMR models in `exness.events`. Write request
models and the PendingOrderRequest union are public in `exness.requests`. Leading
underscore names, transport factories and helpers are internal. Go exports all
models and clients from `github.com/angru/exness-go`. Response DTOs preserve optional
fields and are not constructors for trading writes.

| Failure | Python | Go |
| --- | --- | --- |
| Invalid configuration | `ConfigurationError` (`ValueError`, `ExnessError`) | Constructor error |
| Invalid request options | `ValueError` | Returned error; lazy iterators yield it |
| HTTP error | `APIError` | `*APIError` via `errors.As` |
| Invalid response | `ProtocolError` | `*ProtocolError` |
| Subscription rejection | `WebSocketAPIError` | `*WebSocketAPIError` |
| Terminal WebSocket code | `StreamClosed` | `*StreamClosed` |
| Transport/reconnect exhaustion | `TransportError` | `*TransportError` |
| Ambiguous submission outcome | `UnknownOutcomeError` with method/path/account/key | `*UnknownOutcomeError`, preserving the cause through `Unwrap` |
| Operation wait deadline | `OperationWaitTimeout` with reference/last_status/attempts | `*OperationWaitTimeoutError` with Reference/LastStatus/Attempts; unwraps DeadlineExceeded |
| Caller cancellation | `asyncio.CancelledError`, propagated | `errors.Is(err, context.Canceled/DeadlineExceeded)` |
| Closed client | `RuntimeError` for new work; an active stream read may end normally | Returned error; active streams receive cancellation |

Error strings omit signed headers and raw server text. Error fields and Go's
unwrapped transport causes may contain untrusted details. Operation `rejected` or
`failed` events are data, not subscription failures. Never infer success from an
absent status or from `OperationStatus.IsTerminal` alone.

## Lifecycle and recovery findings

Fixed: Python now refuses new work as soon as `aclose()` starts, before awaiting
socket cleanup. A deterministic regression holds socket closure open while trying
new REST, stream and context-manager entry operations.
Subsequent hardening also interrupts owned stream discovery, handshake and backoff
waits, including discovery lock waiters. A connection that opens concurrently with
shutdown is closed; caller cancellation still propagates unchanged.

Retained contracts: break a Go iterator to clean up; Python async generators need
`contextlib.aclosing` on early exit. Python closure interrupts SDK stream waits,
but does not cancel consumer tasks or arbitrary code processing a yielded event.
Await consumers to finish cleanup; cancel/await independent REST tasks before
closing. Go `Close` cancels owned stream contexts; in-flight REST may finish.
Python asyncio clients belong to one event loop; Go clients support concurrent use.

Transactions and HMR require fresh snapshots before deltas on each connection.
All gaps and terminations invalidate continuity. Account/instrument streams do not
promise initial snapshots. No replay, hidden REST reconciliation, automatic trade
submission or exactly-once delivery is provided. See README for cache rules.

## Deliberate differences and alpha limits

- Python keyword options have explicit defaults; Go option structs use zero for
  default timeout/message limits and zero reconnects for no retries. Configure
  `OpenTimeout`/`open_timeout` separately from REST timeouts.
- Python HTTPX timeouts bound individual network phases; Go's HTTP timeout bounds
  the entire REST request. Both SDKs cap REST response bodies at 16 MiB; Python
  counts decoded bytes, including compressed and error responses. Streaming message limits
  are shared (64 KiB default, configurable through 16 MiB).
- Python response models preserve unknown fields; Go drops unknown fields. Both
  enforce the audited uint64 identifiers, required/non-null fields and critical
  states recorded in RESPONSE_VALIDATION.md (170 shared cases). Other optional
  fields and enum handling can differ.
  Neither SDK claims complete runtime JSON Schema validation. Fixture schema checks
  are an offline development gate, not a blanket guarantee for arbitrary responses.
- Decimal values are exact in both SDKs; Go also retains original string spelling.
  Candle/tick doubles remain floating point. Optional fields must be checked.
- Python has a bounded background WebSocket frame queue; Go reads with iteration
  and transport backpressure. Slow Go consumers can delay pong processing.
- HMR schedule strings carry no inferred timezone or active-now calculation.
- Breaking changes remain possible in later alphas; release notes must identify
  them. The neutral aliases added here preserve all existing names.
- Current trading submissions and waiting are verified only against synthetic
  transports and local TLS servers. Live account interoperability and registry
  publication remain deferred. No live credentials are required by tests.

## Submission and outcome contracts

All five trading mutations are now implemented with typed inputs and caller-owned
idempotency keys. The alpha 7 tags remain read-only. See TRADING_WRITES.md for
methods, request variants, PATCH null handling, ACK correlation and unknown outcomes.
Python exports UnknownOutcomeError from the package root and request types from
exness.requests. Go exposes request types, PriceChange constructors and
UnknownOutcomeError at the package root. Submission returns admission only;
explicit result tracking is now available on main. OperationReference correlates
ACKs, status reads and transaction events; wait_operation / WaitOperation perform
bounded reads only. Timeout errors preserve identity and the last status. Python
sync waiting has cooperative network deadlines; async/Go support cancellation
through in-flight reads and discovery waits. The validation evidence is offline.


## Offline recovery examples

Stage 4 adds application examples only; public SDK signatures are unchanged.
The Python and Go programs use byte-identical synthetic scenarios with 49 expected
views. They demonstrate operation correlation independently of snapshot continuity,
with bounded duplicate/entity storage and explicit invalidation. See
[examples/RECOVERY.md](examples/RECOVERY.md) for commands, policy and limitations.
The examples are not a durable account cache or a complete deal ledger.

## Verification from installed consumers

Python builds a wheel from the sdist, installs it in a fresh environment, runs
synthetic sync/async submission and waiting scenarios with runtime dependencies
only, then checks a valid consumer and three invalid arguments with strict mypy.
The SDK must load from that environment and contain py.typed; source-tree paths
and editable installs cannot satisfy this check.

Go compiles and runs a separate module against an isolated copy of production SDK
files, with GOWORK disabled and no SDK test helpers/fixtures. Its synthetic transport
exercises all submissions, pending-to-confirmed waiting, unknown outcomes, response
body cleanup and all five pre-cancelled stream entry points. A deliberately invalid
request must fail compilation. These checks run on every supported-version CI job;
see [CONSUMERS.md](CONSUMERS.md) for exact commands and limits.

Shared fixture/behavior checks and bounded lifecycle stress remain separate gates
([PARITY.md](PARITY.md), [PERFORMANCE.md](PERFORMANCE.md)). These consumer checks
verify the current local artifact/source revision, not a registry-published release.
