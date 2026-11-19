# Public API review — alpha 7

Reviewed 2026-11-19 (Asia/Nicosia) against the pinned Trading API 1.0.2 contract.
This is a reviewed alpha surface, not a stable-API or live-interoperability claim.

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
| Ticks | `AsyncExnessClient.stream_ticks(names)` | `StreamTicks(ctx, names, options)` |
| Transactions | `stream_trading()` | `StreamTrading(ctx, options)` |
| Floating account state | `stream_account_state()` | `StreamAccountState(ctx, options)` |
| Instrument events | `stream_instruments(names)` | `StreamInstruments(ctx, names, options)` |
| HMR | `stream_hmr(names)` | `StreamHMR(ctx, names, options)` |
| Close | `close()` / `await aclose()`; context managers | `Close()`; usually `defer client.Close()` |

Python's 12 discovery/REST/history methods have a signature parity regression test:
parameter names, defaults and types must match, with iterator yield types aligned.
All five Go stream methods are exercised from an external test package with common
options and inspectable cancellation. Examples are included in type/build checks.

## Names, models and errors

Use `StreamOptions` and `StreamGap` for any stream. Existing `TickStream*`,
`TradingStream*` and `ServerStream*` names remain identical aliases, without a
migration requirement. Python retains the historical runtime class names so
pickling, identity checks and existing imports keep working.

Python package-root exports cover clients, credentials, errors and stream options.
Response models are public in `exness.models`, transaction models in
`exness.trading`, and account/instrument/HMR models in `exness.events`. Leading
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
| Caller cancellation | `asyncio.CancelledError`, propagated | `errors.Is(err, context.Canceled/DeadlineExceeded)` |
| Closed client | `RuntimeError` | Returned error; active streams receive cancellation |

Error strings omit signed headers and raw server text. Error fields and Go's
unwrapped transport causes may contain untrusted details. Operation `rejected` or
`failed` events are data, not subscription failures. Never infer success from an
absent status or from `OperationStatus.IsTerminal` alone.

## Lifecycle and recovery findings

Fixed: Python now refuses new work as soon as `aclose()` starts, before awaiting
socket cleanup. A deterministic regression holds socket closure open while trying
new REST, stream and context-manager entry operations.

Retained contracts: break a Go iterator to clean up; Python async generators need
`contextlib.aclosing` on early exit. Close Python clients after canceling/awaiting
active consumer tasks; closing sockets does not interrupt arbitrary consumer code
or promise cancellation of pending discovery/handshake/backoff tasks. Go `Close`
cancels owned stream contexts (including these waits); in-flight REST may finish.
Python clients belong to one event loop; Go clients support concurrent use.

Transactions and HMR require fresh snapshots before deltas on each connection.
All gaps and terminations invalidate continuity. Account/instrument streams do not
promise initial snapshots. No replay, hidden REST reconciliation, automatic trade
submission or exactly-once delivery is provided. See README for cache rules.

## Deliberate differences and alpha limits

- Python keyword options have explicit defaults; Go option structs use zero for
  default timeout/message limits and zero reconnects for no retries. Configure
  `OpenTimeout`/`open_timeout` separately from REST timeouts.
- Python HTTPX timeouts bound individual network phases; Go's HTTP timeout bounds
  the entire REST request. Go caps REST response bodies at 16 MiB; Python currently
  buffers REST responses without a fixed SDK body cap. Streaming message limits
  are shared (64 KiB default, configurable through 16 MiB).
- Python response models preserve unknown fields and validate many enums; Go drops
  unknown fields and retains many nested enum strings, with essential-field checks.
  Neither SDK claims complete runtime JSON Schema validation. Fixture schema checks
  are an offline development gate, not a blanket guarantee for arbitrary responses.
- Decimal values are exact in both SDKs; Go also retains original string spelling.
  Candle/tick doubles remain floating point. Optional fields must be checked.
- Python has a bounded background WebSocket frame queue; Go reads with iteration
  and transport backpressure. Slow Go consumers can delay pong processing.
- HMR schedule strings carry no inferred timezone or active-now calculation.
- Breaking changes remain possible in later alphas; release notes must identify
  them. The neutral aliases added here preserve all existing names.
- Live account interoperability, trading mutations and registry publication remain
  outside this release's verified scope. No credentials are required by tests.
