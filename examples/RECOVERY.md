# Offline account recovery example

Run from a development checkout. The examples use only bundled synthetic messages:
no account, credentials, environment variables, HTTP/WebSocket connection, or
trading submission is involved. They are application code in `examples/`, not a
new SDK cache API. Alpha 7 tags predate these examples.

Python:

```sh
uv sync --locked
uv run python examples/recover_trading.py
```

Go:

```sh
go run ./examples/recover-trading
```

Each command checks eight scenarios and writes 49 JSON Lines checkpoints. Each
line includes the scenario, step, whether the application rejected the observation,
and the resulting view. A mismatched expected result makes the command fail.
Python loads `examples/recovery_scenarios.json`; Go embeds the byte-identical
`examples/recover-trading/scenarios.json`. Both repositories test their own copy,
so neither requires the other checkout. Update both copies together. Python also
validates every bundled wire message against the pinned OpenAPI schemas.

## What the checkpoints demonstrate

| Scenario | Evidence |
| --- | --- |
| Partial close and duplicates | Replace remaining position volume, remove closed positions/cancelled orders, replace balance, keep observed deals by ID, ignore an older exact duplicate after newer changes |
| Gap, timeout and status recovery | Preserve a pending operation through wait expiry; refuse deltas without a baseline; confirmed status does not restore the account cache; a fresh snapshot replaces it |
| Rejected execution | Preserve code/message without changing orders, positions or balances |
| Failed execution | Preserve the distinct failure result; duplicate failure observations have no financial side effect |
| Lost ACK and late observations | A persisted caller key can correlate an event and recover its operation ID; late ACK/pending observations cannot downgrade a terminal result; contradictory terminal results require review |
| Missing event status | Matching identity does not establish completion; conservatively invalidate the account view, then use an explicit status read once the ID is known |
| Conflicting identifiers | Refuse contradictory operation ID/key evidence and invalidate the live baseline |
| Snapshot replacement and key-only result | An empty snapshot removes old entities, new active orders are added, filled orders disappear, and an event can report a terminal result without an operation ID |

`ready` means a stream baseline is available for applying subsequent ordered
transactions. It does not promise that floating equity is current at wall-clock
now. `account` stores the most recent supplied account observation. Entity IDs
are normalized numerically; prices, volumes and monetary values remain exact.
The operation summary is separate from account state. Its `wait_expired` flag is
historical and remains true even if a later event establishes a terminal result.
A fresh snapshot proves neither completion nor rejection of a particular request.
The timeout action simulates receiving a wait-timeout error; it starts no timer
and does not invoke status polling. A status action supplies a recorded response,
not a lookup by client request key. Missing ACKs never trigger resubmission.

## Policy and limits

The reducer assumes one account and one ordered transactions stream, with SDK
validated messages. Keep all updates on one consumer; it is not concurrency-safe.
The scenario adapter decodes trusted bundled fixtures directly into public SDK
models. It is not a replacement for the SDK's network response validation.

Every snapshot replaces active orders, open positions and account state. A gap,
stream exit or rejected observation clears that baseline. Deals are observations
since the latest snapshot only; they are cleared on replacement or invalidation.
They are not a complete deal history or a P&L ledger. Reconcile historical records
explicitly through history endpoints when needed. The example never adds deal
profits to a supplied balance or subtracts a closed volume from a supplied
remaining volume.

A bounded set of hashes detects exact decoded duplicate transactions within the
current snapshot epoch. It is cleared on a snapshot or invalidation. No operation
ID is treated as a globally unique event ID. No replay token or total event
sequence is available, so unseen out-of-order events, delayed messages from an old
connection and duplicates across reconnects cannot be repaired by this set.
Only feed events from the active connection; resubscribe and obtain a fresh
snapshot when continuity is uncertain. Do not interleave a REST snapshot with a
running stream and assume they form an atomic baseline.

The duplicate window and each entity map are limited to 256 entries. On exhaustion
the example raises a protocol error and clears the baseline instead of silently
evicting evidence. This small cap is a teaching policy, not a service limit.
Likewise, missing transaction status, a nonempty rejected/failed payload, duplicate
IDs in a snapshot or non-active snapshot entities cause conservative invalidation.
These are application decisions, not additional claims about the API contract.

Operation correlation follows the SDK helper: compatible identifiers can match;
contradictory identifiers fail. Repeated terminal observations are not applied as
new monetary changes. An incompatible terminal result preserves earlier evidence,
invalidates the baseline and raises an error for manual reconciliation; that
preserved result must not be taken as a resolution of the conflict.

## Adapting the example to a real consumer

Persist the account ID, request key and intended request **before** submission.
Keep the same account scope for the reference, status reads and transaction stream.
Pass the returned ACK to the example's acknowledgement method. On an unknown
outcome, retain the caller key and reconcile; do not create a new key and repeat
the trade. Poll only if an operation ID is available, using the explicit bounded
SDK wait helper. Record the result separately from the stream baseline.

In Python, pass events from `client.stream_trading(...)` to `view.on_stream(event)`
and always call `view.invalidate()` in the consumer's `finally` block. In Go, pass
`TradingStreamEvent` values to `view.onStream(event)`, stop on errors and `defer
view.invalidate()` around consumption. Reconnect remains an explicit bounded SDK
option. An application error requires a fresh subscription/snapshot before further
deltas. The examples do not persist data or implement restart/crash recovery,
background polling, retries, a trading strategy or live interoperability checks.

See [runtime semantics](https://www.exness-api.com/trading/guides/runtime-semantics)
for the underlying event, snapshot and correlation rules. Live verification and
publication remain separate tasks.
