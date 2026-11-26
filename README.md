# Exness Python

Unofficial, typed Python SDK for the **public Exness Trading API**. Initial alpha,
API contract 1.0.2. This is not an official Exness product or a MetaTrader wrapper.
The package has not been published to PyPI or verified against a live account.

## Supported workflows

- Ed25519 authentication: Exness 128-character hex keys and unencrypted PKCS#8 PEM.
- Gateway discovery, cached per client/account, with explicit refresh.
- Account details, available instrument names, current trading snapshot and API limits.
- Instrument conditions, candle history (bid/ask/both), and existing operation status.
- Orders/deals history, individual pages and lazy iteration with opaque cursors.
- Pydantic response models, exact `Decimal` money/price/volume fields, timezone-aware dates.
- Structured errors, finite REST timeouts, no automatic REST retries or redirects.
- Asyncio tick/transaction streams with opt-in bounded reconnects and visible gaps.

Python 3.11+. Both synchronous and asyncio clients support the same read-only REST
operations. The asyncio client also streams live ticks and transactions over
WebSocket, including account-state, instrument and HMR events. Trading mutations
remain future work.
The companion [Go SDK](https://github.com/angru/exness-go) has the same read-only operations
and shared synthetic response/signing fixtures. Each repository builds independently.

## Installation and local setup

Until a PyPI release is available, install directly from GitHub:

```sh
uv add "git+https://github.com/angru/exness-python.git@v0.1.0a7"
```

For development:

```sh
git clone https://github.com/angru/exness-python.git
cd exness-python
uv sync --locked
```

Distribution name: `exness-python`; import package: `exness`. For another uv project,
install this checkout with `uv add /absolute/path/to/exness-python`. The name is
provisional until publishing; a matching package on an index is not this checkout.

## Read account data

Create a key with Trading read access, then set these environment variables locally:

- `EXN_API_KEY`: API key identifier.
- `EXN_PRIVATE_KEY`: the 128-character private key text or complete unencrypted PEM.
- `EXN_ACCOUNT_ID`: positive uint64 account identifier, as a string.
- `EXN_DISCOVERY_BASE_URL`: HTTPS Gateway origin from Key Management's DNS widget.

```python
import os

from exness import Credentials, ExnessClient

with ExnessClient(
    credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
    account_id=os.environ["EXN_ACCOUNT_ID"],
    discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
) as client:
    account = client.get_account()
    snapshot = client.get_snapshot()
    print(account.account_id, snapshot.account_state.balance)
    print(client.get_instruments().instruments)
```

Run the complete example with `uv run python examples/read_account.py`. It performs
only GET requests. No credentials are needed to run tests.

The Gateway is used only for discovery; account calls use its returned Access Point.
`client.discover(refresh=True)` explicitly re-resolves it. Clients own their HTTP
connection pool; close them or use `with`. Use one client per account. Environment
proxy and netrc settings are ignored (`trust_env=False`); inject an HTTPX transport
for custom networking. Do not log credentials or signed headers.

## Asyncio client

`AsyncExnessClient` uses the same credentials, parameters, response models and
exceptions as `ExnessClient`. Await REST methods and use `async for` for history:

```python
import asyncio
import os
from datetime import UTC, datetime, timedelta

from exness import AsyncExnessClient, Credentials


async def main() -> None:
    async with AsyncExnessClient(
        credentials=Credentials(os.environ["EXN_API_KEY"], os.environ["EXN_PRIVATE_KEY"]),
        account_id=os.environ["EXN_ACCOUNT_ID"],
        discovery_url=os.environ["EXN_DISCOVERY_BASE_URL"],
    ) as client:
        async with asyncio.timeout(30), asyncio.TaskGroup() as group:
            account = group.create_task(client.get_account())
            snapshot = group.create_task(client.get_snapshot())
        print(account.result().account_id, snapshot.result().account_state.balance)

        async with asyncio.timeout(30):
            async for item in client.iter_orders_history(
                from_time=datetime.now(UTC) - timedelta(days=1), limit=100
            ):
                print(item.order.order_id)


asyncio.run(main())
```

Run `uv run python examples/read_account_async.py` for the complete example.
Use one async client per account within one asyncio event loop. Share it among
tasks; do not share across threads or event loops. Concurrent initial reads share
a cached discovery result. `await client.discover(refresh=True)` explicitly
refreshes it; cached reads can use the previous host while refresh is running.
Failed or cancelled discovery releases its lock and does not replace the cache.

`asyncio.CancelledError` propagates unchanged. A waiting task can be cancelled
without cancelling another task's discovery. HTTPX's `timeout` is a timeout for
individual network phases, not an overall operation deadline; use
`asyncio.timeout(...)` to bound discovery waiting and a complete call or iteration.
Both REST clients limit each decoded response body to 16 MiB, including discovery
and error responses. Exceeding the limit raises `ProtocolError` and closes the
response without reading the remaining body; compressed responses are counted
after decompression, independently of `Content-Length`.

The client owns its HTTP pool and any injected `httpx.AsyncBaseTransport`. Use
`async with` or `await client.aclose()`; cancel and await active REST tasks before closing.
`TaskGroup` does this for sibling tasks when one fails. History generators do not
prefetch pages; early termination makes no extra request. Use `contextlib.aclosing`
when explicitly managing a partially consumed generator. Async REST supports
asyncio; Trio is not implemented.

## Live ticks

Inside an `AsyncExnessClient` context:

```python
from contextlib import aclosing
from exness import TickStreamGap, TickStreamOptions

async with aclosing(
    client.stream_ticks(
        ["EURUSD", "XAUUSD"],
        options=TickStreamOptions(max_reconnects=3),
    )
) as stream:
    async for event in stream:
        if isinstance(event, TickStreamGap):
            print("Connection interrupted; missed ticks cannot be replayed")
        else:
            print(event.instrument, event.bid, event.ask, event.timestamp)
```

Run `uv run python examples/read_ticks.py` with the same `EXN_*` settings and
optional `EXN_INSTRUMENT`. This subscribes to market data; it does not trade.
The account Access Point is discovered through REST, and each WSS handshake is
freshly signed as GET on `/v1/server-events/accounts/{account_id}/ws/ticks`.
A new request id and subscription are sent for each connection. Instrument names
must be nonempty, valid and unique; availability and account limits are server rules.

Ticks use finite `float` bid/ask values as specified by the market-data schema;
missing fields remain `None`. A received tick must identify a subscribed instrument.
Malformed or unknown messages fail with `ProtocolError`; the SDK does not invent
an acknowledgement format. Subscription rejections raise `WebSocketAPIError`;
its `error_message` is untrusted and omitted from exception text.

Reconnection is **off by default**. `max_reconnects` is a lifetime budget (0–1000),
never reset after receiving a tick. Transport failures and close codes 1001, 1006,
1011 and 1012 can consume that budget. Before each attempt, `TickStreamGap` reports
the attempt number, delay and optional close code. Delays double from 1 second to
a 30-second cap by default; configure them with `TickStreamOptions`. The last known
tick may arrive again after resubscription; ticks are neither replayed nor deduplicated.
No automatic discovery refresh occurs during reconnects; refresh explicitly after
account migration. No hidden retry is added to REST calls.

Normal close (1000) ends iteration. Other close codes, including policy violations
(1008) and oversized messages (1009), raise `StreamClosed`. HTTP handshake failures
(including 429/503), invalid TLS certificates and subscription errors are terminal.
Exhausted reconnect budgets raise `TransportError`. Task cancellation propagates
unchanged, including during backoff. Use `aclosing` for early breaks. Closing the
client closes active WebSocket connections and interrupts stream discovery
(including lock waits), handshakes and reconnect backoff with a closed-client
`RuntimeError`. It does not cancel the consumer task or code processing a yielded
event. Await consumers to finish their cleanup; cancel active REST tasks before
closing the client.

The default message limit is 64 KiB (configurable up to 16 MiB), with a 16-frame
receive queue and transport backpressure. Slow consumers are not given an unbounded
SDK queue; a server-side overflow may close the connection. Ping/pong keepalive
uses 20-second intervals/timeouts. There is no tick inactivity timeout because
markets can be quiet; use `asyncio.timeout` for an application deadline. Opening
defaults to 15 seconds; close handshake timeout is 3 seconds. Redirects, environment
proxies and compression are disabled. Transport debug logging is disabled to avoid
logging signed headers. `ssl_context=` customizes WebSocket TLS only, independently
of an injected REST transport. The transport dependency is pinned to websockets 15.x.

Live interoperability remains unverified. The pinned contract and local TLS tests
cover signing, subscriptions, errors, cancellation, reconnects, oversized messages,
ping/pong and slow consumers. Transactions streaming is described below;
account, instrument and HMR subscriptions are described in the next section.

## History and precision

```python
from datetime import UTC, datetime, timedelta

# Inside the client context; choose a range allowed by client.get_limits().
end = datetime.now(UTC)
start = end - timedelta(hours=1)
for item in client.iter_deals_history(from_time=start, to_time=end, limit=100):
    print(item.event_time, item.deal.deal_id, item.deal.profit)
```

`get_orders_history()` and `get_deals_history()` return `HistoryPage` objects.
Use `from_time` and optionally `to_time` for the first page. Subsequent requests
use only `cursor` and `limit`; the iterator handles this automatically. Limits are
1–200. Times must have a timezone and are sent in UTC. The server validates history
age/range against its current limits. Iteration is lazy and makes one request per
page; it does not throttle automatically. An error can occur after earlier items
have already been yielded. Repeated cursors and inconsistent continuation metadata
raise `ProtocolError`.

Money, trading prices and trading volumes are `Decimal` parsed directly from decimal
strings. Floats, NaN and scientific notation in these wire fields are rejected.
Optional fields remain `None` when absent; unknown response fields are retained in
Pydantic's `model_extra`. Unknown enum values fail validation so contract changes
are visible. `model_dump_json()` serializes Decimal values as strings.

## Candles, conditions and operation status

```python
conditions = client.get_instrument_conditions("EURUSD")
print(conditions.point_digits, conditions.volume_step)

candles = client.get_candle_history(
    instrument="EURUSD",
    timeframe="M1",
    from_time=start,
    count=10,
    price_type="both",
)
for candle in candles.candles:
    print(candle.time, candle.bid, candle.ask)

# Read an operation ID you already have; this does not submit a trade.
operation = client.get_operation_status("789")
print(operation.status, operation.is_terminal)
```

These calls belong inside the client context. `from_time` is required for candles;
provide exactly one of `to_time` (exclusive) or a nonzero `count`. Negative counts
are valid. `price_type` defaults to `bid`; `ask` and `both` are supported. Supported
timeframes: S1, S5, S15, M1, M3, M5, M10, M15, M30, H1, H2, H4, D1, W1, MN.
The server enforces current count/range caps from `get_limits()`; this method does
not silently fetch limits, split requests or sort results.

Candle OHLC and volume fields are finite JSON numbers exposed as `float`, as
specified by market data. Single-side candles use `open`/`high`/`low`/`close`;
`both` uses nested `bid` and `ask`. Missing fields stay `None`. The SDK preserves
both shapes; see the [upstream oneOf ambiguity](contracts/README.md#candle-schema-ambiguity).
Instrument constraints, swaps and commission amounts use exact `Decimal` values.

Operation states are `pending`, `confirmed`, `rejected`, `failed`. `is_terminal`
means processing ended, not success. Rejection/failure codes are returned as model
fields; HTTP failures still raise `APIError`. There is no automatic polling.
`examples/read_market.py` demonstrates these reads using local environment inputs.

## Errors

- `ConfigurationError`: invalid key, account ID, URL or timeout.
- `ValueError`: invalid history arguments, before any network request.
- `TransportError`: HTTPX network/timeout failure, sanitized message.
- `APIError`: non-200 HTTP response, with `status_code`, optional numeric `code`,
  `error_message`, and raw `retry_after` header.
- `ProtocolError`: malformed JSON, response schema mismatch or invalid discovery.

The SDK does not automatically retry any request, including 429. Consult limits
and decide when to retry in your application. Exception text excludes raw response
bodies and signed headers; `APIError.error_message` is untrusted server text.

## Development

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
uv build
```

Ruff enforces annotations and formatting; mypy runs in strict mode over source,
tests and examples. `py.typed` ships in the wheel. pytest uses synthetic keys,
HTTPX MockTransport and loopback HTTPS servers, with a 90% branch-aware coverage gate.
Hypothesis generates bounded reproducible cases for exact decimals, key formats,
origins, cursor encoding/signing and pagination termination. Real TLS tests cover
header/body timeouts, truncated responses, redirect refusal, signatures, connection
reuse and closure. Test certificates are generated in temporary directories and
explicitly trusted; TLS verification stays enabled. Local socket binding is required,
but no Exness account or external service is used. Async tests also verify task
cancellation before headers and during body reads, discovery coordination and
cleanup after cancellation. The synchronous client has no task-cancellation API.
CI runs checks on
Python 3.11–3.14, builds the distribution, and smoke-tests the installed wheel.
See [the contract notes](contracts/README.md) for upstream sources and update rules.

Check for public API changes with `uv run python -m tools.check_contract` from this
checkout. Use `--candidate PATH` for offline comparison and `--json-report PATH`
for a full report. Exit codes: 0 unchanged, 1 review needed, 2 check failed. The
command leaves the pinned snapshot untouched; see the
[contract maintenance workflow](contracts/README.md#checking-for-upstream-changes).

See [ROADMAP.md](ROADMAP.md) for development that can proceed without a live account.
SDK code is available under the [MIT license](LICENSE); third-party reference
material is identified in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Trading events

```python
from contextlib import aclosing
from exness import TradingStreamGap, TradingStreamOptions
from exness.trading import TradingSnapshotEvent

async with aclosing(
    client.stream_trading(
        options=TradingStreamOptions(max_reconnects=3),
    )
) as stream:
    async for event in stream:
        if isinstance(event, TradingStreamGap):
            print("State is stale; waiting for a fresh snapshot")
        elif isinstance(event, TradingSnapshotEvent):
            print("New baseline", len(event.payload.positions))
        else:
            print("Changed positions", len(event.payload.positions), event.status)
```

`client` is an `AsyncExnessClient`. Run `uv run python examples/read_trading.py`
with the same four `EXN_*` variables as the other examples. Models live in
`exness.trading`; options and gap types are also exported from `exness`.
`TradingStreamOptions` and `TradingStreamGap` alias the tick equivalents so both
streams have identical transport settings. Use `aclosing` on early exit.

The `transactions` subscription sends a full `trading_state_snapshot`, followed by
`transaction_event` deltas. The SDK requires a valid snapshot before yielding any
transaction on **every** connection, including after reconnect. A transaction
received before that boundary is a terminal protocol error. Repeated snapshots
are accepted as replacement baselines. Each subscription uses a fresh request ID.

Recovery belongs to the caller's application state:

1. On a gap, mark the previous cache stale immediately; on termination (including
   normal close), it is no longer live either.
2. Replace cached active orders, open positions and account state with the next
   snapshot. Do not merge a new baseline into stale state.
3. Apply subsequent transaction payloads as **changed entities**, not full lists.
   Retain an open position after a partial close; remove closed positions and
   terminal orders from an active-state view. Missing entities are not deletions.
4. Use order/deal history separately if missed historical changes matter. Snapshots
   cannot reconstruct missed transactions. There is no resume token, replay or
   exactly-once guarantee; the SDK does not merge, deduplicate or poll implicitly.

`status` is optional: `success`, `rejected` and `failed` are supported. Rejected or
failed operations with empty payloads are still **data events**, even when they
contain `error_message`; subscription errors instead raise/return `WebSocketAPIError`.
An absent status is not inferred as success. `operation_id` can be absent for
balance or system changes. Monetary values remain exact decimals; transaction
account state also requires `leverage`. Server error fields are untrusted data.

Transport behavior is shared with ticks: reconnects are opt-in and lifetime-bounded,
gaps precede retries, HTTP/TLS/protocol failures are terminal, and cancellation and
client closure release sockets. The default message cap is 64 KiB, including the
initial snapshot; explicitly increase it up to 16 MiB for larger accounts. There
is no snapshot-arrival timeout beyond the caller's stream deadline and heartbeat.
Other server subscriptions are documented below.
Live Exness interoperability has not been verified.

## Account, instrument and HMR events

```python
from contextlib import aclosing
from exness import ServerStreamGap, ServerStreamOptions
from exness.events import HmrSnapshot

async with aclosing(
    client.stream_hmr(
        ["EURUSD"],
        options=ServerStreamOptions(max_reconnects=3),
    )
) as stream:
    async for event in stream:
        if isinstance(event, ServerStreamGap):
            print("HMR state is stale")
        elif isinstance(event, HmrSnapshot):
            print("Replace baseline", len(event.periods))
        else:
            print("Changes", event.upserted_periods, event.removed_period_ids)
```

The asyncio client also provides `stream_account_state()` and
`stream_instruments(["EURUSD"])` (or `["all"]`). Models live in `exness.events`;
`ServerStreamOptions` and `ServerStreamGap` are exported from `exness`. Use
`aclosing` when stopping early. Run one of these examples with the four `EXN_*`
variables already configured:

```sh
uv run python examples/read_events.py account_state
uv run python examples/read_events.py instruments all
uv run python examples/read_events.py hmr EURUSD XAUUSD
```

Each call opens a dedicated `/ws/events` connection with one subscription and a
fresh request ID. Multiple concurrent calls consume multiple server connections;
check `GetLimits` / `get_limits` when choosing concurrency. These methods do not
multiplex subscriptions or change instrument lists mid-connection. Close the
iterator to unsubscribe by closing its connection; open a new stream to change
selection. Reconnects reuse the copied selection and the existing signed transport.
Options and gap types alias their tick equivalents; retries remain disabled by
default, bounded when enabled, and preceded by visible gaps. Message/queue limits,
heartbeat, redirect refusal and cancellation rules are unchanged.

| Subscription | Events | Initial state and recovery |
| --- | --- | --- |
| `account_state` | `AccountStateEvent` | No initial snapshot. Each event is a fresh floating-state observation; a gap makes the prior observation stale until the next event. |
| `instruments` | `InstrumentEvent`, action `new`, `upd`, `del` | No initial snapshot. `new`/`upd` contain full conditions; `del` removes the instrument from a cache. Missed changes or deletions require explicit reconciliation. |
| `hmr` | `HmrSnapshot`, `HmrUpdate` | A fresh snapshot is mandatory before updates on every connection. Every snapshot, even empty, replaces the entire baseline. |

Instrument subscriptions require a nonempty explicit list or exactly `["all"]`.
HMR requires explicit instruments and rejects `all`. Duplicate or malformed names
fail before networking; account availability is checked by the server, without a
hidden REST lookup. Unsubscribed instrument events and unknown/wrong event kinds
are terminal protocol errors. Lists are copied when constructing the iterator.

An account-state event contains exact balance, equity and used-margin values. It
is an observation, not a transaction history or a substitute for `transactions`.
The SDK does not assume a fixed delivery interval or generate synthetic values.
For instrument caches, treat a gap or termination as loss of continuity. Explicitly
refresh the available instrument list and conditions through REST when needed;
that REST fetch and concurrent WebSocket delivery are **not an atomic snapshot**.
The SDK does not silently merge a REST result with events or mark the entire cache
current merely because one instrument received an update.

HMR arrays live at the event top level, without `payload` or `event_time`.
Snapshots contain `periods`; updates contain `upserted_periods` and
`removed_period_ids`, both required even when empty. Applications maintain their
own map keyed by `period_id`: replace it on a snapshot, upsert changed periods and
remove the listed IDs on updates. Removing an unknown ID is harmless. A gap or
termination invalidates the baseline; the SDK refuses an update before the new
snapshot. It yields data without automatic merging, sorting, replay or deduplication.

A period preserves the server's complete `instruments` list (not filtered locally)
and string leverage. `regular` schedule strings use calendar dates, while `periodic`
strings use weekly schedules such as `Mon, 13:00:00`. Both are preserved verbatim;
no timezone, recurrence calculation or "active now" flag is inferred. All synthetic
fixtures are shared across SDKs and checked against the pinned OpenAPI. Live Exness
interoperability remains unverified.

## Reviewed alpha API

Prefer `StreamOptions` and `StreamGap` for all five stream types. Existing
`TickStream*`, `TradingStream*` and `ServerStream*` aliases remain supported.
[API_REVIEW.md](API_REVIEW.md) maps the full Python/Go surface and records model,
error, lifecycle and timeout differences. [RELEASE.md](RELEASE.md) contains the
alpha release notes, installation instructions and publication procedure.
