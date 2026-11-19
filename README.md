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
- Structured errors, finite timeouts, no automatic retries or redirects.

Python 3.11+. The current release is synchronous and read-only. Async,
WebSocket streams and trading operations are follow-up work.
The companion [Go SDK](https://github.com/angru/exness-go) has the same read-only operations
and shared synthetic response/signing fixtures. Each repository builds independently.

## Installation and local setup

Until a PyPI release is available, install directly from GitHub:

```sh
uv add "git+https://github.com/angru/exness-python.git@main"
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
tests and examples. `py.typed` ships in the wheel. pytest uses synthetic keys and
HTTPX MockTransport, with a 90% branch-aware coverage gate. CI runs checks on
Python 3.11–3.14, builds the distribution, and smoke-tests the installed wheel.
See [the contract notes](contracts/README.md) for upstream sources and update rules.

See [ROADMAP.md](ROADMAP.md) for development that can proceed without a live account.
SDK code is available under the [MIT license](LICENSE); third-party reference
material is identified in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
