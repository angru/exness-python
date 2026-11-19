import asyncio
import json
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import aclosing
from decimal import Decimal

import httpx
import pytest
from conftest import fixture_bytes
from test_client import START, discovery, verify_request

from exness import APIError, AsyncExnessClient, Credentials, ProtocolError, TransportError
from exness.models import APIModel

pytestmark = pytest.mark.anyio


def async_client_for(
    credentials: Credentials,
    handler: Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]],
) -> AsyncExnessClient:
    return AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(handler),
    )


async def test_all_read_operations_and_wire_signatures(credentials: Credentials) -> None:
    requests: list[httpx.Request] = []
    fixtures = {
        "/v1/configuration/accounts/123456/account": "account",
        "/v1/configuration/accounts/123456/instruments": "instruments",
        "/v1/trading/accounts/123456/snapshot": "snapshot",
        "/v1/configuration/accounts/123456/limits": "limits",
        "/v1/configuration/accounts/123456/instruments/EURUSD/conditions": "conditions",
        "/v1/trading/accounts/123456/operations/000789": "operation_confirmed",
        "/v1/market-data/accounts/123456/candles": "candles_both",
        "/v1/history/accounts/123456/orders": "orders",
        "/v1/history/accounts/123456/deals": "deals",
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        requests.append(request)
        if request.url.host == "gateway.example":
            assert dict(request.url.params) == {"account_id": "123456"}
            return discovery()
        assert request.url.host == "account.example"
        return httpx.Response(200, content=fixture_bytes(fixtures[request.url.path]))

    async with async_client_for(credentials, handler) as client:
        assert (await client.get_account()).account_id == "123456"
        assert (await client.get_instruments()).instruments == ["EURUSD", "XAUUSD"]
        assert (await client.get_snapshot()).account_state.balance == Decimal(
            "12345678901234567890.123456789"
        )
        assert (await client.get_limits()).limits.rest.methods
        assert (await client.get_instrument_conditions("EURUSD")).volume_step == Decimal("0.01")
        assert (await client.get_operation_status("000789")).is_terminal
        assert (
            await client.get_candle_history(
                instrument="EURUSD", timeframe="H1", from_time=START, count=-10, price_type="both"
            )
        ).candles[0].bid is not None
        assert (await client.get_orders_history(from_time=START, order_state="filled")).items
        assert (await client.get_deals_history(from_time=START, deal_type="balance")).items
    assert len(requests) == 10
    assert dict(requests[7].url.params) == {
        "instrument": "EURUSD",
        "timeframe": "H1",
        "from": START.isoformat(),
        "count": "-10",
        "price_type": "both",
    }
    assert dict(requests[8].url.params) == {
        "from": START.isoformat(),
        "limit": "100",
        "order_state": "filled",
    }
    assert dict(requests[9].url.params) == {
        "from": START.isoformat(),
        "limit": "100",
        "deal_type": "balance",
    }


@pytest.mark.parametrize("kind", ["orders", "deals"])
@pytest.mark.parametrize("stop_early", [False, True])
async def test_lazy_pagination_and_cursor_encoding(
    credentials: Credentials, kind: str, stop_early: bool
) -> None:
    pages: list[httpx.Request] = []
    data = json.loads(fixture_bytes(kind))
    cursor = "游标 🌍 +/=&?#%"
    data.update(next_cursor=cursor, has_more=True)

    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        if request.url.host == "gateway.example":
            return discovery()
        pages.append(request)
        assert request.url.path == f"/v1/history/accounts/123456/{kind}"
        return httpx.Response(
            200, json=data if len(pages) == 1 else {"items": [], "has_more": False}
        )

    async with async_client_for(credentials, handler) as client:
        rows = (
            client.iter_orders_history(
                from_time=START, instrument="EURUSD", order_state="filled", limit=17
            )
            if kind == "orders"
            else client.iter_deals_history(
                from_time=START, instrument="EURUSD", deal_type="balance", limit=17
            )
        )
        assert not pages
        # aclosing explicitly closes a partially consumed async generator.
        async with aclosing(rows):
            async for _ in rows:
                if stop_early:
                    break
        assert len(pages) == (1 if stop_early else 2)
        if not stop_early:
            assert dict(pages[1].url.params) == {"cursor": cursor, "limit": "17"}


@pytest.mark.parametrize(
    "metadata",
    [
        {"has_more": True},
        {"has_more": False, "next_cursor": "x"},
        {"has_more": True, "next_cursor": "x"},
    ],
)
async def test_bad_pagination_terminates(
    credentials: Credentials, metadata: dict[str, object]
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.host == "gateway.example":
            return discovery()
        calls += 1
        assert calls <= 2
        return httpx.Response(200, json={"items": [], **metadata})

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(ProtocolError):
            _ = [item async for item in client.iter_orders_history(from_time=START)]


@pytest.mark.parametrize("status", [307, 401, 429, 503])
async def test_http_error_not_retried_or_followed(credentials: Credentials, status: int) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status,
            json={"code": 123, "error_message": "private server text"},
            headers={"Location": "https://elsewhere.example", "Retry-After": "12"},
        )

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(APIError) as caught:
            await client.discover()
        assert caught.value.status_code == status and caught.value.code == 123
        assert caught.value.retry_after == "12"
        assert "private" not in str(caught.value)
    assert calls == 1


@pytest.mark.parametrize(
    "body",
    [
        b"not JSON",
        b"[]",
        b'{"access_point":"http://unsafe.example"}',
        b'{"access_point":"https://account.example","account_id":"999"}',
    ],
)
async def test_bad_discovery_can_be_retried_explicitly(
    credentials: Credentials, body: bytes
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=body) if calls == 1 else discovery()

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(ProtocolError):
            await client.discover()
        assert await client.discover() == "https://account.example"
        assert calls == 2


@pytest.mark.parametrize(
    "operation",
    [
        lambda c: c.get_instrument_conditions(".."),
        lambda c: c.get_operation_status("18446744073709551616"),
        lambda c: c.get_candle_history(
            instrument="EURUSD", timeframe="M1", from_time=START, count=0
        ),
        lambda c: c.get_orders_history(cursor="x", from_time=START),
        lambda c: c.get_deals_history(from_time=START, limit=0),
    ],
)
async def test_invalid_arguments_before_discovery(
    credentials: Credentials, operation: Callable[[AsyncExnessClient], Awaitable[APIModel]]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid arguments made a request")

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(ValueError):
            await operation(client)


@pytest.mark.parametrize(
    "operation,body",
    [
        (lambda c: c.get_account(), {"account_id": "999", "create_date": "2026-01-01T00:00:00Z"}),
        (lambda c: c.get_instrument_conditions("EURUSD"), {"instrument": "XAUUSD"}),
        (
            lambda c: c.get_operation_status("789"),
            {
                "operation_id": "999",
                "status": "pending",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ),
    ],
)
async def test_response_identity(
    credentials: Credentials,
    operation: Callable[[AsyncExnessClient], Awaitable[APIModel]],
    body: dict[str, str],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return (
            discovery() if request.url.host == "gateway.example" else httpx.Response(200, json=body)
        )

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(ProtocolError):
            await operation(client)


async def test_concurrent_discovery_is_shared(credentials: Credentials) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    discoveries = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal discoveries
        verify_request(request)
        if request.url.host == "gateway.example":
            discoveries += 1
            entered.set()
            await release.wait()
            return discovery()
        return httpx.Response(200, content=fixture_bytes("account"))

    async with async_client_for(credentials, handler) as client, asyncio.TaskGroup() as group:
        tasks = [group.create_task(client.get_account()) for _ in range(10)]
        await asyncio.wait_for(entered.wait(), 3)
        release.set()
        accounts = await asyncio.wait_for(asyncio.gather(*tasks), 3)
        assert len(accounts) == 10 and discoveries == 1


async def test_cancel_waiter_then_owner_releases_discovery_lock(credentials: Credentials) -> None:
    entered = asyncio.Event()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await asyncio.Event().wait()
        return discovery()

    async with async_client_for(credentials, handler) as client, asyncio.TaskGroup() as group:
        owner = group.create_task(client.discover())
        await asyncio.wait_for(entered.wait(), 3)
        waiter = group.create_task(client.discover())
        await asyncio.sleep(0)  # Let the second task enter the discovery lock queue.
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not owner.done() and calls == 1
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert await asyncio.wait_for(client.discover(), 3) == "https://account.example"
        assert calls == 2


async def test_refresh_preserves_cache_until_success(credentials: Credentials) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return discovery()
        if calls == 2:
            raise httpx.ConnectError("secret details")
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"access_point": "https://migrated.example"})

    async with async_client_for(credentials, handler) as client, asyncio.TaskGroup() as group:
        assert await client.discover() == "https://account.example"
        with pytest.raises(TransportError) as caught:
            await client.discover(refresh=True)
        assert "secret" not in str(caught.value)
        assert caught.value.__suppress_context__
        task = group.create_task(client.discover(refresh=True))
        await asyncio.wait_for(entered.wait(), 3)
        assert await asyncio.wait_for(client.discover(), 1) == "https://account.example"
        release.set()
        assert await asyncio.wait_for(task, 3) == "https://migrated.example"
        assert await client.discover() == "https://migrated.example"
        assert calls == 3


async def test_closed_client_and_owned_transport(credentials: Credentials) -> None:
    class OwnedTransport(httpx.MockTransport):
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    transport = OwnedTransport(lambda request: discovery())
    client = AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=transport,
    )
    with pytest.raises(ValueError, match="caller failed"):
        async with client:
            await client.discover()
            raise ValueError("caller failed")
    assert transport.closed
    await client.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await client.get_account()
    with pytest.raises(RuntimeError, match="closed"):
        async with client:
            pytest.fail("reopened closed client")


async def test_cancel_history_page_does_not_prefetch_or_close_client(
    credentials: Credentials,
) -> None:
    entered = asyncio.Event()
    pages = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pages
        if request.url.host == "gateway.example":
            return discovery()
        if request.url.path.endswith("/orders"):
            pages += 1
            if pages == 1:
                return httpx.Response(200, content=fixture_bytes("orders"))
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(200, content=fixture_bytes("account"))

    async with async_client_for(credentials, handler) as client:
        async with aclosing(client.iter_orders_history(from_time=START)) as rows:
            assert (await anext(rows)).order.order_id == "1002"
            assert pages == 1
            async with asyncio.TaskGroup() as group:
                task = group.create_task(anext(rows))
                await asyncio.wait_for(entered.wait(), 3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            with pytest.raises(StopAsyncIteration):
                await anext(rows)
        assert (await client.get_account()).account_id == "123456"
        assert pages == 2
