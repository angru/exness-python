import asyncio
import json
import ssl
from contextlib import aclosing
from decimal import Decimal

import httpx
import pytest
from conftest import fixture_bytes
from test_client import verify_request
from test_network import tls_contexts as tls_contexts
from test_ticks import tick_client
from websockets.asyncio.server import ServerConnection

from exness import (
    Credentials,
    ProtocolError,
    TradingStreamGap,
    TradingStreamOptions,
    WebSocketAPIError,
)
from exness.trading import TradingSnapshotEvent, TransactionEvent, _trading_decoder

pytestmark = pytest.mark.anyio


async def test_snapshot_reconnect_boundary_and_transaction_results(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    ids: list[str] = []

    async def handler(ws: ServerConnection) -> None:
        assert ws.request is not None
        assert ws.request.path == "/v1/server-events/accounts/123456/ws/events"
        verify_request(
            httpx.Request(
                "GET",
                "https://account.example" + ws.request.path,
                headers=list(ws.request.headers.raw_items()),
            )
        )
        request = json.loads(await ws.recv())
        assert request["subscribe"] == {"event": "transactions"}
        ids.append(request["id"])
        await ws.send(fixture_bytes("trading_snapshot").decode())
        if len(ids) == 1:
            await ws.send(fixture_bytes("transaction_partial_close").decode())
            ws.transport.abort()
        else:
            await ws.send(fixture_bytes("transaction_balance").decode())
            await ws.send(fixture_bytes("transaction_rejected").decode())
            await ws.close()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        asyncio.timeout(5),
        aclosing(
            client.stream_trading(
                options=TradingStreamOptions(max_reconnects=1, reconnect_delay=0.001),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        rows = [event async for event in stream]
    assert [type(row) for row in rows] == [
        TradingSnapshotEvent,
        TransactionEvent,
        TradingStreamGap,
        TradingSnapshotEvent,
        TransactionEvent,
        TransactionEvent,
    ]
    assert len(set(ids)) == 2
    partial = rows[1]
    assert isinstance(partial, TransactionEvent)
    assert partial.payload.positions[0].volume == Decimal("0.005")
    balance = rows[4]
    assert isinstance(balance, TransactionEvent) and balance.operation_id is None
    assert balance.payload.account_state is not None
    assert balance.payload.account_state.leverage == "100"
    assert balance.payload.account_state.balance == Decimal("12345678901234567890.123456789")
    rejected = rows[5]
    assert isinstance(rejected, TransactionEvent) and rejected.status == "rejected"
    assert rejected.error_message == "SYNTHETIC_REJECTION" and rejected.payload.orders == []


async def test_reconnect_requires_new_snapshot(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await ws.recv()
        if calls == 1:
            await ws.send(fixture_bytes("trading_snapshot").decode())
            ws.transport.abort()
        else:
            await ws.send(fixture_bytes("transaction_balance").decode())
            await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        asyncio.timeout(5),
        aclosing(
            client.stream_trading(
                options=TradingStreamOptions(max_reconnects=3, reconnect_delay=0.001),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        assert isinstance(await anext(stream), TradingSnapshotEvent)
        assert isinstance(await anext(stream), TradingStreamGap)
        with pytest.raises(ProtocolError, match="before trading snapshot"):
            await anext(stream)
    assert calls == 2


@pytest.mark.parametrize("mode", ["cancel", "close-paused", "early-exit"])
async def test_trading_cleanup(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext], mode: str
) -> None:
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(fixture_bytes("trading_snapshot").decode())
        await ws.wait_closed()
        closed.set()

    async with tick_client(credentials, tls_contexts, handler) as client:
        async with aclosing(client.stream_trading(ssl_context=tls_contexts[1])) as stream:
            assert isinstance(await anext(stream), TradingSnapshotEvent)
            if mode == "cancel":
                task = asyncio.create_task(anext(stream))
                await asyncio.sleep(0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif mode == "close-paused":
                await client.aclose()
                await asyncio.wait_for(closed.wait(), 3)
                with pytest.raises(RuntimeError, match="closed"):
                    await anext(stream)
        await asyncio.wait_for(closed.wait(), 3)


@pytest.mark.parametrize(
    "message",
    [
        '{"event_type":"transaction_event","payload":{"account_state":null}}',
        "bad JSON",
        "[]",
        "null",
        "{}",
        '{"event_type":"account_state_event","payload":{}}',
        '{"event_type":"transaction_event"}',
        '{"event_type":"transaction_event","payload":null}',
        '{"event_type":"trading_state_snapshot","payload":{}}',
        '{"event_type":"transaction_event","status":"unknown","payload":{}}',
        '{"event_type":"transaction_event","payload":{"orders":[{}]}}',
        '{"event_type":"transaction_event","payload":{"positions":[{}]}}',
        '{"event_type":"transaction_event","payload":{"deals":[{}]}}',
        '{"event_type":"transaction_event","payload":{"account_state":{"balance":"1","equity":"1","used_margin":"0"}}}',
        '{"event_type":"transaction_event","payload":{"orders":null}}',
        '{"event_type":"transaction_event","operation_id":"bad","payload":{}}',
        '{"id":"synthetic-transactions-1","code":3000}',
    ],
)
async def test_invalid_trading_messages(message: str) -> None:
    decode = _trading_decoder("synthetic-transactions-1")
    decode(fixture_bytes("trading_snapshot"))
    with pytest.raises(ProtocolError):
        decode(message)


async def test_snapshot_gate_subscription_error_and_optional_fields() -> None:
    decode = _trading_decoder("synthetic-transactions-1")
    with pytest.raises(ProtocolError, match="before trading snapshot"):
        decode(fixture_bytes("transaction_rejected"))
    for request_id in ("synthetic-transactions-1", "other"):
        message = json.dumps({"id": request_id, "code": 3000, "error_message": "private details"})
        with pytest.raises(WebSocketAPIError if request_id != "other" else ProtocolError) as caught:
            decode(message)
        assert "private details" not in str(caught.value)
    first = decode(fixture_bytes("trading_snapshot"))
    again = decode(fixture_bytes("trading_snapshot"))
    assert isinstance(first, TradingSnapshotEvent) and first == again
    empty = decode('{"event_type":"transaction_event","payload":{}}')
    assert isinstance(empty, TransactionEvent) and empty.status is None
    assert empty.payload.account_state is None
    for status in ("rejected", "failed"):
        event = decode(
            json.dumps(
                {
                    "event_type": "transaction_event",
                    "status": status,
                    "error_message": "private business error",
                    "payload": {},
                }
            )
        )
        assert isinstance(event, TransactionEvent) and event.status == status
