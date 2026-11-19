import asyncio
import json
import ssl
from collections.abc import AsyncGenerator, Callable
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
    AsyncExnessClient,
    Credentials,
    ProtocolError,
    ServerStreamGap,
    ServerStreamOptions,
    WebSocketAPIError,
)
from exness.events import (
    AccountStateEvent,
    HmrSnapshot,
    HmrUpdate,
    InstrumentEvent,
    _account_decoder,
    _hmr_decoder,
    _instrument_decoder,
    _subscription_instruments,
)

pytestmark = pytest.mark.anyio
Event = AccountStateEvent | InstrumentEvent | HmrSnapshot | HmrUpdate | ServerStreamGap


def open_stream(
    client: AsyncExnessClient,
    kind: str,
    names: list[str],
    context: ssl.SSLContext | None,
    options: ServerStreamOptions | None = None,
) -> AsyncGenerator[Event, None]:
    if kind == "account_state":
        return client.stream_account_state(options=options, ssl_context=context)
    if kind in ("instruments", "all"):
        return client.stream_instruments(names, options=options, ssl_context=context)
    return client.stream_hmr(names, options=options, ssl_context=context)


def first_fixture(kind: str) -> str:
    return {
        "account_state": "account_state_event",
        "instruments": "instrument_new",
        "all": "instrument_new",
        "hmr": "hmr_snapshot",
    }[kind]


@pytest.mark.parametrize("kind", ["account_state", "instruments", "all", "hmr"])
async def test_signed_subscriptions_reconnect_and_models(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
) -> None:
    ids: list[str] = []
    names = ["all"] if kind == "all" else ["EURUSD"]
    expected: dict[str, object] = {"event": "instruments" if kind == "all" else kind}
    if kind != "account_state":
        expected["instruments"] = names.copy()

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
        assert request["subscribe"] == expected
        ids.append(request["id"])
        await ws.send(fixture_bytes(first_fixture(kind)).decode())
        if len(ids) == 1:
            ws.transport.abort()
            return
        if kind == "hmr":
            await ws.send(fixture_bytes("hmr_update").decode())
            await ws.send(fixture_bytes("hmr_empty_snapshot").decode())
        if kind in ("instruments", "all"):
            await ws.send(fixture_bytes("instrument_upd").decode())
            await ws.send(fixture_bytes("instrument_del").decode())
        await ws.close()

    async with tick_client(credentials, tls_contexts, handler) as client:
        stream = open_stream(
            client,
            kind,
            names,
            tls_contexts[1],
            ServerStreamOptions(max_reconnects=1, reconnect_delay=0.001),
        )
        names[0] = "XAUUSD"  # The subscription owns its copy before lazy iteration.
        async with asyncio.timeout(5), aclosing(stream):
            rows = [event async for event in stream]
    assert len(ids) == len(set(ids)) == 2
    assert isinstance(rows[1], ServerStreamGap) and rows[1].reconnect_attempt == 1
    if kind == "account_state":
        assert len(rows) == 3 and isinstance(rows[2], AccountStateEvent)
        assert rows[2].payload.account_state.balance == Decimal("12345678901234567890.123456789")
    elif kind == "hmr":
        assert isinstance(rows[2], HmrSnapshot) and len(rows[2].periods) == 2
        assert rows[2].periods[1].start_time == "Mon, 13:00:00"
        assert rows[2].periods[0].start_time == "2026-11-19 11:00:00"
        assert rows[2].periods[0].instruments == ["EURUSD", "EURGBP"]
        assert isinstance(rows[3], HmrUpdate) and rows[3].removed_period_ids == ["701", "999"]
        assert isinstance(rows[4], HmrSnapshot) and rows[4].periods == []
    else:
        assert [event.action for event in rows if isinstance(event, InstrumentEvent)] == [
            "new",
            "new",
            "upd",
            "del",
        ]


async def test_hmr_reconnect_needs_new_snapshot(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await ws.recv()
        if calls == 1:
            await ws.send(fixture_bytes("hmr_snapshot").decode())
            ws.transport.abort()
        else:
            await ws.send(fixture_bytes("hmr_update").decode())
            await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        asyncio.timeout(5),
        aclosing(
            client.stream_hmr(
                ["EURUSD"],
                options=ServerStreamOptions(max_reconnects=3, reconnect_delay=0.001),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        assert isinstance(await anext(stream), HmrSnapshot)
        assert isinstance(await anext(stream), ServerStreamGap)
        with pytest.raises(ProtocolError, match="before HMR snapshot"):
            await anext(stream)
    assert calls == 2


@pytest.mark.parametrize("kind", ["account_state", "instruments", "hmr"])
@pytest.mark.parametrize("mode", ["cancel", "close-paused", "early-exit"])
async def test_stream_cleanup(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
    mode: str,
) -> None:
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(fixture_bytes(first_fixture(kind)).decode())
        await ws.wait_closed()
        closed.set()

    async with tick_client(credentials, tls_contexts, handler) as client:
        async with aclosing(open_stream(client, kind, ["EURUSD"], tls_contexts[1])) as stream:
            await anext(stream)
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


@pytest.mark.parametrize("kind", ["account_state", "instruments", "hmr"])
async def test_invalid_wire_event_is_terminal(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await ws.recv()
        await ws.send('{"event_type":"unexpected"}')
        await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            open_stream(
                client, kind, ["EURUSD"], tls_contexts[1], ServerStreamOptions(max_reconnects=2)
            )
        ) as stream,
    ):
        with pytest.raises(ProtocolError):
            await anext(stream)
    assert calls == 1


def decoders() -> list[Callable[[str | bytes], object]]:
    return [
        _account_decoder("synthetic-events-1"),
        _instrument_decoder(("EURUSD",), "synthetic-events-1"),
        _hmr_decoder("synthetic-events-1"),
    ]


async def test_invalid_inputs_before_network(credentials: Credentials) -> None:
    def unexpected(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid subscription made a network request")

    async with AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(unexpected),
    ) as client:
        for names in ([], [""], ["EURUSD", "EURUSD"], ["all", "EURUSD"], ["bad/name"]):
            for kind in ("instruments", "hmr"):
                with pytest.raises(ValueError):
                    open_stream(client, kind, names, None)
        with pytest.raises(ValueError):
            client.stream_hmr(["all"])
    assert _subscription_instruments(["all"], allow_all=True) == ("all",)


async def test_protocol_errors_and_subscription_correlation() -> None:
    for decode in decoders():
        for value in ("bad JSON", "[]", "null", "{}", '{"event_type":null}'):
            with pytest.raises(ProtocolError):
                decode(value)
        for request_id in ("synthetic-events-1", "other"):
            with pytest.raises(
                WebSocketAPIError if request_id != "other" else ProtocolError
            ) as caught:
                decode(
                    json.dumps({"id": request_id, "code": 3000, "error_message": "private details"})
                )
            assert "private details" not in str(caught.value)
        with pytest.raises(ProtocolError):
            decode('{"id":"synthetic-events-1","code":3000}')


async def test_required_fields_scopes_and_hmr_ordering() -> None:
    for name, decode, required in (
        ("account_state_event", _account_decoder("id"), ["event_time", "payload"]),
        (
            "instrument_new",
            _instrument_decoder(("EURUSD",), "id"),
            ["event_time", "action", "payload"],
        ),
        ("hmr_snapshot", _hmr_decoder("id"), ["periods"]),
        ("hmr_update", _hmr_decoder("id"), ["upserted_periods", "removed_period_ids"]),
    ):
        for field in required:
            for omit in (True, False):
                value = json.loads(fixture_bytes(name))
                if omit:
                    del value[field]
                else:
                    value[field] = None
                with pytest.raises(ProtocolError):
                    decode(json.dumps(value))
    with pytest.raises(ProtocolError, match="unsubscribed"):
        _instrument_decoder(("XAUUSD",), "id")(fixture_bytes("instrument_new"))
    assert _instrument_decoder(("all",), "id")(fixture_bytes("instrument_new")).action == "new"
    for field in ("period_id", "event_type", "instruments", "leverage", "start_time", "end_time"):
        value = json.loads(fixture_bytes("hmr_snapshot"))
        del value["periods"][0][field]
        with pytest.raises(ProtocolError):
            _hmr_decoder("id")(json.dumps(value))
    for field, bad in (
        ("period_id", "bad"),
        ("event_type", "unknown"),
        ("instruments", []),
        ("instruments", ["bad/name"]),
        ("leverage", 10),
    ):
        value = json.loads(fixture_bytes("hmr_snapshot"))
        value["periods"][0][field] = bad
        with pytest.raises(ProtocolError):
            _hmr_decoder("id")(json.dumps(value))
    decode_hmr = _hmr_decoder("id")
    with pytest.raises(ProtocolError, match="before HMR snapshot"):
        decode_hmr(fixture_bytes("hmr_update"))
    decode_hmr(fixture_bytes("hmr_empty_snapshot"))
    assert isinstance(decode_hmr(fixture_bytes("hmr_update")), HmrUpdate)
    with pytest.raises(ProtocolError):
        decode_hmr('{"event_type":"hmr_update","upserted_periods":[],"removed_period_ids":["bad"]}')
