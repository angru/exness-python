import asyncio
import json
import logging
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager

import httpx
import pytest
from conftest import fixture_bytes
from test_client import verify_request
from test_network import tls_contexts as tls_contexts
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from exness import (
    APIError,
    AsyncExnessClient,
    Credentials,
    ProtocolError,
    StreamClosed,
    TickStreamGap,
    TickStreamOptions,
    TransportError,
    WebSocketAPIError,
)
from exness.models import Tick
from exness.ticks import _instruments, _tick

pytestmark = pytest.mark.anyio


@asynccontextmanager
async def tick_client(
    credentials: Credentials,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    handler: Callable[[ServerConnection], Awaitable[None]],
    process_request: Callable[[ServerConnection, Request], Response | None] | None = None,
) -> AsyncIterator[AsyncExnessClient]:
    async with serve(
        handler, "127.0.0.1", 0, ssl=contexts[0], compression=None, process_request=process_request
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        host = f"https://127.0.0.1:{port}"

        def discovery(request: httpx.Request) -> httpx.Response:
            verify_request(request)
            return httpx.Response(200, json={"access_point": host, "account_id": "123456"})

        async with AsyncExnessClient(
            credentials=credentials,
            account_id="123456",
            discovery_url="https://gateway.example",
            transport=httpx.MockTransport(discovery),
        ) as client:
            yield client


async def test_real_handshake_subscription_partial_ticks_and_cleanup(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    closed = asyncio.Event()
    received: list[dict[str, object]] = []

    async def handler(ws: ServerConnection) -> None:
        assert ws.request is not None
        assert ws.request.path == "/v1/server-events/accounts/123456/ws/ticks"
        verify_request(
            httpx.Request(
                "GET",
                "https://account.example" + ws.request.path,
                headers=list(ws.request.headers.raw_items()),
            )
        )
        received.append(json.loads(await ws.recv()))
        await ws.send(fixture_bytes("tick").decode())
        await ws.send('{"instrument":"EURUSD","bid":1.2}')
        await ws.wait_closed()
        closed.set()

    async with tick_client(credentials, tls_contexts, handler) as client:
        async with aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as rows:
            first = await asyncio.wait_for(anext(rows), 3)
            assert isinstance(first, Tick) and first.bid == 1.08321 and first.timestamp is not None
            second = await anext(rows)
            assert isinstance(second, Tick) and second.ask is None
        await asyncio.wait_for(closed.wait(), 3)
    assert received[0]["subscribe"] == {"event": "ticks", "instruments": ["EURUSD"]}
    assert isinstance(received[0]["id"], str) and received[0]["id"]


async def test_bounded_reconnect_gap_and_resubscribe(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    ids: list[str] = []

    async def handler(ws: ServerConnection) -> None:
        message = json.loads(await ws.recv())
        ids.append(message["id"])
        assert message["subscribe"] == {"event": "ticks", "instruments": ["EURUSD"]}
        await ws.send(fixture_bytes("tick").decode())
        ws.transport.abort()

    options = TickStreamOptions(max_reconnects=2, reconnect_delay=0.01, max_reconnect_delay=0.02)
    async with tick_client(credentials, tls_contexts, handler) as client:
        events: list[Tick | TickStreamGap] = []
        with pytest.raises(TransportError, match="budget exhausted"):
            async with (
                asyncio.timeout(5),
                aclosing(
                    client.stream_ticks(["EURUSD"], options=options, ssl_context=tls_contexts[1])
                ) as stream,
            ):
                async for event in stream:
                    events.append(event)
        assert [type(event) for event in events] == [Tick, TickStreamGap, Tick, TickStreamGap, Tick]
        gaps = [event for event in events if isinstance(event, TickStreamGap)]
        assert [gap.reconnect_attempt for gap in gaps] == [1, 2]
        assert [gap.delay for gap in gaps] == [0.01, 0.02]
        assert all(gap.close_code == 1006 for gap in gaps)
    assert len(ids) == len(set(ids)) == 3


@pytest.mark.parametrize("code", [1000, 1008, 1009])
async def test_close_code_policy(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext], code: int
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await ws.recv()
        await ws.close(code, "private reason")

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            client.stream_ticks(
                ["EURUSD"], options=TickStreamOptions(max_reconnects=3), ssl_context=tls_contexts[1]
            )
        ) as stream,
    ):
        if code == 1000:
            assert [event async for event in stream] == []
        else:
            with pytest.raises(StreamClosed) as caught:
                await anext(stream)
            assert caught.value.code == code and "private" not in str(caught.value)
    assert calls == 1


@pytest.mark.parametrize(
    "body",
    [
        "[]",
        "{}",
        '{"instrument":"XAUUSD","bid":1}',
        '{"instrument":"EURUSD","bid":"1.2"}',
        '{"instrument":"EURUSD","bid":1e999}',
        "broken JSON",
    ],
)
async def test_bad_messages_stop_without_retry(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext], body: str
) -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(body)
        await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        with pytest.raises(ProtocolError):
            await anext(stream)


async def test_subscription_error_is_sanitized(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    async def handler(ws: ServerConnection) -> None:
        request = json.loads(await ws.recv())
        await ws.send(
            json.dumps({"id": request["id"], "code": 3000, "error_message": "private details"})
        )
        await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        with pytest.raises(WebSocketAPIError) as caught:
            await anext(stream)
        assert caught.value.code == 3000 and "private" not in str(caught.value)


@pytest.mark.parametrize("status", [307, 401, 429, 503])
async def test_handshake_errors_do_not_redirect_or_retry(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext], status: int
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        pytest.fail("failed handshake opened a WebSocket")

    def reject(ws: ServerConnection, request: Request) -> Response:
        nonlocal calls
        calls += 1
        response = ws.respond(status, "private details")
        response.headers["Location"] = "wss://127.0.0.1:1/credential-trap"
        response.headers["Retry-After"] = "10"
        return response

    async with (
        tick_client(credentials, tls_contexts, handler, reject) as client,
        aclosing(
            client.stream_ticks(
                ["EURUSD"], options=TickStreamOptions(max_reconnects=3), ssl_context=tls_contexts[1]
            )
        ) as stream,
    ):
        with pytest.raises(APIError) as caught:
            await anext(stream)
        assert caught.value.status_code == status and caught.value.retry_after == "10"
    assert calls == 1


async def test_cancellation_and_client_close_release_socket(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    entered = asyncio.Event()
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        entered.set()
        await ws.wait_closed()
        closed.set()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        async with asyncio.TaskGroup() as group:
            task = group.create_task(anext(stream))
            await asyncio.wait_for(entered.wait(), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await asyncio.wait_for(closed.wait(), 3)


async def test_client_close_closes_paused_stream(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    closed = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(fixture_bytes("tick").decode())
        await ws.wait_closed()
        closed.set()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        await anext(stream)
        await client.aclose()
        await asyncio.wait_for(closed.wait(), 3)
        with pytest.raises(RuntimeError, match="closed"):
            await anext(stream)


async def test_no_credentials_in_debug_logs(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.close()

    async with tick_client(credentials, tls_contexts, handler) as client:
        # Restrict captured logs to the client; server tests intentionally receive headers.
        caplog.set_level(logging.DEBUG, logger="websockets.client")
        async with aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream:
            assert [event async for event in stream] == []
    assert "EXN-SIGN" not in caplog.text and "EXN-DATA" not in caplog.text


async def test_options_and_message_validation() -> None:
    for values in ([], [""], ["EURUSD", "EURUSD"], ["a" * 12], "EURUSD"):
        with pytest.raises(ValueError):
            _instruments(values)
    for count in (-1, 1001, True):
        with pytest.raises(ValueError):
            TickStreamOptions(max_reconnects=count)
    for value in (0.0, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            TickStreamOptions(reconnect_delay=value)
    with pytest.raises(ValueError):
        TickStreamOptions(max_reconnect_delay=0.5)
    with pytest.raises(ValueError):
        TickStreamOptions(max_message_bytes=0)
    with pytest.raises(ProtocolError, match="unknown request"):
        _tick(fixture_bytes("ws_error"), ("EURUSD",), "other")


async def test_oversized_message_is_terminal(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send("x" * 2048)
        await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            client.stream_ticks(
                ["EURUSD"],
                options=TickStreamOptions(max_message_bytes=1024, max_reconnects=2),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        with pytest.raises(StreamClosed) as caught:
            await anext(stream)
        assert caught.value.code == 1009


async def test_cancel_during_backoff_does_not_reconnect(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    calls = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await ws.recv()
        ws.transport.abort()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            client.stream_ticks(
                ["EURUSD"],
                options=TickStreamOptions(max_reconnects=2, reconnect_delay=30),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        assert isinstance(await anext(stream), TickStreamGap)
        async with asyncio.TaskGroup() as group:
            task = group.create_task(anext(stream))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert calls == 1


async def test_slow_consumer_preserves_order_and_handles_ping(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    sent = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        pong = await ws.ping()
        await asyncio.wait_for(pong, 2)
        for index in range(50):
            await ws.send(json.dumps({"instrument": "EURUSD", "bid": 1.0, "sequence": index}))
        sent.set()
        await ws.close()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        first = await anext(stream)
        await asyncio.wait_for(sent.wait(), 2)
        events = [first, *[event async for event in stream]]
        assert len(events) == 50
        assert [
            event.model_extra["sequence"]
            for event in events
            if isinstance(event, Tick) and event.model_extra
        ] == list(range(50))


async def test_untrusted_tls_is_not_retried(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    async def handler(ws: ServerConnection) -> None:
        pytest.fail("untrusted TLS connected")

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            client.stream_ticks(["EURUSD"], options=TickStreamOptions(max_reconnects=2))
        ) as stream,
    ):
        with pytest.raises(TransportError, match="TLS or handshake"):
            await anext(stream)


async def test_transport_failure_budget_and_outgoing_limit(
    credentials: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    from exness import ticks

    def fail_connect(*args: object, **kwargs: object) -> None:
        raise OSError("private transport details")

    monkeypatch.setattr(ticks, "_NoRedirectConnect", fail_connect)
    async with AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"access_point": "https://account.example"})
        ),
    ) as client:
        async with aclosing(
            client.stream_ticks(
                ["EURUSD"], options=TickStreamOptions(max_reconnects=1, reconnect_delay=0.001)
            )
        ) as stream:
            gap = await anext(stream)
            assert isinstance(gap, TickStreamGap) and gap.close_code is None
            with pytest.raises(TransportError, match="budget exhausted") as caught:
                await anext(stream)
            assert "private" not in str(caught.value)
        async with aclosing(
            client.stream_ticks(["EURUSD"], options=TickStreamOptions(max_message_bytes=1))
        ) as stream:
            with pytest.raises(ValueError, match="Subscription exceeds"):
                await anext(stream)


async def test_closing_refuses_new_work_before_socket_cleanup_finishes(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(fixture_bytes("tick").decode())
        await ws.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        await anext(stream)
        connection = next(iter(client._ws_connections))
        original_close = connection.close

        async def blocked_close() -> None:
            entered.set()
            await release.wait()
            await original_close()

        monkeypatch.setattr(connection, "close", blocked_close)
        task = asyncio.create_task(client.aclose())
        try:
            await asyncio.wait_for(entered.wait(), 3)
            with pytest.raises(RuntimeError, match="closed"):
                await client.get_account()
            async with aclosing(client.stream_account_state()) as other:
                with pytest.raises(RuntimeError, match="closed"):
                    await anext(other)
            with pytest.raises(RuntimeError, match="closed"):
                await client.__aenter__()
        finally:
            release.set()
            await asyncio.wait_for(task, 3)
