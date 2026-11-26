"""Client shutdown owns SDK waits without cancelling consumer tasks."""

import asyncio
import ssl
from collections.abc import AsyncGenerator
from contextlib import aclosing

import httpx
import pytest
from conftest import fixture_bytes
from test_async_client import async_client_for
from test_network import tls_contexts as tls_contexts
from test_ticks import tick_client
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from exness import AsyncExnessClient, Credentials, StreamGap, StreamOptions, ticks

pytestmark = pytest.mark.anyio


def events(client: AsyncExnessClient, kind: str) -> AsyncGenerator[object, None]:
    match kind:
        case "ticks":
            return client.stream_ticks(["EURUSD"])
        case "trading":
            return client.stream_trading()
        case "account":
            return client.stream_account_state()
        case "instruments":
            return client.stream_instruments(["EURUSD"])
        case "hmr":
            return client.stream_hmr(["EURUSD"])
        case _:
            raise AssertionError(kind)


@pytest.mark.parametrize("kind", ["ticks", "trading", "account", "instruments", "hmr"])
async def test_close_interrupts_discovery_and_lock_waiters(
    credentials: Credentials, kind: str
) -> None:
    entered, cleaned = asyncio.Event(), asyncio.Event()
    calls = 0

    async def discovery(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        try:
            await asyncio.Future[None]()
        finally:
            cleaned.set()
        raise AssertionError("discovery should have been cancelled")

    async with (
        async_client_for(credentials, discovery) as client,
        aclosing(events(client, kind)) as first,
        aclosing(events(client, kind)) as second,
    ):
        tasks = [asyncio.create_task(anext(stream)) for stream in (first, second)]
        await asyncio.wait_for(entered.wait(), 2)
        assert len(client._stream_tasks) == 2
        await asyncio.wait_for(client.aclose(), 2)
        for task in tasks:
            with pytest.raises(RuntimeError, match="closed"):
                await asyncio.wait_for(task, 2)
            assert not task.cancelled()
        assert cleaned.is_set()
        assert calls == 1
        assert not client._discovery_lock.locked()
        assert not client._stream_tasks


@pytest.mark.parametrize("close_client", [False, True])
async def test_shutdown_during_real_handshake(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    close_client: bool,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    connections: list[ServerConnection] = []

    async def handshake(connection: ServerConnection, request: Request) -> Response | None:
        connections.append(connection)
        entered.set()
        await release.wait()
        return None

    async def handler(connection: ServerConnection) -> None:
        pytest.fail("cancelled handshake must not subscribe")

    async with serve(
        handler, "127.0.0.1", 0, ssl=tls_contexts[0], process_request=handshake
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        async with (
            async_client_for(
                credentials,
                lambda request: httpx.Response(
                    200, json={"access_point": f"https://127.0.0.1:{port}"}
                ),
            ) as client,
            aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
        ):
            task = asyncio.create_task(anext(stream))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                if close_client:
                    await asyncio.wait_for(client.aclose(), 2)
                    with pytest.raises(RuntimeError, match="closed"):
                        await asyncio.wait_for(task, 2)
                else:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 2)
                await asyncio.wait_for(connections[0].wait_closed(), 2)
                assert not client._stream_tasks
                assert not client._ws_connections
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_close_interrupts_backoff_without_reconnecting(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    calls = 0

    async def handler(connection: ServerConnection) -> None:
        nonlocal calls
        calls += 1
        await connection.recv()
        connection.transport.abort()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(
            client.stream_ticks(
                ["EURUSD"],
                options=StreamOptions(max_reconnects=2, reconnect_delay=30),
                ssl_context=tls_contexts[1],
            )
        ) as stream,
    ):
        assert isinstance(await anext(stream), StreamGap)
        task = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        assert client._stream_tasks
        await asyncio.wait_for(client.aclose(), 2)
        with pytest.raises(RuntimeError, match="closed"):
            await asyncio.wait_for(task, 2)
        assert not client._stream_tasks
        assert calls == 1


@pytest.mark.parametrize("close_client", [False, True])
async def test_completed_handshake_shutdown_race_closes_connection(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    monkeypatch: pytest.MonkeyPatch,
    close_client: bool,
) -> None:
    closing: list[asyncio.Task[None]] = []
    accepted: list[ServerConnection] = []

    async def handler(connection: ServerConnection) -> None:
        accepted.append(connection)
        await connection.wait_closed()

    async with (
        tick_client(credentials, tls_contexts, handler) as client,
        aclosing(client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])) as stream,
    ):
        consumer = asyncio.create_task(anext(stream))

        class ClosingConnect(ticks._NoRedirectConnect):
            async def __await_impl__(self) -> ClientConnection:
                connection = await super().__await_impl__()
                if close_client:
                    closing.append(asyncio.create_task(client.aclose()))
                else:
                    asyncio.get_running_loop().call_soon(consumer.cancel)
                return connection

        monkeypatch.setattr(ticks, "_NoRedirectConnect", ClosingConnect)
        if close_client:
            with pytest.raises(RuntimeError, match="closed"):
                await asyncio.wait_for(consumer, 3)
            await asyncio.gather(*closing)
        else:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer, 3)
        assert accepted
        await asyncio.wait_for(accepted[0].wait_closed(), 2)
        assert not client._stream_tasks
        assert not client._ws_connections


async def test_client_close_does_not_cancel_paused_consumer(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    received, resume = asyncio.Event(), asyncio.Event()

    async def handler(connection: ServerConnection) -> None:
        await connection.recv()
        await connection.send(fixture_bytes("tick"))
        await connection.wait_closed()

    async with tick_client(credentials, tls_contexts, handler) as client:

        async def consume() -> None:
            async with aclosing(
                client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])
            ) as stream:
                await anext(stream)
                received.set()
                await resume.wait()
                with pytest.raises(RuntimeError, match="closed"):
                    await anext(stream)

        consumer = asyncio.create_task(consume())
        try:
            await asyncio.wait_for(received.wait(), 2)
            await asyncio.wait_for(client.aclose(), 2)
            assert not consumer.done()
            assert not consumer.cancelling()
        finally:
            resume.set()
            await asyncio.wait_for(consumer, 2)
