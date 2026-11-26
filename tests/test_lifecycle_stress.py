"""Bounded repeated lifecycles with explicit completion/connection accounting."""

import asyncio
import ssl
import threading
from contextlib import aclosing
from http.server import BaseHTTPRequestHandler
from queue import Queue

import pytest
from conftest import fixture_bytes
from test_async_network import network_client, reply
from test_network import serve_tls
from test_network import tls_contexts as tls_contexts
from test_ticks import tick_client
from websockets.asyncio.server import ServerConnection

from exness import Credentials, StreamGap, StreamOptions, TransportError
from exness.models import Tick

pytestmark = pytest.mark.anyio
ROUNDS = 6
WORKERS = 6


async def test_lifecycle_concurrent_rest_cancellation(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    for _ in range(ROUNDS):
        await _concurrent_rest_cancellation(credentials, tls_contexts)


async def _concurrent_rest_cancellation(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    entered: Queue[None] = Queue()
    release = threading.Event()
    body = fixture_bytes("account")

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if handler.path.startswith("/v1/trading/access-point?"):
            reply(handler)
            return
        handler.send_response(200)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.flush()
        entered.put(None)
        assert release.wait(5), "test did not release response barrier"
        try:
            handler.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
            handler.close_connection = True  # Expected for cancelled readers.

    with serve_tls(tls_contexts, respond) as server:
        async with network_client(credentials, server, tls_contexts, timeout=5) as client:
            await client.discover()
            tasks = [asyncio.create_task(client.get_account()) for _ in range(WORKERS)]
            try:
                for _ in tasks:
                    await asyncio.to_thread(entered.get, True, 3)
                for task in tasks[: WORKERS // 2]:
                    task.cancel()
                for task in tasks[: WORKERS // 2]:
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 3)
                release.set()
                results = await asyncio.wait_for(asyncio.gather(*tasks[WORKERS // 2 :]), 3)
                assert all(result.account_id == "123456" for result in results)
            finally:
                release.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            assert all(task.done() for task in tasks)
        assert client._http.is_closed
        assert await asyncio.to_thread(server.connections.wait_closed)
        assert server.connections.opened == server.connections.finished == WORKERS
        assert server.requests.qsize() == WORKERS + 1


async def test_lifecycle_reconnect_budget_and_cleanup(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    closed: asyncio.Queue[None] = asyncio.Queue()

    async def handler(connection: ServerConnection) -> None:
        try:
            await connection.recv()
            await connection.send(fixture_bytes("tick"))
            connection.transport.abort()
            await connection.wait_closed()
        finally:
            closed.put_nowait(None)

    async with tick_client(credentials, tls_contexts, handler) as client:
        for _ in range(ROUNDS):
            received: list[Tick | StreamGap] = []
            async with aclosing(
                client.stream_ticks(
                    ["EURUSD"],
                    options=StreamOptions(max_reconnects=2, reconnect_delay=0.001),
                    ssl_context=tls_contexts[1],
                )
            ) as stream:
                with pytest.raises(TransportError):
                    async with asyncio.timeout(5):
                        async for event in stream:
                            received.append(event)
            assert sum(isinstance(event, Tick) for event in received) == 3
            assert [
                event.reconnect_attempt for event in received if isinstance(event, StreamGap)
            ] == [1, 2]
            for _ in range(3):
                await asyncio.wait_for(closed.get(), 3)
            assert not client._stream_tasks and not client._ws_connections
    assert closed.empty()


async def test_lifecycle_concurrent_stream_shutdown(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    for _ in range(ROUNDS):
        await _concurrent_stream_shutdown(credentials, tls_contexts)


async def _concurrent_stream_shutdown(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    ready: asyncio.Queue[None] = asyncio.Queue()
    closed: asyncio.Queue[None] = asyncio.Queue()

    async def handler(connection: ServerConnection) -> None:
        try:
            await connection.recv()
            await connection.send(fixture_bytes("tick"))
            await connection.wait_closed()
        finally:
            closed.put_nowait(None)

    async with tick_client(credentials, tls_contexts, handler) as client:

        async def consume() -> None:
            async with aclosing(
                client.stream_ticks(["EURUSD"], ssl_context=tls_contexts[1])
            ) as stream:
                assert isinstance(await anext(stream), Tick)
                ready.put_nowait(None)
                await anext(stream)

        tasks = [asyncio.create_task(consume()) for _ in range(WORKERS)]
        try:
            for _ in tasks:
                await asyncio.wait_for(ready.get(), 3)
            for task in tasks[: WORKERS // 2]:
                task.cancel()
            for task in tasks[: WORKERS // 2]:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 3)
            await asyncio.wait_for(client.aclose(), 3)
            for task in tasks[WORKERS // 2 :]:
                with pytest.raises((RuntimeError, StopAsyncIteration)) as outcome:
                    await asyncio.wait_for(task, 3)
                if isinstance(outcome.value, RuntimeError):
                    assert str(outcome.value) == "AsyncExnessClient is closed"
                assert not task.cancelled()
            for _ in tasks:
                await asyncio.wait_for(closed.get(), 3)
            assert not client._stream_tasks and not client._ws_connections
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
