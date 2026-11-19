import asyncio
import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler

import httpx
import pytest
from conftest import fixture_bytes
from test_client import verify_request
from test_network import LocalServer, serve_tls
from test_network import tls_contexts as tls_contexts

from exness import APIError, AsyncExnessClient, Credentials, TransportError

pytestmark = pytest.mark.anyio


def network_client(
    credentials: Credentials,
    server: LocalServer,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    timeout: float = 2,
) -> AsyncExnessClient:
    return AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url=server.url,
        transport=httpx.AsyncHTTPTransport(verify=contexts[1], trust_env=False),
        timeout=timeout,
    )


def reply(handler: BaseHTTPRequestHandler) -> None:
    if handler.path.startswith("/v1/trading/access-point?"):
        body = json.dumps(
            {"access_point": "https://" + handler.headers["Host"], "account_id": "123456"}
        ).encode()
    else:
        body = fixture_bytes("account")
    handler.send_response(200)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


@pytest.mark.parametrize("body_started", [False, True])
async def test_cancel_real_request_and_recover(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    body_started: bool,
) -> None:
    entered, release = threading.Event(), threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if not entered.is_set():
            if body_started:
                handler.send_response(200)
                handler.send_header("Content-Length", "100")
                handler.end_headers()
                handler.wfile.flush()
            entered.set()
            release.wait(timeout=3)
            handler.close_connection = True
        else:
            reply(handler)

    with serve_tls(tls_contexts, respond) as server:
        try:
            async with network_client(credentials, server, tls_contexts) as client:
                async with asyncio.TaskGroup() as group:
                    request = group.create_task(client.discover())
                    assert await asyncio.to_thread(entered.wait, 2)
                    request.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(request, 2)
                # Cancellation must release the discovery lock and the failed connection.
                assert (await asyncio.wait_for(client.get_account(), 2)).account_id == "123456"
            assert server.requests.qsize() == 3
            for _ in range(3):
                sent, _ = server.requests.get_nowait()
                verify_request(sent)
        finally:
            release.set()


@pytest.mark.parametrize("body_started", [False, True])
async def test_real_timeout(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    body_started: bool,
) -> None:
    entered, release = threading.Event(), threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if body_started:
            handler.send_response(200)
            handler.send_header("Content-Length", "100")
            handler.end_headers()
            handler.wfile.flush()
        entered.set()
        release.wait(timeout=3)
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        try:
            async with network_client(credentials, server, tls_contexts, timeout=0.5) as client:
                with pytest.raises(TransportError) as caught:
                    await client.discover()
                assert entered.is_set()
                assert server.url not in str(caught.value)
        finally:
            release.set()
        assert await asyncio.to_thread(server.closed.wait, 2)


async def test_real_truncation(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Length", "100")
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(b'{"access_point":')
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        async with network_client(credentials, server, tls_contexts) as client:
            with pytest.raises(TransportError):
                await client.discover()
        assert server.requests.qsize() == 1


async def test_real_redirect_not_followed(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    with serve_tls(tls_contexts, reply) as target:

        def redirect(handler: BaseHTTPRequestHandler) -> None:
            handler.send_response(307)
            handler.send_header("Location", target.url)
            handler.send_header("Content-Length", "0")
            handler.end_headers()

        with serve_tls(tls_contexts, redirect) as server:
            async with network_client(credentials, server, tls_contexts) as client:
                with pytest.raises(APIError) as caught:
                    await client.discover()
                assert caught.value.status_code == 307
            assert target.requests.empty()


async def test_real_connection_reuse_and_close(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    with serve_tls(tls_contexts, reply) as server:
        async with network_client(credentials, server, tls_contexts) as client:
            await client.get_account()
            await client.get_account()
            assert not server.closed.is_set()
        assert await asyncio.to_thread(server.closed.wait, 2)
        assert server.requests.qsize() == 3
        ports = set()
        for _ in range(3):
            request, port = server.requests.get_nowait()
            verify_request(request)
            ports.add(port)
        assert len(ports) == 1


async def test_context_exit_after_task_cancellation(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    entered, release = threading.Event(), threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        entered.set()
        release.wait(timeout=3)
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        client = network_client(credentials, server, tls_contexts)

        async def use_client() -> None:
            async with client:
                await client.discover()

        try:
            async with asyncio.TaskGroup() as group:
                task = group.create_task(use_client())
                assert await asyncio.to_thread(entered.wait, 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
            with pytest.raises(RuntimeError, match="closed"):
                await client.discover()
        finally:
            release.set()
            await client.aclose()
        assert await asyncio.to_thread(server.closed.wait, 2)
