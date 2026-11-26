"""Operation wait deadlines include discovery and streamed response bodies over TLS."""

import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler

import pytest
from test_async_network import network_client as async_network_client
from test_client import verify_request
from test_network import network_client, serve_tls
from test_network import tls_contexts as tls_contexts

from exness import Credentials, OperationReference, OperationWaitTimeout, TransportError


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("phase", ["discovery", "headers", "body"])
async def test_wait_tls_deadline(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    asynchronous: bool,
    phase: str,
) -> None:
    entered, release = threading.Event(), threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if handler.path.startswith("/v1/trading/access-point") and phase != "discovery":
            body = json.dumps(
                {"access_point": "https://" + handler.headers["Host"], "account_id": "123456"}
            ).encode()
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body)
            return
        if phase == "body":
            handler.send_response(200)
            handler.send_header("Content-Length", "100")
            handler.end_headers()
            handler.wfile.flush()
        entered.set()
        release.wait(timeout=3)
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        try:
            with pytest.raises(OperationWaitTimeout) as caught:
                if asynchronous:
                    async with async_network_client(
                        credentials, server, tls_contexts, timeout=5
                    ) as client:
                        await client.wait_operation(OperationReference("789"), timeout=0.5)
                else:
                    with network_client(credentials, server, tls_contexts, timeout=5) as sync:
                        sync.wait_operation(OperationReference("789"), timeout=0.5)
            assert entered.is_set()
            assert caught.value.reference.operation_id == "789"
            assert caught.value.last_status is None
            assert caught.value.attempts == 1
        finally:
            release.set()
        assert server.closed.wait(timeout=3)
        requests = []
        while not server.requests.empty():
            request, _ = server.requests.get_nowait()
            verify_request(request)
            requests.append(request)
        assert len(requests) == (1 if phase == "discovery" else 2)


def test_shorter_sync_transport_timeout_is_preserved(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    release = threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        release.wait(timeout=3)
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        try:
            with (
                network_client(credentials, server, tls_contexts, timeout=0.1) as client,
                pytest.raises(TransportError),
            ):
                client.wait_operation(OperationReference("789"), timeout=2)
        finally:
            release.set()
        assert server.closed.wait(timeout=3)
