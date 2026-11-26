"""Exercise both clients against streamed, compressed and preloaded HTTP bodies."""

import asyncio
import gzip
import ssl
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler

import httpx
import pytest
from test_async_client import async_client_for
from test_async_network import network_client as async_network_client
from test_client import ACCESS_POINT, client_for
from test_network import network_client, serve_tls
from test_network import tls_contexts as tls_contexts

from exness import APIError, Credentials, ProtocolError, _common


class BodyStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.reads = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self:
            yield chunk

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("encoding", ["streamed", "gzip", "preloaded"])
@pytest.mark.parametrize("status", [200, 503])
@pytest.mark.parametrize("oversize", [False, True])
async def test_response_limit_and_cleanup(
    credentials: Credentials,
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    encoding: str,
    status: int,
    oversize: bool,
) -> None:
    assert _common._MAX_RESPONSE_BYTES == 16 * 1024 * 1024
    limit = 256
    monkeypatch.setattr(_common, "_MAX_RESPONSE_BYTES", limit)
    body = b'{"access_point":"https://account.example"}' if status == 200 else b"{}"
    body += b" " * (limit + int(oversize) - len(body))
    wire = gzip.compress(body) if encoding == "gzip" else body
    chunks = [wire[:16], wire[16:]]
    if oversize:
        chunks.append(b"must not read the rest of an oversized response")
    stream = BodyStream(chunks)
    # A small or absent Content-Length must not bypass decoded-byte accounting.
    headers = {"Content-Encoding": "gzip", "Content-Length": str(len(wire))}
    if encoding != "gzip":
        headers = {"Content-Length": "1"} if oversize else {}
    response = (
        httpx.Response(status, content=body)
        if encoding == "preloaded"
        else httpx.Response(status, stream=stream, headers=headers)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return response

    async def discover() -> str:
        if asynchronous:
            async with async_client_for(credentials, handler) as client:
                return await client.discover()
        with client_for(credentials, handler) as sync_client:
            return sync_client.discover()

    if oversize:
        with pytest.raises(ProtocolError, match="REST response exceeds 16 MiB"):
            await discover()
    elif status != 200:
        with pytest.raises(APIError) as caught:
            await discover()
        assert caught.value.status_code == status
    else:
        assert await discover() == ACCESS_POINT
    assert response.is_closed
    if encoding != "preloaded":
        assert stream.closed
        assert 1 <= stream.reads <= 2


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("compressed", [False, True])
async def test_oversized_chunked_tls_response_closes_without_waiting_for_eof(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    compressed: bool,
) -> None:
    monkeypatch.setattr(_common, "_MAX_RESPONSE_BYTES", 256)
    body = b"x" * 257
    wire = gzip.compress(body) if compressed else body

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Transfer-Encoding", "chunked")
        if compressed:
            handler.send_header("Content-Encoding", "gzip")
        handler.end_headers()
        handler.wfile.write(f"{len(wire):x}\r\n".encode() + wire + b"\r\n")
        handler.wfile.flush()
        # Omit the final chunk: the SDK must stop reading and close the socket.

    with serve_tls(tls_contexts, respond) as server:
        if asynchronous:
            async with async_network_client(credentials, server, tls_contexts) as client:
                with pytest.raises(ProtocolError, match="exceeds 16 MiB"):
                    await asyncio.wait_for(client.discover(), 1)
                assert await asyncio.to_thread(server.closed.wait, 1)
        else:
            with network_client(credentials, server, tls_contexts) as sync_client:
                with pytest.raises(ProtocolError, match="exceeds 16 MiB"):
                    sync_client.discover()
                assert server.closed.wait(1)
