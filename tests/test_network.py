"""Real loopback TLS connections, using ephemeral certificates and synthetic keys."""

import ipaddress
import json
import ssl
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue

import httpx
import pytest
from conftest import fixture_bytes
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from test_client import verify_request

from exness import APIError, Credentials, ExnessClient, TransportError


@pytest.fixture
def tls_contexts(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert_path, key_path)
    client = ssl.create_default_context(cafile=str(cert_path))
    return server, client


@dataclass
class LocalServer:
    url: str
    requests: Queue[tuple[httpx.Request, int]]
    closed: threading.Event


@contextmanager
def serve_tls(
    contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    respond: Callable[[BaseHTTPRequestHandler], None],
) -> Iterator[LocalServer]:
    requests: Queue[tuple[httpx.Request, int]] = Queue()
    failures: Queue[Exception] = Queue()
    closed = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            request = httpx.Request(
                self.command,
                "https://" + self.headers["Host"] + self.path,
                headers=list(self.headers.items()),
                content=self.rfile.read(int(self.headers.get("Content-Length", "0"))),
            )
            requests.put((request, self.client_address[1]))
            try:
                respond(self)
            except Exception as error:
                failures.put(error)
                self.close_connection = True

        do_POST = do_GET
        do_PATCH = do_GET
        do_DELETE = do_GET

        def log_message(self, format: str, *args: object) -> None:
            pass

        def finish(self) -> None:
            try:
                super().finish()
            finally:
                closed.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.socket = contexts[0].wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield LocalServer(f"https://127.0.0.1:{server.server_port}", requests, closed)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()
        if not failures.empty():
            raise failures.get_nowait()


def network_client(
    credentials: Credentials,
    server: LocalServer,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    timeout: float = 2,
) -> ExnessClient:
    return ExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url=server.url,
        transport=httpx.HTTPTransport(verify=contexts[1], trust_env=False),
        timeout=timeout,
    )


@pytest.mark.parametrize("body_timeout", [False, True])
def test_real_read_timeouts(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    body_timeout: bool,
) -> None:
    entered, release = threading.Event(), threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if body_timeout:
            handler.send_response(200)
            handler.send_header("Content-Length", "100")
            handler.end_headers()
            handler.wfile.flush()
        entered.set()
        release.wait(timeout=3)
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        try:
            with network_client(credentials, server, tls_contexts, timeout=0.2) as client:
                with pytest.raises(TransportError) as caught:
                    client.discover()
                assert entered.is_set()
                assert server.url not in str(caught.value)
                assert caught.value.__suppress_context__
        finally:
            release.set()
        assert server.closed.wait(timeout=3)


def test_real_truncated_body(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Length", "100")
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(b'{"access_point":')
        handler.wfile.flush()
        handler.close_connection = True

    with serve_tls(tls_contexts, respond) as server:
        with (
            network_client(credentials, server, tls_contexts) as client,
            pytest.raises(TransportError),
        ):
            client.discover()
        assert server.requests.qsize() == 1


def test_redirect_does_not_forward_signed_headers(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    def target_response(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Length", "0")
        handler.end_headers()

    with serve_tls(tls_contexts, target_response) as target:

        def redirect(handler: BaseHTTPRequestHandler) -> None:
            handler.send_response(307)
            handler.send_header("Location", target.url)
            handler.send_header("Content-Length", "0")
            handler.end_headers()

        with serve_tls(tls_contexts, redirect) as server:
            with network_client(credentials, server, tls_contexts) as client:
                with pytest.raises(APIError) as caught:
                    client.discover()
                assert caught.value.status_code == 307
            assert target.requests.empty()
            assert server.requests.qsize() == 1


def test_tls_connection_reuse_signing_and_close(
    credentials: Credentials, tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext]
) -> None:
    def respond(handler: BaseHTTPRequestHandler) -> None:
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

    with serve_tls(tls_contexts, respond) as server:
        with network_client(credentials, server, tls_contexts) as client:
            assert client.get_account().account_id == "123456"
            assert client.get_account().account_id == "123456"
            assert not server.closed.is_set()
        assert server.closed.wait(timeout=3)
        assert server.requests.qsize() == 3
        ports = set()
        for _ in range(3):
            request, port = server.requests.get_nowait()
            ports.add(port)
            verify_request(request)
        assert len(ports) == 1
