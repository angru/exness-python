"""Compare the public Python and Go clients against shared synthetic TLS fixtures."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from collections.abc import Awaitable, Callable
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from ipaddress import IPv4Address
from pathlib import Path

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from exness import (
    AsyncExnessClient,
    Credentials,
    ExnessError,
    ProtocolError,
    StreamOptions,
    TradingStreamGap,
    UnknownOutcomeError,
)
from exness.models import Tick
from exness.requests import OpenPositionRequest
from exness.trading import TradingEvent, TradingSnapshotEvent, TransactionEvent
from tools.check_parity import ParityError, verify_peer
from tools.transport_transcript import (
    ACCOUNT,
    ACCOUNT_PATH,
    DISCOVERY_PATH,
    OPEN_ORDER,
    POSITIONS_PATH,
    TICKS_PATH,
    TRADING_PATH,
    WRITE_KEY,
    Scenario,
    compare_transcripts,
    verify_request,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures"
VECTOR = FIXTURES / "signing-vector.json"
GO_DRIVER = ROOT / "tools/transport_driver.go"
GO_MODULE = "github.com/angru/exness-go"
LOOPBACK = "127.0.0.1"
INSTRUMENT = "EURUSD"
SIGNED_HEADERS = ("exn-data", "exn-sign", "exn-idempotency-key")
IO_TIMEOUT = 3
PEER_TIMEOUT = 15
BUILD_TIMEOUT = 120
MAX_ORDER_BODY = 1024


class CheckError(Exception):
    """A transport check failure with a diagnostic safe to report."""


@dataclass
class WireTranscript:
    scenario: Scenario
    paths: list[str] = field(default_factory=list)
    subscriptions: list[object] = field(default_factory=list)
    subscription_ids: set[str] = field(default_factory=set)
    completed: int = 0
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    failure: str | None = None

    @property
    def upgrade_path(self) -> str:
        return TICKS_PATH if self.scenario is Scenario.REST_TICKS else TRADING_PATH

    @property
    def expected_connections(self) -> int:
        return 1 if self.scenario is Scenario.REST_TICKS else 2


class FixtureServer:
    def __init__(self) -> None:
        self.origin = ""
        self.active: WireTranscript | None = None
        self.account = (FIXTURES / "account.json").read_text(encoding="utf-8")
        self.tick = (FIXTURES / "tick.json").read_text(encoding="utf-8")
        self.snapshot = (FIXTURES / "trading_snapshot.json").read_text(encoding="utf-8")
        self.partial_close = (FIXTURES / "transaction_partial_close.json").read_text(
            encoding="utf-8"
        )
        self.balance = (FIXTURES / "transaction_balance.json").read_text(encoding="utf-8")
        replacement = json.loads(self.snapshot)
        replacement["payload"]["account_state"]["balance"] = "12345678901234567790.113456789"
        self.replacement_snapshot = json.dumps(replacement)

    def process_request(self, ws: ServerConnection, request: Request) -> Response | None:
        wire = self.active
        if wire is None:
            return ws.respond(HTTPStatus.SERVICE_UNAVAILABLE, "No client scenario is active")
        try:
            self.record_request(wire, request)
        except CheckError as error:
            wire.failure = wire.failure or str(error)
            return ws.respond(HTTPStatus.BAD_REQUEST, "Unexpected request")
        if request.path == wire.upgrade_path:
            return None
        body = self.account
        if request.path == DISCOVERY_PATH:
            body = json.dumps({"access_point": self.origin, "account_id": ACCOUNT})
        response = ws.respond(HTTPStatus.OK, body)
        del response.headers["Content-Type"]
        response.headers["Content-Type"] = "application/json"
        return response

    def record_request(self, wire: WireTranscript, request: Request) -> None:
        limits = {DISCOVERY_PATH: 1, wire.upgrade_path: wire.expected_connections}
        if wire.scenario is Scenario.REST_TICKS:
            limits[ACCOUNT_PATH] = 2
        if request.path not in limits:
            raise CheckError("unexpected request target")
        if wire.paths.count(request.path) >= limits[request.path]:
            raise CheckError("unexpected extra request")
        upgrade = request.headers.get_all("Upgrade")
        if request.path == wire.upgrade_path:
            if [value.lower() for value in upgrade] != ["websocket"]:
                raise CheckError("stream request missing WebSocket upgrade")
        elif upgrade:
            raise CheckError("unexpected upgrade on REST request")
        if any(len(request.headers.get_all(name)) != 1 for name in SIGNED_HEADERS):
            raise CheckError("missing or duplicate signing header")
        # The websockets HTTP parser admits only bodyless GET requests. Its
        # request.path preserves the raw target, including the query string.
        try:
            verify_request("GET", request.path, request.headers)
        except ValueError:
            raise CheckError("invalid signature for received request") from None
        wire.paths.append(request.path)

    async def handle_websocket(self, ws: ServerConnection) -> None:
        wire = self.active
        if wire is None:
            return
        try:
            async with asyncio.timeout(IO_TIMEOUT):
                raw = await ws.recv()
                if not isinstance(raw, str):
                    raise CheckError("expected a text subscription")
                request = json.loads(raw)
                if (
                    not isinstance(request, dict)
                    or set(request) != {"id", "subscribe"}
                    or not isinstance(request["id"], str)
                    or not request["id"]
                ):
                    raise CheckError("invalid subscription envelope")
                if request["id"] in wire.subscription_ids:
                    raise CheckError("reused subscription ID")
                wire.subscription_ids.add(request["id"])
                wire.subscriptions.append(request["subscribe"])
                await self.exchange(ws, wire)
        except (ValueError, ConnectionClosed, TimeoutError):
            wire.failure = wire.failure or "WebSocket fixture exchange did not complete"
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        finally:
            wire.completed += 1
            if wire.completed == wire.expected_connections:
                wire.closed.set()

    async def exchange(self, ws: ServerConnection, wire: WireTranscript) -> None:
        if wire.scenario is Scenario.REST_TICKS:
            await ws.send(self.tick)
            await ws.send("{")
        elif len(wire.subscriptions) == 1:
            await ws.send(self.snapshot)
            await ws.send(self.partial_close)
            await ws.close(code=1012)
        elif wire.scenario is Scenario.TRADING_RECOVERY:
            await ws.send(self.replacement_snapshot)
            await ws.send(self.balance)
            await ws.close(code=1000)
        else:
            await ws.send(self.balance)
        # Check for extra messages even on server-initiated closes. For an
        # invalid event sequence, wait for the client to close the connection.
        try:
            await ws.recv()
        except ConnectionClosed:
            return
        raise CheckError("unexpected extra WebSocket message")


@dataclass
class LostAckWire:
    paths: list[str] = field(default_factory=list)
    post_count: int = 0
    receipt: dict[str, object] | None = None
    failure: str | None = None


class LostAckHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: LostAckServer

    def log_message(self, format: str, *args: object) -> None:
        pass

    def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
        self.close_connection = True
        if (wire := self.server.active) is not None:
            wire.failure = wire.failure or "unexpected HTTP request"

    def handle_expect_100(self) -> bool:
        self.send_error(HTTPStatus.EXPECTATION_FAILED)
        return False

    @property
    def raw_target(self) -> str:
        # BaseHTTPRequestHandler can normalize self.path (e.g. leading slashes).
        return self.requestline.split()[1]

    def verify_signature(self, body: bytes = b"", key: str = "") -> None:
        headers = {}
        for name in SIGNED_HEADERS:
            values = self.headers.get_all(name) or []
            if len(values) != 1:
                raise CheckError("missing or duplicate signing header")
            headers[name] = values[0]
        try:
            verify_request(self.command, self.raw_target, headers, body=body, idempotency_key=key)
        except ValueError:
            raise CheckError("invalid signature for received request") from None

    def do_GET(self) -> None:
        self.close_connection = True
        wire = self.server.active
        if wire is None:
            return
        try:
            if self.raw_target != DISCOVERY_PATH or wire.paths or wire.post_count:
                raise CheckError("unexpected discovery request")
            if self.headers.get_all("Transfer-Encoding") or (
                self.headers.get_all("Content-Length") or []
            ) not in ([], ["0"]):
                raise CheckError("unexpected discovery request body")
            self.verify_signature()
            wire.paths.append(self.raw_target)
            body = json.dumps({"access_point": self.server.origin, "account_id": ACCOUNT}).encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        except OSError:
            wire.failure = wire.failure or "discovery response did not complete"

    def read_order(self) -> bytes:
        lengths = self.headers.get_all("Content-Length") or []
        if (
            self.headers.get_all("Transfer-Encoding")
            or len(lengths) != 1
            or len(lengths[0]) > len(str(MAX_ORDER_BODY))
            or not lengths[0].isascii()
            or not lengths[0].isdigit()
        ):
            raise CheckError("invalid order body length")
        size = int(lengths[0])
        if not 0 < size <= MAX_ORDER_BODY:
            raise CheckError("invalid order body length")
        try:
            body = self.rfile.read(size)
        except OSError:
            raise CheckError("order body did not complete") from None
        if len(body) != size:
            raise CheckError("order body did not complete")
        return body

    def do_POST(self) -> None:
        self.close_connection = True
        wire = self.server.active
        if wire is None:
            return
        wire.post_count += 1
        try:
            if wire.post_count != 1:
                raise CheckError("duplicate POST")
            if self.raw_target != POSITIONS_PATH or wire.paths != [DISCOVERY_PATH]:
                raise CheckError("unexpected order request")
            body = self.read_order()
            self.verify_signature(body, WRITE_KEY)
            order = json.loads(body)
            if order != OPEN_ORDER:
                raise CheckError("unexpected order fields")
            wire.paths.append(self.raw_target)
            wire.receipt = {
                "method": self.command,
                "path": self.raw_target,
                "order": order,
                "signature_verified": True,
                "idempotency_key": WRITE_KEY,
                "acknowledgement_sent": False,
            }
            # Return to the server so it closes the connection without writing an HTTP response.
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        except (ValueError, RecursionError):
            wire.failure = wire.failure or "invalid order JSON"


class LostAckServer(HTTPServer):
    timeout = 0.05

    def __init__(self, tls: ssl.SSLContext) -> None:
        self.tls = tls
        self.active: LostAckWire | None = None
        self._stopping = threading.Event()
        self._connection_lock = threading.Lock()
        self._connection: ssl.SSLSocket | None = None
        super().__init__((LOOPBACK, 0), LostAckHandler)
        self.origin = f"https://{LOOPBACK}:{self.server_port}"

    def get_request(self) -> tuple[socket.socket, tuple[str, int]]:
        sock, address = super().get_request()
        try:
            with self._connection_lock:
                if self._stopping.is_set():
                    raise OSError("HTTPS fixture is stopping")
                sock.settimeout(IO_TIMEOUT)
                connection = self.tls.wrap_socket(
                    sock, server_side=True, do_handshake_on_connect=False
                )
                self._connection = connection
        except OSError:
            sock.close()
            raise
        try:
            connection.do_handshake()
        except OSError:
            connection.close()
            raise
        return connection, address

    def handle_error(self, request: object, client_address: object) -> None:
        if (wire := self.active) is not None:
            wire.failure = wire.failure or "HTTPS fixture exchange did not complete"

    def run(self) -> None:
        while not self._stopping.is_set():
            self.handle_request()

    def stop(self) -> None:
        with self._connection_lock:
            self._stopping.set()
            if self._connection is not None:
                with suppress(OSError):
                    # Interrupt a handshake or read so the server thread can close the socket.
                    socket.socket.shutdown(self._connection, socket.SHUT_RDWR)


def make_tls(directory: Path) -> tuple[ssl.SSLContext, ssl.SSLContext, Path]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, LOOPBACK)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(IPv4Address(LOOPBACK))]), False)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = directory / "cert.pem", directory / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert_path, key_path)
    client = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client.load_verify_locations(cafile=cert_path)
    return server, client, cert_path


async def run_python(origin: str, tls: ssl.SSLContext, scenario: Scenario) -> dict[str, object]:
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    try:
        async with AsyncExnessClient(
            credentials=Credentials(vector["api_key"], vector["private_key_hex"]),
            account_id=ACCOUNT,
            discovery_url=origin,
            timeout=IO_TIMEOUT,
            transport=httpx.AsyncHTTPTransport(verify=tls, trust_env=False),
        ) as client:
            if scenario is Scenario.REST_TICKS:
                return await run_python_ticks(client, tls)
            if scenario is Scenario.LOST_ACK:
                return await run_python_lost_ack(client)
            return await run_python_trading(client, tls, scenario)
    except (ExnessError, StopAsyncIteration):
        raise CheckError("SDK did not complete the scenario") from None


async def run_python_lost_ack(client: AsyncExnessClient) -> dict[str, object]:
    request = OpenPositionRequest.model_validate(OPEN_ORDER)
    try:
        await client.open_position(request, idempotency_key=WRITE_KEY)
    except UnknownOutcomeError as error:
        return {
            "outcome": {
                "error": "UnknownOutcomeError",
                "method": error.method,
                "path": error.path,
                "account_id": error.account_id,
                "idempotency_key": error.idempotency_key,
            }
        }
    raise CheckError("expected UnknownOutcomeError after losing the acknowledgement")


async def run_python_ticks(client: AsyncExnessClient, tls: ssl.SSLContext) -> dict[str, object]:
    accounts = [(await client.get_account()).account_id for _ in range(2)]
    async with aclosing(
        client.stream_ticks(
            [INSTRUMENT], options=StreamOptions(open_timeout=IO_TIMEOUT), ssl_context=tls
        )
    ) as stream:
        tick = await anext(stream)
        if not isinstance(tick, Tick):
            raise CheckError("expected a tick as the first stream event")
        try:
            await anext(stream)
        except ProtocolError:
            return {
                "accounts": accounts,
                "tick": {"instrument": tick.instrument, "bid": tick.bid, "ask": tick.ask},
                "error": "ProtocolError",
            }
        raise CheckError("expected a protocol error after the first tick")


def trading_observation(event: TradingEvent | TradingStreamGap) -> dict[str, object]:
    if isinstance(event, TradingSnapshotEvent):
        return {
            "kind": "snapshot",
            "balance": format(event.payload.account_state.balance, "f"),
            "positions": len(event.payload.positions),
        }
    if isinstance(event, TransactionEvent):
        account = event.payload.account_state
        return {
            "kind": "transaction",
            "operation_id": event.operation_id,
            "status": event.status,
            "positions": len(event.payload.positions),
            "deals": len(event.payload.deals),
            "account_state": (
                {"balance": format(account.balance, "f"), "leverage": account.leverage}
                if account is not None
                else None
            ),
        }
    return {
        "kind": "gap",
        "reconnect_attempt": event.reconnect_attempt,
        "close_code": event.close_code,
    }


async def run_python_trading(
    client: AsyncExnessClient, tls: ssl.SSLContext, scenario: Scenario
) -> dict[str, object]:
    options = StreamOptions(
        open_timeout=IO_TIMEOUT,
        # A protocol error must stop recovery while a second retry is available.
        max_reconnects=2 if scenario is Scenario.TRADING_MISSING_SNAPSHOT else 1,
        reconnect_delay=0.01,
    )
    events: list[dict[str, object]] = []
    outcome = "normal_close"
    async with aclosing(client.stream_trading(options=options, ssl_context=tls)) as stream:
        try:
            async for event in stream:
                events.append(trading_observation(event))
        except ProtocolError:
            outcome = "ProtocolError"
    return {"events": events, "outcome": outcome}


def build_go(command: str, peer: Path, directory: Path) -> Path:
    shutil.copyfile(GO_DRIVER, directory / "main.go")
    # Start with the peer's exact dependency requirements and checksums. All
    # module changes are confined to the independent consumer directory.
    shutil.copyfile(peer / "go.mod", directory / "go.mod")
    shutil.copyfile(peer / "go.sum", directory / "go.sum")
    binary = directory / "transport-driver"
    env = {
        **os.environ,
        "GOENV": "off",
        "GOFLAGS": "",
        "GONOPROXY": "none",
        "GOPROXY": "off",
        "GOSUMDB": "off",
        "GOTOOLCHAIN": "local",
        "GOVCS": "*:off",
        "GOWORK": "off",
    }
    for args in (
        [
            "mod",
            "edit",
            "-module=example.invalid/exness-transport",
            f"-require={GO_MODULE}@v0.0.0",
            f"-replace={GO_MODULE}={peer}",
        ],
        ["build", "-mod=readonly", "-o", str(binary), "."],
    ):
        try:
            result = subprocess.run(
                [command, *args],
                cwd=directory,
                env=env,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=BUILD_TIMEOUT,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise CheckError(f"go {args[0]} timed out") from None
        if result.returncode:
            # Compilation runs before the listener or its credentials are used.
            diagnostic = result.stderr[-4000:].strip()
            raise CheckError(
                f"go {' '.join(args[:2])} failed (exit {result.returncode})\n{diagnostic}"
            )
    return binary


async def run_go(
    binary: Path, origin: str, certificate: Path, scenario: Scenario
) -> dict[str, object]:
    process = await asyncio.create_subprocess_exec(
        str(binary),
        origin,
        str(certificate),
        str(VECTOR),
        scenario,
        env={},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        output, _ = await process.communicate()
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
            await process.wait()
    if process.returncode:
        raise CheckError(f"Go consumer failed (exit {process.returncode})")
    try:
        result = json.loads(output)
    except (ValueError, RecursionError):
        raise CheckError("Go consumer returned an invalid result") from None
    match scenario:
        case Scenario.REST_TICKS:
            fields = {"accounts", "tick", "error"}
        case Scenario.LOST_ACK:
            fields = {"outcome"}
        case _:
            fields = {"events", "outcome"}
    if not isinstance(result, dict) or set(result) != fields:
        raise CheckError("Go consumer returned unexpected result fields")
    return result


async def observe(
    server: FixtureServer,
    scenario: Scenario,
    label: str,
    driver: Callable[[], Awaitable[dict[str, object]]],
) -> dict[str, object]:
    wire = WireTranscript(scenario)
    label = f"{scenario}: {label}"
    server.active = wire
    try:
        async with asyncio.timeout(PEER_TIMEOUT):
            result = await driver()
            if wire.paths.count(wire.upgrade_path) != wire.expected_connections:
                raise CheckError("missing WebSocket handshake")
            await wire.closed.wait()
    except TimeoutError:
        raise CheckError(f"{label}: {wire.failure or 'scenario timed out'}") from None
    except CheckError as error:
        raise CheckError(f"{label}: {wire.failure or error}") from None
    finally:
        server.active = None
    if wire.failure:
        raise CheckError(f"{label}: {wire.failure}")
    return {**result, "paths": wire.paths, "subscriptions": wire.subscriptions}


async def observe_lost_ack(
    server: LostAckServer,
    label: str,
    driver: Callable[[], Awaitable[dict[str, object]]],
) -> dict[str, object]:
    wire = LostAckWire()
    label = f"{Scenario.LOST_ACK}: {label}"
    server.active = wire
    try:
        async with asyncio.timeout(PEER_TIMEOUT):
            result = await driver()
    except TimeoutError:
        raise CheckError(f"{label}: {wire.failure or 'scenario timed out'}") from None
    except CheckError as error:
        raise CheckError(f"{label}: {wire.failure or error}") from None
    finally:
        server.active = None
    if wire.failure:
        raise CheckError(f"{label}: {wire.failure}")
    return {**result, "paths": wire.paths, "post_count": wire.post_count, "receipt": wire.receipt}


async def compare_clients(binary: Path, directory: Path) -> dict[str, object]:
    server_tls, client_tls, certificate = make_tls(directory)
    script = FixtureServer()
    logger = logging.Logger("exness.transport_check")
    logger.disabled = True
    async with serve(
        script.handle_websocket,
        LOOPBACK,
        0,
        ssl=server_tls,
        process_request=script.process_request,
        logger=logger,
        compression=None,
        open_timeout=IO_TIMEOUT,
        close_timeout=IO_TIMEOUT,
        ssl_handshake_timeout=IO_TIMEOUT,
        ssl_shutdown_timeout=IO_TIMEOUT,
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        script.origin = f"https://{LOOPBACK}:{port}"
        report: dict[str, object] = {}
        for scenario in Scenario:
            if scenario is Scenario.LOST_ACK:
                continue
            python = await observe(
                script, scenario, "Python", partial(run_python, script.origin, client_tls, scenario)
            )
            go = await observe(
                script,
                scenario,
                "Go",
                partial(run_go, binary, script.origin, certificate, scenario),
            )
            compare_transcripts(scenario, python, go)
            report[scenario] = python
        scenario = Scenario.LOST_ACK
        with LostAckServer(server_tls) as https:
            thread = threading.Thread(target=https.run, daemon=True)
            thread.start()
            try:
                python = await observe_lost_ack(
                    https, "Python", partial(run_python, https.origin, client_tls, scenario)
                )
                go = await observe_lost_ack(
                    https, "Go", partial(run_go, binary, https.origin, certificate, scenario)
                )
                compare_transcripts(scenario, python, go)
                report[scenario] = python
            finally:
                https.stop()
                thread.join(IO_TIMEOUT)
                if thread.is_alive():
                    raise CheckError("HTTPS fixture shutdown timed out")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-root", type=Path, required=True)
    parser.add_argument("--go-revision", required=True)
    parser.add_argument("--go", default="go")
    args = parser.parse_args(argv)
    peer = args.go_root.resolve()
    try:
        verify_peer(peer, args.go_revision)
        try:
            with tempfile.TemporaryDirectory(prefix="exness-transport-") as temporary:
                directory = Path(temporary)
                binary = build_go(args.go, peer, directory)
                transcript = asyncio.run(compare_clients(binary, directory))
        finally:
            verify_peer(peer, args.go_revision)
    except (CheckError, ParityError, ValueError) as error:
        print(f"Transport check failed: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"Transport check failed: local setup error (errno {error.errno})", file=sys.stderr)
        return 1
    except subprocess.TimeoutExpired:
        print("Transport check failed: checkout verification timed out", file=sys.stderr)
        return 1
    report = {"status": "matched", "go_revision": args.go_revision, "transcript": transcript}
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
