"""Compare the public Python and Go clients against one synthetic TLS listener."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from http import HTTPStatus
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
)
from exness.models import Tick
from exness.trading import TradingEvent, TradingSnapshotEvent, TransactionEvent
from tools.check_parity import ParityError, verify_peer
from tools.transport_transcript import (
    ACCOUNT,
    ACCOUNT_PATH,
    DISCOVERY_PATH,
    TICKS_PATH,
    TRADING_PATH,
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
            return await run_python_trading(client, tls, scenario)
    except (ExnessError, StopAsyncIteration):
        raise CheckError("SDK did not complete the scenario") from None


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
    fields = (
        {"accounts", "tick", "error"} if scenario is Scenario.REST_TICKS else {"events", "outcome"}
    )
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
