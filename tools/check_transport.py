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

from exness import AsyncExnessClient, Credentials, ExnessError, ProtocolError, StreamOptions
from exness.models import Tick
from tools.check_parity import ParityError, verify_peer
from tools.transport_transcript import compare_transcripts, verify_request

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures"
VECTOR = FIXTURES / "signing-vector.json"
GO_DRIVER = ROOT / "tools/transport_driver.go"
GO_MODULE = "github.com/angru/exness-go"
LOOPBACK = "127.0.0.1"
ACCOUNT = "123456"
INSTRUMENT = "EURUSD"
DISCOVERY_PATH = f"/v1/trading/access-point?account_id={ACCOUNT}"
ACCOUNT_PATH = f"/v1/configuration/accounts/{ACCOUNT}/account"
TICKS_PATH = f"/v1/server-events/accounts/{ACCOUNT}/ws/ticks"
SIGNED_HEADERS = ("exn-data", "exn-sign", "exn-idempotency-key")
IO_TIMEOUT = 3
PEER_TIMEOUT = 15
BUILD_TIMEOUT = 120


class CheckError(Exception):
    """A transport check failure with a diagnostic safe to report."""


@dataclass
class WireTranscript:
    paths: list[str] = field(default_factory=list)
    subscription: object = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    failure: str | None = None


class FixtureServer:
    def __init__(self) -> None:
        self.origin = ""
        self.active: WireTranscript | None = None
        self.account = (FIXTURES / "account.json").read_text(encoding="utf-8")
        self.tick = (FIXTURES / "tick.json").read_text(encoding="utf-8")

    def process_request(self, ws: ServerConnection, request: Request) -> Response | None:
        wire = self.active
        if wire is None:
            return ws.respond(HTTPStatus.SERVICE_UNAVAILABLE, "No client scenario is active")
        try:
            self.record_request(wire, request)
        except CheckError as error:
            wire.failure = wire.failure or str(error)
            return ws.respond(HTTPStatus.BAD_REQUEST, "Unexpected request")
        if request.path == TICKS_PATH:
            return None
        body = self.account
        if request.path == DISCOVERY_PATH:
            body = json.dumps({"access_point": self.origin, "account_id": ACCOUNT})
        response = ws.respond(HTTPStatus.OK, body)
        del response.headers["Content-Type"]
        response.headers["Content-Type"] = "application/json"
        return response

    def record_request(self, wire: WireTranscript, request: Request) -> None:
        if request.path not in (DISCOVERY_PATH, ACCOUNT_PATH, TICKS_PATH):
            raise CheckError("unexpected request target")
        if len(wire.paths) >= 4 or TICKS_PATH in wire.paths:
            raise CheckError("unexpected extra request")
        upgrade = request.headers.get_all("Upgrade")
        if request.path == TICKS_PATH:
            if [value.lower() for value in upgrade] != ["websocket"]:
                raise CheckError("ticks request missing WebSocket upgrade")
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

    async def handle_ticks(self, ws: ServerConnection) -> None:
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
                wire.subscription = request["subscribe"]
                await ws.send(self.tick)
                await ws.send("{")
            # Keep the connection open until the client handles the malformed
            # message and closes it. A second subscription is a scenario failure.
            async with asyncio.timeout(IO_TIMEOUT):
                try:
                    await ws.recv()
                except ConnectionClosed:
                    pass
                else:
                    raise CheckError("unexpected extra WebSocket message")
        except (ValueError, ConnectionClosed, TimeoutError):
            wire.failure = wire.failure or "WebSocket fixture exchange did not complete"
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        finally:
            wire.closed.set()


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


async def run_python(origin: str, tls: ssl.SSLContext) -> dict[str, object]:
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    try:
        async with AsyncExnessClient(
            credentials=Credentials(vector["api_key"], vector["private_key_hex"]),
            account_id=ACCOUNT,
            discovery_url=origin,
            timeout=IO_TIMEOUT,
            transport=httpx.AsyncHTTPTransport(verify=tls, trust_env=False),
        ) as client:
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
    except (ExnessError, StopAsyncIteration):
        raise CheckError("SDK did not complete the account and tick scenario") from None


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


async def run_go(binary: Path, origin: str, certificate: Path) -> dict[str, object]:
    process = await asyncio.create_subprocess_exec(
        str(binary),
        origin,
        str(certificate),
        str(VECTOR),
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
    if not isinstance(result, dict) or set(result) != {"accounts", "tick", "error"}:
        raise CheckError("Go consumer returned unexpected result fields")
    return result


async def observe(
    server: FixtureServer, label: str, driver: Callable[[], Awaitable[dict[str, object]]]
) -> dict[str, object]:
    wire = WireTranscript()
    server.active = wire
    try:
        async with asyncio.timeout(PEER_TIMEOUT):
            result = await driver()
            if TICKS_PATH not in wire.paths:
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
    return {**result, "paths": wire.paths, "subscription": wire.subscription}


async def compare_clients(binary: Path, directory: Path) -> dict[str, object]:
    server_tls, client_tls, certificate = make_tls(directory)
    script = FixtureServer()
    logger = logging.Logger("exness.transport_check")
    logger.disabled = True
    async with serve(
        script.handle_ticks,
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
        python = await observe(script, "Python", lambda: run_python(script.origin, client_tls))
        go = await observe(script, "Go", lambda: run_go(binary, script.origin, certificate))
        compare_transcripts(python, go)
    return python


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
