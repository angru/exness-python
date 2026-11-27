"""Compare the public Python and Go clients against shared synthetic TLS fixtures."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
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
from urllib.parse import parse_qsl

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
    OperationReference,
    ProtocolError,
    ServerStreamOptions,
    StreamOptions,
    TradingStreamGap,
    UnknownOutcomeError,
)
from exness.events import HmrSnapshot, HmrUpdate
from exness.models import AckResponse, Tick
from exness.requests import OpenPositionRequest
from exness.trading import TradingEvent, TradingSnapshotEvent, TransactionEvent
from tools.check_parity import ParityError, verify_peer
from tools.transport_transcript import (
    ACCOUNT,
    ACCOUNT_PATH,
    ACKNOWLEDGED_OPEN_KEY,
    DISCOVERY_PATH,
    OPEN_ORDER,
    OPERATION_STATUS_PATH,
    ORDERS_FROM,
    ORDERS_HISTORY_PATH,
    ORDERS_LIMIT,
    ORDERS_QUERIES,
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
STATUS_FIXTURES = ("operation_pending.json", "operation_confirmed.json")
HMR_SCENARIOS = (Scenario.HMR_RECOVERY, Scenario.HMR_MISSING_SNAPSHOT)
TRADING_RECONNECT_SCENARIOS = (Scenario.TRADING_RECOVERY, Scenario.TRADING_MISSING_SNAPSHOT)


class CheckError(Exception):
    """A transport check failure with a diagnostic safe to report."""


@dataclass
class WireTranscript:
    scenario: Scenario
    write: WriteWire | None = None
    paths: list[str] = field(default_factory=list)
    queries: list[dict[str, str]] = field(default_factory=list)
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
        return 2 if self.scenario in (*TRADING_RECONNECT_SCENARIOS, *HMR_SCENARIOS) else 1


class FixtureServer:
    def __init__(self) -> None:
        self.origin = ""
        self.active: WireTranscript | None = None
        self.account = (FIXTURES / "account.json").read_text(encoding="utf-8")
        first_orders_page = (FIXTURES / "orders.json").read_text(encoding="utf-8")
        second_orders_page = json.loads(first_orders_page)
        second_orders_page["items"][0]["order"]["order_id"] = "1003"
        self.orders_pages = (first_orders_page, json.dumps(second_orders_page))
        self.tick = (FIXTURES / "tick.json").read_text(encoding="utf-8")
        self.snapshot = (FIXTURES / "trading_snapshot.json").read_text(encoding="utf-8")
        self.partial_close = (FIXTURES / "transaction_partial_close.json").read_text(
            encoding="utf-8"
        )
        self.balance = (FIXTURES / "transaction_balance.json").read_text(encoding="utf-8")
        self.hmr_snapshot = (FIXTURES / "hmr_snapshot.json").read_text(encoding="utf-8")
        self.hmr_empty_snapshot = (FIXTURES / "hmr_empty_snapshot.json").read_text(encoding="utf-8")
        self.hmr_update = (FIXTURES / "hmr_update.json").read_text(encoding="utf-8")
        replacement = json.loads(self.snapshot)
        replacement["payload"]["account_state"]["balance"] = "12345678901234567790.113456789"
        self.replacement_snapshot = json.dumps(replacement)
        self.foreign_transaction = json.dumps(
            {**json.loads(self.partial_close), "client_request_id": "transport-foreign-0001"}
        )
        rejected = json.loads((FIXTURES / "transaction_rejected.json").read_text(encoding="utf-8"))
        self.matching_transaction = json.dumps({**rejected, "client_request_id": WRITE_KEY})

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
        elif wire.scenario is Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
            body = self.orders_pages[len(wire.queries) - 1]
        response = ws.respond(HTTPStatus.OK, body)
        del response.headers["Content-Type"]
        response.headers["Content-Type"] = "application/json"
        return response

    def record_request(self, wire: WireTranscript, request: Request) -> None:
        if wire.write is not None and wire.write.receipt is None:
            raise CheckError("recovery stream started before the verified POST receipt")
        target = request.path
        path = target
        raw_query = ""
        limits = {DISCOVERY_PATH: 1}
        if wire.scenario is Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
            limits[ORDERS_HISTORY_PATH] = len(ORDERS_QUERIES)
            if target != DISCOVERY_PATH:
                path, _, raw_query = target.partition("?")
        else:
            limits[wire.upgrade_path] = wire.expected_connections
            if wire.scenario is Scenario.REST_TICKS:
                limits[ACCOUNT_PATH] = 2
        if path not in limits:
            raise CheckError("unexpected request target")
        if wire.paths.count(path) >= limits[path]:
            raise CheckError("unexpected extra request")
        upgrade = request.headers.get_all("Upgrade")
        if path == wire.upgrade_path:
            if [value.lower() for value in upgrade] != ["websocket"]:
                raise CheckError("stream request missing WebSocket upgrade")
        elif upgrade:
            raise CheckError("unexpected upgrade on REST request")
        if any(len(request.headers.get_all(name)) != 1 for name in SIGNED_HEADERS):
            raise CheckError("missing or duplicate signing header")
        # The websockets HTTP parser admits only bodyless GET requests. Its
        # request.path preserves the raw target, including the query string.
        try:
            verify_request("GET", target, request.headers)
        except ValueError:
            raise CheckError("invalid signature for received request") from None
        if path == ORDERS_HISTORY_PATH:
            if not wire.paths or wire.paths[0] != DISCOVERY_PATH:
                raise CheckError("order history requested before discovery")
            wire.queries.append(orders_query(raw_query, len(wire.queries)))
        wire.paths.append(path)

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
        elif wire.scenario is Scenario.LOST_ACK_RECOVERY:
            await ws.send(self.snapshot)
            await ws.send(self.foreign_transaction)
            await ws.send(self.matching_transaction)
            await ws.close(code=1000)
        elif wire.scenario in HMR_SCENARIOS:
            if len(wire.subscriptions) == 1:
                await ws.send(self.hmr_snapshot)
                await ws.close(code=1012)
            else:
                if wire.scenario is Scenario.HMR_RECOVERY:
                    await ws.send(self.hmr_empty_snapshot)
                await ws.send(self.hmr_update)
                if wire.scenario is Scenario.HMR_RECOVERY:
                    await ws.close(code=1000)
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


def orders_query(raw_query: str, page: int) -> dict[str, str]:
    try:
        if re.search(r"%(?![0-9a-fA-F]{2})", raw_query):
            raise ValueError
        pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True, errors="strict")
        query = dict(pairs)
        if len(query) != len(pairs):
            raise ValueError
        if page == 0 and "from" in query:
            start = datetime.fromisoformat(query["from"])
            if start == ORDERS_FROM:
                query["from"] = ORDERS_FROM.isoformat()
    except ValueError:
        raise CheckError("invalid order history query") from None
    if query != ORDERS_QUERIES[page]:
        raise CheckError("unexpected order history query")
    return query


@dataclass
class WriteWire:
    scenario: Scenario
    paths: list[str] = field(default_factory=list)
    post_count: int = 0
    receipt: dict[str, object] | None = None
    status_responses: list[object] = field(default_factory=list)
    failure: str | None = None

    @property
    def key(self) -> str:
        return ACKNOWLEDGED_OPEN_KEY if self.scenario is Scenario.ACKNOWLEDGED_OPEN else WRITE_KEY


def acknowledged_response(fixture: str) -> dict[str, object]:
    body = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
    return {**body, "operation_id": "000789", "client_request_id": ACKNOWLEDGED_OPEN_KEY}


class WriteHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: WriteServer

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
            target = self.raw_target
            if target == DISCOVERY_PATH:
                if wire.paths or wire.post_count:
                    raise CheckError("unexpected discovery request")
            elif wire.scenario is not Scenario.ACKNOWLEDGED_OPEN:
                raise CheckError("unexpected discovery request")
            elif target != OPERATION_STATUS_PATH:
                raise CheckError("unexpected operation status target")
            elif wire.receipt is None or not wire.receipt["acknowledgement_sent"]:
                raise CheckError("operation status requested before acknowledgement")
            elif len(wire.status_responses) >= len(STATUS_FIXTURES):
                raise CheckError("unexpected extra operation status request")
            if self.headers.get_all("Transfer-Encoding") or (
                self.headers.get_all("Content-Length") or []
            ) not in ([], ["0"]):
                raise CheckError("unexpected GET request body")
            self.verify_signature()
            wire.paths.append(target)
            if target == DISCOVERY_PATH:
                response: dict[str, object] = {
                    "access_point": self.server.origin,
                    "account_id": ACCOUNT,
                }
            else:
                response = acknowledged_response(STATUS_FIXTURES[len(wire.status_responses)])
                wire.status_responses.append(response["status"])
            self.send_json(HTTPStatus.OK, response)
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        except OSError:
            wire.failure = wire.failure or "GET response did not complete"

    def send_json(self, status: HTTPStatus, response: dict[str, object]) -> None:
        body = json.dumps(response).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

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
            self.verify_signature(body, wire.key)
            order = json.loads(body)
            if order != OPEN_ORDER:
                raise CheckError("unexpected order fields")
            wire.paths.append(self.raw_target)
            wire.receipt = {
                "method": self.command,
                "path": self.raw_target,
                "order": order,
                "signature_verified": True,
                "idempotency_key": wire.key,
                "acknowledgement_sent": False,
            }
            if wire.scenario is Scenario.ACKNOWLEDGED_OPEN:
                self.send_json(HTTPStatus.ACCEPTED, acknowledged_response("ack_accepted.json"))
                wire.receipt["acknowledgement_sent"] = True
        except CheckError as error:
            wire.failure = wire.failure or str(error)
        except (ValueError, RecursionError):
            wire.failure = wire.failure or "invalid order JSON"
        except OSError:
            wire.failure = wire.failure or "acknowledgement response did not complete"


class WriteServer(HTTPServer):
    timeout = 0.05

    def __init__(self, tls: ssl.SSLContext) -> None:
        self.tls = tls
        self.active: WriteWire | None = None
        self._stopping = threading.Event()
        self._connection_lock = threading.Lock()
        self._connection: ssl.SSLSocket | None = None
        super().__init__((LOOPBACK, 0), WriteHandler)
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


async def run_python(
    origin: str, tls: ssl.SSLContext, scenario: Scenario, *, stream_origin: str | None = None
) -> dict[str, object]:
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    credentials = Credentials(vector["api_key"], vector["private_key_hex"])

    def make_client(discovery_url: str) -> AsyncExnessClient:
        return AsyncExnessClient(
            credentials=credentials,
            account_id=ACCOUNT,
            discovery_url=discovery_url,
            timeout=IO_TIMEOUT,
            transport=httpx.AsyncHTTPTransport(verify=tls, trust_env=False),
        )

    try:
        async with make_client(origin) as client:
            if scenario is Scenario.REST_TICKS:
                return await run_python_ticks(client, tls)
            if scenario is Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
                return await run_python_orders_history(client)
            if scenario in TRADING_RECONNECT_SCENARIOS:
                return await run_python_trading(client, tls, scenario)
            if scenario in HMR_SCENARIOS:
                return await run_python_hmr(client, tls, scenario)
            if scenario is Scenario.ACKNOWLEDGED_OPEN:
                return await run_python_acknowledged_open(client)
            error = await submit_lost_ack(client)
        report: dict[str, object] = {
            "outcome": {
                "error": "UnknownOutcomeError",
                "method": error.method,
                "path": error.path,
                "account_id": error.account_id,
                "idempotency_key": error.idempotency_key,
            }
        }
        if scenario is Scenario.LOST_ACK_RECOVERY:
            if stream_origin is None:
                raise CheckError("missing recovery stream fixture origin")
            reference = OperationReference(client_request_id=error.idempotency_key)
            async with make_client(stream_origin) as client:
                report["events"] = await run_python_lost_ack_recovery(client, tls, reference)
        return report
    except (ExnessError, StopAsyncIteration):
        raise CheckError("SDK did not complete the scenario") from None


async def run_python_orders_history(client: AsyncExnessClient) -> dict[str, object]:
    order_ids: list[str] = []
    async with aclosing(
        client.iter_orders_history(
            from_time=ORDERS_FROM, instrument=INSTRUMENT, order_state="filled", limit=ORDERS_LIMIT
        )
    ) as orders:
        try:
            async for item in orders:
                order_ids.append(item.order.order_id)
        except ProtocolError as error:
            if str(error) != "History returned a repeated continuation cursor":
                raise CheckError("unexpected order history protocol error") from None
            if await anext(orders, None) is not None:
                raise CheckError("unexpected order history item after protocol error") from None
            return {
                "order_ids": order_ids,
                "outcome": "ProtocolError: repeated continuation cursor",
            }
    raise CheckError("expected a repeated continuation cursor error")


async def submit_open_position(client: AsyncExnessClient, key: str) -> AckResponse:
    request = OpenPositionRequest.model_validate(OPEN_ORDER)
    return await client.open_position(request, idempotency_key=key)


async def submit_lost_ack(client: AsyncExnessClient) -> UnknownOutcomeError:
    try:
        await submit_open_position(client, WRITE_KEY)
    except UnknownOutcomeError as error:
        if error.idempotency_key != WRITE_KEY:
            raise CheckError("lost acknowledgement did not preserve the caller key") from None
        return error
    raise CheckError("expected UnknownOutcomeError after losing the acknowledgement")


async def run_python_acknowledged_open(client: AsyncExnessClient) -> dict[str, object]:
    ack = await submit_open_position(client, ACKNOWLEDGED_OPEN_KEY)
    reference = OperationReference.from_ack(ack, client_request_id=ACKNOWLEDGED_OPEN_KEY)
    result = await client.wait_operation(reference, timeout=IO_TIMEOUT, poll_interval=0.01)
    return {
        "ack": {
            "status": ack.status,
            "operation_id": ack.operation_id,
            "client_request_id": ack.client_request_id,
        },
        "outcome": {
            "status": result.status,
            "operation_id": result.operation_id,
            "client_request_id": result.client_request_id,
        },
    }


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


async def run_python_hmr(
    client: AsyncExnessClient, tls: ssl.SSLContext, scenario: Scenario
) -> dict[str, object]:
    options = ServerStreamOptions(
        open_timeout=IO_TIMEOUT,
        max_reconnects=2 if scenario is Scenario.HMR_MISSING_SNAPSHOT else 1,
        reconnect_delay=0.01,
    )
    events: list[dict[str, object]] = []
    outcome = "normal_close"
    async with aclosing(
        client.stream_hmr([INSTRUMENT], options=options, ssl_context=tls)
    ) as stream:
        try:
            async for event in stream:
                if isinstance(event, HmrSnapshot):
                    events.append({"kind": "snapshot", **event.model_dump(mode="json")})
                elif isinstance(event, HmrUpdate):
                    events.append({"kind": "update", **event.model_dump(mode="json")})
                else:
                    events.append(
                        {
                            "kind": "gap",
                            "reconnect_attempt": event.reconnect_attempt,
                            "close_code": event.close_code,
                        }
                    )
        except ProtocolError as error:
            if str(error) != "HMR update received before HMR snapshot":
                raise CheckError("unexpected HMR protocol error") from None
            outcome = "ProtocolError"
    return {"events": events, "outcome": outcome}


async def run_python_lost_ack_recovery(
    client: AsyncExnessClient, tls: ssl.SSLContext, reference: OperationReference
) -> list[dict[str, object]]:
    options = StreamOptions(open_timeout=IO_TIMEOUT)
    events: list[dict[str, object]] = []
    selected: dict[str, object] | None = None
    async with aclosing(client.stream_trading(options=options, ssl_context=tls)) as stream:
        async for event in stream:
            observation = trading_observation(event)
            if isinstance(event, TransactionEvent):
                if not reference.matches_event(event):
                    observation["decision"] = "ignored"
                elif event.status is None:
                    observation["decision"] = "matched_without_status"
                else:
                    if selected is not None:
                        raise CheckError("extra matching terminal transaction")
                    selected = {"operation_id": event.operation_id, "status": event.status}
                    observation["decision"] = "selected"
            observation["selected"] = selected
            events.append(observation)
    if selected is None:
        raise CheckError("expected a matching terminal transaction")
    return events


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
    binary: Path,
    origin: str,
    certificate: Path,
    scenario: Scenario,
    *,
    stream_origin: str | None = None,
) -> dict[str, object]:
    extra_args = () if stream_origin is None else (stream_origin,)
    process = await asyncio.create_subprocess_exec(
        str(binary),
        origin,
        str(certificate),
        str(VECTOR),
        scenario,
        *extra_args,
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
        case Scenario.ACKNOWLEDGED_OPEN:
            fields = {"ack", "outcome"}
        case Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
            fields = {"order_ids", "outcome"}
        case _:
            fields = {"events", "outcome"}
    if not isinstance(result, dict) or set(result) != fields:
        raise CheckError("Go consumer returned unexpected result fields")
    return result


async def observe(
    server: FixtureServer | None,
    scenario: Scenario,
    label: str,
    driver: Callable[[], Awaitable[dict[str, object]]],
    *,
    https: WriteServer | None = None,
) -> dict[str, object]:
    stream = None
    write = None
    if https is not None:
        write = https.active = WriteWire(scenario)
    if server is not None:
        stream = server.active = WireTranscript(scenario, write=write)

    def wire_failure() -> str | None:
        return (write.failure if write is not None else None) or (
            stream.failure if stream is not None else None
        )

    try:
        async with asyncio.timeout(PEER_TIMEOUT):
            result = await driver()
            if stream is not None and scenario is not Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
                if stream.paths.count(stream.upgrade_path) != stream.expected_connections:
                    raise CheckError("missing WebSocket handshake")
                await stream.closed.wait()
    except TimeoutError:
        raise CheckError(f"{scenario}: {label}: {wire_failure() or 'scenario timed out'}") from None
    except CheckError as error:
        raise CheckError(f"{scenario}: {label}: {wire_failure() or error}") from None
    finally:
        if server is not None:
            server.active = None
        if https is not None:
            https.active = None
    if failure := wire_failure():
        raise CheckError(f"{scenario}: {label}: {failure}")
    if write is not None:
        result = {
            **result,
            "paths": write.paths,
            "post_count": write.post_count,
            "receipt": write.receipt,
        }
        if scenario is Scenario.ACKNOWLEDGED_OPEN:
            result["status_responses"] = write.status_responses
    if stream is not None:
        if scenario is Scenario.ORDERS_HISTORY_REPEATED_CURSOR:
            result = {**result, "paths": stream.paths, "queries": stream.queries}
        else:
            paths_key = "paths" if write is None else "stream_paths"
            result = {**result, paths_key: stream.paths, "subscriptions": stream.subscriptions}
    return result


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
        with WriteServer(server_tls) as https:
            thread = threading.Thread(target=https.run, daemon=True)
            thread.start()
            try:
                for scenario in Scenario:
                    writes = scenario in (
                        Scenario.LOST_ACK,
                        Scenario.LOST_ACK_RECOVERY,
                        Scenario.ACKNOWLEDGED_OPEN,
                    )
                    stream = (
                        script if not writes or scenario is Scenario.LOST_ACK_RECOVERY else None
                    )
                    write = https if writes else None
                    origin = https.origin if writes else script.origin
                    stream_origin = (
                        script.origin if scenario is Scenario.LOST_ACK_RECOVERY else None
                    )
                    python = await observe(
                        stream,
                        scenario,
                        "Python",
                        partial(
                            run_python, origin, client_tls, scenario, stream_origin=stream_origin
                        ),
                        https=write,
                    )
                    go = await observe(
                        stream,
                        scenario,
                        "Go",
                        partial(
                            run_go,
                            binary,
                            origin,
                            certificate,
                            scenario,
                            stream_origin=stream_origin,
                        ),
                        https=write,
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
