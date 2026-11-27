import asyncio
import json
import ssl
import subprocess
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qsl, quote, urlencode, urlsplit

import httpx
import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close
from websockets.http11 import Request
from websockets.server import ServerProtocol

from exness import (
    AsyncExnessClient,
    Credentials,
    ExnessError,
    OperationReference,
    TransportError,
    UnknownOutcomeError,
)
from exness.auth import _Signer
from exness.models import AckResponse, OperationStatus
from exness.requests import OpenPositionRequest
from tools import check_transport as transport
from tools.transport_transcript import (
    ACCOUNT,
    ACCOUNT_PATH,
    DISCOVERY_PATH,
    EXPECTED,
    OPERATION_STATUS_PATH,
    ORDERS_HISTORY_PATH,
    ORDERS_LIMIT,
    ORDERS_QUERIES,
    POSITIONS_PATH,
    TICKS_PATH,
    TRADING_PATH,
    Scenario,
    compare_transcripts,
)

WRITE_SCENARIOS = (Scenario.LOST_ACK, Scenario.LOST_ACK_RECOVERY, Scenario.ACKNOWLEDGED_OPEN)
LOOPBACK_SCENARIOS = [scenario for scenario in Scenario if scenario not in WRITE_SCENARIOS]


@pytest.mark.anyio
async def test_each_tls_listener_serves_separate_python_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_python = transport.run_python
    runs: list[tuple[str, Scenario, str | None]] = []

    async def python_peer(
        origin: str, tls: ssl.SSLContext, scenario: Scenario, *, stream_origin: str | None = None
    ) -> dict[str, object]:
        runs.append((origin, scenario, stream_origin))
        return await run_python(origin, tls, scenario, stream_origin=stream_origin)

    async def second_peer(
        binary: Path,
        origin: str,
        certificate: Path,
        scenario: Scenario,
        *,
        stream_origin: str | None = None,
    ) -> dict[str, object]:
        assert binary == tmp_path / "driver"
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls.load_verify_locations(cafile=certificate)
        return await python_peer(origin, tls, scenario, stream_origin=stream_origin)

    monkeypatch.setattr(transport, "run_python", python_peer)
    monkeypatch.setattr(transport, "run_go", second_peer)
    assert await transport.compare_clients(tmp_path / "driver", tmp_path) == EXPECTED
    stream_origins = {origin for origin, scenario, _ in runs if scenario in LOOPBACK_SCENARIOS}
    write_origins = {origin for origin, scenario, _ in runs if scenario in WRITE_SCENARIOS}
    assert len(stream_origins) == len(write_origins) == 1
    assert stream_origins.isdisjoint(write_origins)
    assert {stream for _, scenario, stream in runs if scenario is Scenario.LOST_ACK_RECOVERY} == (
        stream_origins
    )
    assert [scenario for _, scenario, _ in runs] == [
        scenario for scenario in Scenario for _ in range(2)
    ]


class MemorySocket:
    def __init__(
        self,
        incoming: asyncio.Queue[str | ConnectionClosed],
        outgoing: asyncio.Queue[str | ConnectionClosed],
    ) -> None:
        self.incoming = incoming
        self.outgoing = outgoing

    async def send(self, message: str) -> None:
        await self.outgoing.put(message)

    async def recv(self) -> str:
        message = await self.incoming.get()
        if isinstance(message, ConnectionClosed):
            raise message
        return message

    async def close(self, code: int = 1000) -> None:
        error = ConnectionClosedOK if code == 1000 else ConnectionClosedError
        await self.outgoing.put(error(Close(code, ""), None))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scenario", "failure"),
    [
        *((scenario, None) for scenario in LOOPBACK_SCENARIOS),
        (Scenario.HMR_MISSING_SNAPSHOT, "unexpected HMR protocol error"),
        (Scenario.LOST_ACK_RECOVERY, None),
        (Scenario.LOST_ACK_RECOVERY, "expected a matching terminal transaction"),
        (Scenario.LOST_ACK_RECOVERY, "lost acknowledgement did not preserve the caller key"),
    ],
)
async def test_python_scenarios_without_sockets(
    monkeypatch: pytest.MonkeyPatch, scenario: Scenario, failure: str | None
) -> None:
    script = transport.FixtureServer()
    script.origin = "https://127.0.0.1"
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ws = Mock(respond=ServerProtocol().reject)
    https = Mock(spec=transport.WriteServer, origin="https://127.0.0.1:4443", active=None)
    recovers_write = scenario is Scenario.LOST_ACK_RECOVERY
    if failure == "unexpected HMR protocol error":
        script.hmr_update = "{"
    if failure == "expected a matching terminal transaction":
        script.matching_transaction = script.foreign_transaction
    if failure == "lost acknowledgement did not preserve the caller key":
        open_position = AsyncExnessClient.open_position

        async def lose_key(
            client: AsyncExnessClient, request: OpenPositionRequest, *, idempotency_key: str
        ) -> AckResponse:
            try:
                return await open_position(client, request, idempotency_key=idempotency_key)
            except UnknownOutcomeError as error:
                error.idempotency_key = ""
                raise

        monkeypatch.setattr(AsyncExnessClient, "open_position", lose_key)

    observed_write: transport.WriteWire | None = None

    def dispatch(request: httpx.Request) -> httpx.Response:
        nonlocal observed_write
        if request.url.port == 4443:
            assert recovers_write
            observed_write = https.active
            response_bytes = exchange_http(https, request)
            if request.method == "POST":
                assert response_bytes == b""
                raise httpx.RemoteProtocolError("synthetic lost acknowledgement")
            assert response_bytes.startswith(b"HTTP/1.1 200 ")
            return httpx.Response(200, content=response_bytes.split(b"\r\n\r\n", 1)[1])
        if recovers_write:
            assert observed_write is not None and observed_write.post_count == 1
            assert observed_write.receipt == EXPECTED[Scenario.LOST_ACK]["receipt"]
            assert failure != "lost acknowledgement did not preserve the caller key"
        response = script.process_request(
            ws,
            Request(request.url.raw_path.decode("ascii"), Headers(request.headers.multi_items())),
        )
        assert response is not None
        return httpx.Response(response.status_code, content=response.body)

    async with asyncio.TaskGroup() as tasks:

        async def connect(
            url: str, *, additional_headers: dict[str, str], **kwargs: object
        ) -> Mock:
            headers = Headers({**additional_headers, "Upgrade": "websocket"})
            assert script.process_request(ws, Request(urlsplit(url).path, headers)) is None
            upstream: asyncio.Queue[str | ConnectionClosed] = asyncio.Queue()
            downstream: asyncio.Queue[str | ConnectionClosed] = asyncio.Queue()
            client = MemorySocket(downstream, upstream)
            server = Mock(wraps=MemorySocket(upstream, downstream))
            tasks.create_task(script.handle_websocket(server))
            return Mock(wraps=client)

        monkeypatch.setattr(
            httpx, "AsyncHTTPTransport", Mock(return_value=httpx.MockTransport(dispatch))
        )
        monkeypatch.setattr("exness.ticks._NoRedirectConnect", connect)

        async def driver() -> dict[str, object]:
            if recovers_write:
                return await transport.run_python(
                    https.origin, tls, scenario, stream_origin=script.origin
                )
            return await transport.run_python(script.origin, tls, scenario)

        async def observe() -> dict[str, object]:
            return await transport.observe(
                script, scenario, "Python", driver, https=https if recovers_write else None
            )

        if failure is None:
            compare_transcripts(scenario, await observe(), EXPECTED[scenario])
        else:
            with pytest.raises(transport.CheckError, match=f"{scenario}: Python: {failure}"):
                await observe()
    if recovers_write:
        assert observed_write is not None and observed_write.post_count == 1
    assert script.active is None and https.active is None


def exchange_http(server: transport.WriteServer, request: httpx.Request) -> bytes:
    line = request.method.encode() + b" " + request.url.raw_path + b" HTTP/1.1\r\n"
    headers = b"".join(name + b": " + value + b"\r\n" for name, value in request.headers.raw)
    connection = Mock()
    connection.makefile.return_value = BytesIO(line + headers + b"\r\n" + request.content)
    transport.WriteHandler(connection, (transport.LOOPBACK, 12345), server)
    return b"".join(call.args[0] for call in connection.sendall.call_args_list)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("case", "failure", "posts"),
    [
        ("valid", None, 1),
        ("duplicate", "duplicate POST", 2),
        ("changed_body", "invalid signature for received request", 1),
        ("changed_key", "invalid signature for received request", 1),
        ("incomplete_body", "order body did not complete", 1),
        ("oversized_body", "invalid order body length", 1),
    ],
)
async def test_lost_ack_http_receipt_and_python_outcome_without_sockets(
    monkeypatch: pytest.MonkeyPatch, case: str, failure: str | None, posts: int
) -> None:
    server = Mock(spec=transport.WriteServer, origin="https://127.0.0.1", active=None)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    observation: transport.WriteWire | None = None

    def dispatch(request: httpx.Request) -> httpx.Response:
        nonlocal observation
        observation = server.active
        if request.method == "GET":
            response = exchange_http(server, request)
            assert response.startswith(b"HTTP/1.1 200 ")
            return httpx.Response(200, content=response.split(b"\r\n\r\n", 1)[1])
        if case == "changed_body":
            request = httpx.Request(
                request.method, request.url, headers=request.headers, content=request.content + b" "
            )
            request.headers["Content-Length"] = str(len(request.content))
        elif case == "changed_key":
            request.headers["EXN-IDEMPOTENCY-KEY"] = "different-test-key"
        elif case == "incomplete_body":
            request.headers["Content-Length"] = str(len(request.content) + 1)
        elif case == "oversized_body":
            request.headers["Content-Length"] = str(transport.MAX_ORDER_BODY + 1)
        assert exchange_http(server, request) == b""
        if case == "duplicate":
            assert exchange_http(server, request) == b""
        raise httpx.RemoteProtocolError("test cause that must stay out of the transcript")

    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport", Mock(return_value=httpx.MockTransport(dispatch))
    )

    async def driver() -> dict[str, object]:
        return await transport.run_python(server.origin, tls, Scenario.LOST_ACK)

    if failure is None:
        result = await transport.observe(None, Scenario.LOST_ACK, "Python", driver, https=server)
        compare_transcripts(Scenario.LOST_ACK, result, EXPECTED[Scenario.LOST_ACK])
    else:
        with pytest.raises(transport.CheckError, match=f"lost_ack: Python: {failure}"):
            await transport.observe(None, Scenario.LOST_ACK, "Python", driver, https=server)
    assert server.active is None
    assert observation is not None and observation.post_count == posts
    if case in ("valid", "duplicate"):
        assert observation.receipt == EXPECTED[Scenario.LOST_ACK]["receipt"]
        assert observation.paths == [DISCOVERY_PATH, POSITIONS_PATH]
    else:
        assert observation.receipt is None
        assert observation.paths == [DISCOVERY_PATH]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("case", "failure", "posts", "polls"),
    [
        ("valid", None, 1, 2),
        ("wrong_target", "unexpected operation status target", 1, 0),
        ("wrong_id", "unexpected operation status target", 1, 0),
        ("before_ack", "operation status requested before acknowledgement", 1, 2),
        ("extra_poll", "unexpected extra operation status request", 1, 2),
        ("duplicate", "duplicate POST", 2, 2),
    ],
)
async def test_acknowledged_open_without_sockets(
    monkeypatch: pytest.MonkeyPatch,
    credentials: Credentials,
    case: str,
    failure: str | None,
    posts: int,
    polls: int,
) -> None:
    scenario = Scenario.ACKNOWLEDGED_OPEN
    server = Mock(spec=transport.WriteServer, origin="https://127.0.0.1", active=None)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    signer = _Signer(credentials)
    observation: transport.WriteWire | None = None
    ack_schema = contract_validator("AckResponse")
    status_schema = contract_validator("OperationStatusResponse")

    def status_request(target: str) -> httpx.Request:
        return httpx.Request("GET", server.origin + target, headers=signer.get_headers(target))

    def dispatch(request: httpx.Request) -> httpx.Response:
        nonlocal observation
        observation = server.active
        assert observation is not None
        if request.method == "POST" and case == "before_ack":
            assert exchange_http(server, status_request(OPERATION_STATUS_PATH)) == b""
        if request.url.path == OPERATION_STATUS_PATH and case in ("wrong_target", "wrong_id"):
            # An equivalent numeric ID still changes the signed request target.
            wrong_id = "789" if case == "wrong_target" else "790"
            wrong_target = f"/v1/trading/accounts/123456/operations/{wrong_id}"
            assert exchange_http(server, status_request(wrong_target)) == b""
            raise httpx.RemoteProtocolError("synthetic rejected request")
        if case == "extra_poll" and observation.status_responses == ["pending", "confirmed"]:
            assert request.url.path == OPERATION_STATUS_PATH
            assert exchange_http(server, request) == b""
            raise httpx.RemoteProtocolError("synthetic rejected extra poll")

        raw_response = exchange_http(server, request)
        headers, body = raw_response.split(b"\r\n\r\n", 1)
        code = int(headers.split(b" ", 2)[1])
        if request.method == "POST":
            assert code == HTTPStatus.ACCEPTED
            ack_schema.validate(json.loads(body))
            if case == "duplicate":
                assert exchange_http(server, request) == b""
        else:
            assert code == HTTPStatus.OK
            if request.url.path == OPERATION_STATUS_PATH:
                status_schema.validate(json.loads(body))
        return httpx.Response(code, content=body)

    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport", Mock(return_value=httpx.MockTransport(dispatch))
    )
    if case == "extra_poll":
        wait_operation = AsyncExnessClient.wait_operation

        async def wait_with_extra_poll(
            client: AsyncExnessClient,
            reference: OperationReference,
            *,
            timeout: float,
            poll_interval: float,
        ) -> OperationStatus:
            result = await wait_operation(
                client, reference, timeout=timeout, poll_interval=poll_interval
            )
            assert result.status == "confirmed" and reference.operation_id is not None
            with pytest.raises(TransportError):
                await client.get_operation_status(reference.operation_id)
            return result

        monkeypatch.setattr(AsyncExnessClient, "wait_operation", wait_with_extra_poll)

    async def driver() -> dict[str, object]:
        return await transport.run_python(server.origin, tls, scenario)

    if failure is None:
        result = await transport.observe(None, scenario, "Python", driver, https=server)
        compare_transcripts(scenario, result, EXPECTED[scenario])
    else:
        with pytest.raises(transport.CheckError, match=f"{scenario}: Python: {failure}"):
            await transport.observe(None, scenario, "Python", driver, https=server)

    assert server.active is None
    assert observation is not None and observation.post_count == posts
    assert observation.status_responses == ["pending", "confirmed"][:polls]
    assert observation.paths == [DISCOVERY_PATH, POSITIONS_PATH, *([OPERATION_STATUS_PATH] * polls)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("case", "failure", "pages"),
    [
        ("equivalent_encoding", None, 2),
        ("changed_cursor", "unexpected order history query", 1),
        ("duplicate_cursor", "invalid order history query", 1),
        ("extra_filter", "unexpected order history query", 1),
        ("changed_encoding", "invalid signature for received request", 1),
        ("bad_signature", "invalid signature for received request", 1),
        ("extra_request", "unexpected extra request", 2),
    ],
)
async def test_order_history_request_validation_without_sockets(
    monkeypatch: pytest.MonkeyPatch,
    credentials: Credentials,
    case: str,
    failure: str | None,
    pages: int,
) -> None:
    scenario = Scenario.ORDERS_HISTORY_REPEATED_CURSOR
    script = transport.FixtureServer()
    script.origin = "https://127.0.0.1"
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ws = Mock(respond=ServerProtocol().reject)
    signer = _Signer(credentials)
    observation: transport.WireTranscript | None = None

    def dispatch(request: httpx.Request) -> httpx.Response:
        nonlocal observation
        observation = script.active
        assert observation is not None
        target = request.url.raw_path.decode("ascii")
        headers = Headers(request.headers.multi_items())
        if request.url.path == ORDERS_HISTORY_PATH:
            if case == "equivalent_encoding":
                pairs = parse_qsl(request.url.query.decode("ascii"))
                query = urlencode(
                    [
                        (key, value.replace("+00:00", "Z") if key == "from" else value)
                        for key, value in reversed(pairs)
                    ],
                    quote_via=quote,
                )
                target = f"{ORDERS_HISTORY_PATH}?{query}"
                headers = Headers(signer.get_headers(target))
            elif len(observation.queries) == 1:
                if case in {"changed_cursor", "duplicate_cursor", "extra_filter"}:
                    if case == "changed_cursor":
                        target = f"{ORDERS_HISTORY_PATH}?cursor=corrupted&limit={ORDERS_LIMIT}"
                    elif case == "duplicate_cursor":
                        cursor = dict(parse_qsl(request.url.query.decode("ascii")))["cursor"]
                        target += "&" + urlencode({"cursor": cursor})
                    else:
                        target += "&instrument=EURUSD"
                    headers = Headers(signer.get_headers(target))
                elif case == "changed_encoding":
                    target = target.replace("+", "%20")
                    assert target != request.url.raw_path.decode("ascii")
                elif case == "bad_signature":
                    del headers["exn-sign"]
                    headers["exn-sign"] = "invalid"
        response = script.process_request(ws, Request(target, headers))
        assert response is not None
        return httpx.Response(response.status_code, content=response.body)

    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport", Mock(return_value=httpx.MockTransport(dispatch))
    )
    if case == "extra_request":
        run_history = transport.run_python_orders_history

        async def run_history_with_extra_request(client: AsyncExnessClient) -> dict[str, object]:
            result = await run_history(client)
            assert result["outcome"] == "ProtocolError: repeated continuation cursor"
            with pytest.raises(ExnessError):
                await client.get_orders_history(
                    cursor=ORDERS_QUERIES[1]["cursor"], limit=ORDERS_LIMIT
                )
            return result

        monkeypatch.setattr(transport, "run_python_orders_history", run_history_with_extra_request)

    async def driver() -> dict[str, object]:
        return await transport.run_python(script.origin, tls, scenario)

    if failure is None:
        result = await transport.observe(script, scenario, "Python", driver)
        compare_transcripts(scenario, result, EXPECTED[scenario])
    else:
        with pytest.raises(transport.CheckError, match=f"{scenario}: Python: {failure}"):
            await transport.observe(script, scenario, "Python", driver)

    assert script.active is None
    assert observation is not None
    assert observation.paths == [DISCOVERY_PATH, *([ORDERS_HISTORY_PATH] * pages)]
    assert observation.queries == list(ORDERS_QUERIES[:pages])


def contract_validator(schema: str) -> Draft202012Validator:
    contract_path = transport.ROOT / "contracts/trading.public.yaml"
    contract = yaml.safe_load(contract_path.read_text())
    resource = Resource.from_contents(contract, default_specification=DRAFT202012)
    return Draft202012Validator(
        {"$ref": f"urn:exness:trading#/components/schemas/{schema}"},
        registry=Registry().with_resource("urn:exness:trading", resource),
        format_checker=FormatChecker(),
    )


def test_derived_trading_frames_match_pinned_schema() -> None:
    validator = contract_validator("WSTradingEvent")
    script = transport.FixtureServer()
    for frame in (
        script.replacement_snapshot,
        script.foreign_transaction,
        script.matching_transaction,
    ):
        validator.validate(json.loads(frame))


def test_hmr_frames_match_pinned_schema() -> None:
    script = transport.FixtureServer()
    snapshot = contract_validator("WSHmrSnapshotEvent")
    snapshot.validate(json.loads(script.hmr_snapshot))
    snapshot.validate(json.loads(script.hmr_empty_snapshot))
    contract_validator("WSHmrUpdateEvent").validate(json.loads(script.hmr_update))


def test_order_history_pages_match_pinned_schema() -> None:
    validator = contract_validator("OrdersHistoryResponse")
    first, second = (json.loads(page) for page in transport.FixtureServer().orders_pages)
    for page in (first, second):
        validator.validate(page)
        assert page["has_more"] is True
        assert page["next_cursor"] == ORDERS_QUERIES[1]["cursor"]
    assert [page["items"][0]["order"]["order_id"] for page in (first, second)] == ["1002", "1003"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scenario", "fixture"),
    [
        (Scenario.TRADING_RECOVERY, "trading_subscription"),
        (Scenario.HMR_RECOVERY, "hmr_subscription"),
    ],
)
async def test_subscription_id_must_change_on_reconnect(scenario: Scenario, fixture: str) -> None:
    script = transport.FixtureServer()
    wire = transport.WireTranscript(scenario)
    script.active = wire
    subscription = (transport.FIXTURES / f"{fixture}.json").read_text(encoding="utf-8")
    first = AsyncMock()
    second = AsyncMock()
    first.recv.side_effect = [subscription, ConnectionClosedError(None, Close(1012, ""))]
    second.recv.return_value = subscription

    await script.handle_websocket(first)
    await script.handle_websocket(second)

    first.close.assert_awaited_once_with(code=1012)
    second.send.assert_not_awaited()
    assert wire.failure == "reused subscription ID"
    assert wire.closed.is_set()


def test_recovery_discovery_requires_verified_post_receipt() -> None:
    script = transport.FixtureServer()
    write = transport.WriteWire(Scenario.LOST_ACK_RECOVERY, post_count=1)
    wire = transport.WireTranscript(Scenario.LOST_ACK_RECOVERY, write=write)
    script.active = wire
    ws = Mock(respond=ServerProtocol().reject)

    response = script.process_request(ws, Request(DISCOVERY_PATH, Headers()))

    assert response is not None and response.status_code == HTTPStatus.BAD_REQUEST
    assert wire.paths == []
    assert wire.failure == "recovery stream started before the verified POST receipt"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario", [Scenario.TRADING_MISSING_SNAPSHOT, Scenario.HMR_MISSING_SNAPSHOT]
)
async def test_third_upgrade_is_rejected_even_if_driver_finishes(scenario: Scenario) -> None:
    script = transport.FixtureServer()
    ws = Mock(respond=ServerProtocol().reject)

    async def driver() -> dict[str, object]:
        wire = script.active
        assert wire is not None
        wire.paths.extend([DISCOVERY_PATH, TRADING_PATH, TRADING_PATH])
        wire.closed.set()
        response = script.process_request(ws, Request(TRADING_PATH, Headers()))
        assert response is not None and response.status_code == HTTPStatus.BAD_REQUEST
        return {}

    with pytest.raises(transport.CheckError, match="peer: unexpected extra request"):
        await transport.observe(script, scenario, "peer", driver)
    assert script.active is None


@pytest.mark.anyio
async def test_signed_account_reads_use_the_fixture_router(credentials: Credentials) -> None:
    script = transport.FixtureServer()
    script.origin = "https://127.0.0.1"
    wire = transport.WireTranscript(Scenario.REST_TICKS)
    script.active = wire
    ws = Mock(respond=ServerProtocol().reject)

    def dispatch(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET" and not request.content
        response = script.process_request(
            ws,
            Request(request.url.raw_path.decode("ascii"), Headers(request.headers.multi_items())),
        )
        assert response is not None
        assert response.headers["Content-Type"] == "application/json"
        return httpx.Response(response.status_code, content=response.body)

    async with AsyncExnessClient(
        credentials=credentials,
        account_id=ACCOUNT,
        discovery_url=script.origin,
        transport=httpx.MockTransport(dispatch),
    ) as client:
        accounts = [(await client.get_account()).account_id for _ in range(2)]
    assert accounts == ["123456", "123456"]
    assert wire.paths == [DISCOVERY_PATH, ACCOUNT_PATH, ACCOUNT_PATH]
    assert wire.failure is None


def test_bad_signature_is_rejected_without_recording_request() -> None:
    script = transport.FixtureServer()
    wire = transport.WireTranscript(Scenario.REST_TICKS)
    script.active = wire
    vector = json.loads(transport.VECTOR.read_text(encoding="utf-8"))
    headers = Headers(
        {
            "exn-data": vector["exn_data"],
            "exn-sign": vector["exn_sign"],
            "exn-idempotency-key": "",
        }
    )
    ws = Mock(respond=ServerProtocol().reject)
    response = script.process_request(ws, Request(ACCOUNT_PATH, headers))
    assert response is not None and response.status_code == HTTPStatus.BAD_REQUEST
    assert wire.paths == []
    assert wire.failure == "invalid signature for received request"


@pytest.mark.anyio
async def test_observation_fails_if_websocket_sends_an_extra_subscription() -> None:
    script = transport.FixtureServer()
    subscription = (transport.FIXTURES / "tick_subscription.json").read_text(encoding="utf-8")
    ws = AsyncMock()
    ws.recv.side_effect = [subscription, subscription]

    async def driver() -> dict[str, object]:
        assert script.active is not None
        script.active.paths.append(TICKS_PATH)
        await script.handle_websocket(ws)
        return {}

    with pytest.raises(transport.CheckError, match="peer: unexpected extra WebSocket message"):
        await transport.observe(script, Scenario.REST_TICKS, "peer", driver)
    assert ws.send.await_count == 2
    assert script.active is None


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", [Scenario.REST_TICKS, Scenario.LOST_ACK_RECOVERY])
async def test_consumer_timeout_kills_and_reaps_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: Scenario
) -> None:
    async def communicate() -> tuple[bytes, bytes]:
        return await asyncio.Future()

    process = Mock(
        returncode=None, communicate=AsyncMock(side_effect=communicate), wait=AsyncMock()
    )
    launch = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    monkeypatch.setattr(transport, "PEER_TIMEOUT", 0)
    script = transport.FixtureServer()
    stream_origin = "https://127.0.0.1:4443" if scenario is Scenario.LOST_ACK_RECOVERY else None

    async def driver() -> dict[str, object]:
        return await transport.run_go(
            tmp_path / "driver",
            script.origin,
            tmp_path / "cert.pem",
            scenario,
            stream_origin=stream_origin,
        )

    with pytest.raises(transport.CheckError, match="Go: scenario timed out"):
        await transport.observe(script, scenario, "Go", driver)
    assert launch.call_args.kwargs["env"] == {}
    assert launch.call_args.args[4:] == (
        (scenario,) if stream_origin is None else (scenario, stream_origin)
    )
    process.kill.assert_called_once_with()
    process.wait.assert_awaited_once_with()
    assert script.active is None


def test_cli_rechecks_peer_and_cleans_build_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    peer = tmp_path / "peer"
    peer.mkdir()
    module = f"module {transport.GO_MODULE}\n\ngo 1.26\n"
    (peer / "go.mod").write_text(module, encoding="utf-8")
    (peer / "go.sum").touch()
    verify = Mock()
    commands = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(transport, "verify_peer", verify)
    monkeypatch.setattr(subprocess, "run", commands)
    monkeypatch.setattr(
        transport, "compare_clients", AsyncMock(side_effect=transport.CheckError("fixture failed"))
    )

    sha = "0" * 40
    assert transport.main(["--go-root", str(peer), "--go-revision", sha]) == 1
    assert capsys.readouterr().err == "Transport check failed: fixture failed\n"
    assert [call.args for call in verify.call_args_list] == [(peer, sha), (peer, sha)]
    mod_edit, build = commands.call_args_list
    assert mod_edit.args[0][:3] == ["go", "mod", "edit"]
    assert f"-replace={transport.GO_MODULE}={peer}" in mod_edit.args[0]
    assert build.args[0][:3] == ["go", "build", "-mod=readonly"]
    for command in (mod_edit, build):
        assert not command.kwargs["cwd"].exists()
        assert command.kwargs["env"]["GOPROXY"] == "off"
        assert command.kwargs["env"]["GOWORK"] == "off"
    assert (peer / "go.mod").read_text(encoding="utf-8") == module
