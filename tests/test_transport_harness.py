import asyncio
import json
import ssl
import subprocess
from http import HTTPStatus
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlsplit

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

from exness import AsyncExnessClient, Credentials
from tools import check_transport as transport
from tools.transport_transcript import (
    ACCOUNT,
    ACCOUNT_PATH,
    DISCOVERY_PATH,
    EXPECTED,
    TICKS_PATH,
    TRADING_PATH,
    Scenario,
)


@pytest.mark.anyio
async def test_one_tls_listener_serves_separate_python_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_python = transport.run_python
    runs: list[tuple[str, Scenario]] = []

    async def python_peer(
        origin: str, tls: ssl.SSLContext, scenario: Scenario
    ) -> dict[str, object]:
        runs.append((origin, scenario))
        return await run_python(origin, tls, scenario)

    async def second_peer(
        binary: Path, origin: str, certificate: Path, scenario: Scenario
    ) -> dict[str, object]:
        assert binary == tmp_path / "driver"
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls.load_verify_locations(cafile=certificate)
        return await python_peer(origin, tls, scenario)

    monkeypatch.setattr(transport, "run_python", python_peer)
    monkeypatch.setattr(transport, "run_go", second_peer)
    assert await transport.compare_clients(tmp_path / "driver", tmp_path) == EXPECTED
    assert len({origin for origin, _ in runs}) == 1
    assert [scenario for _, scenario in runs] == [
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
@pytest.mark.parametrize("scenario", list(Scenario))
async def test_python_scenarios_without_sockets(
    monkeypatch: pytest.MonkeyPatch, scenario: Scenario
) -> None:
    script = transport.FixtureServer()
    script.origin = "https://127.0.0.1"
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ws = Mock(respond=ServerProtocol().reject)

    def dispatch(request: httpx.Request) -> httpx.Response:
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
            return await transport.run_python(script.origin, tls, scenario)

        result = await transport.observe(script, scenario, "Python", driver)
    assert result == EXPECTED[scenario]


def test_replacement_snapshot_matches_pinned_schema() -> None:
    contract_path = transport.ROOT / "contracts/trading.public.yaml"
    contract = yaml.safe_load(contract_path.read_text())
    resource = Resource.from_contents(contract, default_specification=DRAFT202012)
    validator = Draft202012Validator(
        {"$ref": "urn:exness:trading#/components/schemas/WSTradingEvent"},
        registry=Registry().with_resource("urn:exness:trading", resource),
        format_checker=FormatChecker(),
    )
    validator.validate(json.loads(transport.FixtureServer().replacement_snapshot))


@pytest.mark.anyio
async def test_transaction_subscription_id_must_change_on_reconnect() -> None:
    script = transport.FixtureServer()
    wire = transport.WireTranscript(Scenario.TRADING_RECOVERY)
    script.active = wire
    subscription = (transport.FIXTURES / "trading_subscription.json").read_text(encoding="utf-8")
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


@pytest.mark.anyio
async def test_third_trading_upgrade_is_rejected_even_if_driver_finishes() -> None:
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
        await transport.observe(script, Scenario.TRADING_MISSING_SNAPSHOT, "peer", driver)
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
async def test_consumer_timeout_kills_and_reaps_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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

    async def driver() -> dict[str, object]:
        return await transport.run_go(
            tmp_path / "driver", script.origin, tmp_path / "cert.pem", Scenario.REST_TICKS
        )

    with pytest.raises(transport.CheckError, match="Go: scenario timed out"):
        await transport.observe(script, Scenario.REST_TICKS, "Go", driver)
    assert launch.call_args.kwargs["env"] == {}
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
