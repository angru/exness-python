import asyncio
import json
import ssl
import subprocess
from http import HTTPStatus
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from websockets.datastructures import Headers
from websockets.http11 import Request
from websockets.server import ServerProtocol

from exness import AsyncExnessClient, Credentials
from tools import check_transport as transport
from tools.transport_transcript import EXPECTED


@pytest.mark.anyio
async def test_one_tls_listener_serves_separate_python_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_python = transport.run_python
    origins = []

    async def python_peer(origin: str, tls: ssl.SSLContext) -> dict[str, object]:
        origins.append(origin)
        return await run_python(origin, tls)

    async def second_peer(binary: Path, origin: str, certificate: Path) -> dict[str, object]:
        assert binary == tmp_path / "driver"
        origins.append(origin)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls.load_verify_locations(cafile=certificate)
        return await run_python(origin, tls)

    monkeypatch.setattr(transport, "run_python", python_peer)
    monkeypatch.setattr(transport, "run_go", second_peer)
    assert await transport.compare_clients(tmp_path / "driver", tmp_path) == EXPECTED
    assert len(origins) == 2 and origins[0] == origins[1]


@pytest.mark.anyio
async def test_signed_account_reads_use_the_fixture_router(credentials: Credentials) -> None:
    script = transport.FixtureServer()
    script.origin = "https://127.0.0.1"
    wire = transport.WireTranscript()
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
        account_id=transport.ACCOUNT,
        discovery_url=script.origin,
        transport=httpx.MockTransport(dispatch),
    ) as client:
        accounts = [(await client.get_account()).account_id for _ in range(2)]
    assert accounts == EXPECTED["accounts"]
    assert wire.paths == EXPECTED["paths"][:3]
    assert wire.failure is None


def test_bad_signature_is_rejected_without_recording_request() -> None:
    script = transport.FixtureServer()
    wire = transport.WireTranscript()
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
    response = script.process_request(ws, Request(transport.ACCOUNT_PATH, headers))
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
        script.active.paths.append(transport.TICKS_PATH)
        await script.handle_ticks(ws)
        return {}

    with pytest.raises(transport.CheckError, match="peer: unexpected extra WebSocket message"):
        await transport.observe(script, "peer", driver)
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
        return await transport.run_go(tmp_path / "driver", script.origin, tmp_path / "cert.pem")

    with pytest.raises(transport.CheckError, match="Go: scenario timed out"):
        await transport.observe(script, "Go", driver)
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
