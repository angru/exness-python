import asyncio
import hashlib
import json
import ssl
import threading
from collections.abc import AsyncIterator, Awaitable
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import TypeAlias

import httpx
import pytest
import yaml
from conftest import PUBLIC, fixture_bytes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
from test_async_network import network_client as async_network_client
from test_auth import decode
from test_network import network_client, serve_tls
from test_network import tls_contexts as tls_contexts

from exness import (
    APIError,
    AsyncExnessClient,
    Credentials,
    ExnessClient,
    ProtocolError,
    TransportError,
    UnknownOutcomeError,
    _common,
)
from exness.models import AckResponse
from exness.requests import (
    ClosePositionRequest,
    LimitOrderRequest,
    ModifyOrderRequest,
    OpenPositionRequest,
    SltpOrderRequest,
    StopOrderRequest,
)

Client: TypeAlias = ExnessClient | AsyncExnessClient
KEY = "trade-test-0001"


def opening() -> OpenPositionRequest:
    return OpenPositionRequest.model_validate(
        {"instrument": "EURUSD", "side": "buy", "volume": "0.01"}
    )


async def resolve(result: AckResponse | Awaitable[AckResponse]) -> AckResponse:
    return result if isinstance(result, AckResponse) else await result


async def close(client: Client) -> None:
    if isinstance(client, AsyncExnessClient):
        await client.aclose()
    else:
        client.close()


def dispatch(
    client: Client, name: str, body: object, query: object, key: str
) -> AckResponse | Awaitable[AckResponse]:
    match name:
        case "open":
            return client.open_position(
                OpenPositionRequest.model_validate(body), idempotency_key=key
            )
        case "limit":
            return client.place_pending_order(
                LimitOrderRequest.model_validate(body), idempotency_key=key
            )
        case "stop":
            return client.place_pending_order(
                StopOrderRequest.model_validate(body), idempotency_key=key
            )
        case "sltp":
            return client.place_pending_order(
                SltpOrderRequest.model_validate(body), idempotency_key=key
            )
        case "modify":
            return client.modify_order(
                "000456", ModifyOrderRequest.model_validate(body), idempotency_key=key
            )
        case "cancel":
            return client.cancel_pending_order("000456", idempotency_key=key)
        case _:
            return client.close_position(
                "000789", ClosePositionRequest.model_validate(query), idempotency_key=key
            )


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_all_submissions_on_real_tls(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    asynchronous: bool,
) -> None:
    cases = json.loads(fixture_bytes("write-cases"))

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if handler.command == "GET":
            reply = {"access_point": "https://" + handler.headers["Host"]}
            status = 200
        else:
            reply = {
                "status": "accepted",
                "operation_id": "000789",
                "client_request_id": handler.headers["EXN-IDEMPOTENCY-KEY"],
            }
            status = 202
        body = json.dumps(reply).encode()
        handler.send_response(status)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    with serve_tls(tls_contexts, respond) as server:
        client: Client = (
            async_network_client(credentials, server, tls_contexts)
            if asynchronous
            else network_client(credentials, server, tls_contexts)
        )
        try:
            for case in cases:
                ack = await resolve(
                    dispatch(client, case["name"], case["body"], case["query"], case["key"])
                )
                assert ack.status == "accepted" and ack.operation_id == "000789"
                assert ack.client_request_id == case["key"]
        finally:
            await close(client)
        assert server.requests.qsize() == len(cases) + 1
        discovery, _ = server.requests.get_nowait()
        assert discovery.method == "GET"
        for case in cases:
            wire, _ = server.requests.get_nowait()
            assert wire.method == case["method"]
            assert wire.url.path == "/v1/trading/accounts/123456/" + case["resource"]
            assert dict(wire.url.params) == case["query"]
            assert (json.loads(wire.content) if wire.content else None) == case["body"]
            if wire.content:
                assert wire.headers["Content-Type"] == "application/json"
            payload = decode(wire.headers["EXN-DATA"])
            Ed25519PublicKey.from_public_bytes(bytes.fromhex(PUBLIC)).verify(
                decode(wire.headers["EXN-SIGN"]), payload
            )
            metadata = json.loads(payload)
            assert metadata["method"] == wire.method
            assert metadata["path"] == wire.url.raw_path.decode("ascii")
            assert metadata["idempotency_key"] == wire.headers["EXN-IDEMPOTENCY-KEY"] == case["key"]
            assert decode(metadata["body_hash"]) == hashlib.sha256(wire.content).digest()


def test_shared_write_shapes_against_contract() -> None:
    contract = yaml.safe_load(
        (Path(__file__).parents[1] / "contracts/trading.public.yaml").read_text()
    )
    # OpenAPI 3.1 still uses legacy nullable here. Apply only its documented SL/TP
    # null semantics to the in-memory validation schema; never edit the snapshot.
    properties = contract["components"]["schemas"]["ModifyOrderRequest"]["properties"]
    for name in ("stop_loss_price", "take_profit_price"):
        original = properties[name]
        assert original["nullable"] is True
        properties[name] = {"anyOf": [original, {"type": "null"}]}
    registry = Registry().with_resource(
        "urn:exness:trading", Resource.from_contents(contract, default_specification=DRAFT202012)
    )
    for case in json.loads(fixture_bytes("write-cases")):
        if case["schema"]:
            Draft202012Validator(
                {"$ref": "urn:exness:trading#/components/schemas/" + case["schema"]},
                registry=registry,
            ).validate(case["body"])
        if case["name"].startswith("close"):
            endpoint = contract["paths"][
                "/v1/trading/accounts/{account_id}/positions/{position_id}"
            ]["delete"]
            schemas = {
                p["name"]: p["schema"] for p in endpoint["parameters"] if p.get("in") == "query"
            }
            for name, value in case["query"].items():
                Draft202012Validator(
                    json.loads(json.dumps(schemas[name]).replace('"#/', '"urn:exness:trading#/')),
                    registry=registry,
                ).validate(value)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "mode",
    [
        "400",
        "401",
        "403",
        "404",
        "409",
        "422",
        "429",
        "408",
        "500",
        "503",
        "307",
        "200",
        "bad_ack",
        "wrong_key",
        "oversize",
        "transport",
        "missing_id",
    ],
)
async def test_error_boundaries_and_single_attempt(
    credentials: Credentials, monkeypatch: pytest.MonkeyPatch, asynchronous: bool, mode: str
) -> None:
    writes = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"access_point": "https://account.example"})
        writes.append(request)
        if mode == "transport":
            raise httpx.ReadError("private untrusted details")
        if mode == "bad_ack":
            return httpx.Response(202, json={"status": "confirmed"})
        if mode == "wrong_key":
            return httpx.Response(
                202, json={"status": "accepted", "client_request_id": "other-key"}
            )
        if mode == "oversize":
            return httpx.Response(202, content=b"x" * 257)
        if mode == "missing_id":
            return httpx.Response(202, json={"status": "accepted"})
        return httpx.Response(
            int(mode),
            json={"code": 3000, "error_message": "untrusted"},
            headers={"Retry-After": "7", "Location": "https://other.example"},
        )

    monkeypatch.setattr(_common, "_MAX_RESPONSE_BYTES", 256)
    client: Client = (AsyncExnessClient if asynchronous else ExnessClient)(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        if mode == "missing_id":
            ack = await resolve(client.open_position(opening(), idempotency_key=KEY))
            assert ack.operation_id is None
        elif mode in ("400", "401", "403", "404", "409", "422", "429"):
            with pytest.raises(APIError) as error:
                await resolve(client.open_position(opening(), idempotency_key=KEY))
            assert error.value.status_code == int(mode) and error.value.retry_after == "7"
        else:
            with pytest.raises(UnknownOutcomeError) as unknown:
                await resolve(client.open_position(opening(), idempotency_key=KEY))
            assert unknown.value.method == "POST"
            assert unknown.value.path == "/v1/trading/accounts/123456/positions"
            assert unknown.value.account_id == "123456" and unknown.value.idempotency_key == KEY
            assert KEY not in str(unknown.value) and "private" not in str(unknown.value)
            assert isinstance(unknown.value.cause, (APIError, ProtocolError, TransportError))
        assert len(writes) == 1
    finally:
        await close(client)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_preflight_validation_and_discovery_failures(
    credentials: Credentials, asynchronous: bool
) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ConnectError("private")

    client: Client = (AsyncExnessClient if asynchronous else ExnessClient)(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(ValueError):
            await resolve(client.open_position(opening(), idempotency_key="bad"))
        with pytest.raises(ValueError):
            await resolve(client.cancel_pending_order("../123", idempotency_key=KEY))
        with pytest.raises(ValidationError):
            await resolve(
                client.open_position(
                    opening().model_copy(update={"volume": -1}), idempotency_key=KEY
                )
            )
        assert not calls
        with pytest.raises(TransportError):
            await resolve(client.open_position(opening(), idempotency_key=KEY))
        assert len(calls) == 1 and calls[0].method == "GET"
    finally:
        await close(client)
    with pytest.raises(RuntimeError, match="closed"):
        await resolve(client.open_position(opening(), idempotency_key=KEY))


@pytest.mark.parametrize(
    "model,body",
    [
        (
            LimitOrderRequest,
            {"instrument": "EURUSD", "side": "buy", "volume": "0.01", "price": "0"},
        ),
        (
            StopOrderRequest,
            {
                "instrument": "EURUSD",
                "side": "buy",
                "volume": "0.01",
                "price": "1",
                "type": "market",
            },
        ),
        (SltpOrderRequest, {"position_id": "1"}),
        (SltpOrderRequest, {"position_id": "18446744073709551616", "stop_loss_price": "1"}),
        (ModifyOrderRequest, {}),
        (ModifyOrderRequest, {"comment": "only comment"}),
        (ModifyOrderRequest, {"price": None}),
        (ModifyOrderRequest, {"stop_loss_price": "0"}),
        (ClosePositionRequest, {"volume": "0"}),
        (ClosePositionRequest, {"volume": "-0", "close_by_position_id": "2"}),
        (ClosePositionRequest, {"price": None}),
        (ClosePositionRequest, {"close_by_position_id": "../2"}),
    ],
)
def test_invalid_request_shapes(
    model: type[OpenPositionRequest]
    | type[LimitOrderRequest]
    | type[StopOrderRequest]
    | type[SltpOrderRequest]
    | type[ModifyOrderRequest]
    | type[ClosePositionRequest],
    body: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(body)


def test_patch_omission_null_and_value() -> None:
    assert json.loads(ModifyOrderRequest(stop_loss_price=None).to_body()) == {
        "stop_loss_price": None
    }
    assert json.loads(ModifyOrderRequest.model_validate({"price": "1.23"}).to_body()) == {
        "price": "1.23"
    }
    assert ClosePositionRequest().to_query() == {}


@pytest.mark.anyio
@pytest.mark.parametrize("during_body", [False, True])
async def test_cancellation_after_submission_remains_cancelled(
    credentials: Credentials, during_body: bool
) -> None:
    entered, cleaned = asyncio.Event(), asyncio.Event()
    writes = 0

    class BlockingBody(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            entered.set()
            await asyncio.Future[None]()
            yield b""

        async def aclose(self) -> None:
            cleaned.set()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal writes
        if request.method == "GET":
            return httpx.Response(200, json={"access_point": "https://account.example"})
        writes += 1
        if during_body:
            return httpx.Response(202, stream=BlockingBody())
        entered.set()
        await asyncio.Future[None]()
        raise AssertionError("cancelled")

    async with AsyncExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url="https://gateway.example",
        transport=httpx.MockTransport(handler),
    ) as client:
        task = asyncio.create_task(client.open_position(opening(), idempotency_key=KEY))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert writes == 1
        if during_body:
            assert cleaned.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("mode", ["timeout", "truncated", "redirect"])
async def test_lost_ack_over_tls(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    asynchronous: bool,
    mode: str,
) -> None:
    release = threading.Event()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        if handler.command == "GET":
            data = json.dumps({"access_point": "https://" + handler.headers["Host"]}).encode()
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(data)))
            handler.end_headers()
            handler.wfile.write(data)
        elif mode == "timeout":
            release.wait(2)
            handler.close_connection = True
        elif mode == "truncated":
            handler.send_response(202)
            handler.send_header("Content-Length", "100")
            handler.end_headers()
            handler.wfile.write(b"{")
            handler.close_connection = True
        else:
            handler.send_response(307)
            handler.send_header(
                "Location", "https://" + handler.headers["Host"] + "/redirect-target"
            )
            handler.send_header("Content-Length", "0")
            handler.end_headers()

    with serve_tls(tls_contexts, respond) as server:
        client: Client = (
            async_network_client(credentials, server, tls_contexts, timeout=0.15)
            if asynchronous
            else network_client(credentials, server, tls_contexts, timeout=0.15)
        )
        try:
            with pytest.raises(UnknownOutcomeError):
                await resolve(client.open_position(opening(), idempotency_key=KEY))
            assert server.requests.qsize() == 2
        finally:
            release.set()
            await close(client)
