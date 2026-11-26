"""Shared positive/negative response corpus; no live API access."""

import json
from typing import Any

import httpx
import pytest
from conftest import fixture_bytes
from pydantic import BaseModel, ValidationError
from test_async_client import async_client_for
from test_client import client_for, discovery

from exness import Credentials, ProtocolError
from exness.events import _hmr_decoder
from exness.models import AckResponse, OperationStatus, TradingSnapshot
from exness.trading import _trading_decoder

CASES: list[dict[str, Any]] = json.loads(fixture_bytes("response-validation"))


def response_case(case: dict[str, Any]) -> bytes:
    value = json.loads(fixture_bytes(case["fixture"]))
    if case["action"] != "none":
        parts = case["path"].split(".")
        parent = value
        for part in parts[:-1]:
            parent = parent[int(part)] if isinstance(parent, list) else parent[part]
        key = int(parts[-1]) if isinstance(parent, list) else parts[-1]
        if case["action"] == "delete":
            parent.pop(key, None)
        else:
            parent[key] = case["value"]
    return json.dumps(value).encode()


def decode_case(kind: str, data: bytes) -> BaseModel:
    if kind == "transaction":
        decode = _trading_decoder("test")
        decode(fixture_bytes("trading_snapshot"))
        return decode(data)
    if kind.startswith("hmr_"):
        decode_hmr = _hmr_decoder("test")
        decode_hmr(fixture_bytes("hmr_empty_snapshot"))
        return decode_hmr(data)
    models: dict[str, type[BaseModel]] = {
        "ack": AckResponse,
        "status": OperationStatus,
        "snapshot": TradingSnapshot,
    }
    return models[kind].model_validate_json(data)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_response_validation(case: dict[str, Any]) -> None:
    data = response_case(case)
    if case["valid"]:
        decode_case(case["kind"], data)
    else:
        with pytest.raises((ProtocolError, ValidationError)):
            decode_case(case["kind"], data)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_rest_nested_overflow_is_protocol_error(
    credentials: Credentials, asynchronous: bool
) -> None:
    calls = 0
    case = next(case for case in CASES if case["name"] == "id_3_overflow")

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.host == "gateway.example":
            return discovery()
        return httpx.Response(200, content=response_case(case))

    with pytest.raises(ProtocolError):
        if asynchronous:
            async with async_client_for(credentials, handler) as client:
                await client.get_snapshot()
        else:
            with client_for(credentials, handler) as sync:
                sync.get_snapshot()
    assert calls == 2
