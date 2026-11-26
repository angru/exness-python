"""Bounded decoder fuzzing and independent snapshot-state transition checks."""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import fixture_bytes
from hypothesis import example, given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from exness import APIError, ProtocolError, WebSocketAPIError
from exness._common import _response
from exness.events import HmrUpdate, _account_decoder, _hmr_decoder, _instrument_decoder
from exness.models import AckResponse, APIModel, OperationStatus, TradingSnapshot
from exness.ticks import _tick
from exness.trading import TransactionEvent, _trading_decoder

Decoder = Callable[[str | bytes], BaseModel]
generated = settings(max_examples=150, derandomize=True, database=None, deadline=None)
RAW_SEEDS: list[str] = json.loads(fixture_bytes("decoder-seeds"))
SEQUENCES: list[dict[str, str | list[int]]] = json.loads(fixture_bytes("decoder-sequences"))
FIXTURE_SEEDS = [
    fixture_bytes(name)
    for name in (
        "ack_accepted",
        "operation_pending",
        "snapshot",
        "trading_snapshot",
        "transaction_rejected",
        "hmr_empty_snapshot",
        "hmr_update",
        "tick",
        "account_state_event",
        "instrument_new",
    )
]
json_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**65), max_value=2**65)
    | st.text(max_size=32),
    lambda child: (
        st.lists(child, max_size=4) | st.dictionaries(st.text(max_size=24), child, max_size=4)
    ),
    max_leaves=20,
)


@st.composite
def mutated_fixture(draw: st.DrawFn) -> bytes:
    value: dict[str, Any] = json.loads(draw(st.sampled_from(FIXTURE_SEEDS)))
    key = draw(st.sampled_from(list(value)))
    if draw(st.booleans()):
        del value[key]
    else:
        value[key] = draw(json_values)
    return json.dumps(value).encode()


messages = st.one_of(
    st.binary(max_size=4096),
    st.sampled_from([value.encode() for value in RAW_SEEDS] + FIXTURE_SEEDS),
    json_values.map(lambda value: json.dumps(value).encode()),
    mutated_fixture(),
).filter(lambda data: len(data) <= 8192)


def websocket_decoder(kind: str, baseline: bool = False) -> Decoder:
    if kind == "trading":
        decoder: Decoder = _trading_decoder("test")
        if baseline:
            decoder(fixture_bytes("trading_snapshot"))
        return decoder
    if kind == "hmr":
        decoder = _hmr_decoder("test")
        if baseline:
            decoder(fixture_bytes("hmr_empty_snapshot"))
        return decoder
    if kind == "account":
        return _account_decoder("test")
    if kind == "instrument":
        return _instrument_decoder(("EURUSD",), "test")
    return lambda message: _tick(message, ("EURUSD",), "test")


@pytest.mark.parametrize("kind", ["trading", "hmr", "account", "instrument", "tick"])
@pytest.mark.parametrize("data", [value.encode() for value in RAW_SEEDS], ids=range(len(RAW_SEEDS)))
def test_malformed_decoder_seeds(kind: str, data: bytes) -> None:
    # Unexpected runtime exceptions are test failures, not accepted protocol outcomes.
    with pytest.raises((ProtocolError, WebSocketAPIError)):
        websocket_decoder(kind)(data)


@pytest.mark.parametrize("kind", ["trading", "hmr", "account", "instrument", "tick"])
@generated
@given(data=messages, baseline=st.booleans())
def test_generated_websocket_messages(kind: str, data: bytes, baseline: bool) -> None:
    try:
        result = websocket_decoder(kind, baseline)(data)
    except (ProtocolError, WebSocketAPIError):
        return
    if isinstance(result, (TransactionEvent, HmrUpdate)):
        assert baseline  # Arbitrary data cannot bypass the fresh-snapshot prerequisite.


@pytest.mark.parametrize("model", [AckResponse, OperationStatus, TradingSnapshot])
@generated
@given(data=messages, status=st.sampled_from([200, 400, 429, 500]))
def test_generated_rest_responses(model: type[APIModel], data: bytes, status: int) -> None:
    try:
        result = _response(httpx.Response(status), model, bytearray(data))
    except APIError as error:
        assert status != 200 and error.status_code == status
    except ProtocolError:
        assert status == 200
    else:
        assert status == 200 and isinstance(result, model)
        # Omission is significant: do not serialize missing optional fields as null.
        model.model_validate_json(result.model_dump_json(exclude_unset=True))


def check_sequence(kind: str, actions: list[int]) -> None:
    decoder = websocket_decoder(kind)
    ready = False
    snapshot = fixture_bytes("trading_snapshot" if kind == "trading" else "hmr_snapshot")
    empty = (
        b'{"event_type":"trading_state_snapshot","payload":{"account_state":'
        b'{"balance":"0","equity":"0","used_margin":"0"}}}'
        if kind == "trading"
        else fixture_bytes("hmr_empty_snapshot")
    )
    delta = fixture_bytes("transaction_rejected" if kind == "trading" else "hmr_update")
    malformed = (
        b'{"event_type":"trading_state_snapshot","payload":{}}'
        if kind == "trading"
        else b'{"event_type":"hmr_snapshot","periods":[{}]}'
    )
    for action in actions:
        if action == 3:
            decoder, ready = websocket_decoder(kind), False
        elif action in {0, 4}:
            decoder(snapshot if action == 0 else empty)
            ready = True
        elif action == 2:
            with pytest.raises(ProtocolError):
                decoder(malformed)
        elif ready:
            decoder(delta)
        else:
            with pytest.raises(ProtocolError):
                decoder(delta)


@pytest.mark.parametrize("kind", ["trading", "hmr"])
@pytest.mark.parametrize("sequence", SEQUENCES, ids=lambda sequence: sequence["name"])
def test_shared_decoder_sequences(kind: str, sequence: dict[str, str | list[int]]) -> None:
    actions = sequence["actions"]
    assert isinstance(actions, list)
    check_sequence(kind, actions)


@pytest.mark.parametrize("kind", ["trading", "hmr"])
@generated
@given(actions=st.lists(st.integers(min_value=0, max_value=4), min_size=1, max_size=32))
@example(actions=[2, 1, 3, 1, 0, 1, 4, 1])
def test_generated_decoder_sequences(kind: str, actions: list[int]) -> None:
    check_sequence(kind, actions)
