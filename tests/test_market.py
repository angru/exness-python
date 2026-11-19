import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import yaml
from conftest import fixture_bytes
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
from test_client import START, client_for, discovery, verify_request

from exness import APIError, Credentials, ProtocolError
from exness.models import CandleHistory, CandlePriceType, CandleTimeframe, InstrumentConditions


@pytest.mark.parametrize("price_type", ["bid", "ask", "both"])
def test_candle_fixture_against_request_selected_contract_branch(price_type: str) -> None:
    contract = yaml.safe_load(
        (Path(__file__).parents[1] / "contracts/trading.public.yaml").read_text()
    )
    schema_uri = "urn:exness:trading#/components/schemas/CandleHistoryResponse"
    resource = Resource.from_contents(contract, default_specification=DRAFT202012)
    registry = Registry().with_resource("urn:exness:trading", resource)
    original = Draft202012Validator({"$ref": schema_uri}, registry=registry)
    data = json.loads(fixture_bytes(f"candles_{price_type}"))
    # Both upstream alternatives have only optional fields and allow extras.
    # Record the actual defect, then select the branch using the request's price_type.
    errors = list(original.iter_errors(data))
    assert len(errors) == 1 and errors[0].validator == "oneOf"
    branch = "CandleBidAsk" if price_type == "both" else "Candle"
    contract["components"]["schemas"]["CandleHistoryResponse"]["properties"]["candles"]["items"] = {
        "$ref": f"#/components/schemas/{branch}"
    }
    selected = Resource.from_contents(contract, default_specification=DRAFT202012)
    Draft202012Validator(
        {"$ref": schema_uri},
        registry=Registry().with_resource("urn:exness:trading", selected),
        format_checker=FormatChecker(),
    ).validate(data)
    CandleHistory.model_validate_json(json.dumps(data))


@pytest.mark.parametrize("price_type", ["bid", "ask", "both"])
@pytest.mark.parametrize("use_range", [False, True])
def test_signed_candle_history(
    credentials: Credentials,
    price_type: CandlePriceType,
    use_range: bool,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        requests.append(request)
        if request.url.host == "gateway.example":
            return discovery()
        assert request.url.path == "/v1/market-data/accounts/123456/candles"
        expected = {
            "instrument": "EURUSD",
            "timeframe": "H1",
            "price_type": price_type,
            "from": START.isoformat(),
        }
        expected.update(
            {"to": (START + timedelta(hours=1)).isoformat()} if use_range else {"count": "-10"}
        )
        assert dict(request.url.params) == expected
        return httpx.Response(200, content=fixture_bytes(f"candles_{price_type}"))

    with client_for(credentials, handler) as client:
        result = client.get_candle_history(
            instrument="EURUSD",
            timeframe="H1",
            price_type=price_type,
            from_time=START.astimezone(timezone(timedelta(hours=2))),
            to_time=START + timedelta(hours=1) if use_range else None,
            count=None if use_range else -10,
        )
    assert len(requests) == 2  # Discovery + the requested endpoint; no hidden limits query.
    candle = result.candles[0]
    assert candle.volume == 124.0
    if price_type == "both":
        assert candle.open is None and candle.bid is not None and candle.ask is not None
        assert candle.bid.open == 1.08321 and candle.ask.open == 1.08331
    else:
        assert candle.bid is None and candle.ask is None
        assert candle.open == (1.08321 if price_type == "bid" else 1.08331)


def test_conditions_and_precision(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        if request.url.host == "gateway.example":
            return discovery()
        assert request.url.path == "/v1/configuration/accounts/123456/instruments/EURUSD/conditions"
        assert not request.url.query
        return httpx.Response(200, content=fixture_bytes("conditions"))

    with client_for(credentials, handler) as client:
        result = client.get_instrument_conditions("EURUSD")
    assert result.point_digits == 5
    assert result.volume_step == Decimal("0.01") and result.swap_long == Decimal("-6.35")
    assert result.commission_settings is not None
    assert result.commission_settings.commission_value is not None
    assert result.commission_settings.commission_value.amount == Decimal("3.50")
    assert (
        result.trade_sessions is not None
        and result.trade_sessions[0].week_start_time == "Mon, 00:00:00"
    )
    minimal = InstrumentConditions.model_validate_json('{"instrument":"EURUSD"}')
    assert minimal.point_digits is None and minimal.volume_min is None
    nullable = InstrumentConditions.model_validate_json('{"instrument":"EURUSD","volume_min":null}')
    assert nullable.volume_min is None


@pytest.mark.parametrize("state", ["pending", "confirmed", "rejected", "failed"])
def test_operation_states_are_data_not_http_failures(credentials: Credentials, state: str) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        verify_request(request)
        if request.url.host == "gateway.example":
            return discovery()
        assert request.url.path == "/v1/trading/accounts/123456/operations/00789"
        return httpx.Response(200, content=fixture_bytes(f"operation_{state}"))

    with client_for(credentials, handler) as client:
        result = client.get_operation_status("00789")
    assert calls == 2 and result.status == state
    assert result.is_terminal is (state != "pending")
    assert (result.error_code is not None) is (state in ("failed", "rejected"))


def test_invalid_parameters_before_discovery(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid parameters must not cause network requests")

    with client_for(credentials, handler) as client:
        for name in ("", ".", "..", "EUR/USD", "EUR%2FUSD", "A?B", "a" * 12):
            with pytest.raises(ValueError):
                client.get_instrument_conditions(name)
        for operation_id in ("", "-1", "1/2", "\uff11\uff12", str(2**64), "0" * 21):
            with pytest.raises(ValueError):
                client.get_operation_status(operation_id)
        with pytest.raises(ValueError, match="exactly one"):
            client.get_candle_history(instrument="EURUSD", timeframe="H1", from_time=START)
        with pytest.raises(ValueError, match="exactly one"):
            client.get_candle_history(
                instrument="EURUSD", timeframe="H1", from_time=START, count=1, to_time=START
            )
        with pytest.raises(ValueError, match="later"):
            client.get_candle_history(
                instrument="EURUSD", timeframe="H1", from_time=START, to_time=START
            )
        with pytest.raises(ValueError, match="timezone"):
            client.get_candle_history(
                instrument="EURUSD", timeframe="H1", from_time=datetime(2026, 1, 1), count=1
            )
        with pytest.raises(ValueError, match="timezone"):
            client.get_candle_history(
                instrument="EURUSD", timeframe="H1", from_time=START, to_time=datetime(2026, 1, 2)
            )
        with pytest.raises(ValueError, match="instrument"):
            client.get_candle_history(
                instrument="EUR/USD", timeframe="H1", from_time=START, count=1
            )


@pytest.mark.parametrize("count", [0, True, 1.5])
def test_invalid_count(credentials: Credentials, count: int) -> None:
    with (
        client_for(credentials, lambda request: pytest.fail("network called")) as client,
        pytest.raises(ValueError, match="count"),
    ):
        client.get_candle_history(instrument="EURUSD", timeframe="M1", from_time=START, count=count)


@pytest.mark.parametrize("timeframe", ["M2", "h1", ""])
def test_invalid_timeframe(credentials: Credentials, timeframe: CandleTimeframe) -> None:
    with (
        client_for(credentials, lambda request: pytest.fail("network called")) as client,
        pytest.raises(ValueError, match="timeframe"),
    ):
        client.get_candle_history(
            instrument="EURUSD", timeframe=timeframe, from_time=START, count=1
        )


@pytest.mark.parametrize("price_type", ["last", "BID", ""])
def test_invalid_price_type(credentials: Credentials, price_type: CandlePriceType) -> None:
    with (
        client_for(credentials, lambda request: pytest.fail("network called")) as client,
        pytest.raises(ValueError, match="price_type"),
    ):
        client.get_candle_history(
            instrument="EURUSD", timeframe="M1", from_time=START, count=1, price_type=price_type
        )


@pytest.mark.parametrize(
    "body",
    [
        '{"operation_id":"99","status":"pending","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}',
        '{"operation_id":"789","status":"accepted"}',
        "{}",
    ],
)
def test_bad_operation_response(credentials: Credentials, body: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return (
            discovery() if request.url.host == "gateway.example" else httpx.Response(200, text=body)
        )

    with client_for(credentials, handler) as client, pytest.raises(ProtocolError):
        client.get_operation_status("789")


def test_conditions_mismatch_and_http_error(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "gateway.example":
            return discovery()
        if request.url.path.endswith("/conditions"):
            return httpx.Response(200, json={"instrument": "GBPUSD"})
        return httpx.Response(
            404, json={"code": 3014, "error_message": "REQUEST_OPERATION_NOT_FOUND"}
        )

    with client_for(credentials, handler) as client:
        with pytest.raises(ProtocolError, match="different instrument"):
            client.get_instrument_conditions("EURUSD")
        with pytest.raises(APIError) as caught:
            client.get_operation_status("789")
        assert caught.value.code == 3014


@pytest.mark.parametrize("value", ['"1.2"', "true", "NaN", "Infinity", "1e400"])
def test_candle_prices_are_finite_json_numbers(value: str) -> None:
    with pytest.raises(ValidationError):
        CandleHistory.model_validate_json('{"candles":[{"open":' + value + "}]}")


def test_empty_and_partial_candle_responses() -> None:
    assert CandleHistory.model_validate_json("{}").candles == []
    result = CandleHistory.model_validate_json('{"candles":[{"bid":{"open":0},"volume":0}]}')
    assert result.candles[0].time is None
    assert result.candles[0].bid is not None and result.candles[0].bid.open == 0
