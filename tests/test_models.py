import json
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from conftest import fixture_bytes
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from exness.models import (
    AccountDetails,
    AccountState,
    APIModel,
    DealHistoryItem,
    HistoryPage,
    InstrumentConditions,
    Instruments,
    Limits,
    OperationStatus,
    OrderHistoryItem,
    TradingSnapshot,
)


@pytest.mark.parametrize(
    ("name", "schema", "model"),
    [
        ("account", "AccountDetailsResponse", AccountDetails),
        ("instruments", "InstrumentNameListResponse", Instruments),
        ("snapshot", "TradingStateSnapshot", TradingSnapshot),
        ("orders", "OrdersHistoryResponse", HistoryPage[OrderHistoryItem]),
        ("deals", "DealsHistoryResponse", HistoryPage[DealHistoryItem]),
        ("limits", "LimitsResponse", Limits),
        ("conditions", "InstrumentConditionResponse", InstrumentConditions),
        ("operation_pending", "OperationStatusResponse", OperationStatus),
        ("operation_confirmed", "OperationStatusResponse", OperationStatus),
        ("operation_rejected", "OperationStatusResponse", OperationStatus),
        ("operation_failed", "OperationStatusResponse", OperationStatus),
    ],
)
def test_fixtures_match_pinned_openapi_and_sdk(
    name: str, schema: str, model: type[APIModel]
) -> None:
    contract_path = Path(__file__).parents[1] / "contracts/trading.public.yaml"
    contract = yaml.safe_load(contract_path.read_text())
    resource = Resource.from_contents(contract, default_specification=DRAFT202012)
    registry = Registry().with_resource("urn:exness:trading", resource)
    validator = Draft202012Validator(
        {"$ref": f"urn:exness:trading#/components/schemas/{schema}"},
        registry=registry,
        format_checker=FormatChecker(),
    )
    data = fixture_bytes(name)
    validator.validate(json.loads(data))
    model.model_validate_json(data)


def test_precise_money_and_optional_fields() -> None:
    snapshot = TradingSnapshot.model_validate_json(fixture_bytes("snapshot"))
    assert snapshot.account_state.balance == Decimal("12345678901234567890.123456789")
    assert snapshot.positions[0].volume == Decimal("0.01")
    assert '"12345678901234567890.123456789"' in snapshot.model_dump_json()
    history = HistoryPage[DealHistoryItem].model_validate_json(fixture_bytes("deals"))
    assert history.items[0].deal.direction is None
    assert history.items[0].event_time.utcoffset() is not None
    assert history.items[0].deal.profit is not None
    assert history.items[0].deal.profit.amount == Decimal("100.01")


@pytest.mark.parametrize("balance", [1.2, 100, "NaN", "Infinity", "1e3", "1,000", ""])
def test_invalid_wire_money_is_rejected(balance: object) -> None:
    with pytest.raises(ValidationError):
        AccountState.model_validate_json(
            json.dumps({"balance": balance, "equity": "1.00", "used_margin": "0"})
        )


def test_python_decimal_construction() -> None:
    state = AccountState(balance=Decimal("1.00"), equity=Decimal("1"), used_margin=Decimal("0"))
    assert state.balance == Decimal("1")


def test_unknown_fields_are_preserved_and_unknown_enums_fail() -> None:
    account = json.loads(fixture_bytes("account"))
    account["new_field"] = {"example": 1}
    parsed = AccountDetails.model_validate_json(json.dumps(account))
    assert parsed.model_extra == {"new_field": {"example": 1}}
    account["settings"]["trade_mode"] = "unrecognized-mode"
    with pytest.raises(ValidationError):
        AccountDetails.model_validate_json(json.dumps(account))


def test_naive_response_date_fails() -> None:
    with pytest.raises(ValidationError):
        AccountDetails.model_validate_json(
            '{"account_id":"123456","create_date":"2026-01-01T00:00:00"}'
        )
