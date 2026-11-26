"""Application recovery checkpoints shared with the Go example, plus policy boundaries."""

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from examples.recover_trading import LIMIT, RecoveryView, Suite, main, replay
from exness import OperationReference, ProtocolError
from exness.models import AckResponse
from exness.trading import TradingSnapshotEvent, TransactionEvent

ROOT = Path(__file__).parents[1]
SUITE = Suite.model_validate_json((ROOT / "examples/recovery_scenarios.json").read_bytes())


@pytest.mark.parametrize(
    "index", range(len(SUITE.scenarios)), ids=[s.name for s in SUITE.scenarios]
)
def test_shared_recovery_checkpoints(index: int) -> None:
    scenario = SUITE.scenarios[index]
    rows = replay(Suite(messages=SUITE.messages, scenarios=[scenario]))
    assert len(rows) == len(scenario.steps)


def test_all_wire_messages_match_pinned_contract() -> None:
    contract = yaml.safe_load((ROOT / "contracts/trading.public.yaml").read_text())
    resource = Resource.from_contents(contract, default_specification=DRAFT202012)
    registry = Registry().with_resource("urn:exness:trading", resource)
    for name, message in SUITE.messages.items():
        schema = (
            "WSTradingEvent"
            if "event_type" in message
            else "AckResponse"
            if message.get("status") == "accepted"
            else "OperationStatusResponse"
        )
        validator = Draft202012Validator(
            {"$ref": f"urn:exness:trading#/components/schemas/{schema}"},
            registry=registry,
            format_checker=FormatChecker(),
        )
        errors = list(validator.iter_errors(message))
        assert not errors, (name, [error.message for error in errors])


def test_runnable_example_output(capsys: pytest.CaptureFixture[str]) -> None:
    main()
    actual = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert actual == replay(SUITE)
    assert len(actual) == 49


def baseline() -> RecoveryView:
    view = RecoveryView(OperationReference(client_request_id="demo-close"))
    view.on_stream(TradingSnapshotEvent.model_validate_json(json.dumps(SUITE.messages["baseline"])))
    return view


def test_duplicate_window_is_bounded_and_snapshot_resets_it() -> None:
    view = baseline()
    for index in range(LIMIT):
        event = TransactionEvent.model_validate_json(
            json.dumps(
                {
                    "event_type": "transaction_event",
                    "status": "success",
                    "operation_id": str(index),
                    "payload": {},
                }
            )
        )
        view.on_stream(event)
    assert len(view.seen) == LIMIT
    view.on_stream(event)  # Exact duplicates still fit a full window.
    assert view.summary()["ready"] is True
    with pytest.raises(ProtocolError, match="window exhausted"):
        view.on_stream(TransactionEvent.model_validate_json(json.dumps(SUITE.messages["balance"])))
    assert view.summary()["ready"] is False and not view.seen and not view.positions
    view.on_stream(TradingSnapshotEvent.model_validate_json(json.dumps(SUITE.messages["baseline"])))
    view.on_stream(TransactionEvent.model_validate_json(json.dumps(SUITE.messages["balance"])))
    assert view.ready and len(view.deals) == 1


@pytest.mark.parametrize("invalid", ["duplicate", "closed", "overflow", "oversized"])
def test_bad_snapshot_invalidates_instead_of_merging(invalid: str) -> None:
    data = json.loads(json.dumps(SUITE.messages["baseline"]))
    positions = data["payload"]["positions"]
    if invalid == "duplicate":
        positions.append({**positions[0], "position_id": "0002001"})
    elif invalid == "closed":
        positions[0]["state"] = "closed"
    elif invalid == "oversized":
        data["payload"]["positions"] = [
            {**positions[0], "position_id": str(index)} for index in range(LIMIT + 1)
        ]
    view = baseline()
    snapshot = TradingSnapshotEvent.model_validate_json(json.dumps(data))
    if invalid == "overflow":
        # The SDK now rejects overflow during decoding; bypass it to exercise
        # the example's independent defense for manually constructed objects.
        position = snapshot.payload.positions[0].model_copy(
            update={"position_id": "18446744073709551616"}
        )
        snapshot = snapshot.model_copy(
            update={"payload": snapshot.payload.model_copy(update={"positions": [position]})}
        )
    with pytest.raises(ProtocolError):
        view.on_stream(snapshot)
    assert not view.ready and not view.positions and view.account is None


def test_entity_limit_and_failure_payload_invalidate_cache() -> None:
    for failure in (False, True):
        view = baseline()
        data = json.loads(json.dumps(SUITE.messages["balance"]))
        if failure:
            data["status"] = "failed"
        else:
            deal = data["payload"]["deals"][0]
            data["payload"]["deals"] = [{**deal, "deal_id": str(i)} for i in range(LIMIT + 1)]
        with pytest.raises(ProtocolError):
            view.on_stream(TransactionEvent.model_validate_json(json.dumps(data)))
        assert not view.ready and not view.deals


def test_conflicting_ack_cannot_replace_known_identity() -> None:
    view = baseline()
    view.acknowledge(
        AckResponse(status="accepted", operation_id="790", client_request_id="demo-close")
    )
    with pytest.raises(ProtocolError):
        view.acknowledge(
            AckResponse(status="accepted", operation_id="791", client_request_id="demo-close")
        )
    assert view.reference.operation_id == "790" and view.outcome == "accepted"
    assert not view.ready
