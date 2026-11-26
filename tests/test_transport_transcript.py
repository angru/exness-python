import base64
import copy
import json
from pathlib import Path
from typing import cast

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from tools.transport_transcript import EXPECTED, Scenario, compare_transcripts, verify_request


def test_transcripts_reject_agreement_on_wrong_scenario() -> None:
    wrong = copy.deepcopy(EXPECTED[Scenario.REST_TICKS])
    wrong["accounts"] = ["123456"]
    with pytest.raises(ValueError, match="Python transcript mismatch: accounts"):
        compare_transcripts(Scenario.REST_TICKS, wrong, wrong)


def test_transcripts_reject_mismatch_without_reporting_values() -> None:
    expected = EXPECTED[Scenario.REST_TICKS]
    wrong = {**expected, "subscriptions": [{"event": "ticks", "instruments": ["secret-marker"]}]}
    with pytest.raises(ValueError, match="Go transcript mismatch: subscriptions") as error:
        compare_transcripts(Scenario.REST_TICKS, expected, wrong)
    assert "secret-marker" not in str(error.value)
    compare_transcripts(Scenario.REST_TICKS, expected, expected)


def test_recovery_rejects_stale_snapshot_even_when_peers_agree() -> None:
    scenario = Scenario.TRADING_RECOVERY
    wrong = copy.deepcopy(EXPECTED[scenario])
    events = cast(list[dict[str, object]], wrong["events"])
    events[3] = events[0]
    with pytest.raises(ValueError, match="trading_recovery: Python transcript mismatch: events"):
        compare_transcripts(scenario, wrong, wrong)


@pytest.mark.parametrize("field", ["leaked_transaction", "extra_gap", "wrong_outcome"])
def test_missing_snapshot_rejects_extra_events_and_normal_close(field: str) -> None:
    scenario = Scenario.TRADING_MISSING_SNAPSHOT
    expected = EXPECTED[scenario]
    wrong = copy.deepcopy(expected)
    events = cast(list[dict[str, object]], wrong["events"])
    recovery_events = cast(list[dict[str, object]], EXPECTED[Scenario.TRADING_RECOVERY]["events"])
    if field == "leaked_transaction":
        events.append(recovery_events[-1])
    elif field == "extra_gap":
        events.append(events[-1])
    else:
        wrong["outcome"] = "normal_close"
    mismatch = "outcome" if field == "wrong_outcome" else "events"
    with pytest.raises(ValueError, match=f"{scenario}: Go transcript mismatch: {mismatch}"):
        compare_transcripts(scenario, expected, wrong)


@pytest.mark.parametrize("field", ["error", "account_id", "method", "path", "idempotency_key"])
def test_lost_ack_rejects_wrong_identity_without_reporting_values(field: str) -> None:
    scenario = Scenario.LOST_ACK
    expected = EXPECTED[scenario]
    outcome = cast(dict[str, str], expected["outcome"])
    wrong = {**expected, "outcome": {**outcome, field: "untrusted-value"}}

    with pytest.raises(ValueError, match="lost_ack: Go transcript mismatch: outcome") as error:
        compare_transcripts(scenario, expected, wrong)
    assert "untrusted-value" not in str(error.value)
    with pytest.raises(ValueError, match="lost_ack: Python transcript mismatch: outcome"):
        compare_transcripts(scenario, wrong, wrong)


@pytest.mark.parametrize(("field", "value"), [("post_count", 2), ("receipt", None)])
def test_lost_ack_requires_single_verified_receipt(field: str, value: object) -> None:
    expected = EXPECTED[Scenario.LOST_ACK]
    wrong = {**expected, field: value}
    with pytest.raises(ValueError, match=f"lost_ack: Python transcript mismatch: {field}"):
        compare_transcripts(Scenario.LOST_ACK, wrong, wrong)


@pytest.mark.parametrize("failure", ["foreign_selected", "snapshot_selected", "missing_match"])
def test_lost_ack_recovery_requires_a_matching_event_before_selection(failure: str) -> None:
    scenario = Scenario.LOST_ACK_RECOVERY
    wrong = copy.deepcopy(EXPECTED[scenario])
    events = cast(list[dict[str, object]], wrong["events"])
    if failure == "foreign_selected":
        events[1]["decision"] = "selected"
        events[1]["selected"] = {"operation_id": "790", "status": "success"}
    elif failure == "snapshot_selected":
        events[0]["selected"] = events[-1]["selected"]
    else:
        events.pop()
    with pytest.raises(ValueError, match=f"{scenario}: Python transcript mismatch: events"):
        compare_transcripts(scenario, wrong, wrong)


def test_request_signature_is_bound_to_received_target() -> None:
    vector = json.loads((Path(__file__).parent / "fixtures/signing-vector.json").read_text())
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(vector["private_key_hex"])[:32])
    payload = vector["payload_utf8"].encode()

    def encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    headers = {
        "exn-data": encode(payload),
        "exn-sign": encode(key.sign(payload)),
        "exn-idempotency-key": "",
    }
    verify_request("GET", vector["path"], headers)
    with pytest.raises(ValueError, match="Invalid signed request"):
        verify_request("GET", vector["path"] + "x", headers)
    headers["exn-sign"] = encode(b"x" * 64)
    with pytest.raises(ValueError, match="Invalid signed request"):
        verify_request("GET", vector["path"], headers)
