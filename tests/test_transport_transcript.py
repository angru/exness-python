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
