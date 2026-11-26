import base64
import copy
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from tools.transport_transcript import EXPECTED, compare_transcripts, verify_request


def test_transcripts_reject_agreement_on_wrong_scenario() -> None:
    wrong = copy.deepcopy(EXPECTED)
    wrong["accounts"] = ["123456"]
    with pytest.raises(ValueError, match="Python transcript mismatch: accounts"):
        compare_transcripts(wrong, wrong)


def test_transcripts_reject_mismatch_without_reporting_values() -> None:
    wrong = copy.deepcopy(EXPECTED)
    wrong["subscription"]["instruments"] = ["secret-marker"]
    with pytest.raises(ValueError, match="Go transcript mismatch: subscription") as error:
        compare_transcripts(EXPECTED, wrong)
    assert "secret-marker" not in str(error.value)
    compare_transcripts(EXPECTED, EXPECTED)


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
