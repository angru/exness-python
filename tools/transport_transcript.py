"""Validation primitives for the cross-SDK transport scenarios.

Only normalized, non-secret observations may be passed to compare_transcripts.
"""

import base64
import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PUBLIC_KEY = bytes.fromhex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
ACCOUNT = "123456"
DISCOVERY_PATH = f"/v1/trading/access-point?account_id={ACCOUNT}"
ACCOUNT_PATH = f"/v1/configuration/accounts/{ACCOUNT}/account"
TICKS_PATH = f"/v1/server-events/accounts/{ACCOUNT}/ws/ticks"
TRADING_PATH = f"/v1/server-events/accounts/{ACCOUNT}/ws/events"
POSITIONS_PATH = f"/v1/trading/accounts/{ACCOUNT}/positions"
WRITE_KEY = "transport-lost-ack-0001"
OPEN_ORDER = {"instrument": "EURUSD", "side": "buy", "volume": "0.01"}


class Scenario(StrEnum):
    REST_TICKS = "rest_ticks"
    TRADING_RECOVERY = "trading_recovery"
    TRADING_MISSING_SNAPSHOT = "trading_missing_snapshot"
    LOST_ACK = "lost_ack"


EXPECTED_TICKS = {
    "paths": [DISCOVERY_PATH, ACCOUNT_PATH, ACCOUNT_PATH, TICKS_PATH],
    "subscriptions": [{"event": "ticks", "instruments": ["EURUSD"]}],
    "accounts": ["123456", "123456"],
    "tick": {"instrument": "EURUSD", "bid": 1.08321, "ask": 1.08331},
    "error": "ProtocolError",
}
TRADING_START = [
    {"kind": "snapshot", "balance": "12345678901234567890.123456789", "positions": 1},
    {
        "kind": "transaction",
        "operation_id": "790",
        "status": "success",
        "positions": 1,
        "deals": 0,
        "account_state": None,
    },
    {"kind": "gap", "reconnect_attempt": 1, "close_code": 1012},
]
TRADING_WIRE = {
    "paths": [DISCOVERY_PATH, TRADING_PATH, TRADING_PATH],
    "subscriptions": [{"event": "transactions"}, {"event": "transactions"}],
}
EXPECTED: dict[Scenario, dict[str, object]] = {
    Scenario.REST_TICKS: EXPECTED_TICKS,
    Scenario.TRADING_RECOVERY: {
        **TRADING_WIRE,
        "events": [
            *TRADING_START,
            {"kind": "snapshot", "balance": "12345678901234567790.113456789", "positions": 1},
            {
                "kind": "transaction",
                "operation_id": None,
                "status": "success",
                "positions": 0,
                "deals": 1,
                "account_state": {"balance": "12345678901234567890.123456789", "leverage": "100"},
            },
        ],
        "outcome": "normal_close",
    },
    Scenario.TRADING_MISSING_SNAPSHOT: {
        **TRADING_WIRE,
        "events": TRADING_START,
        "outcome": "ProtocolError",
    },
    Scenario.LOST_ACK: {
        "paths": [DISCOVERY_PATH, POSITIONS_PATH],
        "post_count": 1,
        "receipt": {
            "method": "POST",
            "path": POSITIONS_PATH,
            "order": OPEN_ORDER,
            "signature_verified": True,
            "idempotency_key": WRITE_KEY,
            "acknowledgement_sent": False,
        },
        "outcome": {
            "error": "UnknownOutcomeError",
            "method": "POST",
            "path": POSITIONS_PATH,
            "account_id": ACCOUNT,
            "idempotency_key": WRITE_KEY,
        },
    },
}


def _decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def verify_request(
    method: str,
    target: str,
    headers: Mapping[str, str],
    *,
    body: bytes = b"",
    idempotency_key: str = "",
) -> None:
    """Bind the synthetic signature to the received method, target, body and key.

    Errors deliberately contain no header values or decoded credential material.
    Header names must be supplied in lowercase.
    """
    try:
        payload = _decode(headers["exn-data"])
        signature = _decode(headers["exn-sign"])
        Ed25519PublicKey.from_public_bytes(PUBLIC_KEY).verify(signature, payload)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError
        body_hash = base64.urlsafe_b64encode(hashlib.sha256(body).digest()).rstrip(b"=")
        if (
            data.get("method") != method
            or data.get("path") != target
            or data.get("body_hash") != body_hash.decode("ascii")
            or data.get("idempotency_key") != idempotency_key
            or headers["exn-idempotency-key"] != idempotency_key
        ):
            raise ValueError
    except (KeyError, ValueError, TypeError, InvalidSignature):
        raise ValueError("Invalid signed request") from None


def compare_transcripts(
    scenario: Scenario, python: dict[str, object], go: dict[str, object]
) -> None:
    """Require each peer to satisfy the expected scenario as well as each other."""
    expected = EXPECTED[scenario]
    for label, transcript in (("Python", python), ("Go", go)):
        for field, value in expected.items():
            if json.dumps(transcript.get(field), sort_keys=True) != json.dumps(
                value, sort_keys=True
            ):
                raise ValueError(f"{scenario}: {label} transcript mismatch: {field}")
        if set(transcript) != set(expected):
            raise ValueError(f"{scenario}: {label} transcript contains unexpected fields")
    if python != go:
        raise ValueError(f"{scenario}: cross-SDK transcript mismatch")
