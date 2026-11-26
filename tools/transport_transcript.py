"""Validation primitives for the proposed cross-SDK transport scenario.

Only normalized, non-secret observations may be passed to compare_transcripts.
"""

import base64
import json
from collections.abc import Mapping
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PUBLIC_KEY = bytes.fromhex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
EMPTY_HASH = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
EXPECTED: dict[str, Any] = {
    "paths": [
        "/v1/trading/access-point?account_id=123456",
        "/v1/configuration/accounts/123456/account",
        "/v1/configuration/accounts/123456/account",
        "/v1/server-events/accounts/123456/ws/ticks",
    ],
    "subscription": {"event": "ticks", "instruments": ["EURUSD"]},
    "accounts": ["123456", "123456"],
    "tick": {"instrument": "EURUSD", "bid": 1.08321, "ask": 1.08331},
    "error": "ProtocolError",
}


def _decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def verify_request(method: str, target: str, headers: Mapping[str, str]) -> None:
    """Check the synthetic signature and bind it to the transmitted request target.

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
        if (
            method != "GET"
            or data.get("method") != method
            or data.get("path") != target
            or data.get("body_hash") != EMPTY_HASH
            or data.get("idempotency_key") != ""
            or headers["exn-idempotency-key"] != ""
        ):
            raise ValueError
    except (KeyError, ValueError, TypeError, InvalidSignature) as exc:
        raise ValueError("Invalid signed request") from exc


def compare_transcripts(python: Mapping[str, Any], go: Mapping[str, Any]) -> None:
    """Require each peer to satisfy the expected scenario as well as each other."""
    for label, transcript in (("Python", python), ("Go", go)):
        for field, expected in EXPECTED.items():
            if transcript.get(field) != expected:
                raise ValueError(f"{label} transcript mismatch: {field}")
        if set(transcript) != set(EXPECTED):
            raise ValueError(f"{label} transcript contains unexpected fields")
    if python != go:
        raise ValueError("Cross-SDK transcript mismatch")
