"""Ed25519 signing for the read-only EXN protocol."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from exness.errors import ConfigurationError


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _load_key(value: str) -> Ed25519PrivateKey:
    value = value.strip()
    try:
        if re.fullmatch(r"[0-9a-fA-F]{128}", value):
            raw = bytes.fromhex(value)
            key = Ed25519PrivateKey.from_private_bytes(raw[:32])
            if not hmac.compare_digest(key.public_key().public_bytes_raw(), raw[32:]):
                raise ValueError("Mismatched public key")
            return key
        loaded = serialization.load_pem_private_key(value.encode("utf-8"), password=None)
        if isinstance(loaded, Ed25519PrivateKey):
            return loaded
    except (ValueError, TypeError, UnsupportedAlgorithm):
        pass
    raise ConfigurationError(
        "Expected a valid 128-character Ed25519 hex key or unencrypted Ed25519 PKCS#8 PEM"
    ) from None


class Credentials:
    """Signing credentials; repr hides both the API identifier and private material."""

    def __init__(self, api_key: str, private_key: str) -> None:
        if not api_key or any(ord(char) < 33 or ord(char) > 126 for char in api_key):
            raise ConfigurationError("API key must be nonempty printable ASCII without whitespace")
        self._api_key = api_key
        self._key = _load_key(private_key)

    def __repr__(self) -> str:
        return "Credentials(<redacted>)"


class _Signer:
    def __init__(self, credentials: Credentials, clock: Callable[[], float] = time.time) -> None:
        self._credentials = credentials
        self._clock = clock

    def get_headers(self, path: str) -> dict[str, str]:
        """Sign the final encoded path and query of a bodyless GET request."""
        timestamp = int(self._clock() * 1000)
        api_key = self._credentials._api_key
        payload = json.dumps(
            {
                "api_key": api_key,
                "idempotency_key": "",
                "timestamp": timestamp,
                "sign_version": 1,
                "method": "GET",
                "path": path,
                "body_hash": _base64url(hashlib.sha256(b"").digest()),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return {
            "EXN-API-KEY": api_key,
            "EXN-IDEMPOTENCY-KEY": "",
            "EXN-TIMESTAMP": str(timestamp),
            "EXN-SIGN-VERSION": "1",
            "EXN-DATA": _base64url(payload),
            "EXN-SIGN": _base64url(self._credentials._key.sign(payload)),
        }
