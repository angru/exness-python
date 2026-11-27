"""Validation primitives for the cross-SDK transport scenarios.

Only normalized, non-secret observations may be passed to compare_transcripts.
"""

import base64
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PUBLIC_KEY = bytes.fromhex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
ACCOUNT = "123456"
DISCOVERY_PATH = f"/v1/trading/access-point?account_id={ACCOUNT}"
ACCOUNT_PATH = f"/v1/configuration/accounts/{ACCOUNT}/account"
ORDERS_HISTORY_PATH = f"/v1/history/accounts/{ACCOUNT}/orders"
TICKS_PATH = f"/v1/server-events/accounts/{ACCOUNT}/ws/ticks"
TRADING_PATH = f"/v1/server-events/accounts/{ACCOUNT}/ws/events"
POSITIONS_PATH = f"/v1/trading/accounts/{ACCOUNT}/positions"
# Pin the raw target independently of the ACK fixture and status bodies.
OPERATION_STATUS_PATH = "/v1/trading/accounts/123456/operations/000789"
WRITE_KEY = "transport-lost-ack-0001"
ACKNOWLEDGED_OPEN_KEY = "transport-acknowledged-open-0001"
OPEN_ORDER = {"instrument": "EURUSD", "side": "buy", "volume": "0.01"}
ORDERS_FROM = datetime(2026, 1, 1, tzinfo=UTC)
ORDERS_LIMIT = 7
ORDERS_QUERIES = (
    {
        "from": ORDERS_FROM.isoformat(),
        "instrument": "EURUSD",
        "order_state": "filled",
        "limit": "7",
    },
    {"cursor": "opaque+/= &%?Ж", "limit": "7"},
)


class Scenario(StrEnum):
    REST_TICKS = "rest_ticks"
    TRADING_RECOVERY = "trading_recovery"
    TRADING_MISSING_SNAPSHOT = "trading_missing_snapshot"
    LOST_ACK = "lost_ack"
    LOST_ACK_RECOVERY = "lost_ack_recovery"
    ACKNOWLEDGED_OPEN = "acknowledged_open"
    ACKNOWLEDGED_OPEN_RATE_LIMITED = "acknowledged_open_rate_limited"
    HMR_RECOVERY = "hmr_recovery"
    HMR_MISSING_SNAPSHOT = "hmr_missing_snapshot"
    ORDERS_HISTORY_REPEATED_CURSOR = "orders_history_repeated_cursor"
    INSTRUMENT_RECOVERY = "instrument_recovery"


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
HMR_WIRE = {
    "paths": [DISCOVERY_PATH, TRADING_PATH, TRADING_PATH],
    "subscriptions": [
        {"event": "hmr", "instruments": ["EURUSD"]},
        {"event": "hmr", "instruments": ["EURUSD"]},
    ],
}
INSTRUMENT_OBSERVATION = {
    "kind": "instrument",
    "event_type": "instrument_event",
    "instrument": "EURUSD",
    "point_digits": 5,
    "trade_mode": "enabled",
    "volume_min": "0.01",
}
HMR_PERIODIC = {
    "period_id": "702",
    "event_type": "periodic",
    "instruments": ["EURUSD"],
    "leverage": "20",
    "start_time": "Mon, 13:00:00",
    "end_time": "Mon, 14:00:00",
}
HMR_START = [
    {
        "kind": "snapshot",
        "event_type": "hmr_snapshot",
        "periods": [
            {
                "period_id": "701",
                "event_type": "regular",
                "instruments": ["EURUSD", "EURGBP"],
                "leverage": "10",
                "start_time": "2026-11-19 11:00:00",
                "end_time": "2026-11-19 12:00:00",
            },
            HMR_PERIODIC,
        ],
    },
    {"kind": "gap", "reconnect_attempt": 1, "close_code": 1012},
]
OPEN_RECEIPT = {
    "method": "POST",
    "path": POSITIONS_PATH,
    "order": OPEN_ORDER,
    "signature_verified": True,
}
LOST_ACK_WIRE = {
    "paths": [DISCOVERY_PATH, POSITIONS_PATH],
    "post_count": 1,
    "receipt": {
        **OPEN_RECEIPT,
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
}
ACKNOWLEDGED_OPEN_WIRE = {
    "paths": [DISCOVERY_PATH, POSITIONS_PATH, OPERATION_STATUS_PATH, OPERATION_STATUS_PATH],
    "post_count": 1,
    "receipt": {
        **OPEN_RECEIPT,
        "idempotency_key": ACKNOWLEDGED_OPEN_KEY,
        "acknowledgement_sent": True,
    },
    "ack": {
        "status": "accepted",
        "operation_id": "000789",
        "client_request_id": ACKNOWLEDGED_OPEN_KEY,
    },
}
EXPECTED: dict[Scenario, dict[str, object]] = {
    Scenario.REST_TICKS: EXPECTED_TICKS,
    Scenario.ORDERS_HISTORY_REPEATED_CURSOR: {
        "paths": [DISCOVERY_PATH, ORDERS_HISTORY_PATH, ORDERS_HISTORY_PATH],
        "queries": list(ORDERS_QUERIES),
        "order_ids": ["1002"],
        "outcome": "ProtocolError: repeated continuation cursor",
    },
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
    Scenario.HMR_RECOVERY: {
        **HMR_WIRE,
        "events": [
            *HMR_START,
            {"kind": "snapshot", "event_type": "hmr_snapshot", "periods": []},
            {
                "kind": "update",
                "event_type": "hmr_update",
                "upserted_periods": [{**HMR_PERIODIC, "leverage": "30"}],
                "removed_period_ids": ["701", "999"],
            },
        ],
        "outcome": "normal_close",
    },
    Scenario.HMR_MISSING_SNAPSHOT: {
        **HMR_WIRE,
        "events": HMR_START,
        "outcome": "ProtocolError",
    },
    Scenario.INSTRUMENT_RECOVERY: {
        "paths": [DISCOVERY_PATH, TRADING_PATH, TRADING_PATH],
        "subscriptions": [
            {"event": "instruments", "instruments": ["EURUSD"]},
            {"event": "instruments", "instruments": ["EURUSD"]},
        ],
        "events": [
            {**INSTRUMENT_OBSERVATION, "action": "new"},
            {"kind": "gap", "reconnect_attempt": 1, "close_code": 1012},
            {**INSTRUMENT_OBSERVATION, "action": "upd"},
            {**INSTRUMENT_OBSERVATION, "action": "del"},
        ],
        "outcome": "normal_close",
    },
    Scenario.LOST_ACK: LOST_ACK_WIRE,
    Scenario.LOST_ACK_RECOVERY: {
        **LOST_ACK_WIRE,
        "stream_paths": [DISCOVERY_PATH, TRADING_PATH],
        "subscriptions": [{"event": "transactions"}],
        "events": [
            {**TRADING_START[0], "selected": None},
            {**TRADING_START[1], "decision": "ignored", "selected": None},
            {
                "kind": "transaction",
                "operation_id": "789",
                "status": "rejected",
                "positions": 0,
                "deals": 0,
                "account_state": None,
                "decision": "selected",
                "selected": {"operation_id": "789", "status": "rejected"},
            },
        ],
    },
    Scenario.ACKNOWLEDGED_OPEN: {
        **ACKNOWLEDGED_OPEN_WIRE,
        "status_responses": ["pending", "confirmed"],
        "outcome": {
            "status": "confirmed",
            "operation_id": "000789",
            "client_request_id": ACKNOWLEDGED_OPEN_KEY,
        },
    },
    Scenario.ACKNOWLEDGED_OPEN_RATE_LIMITED: {
        **ACKNOWLEDGED_OPEN_WIRE,
        "status_responses": ["pending", 429],
        "outcome": {"error": "APIError", "status_code": 429, "retry_after": "7"},
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
