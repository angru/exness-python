"""Emit actual decoder outcomes for the optional cross-repository check."""

import json
import os
from pathlib import Path

from pydantic import ValidationError
from test_response_validation import CASES, decode_case, response_case

from exness import ProtocolError


def test_parity_report() -> None:
    actual: dict[str, str] = {}
    for case in CASES:
        try:
            decode_case(case["kind"], response_case(case))
        except (ProtocolError, ValidationError):
            outcome = "protocol_error"
        else:
            outcome = "accepted"
        assert case["name"] not in actual
        actual[case["name"]] = outcome
    assert actual == {
        case["name"]: "accepted" if case["valid"] else "protocol_error" for case in CASES
    }
    if destination := os.environ.get("EXNESS_PARITY_REPORT"):
        Path(destination).write_text(json.dumps(actual, sort_keys=True) + "\n")
