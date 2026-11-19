"""Bounded, reproducible generated cases; never contact a live API."""

import base64
import json
from decimal import Decimal
from fractions import Fraction

import httpx
import pytest
from conftest import PUBLIC, SEED
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from hypothesis import example, given, settings
from hypothesis import strategies as st
from pydantic import TypeAdapter, ValidationError
from test_client import client_for, discovery, verify_request

from exness import ConfigurationError, Credentials, ProtocolError
from exness.auth import _Signer
from exness.client import _base_url, _iterate
from exness.models import DecimalValue, HistoryPage

generated = settings(max_examples=100, derandomize=True, database=None, deadline=None)
decimal_adapter = TypeAdapter(DecimalValue)


@generated
@given(
    whole=st.text(alphabet="0123456789", min_size=1, max_size=60),
    fraction=st.text(alphabet="0123456789", max_size=60),
    sign=st.sampled_from(["", "+", "-"]),
)
@example(whole="010", fraction="", sign="")
@example(whole="08", fraction="", sign="")
def test_exact_decimal_arithmetic(whole: str, fraction: str, sign: str) -> None:
    text = sign + whole + ("." + fraction if fraction else "")
    value = decimal_adapter.validate_json(json.dumps(text))
    expected = Fraction(int(sign + whole + fraction), 10 ** len(fraction))
    assert Fraction(value) == expected
    assert decimal_adapter.validate_json(decimal_adapter.dump_json(value)) == value


@generated
@given(value=st.text(max_size=120))
def test_decimal_grammar(value: str) -> None:
    unsigned = value[1:] if value.startswith(("+", "-")) else value
    parts = unsigned.split(".")
    valid = len(parts) <= 2 and all(
        part and all("0" <= char <= "9" for char in part) for part in parts
    )
    if valid:
        assert decimal_adapter.validate_python(value) == Decimal(value)
    else:
        with pytest.raises(ValidationError):
            decimal_adapter.validate_python(value)


@generated
@given(seed=st.binary(min_size=32, max_size=32))
def test_key_formats_sign_identical_bytes(seed: bytes) -> None:
    key = Ed25519PrivateKey.from_private_bytes(seed)
    hex_key = (seed + key.public_key().public_bytes_raw()).hex()
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")
    path = "/v1/test?cursor=a%2B%2F%3D"
    headers = [
        _Signer(Credentials("synthetic", value), clock=lambda: 123.0).get_headers(path)
        for value in (hex_key, pem)
    ]
    assert headers[0] == headers[1]
    payload = base64.urlsafe_b64decode(headers[0]["EXN-DATA"] + "==")
    signature = base64.urlsafe_b64decode(headers[0]["EXN-SIGN"] + "==")
    key.public_key().verify(signature, payload)
    assert json.loads(payload)["path"] == path


@generated
@given(value=st.text(max_size=512))
def test_arbitrary_key_text_fails_cleanly(value: str) -> None:
    # Accepted inputs must sign successfully and keep their representation redacted.
    try:
        credentials = Credentials("synthetic", value)
    except ConfigurationError as error:
        assert str(error).startswith("Expected a valid")
    else:
        assert repr(credentials) == "Credentials(<redacted>)"
        assert _Signer(credentials).get_headers("/test")["EXN-SIGN"]


@generated
@given(value=st.text(max_size=512))
@example(value="https://example.test/")
@example(value="https://example.test/%2f")
def test_accepted_origins_preserve_route(value: str) -> None:
    try:
        origin = _base_url(value)
    except ConfigurationError:
        return
    url = httpx.URL(origin + "/v1/test")
    assert url.scheme == "https" and url.host and not url.userinfo
    assert url.path == "/v1/test" and not url.query and not url.fragment


@generated
@given(cursor=st.text(min_size=1, max_size=128))
@example(cursor="Курсор 🌍 +/=&?#%\x00")  # noqa: RUF001 -- intentional Unicode cursor
def test_cursor_roundtrip_matches_signature(cursor: str) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        verify_request(request)
        if request.url.host == "gateway.example":
            return discovery()
        calls += 1
        assert dict(request.url.params) == {"cursor": cursor, "limit": "17"}
        return httpx.Response(200, json={"items": [], "has_more": False})

    with client_for(Credentials("synthetic", SEED + PUBLIC), handler) as client:
        assert client.get_orders_history(cursor=cursor, limit=17).items == []
    assert calls == 1


@generated
@given(count=st.integers(min_value=1, max_value=32), cycle=st.booleans(), bad=st.booleans())
def test_pagination_terminates_before_duplicate_page(count: int, cycle: bool, bad: bool) -> None:
    calls = 0

    def page(cursor: str = "") -> HistoryPage[int]:
        nonlocal calls
        index = calls
        calls += 1
        assert calls <= count + 1
        more = index < count - 1 or cycle
        following = (str(index + 1) if index < count else "1") if more else None
        return HistoryPage[int](
            items=[index], next_cursor=following, has_more=not more if bad and index == 0 else more
        )

    rows: list[int] = []
    if bad or cycle:
        with pytest.raises(ProtocolError):
            rows.extend(_iterate(page, page))
    else:
        rows.extend(_iterate(page, page))
    assert rows == list(range(0 if bad else count))
    assert calls == (1 if bad else count + int(cycle))
