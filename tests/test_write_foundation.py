import hashlib
import json
import ssl
from decimal import Decimal
from http.server import BaseHTTPRequestHandler

import httpx
import pytest
from conftest import PUBLIC, fixture_bytes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import ValidationError
from test_auth import decode
from test_network import serve_tls
from test_network import tls_contexts as tls_contexts

from exness import Credentials
from exness.auth import _Signer
from exness.models import AckResponse
from exness.requests import OpenPositionRequest


@pytest.mark.parametrize("index", [0, 1, 2])
def test_shared_openssl_mutation_vectors(credentials: Credentials, index: int) -> None:
    vector = json.loads(fixture_bytes("mutation-signing-vectors"))["vectors"][index]
    signer = _Signer(credentials, clock=lambda: vector["timestamp_ms"] / 1000)
    headers = signer.request_headers(
        vector["method"],
        vector["path"],
        body=vector["body_utf8"].encode(),
        idempotency_key=vector["idempotency_key"],
    )
    assert headers["EXN-DATA"] == vector["exn_data"]
    assert headers["EXN-SIGN"] == vector["exn_sign"]
    assert decode(headers["EXN-DATA"]).decode() == vector["payload_utf8"]
    assert headers["EXN-IDEMPOTENCY-KEY"] == vector["idempotency_key"]


@pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE"])
@pytest.mark.parametrize(
    "key", ["", "abcd", "a" * 129, "has space", "тест-123", "abcde\n", "abcde/"]
)
def test_invalid_mutation_keys(credentials: Credentials, method: str, key: str) -> None:
    with pytest.raises(ValueError, match="Invalid idempotency key"):
        _Signer(credentials).request_headers(method, "/test", idempotency_key=key)


@pytest.mark.parametrize("key", ["a._~-", "A" * 128])
def test_key_boundaries_and_fresh_signatures(credentials: Credentials, key: str) -> None:
    times = iter([1000.0, 1001.0])
    signer = _Signer(credentials, clock=lambda: next(times))
    first = signer.request_headers("POST", "/test", body=b"{}", idempotency_key=key)
    second = signer.request_headers("POST", "/test", body=b"{}", idempotency_key=key)
    assert first["EXN-IDEMPOTENCY-KEY"] == second["EXN-IDEMPOTENCY-KEY"] == key
    assert first["EXN-SIGN"] != second["EXN-SIGN"]


@pytest.mark.parametrize(
    ("method", "body", "key"),
    [("GET", b"{}", ""), ("GET", b"", "abcde"), ("post", b"", "abcde"), ("PUT", b"", "abcde")],
)
def test_invalid_signing_shape(
    credentials: Credentials, method: str, body: bytes, key: str
) -> None:
    with pytest.raises(ValueError):
        _Signer(credentials).request_headers(method, "/test", body=body, idempotency_key=key)


def test_request_encoding_preserves_decimal_values_and_omission() -> None:
    request = OpenPositionRequest.model_validate_json(fixture_bytes("open_position_request"))
    assert json.loads(request.to_body()) == json.loads(fixture_bytes("open_position_request"))
    small = OpenPositionRequest(instrument="EURUSD", side="sell", volume=Decimal("1E-20"))
    assert json.loads(small.to_body()) == {
        "instrument": "EURUSD",
        "side": "sell",
        "volume": "0.00000000000000000001",
    }
    assert "price" not in json.loads(small.to_body())
    # Pydantic's copy/construct shortcuts must not bypass final body validation.
    with pytest.raises(ValidationError):
        small.model_copy(update={"volume": Decimal("-1")}).to_body()


@pytest.mark.parametrize(
    "patch",
    [
        {"volume": "0"},
        {"volume": "-1"},
        {"volume": 0.1},
        {"volume": True},
        {"volume": "NaN"},
        {"volume": "1e-7"},
        {"price": "0"},
        {"price": None},
        {"stop_loss_price": "-1"},
        {"take_profit_price": "0"},
        {"deviation": "Infinity"},
        {"comment": None},
        {"instrument": "EUR/USD"},
        {"side": "BUY"},
        {"volum": "1"},
    ],
)
def test_invalid_request_fields(patch: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        OpenPositionRequest.model_validate(
            {"instrument": "EURUSD", "side": "buy", "volume": "0.01", **patch}
        )


@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b"null",
        b'{"status":"confirmed"}',
        b'{"status":"accepted","operation_id":"18446744073709551616"}',
    ],
)
def test_invalid_acknowledgements(body: bytes) -> None:
    with pytest.raises(ValidationError):
        AckResponse.model_validate_json(body)


def test_acknowledgement_is_admission_and_preserves_missing_id() -> None:
    ack = AckResponse.model_validate_json(fixture_bytes("ack_accepted"))
    assert ack.status == "accepted" and ack.operation_id == "000789"
    assert ack.client_request_id == "trade-test-0001"
    missing = AckResponse.model_validate_json(fixture_bytes("ack_without_id"))
    assert missing.operation_id is None and missing.client_request_id is None
    assert AckResponse(status="accepted", operation_id="18446744073709551615").operation_id


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("index", [0, 1, 2])
async def test_signed_mutation_bytes_on_local_tls_wire(
    credentials: Credentials,
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    asynchronous: bool,
    index: int,
) -> None:
    vector = json.loads(fixture_bytes("mutation-signing-vectors"))["vectors"][index]
    if index == 0:
        body = OpenPositionRequest.model_validate_json(
            fixture_bytes("open_position_request")
        ).to_body()
    else:
        body = vector["body_utf8"].encode()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        response = fixture_bytes("ack_accepted")
        handler.send_response(202)
        handler.send_header("Content-Length", str(len(response)))
        handler.end_headers()
        handler.wfile.write(response)

    def signed_request(url: str) -> httpx.Request:
        request = httpx.Request(vector["method"], url, content=body)
        request.headers.update(
            _Signer(credentials).request_headers(
                request.method,
                request.url.raw_path.decode("ascii"),
                body=request.content,
                idempotency_key=vector["idempotency_key"],
            )
        )
        request.headers["Content-Type"] = "application/json"
        return request

    with serve_tls(tls_contexts, respond) as server:
        request = signed_request(server.url + vector["path"])
        if asynchronous:
            async with httpx.AsyncClient(
                verify=tls_contexts[1], trust_env=False, timeout=2
            ) as client:
                response = await client.send(request)
        else:
            with httpx.Client(verify=tls_contexts[1], trust_env=False, timeout=2) as sync_client:
                response = sync_client.send(request)
        assert response.status_code == 202
        assert AckResponse.model_validate_json(response.content).status == "accepted"
        wire, _ = server.requests.get_nowait()
        assert server.requests.empty()
        payload = decode(wire.headers["EXN-DATA"])
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(PUBLIC)).verify(
            decode(wire.headers["EXN-SIGN"]),
            payload,
        )
        metadata = json.loads(payload)
        assert metadata["method"] == wire.method == vector["method"]
        assert metadata["path"] == wire.url.raw_path.decode("ascii") == vector["path"]
        assert metadata["idempotency_key"] == wire.headers["EXN-IDEMPOTENCY-KEY"]
        assert decode(metadata["body_hash"]) == hashlib.sha256(wire.content).digest()
        assert wire.content == body
