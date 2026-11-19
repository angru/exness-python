import base64
import json

import pytest
from conftest import PUBLIC, SEED, fixture_bytes
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from exness import ConfigurationError, Credentials
from exness.auth import _Signer


def decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def test_fixed_vector_signed_independently_by_openssl() -> None:
    vector = json.loads(fixture_bytes("signing-vector"))
    credentials = Credentials(vector["api_key"], vector["private_key_hex"])
    signer = _Signer(credentials, clock=lambda: vector["timestamp_ms"] / 1000)
    headers = signer.get_headers(vector["path"])
    assert headers["EXN-DATA"] == vector["exn_data"]
    assert headers["EXN-SIGN"] == vector["exn_sign"]
    assert decode(headers["EXN-DATA"]).decode() == vector["payload_utf8"]


def test_signature_covers_exact_payload_and_empty_body(credentials: Credentials) -> None:
    path = "/v1/history/accounts/123456/deals?cursor=a%2Bb%2F%3D&limit=10"
    headers = _Signer(credentials, clock=lambda: 1773656070.123).get_headers(path)
    payload = decode(headers["EXN-DATA"])
    signature = decode(headers["EXN-SIGN"])
    assert json.loads(payload) == {
        "api_key": "exnsk-synthetic-test",
        "idempotency_key": "",
        "timestamp": 1773656070123,
        "sign_version": 1,
        "method": "GET",
        "path": path,
        "body_hash": "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU",
    }
    assert headers["EXN-IDEMPOTENCY-KEY"] == ""
    assert headers["EXN-TIMESTAMP"] == "1773656070123"
    assert headers["EXN-SIGN-VERSION"] == "1"
    assert "=" not in headers["EXN-DATA"] + headers["EXN-SIGN"]
    public_key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(PUBLIC))
    public_key.verify(signature, payload)
    with pytest.raises(InvalidSignature):
        public_key.verify(signature, headers["EXN-DATA"].encode())
    with pytest.raises(InvalidSignature):
        public_key.verify(signature, payload.replace(b"limit=10", b"limit=11"))


def test_fresh_timestamp_per_request(credentials: Credentials) -> None:
    times = iter([1000.0, 1001.0])
    signer = _Signer(credentials, clock=lambda: next(times))
    assert signer.get_headers("/test")["EXN-SIGN"] != signer.get_headers("/test")["EXN-SIGN"]


def test_pem_and_hex_produce_same_signature(credentials: Credentials) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SEED))
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    expected = _Signer(credentials, clock=lambda: 123.0).get_headers("/test")
    actual = _Signer(Credentials("exnsk-synthetic-test", pem), clock=lambda: 123.0)
    assert actual.get_headers("/test") == expected
    assert SEED not in repr(credentials)
    assert "exnsk-synthetic-test" not in repr(credentials)


@pytest.mark.parametrize("private_key", ["", "secret", SEED, SEED + "00" * 32, "g" * 128])
def test_bad_keys_are_rejected_without_exposing_input(private_key: str) -> None:
    with pytest.raises(ConfigurationError) as error:
        Credentials("test", private_key)
    if private_key:
        assert private_key not in str(error.value)


@pytest.mark.parametrize("api_key", ["", "contains space", "key\r\nHost: bad", "ключ"])
def test_bad_api_keys(api_key: str) -> None:
    with pytest.raises(ConfigurationError):
        Credentials(api_key, SEED + PUBLIC)


def test_wrong_pem_key_type_and_encrypted_key() -> None:
    for key, encryption in (
        (ec.generate_private_key(ec.SECP256R1()), serialization.NoEncryption()),
        (Ed25519PrivateKey.generate(), serialization.BestAvailableEncryption(b"test")),
    ):
        pem = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption
        ).decode()
        with pytest.raises(ConfigurationError):
            Credentials("test", pem)
