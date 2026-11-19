import base64
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest
from conftest import PUBLIC, fixture_bytes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from exness import (
    APIError,
    ConfigurationError,
    Credentials,
    ExnessClient,
    ProtocolError,
    TransportError,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
GATEWAY = "https://gateway.example"
ACCESS_POINT = "https://account.example"


def client_for(
    credentials: Credentials, handler: Callable[[httpx.Request], httpx.Response]
) -> ExnessClient:
    return ExnessClient(
        credentials=credentials,
        account_id="123456",
        discovery_url=GATEWAY,
        transport=httpx.MockTransport(handler),
    )


def discovery() -> httpx.Response:
    return httpx.Response(200, json={"access_point": ACCESS_POINT, "account_id": "123456"})


def verify_request(request: httpx.Request) -> None:
    encoded = request.headers["EXN-DATA"]
    payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    encoded_signature = request.headers["EXN-SIGN"]
    signature = base64.urlsafe_b64decode(encoded_signature + "=" * (-len(encoded_signature) % 4))
    Ed25519PublicKey.from_public_bytes(bytes.fromhex(PUBLIC)).verify(signature, payload)
    data = json.loads(payload)
    assert data["path"] == request.url.raw_path.decode("ascii")
    assert data["method"] == request.method == "GET"
    assert data["body_hash"] == "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
    assert data["idempotency_key"] == request.headers["EXN-IDEMPOTENCY-KEY"] == ""
    assert request.content == b""


def test_read_workflows_cached_discovery_and_final_request_signatures(
    credentials: Credentials,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        verify_request(request)
        if request.url.host == "gateway.example":
            assert request.url.path == "/v1/trading/access-point"
            assert dict(request.url.params) == {"account_id": "123456"}
            return discovery()
        assert request.url.host == "account.example"
        fixture = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, content=fixture_bytes(fixture))

    with client_for(credentials, handler) as client:
        account = client.get_account()
        assert account.account_id == "123456"
        assert account.settings is not None
        assert account.settings.margin_call_level == Decimal("60.00")
        assert client.get_instruments().instruments == ["EURUSD", "XAUUSD"]
        assert client.get_snapshot().positions[0].position_id == "2001"
        assert client.get_limits().limits.rest.methods[0].limits is not None
        assert client.get_orders_history(from_time=START).items[0].order.order_id == "1002"
        assert client.get_deals_history(from_time=START).items[0].deal.type == "balance"
    assert len(requests) == 7
    assert [r.url.path for r in requests[1:]] == [
        "/v1/configuration/accounts/123456/account",
        "/v1/configuration/accounts/123456/instruments",
        "/v1/trading/accounts/123456/snapshot",
        "/v1/configuration/accounts/123456/limits",
        "/v1/history/accounts/123456/orders",
        "/v1/history/accounts/123456/deals",
    ]


@pytest.mark.parametrize("kind", ["orders", "deals"])
def test_pagination_discards_filters_preserves_cursor_and_signs_encoded_url(
    credentials: Credentials,
    kind: str,
) -> None:
    pages: list[httpx.Request] = []
    first = json.loads(fixture_bytes(kind))
    cursor = first["next_cursor"]

    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        if request.url.host == "gateway.example":
            return discovery()
        pages.append(request)
        return httpx.Response(
            200, json=first if len(pages) == 1 else {"items": [], "has_more": False}
        )

    with client_for(credentials, handler) as client:
        if kind == "orders":
            rows = list(
                client.iter_orders_history(
                    from_time=START,
                    to_time=START + timedelta(days=2),
                    limit=20,
                    instrument="EURUSD",
                    order_state="filled",
                )
            )
            assert len(rows) == 1
            assert pages[0].url.params["order_state"] == "filled"
        else:
            deals = list(
                client.iter_deals_history(
                    from_time=START,
                    to_time=START + timedelta(days=2),
                    limit=20,
                    instrument="EURUSD",
                    deal_type="balance",
                )
            )
            assert len(deals) == 1
            assert pages[0].url.params["deal_type"] == "balance"
    assert len(pages) == 2
    assert dict(pages[1].url.params) == {"limit": "20", "cursor": cursor}
    assert pages[0].url.params["from"] == "2026-01-01T00:00:00+00:00"


@pytest.mark.parametrize(
    "page",
    [
        {"items": [], "has_more": True},
        {"items": [], "has_more": False, "next_cursor": "unexpected"},
        {"items": [], "has_more": True, "next_cursor": "repeated"},
    ],
)
def test_bad_pagination_fails_without_looping(
    credentials: Credentials,
    page: dict[str, object],
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert calls <= 3
        return (
            discovery() if request.url.host == "gateway.example" else httpx.Response(200, json=page)
        )

    with client_for(credentials, handler) as client, pytest.raises(ProtocolError):
        list(client.iter_deals_history(from_time=START))


def test_pagination_uses_cursor_when_has_more_is_omitted(credentials: Credentials) -> None:
    pages = iter([{"items": [], "next_cursor": "second"}, {"items": []}])

    def handler(request: httpx.Request) -> httpx.Response:
        return (
            discovery()
            if request.url.host == "gateway.example"
            else httpx.Response(200, json=next(pages))
        )

    with client_for(credentials, handler) as client:
        assert list(client.iter_deals_history(from_time=START)) == []


@pytest.mark.parametrize("status", [301, 302, 307, 401, 403, 429, 503])
def test_http_errors_never_redirect_or_retry(credentials: Credentials, status: int) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status,
            json={"code": 1001, "error_message": "AUTH_INVALID_SIGNATURE"},
            headers={"Retry-After": "60", "Location": "https://elsewhere.example"},
        )

    with client_for(credentials, handler) as client, pytest.raises(APIError) as caught:
        client.get_account()
    assert calls == 1
    assert caught.value.status_code == status
    assert caught.value.code == 1001
    assert caught.value.error_message == "AUTH_INVALID_SIGNATURE"
    assert caught.value.retry_after == "60"
    assert "exnsk-synthetic-test" not in str(caught.value)


def test_non_json_http_error(credentials: Credentials) -> None:
    with (
        client_for(credentials, lambda request: httpx.Response(502, text="private body")) as client,
        pytest.raises(APIError) as caught,
    ):
        client.discover()
    assert caught.value.code is None
    assert "private body" not in str(caught.value)


@pytest.mark.parametrize("body", [b"not json", b"{}", b'{"access_point":123}'])
def test_invalid_success_response(credentials: Credentials, body: bytes) -> None:
    with (
        client_for(credentials, lambda request: httpx.Response(200, content=body)) as client,
        pytest.raises(ProtocolError),
    ):
        client.discover()


def test_timeout_is_sanitized_and_not_retried(credentials: Credentials) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.extensions["timeout"]["read"] == 15.0
        raise httpx.ReadTimeout("SECRET signed request details", request=request)

    with client_for(credentials, handler) as client, pytest.raises(TransportError) as caught:
        client.get_account()
    assert calls == 1
    assert "SECRET" not in str(caught.value)
    assert caught.value.__suppress_context__


@pytest.mark.parametrize(
    "result",
    [
        {"access_point": "http://insecure.example"},
        {"access_point": "https://account.example", "account_id": "99"},
    ],
)
def test_invalid_discovery_never_sends_account_request(
    credentials: Credentials,
    result: dict[str, str],
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=result)

    with client_for(credentials, handler) as client, pytest.raises(ProtocolError):
        client.get_account()
    assert calls == 1


def test_explicit_refresh_and_close(credentials: Credentials) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"access_point": f"https://ap{calls}.example"})

    client = client_for(credentials, handler)
    assert client.discover() == client.discover() == "https://ap1.example"
    assert client.discover(refresh=True) == "https://ap2.example"
    client.close()
    client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.get_account()
    with pytest.raises(RuntimeError, match="closed"), client:
        pass


def test_account_mismatch(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "gateway.example":
            return discovery()
        return httpx.Response(200, json={"account_id": "99", "create_date": START.isoformat()})

    with client_for(credentials, handler) as client, pytest.raises(ProtocolError, match="account"):
        client.get_account()


@pytest.mark.parametrize("account_id", ["", "0", "-1", "1/2", "١٢", "01", str(2**64)])
def test_invalid_accounts(credentials: Credentials, account_id: str) -> None:
    with pytest.raises(ConfigurationError):
        ExnessClient(credentials=credentials, account_id=account_id, discovery_url=GATEWAY)


@pytest.mark.parametrize(
    "url",
    [
        "bad",
        "http://a.example",
        "https://a.example/path",
        "https://u:p@a.example",
        "https://a.example?",
        "https://a.example#",
        "https://a.example:bad",
    ],
)
def test_invalid_hosts(credentials: Credentials, url: str) -> None:
    with pytest.raises(ConfigurationError):
        ExnessClient(credentials=credentials, account_id="123456", discovery_url=url)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout(credentials: Credentials, timeout: float) -> None:
    with pytest.raises(ConfigurationError):
        ExnessClient(
            credentials=credentials, account_id="123456", discovery_url=GATEWAY, timeout=timeout
        )


def test_history_validation_before_network(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid history arguments must not trigger discovery or network calls")

    with client_for(credentials, handler) as client:
        with pytest.raises(ValueError, match="from_time"):
            client.get_orders_history()
        with pytest.raises(ValueError, match="timezone"):
            client.get_deals_history(from_time=datetime(2026, 1, 1))
        with pytest.raises(ValueError, match="later"):
            client.get_deals_history(from_time=START, to_time=START)
        for limit in (0, 201, True):
            with pytest.raises(ValueError, match="limit"):
                client.get_deals_history(from_time=START, limit=limit)
        for instrument in ("", "too-long-instrument", "EUR/USD"):
            with pytest.raises(ValueError, match="instrument"):
                client.get_deals_history(from_time=START, instrument=instrument)
        with pytest.raises(ValueError, match="cursor"):
            client.get_orders_history(cursor="")
        with pytest.raises(ValueError, match="cursor"):
            client.get_orders_history(cursor="test", from_time=START)
        with pytest.raises(ValueError, match="cursor"):
            client.get_orders_history(cursor="test", order_state="filled")
        with pytest.raises(ValueError, match="cursor"):
            client.get_deals_history(cursor="test", deal_type="balance")


def test_input_time_is_normalized_to_utc(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "gateway.example":
            return discovery()
        assert request.url.params["from"] == START.isoformat()
        return httpx.Response(200, json={"items": []})

    with client_for(credentials, handler) as client:
        client.get_deals_history(from_time=START.astimezone(timezone(timedelta(hours=2))))
