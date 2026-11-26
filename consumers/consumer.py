"""Run and type-check this file outside the checkout against the installed wheel."""

import asyncio
import json
import sys
from decimal import Decimal
from importlib.metadata import distribution
from importlib.resources import files
from pathlib import Path
from typing import assert_type

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import exness
from exness import (
    APIError,
    AsyncExnessClient,
    ConfigurationError,
    Credentials,
    ExnessClient,
    ExnessError,
    OperationReference,
    OperationWaitTimeout,
    ProtocolError,
    ServerStreamGap,
    ServerStreamOptions,
    StreamClosed,
    StreamGap,
    StreamOptions,
    TickStreamGap,
    TickStreamOptions,
    TradingStreamGap,
    TradingStreamOptions,
    TransportError,
    UnknownOutcomeError,
    WebSocketAPIError,
)
from exness.events import AccountStateEvent, HmrSnapshot, HmrUpdate, InstrumentEvent
from exness.models import AckResponse, OperationStatus
from exness.requests import (
    ClosePositionRequest,
    LimitOrderRequest,
    ModifyOrderRequest,
    OpenPositionRequest,
    PendingOrderRequest,
    SltpOrderRequest,
    StopOrderRequest,
)
from exness.trading import TradingSnapshotEvent, TransactionEvent


class SyntheticAPI:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []
        self.status_reads = 0
        self.key = ""
        self.fail_next = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "consumer.invalid"
        assert request.headers["EXN-SIGN"]
        path = request.url.path
        if path == "/v1/trading/access-point":
            assert request.method == "GET"
            return httpx.Response(200, json={"access_point": "https://consumer.invalid"})
        if request.method == "GET":
            assert path == "/v1/trading/accounts/123456/operations/789"
            self.status_reads += 1
            return httpx.Response(
                200,
                json={
                    "operation_id": "789",
                    "client_request_id": self.key,
                    "status": "pending" if self.status_reads == 1 else "confirmed",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            )
        self.key = request.headers["EXN-IDEMPOTENCY-KEY"]
        self.writes.append((request.method, path.rsplit("/", 1)[-1]))
        if request.method == "PATCH":
            assert json.loads(request.content) == {"stop_loss_price": None}
        if request.method == "DELETE" and path.endswith("/789"):
            assert request.url.params["volume"] == "0.01"
        if self.fail_next:
            return httpx.Response(503, json={"code": 5000})
        return httpx.Response(
            202, json={"status": "accepted", "operation_id": "789", "client_request_id": self.key}
        )


def credentials() -> Credentials:
    key = Ed25519PrivateKey.from_private_bytes(bytes(32))
    return Credentials(
        "consumer-synthetic", (bytes(32) + key.public_key().public_bytes_raw()).hex()
    )


def opening() -> OpenPositionRequest:
    return OpenPositionRequest(instrument="EURUSD", side="buy", volume=Decimal("0.01"))


def pending_orders() -> list[PendingOrderRequest]:
    return [
        LimitOrderRequest(
            instrument="EURUSD", side="buy", volume=Decimal("0.01"), price=Decimal("1.1")
        ),
        StopOrderRequest(
            instrument="EURUSD", side="sell", volume=Decimal("0.01"), price=Decimal("1.2")
        ),
        SltpOrderRequest(position_id="789", stop_loss_price=Decimal("1.0")),
    ]


def reference_from(ack: AckResponse) -> OperationReference:
    assert ack.status == "accepted"
    return OperationReference.from_ack(ack, client_request_id="consumer-open-0001")


def check_status(reference: OperationReference, status: OperationStatus) -> None:
    assert status.is_terminal and status.status == "confirmed"
    assert reference.matches_status(status)
    event = TransactionEvent.model_validate_json(
        '{"event_type":"transaction_event","operation_id":"789",'
        '"client_request_id":"consumer-open-0001","status":"success","payload":{}}'
    )
    assert reference.matches_event(event)
    timeout = OperationWaitTimeout(reference, status, 2)
    assert (
        timeout.reference == reference and timeout.last_status is status and timeout.attempts == 2
    )


def check_requests(api: SyntheticAPI) -> None:
    assert api.status_reads == 2
    assert api.writes == [
        ("POST", "positions"),
        ("POST", "orders"),
        ("POST", "orders"),
        ("POST", "orders"),
        ("PATCH", "456"),
        ("DELETE", "456"),
        ("DELETE", "789"),
        ("DELETE", "456"),
    ]


def sync_consumer() -> None:
    api = SyntheticAPI()
    with ExnessClient(
        credentials=credentials(),
        account_id="123456",
        discovery_url="https://consumer.invalid",
        transport=httpx.MockTransport(api),
    ) as client:
        ack = assert_type(
            client.open_position(opening(), idempotency_key="consumer-open-0001"), AckResponse
        )
        reference = reference_from(ack)
        status = assert_type(
            client.wait_operation(reference, timeout=1, poll_interval=0.001), OperationStatus
        )
        check_status(reference, status)
        for index, request in enumerate(pending_orders()):
            assert_type(
                client.place_pending_order(request, idempotency_key=f"consumer-order-{index}"),
                AckResponse,
            )
        assert_type(
            client.modify_order(
                "456",
                ModifyOrderRequest(stop_loss_price=None),
                idempotency_key="consumer-modify-0001",
            ),
            AckResponse,
        )
        assert_type(
            client.cancel_pending_order("456", idempotency_key="consumer-cancel-0001"), AckResponse
        )
        assert_type(
            client.close_position(
                "789",
                ClosePositionRequest(volume=Decimal("0.01")),
                idempotency_key="consumer-close-0001",
            ),
            AckResponse,
        )
        api.fail_next = True
        try:
            client.cancel_pending_order("456", idempotency_key="consumer-unknown-0001")
        except UnknownOutcomeError as error:
            assert error.idempotency_key == "consumer-unknown-0001" and error.method == "DELETE"
        else:
            raise AssertionError("ambiguous response lost its public error type")
    check_requests(api)


async def async_consumer() -> None:
    api = SyntheticAPI()
    async with AsyncExnessClient(
        credentials=credentials(),
        account_id="123456",
        discovery_url="https://consumer.invalid",
        transport=httpx.MockTransport(api),
    ) as client:
        ack = assert_type(
            await client.open_position(opening(), idempotency_key="consumer-open-0001"), AckResponse
        )
        reference = reference_from(ack)
        status = assert_type(
            await client.wait_operation(reference, timeout=1, poll_interval=0.001), OperationStatus
        )
        check_status(reference, status)
        for index, request in enumerate(pending_orders()):
            assert_type(
                await client.place_pending_order(
                    request, idempotency_key=f"consumer-order-{index}"
                ),
                AckResponse,
            )
        assert_type(
            await client.modify_order(
                "456",
                ModifyOrderRequest(stop_loss_price=None),
                idempotency_key="consumer-modify-0001",
            ),
            AckResponse,
        )
        assert_type(
            await client.cancel_pending_order("456", idempotency_key="consumer-cancel-0001"),
            AckResponse,
        )
        assert_type(
            await client.close_position(
                "789",
                ClosePositionRequest(volume=Decimal("0.01")),
                idempotency_key="consumer-close-0001",
            ),
            AckResponse,
        )
        options = StreamOptions()
        # Exercise signatures and generator construction; transport behavior has TLS tests.
        await client.stream_ticks(["EURUSD"], options=options).aclose()
        await client.stream_trading(options=options).aclose()
        await client.stream_account_state(options=options).aclose()
        await client.stream_instruments(["all"], options=options).aclose()
        await client.stream_hmr(["EURUSD"], options=options).aclose()
        api.fail_next = True
        try:
            await client.cancel_pending_order("456", idempotency_key="consumer-unknown-0001")
        except UnknownOutcomeError as error:
            assert error.idempotency_key == "consumer-unknown-0001" and error.method == "DELETE"
        else:
            raise AssertionError("ambiguous response lost its public error type")
    check_requests(api)


def main() -> None:
    assert exness.__file__ is not None
    assert Path(exness.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert files("exness").joinpath("py.typed").is_file()
    installed_files = distribution("exness-python").files
    assert installed_files
    assert all(str(path).startswith(("exness/", "exness_python-")) for path in installed_files), (
        "wheel contains unexpected top-level files"
    )
    assert StreamOptions is TickStreamOptions is TradingStreamOptions is ServerStreamOptions
    assert StreamGap is TickStreamGap is TradingStreamGap is ServerStreamGap
    for error in (
        APIError,
        ConfigurationError,
        ProtocolError,
        StreamClosed,
        TransportError,
        WebSocketAPIError,
        UnknownOutcomeError,
        OperationWaitTimeout,
    ):
        assert issubclass(error, ExnessError)
    for model in (AccountStateEvent, InstrumentEvent, HmrSnapshot, HmrUpdate, TradingSnapshotEvent):
        assert model.model_fields
    sync_consumer()
    asyncio.run(async_consumer())
    print("Installed Python consumer: sync/async submissions, waiting, exports and models passed")


if __name__ == "__main__":
    main()
