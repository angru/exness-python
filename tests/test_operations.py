"""Shared identity cases, explicit polling policy and cooperative sync budgets."""

import asyncio
import json
from typing import Any

import httpx
import pytest
from conftest import fixture_bytes
from test_async_client import async_client_for
from test_client import client_for, discovery, verify_request

from exness import (
    APIError,
    Credentials,
    OperationReference,
    OperationWaitTimeout,
    ProtocolError,
    TransportError,
)
from exness.models import AckResponse, OperationStatus
from exness.operations import _sync_deadline
from exness.trading import TransactionEvent


@pytest.mark.parametrize(
    "case", json.loads(fixture_bytes("operation-correlation-cases")), ids=lambda case: case["name"]
)
def test_shared_correlation(case: dict[str, Any]) -> None:
    ref = OperationReference(**case["reference"])
    event = TransactionEvent.model_validate(
        {"event_type": "transaction_event", "payload": {}, **case["candidate"]}
    )
    if case["outcome"] == "conflict":
        with pytest.raises(ProtocolError):
            ref.matches_event(event)
    else:
        assert ref.matches_event(event) == (case["outcome"] == "match")
    assert event.status is None  # Identity alone does not establish a terminal outcome.
    if "operation_id" in case["candidate"]:
        data = json.loads(fixture_bytes("operation_pending"))
        data.pop("client_request_id", None)
        status = OperationStatus.model_validate_json(json.dumps({**data, **case["candidate"]}))
        if case["outcome"] == "conflict":
            with pytest.raises(ProtocolError):
                ref.matches_status(status)
        else:
            assert ref.matches_status(status) == (case["outcome"] == "match")


def test_ack_identity_and_validation() -> None:
    ack = AckResponse.model_validate_json(fixture_bytes("ack_accepted"))
    ref = OperationReference.from_ack(ack, client_request_id="trade-test-0001")
    assert ref == OperationReference("000789", "trade-test-0001")
    assert ref == OperationReference.from_ack(ack)
    assert "trade-test" not in repr(ref)
    with pytest.raises(ProtocolError):
        OperationReference.from_ack(ack, client_request_id="other-request")
    empty = AckResponse(status="accepted")
    with pytest.raises(ValueError):
        OperationReference.from_ack(empty)
    assert OperationReference.from_ack(
        empty, client_request_id="saved-request"
    ) == OperationReference(client_request_id="saved-request")
    for identity in ("", "-1", "18446744073709551616", "0" * 21):
        with pytest.raises(ValueError):
            OperationReference(operation_id=identity)
    for key in ("", "tiny", "private key", "x" * 129):
        with pytest.raises(ValueError):
            OperationReference(client_request_id=key)
    with pytest.raises(ProtocolError):
        ref.matches_event(
            # Bypass construction validation to retain the correlation helper's guard test.
            TransactionEvent.model_validate(
                {"event_type": "transaction_event", "payload": {}}
            ).model_copy(update={"operation_id": "18446744073709551616"})
        )


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("terminal", ["confirmed", "rejected", "failed"])
async def test_pending_then_terminal(
    credentials: Credentials, asynchronous: bool, terminal: str
) -> None:
    calls: list[str] = []
    statuses = iter(["pending", terminal])

    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(request)
        calls.append(request.url.path)
        if request.url.host == "gateway.example":
            return discovery()
        assert request.url.path.endswith("/operations/000789")
        return httpx.Response(200, content=fixture_bytes("operation_" + next(statuses)))

    reference = OperationReference("000789", "synthetic-operation-1")
    if asynchronous:
        async with async_client_for(credentials, handler) as client:
            result = await client.wait_operation(reference, timeout=1, poll_interval=0.001)
    else:
        with client_for(credentials, handler) as sync:
            result = sync.wait_operation(reference, timeout=1, poll_interval=0.001)
    assert result == OperationStatus.model_validate_json(fixture_bytes("operation_" + terminal))
    assert len(calls) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_pending_deadline_preserves_identity(
    credentials: Credentials, asynchronous: bool
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.host == "gateway.example":
            return discovery()
        return httpx.Response(200, content=fixture_bytes("operation_pending"))

    ref = OperationReference("789", "synthetic-operation-1")
    with pytest.raises(OperationWaitTimeout) as caught:
        if asynchronous:
            async with async_client_for(credentials, handler) as client:
                await client.wait_operation(ref, timeout=0.2, poll_interval=10)
        else:
            with client_for(credentials, handler) as sync:
                sync.wait_operation(ref, timeout=0.2, poll_interval=10)
    assert caught.value.reference == ref
    assert caught.value.last_status is not None
    assert caught.value.last_status.status == "pending"
    assert caught.value.attempts == 1
    assert "synthetic" not in str(caught.value)
    assert calls == 2
    assert _sync_deadline.get() is None


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("failure", ["404", "429", "transport", "key", "id"])
async def test_read_errors_stop(credentials: Credentials, asynchronous: bool, failure: str) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.host == "gateway.example":
            return discovery()
        if failure.isdigit():
            return httpx.Response(int(failure), json={"code": 4000, "message": "SYNTHETIC"})
        if failure == "transport":
            raise httpx.ReadTimeout("private transport details")
        data = json.loads(fixture_bytes("operation_pending"))
        data["operation_id" if failure == "id" else "client_request_id"] = (
            "790" if failure == "id" else "other-key"
        )
        return httpx.Response(200, json=data)

    error = (
        APIError
        if failure.isdigit()
        else TransportError
        if failure == "transport"
        else ProtocolError
    )
    ref = OperationReference("789", "synthetic-operation-1")
    with pytest.raises(error):
        if asynchronous:
            async with async_client_for(credentials, handler) as client:
                await client.wait_operation(ref)
        else:
            with client_for(credentials, handler) as sync:
                sync.wait_operation(ref)
    assert calls == 2
    assert _sync_deadline.get() is None


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_invalid_wait_never_sends(credentials: Credentials, asynchronous: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid wait made a request")

    cases = [
        (OperationReference("789"), value, 1.0)
        for value in (0.0, -1.0, float("nan"), float("inf"), True)
    ]
    cases += [
        (OperationReference("789"), 1.0, 0.0),
        (OperationReference(client_request_id="saved-key"), 1.0, 1.0),
    ]
    for ref, timeout, interval in cases:
        with pytest.raises(ValueError):
            if asynchronous:
                async with async_client_for(credentials, handler) as client:
                    await client.wait_operation(ref, timeout=timeout, poll_interval=interval)
            else:
                with client_for(credentials, handler) as sync:
                    sync.wait_operation(ref, timeout=timeout, poll_interval=interval)


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["lock", "discovery", "status", "delay"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_async_deadline_and_cancellation(
    credentials: Credentials, phase: str, cancel: bool
) -> None:
    entered = asyncio.Event()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.host == "gateway.example" and phase != "discovery":
            return discovery()
        entered.set()
        if phase == "delay":
            return httpx.Response(200, content=fixture_bytes("operation_pending"))
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with async_client_for(credentials, handler) as client:
        if phase == "lock":
            await client._discovery_lock.acquire()
            entered.set()
        task = asyncio.create_task(
            client.wait_operation(
                OperationReference("789"), timeout=5 if cancel else 0.2, poll_interval=10
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(OperationWaitTimeout) as caught:
                    await task
                assert (caught.value.last_status is not None) == (phase == "delay")
        finally:
            if phase == "lock":
                client._discovery_lock.release()
        assert calls == {"lock": 0, "discovery": 1, "status": 2, "delay": 2}[phase]


@pytest.mark.anyio
async def test_unrelated_timeout_is_not_wait_timeout(credentials: Credentials) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise TimeoutError("custom transport")

    async with async_client_for(credentials, handler) as client:
        with pytest.raises(TimeoutError) as caught:
            await client.wait_operation(OperationReference("789"))
    assert not isinstance(caught.value, OperationWaitTimeout)


def test_sync_budget_is_local_and_discards_late_success(
    credentials: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [10.0]
    monkeypatch.setattr("exness.client.monotonic", lambda: clock[0])
    monkeypatch.setattr("exness.operations.monotonic", lambda: clock[0])
    seen: list[dict[str, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.extensions["timeout"].copy())
        if request.url.host == "gateway.example":
            clock[0] += 0.25
            return discovery()
        clock[0] += 1
        return httpx.Response(200, content=fixture_bytes("operation_confirmed"))

    with client_for(credentials, handler) as client:
        with pytest.raises(OperationWaitTimeout) as caught:
            client.wait_operation(OperationReference("789"), timeout=1)
        assert caught.value.last_status is None
        assert seen[0]["read"] == 1
        assert seen[1]["read"] == 0.75
        assert client.get_operation_status("789").is_terminal
        assert seen[-1]["read"] == 15
    assert _sync_deadline.get() is None
