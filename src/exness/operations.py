"""Explicit operation identity and bounded waiting support; never submits writes."""

from __future__ import annotations

import math
from contextvars import ContextVar
from dataclasses import dataclass
from time import monotonic

import httpx

from exness._common import _correlates
from exness.auth import _idempotency_key
from exness.errors import ProtocolError
from exness.models import AckResponse, OperationStatus
from exness.requests import _request_identifier
from exness.trading import TransactionEvent


@dataclass(frozen=True, repr=False)
class OperationReference:
    """Identity within one account. A key-only reference can match events, not poll."""

    operation_id: str | None = None
    client_request_id: str | None = None

    def __post_init__(self) -> None:
        if self.operation_id is None and self.client_request_id is None:
            raise ValueError("Operation identity is required")
        if self.operation_id is not None:
            _request_identifier(self.operation_id)
        if self.client_request_id is not None:
            _idempotency_key(self.client_request_id)

    @classmethod
    def from_ack(
        cls, ack: AckResponse, *, client_request_id: str | None = None
    ) -> OperationReference:
        """Use the persisted caller key when the ACK omits its echo."""
        if ack.status != "accepted":
            raise ProtocolError("Invalid acknowledgement status")
        if (
            client_request_id is not None
            and ack.client_request_id is not None
            and client_request_id != ack.client_request_id
        ):
            raise ProtocolError("Acknowledgement client request ID mismatch")
        return cls(
            ack.operation_id,
            client_request_id if client_request_id is not None else ack.client_request_id,
        )

    def matches_status(self, status: OperationStatus) -> bool:
        """Match identity, raising ProtocolError for contradictory matching evidence."""
        return _correlates(
            self.operation_id, self.client_request_id, status.operation_id, status.client_request_id
        )

    def matches_event(self, event: TransactionEvent) -> bool:
        """Match identity only; an event without status does not prove completion."""
        return _correlates(
            self.operation_id, self.client_request_id, event.operation_id, event.client_request_id
        )


def _wait_arguments(reference: OperationReference, timeout: float, interval: float) -> str:
    if reference.operation_id is None:
        raise ValueError("Status polling requires an operation ID")
    if any(
        isinstance(value, bool) or not math.isfinite(value) or value <= 0
        for value in (timeout, interval)
    ):
        raise ValueError("Wait timeout and poll interval must be finite and positive")
    return reference.operation_id


# Only synchronous waiters set this context-local budget. Never mutate the shared pool.
_sync_deadline: ContextVar[float | None] = ContextVar("exness_operation_deadline", default=None)


class _DeadlineExpired(Exception):
    pass


def _remaining() -> float | None:
    deadline = _sync_deadline.get()
    if deadline is None:
        return None
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise _DeadlineExpired
    return remaining


def _limit_request(request: httpx.Request) -> None:
    remaining = _remaining()
    if remaining is not None:
        request.extensions["timeout"] = {
            phase: remaining if value is None else min(value, remaining)
            for phase, value in request.extensions["timeout"].items()
        }
