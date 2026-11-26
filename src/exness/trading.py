"""Typed transactions stream with an explicit snapshot boundary on every connection."""

from __future__ import annotations

import json
import ssl
from collections.abc import AsyncGenerator, Callable
from typing import TYPE_CHECKING, Literal

from pydantic import AwareDatetime, Field, ValidationError, field_validator

from exness.errors import ProtocolError, WebSocketAPIError
from exness.models import (
    AccountState,
    APIModel,
    Deal,
    Identifier,
    Int32,
    Order,
    Position,
    TradingSnapshot,
    WebSocketErrorResponse,
    _nonnull,
)
from exness.ticks import TickStreamGap, TickStreamOptions, _stream

if TYPE_CHECKING:
    from exness.async_client import AsyncExnessClient

# Both streams intentionally share their transport policy and gap representation.
TradingStreamOptions = TickStreamOptions
TradingStreamGap = TickStreamGap


class TransactionAccountState(AccountState):
    leverage: str


class TransactionPayload(APIModel):
    orders: list[Order] = Field(default_factory=list)
    positions: list[Position] = Field(default_factory=list)
    deals: list[Deal] = Field(default_factory=list)
    account_state: TransactionAccountState | None = None

    @field_validator("account_state", mode="before")
    @classmethod
    def nonnull_account_state(cls, value: object) -> object:
        if value is None:
            raise ValueError("account_state may be absent, but not null")
        return value


class TradingSnapshotEvent(APIModel):
    event_type: Literal["trading_state_snapshot"]
    payload: TradingSnapshot


class TransactionEvent(APIModel):
    event_type: Literal["transaction_event"]
    payload: TransactionPayload
    operation_id: Identifier | None = None
    client_request_id: str | None = None
    status: Literal["success", "rejected", "failed"] | None = None
    error_code: Int32 | None = None
    error_message: str | None = None
    event_time: AwareDatetime | None = None

    @field_validator("operation_id", "status", "error_code", "error_message", "event_time")
    @classmethod
    def _present_fields(cls, value: object) -> object:
        # client_request_id explicitly allows null in the pinned contract.
        return _nonnull(value)


TradingEvent = TradingSnapshotEvent | TransactionEvent


def _trading_decoder(request_id: str) -> Callable[[str | bytes], TradingEvent]:
    have_snapshot = False

    def decode(message: str | bytes) -> TradingEvent:
        nonlocal have_snapshot
        try:
            value = json.loads(message)
            if not isinstance(value, dict):
                raise ValueError("Expected an object")
            # Transaction rejections legitimately contain error_message. They are
            # data events, not subscription errors; dispatch by event_type first.
            kind = value.get("event_type")
            if kind == "trading_state_snapshot":
                snapshot = TradingSnapshotEvent.model_validate_json(message)
                have_snapshot = True
                return snapshot
            if kind == "transaction_event":
                event = TransactionEvent.model_validate_json(message)
                if not have_snapshot:
                    raise ProtocolError("Transaction received before trading snapshot")
                return event
            if kind is None and ("code" in value or "error_message" in value):
                error = WebSocketErrorResponse.model_validate_json(message)
                if error.id != request_id:
                    raise ProtocolError("WebSocket error returned an unknown request id")
                raise WebSocketAPIError(error.id, error.code, error.error_message)
            raise ProtocolError("Unexpected transactions stream event")
        except (ValueError, ValidationError, RecursionError):
            raise ProtocolError("Invalid transactions stream message") from None

    return decode


def stream_trading(
    client: AsyncExnessClient,
    options: TradingStreamOptions,
    ssl_context: ssl.SSLContext | None,
) -> AsyncGenerator[TradingEvent | TradingStreamGap, None]:
    return _stream(
        client, "events", {"event": "transactions"}, options, ssl_context, _trading_decoder
    )
