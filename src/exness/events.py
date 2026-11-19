"""Account, instrument and HMR subscriptions over the shared signed transport."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Literal, TypeVar

from pydantic import AwareDatetime, Field, ValidationError

from exness.errors import ProtocolError, WebSocketAPIError
from exness.models import (
    AccountState,
    APIModel,
    Identifier,
    Instrument,
    InstrumentConditions,
    WebSocketErrorResponse,
)
from exness.ticks import TickStreamGap, TickStreamOptions, _instruments

ServerStreamOptions = TickStreamOptions
ServerStreamGap = TickStreamGap


class AccountStatePayload(APIModel):
    account_state: AccountState


class AccountStateEvent(APIModel):
    event_type: Literal["account_state_event"]
    event_time: AwareDatetime
    payload: AccountStatePayload


class InstrumentEvent(APIModel):
    event_type: Literal["instrument_event"]
    event_time: AwareDatetime
    action: Literal["new", "upd", "del"]
    payload: InstrumentConditions


class HmrPeriod(APIModel):
    period_id: Identifier
    event_type: Literal["regular", "periodic"]
    instruments: list[Instrument] = Field(min_length=1)
    leverage: str
    # Keep server schedule strings: no timezone or date arithmetic is implied.
    start_time: str
    end_time: str


class HmrSnapshot(APIModel):
    event_type: Literal["hmr_snapshot"]
    periods: list[HmrPeriod]


class HmrUpdate(APIModel):
    event_type: Literal["hmr_update"]
    upserted_periods: list[HmrPeriod]
    removed_period_ids: list[Identifier]


HmrEvent = HmrSnapshot | HmrUpdate
EventT = TypeVar("EventT")


def _subscription_instruments(values: Sequence[str], *, allow_all: bool) -> tuple[str, ...]:
    names = _instruments(values)
    if "all" in names and (not allow_all or names != ("all",)):
        raise ValueError("all is allowed only as the sole instruments subscription name")
    return names


def _event_decoder(
    parse: Callable[[str | bytes], EventT], request_id: str
) -> Callable[[str | bytes], EventT]:
    def decode(message: str | bytes) -> EventT:
        try:
            value = json.loads(message)
            if not isinstance(value, dict):
                raise ValueError("Expected an object")
            if "event_type" not in value and ("code" in value or "error_message" in value):
                error = WebSocketErrorResponse.model_validate_json(message)
                if error.id != request_id:
                    raise ProtocolError("WebSocket error returned an unknown request id")
                raise WebSocketAPIError(error.id, error.code, error.error_message)
            return parse(message)
        except (ValueError, ValidationError):
            raise ProtocolError("Invalid server event message") from None

    return decode


def _account_decoder(request_id: str) -> Callable[[str | bytes], AccountStateEvent]:
    return _event_decoder(AccountStateEvent.model_validate_json, request_id)


def _instrument_decoder(
    names: tuple[str, ...], request_id: str
) -> Callable[[str | bytes], InstrumentEvent]:
    def parse(message: str | bytes) -> InstrumentEvent:
        event = InstrumentEvent.model_validate_json(message)
        if names != ("all",) and event.payload.instrument not in names:
            raise ProtocolError("Instrument event returned an unsubscribed instrument")
        return event

    return _event_decoder(parse, request_id)


def _hmr_decoder(request_id: str) -> Callable[[str | bytes], HmrEvent]:
    have_snapshot = False

    def parse(message: str | bytes) -> HmrEvent:
        nonlocal have_snapshot
        value = json.loads(message)
        if value.get("event_type") == "hmr_snapshot":
            snapshot = HmrSnapshot.model_validate_json(message)
            have_snapshot = True
            return snapshot
        update = HmrUpdate.model_validate_json(message)
        if not have_snapshot:
            raise ProtocolError("HMR update received before HMR snapshot")
        return update

    return _event_decoder(parse, request_id)
