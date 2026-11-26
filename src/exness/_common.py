"""Shared validation and response decoding for synchronous and asyncio clients."""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from typing import TypeVar, get_args

import httpx
from pydantic import ValidationError

from exness.errors import APIError, ConfigurationError, ProtocolError
from exness.models import (
    AccessPoint,
    APIModel,
    CandlePriceType,
    CandleTimeframe,
    ClosedOrderState,
    DealType,
    HistoryPage,
    PublicError,
)

ModelT = TypeVar("ModelT", bound=APIModel)
ItemT = TypeVar("ItemT")
_MAX_RESPONSE_BYTES = 16 * 1024 * 1024


def _append_response_chunk(body: bytearray, chunk: bytes) -> None:
    if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
        raise ProtocolError("REST response exceeds 16 MiB")
    body.extend(chunk)


def _base_url(value: str) -> str:
    try:
        url = httpx.URL(value)
    except httpx.InvalidURL:
        raise ConfigurationError("Invalid API host URL") from None
    if (
        url.scheme != "https"
        or not url.host
        or url.userinfo
        or url.path != "/"
        or url.query
        or url.fragment
        or "?" in value
        or "#" in value
    ):
        raise ConfigurationError(
            "API host must be an HTTPS origin without credentials, path or query"
        )
    return str(url).rstrip("/")


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("History times must include a timezone")
    return value.astimezone(UTC).isoformat()


def _history_params(
    *,
    from_time: datetime | None,
    to_time: datetime | None,
    cursor: str | None,
    limit: int,
    instrument: str | None,
    filter_name: str,
    filter_value: str | None,
) -> dict[str, str | int]:
    if filter_value is not None:
        if filter_name == "order_state" and filter_value not in get_args(ClosedOrderState):
            raise ValueError("Invalid closed order state")
        if filter_name == "deal_type" and filter_value not in get_args(DealType):
            raise ValueError("Invalid deal type")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ValueError("History limit must be an integer between 1 and 200")
    params: dict[str, str | int] = {"limit": limit}
    if cursor is not None:
        if not cursor or any(v is not None for v in (from_time, to_time, instrument, filter_value)):
            raise ValueError("A nonempty cursor can only be combined with limit")
        params["cursor"] = cursor
        return params
    if from_time is None:
        raise ValueError("from_time is required unless a cursor is provided")
    params["from"] = _utc(from_time)
    if to_time is not None:
        params["to"] = _utc(to_time)
        if to_time <= from_time:
            raise ValueError("to_time must be later than from_time")
    if instrument is not None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,11}", instrument):
            raise ValueError("Invalid instrument name")
        params["instrument"] = instrument
    if filter_value is not None:
        params[filter_name] = filter_value
    return params


def _configuration(account_id: str, discovery_url: str, timeout: float) -> str:
    if not re.fullmatch(r"[1-9][0-9]{0,19}", account_id) or int(account_id) >= 2**64:
        raise ConfigurationError("account_id must be a positive uint64 decimal string")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ConfigurationError("timeout must be positive and finite")
    return _base_url(discovery_url)


def _response(response: httpx.Response, model: type[ModelT], body: bytearray) -> ModelT:
    if response.status_code != 200:
        code: int | None = None
        message: str | None = None
        try:
            error = PublicError.model_validate_json(body)
            code, message = error.code, error.error_message
        except ValidationError:
            pass
        raise APIError(
            response.status_code,
            code=code,
            error_message=message,
            retry_after=response.headers.get("Retry-After"),
        )
    try:
        return model.model_validate_json(body)
    except ValidationError:
        raise ProtocolError(f"Invalid {model.__name__} response") from None


def _discovered_host(result: AccessPoint, account_id: str) -> str:
    if result.account_id is not None and result.account_id != account_id:
        raise ProtocolError("Discovery returned a different account")
    try:
        access_point = _base_url(result.access_point)
    except ConfigurationError:
        raise ProtocolError("Discovery returned an invalid HTTPS origin") from None
    return access_point


def _continuation(page: HistoryPage[ItemT], seen: set[str]) -> str | None:
    cursor = page.next_cursor
    if page.has_more is True and not cursor:
        raise ProtocolError("History has_more is true without a continuation cursor")
    if page.has_more is False and cursor:
        raise ProtocolError("History has_more is false with a continuation cursor")
    if cursor and cursor in seen:
        raise ProtocolError("History returned a repeated continuation cursor")
    return cursor


def _instrument_path(instrument: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,11}", instrument) or instrument in (".", ".."):
        raise ValueError("Invalid instrument name for a path segment")


def _operation_id(operation_id: str) -> None:
    if not re.fullmatch(r"[0-9]{1,20}", operation_id) or int(operation_id) >= 2**64:
        raise ValueError("operation_id must be a uint64 decimal string")


def _candle_params(
    *,
    instrument: str,
    timeframe: CandleTimeframe,
    from_time: datetime,
    to_time: datetime | None,
    count: int | None,
    price_type: CandlePriceType,
) -> dict[str, str | int]:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,11}", instrument):
        raise ValueError("Invalid instrument name")
    if timeframe not in get_args(CandleTimeframe):
        raise ValueError("Invalid candle timeframe")
    if price_type not in get_args(CandlePriceType):
        raise ValueError("Invalid candle price_type")
    if (to_time is None) == (count is None):
        raise ValueError("Provide exactly one of to_time or count")
    params: dict[str, str | int] = {
        "instrument": instrument,
        "timeframe": timeframe,
        "from": _utc(from_time),
        "price_type": price_type,
    }
    if to_time is not None:
        params["to"] = _utc(to_time)
        if to_time <= from_time:
            raise ValueError("to_time must be later than from_time")
    if count is not None:
        if isinstance(count, bool) or not isinstance(count, int) or count == 0:
            raise ValueError("count must be a nonzero integer")
        params["count"] = count
    return params
