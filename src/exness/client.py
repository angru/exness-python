"""Synchronous, read-only Exness Trading API client."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from types import TracebackType
from typing import Self, TypeVar, get_args

import httpx
from pydantic import ValidationError

from exness.auth import Credentials, _Signer
from exness.errors import APIError, ConfigurationError, ProtocolError, TransportError
from exness.models import (
    AccessPoint,
    AccountDetails,
    APIModel,
    CandleHistory,
    CandlePriceType,
    CandleTimeframe,
    ClosedOrderState,
    DealHistoryItem,
    DealType,
    HistoryPage,
    InstrumentConditions,
    Instruments,
    Limits,
    OperationStatus,
    OrderHistoryItem,
    PublicError,
    TradingSnapshot,
)

ModelT = TypeVar("ModelT", bound=APIModel)
ItemT = TypeVar("ItemT")


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


def _iterate(
    first: Callable[[], HistoryPage[ItemT]],
    following: Callable[[str], HistoryPage[ItemT]],
) -> Iterator[ItemT]:
    seen: set[str] = set()
    page = first()
    while True:
        cursor = page.next_cursor
        if page.has_more is True and not cursor:
            raise ProtocolError("History has_more is true without a continuation cursor")
        if page.has_more is False and cursor:
            raise ProtocolError("History has_more is false with a continuation cursor")
        if cursor and cursor in seen:
            raise ProtocolError("History returned a repeated continuation cursor")
        yield from page.items
        if not cursor:
            return
        seen.add(cursor)
        page = following(cursor)


class ExnessClient:
    """One account per client, lazy cached discovery, no automatic retries.

    The client owns its HTTP connection pool. Use a context manager or close().
    A custom transport is useful for tests; it is also closed with the client.
    """

    def __init__(
        self,
        *,
        credentials: Credentials,
        account_id: str,
        discovery_url: str,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not re.fullmatch(r"[1-9][0-9]{0,19}", account_id) or int(account_id) >= 2**64:
            raise ConfigurationError("account_id must be a positive uint64 decimal string")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ConfigurationError("timeout must be positive and finite")
        self._discovery_url = _base_url(discovery_url)
        self._account_id = account_id
        self._signer = _Signer(credentials)
        self._access_point: str | None = None
        self._http = httpx.Client(
            timeout=timeout, follow_redirects=False, trust_env=False, transport=transport
        )

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _ensure_open(self) -> None:
        if self._http.is_closed:
            raise RuntimeError("ExnessClient is closed")

    def _get(
        self,
        base_url: str,
        path: str,
        model: type[ModelT],
        params: dict[str, str | int] | None = None,
    ) -> ModelT:
        self._ensure_open()
        request = self._http.build_request("GET", base_url + path, params=params)
        request.headers.update(self._signer.get_headers(request.url.raw_path.decode("ascii")))
        try:
            response = self._http.send(request)
        except httpx.RequestError:
            raise TransportError("Exness request failed at the transport layer") from None
        if response.status_code != 200:
            code: int | None = None
            message: str | None = None
            try:
                error = PublicError.model_validate_json(response.content)
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
            return model.model_validate_json(response.content)
        except ValidationError:
            raise ProtocolError(f"Invalid {model.__name__} response") from None

    def discover(self, *, refresh: bool = False) -> str:
        """Resolve and cache the account Access Point; refresh explicitly after migration."""
        self._ensure_open()
        if self._access_point is None or refresh:
            result = self._get(
                self._discovery_url,
                "/v1/trading/access-point",
                AccessPoint,
                {"account_id": self._account_id},
            )
            if result.account_id is not None and result.account_id != self._account_id:
                raise ProtocolError("Discovery returned a different account")
            try:
                access_point = _base_url(result.access_point)
            except ConfigurationError:
                raise ProtocolError("Discovery returned an invalid HTTPS origin") from None
            self._access_point = access_point
        return self._access_point

    def get_account(self) -> AccountDetails:
        result = self._get(
            self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/account",
            AccountDetails,
        )
        if result.account_id != self._account_id:
            raise ProtocolError("Account response returned a different account")
        return result

    def get_instruments(self) -> Instruments:
        return self._get(
            self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/instruments",
            Instruments,
        )

    def get_snapshot(self) -> TradingSnapshot:
        """Read balance/equity, open orders and positions (history contains closed orders)."""
        return self._get(
            self.discover(), f"/v1/trading/accounts/{self._account_id}/snapshot", TradingSnapshot
        )

    def get_limits(self) -> Limits:
        return self._get(
            self.discover(), f"/v1/configuration/accounts/{self._account_id}/limits", Limits
        )

    def get_instrument_conditions(self, instrument: str) -> InstrumentConditions:
        """Read precision, volume constraints, sessions and commissions for one instrument."""
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,11}", instrument) or instrument in (".", ".."):
            raise ValueError("Invalid instrument name for a path segment")
        result = self._get(
            self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/instruments/{instrument}/conditions",
            InstrumentConditions,
        )
        if result.instrument != instrument:
            raise ProtocolError("Conditions response returned a different instrument")
        return result

    def get_operation_status(self, operation_id: str) -> OperationStatus:
        """Read an existing operation once; rejected/failed outcomes are returned as data."""
        if not re.fullmatch(r"[0-9]{1,20}", operation_id) or int(operation_id) >= 2**64:
            raise ValueError("operation_id must be a uint64 decimal string")
        result = self._get(
            self.discover(),
            f"/v1/trading/accounts/{self._account_id}/operations/{operation_id}",
            OperationStatus,
        )
        if int(result.operation_id) != int(operation_id):
            raise ProtocolError("Status response returned a different operation")
        return result

    def get_candle_history(
        self,
        *,
        instrument: str,
        timeframe: CandleTimeframe,
        from_time: datetime,
        to_time: datetime | None = None,
        count: int | None = None,
        price_type: CandlePriceType = "bid",
    ) -> CandleHistory:
        """Read candles with either from+to or from+nonzero signed count.

        Dynamic count/range caps come from get_limits() and are enforced by the
        server. No extra request, splitting, retries or automatic sorting occurs.
        """
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
        return self._get(
            self.discover(),
            f"/v1/market-data/accounts/{self._account_id}/candles",
            CandleHistory,
            params,
        )

    def get_orders_history(
        self,
        *,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
        instrument: str | None = None,
        order_state: ClosedOrderState | None = None,
    ) -> HistoryPage[OrderHistoryItem]:
        if order_state is not None and order_state not in ("filled", "cancelled", "rejected"):
            raise ValueError("Invalid closed order state")
        params = _history_params(
            from_time=from_time,
            to_time=to_time,
            cursor=cursor,
            limit=limit,
            instrument=instrument,
            filter_name="order_state",
            filter_value=order_state,
        )
        return self._get(
            self.discover(),
            f"/v1/history/accounts/{self._account_id}/orders",
            HistoryPage[OrderHistoryItem],
            params,
        )

    def get_deals_history(
        self,
        *,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
        instrument: str | None = None,
        deal_type: DealType | None = None,
    ) -> HistoryPage[DealHistoryItem]:
        if deal_type is not None and deal_type not in (
            "open",
            "close",
            "balance",
            "null_compensation",
            "swap_fee",
            "admin_fee",
            "dividend",
            "exness_dollar",
        ):
            raise ValueError("Invalid deal type")
        params = _history_params(
            from_time=from_time,
            to_time=to_time,
            cursor=cursor,
            limit=limit,
            instrument=instrument,
            filter_name="deal_type",
            filter_value=deal_type,
        )
        return self._get(
            self.discover(),
            f"/v1/history/accounts/{self._account_id}/deals",
            HistoryPage[DealHistoryItem],
            params,
        )

    def iter_orders_history(
        self,
        *,
        from_time: datetime,
        to_time: datetime | None = None,
        limit: int = 100,
        instrument: str | None = None,
        order_state: ClosedOrderState | None = None,
    ) -> Iterator[OrderHistoryItem]:
        return _iterate(
            lambda: self.get_orders_history(
                from_time=from_time,
                to_time=to_time,
                limit=limit,
                instrument=instrument,
                order_state=order_state,
            ),
            lambda cursor: self.get_orders_history(cursor=cursor, limit=limit),
        )

    def iter_deals_history(
        self,
        *,
        from_time: datetime,
        to_time: datetime | None = None,
        limit: int = 100,
        instrument: str | None = None,
        deal_type: DealType | None = None,
    ) -> Iterator[DealHistoryItem]:
        return _iterate(
            lambda: self.get_deals_history(
                from_time=from_time,
                to_time=to_time,
                limit=limit,
                instrument=instrument,
                deal_type=deal_type,
            ),
            lambda cursor: self.get_deals_history(cursor=cursor, limit=limit),
        )
