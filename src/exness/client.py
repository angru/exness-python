"""Synchronous, read-only Exness Trading API client."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime
from types import TracebackType
from typing import Self, TypeVar

import httpx

from exness._common import (
    _candle_params,
    _configuration,
    _continuation,
    _discovered_host,
    _history_params,
    _instrument_path,
    _operation_id,
    _response,
)
from exness.auth import Credentials, _Signer
from exness.errors import ProtocolError, TransportError
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
    TradingSnapshot,
)

ModelT = TypeVar("ModelT", bound=APIModel)
ItemT = TypeVar("ItemT")


def _iterate(
    first: Callable[[], HistoryPage[ItemT]],
    following: Callable[[str], HistoryPage[ItemT]],
) -> Iterator[ItemT]:
    seen: set[str] = set()
    page = first()
    while True:
        cursor = _continuation(page, seen)
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
        self._discovery_url = _configuration(account_id, discovery_url, timeout)
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
        return _response(response, model)

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
            self._access_point = _discovered_host(result, self._account_id)
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
        _instrument_path(instrument)
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
        _operation_id(operation_id)
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
        params = _candle_params(
            instrument=instrument,
            timeframe=timeframe,
            from_time=from_time,
            to_time=to_time,
            count=count,
            price_type=price_type,
        )
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
