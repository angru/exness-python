"""Asyncio, Exness Trading API client."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from datetime import datetime
from types import TracebackType
from typing import Self, TypeVar

import httpx
from websockets.asyncio.client import ClientConnection

from exness._common import (
    _ack_response,
    _append_response_chunk,
    _candle_params,
    _configuration,
    _continuation,
    _discovered_host,
    _history_params,
    _instrument_path,
    _operation_id,
    _response,
)
from exness.auth import Credentials, _idempotency_key, _Signer
from exness.errors import APIError, ExnessError, ProtocolError, TransportError, UnknownOutcomeError
from exness.events import (
    AccountStateEvent,
    HmrEvent,
    InstrumentEvent,
    ServerStreamGap,
    ServerStreamOptions,
    _account_decoder,
    _hmr_decoder,
    _instrument_decoder,
    _subscription_instruments,
)
from exness.models import (
    AccessPoint,
    AccountDetails,
    AckResponse,
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
    Tick,
    TradingSnapshot,
)
from exness.requests import (
    ClosePositionRequest,
    ModifyOrderRequest,
    OpenPositionRequest,
    PendingOrderRequest,
    _request_identifier,
)
from exness.ticks import TickStreamGap, TickStreamOptions, _stream, stream_ticks
from exness.trading import TradingEvent, TradingStreamGap, TradingStreamOptions, stream_trading

ModelT = TypeVar("ModelT", bound=APIModel)
ItemT = TypeVar("ItemT")
ResultT = TypeVar("ResultT")


async def _iterate(
    first: Callable[[], Awaitable[HistoryPage[ItemT]]],
    following: Callable[[str], Awaitable[HistoryPage[ItemT]]],
) -> AsyncGenerator[ItemT, None]:
    seen: set[str] = set()
    page = await first()
    while True:
        cursor = _continuation(page, seen)
        for item in page.items:
            yield item
        if not cursor:
            return
        seen.add(cursor)
        page = await following(cursor)


class AsyncExnessClient:
    """One account per client, lazy cached discovery, no automatic retries.

    The client owns its HTTP connection pool. Use an async context manager or await aclose().
    Share only among tasks in one asyncio event loop; do not use across threads.
    Closure interrupts stream waits; cancel and await active REST tasks before closing.
    A custom transport is useful for tests; it is also closed with the client.
    """

    def __init__(
        self,
        *,
        credentials: Credentials,
        account_id: str,
        discovery_url: str,
        timeout: float = 15.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._discovery_url = _configuration(account_id, discovery_url, timeout)
        self._account_id = account_id
        self._signer = _Signer(credentials)
        self._access_point: str | None = None
        self._discovery_lock = asyncio.Lock()
        self._closing = False
        self._stream_tasks: set[asyncio.Task[object]] = set()
        self._ws_connections: set[ClientConnection] = set()
        self._http = httpx.AsyncClient(
            timeout=timeout, follow_redirects=False, trust_env=False, transport=transport
        )

    async def __aenter__(self) -> Self:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        # Refuse new work before the first await, including during socket cleanup.
        self._closing = True
        try:
            tasks = tuple(self._stream_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(
                *(connection.close() for connection in tuple(self._ws_connections))
            )
        finally:
            await self._http.aclose()

    def _ensure_open(self) -> None:
        if self._closing or self._http.is_closed:
            raise RuntimeError("AsyncExnessClient is closed")

    async def _stream_wait(self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Own only the SDK wait, never the task consuming a yielded event."""
        self._ensure_open()

        async def run() -> ResultT:
            return await operation()

        task = asyncio.create_task(run())
        self._stream_tasks.add(task)
        try:
            try:
                result = await task
            except asyncio.CancelledError:
                consumer = asyncio.current_task()
                if consumer is not None and not consumer.cancelling():
                    self._ensure_open()
                raise
            self._ensure_open()
            return result
        finally:
            self._stream_tasks.discard(task)

    async def _get(
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
            response = await self._http.send(request, stream=True)
            try:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    _append_response_chunk(body, chunk)
                return _response(response, model, body)
            finally:
                await response.aclose()
        except httpx.RequestError:
            raise TransportError("Exness request failed at the transport layer") from None

    async def _submit(
        self,
        method: str,
        resource: str,
        body: bytes,
        idempotency_key: str,
        params: dict[str, str] | None = None,
    ) -> AckResponse:
        self._ensure_open()
        _idempotency_key(idempotency_key)
        host = await self.discover()
        self._ensure_open()
        path = f"/v1/trading/accounts/{self._account_id}/{resource}"
        request = self._http.build_request(method, host + path, content=body, params=params)
        request.headers.update(
            self._signer.request_headers(
                method,
                request.url.raw_path.decode("ascii"),
                body=request.content,
                idempotency_key=idempotency_key,
            )
        )
        request.headers["Accept"] = "application/json"
        if body:
            request.headers["Content-Type"] = "application/json"
        try:
            response = await self._http.send(request, stream=True)
            try:
                received = bytearray()
                async for chunk in response.aiter_bytes():
                    _append_response_chunk(received, chunk)
                return _ack_response(response, received, idempotency_key)
            finally:
                await response.aclose()
        except APIError as error:
            if 400 <= error.status_code < 500 and error.status_code != 408:
                raise
            cause: ExnessError = error
        except ProtocolError as error:
            cause = error
        except Exception:
            # Once transport starts, even a connection failure cannot prove non-delivery.
            # CancelledError is a BaseException and continues to propagate unchanged.
            cause = TransportError("Exness request failed at the transport layer")
        raise UnknownOutcomeError(
            method=method,
            path=path,
            account_id=self._account_id,
            idempotency_key=idempotency_key,
            cause=cause,
        ) from None

    async def open_position(
        self, request: OpenPositionRequest, *, idempotency_key: str
    ) -> AckResponse:
        """Submit once; accepted acknowledges admission, not execution."""
        body = request.to_body()
        return await self._submit("POST", "positions", body, idempotency_key)

    async def place_pending_order(
        self, request: PendingOrderRequest, *, idempotency_key: str
    ) -> AckResponse:
        """Submit one limit, stop or SLTP order; no automatic retry."""
        body = request.to_body()
        return await self._submit("POST", "orders", body, idempotency_key)

    async def modify_order(
        self, order_id: str, request: ModifyOrderRequest, *, idempotency_key: str
    ) -> AckResponse:
        """Omitted SL/TP fields are unchanged; explicit None removes them."""
        _request_identifier(order_id)
        body = request.to_body()
        return await self._submit("PATCH", f"orders/{order_id}", body, idempotency_key)

    async def cancel_pending_order(self, order_id: str, *, idempotency_key: str) -> AckResponse:
        """Request cancellation once; the returned ACK is not a final result."""
        _request_identifier(order_id)
        return await self._submit("DELETE", f"orders/{order_id}", b"", idempotency_key)

    async def close_position(
        self, position_id: str, request: ClosePositionRequest, *, idempotency_key: str
    ) -> AckResponse:
        """Request full/partial/close-by closure once, using signed query parameters."""
        _request_identifier(position_id)
        params = request.to_query()
        return await self._submit(
            "DELETE", f"positions/{position_id}", b"", idempotency_key, params
        )

    async def discover(self, *, refresh: bool = False) -> str:
        """Resolve and cache the account Access Point; refresh explicitly after migration."""
        self._ensure_open()
        if self._access_point is not None and not refresh:
            return self._access_point
        async with self._discovery_lock:
            self._ensure_open()
            if self._access_point is not None and not refresh:
                return self._access_point
            result = await self._get(
                self._discovery_url,
                "/v1/trading/access-point",
                AccessPoint,
                {"account_id": self._account_id},
            )
            self._ensure_open()
            self._access_point = _discovered_host(result, self._account_id)
            return self._access_point

    async def get_account(self) -> AccountDetails:
        result = await self._get(
            await self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/account",
            AccountDetails,
        )
        if result.account_id != self._account_id:
            raise ProtocolError("Account response returned a different account")
        return result

    async def get_instruments(self) -> Instruments:
        return await self._get(
            await self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/instruments",
            Instruments,
        )

    async def get_snapshot(self) -> TradingSnapshot:
        """Read balance/equity, open orders and positions (history contains closed orders)."""
        return await self._get(
            await self.discover(),
            f"/v1/trading/accounts/{self._account_id}/snapshot",
            TradingSnapshot,
        )

    async def get_limits(self) -> Limits:
        return await self._get(
            await self.discover(), f"/v1/configuration/accounts/{self._account_id}/limits", Limits
        )

    async def get_instrument_conditions(self, instrument: str) -> InstrumentConditions:
        """Read precision, volume constraints, sessions and commissions for one instrument."""
        _instrument_path(instrument)
        result = await self._get(
            await self.discover(),
            f"/v1/configuration/accounts/{self._account_id}/instruments/{instrument}/conditions",
            InstrumentConditions,
        )
        if result.instrument != instrument:
            raise ProtocolError("Conditions response returned a different instrument")
        return result

    async def get_operation_status(self, operation_id: str) -> OperationStatus:
        """Read an existing operation once; rejected/failed outcomes are returned as data."""
        _operation_id(operation_id)
        result = await self._get(
            await self.discover(),
            f"/v1/trading/accounts/{self._account_id}/operations/{operation_id}",
            OperationStatus,
        )
        if int(result.operation_id) != int(operation_id):
            raise ProtocolError("Status response returned a different operation")
        return result

    async def get_candle_history(
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
        return await self._get(
            await self.discover(),
            f"/v1/market-data/accounts/{self._account_id}/candles",
            CandleHistory,
            params,
        )

    async def get_orders_history(
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
        return await self._get(
            await self.discover(),
            f"/v1/history/accounts/{self._account_id}/orders",
            HistoryPage[OrderHistoryItem],
            params,
        )

    async def get_deals_history(
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
        return await self._get(
            await self.discover(),
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
    ) -> AsyncGenerator[OrderHistoryItem, None]:
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
    ) -> AsyncGenerator[DealHistoryItem, None]:
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

    def stream_ticks(
        self,
        instruments: Sequence[str],
        *,
        options: TickStreamOptions | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> AsyncGenerator[Tick | TickStreamGap, None]:
        """Read live ticks. Use aclosing() when stopping iteration early.

        Reconnects are opt-in and emit TickStreamGap before every retry.
        TLS customization applies to WebSocket connections only.
        """
        return stream_ticks(self, instruments, options or TickStreamOptions(), ssl_context)

    def stream_trading(
        self,
        *,
        options: TradingStreamOptions | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> AsyncGenerator[TradingEvent | TradingStreamGap, None]:
        """Subscribe to transactions; every connection requires a fresh snapshot.

        A gap invalidates the caller's cached state. Replace it with the next
        snapshot before applying transaction deltas. Use aclosing() on early exit.
        """
        return stream_trading(self, options or TradingStreamOptions(), ssl_context)

    def stream_account_state(
        self,
        *,
        options: ServerStreamOptions | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> AsyncGenerator[AccountStateEvent | ServerStreamGap, None]:
        """Periodic floating state; no initial snapshot or transaction history.

        Use aclosing() on early exit. A gap invalidates the last observed value.
        """
        return _stream(
            self,
            "events",
            {"event": "account_state"},
            options or ServerStreamOptions(),
            ssl_context,
            _account_decoder,
        )

    def stream_instruments(
        self,
        instruments: Sequence[str],
        *,
        options: ServerStreamOptions | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> AsyncGenerator[InstrumentEvent | ServerStreamGap, None]:
        """Full instrument updates; ['all'] is supported, with no initial snapshot.

        Lists are copied. A gap requires explicit cache reconciliation by the caller.
        """
        names = _subscription_instruments(instruments, allow_all=True)
        return _stream(
            self,
            "events",
            {"event": "instruments", "instruments": names},
            options or ServerStreamOptions(),
            ssl_context,
            lambda request_id: _instrument_decoder(names, request_id),
        )

    def stream_hmr(
        self,
        instruments: Sequence[str],
        *,
        options: ServerStreamOptions | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> AsyncGenerator[HmrEvent | ServerStreamGap, None]:
        """HMR periods: replace state on every snapshot, apply deltas only afterward.

        Requires explicit instruments (no 'all'). Each connection needs a new baseline.
        """
        names = _subscription_instruments(instruments, allow_all=False)
        return _stream(
            self,
            "events",
            {"event": "hmr", "instruments": names},
            options or ServerStreamOptions(),
            ssl_context,
            _hmr_decoder,
        )
