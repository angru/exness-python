"""Typed response models for the supported portion of Trading API 1.0.2.

Unknown response fields are preserved for forward compatibility. Monetary values
and volumes are Decimal, never binary floats. Optional fields remain optional.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    FiniteFloat,
    field_validator,
)


def _decimal_string(value: object) -> Decimal:
    if isinstance(value, Decimal) and value.is_finite():
        return value
    if isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", value):
        return Decimal(value)
    raise ValueError("Expected a finite decimal string in plain dot notation")


DecimalValue = Annotated[Decimal, BeforeValidator(_decimal_string)]


def _uint64(value: str) -> str:
    if int(value) >= 2**64:
        raise ValueError("Identifier must fit uint64")
    return value


def _nonnull(value: object) -> object:
    if value is None:
        raise ValueError("Omit this field instead of supplying null")
    return value


Identifier = Annotated[str, Field(pattern=r"^[0-9]{1,20}$"), AfterValidator(_uint64)]
Instrument = Annotated[str, Field(min_length=1, max_length=11, pattern=r"^[A-Za-z0-9._-]+$")]
PositiveInt = Annotated[int, Field(ge=1)]
Direction = Literal["buy", "sell"]
ClosedOrderState = Literal["filled", "cancelled", "rejected"]
DealType = Literal[
    "open",
    "close",
    "balance",
    "null_compensation",
    "swap_fee",
    "admin_fee",
    "dividend",
    "exness_dollar",
]
HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]


class APIModel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True, strict=True, hide_input_in_errors=True)


class AccessPoint(APIModel):
    access_point: str
    account_id: Identifier | None = None


class AckResponse(APIModel):
    """Admission acknowledgement, not execution success; operation ID may be absent."""

    status: Literal["accepted"]
    operation_id: Identifier | None = None
    client_request_id: str | None = None

    @field_validator("operation_id", "client_request_id")
    @classmethod
    def _present_fields(cls, value: object) -> object:
        return _nonnull(value)


class PublicError(APIModel):
    code: int
    error_message: str


class AccountSettings(APIModel):
    currency: str
    leverage: str
    account_status: Literal["active", "close_only"]
    trade_mode: Literal["enabled", "trading_disabled"]
    execution_mode: Literal["undefined", "instant", "market"]
    margin_call_level: DecimalValue | None = None
    stopout_level: DecimalValue | None = None


class AccountDetails(APIModel):
    account_id: Identifier
    create_date: AwareDatetime
    account_name: str | None = None
    settings: AccountSettings | None = None


class AccountState(APIModel):
    balance: DecimalValue
    equity: DecimalValue
    used_margin: DecimalValue


class Instruments(APIModel):
    instruments: list[Instrument] = Field(default_factory=list)


class Order(APIModel):
    order_id: Identifier
    order_type: Literal["market", "stop", "limit", "sltp"]
    state: Literal["pending_new", "placed", "filled", "cancelled", "rejected"]
    create_time: AwareDatetime
    position_id: Identifier | None = None
    direction: Direction | None = None
    instrument: Instrument | None = None
    price: DecimalValue | None = None
    stop_loss_price: DecimalValue | None = None
    take_profit_price: DecimalValue | None = None
    update_time: AwareDatetime | None = None
    volume: DecimalValue | None = None
    reason: (
        Literal[
            "undefined",
            "sl",
            "tp",
            "so",
            "margin_check_failed",
            "dealer",
            "close_by",
            "stock_split",
            "trading_server",
        ]
        | None
    ) = None

    @field_validator("position_id", "direction")
    @classmethod
    def _present_fields(cls, value: object) -> object:
        return _nonnull(value)


class Position(APIModel):
    position_id: Identifier
    direction: Direction
    instrument: Instrument
    state: Literal["open", "closed"]
    create_time: AwareDatetime
    volume: DecimalValue
    initial_volume: DecimalValue | None = None
    open_price: DecimalValue | None = None
    margin_rate: DecimalValue | None = None
    commission: DecimalValue | None = None
    swap: DecimalValue | None = None
    rollover_time: AwareDatetime | None = None


class TradingSnapshot(APIModel):
    account_state: AccountState
    orders: list[Order] = Field(default_factory=list)
    positions: list[Position] = Field(default_factory=list)


class ProfitAmount(APIModel):
    amount: DecimalValue | None = None
    currency: str | None = None


class Deal(APIModel):
    deal_id: Identifier
    type: DealType
    create_time: AwareDatetime
    order_id: Identifier | None = None
    position_id: Identifier | None = None
    direction: Direction | None = None
    instrument: Instrument | None = None
    volume: DecimalValue | None = None
    price: DecimalValue | None = None
    profit: ProfitAmount | None = None
    commission: DecimalValue | None = None

    @field_validator("order_id", "position_id", "direction")
    @classmethod
    def _present_fields(cls, value: object) -> object:
        return _nonnull(value)


class OrderHistoryItem(APIModel):
    event_time: AwareDatetime
    order: Order


class DealHistoryItem(APIModel):
    event_time: AwareDatetime
    deal: Deal


class HistoryRange(APIModel):
    from_time: AwareDatetime | None = Field(default=None, alias="from")
    to_time: AwareDatetime | None = Field(default=None, alias="to")


ItemT = TypeVar("ItemT")


class HistoryPage(APIModel, Generic[ItemT]):
    items: list[ItemT]
    next_cursor: str | None = None
    has_more: bool | None = None
    range: HistoryRange | None = None


class RateLimit(APIModel):
    limit: PositiveInt
    window_seconds: PositiveInt


class RequestLimits(APIModel):
    max_candles_per_request: PositiveInt | None = None
    max_candles_per_request_description: str | None = None
    history_range_days: PositiveInt | None = None
    history_age_days: PositiveInt | None = None


class RestMethodLimits(APIModel):
    api_operation_id: str
    http_method: HttpMethod
    path: str
    rate_limit: RateLimit
    limits: RequestLimits | None = None


class RestLimits(APIModel):
    global_account_rate: RateLimit
    methods: list[RestMethodLimits]


class TokenBucketLimit(APIModel):
    limit: PositiveInt
    window_seconds: Annotated[float, Field(gt=0)]
    burst: PositiveInt


class StrikeLimits(APIModel):
    refill_amount: PositiveInt
    threshold: PositiveInt
    period_ms: PositiveInt


class WebSocketAccountLimits(APIModel):
    max_active_connections: PositiveInt
    max_inbound_message_bytes: PositiveInt
    connection_rate: TokenBucketLimit
    inbound_message_rate: TokenBucketLimit
    strikes: StrikeLimits


class WebSocketEndpointLimits(APIModel):
    api_operation_id: str
    http_method: HttpMethod
    path: str
    connection_rate: TokenBucketLimit
    inbound_message_rate: TokenBucketLimit
    subscription_operation_rate: dict[str, TokenBucketLimit]


class WebSocketLimits(APIModel):
    global_account_limits: WebSocketAccountLimits
    endpoints: list[WebSocketEndpointLimits]


class ApiLimits(APIModel):
    rest: RestLimits
    websocket: WebSocketLimits


class Limits(APIModel):
    limits: ApiLimits


CandleTimeframe = Literal[
    "S1",
    "S5",
    "S15",
    "M1",
    "M3",
    "M5",
    "M10",
    "M15",
    "M30",
    "H1",
    "H2",
    "H4",
    "D1",
    "W1",
    "MN",
]
CandlePriceType = Literal["bid", "ask", "both"]
OperationState = Literal["pending", "confirmed", "rejected", "failed"]
Int32 = Annotated[int, Field(ge=-(2**31), le=2**31 - 1)]


class CandleSide(APIModel):
    open: FiniteFloat | None = None
    high: FiniteFloat | None = None
    low: FiniteFloat | None = None
    close: FiniteFloat | None = None


class Candle(CandleSide):
    """Single-side OHLC or nested bid/ask; fields absent on the wire stay None.

    The upstream oneOf is ambiguous. Keep both documented shapes without guessing
    a discriminator or dropping a side. See contracts/README.md.
    """

    time: AwareDatetime | None = None
    volume: FiniteFloat | None = None
    bid: CandleSide | None = None
    ask: CandleSide | None = None


class CandleHistory(APIModel):
    candles: list[Candle] = Field(default_factory=list)


class SwapMultiplier(APIModel):
    day: str | None = None
    multiplier: Int32 | None = None


class Money(APIModel):
    amount: DecimalValue | None = None
    currency: str | None = None


class CommissionSettings(APIModel):
    commission_type: (
        Literal["undefined", "per_lot_per_side", "per_lot_per_trade", "per_million_usd"] | None
    ) = None
    commission_value: Money | None = None


class InstrumentSession(APIModel):
    trade_mode: Literal["enabled", "trading_disabled"] | None = None
    week_start_time: str | None = None
    week_end_time: str | None = None


class InstrumentConditions(APIModel):
    instrument: Instrument
    description: str | None = None
    international_name: str | None = None
    category: str | None = None
    base_currency: str | None = None
    quote_currency: str | None = None
    margin_currency: str | None = None
    point_digits: Int32 | None = None
    calc_mode: Literal["undefined", "forex", "forex_no_leverage", "cfd", "cfd_leverage"] | None = (
        None
    )
    exec_mode: Literal["undefined", "instant", "market"] | None = None
    margin_percent: DecimalValue | None = None
    contract_size: DecimalValue | None = None
    volume_min: DecimalValue | None = None
    volume_max: DecimalValue | None = None
    volume_step: DecimalValue | None = None
    swap_long: DecimalValue | None = None
    swap_short: DecimalValue | None = None
    swap_multipliers: list[SwapMultiplier] | None = None
    trade_mode: Literal["enabled", "trading_disabled"] | None = None
    order_types: list[Literal["stop", "limit", "stoplimit", "market"]] | None = None
    filling_modes: list[Literal["fok", "ioc", "boc"]] | None = None
    trade_sessions: list[InstrumentSession] | None = None
    commission_settings: CommissionSettings | None = None


class OperationStatus(APIModel):
    operation_id: Identifier
    status: OperationState
    created_at: AwareDatetime
    updated_at: AwareDatetime
    client_request_id: str | None = None
    error_code: Int32 | None = None
    error_message: str | None = None

    @field_validator("client_request_id", "error_code", "error_message")
    @classmethod
    def _present_fields(cls, value: object) -> object:
        return _nonnull(value)

    @property
    def is_terminal(self) -> bool:
        """A terminal outcome can be success, rejection or failure."""
        return self.status != "pending"


class Tick(APIModel):
    """Market-data doubles; missing fields remain None as in WSTick."""

    instrument: Instrument | None = None
    bid: FiniteFloat | None = None
    ask: FiniteFloat | None = None
    timestamp: AwareDatetime | None = None


class WebSocketErrorResponse(APIModel):
    id: str
    code: int
    error_message: str
