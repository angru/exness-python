"""Validated trading bodies and query parameters; encoding performs no network I/O."""

import re
from decimal import Decimal
from typing import Annotated, ClassVar, Literal, Self, cast

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, PlainSerializer, model_validator

from exness.models import DecimalValue, Direction, Instrument


def _plain_decimal(value: Decimal) -> str:
    return format(value, "f")


_RequestDecimal = Annotated[
    DecimalValue, PlainSerializer(_plain_decimal, return_type=str, when_used="json")
]
_PositiveDecimal = Annotated[_RequestDecimal, Field(gt=0)]


def _request_identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,20}", value) or int(value) >= 2**64:
        raise ValueError("Trading identifier must be a uint64 decimal string")
    return value


_RequestIdentifier = Annotated[str, AfterValidator(_request_identifier)]


class _Request(BaseModel):
    _nullable: ClassVar[frozenset[str]] = frozenset()
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
        revalidate_instances="always",
    )

    @model_validator(mode="after")
    def _reject_explicit_null(self) -> Self:
        if any(getattr(self, name) is None for name in self.model_fields_set - self._nullable):
            raise ValueError("Omit optional request fields instead of sending null")
        return self

    def to_body(self) -> bytes:
        """Return validated UTF-8 JSON to sign and send unchanged."""
        validated = type(self).model_validate(self)
        omitted = set(type(self).model_fields) - validated.model_fields_set - {"type"}
        return validated.model_dump_json(exclude=omitted).encode("utf-8")


class OpenPositionRequest(_Request):
    """Opening request. Omit optional fields; prices/volume are positive exact decimals."""

    instrument: Instrument
    side: Direction
    volume: _PositiveDecimal
    price: _PositiveDecimal | None = None
    deviation: _RequestDecimal | None = None
    stop_loss_price: _PositiveDecimal | None = None
    take_profit_price: _PositiveDecimal | None = None
    comment: str | None = None


class _PendingOrder(_Request):
    instrument: Instrument
    side: Direction
    volume: _PositiveDecimal
    price: _PositiveDecimal
    stop_loss_price: _PositiveDecimal | None = None
    take_profit_price: _PositiveDecimal | None = None
    comment: str | None = None


class LimitOrderRequest(_PendingOrder):
    type: Literal["limit"] = "limit"


class StopOrderRequest(_PendingOrder):
    type: Literal["stop"] = "stop"


class SltpOrderRequest(_Request):
    position_id: _RequestIdentifier
    type: Literal["sltp"] = "sltp"
    stop_loss_price: _PositiveDecimal | None = None
    take_profit_price: _PositiveDecimal | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def _require_level(self) -> Self:
        if self.stop_loss_price is None and self.take_profit_price is None:
            raise ValueError("SLTP requires at least one stop-loss or take-profit price")
        return self


PendingOrderRequest = LimitOrderRequest | StopOrderRequest | SltpOrderRequest


class ModifyOrderRequest(_Request):
    """Omit SL/TP to preserve it; pass None to clear it, or Decimal to replace it."""

    _nullable: ClassVar[frozenset[str]] = frozenset({"stop_loss_price", "take_profit_price"})
    price: _PositiveDecimal | None = None
    stop_loss_price: _PositiveDecimal | None = None
    take_profit_price: _PositiveDecimal | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def _require_change(self) -> Self:
        if not self.model_fields_set & {"price", "stop_loss_price", "take_profit_price"}:
            raise ValueError("Modification requires a price, stop-loss or take-profit change")
        return self


class ClosePositionRequest(_Request):
    """Empty options close the full position; supplied options become query parameters."""

    volume: Annotated[_RequestDecimal, Field(ge=0)] | None = None
    price: _PositiveDecimal | None = None
    deviation: _RequestDecimal | None = None
    close_by_position_id: _RequestIdentifier | None = None

    @model_validator(mode="after")
    def _close_volume(self) -> Self:
        if self.volume is not None and (
            self.volume.is_signed() or (self.volume == 0 and self.close_by_position_id is None)
        ):
            raise ValueError("Close volume must be positive, or zero with a close-by position")
        return self

    def to_query(self) -> dict[str, str]:
        validated = type(self).model_validate(self)
        return cast(dict[str, str], validated.model_dump(mode="json", exclude_unset=True))
