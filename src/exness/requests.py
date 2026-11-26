"""Validated trading request bodies; client submission methods are still planned."""

from decimal import Decimal
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, model_validator

from exness.models import DecimalValue, Direction, Instrument


def _plain_decimal(value: Decimal) -> str:
    return format(value, "f")


_RequestDecimal = Annotated[
    DecimalValue, PlainSerializer(_plain_decimal, return_type=str, when_used="json")
]
_PositiveDecimal = Annotated[_RequestDecimal, Field(gt=0)]


class OpenPositionRequest(BaseModel):
    """Encode a position-opening request; instrument-specific rules belong to the server.

    Omit optional fields rather than passing None. Prices and volume are positive
    Decimal values; binary floats are rejected. No network access occurs here.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
        revalidate_instances="always",
    )

    instrument: Instrument
    side: Direction
    volume: _PositiveDecimal
    price: _PositiveDecimal | None = None
    deviation: _RequestDecimal | None = None
    stop_loss_price: _PositiveDecimal | None = None
    take_profit_price: _PositiveDecimal | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def _reject_explicit_null(self) -> Self:
        if any(getattr(self, name) is None for name in self.model_fields_set):
            raise ValueError("Omit optional opening-position fields instead of sending null")
        return self

    def to_body(self) -> bytes:
        """Return validated UTF-8 JSON to sign and send unchanged."""
        validated = type(self).model_validate(self)
        return validated.model_dump_json(exclude_unset=True).encode("utf-8")
