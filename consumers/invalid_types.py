"""Must fail mypy against the installed SDK; never execute this file."""

from exness import ExnessClient
from exness.requests import OpenPositionRequest


def invalid(client: ExnessClient) -> None:
    client.open_position("not a request", idempotency_key=123)
    OpenPositionRequest(instrument="EURUSD", side="buy", volume="not a Decimal")
