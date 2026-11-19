"""Bounded asyncio tick streaming; reconnection is opt-in and gaps are explicit."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import ssl
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar
from uuid import uuid4

from pydantic import ValidationError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from exness.errors import APIError, ProtocolError, StreamClosed, TransportError, WebSocketAPIError
from exness.models import Tick, WebSocketErrorResponse

if TYPE_CHECKING:
    from exness.async_client import AsyncExnessClient

# WebSocket debug logs include handshake headers. Never route them to application logs.
_LOGGER = logging.Logger("exness.ticks.transport")
_LOGGER.disabled = True


class _NoRedirectConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


@dataclass(frozen=True)
class TickStreamOptions:
    """Reconnect budget is lifetime-wide; zero disables reconnects."""

    max_reconnects: int = 0
    reconnect_delay: float = 1.0
    max_reconnect_delay: float = 30.0
    open_timeout: float = 15.0
    max_message_bytes: int = 65536

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_reconnects, bool)
            or not isinstance(self.max_reconnects, int)
            or not 0 <= self.max_reconnects <= 1000
        ):
            raise ValueError("max_reconnects must be an integer between 0 and 1000")
        for value in (self.reconnect_delay, self.max_reconnect_delay, self.open_timeout):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Stream delays and timeout must be positive and finite")
        if self.max_reconnect_delay < self.reconnect_delay:
            raise ValueError("max_reconnect_delay must be at least reconnect_delay")
        if (
            isinstance(self.max_message_bytes, bool)
            or not isinstance(self.max_message_bytes, int)
            or not 1 <= self.max_message_bytes <= 16 * 1024 * 1024
        ):
            raise ValueError("max_message_bytes must be an integer between 1 and 16 MiB")


@dataclass(frozen=True)
class TickStreamGap:
    """A connection was interrupted; missed stream messages cannot be replayed or counted."""

    reconnect_attempt: int
    delay: float
    close_code: int | None = None


# Neutral aliases for every WebSocket subscription; retain historical type identity.
StreamOptions = TickStreamOptions
StreamGap = TickStreamGap


def _instruments(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not values:
        raise ValueError("Provide a nonempty sequence of instrument names")
    result = tuple(values)
    if any(not re.fullmatch(r"[A-Za-z0-9._-]{1,11}", name) for name in result):
        raise ValueError("Invalid instrument name")
    if len(set(result)) != len(result):
        raise ValueError("Duplicate instruments are not allowed")
    return result


def _tick(message: str | bytes, instruments: tuple[str, ...], request_id: str) -> Tick:
    try:
        value = json.loads(message)
        if not isinstance(value, dict):
            raise ValueError("Expected an object")
        if "code" in value or "error_message" in value:
            error = WebSocketErrorResponse.model_validate_json(message)
            if error.id != request_id:
                raise ProtocolError("WebSocket error returned an unknown request id")
            raise WebSocketAPIError(error.id, error.code, error.error_message)
        tick = Tick.model_validate_json(message)
    except (ValueError, ValidationError):
        raise ProtocolError("Invalid tick stream message") from None
    if tick.instrument is None or tick.instrument not in instruments:
        raise ProtocolError("Tick returned a missing or unsubscribed instrument")
    return tick


EventT = TypeVar("EventT")


def stream_ticks(
    client: AsyncExnessClient,
    instruments: Sequence[str],
    options: TickStreamOptions,
    ssl_context: ssl.SSLContext | None,
) -> AsyncGenerator[Tick | TickStreamGap, None]:
    names = _instruments(instruments)
    return _stream(
        client,
        "ticks",
        {"event": "ticks", "instruments": names},
        options,
        ssl_context,
        lambda request_id: lambda message: _tick(message, names, request_id),
    )


async def _stream(
    client: AsyncExnessClient,
    endpoint: str,
    subscribe: dict[str, object],
    options: TickStreamOptions,
    ssl_context: ssl.SSLContext | None,
    decoder_factory: Callable[[str], Callable[[str | bytes], EventT]],
) -> AsyncGenerator[EventT | TickStreamGap, None]:
    attempts = 0
    delay = options.reconnect_delay
    path = f"/v1/server-events/accounts/{client._account_id}/ws/{endpoint}"
    while True:
        client._ensure_open()
        host = await client.discover()
        request_id = str(uuid4())
        subscription = json.dumps(
            {"id": request_id, "subscribe": subscribe},
            separators=(",", ":"),
        )
        if len(subscription.encode()) > options.max_message_bytes:
            raise ValueError("Subscription exceeds max_message_bytes")
        decode = decoder_factory(request_id)
        code: int | None = None
        try:
            async with _NoRedirectConnect(
                "wss://" + host.removeprefix("https://") + path,
                additional_headers=client._signer.get_headers(path),
                proxy=None,
                compression=None,
                logger=_LOGGER,
                ssl=ssl_context or ssl.create_default_context(),
                open_timeout=options.open_timeout,
                close_timeout=3,
                ping_interval=20,
                ping_timeout=20,
                max_size=options.max_message_bytes,
                max_queue=16,
            ) as websocket:
                client._ensure_open()
                client._ws_connections.add(websocket)
                try:
                    await websocket.send(subscription)
                    while True:
                        client._ensure_open()
                        message = await websocket.recv()
                        client._ensure_open()
                        yield decode(message)
                finally:
                    client._ws_connections.discard(websocket)
        except InvalidStatus as error:
            raise APIError(
                error.response.status_code, retry_after=error.response.headers.get("Retry-After")
            ) from None
        except (ssl.SSLError, InvalidHandshake):
            raise TransportError("WebSocket TLS or handshake failed") from None
        except ConnectionClosed as error:
            code = error.rcvd.code if error.rcvd else (error.sent.code if error.sent else 1006)
            if code == 1000:
                return
            if code not in (1001, 1006, 1011, 1012):
                raise StreamClosed(code) from None
        except (OSError, TimeoutError):
            pass
        client._ensure_open()
        if attempts >= options.max_reconnects:
            raise TransportError(
                "WebSocket stream disconnected; reconnect budget exhausted"
            ) from None
        attempts += 1
        yield TickStreamGap(attempts, delay, code)
        await asyncio.sleep(delay)
        delay = min(delay * 2, options.max_reconnect_delay)
