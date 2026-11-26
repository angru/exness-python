"""Compatibility gates for the reviewed public entry points."""

import inspect
from collections.abc import AsyncGenerator, Iterator
from typing import get_args, get_origin, get_type_hints

from exness import (
    AsyncExnessClient,
    ExnessClient,
    ServerStreamGap,
    ServerStreamOptions,
    StreamGap,
    StreamOptions,
    TickStreamGap,
    TickStreamOptions,
    TradingStreamGap,
    TradingStreamOptions,
)


def test_sync_async_rest_signature_parity() -> None:
    methods = {
        name
        for name in vars(ExnessClient)
        if name == "discover"
        or name.startswith(("get_", "iter_"))
        or name
        in {
            "open_position",
            "close_position",
            "place_pending_order",
            "modify_order",
            "cancel_pending_order",
        }
    }
    assert len(methods) == 17
    for name in methods:
        sync = getattr(ExnessClient, name)
        asynchronous = getattr(AsyncExnessClient, name)
        left, right = inspect.signature(sync), inspect.signature(asynchronous)
        assert left.parameters == right.parameters, name
        sync_types, async_types = get_type_hints(sync), get_type_hints(asynchronous)
        if name.startswith("iter_"):
            assert get_origin(sync_types["return"]) is Iterator
            assert get_origin(async_types["return"]) is AsyncGenerator
            assert get_args(sync_types["return"])[0] == get_args(async_types["return"])[0]
        else:
            assert inspect.iscoroutinefunction(asynchronous)
            assert sync_types == async_types, name


def test_common_stream_names_preserve_legacy_identity() -> None:
    assert StreamOptions is TickStreamOptions is TradingStreamOptions is ServerStreamOptions
    assert StreamGap is TickStreamGap is TradingStreamGap is ServerStreamGap
    assert StreamOptions().max_reconnects == 0
    assert isinstance(StreamGap(1, 1), TickStreamGap)
    for name in (
        "stream_ticks",
        "stream_trading",
        "stream_account_state",
        "stream_instruments",
        "stream_hmr",
    ):
        assert callable(getattr(AsyncExnessClient, name))
        assert not hasattr(ExnessClient, name)
