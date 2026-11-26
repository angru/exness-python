"""Conservative, offline triage hints for the pinned public contract."""

from __future__ import annotations

from urllib.parse import unquote

from tools.check_contract import JSON, Change

# Operation IDs and declarations reviewed against the pinned contract and SDK sources.
# Each entry is (Python declarations/location, Go declarations/location).
OPERATIONS: dict[str, tuple[str, str]] = {
    "getCandleHistory": (
        "get_candle_history (src/exness/client.py, async_client.py)",
        "GetCandleHistory (market.go)",
    ),
    "placePendingOrder": (
        "place_pending_order (src/exness/client.py, async_client.py)",
        "PlacePendingOrder (writes.go)",
    ),
    "modifyOrder": (
        "modify_order (src/exness/client.py, async_client.py)",
        "ModifyOrder (writes.go)",
    ),
    "cancelPendingOrder": (
        "cancel_pending_order (src/exness/client.py, async_client.py)",
        "CancelPendingOrder (writes.go)",
    ),
    "openPosition": (
        "open_position (src/exness/client.py, async_client.py)",
        "OpenPosition (writes.go)",
    ),
    "closePosition": (
        "close_position (src/exness/client.py, async_client.py)",
        "ClosePosition (writes.go)",
    ),
    "getTradingStateSnapshot": (
        "get_snapshot (src/exness/client.py, async_client.py)",
        "GetSnapshot (client.go)",
    ),
    "getOperationStatus": (
        "get_operation_status (src/exness/client.py, async_client.py)",
        "GetOperationStatus (market.go)",
    ),
    "getOrdersHistory": (
        "get_orders_history, iter_orders_history (src/exness/client.py, async_client.py)",
        "GetOrdersHistory, IterOrdersHistory (history.go)",
    ),
    "getDealsHistory": (
        "get_deals_history, iter_deals_history (src/exness/client.py, async_client.py)",
        "GetDealsHistory, IterDealsHistory (history.go)",
    ),
    "getWSEvents": (
        "stream_trading, stream_account_state, stream_instruments, stream_hmr "
        "(src/exness/async_client.py)",
        "StreamTrading (trading.go); StreamAccountState, StreamInstruments, StreamHMR (events.go)",
    ),
    "getWSTicks": ("stream_ticks (src/exness/async_client.py)", "StreamTicks (ticks.go)"),
    "getAccountDetails": (
        "get_account (src/exness/client.py, async_client.py)",
        "GetAccount (client.go)",
    ),
    "getAvailableInstrumentList": (
        "get_instruments (src/exness/client.py, async_client.py)",
        "GetInstruments (client.go)",
    ),
    "getInstrumentCondition": (
        "get_instrument_conditions (src/exness/client.py, async_client.py)",
        "GetInstrumentConditions (market.go)",
    ),
    "getLimits": ("get_limits (src/exness/client.py, async_client.py)", "GetLimits (client.go)"),
}
SCHEMAS: dict[str, tuple[str, str]] = {
    "AccountState": ("AccountState (src/exness/models.py)", "AccountState (models.go)"),
    "Order": ("Order (src/exness/models.py)", "Order (models.go)"),
    "Position": ("Position (src/exness/models.py)", "Position (models.go)"),
    "WSTradingTransactionEvent": (
        "TransactionEvent (src/exness/trading.py)",
        "TransactionEvent (trading.go)",
    ),
    "WSAccountStateEvent": (
        "AccountStateEvent (src/exness/events.py)",
        "AccountStateEvent (events.go)",
    ),
    "WSInstrumentEvent": ("InstrumentEvent (src/exness/events.py)", "InstrumentEvent (events.go)"),
    "WSHmrSnapshotEvent": ("HmrSnapshot (src/exness/events.py)", "HmrSnapshot (events.go)"),
    "WSHmrUpdateEvent": ("HmrUpdate (src/exness/events.py)", "HmrUpdate (events.go)"),
    "WSTick": ("Tick (src/exness/models.py)", "Tick (ticks_models.go)"),
}


def parts(pointer: str) -> tuple[str, ...]:
    return tuple(p.replace("~1", "/").replace("~0", "~") for p in pointer.split("/")[1:])


def resolve(document: dict[str, JSON], path: tuple[str, ...]) -> JSON:
    value: JSON = document
    for part in path:
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return None
    return value


def references(document: dict[str, JSON], roots: list[JSON]) -> set[tuple[str, ...]]:
    """Visit each referenced target once, including cycles; never follow external refs."""
    found: set[tuple[str, ...]] = set()
    stack = roots[:]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            ref = node.get("$ref")
            fragment = unquote(ref[1:]) if isinstance(ref, str) and ref.startswith("#") else ""
            if fragment.startswith("/"):
                target = parts(fragment)
                if target not in found:
                    found.add(target)
                    stack.append(resolve(document, target))
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return found


def impact(
    before: dict[str, JSON], after: dict[str, JSON], changes: list[Change]
) -> dict[str, JSON]:
    from tools.check_contract import METHODS

    # Keep side-specific operation IDs and reference closures so additions/removals work.
    operations: dict[str, list[tuple[tuple[str, ...], str, set[tuple[str, ...]]]]] = {}
    for document in (before, after):
        paths = document["paths"]
        assert isinstance(paths, dict)
        for path, item in paths.items():
            if not path.startswith("/") or not isinstance(item, dict):
                continue
            for method, operation in item.items():
                if method not in METHODS or not isinstance(operation, dict):
                    continue
                key = f"{method.upper()} {path}"
                op_id = operation.get("operationId")
                operations.setdefault(key, []).append(
                    (
                        ("paths", path, method),
                        op_id if isinstance(op_id, str) else "",
                        references(document, [operation, item.get("parameters")]),
                    )
                )
    entries: list[JSON] = []
    for change in changes:
        location = parts(change.pointer)
        endpoints: list[JSON] = []
        has_unmapped_operation = False
        for endpoint, sides in sorted(operations.items()):
            reasons: set[str] = set()
            ids: set[str] = set()
            for op_path, op_id, refs in sides:
                if location[:2] == op_path[:2] and (
                    len(location) <= 2 or location[2] in (op_path[2], "parameters")
                ):
                    reasons.add("operation or path-level parameters")
                    ids.add(op_id)
                if any(
                    location[: len(ref)] == ref or ref[: len(location)] == location for ref in refs
                ):
                    reasons.add("local reference (possibly transitive)")
                    ids.add(op_id)
            if reasons:
                has_unmapped_operation |= any(i not in OPERATIONS for i in ids)
                mappings = sorted({OPERATIONS[i] for i in ids if i in OPERATIONS})
                endpoints.append(
                    {
                        "endpoint": endpoint,
                        "reasons": list[JSON](sorted(reasons)),
                        "python": list[JSON](sorted({m[0] for m in mappings})),
                        "go": list[JSON](sorted({m[1] for m in mappings})),
                        "unmapped_operation_ids": list[JSON](
                            sorted(i or "(missing)" for i in ids if i not in OPERATIONS)
                        ),
                    }
                )
        schema = (
            location[2] if len(location) >= 3 and location[:2] == ("components", "schemas") else ""
        )
        mapping = SCHEMAS.get(schema)
        entries.append(
            {
                "pointer": change.pointer,
                "endpoints": endpoints,
                "schema": schema or None,
                "python_types": [mapping[0]] if mapping else [],
                "go_types": [mapping[1]] if mapping else [],
                "unmapped": not endpoints
                or has_unmapped_operation
                or (bool(schema) and mapping is None),
            }
        )
    return {
        "changes": entries,
        "limitations": [
            "Triage hints only; documentation changes and mappings do not prove runtime impact.",
            "Mappings are intentionally incomplete; external references are not followed.",
        ],
    }
