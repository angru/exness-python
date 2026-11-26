"""Replay synthetic recovery scenarios without credentials, sockets or trading writes.

This is a single-account application example, not a durable state store. Feed one
ordered stream into on_stream; invalidate on every exit, including cancellation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, JsonValue

from exness import OperationReference, ProtocolError, TradingStreamGap
from exness.models import AccountState, AckResponse, Deal, OperationStatus, Order, Position
from exness.trading import TradingEvent, TradingSnapshotEvent, TransactionEvent

TERMINAL = {"confirmed", "rejected", "failed"}
LIMIT = 256  # Bound this teaching example; reaching a limit requires a new snapshot.


def entity_id(value: str) -> str:
    if not value.isascii() or not value.isdigit() or len(value) > 20 or int(value) >= 2**64:
        raise ProtocolError("Invalid entity ID")
    return str(int(value))


@dataclass
class RecoveryView:
    reference: OperationReference
    ready: bool = False
    account: AccountState | None = None
    orders: dict[str, Order] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    deals: dict[str, Deal] = field(default_factory=dict)
    seen: set[bytes] = field(default_factory=set)
    outcome: str = "unknown"
    error_code: int | None = None
    error_message: str | None = None
    wait_expired: bool = False

    def invalidate(self) -> None:
        """Forget the live baseline, preserving separately tracked operation evidence."""
        self.ready = False
        self.account = None
        self.orders.clear()
        self.positions.clear()
        self.deals.clear()
        self.seen.clear()

    def record(self, outcome: str, code: int | None = None, message: str | None = None) -> None:
        if self.outcome in TERMINAL:
            if outcome in TERMINAL and outcome != self.outcome:
                self.invalidate()
                raise ProtocolError("Conflicting terminal observations; reconcile manually")
            if outcome != self.outcome:
                return  # A delayed ACK/pending read cannot undo an observed terminal result.
        self.outcome = outcome
        if code is not None:
            self.error_code = code
        if message is not None:
            self.error_message = message

    def acknowledge(self, ack: AckResponse) -> None:
        try:
            candidate = OperationReference.from_ack(
                ack, client_request_id=self.reference.client_request_id
            )
            if (
                candidate.operation_id is not None
                and self.reference.operation_id is not None
                and int(candidate.operation_id) != int(self.reference.operation_id)
            ):
                raise ProtocolError("Conflicting acknowledgement operation ID")
            self.reference = OperationReference(
                candidate.operation_id or self.reference.operation_id, candidate.client_request_id
            )
            self.record("accepted")
        except ProtocolError:
            self.invalidate()
            raise

    def status(self, status: OperationStatus) -> None:
        try:
            if not self.reference.matches_status(status):
                raise ProtocolError("Status does not match the tracked operation")
            self.record(status.status, status.error_code, status.error_message)
            self.reference = OperationReference(
                status.operation_id, self.reference.client_request_id
            )
        except ProtocolError:
            self.invalidate()
            raise
        # A terminal result does not repair the account baseline or fill deal history.

    def on_stream(self, event: TradingEvent | TradingStreamGap) -> None:
        try:
            self._on_stream(event)
        except ProtocolError:
            self.invalidate()
            raise

    def _on_stream(self, event: TradingEvent | TradingStreamGap) -> None:
        if isinstance(event, TradingStreamGap):
            self.invalidate()
            return
        if isinstance(event, TradingSnapshotEvent):
            snapshot = event.payload
            orders = {entity_id(order.order_id): order for order in snapshot.orders}
            positions = {
                entity_id(position.position_id): position for position in snapshot.positions
            }
            if (
                len(orders) != len(snapshot.orders)
                or len(positions) != len(snapshot.positions)
                or any(order.state not in {"pending_new", "placed"} for order in orders.values())
                or any(position.state != "open" for position in positions.values())
                or max(len(orders), len(positions)) > LIMIT
            ):
                raise ProtocolError("Snapshot cannot establish an active-state baseline")
            self.invalidate()
            self.orders, self.positions, self.account = orders, positions, snapshot.account_state
            self.ready = True
            return
        if not self.ready:
            raise ProtocolError("A fresh stream snapshot is required before transactions")

        # No event sequence/replay token exists. This bounded set recognizes exact
        # decoded duplicates only within the current snapshot epoch, not arbitrary reordering.
        digest = hashlib.sha256(event.model_dump_json().encode()).digest()
        if digest in self.seen:
            return
        if len(self.seen) >= LIMIT:
            raise ProtocolError("Duplicate window exhausted; obtain a fresh snapshot")
        matches = self.reference.matches_event(event)
        if matches:
            if event.status is not None:
                outcome = "confirmed" if event.status == "success" else event.status
                self.record(outcome, event.error_code, event.error_message)
            self.reference = OperationReference(
                event.operation_id or self.reference.operation_id, self.reference.client_request_id
            )
        if event.status is None:
            self.invalidate()  # Conservative example policy; no completion is fabricated.
            return
        if event.status in {"rejected", "failed"}:
            if (
                event.payload.orders
                or event.payload.positions
                or event.payload.deals
                or event.payload.account_state
            ):
                raise ProtocolError("Unexpected failure payload; reconcile account state")
            self.seen.add(digest)
            return

        orders, positions, deals = self.orders.copy(), self.positions.copy(), self.deals.copy()
        for order in event.payload.orders:
            key = entity_id(order.order_id)
            if order.state in {"pending_new", "placed"}:
                orders[key] = order
            else:
                orders.pop(key, None)
        for position in event.payload.positions:
            key = entity_id(position.position_id)
            if position.state == "open":
                positions[key] = position  # Replace remaining volume; never subtract twice.
            else:
                positions.pop(key, None)
        for deal in event.payload.deals:
            deals[entity_id(deal.deal_id)] = deal  # Observed entities, not an additive ledger.
        if max(len(orders), len(positions), len(deals)) > LIMIT:
            raise ProtocolError("Example entity limit exceeded; reconcile account state")
        self.orders, self.positions, self.deals = orders, positions, deals
        if event.payload.account_state is not None:
            self.account = event.payload.account_state
        self.seen.add(digest)

    def summary(self) -> dict[str, JsonValue]:
        account: JsonValue = None
        if self.account is not None:
            account = {
                "balance": format(self.account.balance, "f"),
                "equity": format(self.account.equity, "f"),
                "used_margin": format(self.account.used_margin, "f"),
            }
        return {
            "ready": self.ready,
            "account": account,
            "order_ids": [*sorted(self.orders)],
            "position_volumes": {
                key: format(value.volume, "f") for key, value in self.positions.items()
            },
            "observed_deal_ids": [*sorted(self.deals)],
            "operation": {
                "operation_id": self.reference.operation_id,
                "outcome": self.outcome,
                "error_code": self.error_code,
                "error_message": self.error_message,
                "wait_expired": self.wait_expired,
            },
        }


class Step(BaseModel):
    label: str
    kind: Literal["stream", "ack", "status", "timeout", "gap", "end"]
    message: str | None = None
    expected: dict[str, JsonValue]
    expected_error: bool = False


class Scenario(BaseModel):
    name: str
    key: str
    steps: list[Step]


class Suite(BaseModel):
    messages: dict[str, dict[str, JsonValue]]
    scenarios: list[Scenario]


def apply_step(view: RecoveryView, step: Step, messages: dict[str, dict[str, JsonValue]]) -> None:
    if step.kind == "gap":
        view.on_stream(TradingStreamGap(1, 1))
    elif step.kind == "end":
        view.invalidate()  # Also call this in a real consumer's finally block.
    elif step.kind == "timeout":
        view.wait_expired = True  # Simulated wait expiry; preserve the last outcome and key.
    else:
        assert step.message is not None
        data = json.dumps(messages[step.message])
        if step.kind == "ack":
            view.acknowledge(AckResponse.model_validate_json(data))
        elif step.kind == "status":
            view.status(OperationStatus.model_validate_json(data))
        elif messages[step.message]["event_type"] == "trading_state_snapshot":
            view.on_stream(TradingSnapshotEvent.model_validate_json(data))
        else:
            view.on_stream(TransactionEvent.model_validate_json(data))


def replay(suite: Suite) -> list[dict[str, JsonValue]]:
    rows: list[dict[str, JsonValue]] = []
    for scenario in suite.scenarios:
        view = RecoveryView(OperationReference(client_request_id=scenario.key))
        for step in scenario.steps:
            rejected = False
            try:
                apply_step(view, step, suite.messages)
            except ProtocolError:
                rejected = True
            summary = view.summary()
            if rejected != step.expected_error or summary != step.expected:
                raise AssertionError(f"Unexpected recovery result: {scenario.name}/{step.label}")
            rows.append(
                {
                    "scenario": scenario.name,
                    "step": step.label,
                    "rejected": rejected,
                    "view": summary,
                }
            )
    return rows


def main() -> None:
    suite = Suite.model_validate_json(
        Path(__file__).with_name("recovery_scenarios.json").read_bytes()
    )
    for row in replay(suite):
        print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
