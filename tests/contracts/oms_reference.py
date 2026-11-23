"""Executable OMS reference contract for future M1 implementation tests.

This is deliberately a test-only model, not a production order book.  It
expresses the non-negotiable state and reservation invariants that M1 must
preserve when the real OMS domain objects are introduced.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReferenceOrderState(str, Enum):
    LIVE = "LIVE"
    PART_FILLED = "PART_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class ReferenceOrder:
    order_id: str
    quantity: int
    filled_quantity: int
    state: ReferenceOrderState

    @property
    def leaves_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def reserves_quantity(self) -> bool:
        return self.state in {ReferenceOrderState.LIVE, ReferenceOrderState.PART_FILLED}


class ReferenceOrderBook:
    """Small idempotent model of order state, fills and quantity reservation."""

    def __init__(self) -> None:
        self._orders: dict[str, ReferenceOrder] = {}
        self._execution_keys: set[str] = set()

    def submit(self, *, order_id: str, quantity: int) -> ReferenceOrder:
        if not order_id or quantity <= 0:
            raise ValueError("order_id and a positive quantity are required")
        existing = self._orders.get(order_id)
        if existing is not None:
            if existing.quantity != quantity:
                raise ValueError("idempotency conflict")
            return existing
        order = ReferenceOrder(order_id, quantity, 0, ReferenceOrderState.LIVE)
        self._orders[order_id] = order
        return order

    def apply_trade(self, *, order_id: str, execution_key: str, quantity: int) -> bool:
        if not execution_key or quantity <= 0:
            raise ValueError("execution_key and a positive quantity are required")
        if execution_key in self._execution_keys:
            return False
        order = self._require_open(order_id)
        if quantity > order.leaves_quantity:
            raise ValueError("trade quantity exceeds leaves")
        self._execution_keys.add(execution_key)
        filled = order.filled_quantity + quantity
        state = (
            ReferenceOrderState.FILLED
            if filled == order.quantity
            else ReferenceOrderState.PART_FILLED
        )
        self._orders[order_id] = ReferenceOrder(order.order_id, order.quantity, filled, state)
        return True

    def cancel(self, *, order_id: str) -> ReferenceOrder:
        order = self._require_open(order_id)
        cancelled = ReferenceOrder(
            order.order_id,
            order.quantity,
            order.filled_quantity,
            ReferenceOrderState.CANCELED,
        )
        self._orders[order_id] = cancelled
        return cancelled

    def reject(self, *, order_id: str) -> ReferenceOrder:
        order = self._require_open(order_id)
        rejected = ReferenceOrder(
            order.order_id,
            order.quantity,
            order.filled_quantity,
            ReferenceOrderState.REJECTED,
        )
        self._orders[order_id] = rejected
        return rejected

    def order(self, order_id: str) -> ReferenceOrder:
        return self._orders[order_id]

    def reservations(self) -> int:
        return sum(
            order.leaves_quantity for order in self._orders.values() if order.reserves_quantity
        )

    def orders(self) -> tuple[ReferenceOrder, ...]:
        return tuple(self._orders.values())

    def _require_open(self, order_id: str) -> ReferenceOrder:
        order = self._orders[order_id]
        if not order.reserves_quantity:
            raise ValueError("order is terminal")
        return order
