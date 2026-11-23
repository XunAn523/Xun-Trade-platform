"""Fail-closed EMT order-request construction for the TradeCore execution Adapter.

The SDK-facing execution factory receives an already manifest-mapped
``EmtSubmitRequest``.  It is deliberately the only concrete production
translation from TradeCore price ticks and enum mappings into the small EMT
``insertOrder`` dictionary.  No arbitrary execution parameters are forwarded
to the vendor binding.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from re import fullmatch
from typing import Any

from xuanling_tradecore.adapters.emt import EmtSubmitRequest


class EmtTradeCoreOrderRequestError(ValueError):
    """The approved TradeCore instruction cannot form a bounded EMT request."""


@dataclass(frozen=True)
class EmtTradeCoreOrderRequestFactory:
    """Convert one approved limit instruction to EMT's documented order shape.

    ``price_ticks_per_unit`` is an explicit profile property rather than an
    inferred market convention.  The first EMT profile uses cent ticks, so the
    configured value is 100.  A different venue precision requires a new,
    tested factory configuration and profile evidence.
    """

    price_ticks_per_unit: int = 100

    def __post_init__(self) -> None:
        if type(self.price_ticks_per_unit) is not int or self.price_ticks_per_unit <= 0:
            raise EmtTradeCoreOrderRequestError("price_ticks_per_unit must be a positive integer")

    def __call__(self, request: EmtSubmitRequest) -> dict[str, Any]:
        instruction = request.instruction
        mapped = request.mapped_order
        if instruction.execution_id != mapped.execution_id:
            raise EmtTradeCoreOrderRequestError("mapped order belongs to another execution")
        if instruction.account_id != mapped.account_id:
            raise EmtTradeCoreOrderRequestError("mapped order belongs to another account")
        if instruction.execution_parameters:
            raise EmtTradeCoreOrderRequestError(
                "EMT execution request rejects unapproved execution_parameters"
            )
        if instruction.price_type != "LIMIT" or mapped.price_type != 1:
            raise EmtTradeCoreOrderRequestError(
                "EMT execution request supports mapped limit orders only"
            )
        if instruction.limit_price_ticks is None or instruction.limit_price_ticks <= 0:
            raise EmtTradeCoreOrderRequestError(
                "EMT limit order requires a positive limit_price_ticks"
            )
        if not fullmatch(r"[0-9]{6}", instruction.symbol):
            raise EmtTradeCoreOrderRequestError("EMT equity symbol must be exactly six digits")
        if instruction.quantity > 100:
            raise EmtTradeCoreOrderRequestError("controlled EMT execution quantity exceeds 100")

        try:
            price = Decimal(instruction.limit_price_ticks) / Decimal(self.price_ticks_per_unit)
        except (InvalidOperation, ValueError) as exc:  # pragma: no cover - Decimal guards integers.
            raise EmtTradeCoreOrderRequestError("EMT limit price cannot be represented") from exc
        if price <= 0:
            raise EmtTradeCoreOrderRequestError("EMT limit price must be positive")

        return {
            "ticker": instruction.symbol,
            "market": mapped.market,
            "side": mapped.side,
            "price_type": mapped.price_type,
            "price": float(price),
            "quantity": instruction.quantity,
            "business_type": mapped.business_type,
            "order_client_id": request.order_client_id,
        }
