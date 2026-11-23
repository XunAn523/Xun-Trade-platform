"""Executable V1/V2 callback-equivalence admission matrix.

The native V2 slot is deliberately bounded.  This module makes its exact
coverage explicit before a runtime can claim that V2 is a drop-in replacement
for the legacy V1 callback path.  It is not a payload mapper and is never
called from the SDK callback thread.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256

from .spi_bridge import OFFICIAL_TRADER_SPI


class FastSpiV2EquivalenceError(RuntimeError):
    """The fixed V2 record cannot yet prove a required public surface."""


class FastSpiV2CallbackField(str, Enum):
    """Canonical fields used when comparing V1 against the V2 POD slot."""

    CALLBACK_KIND = "callback_kind"
    CLIENT_ID = "client_id"
    ORDER_CLIENT_ID = "order_client_id"
    ORDER_EMT_ID = "order_emt_id"
    ORDER_CANCEL_EMT_ID = "order_cancel_emt_id"
    STATUS_OR_ERROR_ID = "status_or_error_id"
    QUANTITY = "quantity"
    TRADE_DEDUPLICATION_KEY = "trade_deduplication_key"
    ERROR_SUMMARY = "error_summary"
    QUERY_REQUEST_ID = "query_request_id"
    QUERY_ITEM_PRESENT = "query_item_present"
    QUERY_LAST = "query_last"
    ORDER_UPDATE_TIME = "order_update_time"
    ORDER_QTY_TRADED = "order_qty_traded"
    ORDER_QTY_LEFT = "order_qty_left"
    TRADE_MARKET = "trade_market"
    TRADE_PRICE = "trade_price"
    TRADE_EXECUTION_ID = "trade_execution_id"
    QUERY_PAGE_REFERENCE = "query_page_reference"


class FastSpiV2EquivalenceSurface(str, Enum):
    """A public V1 behavior whose V2 replacement must be proven."""

    CALLBACK_DELIVERY = "callback_delivery"
    ORDER_LEDGER_FACT = "order_ledger_fact"
    TRADE_LEDGER_FACT = "trade_ledger_fact"
    CANCEL_ERROR = "cancel_error"
    QUERY_CORRELATION = "query_correlation"


_V2_SLOT_FIELDS = frozenset(
    {
        FastSpiV2CallbackField.CALLBACK_KIND,
        FastSpiV2CallbackField.CLIENT_ID,
        FastSpiV2CallbackField.ORDER_CLIENT_ID,
        FastSpiV2CallbackField.ORDER_EMT_ID,
        FastSpiV2CallbackField.ORDER_CANCEL_EMT_ID,
        FastSpiV2CallbackField.STATUS_OR_ERROR_ID,
        FastSpiV2CallbackField.QUANTITY,
        FastSpiV2CallbackField.TRADE_DEDUPLICATION_KEY,
        FastSpiV2CallbackField.ERROR_SUMMARY,
        FastSpiV2CallbackField.QUERY_REQUEST_ID,
        FastSpiV2CallbackField.QUERY_ITEM_PRESENT,
        FastSpiV2CallbackField.QUERY_LAST,
        FastSpiV2CallbackField.ORDER_UPDATE_TIME,
        FastSpiV2CallbackField.ORDER_QTY_TRADED,
        FastSpiV2CallbackField.ORDER_QTY_LEFT,
        FastSpiV2CallbackField.TRADE_MARKET,
        FastSpiV2CallbackField.TRADE_PRICE,
        FastSpiV2CallbackField.TRADE_EXECUTION_ID,
        FastSpiV2CallbackField.QUERY_PAGE_REFERENCE,
    }
)


@dataclass(frozen=True)
class FastSpiV2EquivalenceRequirement:
    """One public V1 surface and the exact fixed fields it needs."""

    callback_name: str
    surface: FastSpiV2EquivalenceSurface
    required_fields: frozenset[FastSpiV2CallbackField]
    available_fields: frozenset[FastSpiV2CallbackField]

    @property
    def missing_fields(self) -> tuple[FastSpiV2CallbackField, ...]:
        return tuple(
            sorted(self.required_fields - self.available_fields, key=lambda field: field.value)
        )

    @property
    def equivalent(self) -> bool:
        return not self.missing_fields

    def canonical_payload(self) -> dict[str, object]:
        """Return a deterministic, payload-free representation for evidence."""

        return {
            "callback_name": self.callback_name,
            "surface": self.surface.value,
            "required_fields": sorted(field.value for field in self.required_fields),
            "available_fields": sorted(field.value for field in self.available_fields),
            "missing_fields": [field.value for field in self.missing_fields],
        }


@dataclass(frozen=True)
class FastSpiV2EquivalenceMatrix:
    """All official callback obligations for one bounded V2 record version."""

    requirements: tuple[FastSpiV2EquivalenceRequirement, ...]

    def __post_init__(self) -> None:
        if not self.requirements:
            raise ValueError("fastspi v2 equivalence matrix requires obligations")
        covered_callbacks = {requirement.callback_name for requirement in self.requirements}
        missing_callbacks = set(OFFICIAL_TRADER_SPI) - covered_callbacks
        unknown_callbacks = covered_callbacks - set(OFFICIAL_TRADER_SPI)
        if missing_callbacks or unknown_callbacks:
            raise ValueError(
                "fastspi v2 equivalence matrix must cover exactly the official callback set"
            )

    @property
    def certificate_sha256(self) -> str:
        """Hash the declared evidence surface; it is not a completion claim."""

        encoded = json.dumps(
            [requirement.canonical_payload() for requirement in self.requirements],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        return sha256(encoded).hexdigest()

    @property
    def blocking_requirements(self) -> tuple[FastSpiV2EquivalenceRequirement, ...]:
        return tuple(requirement for requirement in self.requirements if not requirement.equivalent)

    @property
    def runtime_equivalent(self) -> bool:
        return not self.blocking_requirements

    def requirements_for(self, callback_name: str) -> tuple[FastSpiV2EquivalenceRequirement, ...]:
        if callback_name not in OFFICIAL_TRADER_SPI:
            raise ValueError(f"unknown official TraderSpi callback: {callback_name}")
        return tuple(
            requirement
            for requirement in self.requirements
            if requirement.callback_name == callback_name
        )

    def assert_runtime_equivalent(self) -> None:
        """Reject V2 runtime binding until every V1 public surface is proven."""

        if self.runtime_equivalent:
            return
        summaries = ", ".join(
            f"{requirement.callback_name}/{requirement.surface.value}="
            f"{'+'.join(field.value for field in requirement.missing_fields)}"
            for requirement in self.blocking_requirements
        )
        raise FastSpiV2EquivalenceError(
            "fastspi v2 record layout cannot yet prove V1/V2 equivalence: " + summaries
        )


def legacy_emt_fastspi_v2_equivalence_matrix() -> FastSpiV2EquivalenceMatrix:
    """Declare the current V1 legacy bridge's required public scalar surfaces.

    This has deliberately no optimistic default.  A future layout extension
    must add fixed fields here and prove the associated V1/V2 vectors before
    the matrix can become a runtime-equivalence certificate.
    """

    requirements: list[FastSpiV2EquivalenceRequirement] = []
    for callback_name in OFFICIAL_TRADER_SPI:
        requirements.append(
            FastSpiV2EquivalenceRequirement(
                callback_name=callback_name,
                surface=FastSpiV2EquivalenceSurface.CALLBACK_DELIVERY,
                required_fields=frozenset({FastSpiV2CallbackField.CALLBACK_KIND}),
                available_fields=_V2_SLOT_FIELDS,
            )
        )
        if callback_name == "OnOrderEvent":
            requirements.append(
                FastSpiV2EquivalenceRequirement(
                    callback_name=callback_name,
                    surface=FastSpiV2EquivalenceSurface.ORDER_LEDGER_FACT,
                    required_fields=frozenset(
                        {
                            FastSpiV2CallbackField.ORDER_EMT_ID,
                            FastSpiV2CallbackField.ORDER_CLIENT_ID,
                            FastSpiV2CallbackField.ORDER_CANCEL_EMT_ID,
                            FastSpiV2CallbackField.STATUS_OR_ERROR_ID,
                            FastSpiV2CallbackField.ORDER_UPDATE_TIME,
                            FastSpiV2CallbackField.ORDER_QTY_TRADED,
                            FastSpiV2CallbackField.ORDER_QTY_LEFT,
                        }
                    ),
                    available_fields=_V2_SLOT_FIELDS,
                )
            )
        elif callback_name == "OnTradeEvent":
            requirements.append(
                FastSpiV2EquivalenceRequirement(
                    callback_name=callback_name,
                    surface=FastSpiV2EquivalenceSurface.TRADE_LEDGER_FACT,
                    required_fields=frozenset(
                        {
                            FastSpiV2CallbackField.ORDER_EMT_ID,
                            FastSpiV2CallbackField.ORDER_CLIENT_ID,
                            FastSpiV2CallbackField.QUANTITY,
                            FastSpiV2CallbackField.TRADE_MARKET,
                            FastSpiV2CallbackField.TRADE_PRICE,
                            FastSpiV2CallbackField.TRADE_EXECUTION_ID,
                        }
                    ),
                    available_fields=_V2_SLOT_FIELDS,
                )
            )
        elif callback_name == "OnCancelOrderError":
            requirements.append(
                FastSpiV2EquivalenceRequirement(
                    callback_name=callback_name,
                    surface=FastSpiV2EquivalenceSurface.CANCEL_ERROR,
                    required_fields=frozenset(
                        {
                            FastSpiV2CallbackField.ORDER_EMT_ID,
                            FastSpiV2CallbackField.ORDER_CANCEL_EMT_ID,
                            FastSpiV2CallbackField.STATUS_OR_ERROR_ID,
                            FastSpiV2CallbackField.ERROR_SUMMARY,
                        }
                    ),
                    available_fields=_V2_SLOT_FIELDS,
                )
            )
        elif callback_name.startswith("OnQuery"):
            query_fields = {
                FastSpiV2CallbackField.QUERY_REQUEST_ID,
                FastSpiV2CallbackField.QUERY_ITEM_PRESENT,
                FastSpiV2CallbackField.QUERY_LAST,
            }
            if callback_name.endswith("ByPage"):
                query_fields.add(FastSpiV2CallbackField.QUERY_PAGE_REFERENCE)
            requirements.append(
                FastSpiV2EquivalenceRequirement(
                    callback_name=callback_name,
                    surface=FastSpiV2EquivalenceSurface.QUERY_CORRELATION,
                    required_fields=frozenset(query_fields),
                    available_fields=_V2_SLOT_FIELDS,
                )
            )
    return FastSpiV2EquivalenceMatrix(requirements=tuple(requirements))
