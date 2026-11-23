"""A fail-closed M0 bridge for callers still organized around OrderGateway.

The bridge intentionally does not invoke ``OrderGateway.submit_order`` or any
broker method.  M1/M2 will attach the durable OMS/EMS execution lane after
the new Journal and ID allocators exist.  Keeping this bridge canonical-only
means adding it cannot alter P0 EMT request bytes or create a second native
side effect path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Mapping, Protocol

from emt_gateway.gateway import OrderGateway

from ..consumer import IntentReceipt
from ..domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerStrategyBinding,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)
from ..domain._validation import (
    DomainValidationError,
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_text,
)


def _empty_text_mapping() -> Mapping[str, str]:
    """Return a typed empty execution-parameter map."""

    return {}


@dataclass(frozen=True)
class LegacyInvocationContext:
    """A deployment-provided binding for one legacy caller lane.

    It is built from approved consumer/account/strategy bindings, not from a
    strategy request.  This prevents a legacy caller from choosing another
    consumer, employee, venue account, or strategy binding.
    """

    account_binding: ConsumerAccountBinding
    strategy_binding: ConsumerStrategyBinding
    attribution_binding: AttributionBinding
    trading_day: TradingDayEpoch

    def __post_init__(self) -> None:
        account = self.account_binding
        strategy = self.strategy_binding
        attribution = self.attribution_binding
        if not account.active or not strategy.active:
            raise DomainValidationError("legacy compatibility requires active consumer bindings")
        if strategy.consumer_id != account.consumer_id:
            raise DomainValidationError("legacy strategy binding belongs to another consumer")
        if strategy.consumer_account_id != account.consumer_account_id:
            raise DomainValidationError(
                "legacy strategy binding belongs to another consumer account"
            )
        if strategy.consumer_account_binding_id != account.binding_id:
            raise DomainValidationError("legacy strategy binding has another account binding")
        if attribution.consumer_id != account.consumer_id:
            raise DomainValidationError("legacy attribution belongs to another consumer")
        if attribution.consumer_account_id != account.consumer_account_id:
            raise DomainValidationError("legacy attribution belongs to another consumer account")
        if attribution.consumer_account_binding_id != account.binding_id:
            raise DomainValidationError("legacy attribution has another account binding")
        if attribution.consumer_strategy_binding_id != strategy.binding_id:
            raise DomainValidationError("legacy attribution has another strategy binding")
        if attribution.strategy_id != strategy.strategy_id:
            raise DomainValidationError("legacy attribution has another strategy")


@dataclass(frozen=True)
class LegacyOrderRequest:
    """A normalized legacy order request before it enters the new OMS.

    ``legacy_client_order_id`` stays a local P0 retry key.  It is excluded
    from the canonical business hash and never becomes an EMT/QMT client order
    reference.  M1 will allocate the venue-specific reference after durable
    OMS acceptance.
    """

    legacy_client_order_id: str
    strategy_order_id: str
    symbol: str
    market: str
    side: str
    price_type: str
    business_type: str
    quantity: int
    protection_price_ticks: int
    limit_price_ticks: int | None = None
    execution_parameters: Mapping[str, str] = field(default_factory=_empty_text_mapping)

    def __post_init__(self) -> None:
        require_text(self.legacy_client_order_id, "legacy_client_order_id")
        require_text(self.strategy_order_id, "legacy strategy_order_id")
        require_text(self.symbol, "legacy symbol", max_length=64)
        require_text(self.market, "legacy market", max_length=64)
        require_text(self.side, "legacy side", max_length=64)
        require_text(self.price_type, "legacy price_type", max_length=64)
        require_text(self.business_type, "legacy business_type", max_length=64)
        require_positive(self.quantity, "legacy quantity")
        require_nonnegative(self.protection_price_ticks, "legacy protection_price_ticks")
        if self.limit_price_ticks is not None:
            require_nonnegative(self.limit_price_ticks, "legacy limit_price_ticks")
        object.__setattr__(
            self,
            "execution_parameters",
            freeze_text_mapping(self.execution_parameters, "legacy execution_parameters"),
        )


@dataclass(frozen=True)
class LegacyCanonicalInstruction:
    """The canonical M0 handoff plus the legacy-local retry key.

    ``provisional_*`` ids are deterministic test/compatibility placeholders,
    not durable OMS identifiers.  M1 replaces them with Journal-backed global
    IDs before the bridge may dispatch any adapter side effect.
    """

    instruction: ExecutionInstruction
    canonical_intent_hash: str
    legacy_client_order_id: str

    def __post_init__(self) -> None:
        require_text(self.legacy_client_order_id, "legacy canonical client order id")
        if len(self.canonical_intent_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.canonical_intent_hash
        ):
            raise DomainValidationError("legacy canonical intent hash must be a SHA-256 hex digest")


class LegacyOmsPort(Protocol):
    """The future M1 OMS handoff used by the P0 compatibility bridge."""

    def accept_legacy_instruction(self, instruction: LegacyCanonicalInstruction) -> IntentReceipt:
        """Accept a canonical instruction only after durable M1 admission."""

        ...


class LegacyOrderGatewayBridge:
    """Map one approved legacy caller lane into the canonical domain language.

    The retained ``legacy_gateway`` is intentionally opaque here.  Its P0
    implementation continues serving existing callers unchanged until the
    M1/M2 compatibility contract can prove a single shared Journal causality.
    """

    def __init__(
        self,
        *,
        legacy_gateway: OrderGateway,
        context: LegacyInvocationContext,
        oms_port: LegacyOmsPort,
    ) -> None:
        self._legacy_gateway = legacy_gateway
        self._context = context
        self._oms_port = oms_port

    @property
    def legacy_gateway(self) -> OrderGateway:
        """Expose the frozen P0 entry for controlled host wiring only."""

        return self._legacy_gateway

    def canonicalize(self, request: LegacyOrderRequest) -> LegacyCanonicalInstruction:
        """Build an SDK-free canonical instruction without dispatching anything."""

        context = self._context
        account = context.account_binding
        strategy = context.strategy_binding
        trace = ConsumerTrace(
            consumer_id=account.consumer_id,
            consumer_account_id=account.consumer_account_id,
            account_id=account.account_id,
            strategy_id=strategy.strategy_id,
            strategy_order_id=request.strategy_order_id,
            attribution_binding_id=context.attribution_binding.attribution_binding_id,
            binding_version=context.attribution_binding.binding_version,
            order_origin=OrderOrigin.COMPATIBILITY,
        )
        canonical_payload = self._canonical_payload(request, trace)
        canonical_intent_hash = self._sha256_json(canonical_payload)
        identity_payload = {
            "account_id": account.account_id,
            "attribution_binding_id": context.attribution_binding.attribution_binding_id,
            "strategy_id": strategy.strategy_id,
            "strategy_order_id": request.strategy_order_id,
            "trading_day": context.trading_day.value,
        }
        provisional_identity = self._sha256_json(identity_payload)
        instruction = ExecutionInstruction(
            execution_id=f"m0-legacy-execution:{provisional_identity}",
            oms_order_id=f"m0-legacy-order:{provisional_identity}",
            account_id=account.account_id,
            trading_day=context.trading_day,
            consumer_trace=trace,
            symbol=request.symbol,
            market=request.market,
            side=request.side,
            price_type=request.price_type,
            business_type=request.business_type,
            quantity=request.quantity,
            protection_price_ticks=request.protection_price_ticks,
            limit_price_ticks=request.limit_price_ticks,
            execution_parameters=request.execution_parameters,
        )
        return LegacyCanonicalInstruction(
            instruction=instruction,
            canonical_intent_hash=canonical_intent_hash,
            legacy_client_order_id=request.legacy_client_order_id,
        )

    def submit(self, request: LegacyOrderRequest) -> IntentReceipt:
        """Hand the canonical form to an M1 OMS port; never call P0 directly."""

        return self._oms_port.accept_legacy_instruction(self.canonicalize(request))

    def _canonical_payload(
        self,
        request: LegacyOrderRequest,
        trace: ConsumerTrace,
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "consumer_id": trace.consumer_id,
            "consumer_account_id": trace.consumer_account_id,
            "account_id": trace.account_id,
            "attribution_binding_id": trace.attribution_binding_id,
            "binding_version": trace.binding_version,
            "strategy_id": trace.strategy_id,
            "strategy_order_id": trace.strategy_order_id,
            "trading_day": self._context.trading_day.value,
            "order_origin": trace.order_origin.value,
            "symbol": request.symbol,
            "market": request.market,
            "side": request.side,
            "price_type": request.price_type,
            "business_type": request.business_type,
            "quantity": request.quantity,
            "protection_price_ticks": request.protection_price_ticks,
            "limit_price_ticks": request.limit_price_ticks,
            "execution_parameters": dict(sorted(request.execution_parameters.items())),
        }

    @staticmethod
    def _sha256_json(value: Mapping[str, object]) -> str:
        encoded = json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return sha256(encoded).hexdigest()
