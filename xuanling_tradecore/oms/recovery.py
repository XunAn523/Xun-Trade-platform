"""Versioned, canonical OMS admission seed required for complete restart recovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Mapping, TypeVar, cast

import msgspec

from ..domain import (
    ConsumerTrace,
    DecisionContext,
    ExecutionInstruction,
    OmsOrder,
    OmsOrderState,
    OrderOrigin,
    TradingDayEpoch,
)
from ..domain._validation import DomainValidationError
from .order_book import (
    OmsFactKind,
    OrderBookOrder,
    Reservation,
    ReservationRequirement,
    ReservationStatus,
)


class OmsRecoverySeedError(DomainValidationError):
    """The admission seed cannot prove a complete, canonical OMS order."""


_EnumT = TypeVar("_EnumT", bound=Enum)


@dataclass(frozen=True)
class BrokerOrderObservationV1:
    """One normalized, complete broker order observation used by the OMS reducer."""

    broker_event_id: str
    source_key: str
    order_state: OmsOrderState
    cumulative_filled_quantity: int
    leaves_quantity: int
    trade_keys: tuple[str, ...]
    venue_order_id: str | None = None

    def __post_init__(self) -> None:
        if not self.broker_event_id or not self.source_key:
            raise OmsRecoverySeedError("broker observation identity is missing")
        if self.order_state not in {
            OmsOrderState.WORKING,
            OmsOrderState.PARTIALLY_FILLED,
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
        }:
            raise OmsRecoverySeedError("broker observation order state is unsupported")
        if self.cumulative_filled_quantity < 0 or self.leaves_quantity < 0:
            raise OmsRecoverySeedError("broker observation quantities must be nonnegative")
        if self.order_state is OmsOrderState.WORKING and self.cumulative_filled_quantity != 0:
            raise OmsRecoverySeedError("WORKING observation cannot contain fills")
        if self.order_state is OmsOrderState.PARTIALLY_FILLED and (
            self.cumulative_filled_quantity == 0 or self.leaves_quantity == 0
        ):
            raise OmsRecoverySeedError("PARTIALLY_FILLED observation quantities are invalid")
        if self.order_state is OmsOrderState.FILLED and self.leaves_quantity != 0:
            raise OmsRecoverySeedError("FILLED observation must have zero leaves")
        if self.order_state is OmsOrderState.CANCELED and self.leaves_quantity == 0:
            raise OmsRecoverySeedError("CANCELED observation must retain unfilled quantity")
        if self.order_state is OmsOrderState.REJECTED and self.cumulative_filled_quantity != 0:
            raise OmsRecoverySeedError("REJECTED observation cannot contain fills")
        keys = tuple(self.trade_keys)
        if len(keys) != len(set(keys)) or any(not key for key in keys):
            raise OmsRecoverySeedError("broker observation trade keys are invalid")
        if self.cumulative_filled_quantity > 0 and not keys:
            raise OmsRecoverySeedError("filled broker observation requires trade keys")
        if self.venue_order_id is not None and not self.venue_order_id:
            raise OmsRecoverySeedError("broker observation venue order ID is invalid")
        object.__setattr__(self, "trade_keys", keys)

    def validate_total_quantity(self, total_quantity: int) -> None:
        if total_quantity <= 0 or (
            self.cumulative_filled_quantity + self.leaves_quantity != total_quantity
        ):
            raise OmsRecoverySeedError("broker observation does not conserve order quantity")

    def validate_successor(
        self,
        previous: BrokerOrderObservationV1 | None,
        *,
        total_quantity: int,
    ) -> None:
        """Validate a complete observation against the prior durable broker state."""

        self.validate_total_quantity(total_quantity)
        if previous is None:
            return
        if previous.order_state in {
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
        }:
            raise OmsRecoverySeedError("broker observation follows a terminal state")
        if self.broker_event_id == previous.broker_event_id:
            raise OmsRecoverySeedError("broker observation event identity is duplicated")
        if (
            previous.venue_order_id is not None
            and self.venue_order_id is not None
            and self.venue_order_id != previous.venue_order_id
        ):
            raise OmsRecoverySeedError("broker observation venue order identity changed")
        if self.cumulative_filled_quantity < previous.cumulative_filled_quantity:
            raise OmsRecoverySeedError("broker cumulative fill regressed")
        previous_trades = set(previous.trade_keys)
        current_trades = set(self.trade_keys)
        if not previous_trades.issubset(current_trades):
            raise OmsRecoverySeedError("broker trade history regressed")
        if (
            self.cumulative_filled_quantity > previous.cumulative_filled_quantity
            and current_trades == previous_trades
        ):
            raise OmsRecoverySeedError("broker cumulative fill advanced without a new trade")
        allowed_states = {
            OmsOrderState.WORKING: {
                OmsOrderState.WORKING,
                OmsOrderState.PARTIALLY_FILLED,
                OmsOrderState.FILLED,
                OmsOrderState.CANCELED,
                OmsOrderState.REJECTED,
            },
            OmsOrderState.PARTIALLY_FILLED: {
                OmsOrderState.PARTIALLY_FILLED,
                OmsOrderState.FILLED,
                OmsOrderState.CANCELED,
            },
        }
        if self.order_state not in allowed_states[previous.order_state]:
            raise OmsRecoverySeedError("broker order state regressed")

    @property
    def reservation_fact_kind(self) -> OmsFactKind | None:
        if self.order_state in {
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
        }:
            return OmsFactKind.RESERVATION_RELEASED
        if self.cumulative_filled_quantity > 0:
            return OmsFactKind.RESERVATION_ADJUSTED
        return None

    @property
    def reservation_leaves_quantity(self) -> int:
        """Return risk-bearing leaves; terminal broker states retain no reservation."""

        if self.reservation_fact_kind is OmsFactKind.RESERVATION_RELEASED:
            return 0
        return self.leaves_quantity

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "broker_event_id": self.broker_event_id,
            "source_key": self.source_key,
            "order_state": self.order_state.value,
            "cumulative_filled_quantity": self.cumulative_filled_quantity,
            "leaves_quantity": self.leaves_quantity,
            "trade_keys": list(self.trade_keys),
            "venue_order_id": self.venue_order_id,
        }

    @classmethod
    def from_payload(cls, raw: object, *, total_quantity: int) -> BrokerOrderObservationV1:
        payload = _object(raw, "broker_observation")
        if (
            set(payload)
            != {
                "schema_version",
                "broker_event_id",
                "source_key",
                "order_state",
                "cumulative_filled_quantity",
                "leaves_quantity",
                "trade_keys",
                "venue_order_id",
            }
            or _integer(payload, "schema_version") != 1
        ):
            raise OmsRecoverySeedError("broker observation fields or schema are invalid")
        raw_keys = payload.get("trade_keys")
        if not isinstance(raw_keys, list) or any(
            not isinstance(key, str) for key in cast(list[object], raw_keys)
        ):
            raise OmsRecoverySeedError("broker observation trade keys must be text")
        result = cls(
            broker_event_id=_text(payload, "broker_event_id"),
            source_key=_text(payload, "source_key"),
            order_state=_enum(OmsOrderState, payload, "order_state"),
            cumulative_filled_quantity=_integer(payload, "cumulative_filled_quantity"),
            leaves_quantity=_integer(payload, "leaves_quantity"),
            trade_keys=tuple(cast(list[str], raw_keys)),
            venue_order_id=_optional_text(payload, "venue_order_id"),
        )
        result.validate_total_quantity(total_quantity)
        return result


@dataclass(frozen=True)
class OmsRecoverySeedV1:
    """Canonical admission-time state sufficient to reconstruct one OMS order."""

    order: OrderBookOrder
    encoded: bytes
    seed_hash: str

    @classmethod
    def build(cls, order: OrderBookOrder) -> OmsRecoverySeedV1:
        cls._require_admission_state(order)
        trace = order.order.consumer_trace
        payload = {
            "schema_version": 1,
            "consumer_trace": {
                "consumer_id": trace.consumer_id,
                "consumer_account_id": trace.consumer_account_id,
                "account_id": trace.account_id,
                "strategy_id": trace.strategy_id,
                "strategy_order_id": trace.strategy_order_id,
                "attribution_binding_id": trace.attribution_binding_id,
                "binding_version": trace.binding_version,
                "order_origin": trace.order_origin.value,
            },
            "order": {
                "oms_order_id": order.order.oms_order_id,
                "account_id": order.order.account_id,
                "trading_day": order.order.trading_day.value,
                "intent_hash": order.order.intent_hash,
                "order_origin": order.order.order_origin.value,
                "state": order.order.state.value,
                "created_monotonic_ns": order.order.created_monotonic_ns,
                "order_revision": order.order.order_revision,
            },
            "execution": {
                "execution_id": order.execution.execution_id,
                "oms_order_id": order.execution.oms_order_id,
                "account_id": order.execution.account_id,
                "trading_day": order.execution.trading_day.value,
                "symbol": order.execution.symbol,
                "market": order.execution.market,
                "side": order.execution.side,
                "price_type": order.execution.price_type,
                "business_type": order.execution.business_type,
                "quantity": order.execution.quantity,
                "protection_price_ticks": order.execution.protection_price_ticks,
                "limit_price_ticks": order.execution.limit_price_ticks,
                "execution_parameters": dict(order.execution.execution_parameters),
            },
            "decision_context": {
                "decision_context_id": order.decision_context.decision_context_id,
                "account_id": order.decision_context.account_id,
                "trading_day": order.decision_context.trading_day.value,
                "risk_snapshot_id": order.decision_context.risk_snapshot_id,
                "risk_rule_bundle_hash": order.decision_context.risk_rule_bundle_hash,
                "protection_price_ticks": order.decision_context.protection_price_ticks,
                "fee_model_version": order.decision_context.fee_model_version,
                "margin_model_version": order.decision_context.margin_model_version,
                "reservation_model_version": order.decision_context.reservation_model_version,
                "decided_monotonic_ns": order.decision_context.decided_monotonic_ns,
                "decision_bundle_hash": order.decision_context.decision_bundle_hash,
                "decision_bundle_epoch": order.decision_context.decision_bundle_epoch,
            },
            "reservation": {
                "reservation_id": order.reservation.reservation_id,
                "oms_order_id": order.reservation.oms_order_id,
                "account_id": order.reservation.account_id,
                "trading_day": order.reservation.trading_day.value,
                "risk_snapshot_id": order.reservation.risk_snapshot_id,
                "decision_context_id": order.reservation.decision_context_id,
                "original_requirement": _requirement_payload(
                    order.reservation.original_requirement
                ),
                "active_requirement": _requirement_payload(order.reservation.active_requirement),
                "status": order.reservation.status.value,
                "reservation_revision": order.reservation.reservation_revision,
                "created_monotonic_ns": order.reservation.created_monotonic_ns,
            },
            "filled_quantity": order.filled_quantity,
            "trade_keys": sorted(order.trade_keys),
        }
        encoded = _canonical_built_seed(payload)
        return cls(order=order, encoded=encoded, seed_hash=sha256(encoded).hexdigest())

    @classmethod
    def decode_and_verify(
        cls, encoded: bytes, *, maximum_encoded_bytes: int = 64 * 1024
    ) -> OmsRecoverySeedV1:
        if not encoded or len(encoded) > maximum_encoded_bytes:
            raise OmsRecoverySeedError("OMS recovery seed encoded size is invalid")
        try:
            raw: object = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise OmsRecoverySeedError("OMS recovery seed is not valid JSON") from error
        payload = _object(raw, "seed")
        if _canonical(payload) != encoded or set(payload) != {
            "schema_version",
            "consumer_trace",
            "order",
            "execution",
            "decision_context",
            "reservation",
            "filled_quantity",
            "trade_keys",
        }:
            raise OmsRecoverySeedError("OMS recovery seed is not canonical or has unknown fields")
        if _integer(payload, "schema_version") != 1:
            raise OmsRecoverySeedError("OMS recovery seed schema version is unsupported")
        trace_payload = _object(payload.get("consumer_trace"), "consumer_trace")
        trace = ConsumerTrace(
            consumer_id=_text(trace_payload, "consumer_id"),
            consumer_account_id=_text(trace_payload, "consumer_account_id"),
            account_id=_text(trace_payload, "account_id"),
            strategy_id=_optional_text(trace_payload, "strategy_id"),
            strategy_order_id=_optional_text(trace_payload, "strategy_order_id"),
            attribution_binding_id=_text(trace_payload, "attribution_binding_id"),
            binding_version=_integer(trace_payload, "binding_version"),
            order_origin=_enum(OrderOrigin, trace_payload, "order_origin"),
        )
        order_payload = _object(payload.get("order"), "order")
        order = OmsOrder(
            oms_order_id=_text(order_payload, "oms_order_id"),
            account_id=_text(order_payload, "account_id"),
            trading_day=TradingDayEpoch(_text(order_payload, "trading_day")),
            consumer_trace=trace,
            intent_hash=_text(order_payload, "intent_hash"),
            order_origin=_enum(OrderOrigin, order_payload, "order_origin"),
            state=_enum(OmsOrderState, order_payload, "state"),
            created_monotonic_ns=_integer(order_payload, "created_monotonic_ns"),
            order_revision=_integer(order_payload, "order_revision"),
        )
        execution_payload = _object(payload.get("execution"), "execution")
        execution = ExecutionInstruction(
            execution_id=_text(execution_payload, "execution_id"),
            oms_order_id=_text(execution_payload, "oms_order_id"),
            account_id=_text(execution_payload, "account_id"),
            trading_day=TradingDayEpoch(_text(execution_payload, "trading_day")),
            consumer_trace=trace,
            symbol=_text(execution_payload, "symbol"),
            market=_text(execution_payload, "market"),
            side=_text(execution_payload, "side"),
            price_type=_text(execution_payload, "price_type"),
            business_type=_text(execution_payload, "business_type"),
            quantity=_integer(execution_payload, "quantity"),
            protection_price_ticks=_integer(execution_payload, "protection_price_ticks"),
            limit_price_ticks=_optional_integer(execution_payload, "limit_price_ticks"),
            execution_parameters=_text_mapping(execution_payload, "execution_parameters"),
        )
        context_payload = _object(payload.get("decision_context"), "decision_context")
        context = DecisionContext(
            decision_context_id=_text(context_payload, "decision_context_id"),
            account_id=_text(context_payload, "account_id"),
            trading_day=TradingDayEpoch(_text(context_payload, "trading_day")),
            risk_snapshot_id=_text(context_payload, "risk_snapshot_id"),
            risk_rule_bundle_hash=_text(context_payload, "risk_rule_bundle_hash"),
            protection_price_ticks=_integer(context_payload, "protection_price_ticks"),
            fee_model_version=_text(context_payload, "fee_model_version"),
            margin_model_version=_text(context_payload, "margin_model_version"),
            reservation_model_version=_text(context_payload, "reservation_model_version"),
            decided_monotonic_ns=_integer(context_payload, "decided_monotonic_ns"),
            decision_bundle_hash=_optional_text(context_payload, "decision_bundle_hash"),
            decision_bundle_epoch=_optional_integer(context_payload, "decision_bundle_epoch"),
        )
        reservation_payload = _object(payload.get("reservation"), "reservation")
        reservation = Reservation(
            reservation_id=_text(reservation_payload, "reservation_id"),
            oms_order_id=_text(reservation_payload, "oms_order_id"),
            account_id=_text(reservation_payload, "account_id"),
            trading_day=TradingDayEpoch(_text(reservation_payload, "trading_day")),
            risk_snapshot_id=_text(reservation_payload, "risk_snapshot_id"),
            decision_context_id=_text(reservation_payload, "decision_context_id"),
            original_requirement=_requirement(reservation_payload, "original_requirement"),
            active_requirement=_requirement(reservation_payload, "active_requirement"),
            status=_enum(ReservationStatus, reservation_payload, "status"),
            reservation_revision=_integer(reservation_payload, "reservation_revision"),
            created_monotonic_ns=_integer(reservation_payload, "created_monotonic_ns"),
        )
        trade_values = payload.get("trade_keys")
        if not isinstance(trade_values, list) or any(
            not isinstance(value, str) for value in cast(list[object], trade_values)
        ):
            raise OmsRecoverySeedError("OMS recovery seed trade_keys are invalid")
        rebuilt = OrderBookOrder(
            order=order,
            execution=execution,
            decision_context=context,
            reservation=reservation,
            filled_quantity=_integer(payload, "filled_quantity"),
            trade_keys=frozenset(cast(list[str], trade_values)),
        )
        result = cls.build(rebuilt)
        if result.encoded != encoded:
            raise OmsRecoverySeedError("OMS recovery seed does not round-trip canonically")
        return result

    @staticmethod
    def _require_admission_state(order: OrderBookOrder) -> None:
        if (
            order.order.state is not OmsOrderState.ACCEPTED
            or order.order.order_revision != 0
            or order.reservation.status is not ReservationStatus.ACTIVE
            or order.reservation.reservation_revision != 0
            or order.filled_quantity != 0
            or order.trade_keys
        ):
            raise OmsRecoverySeedError("OMS recovery seed must capture initial accepted state")
        identities = {
            order.order.account_id,
            order.execution.account_id,
            order.decision_context.account_id,
            order.reservation.account_id,
        }
        days = {
            order.order.trading_day,
            order.execution.trading_day,
            order.decision_context.trading_day,
            order.reservation.trading_day,
        }
        if len(identities) != 1 or len(days) != 1:
            raise OmsRecoverySeedError("OMS recovery seed crosses account or trading-day scope")
        if (
            order.decision_context.decision_context_id != order.reservation.decision_context_id
            or order.decision_context.risk_snapshot_id != order.reservation.risk_snapshot_id
            or order.execution.protection_price_ticks
            != order.decision_context.protection_price_ticks
        ):
            raise OmsRecoverySeedError("OMS recovery seed decision evidence is inconsistent")


def _requirement_payload(requirement: ReservationRequirement) -> dict[str, object]:
    return {
        "symbol": requirement.symbol,
        "cash_minor": requirement.cash_minor,
        "margin_minor": requirement.margin_minor,
        "sellable_quantity": requirement.sellable_quantity,
        "borrowable_quantity": requirement.borrowable_quantity,
        "currency": requirement.currency,
    }


def _requirement(payload: Mapping[str, object], name: str) -> ReservationRequirement:
    value = _object(payload.get(name), name)
    return ReservationRequirement(
        symbol=_text(value, "symbol"),
        cash_minor=_integer(value, "cash_minor"),
        margin_minor=_integer(value, "margin_minor"),
        sellable_quantity=_integer(value, "sellable_quantity"),
        borrowable_quantity=_integer(value, "borrowable_quantity"),
        currency=_text(value, "currency"),
    )


def _canonical_built_seed(payload: Mapping[str, object]) -> bytes:
    """Internal typed seed only: preserve v1 ASCII canonical bytes exactly.

    Seed construction above contains only strings, integers, null and arrays/
    string-keyed objects. No float/set or generic caller object is admitted.
    Non-ASCII (including surrogate) paths retain the original ensure_ascii
    encoder. Untrusted decode STILL uses the reference canonical validator.
    """
    try:
        encoded = msgspec.json.encode(payload, order="sorted")
        if encoded.isascii() and b"\x7f" not in encoded:
            return encoded
    except (TypeError, ValueError, UnicodeEncodeError, OverflowError):
        pass
    return _canonical(payload)


def _canonical(payload: object) -> bytes:
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )
    except (TypeError, UnicodeEncodeError) as error:
        raise OmsRecoverySeedError("OMS recovery seed contains unsupported values") from error


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise OmsRecoverySeedError(f"OMS recovery seed {name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise OmsRecoverySeedError(f"OMS recovery seed {name} must use text keys")
    return cast(dict[str, object], raw)


def _text(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise OmsRecoverySeedError(f"OMS recovery seed {name} must be text")
    return value


def _optional_text(payload: Mapping[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise OmsRecoverySeedError(f"OMS recovery seed {name} must be optional text")
    return value


def _integer(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise OmsRecoverySeedError(f"OMS recovery seed {name} must be an integer")
    return value


def _optional_integer(payload: Mapping[str, object], name: str) -> int | None:
    value = payload.get(name)
    if value is None:
        return None
    return _integer(payload, name)


def _text_mapping(payload: Mapping[str, object], name: str) -> dict[str, str]:
    values = _object(payload.get(name), name)
    if any(not isinstance(value, str) for value in values.values()):
        raise OmsRecoverySeedError(f"OMS recovery seed {name} values must be text")
    return cast(dict[str, str], values)


def _enum(enum_type: type[_EnumT], payload: Mapping[str, object], name: str) -> _EnumT:
    try:
        return enum_type(_text(payload, name))
    except ValueError as error:
        raise OmsRecoverySeedError(f"OMS recovery seed {name} is unsupported") from error
