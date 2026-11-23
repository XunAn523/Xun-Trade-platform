"""Strict QMT-only event protocol shared by Linux and the authenticated Edge.

The outbox epoch identifies a durable event database, not a socket or ring.
Replacing the database requires a new epoch and explicit reconciliation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping, cast

from ...domain import (
    BrokerEventKind,
    BrokerSourceKind,
    CallbackGapEvidence,
    ErrorCategory,
    TradingDayEpoch,
)
from ...domain._validation import (
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)
from .bridge_protocol import (
    QmtBridgeDelivery,
    QmtBridgeError,
    QmtBridgeEventDelivery,
    QmtBridgeGapDelivery,
    QmtBridgeNativeEvent,
)
from .bridge_wire import (
    QmtBridgeWireError,
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)


class QmtEventWireError(QmtBridgeError):
    """An event stream violated its schema, account, epoch or scope fence."""


@dataclass(frozen=True)
class QmtEventStreamConfig:
    account_id: str
    outbox_epoch: str
    resume_delivery_id: int = 0

    def __post_init__(self) -> None:
        require_text(self.account_id, "QMT event account")
        require_sha256(self.outbox_epoch, "QMT event outbox epoch")
        require_nonnegative(self.resume_delivery_id, "QMT event resume cursor")


class QmtEventWireCodec:
    """Bounded canonical envelopes. No QMT raw objects or SDK dependency."""

    def __init__(self, *, scope_digest: str, config: QmtEventStreamConfig) -> None:
        self.scope_digest = require_sha256(scope_digest, "QMT event scope")
        self.config = config

    def control(self, kind: QmtBridgeWireKind, schema: str, **fields: object) -> QmtBridgeWireFrame:
        return QmtBridgeWireFrame(
            kind,
            canonical_wire_payload(
                {
                    "schema": schema,
                    "scope_digest": self.scope_digest,
                    "outbox_epoch": self.config.outbox_epoch,
                    **fields,
                }
            ),
        )

    def decode_control(
        self, frame: QmtBridgeWireFrame, *, kind: QmtBridgeWireKind, schema: str, fields: set[str]
    ) -> dict[str, object]:
        try:
            payload = dict(decode_canonical_wire_payload(frame.payload))
            if (
                frame.kind is not kind
                or set(payload) != fields | {"schema", "scope_digest", "outbox_epoch"}
                or payload["schema"] != schema
                or payload["scope_digest"] != self.scope_digest
                or payload["outbox_epoch"] != self.config.outbox_epoch
            ):
                raise QmtEventWireError("QMT event envelope has another schema, scope or epoch")
            return payload
        except (ValueError, QmtBridgeWireError) as error:
            raise QmtEventWireError("QMT event envelope is invalid") from error

    def encode_fact(self, delivery: QmtBridgeDelivery) -> bytes:
        """Encode a normalized fact for durable outbox storage before delivery."""
        if isinstance(delivery, QmtBridgeGapDelivery):
            kind = "GAP"
            fact: dict[str, object] = asdict(delivery.evidence)
        else:
            kind = "EVENT"
            event = delivery.event
            fact = {
                "external_event_id": event.external_event_id,
                "event_kind": event.event_kind.value,
                "source_kind": event.source_kind.value,
                "account_id": event.account_id,
                "trading_day": event.trading_day.value,
                "received_monotonic_ns": event.received_monotonic_ns,
                "client_order_ref": event.client_order_ref,
                "venue_order_id": event.venue_order_id,
                "venue_cancel_id": event.venue_cancel_id,
                "execution_id": event.execution_id,
                "cancel_request_id": event.cancel_request_id,
                "trade_key": event.trade_key,
                "filled_qty": event.filled_qty,
                "leaves_qty": event.leaves_qty,
                "error_category": event.error_category.value if event.error_category else None,
                "normalized_summary": dict(event.normalized_summary),
            }
        raw = canonical_wire_payload({"kind": kind, "fact": fact})
        # Use the same strict validation on both sides of the outbox boundary.
        self.decode_fact(raw, delivery_id=1)
        return raw

    def encode_delivery(self, *, delivery_id: int, fact_payload: bytes) -> QmtBridgeWireFrame:
        require_positive(delivery_id, "QMT delivery id")
        self.decode_fact(fact_payload, delivery_id=delivery_id)
        return self.control(
            QmtBridgeWireKind.EVENT,
            "QmtEventDeliveryV1",
            delivery_id=delivery_id,
            content=dict(decode_canonical_wire_payload(fact_payload)),
        )

    def decode_delivery(self, frame: QmtBridgeWireFrame) -> QmtBridgeDelivery:
        payload = self.decode_control(
            frame,
            kind=QmtBridgeWireKind.EVENT,
            schema="QmtEventDeliveryV1",
            fields={"delivery_id", "content"},
        )
        try:
            delivery_id = require_positive(payload["delivery_id"], "QMT delivery id")
            content = _mapping(payload["content"])
            return self.decode_fact(canonical_wire_payload(content), delivery_id=delivery_id)
        except (ValueError, QmtBridgeWireError) as error:
            raise QmtEventWireError("QMT event delivery is invalid") from error

    def decode_fact(self, raw: bytes, *, delivery_id: int) -> QmtBridgeDelivery:
        try:
            content = decode_canonical_wire_payload(raw)
            if set(content) != {"kind", "fact"}:
                raise QmtEventWireError("QMT outbox fact schema is invalid")
            fact = _mapping(content["fact"])
            if fact.get("account_id") != self.config.account_id:
                raise QmtEventWireError("QMT outbox fact belongs to another account")
            if content["kind"] == "GAP":
                if set(fact) != {
                    "evidence_id",
                    "account_id",
                    "producer_id",
                    "first_rejected_sequence",
                    "last_rejected_sequence",
                    "rejected_count",
                    "observed_monotonic_ns",
                }:
                    raise QmtEventWireError("QMT gap fact fields are invalid")
                return QmtBridgeGapDelivery(
                    delivery_id,
                    CallbackGapEvidence(
                        evidence_id=require_text(fact["evidence_id"], "gap id"),
                        account_id=self.config.account_id,
                        producer_id=require_text(fact["producer_id"], "gap producer"),
                        first_rejected_sequence=require_nonnegative(
                            fact["first_rejected_sequence"], "gap first"
                        ),
                        last_rejected_sequence=require_nonnegative(
                            fact["last_rejected_sequence"], "gap last"
                        ),
                        rejected_count=require_positive(fact["rejected_count"], "gap count"),
                        observed_monotonic_ns=require_nonnegative(
                            fact["observed_monotonic_ns"], "gap time"
                        ),
                    ),
                )
            optional_text = {
                "client_order_ref",
                "venue_order_id",
                "venue_cancel_id",
                "execution_id",
                "cancel_request_id",
                "trade_key",
            }
            if content["kind"] != "EVENT" or set(fact) != optional_text | {
                "external_event_id",
                "event_kind",
                "source_kind",
                "account_id",
                "trading_day",
                "received_monotonic_ns",
                "filled_qty",
                "leaves_qty",
                "error_category",
                "normalized_summary",
            }:
                raise QmtEventWireError("QMT event fact fields are invalid")
            texts = {
                key: None if fact[key] is None else require_text(fact[key], key)
                for key in optional_text
            }
            summary = {
                key: require_text(value, "event summary")
                for key, value in _mapping(fact["normalized_summary"]).items()
            }
            return QmtBridgeEventDelivery(
                delivery_id,
                QmtBridgeNativeEvent(
                    external_event_id=require_text(fact["external_event_id"], "event id"),
                    event_kind=BrokerEventKind(require_text(fact["event_kind"], "event kind")),
                    source_kind=BrokerSourceKind(require_text(fact["source_kind"], "source kind")),
                    account_id=self.config.account_id,
                    trading_day=TradingDayEpoch(require_text(fact["trading_day"], "trading day")),
                    received_monotonic_ns=require_nonnegative(
                        fact["received_monotonic_ns"], "event time"
                    ),
                    client_order_ref=texts["client_order_ref"],
                    venue_order_id=texts["venue_order_id"],
                    venue_cancel_id=texts["venue_cancel_id"],
                    execution_id=texts["execution_id"],
                    cancel_request_id=texts["cancel_request_id"],
                    trade_key=texts["trade_key"],
                    filled_qty=_optional_quantity(fact["filled_qty"]),
                    leaves_qty=_optional_quantity(fact["leaves_qty"]),
                    error_category=None
                    if fact["error_category"] is None
                    else ErrorCategory(require_text(fact["error_category"], "error category")),
                    normalized_summary=summary,
                ),
            )
        except (ValueError, QmtBridgeWireError) as error:
            raise QmtEventWireError("QMT outbox fact is invalid") from error


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise QmtEventWireError("QMT event field must be an object")
    return cast(dict[str, object], value)


def _optional_quantity(value: object) -> int | None:
    return None if value is None else require_nonnegative(value, "QMT event quantity")
