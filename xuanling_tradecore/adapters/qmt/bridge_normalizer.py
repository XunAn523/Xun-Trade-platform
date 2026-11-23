"""QMT Edge delivery normalization and duplicate/conflict protection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256

from ...domain import BrokerEvent, VenueOrderIdentity
from ..base import BrokerAdapterError
from .bridge_protocol import QmtBridgeNativeEvent


class QmtBridgeEventConflictError(BrokerAdapterError):
    """An Edge external event id was reused for a different normalized fact."""


@dataclass(frozen=True)
class QmtBridgeEventNormalizer:
    """Construct domain events without preserving QMT SDK payload objects."""

    adapter_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    account_id: str

    def normalize(self, event: QmtBridgeNativeEvent) -> BrokerEvent:
        if event.account_id != self.account_id:
            raise QmtBridgeEventConflictError("QMT Edge event belongs to another account")
        identity = (
            VenueOrderIdentity(
                venue_id="QMT",
                adapter_instance_id=self.adapter_instance_id,
                adapter_client_scope=self.adapter_client_scope,
                client_order_ref=event.client_order_ref,
                venue_order_id=event.venue_order_id,
                venue_cancel_id=event.venue_cancel_id,
            )
            if any((event.client_order_ref, event.venue_order_id, event.venue_cancel_id))
            else None
        )
        return BrokerEvent(
            event_id=f"qmt:{event.external_event_id}",
            schema_version=1,
            source_key_schema_version=1,
            source_key=self._source_key(event.external_event_id),
            event_kind=event.event_kind,
            source_kind=event.source_kind,
            venue_id="QMT",
            adapter_instance_id=self.adapter_instance_id,
            account_id=event.account_id,
            trading_day=event.trading_day,
            received_monotonic_ns=event.received_monotonic_ns,
            venue_order_identity=identity,
            execution_id=event.execution_id,
            cancel_request_id=event.cancel_request_id,
            trade_key=event.trade_key,
            filled_qty=event.filled_qty,
            leaves_qty=event.leaves_qty,
            error_category=event.error_category,
            normalized_summary=event.normalized_summary,
        )

    @staticmethod
    def fingerprint(event: QmtBridgeNativeEvent) -> str:
        """Hash only normalized values, so raw vendor payloads never enter state."""

        payload = {
            "event_kind": event.event_kind.value,
            "source_kind": event.source_kind.value,
            "account_id": event.account_id,
            "trading_day": event.trading_day.value,
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
        return sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _source_key(external_event_id: str) -> str:
        digest = sha256(f"qmt-edge-event:{external_event_id}".encode("utf-8")).hexdigest()
        return f"Q1:{digest}"
