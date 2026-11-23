"""Bounded OMS/Journal operation seam for Consumer Gateway V2.

The socket-facing gateway may authorize and normalize a request, but it must
not turn that request into an adapter call itself.  This port is the one
in-process handoff to the M1 OMS/EMS lane: successful command results have
already crossed that lane's Journal-before-side-effect boundary, while read
results are Journal-derived projections rather than PostgreSQL substitutes.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..domain import ConsumerAccountBinding, ConsumerAuditTrail, ConsumerOrderEvent
from .base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerEventSubscription,
    ConsumerOrderSignal,
    ConsumerSession,
    IntentReceipt,
)


@runtime_checkable
class ConsumerGatewayOperationsV2(Protocol):
    """The durable command/read service used after Gateway authorization.

    Implementations own OMS lineage checks, the Journal commit and any later
    EMS dispatch.  They must not return a receipt until its accepted event is
    durable, and must not use a read-model database as audit evidence.
    """

    def submit_signal(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        strategy_binding_id: str,
        signal: ConsumerOrderSignal,
    ) -> IntentReceipt: ...

    def cancel_order(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        consumer_request_id: str,
        oms_order_id: str,
        reason: str,
    ) -> IntentReceipt: ...

    def get_order(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> ConsumerOrderEvent: ...

    def get_order_audit_trail(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]: ...

    def get_account_order_snapshot(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        reset_context: str | None,
    ) -> ConsumerAccountOrderSnapshotV2: ...

    def subscribe_order_events(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        cursor_token: str,
    ) -> ConsumerEventSubscription: ...
