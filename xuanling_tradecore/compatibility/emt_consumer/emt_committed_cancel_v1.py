"""Verify an exact OMS cancellation batch before preparing an EMT cancel.

This verifier does not submit, retry or prove the remote outcome. In
particular, a committed request is not evidence that a native cancel was or
was not invoked before a crash.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...domain import OmsOrderState
from ...domain._validation import require_text
from ...oms import (
    MmapOmsFactJournalV1,
    OmsFactKind,
    OmsJournalCommit,
    OmsOrderBookError,
    OmsOrderEvent,
    OmsOrderEventKind,
    OrderBookOrder,
)


class EmtCommittedCancelErrorV1(RuntimeError):
    """The cancellation is not backed by its exact committed OMS batch."""


@dataclass(frozen=True)
class EmtCommittedCancelV1:
    commit: OmsJournalCommit
    order: OrderBookOrder
    cancel_request_id: str
    reason: str


def verify_emt_committed_cancel_v1(
    *,
    journal: MmapOmsFactJournalV1,
    event: OmsOrderEvent,
    order: OrderBookOrder,
    cancel_request_id: str,
    reason: str,
) -> EmtCommittedCancelV1:
    """Verify framing, attribution and the exact original request payload."""

    try:
        request_id = require_text(cancel_request_id, "cancel request id", max_length=128)
        reason = require_text(reason, "cancel reason", max_length=256)
        if (
            event.kind is not OmsOrderEventKind.ORDER_STATE_CHANGED
            or event.order_state is not OmsOrderState.CANCEL_PENDING
            or event.journal_lsn < 2
            or event.order_revision != order.order.order_revision + 1
            or event.account_id != order.order.account_id
            or event.oms_order_id != order.order.oms_order_id
            or event.execution_id != order.execution.execution_id
            or event.event_id != f"oms-event:{event.journal_id}:{event.journal_lsn}"
            or event.source_key != f"oms:{event.oms_order_id}:state:{event.order_revision}"
        ):
            raise EmtCommittedCancelErrorV1("cancel event identity is invalid")
        commit = OmsJournalCommit(event.journal_id, event.journal_lsn - 1, event.journal_lsn, 2)
        rows = journal.read_committed_batch(commit)
        first, second = (row.fact for row in rows)
        expected = {
            "from": order.order.state.value,
            "to": OmsOrderState.CANCEL_PENDING.value,
            "cancel_request_id": request_id,
            "reason": reason,
        }
        if (
            first.kind is not OmsFactKind.CANCEL_REQUESTED
            or second.kind is not OmsFactKind.ORDER_STATE_CHANGED
            or dict(first.attributes) != expected
            or dict(second.attributes) != expected
            or any(
                fact.account_id != event.account_id
                or fact.oms_order_id != event.oms_order_id
                or fact.execution_id != event.execution_id
                or fact.recovery_payload is not None
                for fact in (first, second)
            )
        ):
            raise EmtCommittedCancelErrorV1("cancel batch does not match the request")
        return EmtCommittedCancelV1(commit, order, request_id, reason)
    except (OmsOrderBookError, ValueError) as exc:
        raise EmtCommittedCancelErrorV1("cancel batch could not be verified") from exc
