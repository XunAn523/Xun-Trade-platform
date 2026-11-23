"""Bounded Journal-backed audit verification; read indexes supply hints only."""

from __future__ import annotations

from dataclasses import dataclass

from ..domain import ConsumerAuditTrail
from .journal_tail import JournalTailCursor, JournalTailSource, SegmentedJournalTail
from .order_projection import reduce_order_projection
from .recovery import OmsRecoverySeedV1
from .recovery_checkpoint import decode_checkpoint


class JournalAuditUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class JournalAuditAnchor:
    before: JournalTailCursor
    final_lsn: int
    sha256: str


@dataclass(frozen=True)
class JournalOrderAuditPlan:
    checkpoint: bytes
    anchors: tuple[JournalAuditAnchor, ...]


def verify_order_audit(
    journal: JournalTailSource,
    plan: JournalOrderAuditPlan,
    *,
    scope: tuple[str, str, str],
    oms_order_id: str,
    maximum_batches: int = 128,
    maximum_facts: int = 512,
    maximum_order_events: int = 64,
) -> tuple[ConsumerAuditTrail, ...]:
    """Re-read complete physical batches and reproduce the bounded checkpoint.

    Only normalized OMS lifecycle events (the existing audit contract) are
    returned. This does not attest to all raw SDK/account/decision facts, or to
    persistence across power loss. No audit result authorizes dispatch/recovery.
    """
    if (
        type(maximum_batches) is not int
        or not 1 <= maximum_batches <= 256
        or type(maximum_facts) is not int
        or not 1 <= maximum_facts <= 4096
        or type(maximum_order_events) is not int
        or not 1 <= maximum_order_events <= 1024
        or not 1 <= len(plan.anchors) <= maximum_batches
    ):
        raise JournalAuditUnavailable("audit verification capacity is invalid")
    expected = decode_checkpoint(plan.checkpoint, maximum_bytes=262144)
    if len(expected.orders) != 1 or not 1 <= len(expected.events) <= maximum_order_events:
        raise JournalAuditUnavailable("audit checkpoint scope/capacity is invalid")
    seed = OmsRecoverySeedV1.decode_and_verify(expected.orders[0].admission_seed).order
    trace = seed.order.consumer_trace
    if (
        expected.journal_id != journal.journal_id
        or seed.order.oms_order_id != oms_order_id
        or (trace.consumer_id, trace.consumer_account_id, seed.order.account_id) != scope
    ):
        raise JournalAuditUnavailable("audit checkpoint belongs to another order")
    previous = None
    used = 0
    last_lsn = 0
    for anchor in plan.anchors:
        count = anchor.final_lsn - anchor.before.next_lsn + 1
        if (
            count < 1
            or used + count > maximum_facts
            or anchor.before.next_lsn <= last_lsn
            or anchor.final_lsn > expected.watermark
        ):
            raise JournalAuditUnavailable("audit hint capacity/order is invalid")
        tail = SegmentedJournalTail(journal, cursor=anchor.before, maximum_batch_facts=256)
        try:
            batch = tail.read_next()
            if (
                batch is None
                or batch.after.next_lsn - 1 != anchor.final_lsn
                or batch.sha256 != anchor.sha256
                or len(batch.records) != count
                or any(
                    (r.fact.account_id, r.fact.oms_order_id, r.fact.execution_id)
                    != (scope[2], oms_order_id, seed.execution.execution_id)
                    for r in batch.records
                )
            ):
                raise JournalAuditUnavailable("audit Journal batch cannot be verified")
            result = reduce_order_projection(
                tuple(row.fact for row in batch.records),
                batch.commit,
                previous=previous,
                maximum_order_events=maximum_order_events,
            )
            previous = result.checkpoint
            used += count
            last_lsn = anchor.final_lsn
        finally:
            tail.close()
    if previous is None:
        raise JournalAuditUnavailable("audit Journal history is absent")
    actual = decode_checkpoint(previous, maximum_bytes=262144)
    if actual != expected:
        raise JournalAuditUnavailable("audit Journal replay differs from its index hint")
    return tuple(
        ConsumerAuditTrail(
            audit_id=f"audit:{event.event_id}",
            consumer_id=scope[0],
            account_id=scope[2],
            oms_order_id=oms_order_id,
            execution_id=event.execution_id,
            journal_id=actual.journal_id,
            journal_lsn=event.journal_lsn,
            action=event.kind,
        )
        for event in actual.events
    )
