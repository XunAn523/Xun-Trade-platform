"""Verify an OMS admission before constructing an EMT pre-native route.

This is an opt-in execution-worker primitive, not a dispatch authorization or
recovery certificate. The current benchmark Consumer does not persist the
required admission seed and must not use it for live dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...oms import (
    MmapOmsFactJournalV1,
    OmsFactJournal,
    OmsFactKind,
    OmsJournalCommit,
    OmsJournalFact,
    OmsOrderBookError,
    OmsRecoverySeedV1,
    OrderBook,
    OrderBookOrder,
)


class EmtCommittedAdmissionErrorV1(RuntimeError):
    """The consumer admission cannot be used to construct an EMT route."""


@dataclass(frozen=True)
class EmtCommittedAdmissionV1:
    """A revalidated original journal receipt and its complete OMS seed."""

    commit: OmsJournalCommit
    facts: tuple[OmsJournalFact, OmsJournalFact]
    seed: OmsRecoverySeedV1


class _NoWrites(OmsFactJournal):
    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        raise EmtCommittedAdmissionErrorV1("admission verifier must never append")


def verify_emt_committed_admission_v1(
    *, journal: MmapOmsFactJournalV1, commit: OmsJournalCommit, expected: OrderBookOrder
) -> EmtCommittedAdmissionV1:
    """Verify exact committed frames, canonical seed, attribution and reservation.

    Use the existing OMS replay validator on an isolated no-write book. The
    validator requires replay positions beginning at one, so its *local*
    receipt uses normalized positions; the returned proof retains the original
    validated journal ID and positions. This does not prove device-level fsync,
    a broker session, native dispatch, replay completeness or READY.
    """

    try:
        rows = journal.read_committed_batch(commit)
        if len(rows) != 2:
            raise EmtCommittedAdmissionErrorV1("admission requires exactly two facts")
        first, reservation = rows[0].fact, rows[1].fact
        if (
            first.kind is not OmsFactKind.ORDER_ATTRIBUTED
            or reservation.kind is not OmsFactKind.RESERVATION_CREATED
            or first.recovery_payload is None
        ):
            raise EmtCommittedAdmissionErrorV1("admission lacks canonical recovery evidence")
        seed = OmsRecoverySeedV1.decode_and_verify(first.recovery_payload)
        replay_book = OrderBook(journal=_NoWrites(), persist_recovery_seed=True)
        replay_book.restore_committed_batch(
            (first, reservation), OmsJournalCommit(commit.journal_id, 1, 2, 2)
        )
        if seed.order != expected or replay_book.order(seed.order.order.oms_order_id) != expected:
            raise EmtCommittedAdmissionErrorV1("committed admission does not match queued order")
        return EmtCommittedAdmissionV1(commit, (first, reservation), seed)
    except (OmsOrderBookError, ValueError) as exc:
        raise EmtCommittedAdmissionErrorV1("OMS admission verification failed") from exc
