"""Bounded replay cache projected directly from the committed mmap Journal.

This is an explicit no-writer, no-SQL fast path for the isolated replay plane.
It starts at the owner's already recovered fence, verifies every later Journal
frame through ``SegmentedJournalTail``, and keeps only a bounded suffix. Missing
predecessor state disables the cache so the canonical projection reader can
fail over to its verified cold path; it is never guessed or reconstructed from
an incomplete order history.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import Event, RLock, Thread

from ..domain import ConsumerEventKind, ConsumerOrderEvent
from .committed_replay_window import (
    CommittedReplayEntry,
    CommittedReplayPage,
    CommittedReplayWindow,
)
from .journal_tail import (
    JournalTailCursor,
    JournalTailSource,
    SegmentedJournalTail,
    VerifiedJournalBatch,
)
from .order_book import (
    OmsFactKind,
    OmsJournalFact,
    OmsOrderEvent,
    OmsOrderEventKind,
    OmsOrderIntent,
    _reservation_attributes,  # pyright: ignore[reportPrivateUsage]
    intent_hash_for,
)
from .order_projection import reduce_order_projection
from .recovery import OmsRecoverySeedV1


@dataclass(frozen=True, slots=True)
class _AdmissionBatch:
    facts: tuple[OmsJournalFact, ...]
    verified: VerifiedJournalBatch


class PublishedCommittedReplayBook:
    """One replay-process-owned verified Journal suffix and reducer cache."""

    def __init__(
        self,
        journal: JournalTailSource,
        *,
        event_capacity: int,
        checkpoint_capacity: int = 16_384,
    ) -> None:
        if (
            type(event_capacity) is not int
            or not 1 <= event_capacity <= 1_000_000
            or type(checkpoint_capacity) is not int
            or not 1 <= checkpoint_capacity <= 1_000_000
        ):
            raise ValueError("invalid published replay capacity")
        bound = journal.read_bound()
        self._journal = journal
        self._tail = SegmentedJournalTail(
            journal,
            cursor=JournalTailCursor(
                journal.journal_id,
                journal.profile_hash,
                bound.active_segment,
                bound.active_frames,
                bound.committed_lsn + 1,
                bound.next_batch_id,
            ),
            maximum_batch_facts=256,
            reuse_segment_views=True,
        )
        self._window = CommittedReplayWindow(
            capacity=event_capacity, floor=bound.committed_lsn
        )
        self._checkpoints: OrderedDict[str, bytes] = OrderedDict()
        self._admissions: OrderedDict[str, _AdmissionBatch] = OrderedDict()
        self._checkpoint_capacity = checkpoint_capacity
        self._lock = RLock()
        self._covered = True
        self._closed = False
        self._stopped = Event()
        self._failure: BaseException | None = None
        self._worker: Thread | None = None

    def committed_replay_uses_journal(self, journal: object) -> bool:
        return journal is self._journal

    @property
    def failed(self) -> bool:
        # Lost predecessor coverage deliberately stops this optional cache;
        # callers then use the authoritative projection.  An exception or an
        # unexpected exit while coverage is still claimed remains fatal.
        return self._failure is not None or (
            self._worker is not None
            and not self._closed
            and self._covered
            and not self._worker.is_alive()
        )

    @property
    def failure_kind(self) -> str | None:
        """A redacted diagnostic class, never exception text or business data."""
        if self._failure is not None:
            name = type(self._failure).__name__
            if name == "MmapOmsFactJournalRecoveryRequired":
                # All emitted values are allowlisted constants; paths, payloads,
                # identifiers and arbitrary exception text never enter logs.
                reasons = {
                    "tail source window is unavailable/nested": "WINDOW_UNAVAILABLE",
                    "tail source window changed namespace": "WINDOW_SCOPE_CHANGED",
                    "tail cursor is beyond committed Journal": "CURSOR_BEYOND_COMMIT",
                    "tail cursor does not match activation": "ACTIVATION_MISMATCH",
                    "tail verified resume anchor changed": "ANCHOR_CHANGED",
                    "tail cursor splits/changes a batch": "BATCH_ANCHOR_CHANGED",
                    "tail cursor batch anchor mismatch": "BATCH_ID_MISMATCH",
                    "tail segment activation chain is invalid": "SEGMENT_CHAIN_INVALID",
                    "tail successor skips facts/batches": "SUCCESSOR_SKIPPED",
                    "tail cursor is past sealed content": "SEALED_BOUNDARY_INVALID",
                    "tail batch is incomplete/oversized/corrupt": "BATCH_INVALID",
                    "tail batch member is corrupt": "BATCH_MEMBER_CORRUPT",
                    "Journal segment is not registered": "SEGMENT_UNREGISTERED",
                    "published Journal fence is invalid": "PUBLISHED_FENCE_INVALID",
                    "owner read fence failed/stale": "OWNER_FENCE_FAILED_OR_STALE",
                    "owner read fence is unavailable/torn": "OWNER_FENCE_UNAVAILABLE_OR_TORN",
                }
                return name + ":" + reasons.get(str(self._failure), "UNCLASSIFIED")
            return name
        return "UNEXPECTED_WORKER_EXIT" if self.failed else None

    def start(self) -> None:
        with self._lock:
            if self._closed or self._worker is not None:
                raise RuntimeError("published replay cache cannot restart")
            self._worker = Thread(
                target=self._pump,
                name="published-replay-journal-pump",
                daemon=True,
            )
            self._worker.start()

    def _pump(self) -> None:
        try:
            while not self._stopped.is_set():
                advanced = False
                # Freeze one complete owner-published prefix. A producer which
                # keeps committing cannot make this turn chase a moving tail.
                with self._tail.source_read_window():
                    while not self._stopped.is_set():
                        batch = self._tail.read_next()
                        if batch is None:
                            break
                        with self._lock:
                            self._apply(batch)
                            if not self._covered:
                                return
                        advanced = True
                if not advanced:
                    self._stopped.wait(0.001)
        except BaseException as error:
            self._failure = error
            self._stopped.set()

    def _apply(self, batch: VerifiedJournalBatch) -> None:
        facts = tuple(row.fact for row in batch.records)
        order_id = facts[0].oms_order_id
        if order_id is None or any(fact.oms_order_id != order_id for fact in facts):
            self._covered = False
            return
        projected: ConsumerOrderEvent | None
        if facts[0].kind is OmsFactKind.ORDER_ATTRIBUTED:
            projected = self._project_admission(facts, batch)
            self._admissions[order_id] = _AdmissionBatch(facts, batch)
            self._admissions.move_to_end(order_id)
            if len(self._admissions) > self._checkpoint_capacity:
                self._admissions.popitem(last=False)
        else:
            previous = self._checkpoints.get(order_id)
            if previous is None:
                admission = self._admissions.get(order_id)
                if admission is None:
                    self._covered = False
                    return
                previous = reduce_order_projection(
                    admission.facts,
                    admission.verified.commit,
                    previous=None,
                    maximum_order_events=64,
                ).checkpoint
            reduction = reduce_order_projection(
                facts, batch.commit, previous=previous, maximum_order_events=64
            )
            self._checkpoints[order_id] = reduction.checkpoint
            self._checkpoints.move_to_end(order_id)
            if len(self._checkpoints) > self._checkpoint_capacity:
                self._checkpoints.popitem(last=False)
            projected = reduction.event
        if projected is not None:
            execution_id = projected.execution_id
            if execution_id is None:
                raise RuntimeError("published replay event has no execution identity")
            event_id = projected.consumer_event_id.removeprefix("order:")
            event = OmsOrderEvent(
                event_id,
                (
                    OmsOrderEventKind.ORDER_ACCEPTED
                    if projected.order_revision == 0
                    else OmsOrderEventKind.ORDER_STATE_CHANGED
                ),
                projected.account_id,
                projected.oms_order_id,
                execution_id,
                projected.order_state,
                projected.order_revision,
                projected.journal_id,
                projected.journal_lsn,
                facts[-1].fact_id,
            )
            self._window.append(
                CommittedReplayEntry(event, projected.consumer_trace, projected.trading_day)
            )
        if batch.commit.final_lsn > self._window.high:
            self._window.advance(batch.commit.final_lsn)

    @staticmethod
    def _project_admission(
        facts: tuple[OmsJournalFact, ...], batch: VerifiedJournalBatch
    ) -> ConsumerOrderEvent:
        first = facts[0]
        if (
            tuple(fact.kind for fact in facts)
            != (OmsFactKind.ORDER_ATTRIBUTED, OmsFactKind.RESERVATION_CREATED)
            or first.recovery_payload is None
        ):
            raise RuntimeError("published replay admission envelope is incomplete")
        order = OmsRecoverySeedV1.decode_and_verify(first.recovery_payload).order
        execution = order.execution
        recovered_intent = OmsOrderIntent(
            consumer_trace=order.order.consumer_trace,
            trading_day=order.order.trading_day,
            symbol=execution.symbol,
            market=execution.market,
            side=execution.side,
            price_type=execution.price_type,
            business_type=execution.business_type,
            quantity=execution.quantity,
            protection_price_ticks=execution.protection_price_ticks,
            limit_price_ticks=execution.limit_price_ticks,
            execution_parameters=execution.execution_parameters,
        )
        expected_attribution = {
            "intent_hash": order.order.intent_hash,
            "attribution_binding_id": order.order.consumer_trace.attribution_binding_id,
            "binding_version": str(order.order.consumer_trace.binding_version),
            "decision_context_id": order.decision_context.decision_context_id,
            "risk_snapshot_id": order.decision_context.risk_snapshot_id,
        }
        if (
            order.order.account_id != first.account_id
            or order.order.oms_order_id != first.oms_order_id
            or order.execution.execution_id != first.execution_id
            or intent_hash_for(recovered_intent) != order.order.intent_hash
            or dict(first.attributes) != expected_attribution
            or dict(facts[1].attributes) != dict(_reservation_attributes(order.reservation))
        ):
            raise RuntimeError("published replay admission identity changed")
        return ConsumerOrderEvent(
            consumer_event_id=(
                f"order:oms-event:{batch.commit.journal_id}:{batch.commit.final_lsn}"
            ),
            event_kind=ConsumerEventKind.ORDER,
            schema_version=2,
            account_id=order.order.account_id,
            trading_day=order.order.trading_day,
            journal_id=batch.commit.journal_id,
            journal_lsn=batch.commit.final_lsn,
            consumer_trace=order.order.consumer_trace,
            oms_order_id=order.order.oms_order_id,
            execution_id=order.execution.execution_id,
            order_state=order.order.state,
            order_revision=order.order.order_revision,
        )

    def committed_replay_page(
        self, scope: tuple[str, str, str], *, after: int, high: int, limit: int
    ) -> CommittedReplayPage | None:
        with self._lock:
            self._require_running()
            if not self._covered:
                return None
            return self._window.page(scope, after=after, high=high, limit=limit)

    def committed_replay_bounds(
        self,
        scope: tuple[str, str, str],
        *,
        after: int,
        observed_head: int,
        partitions: int,
    ) -> tuple[tuple[int, int, int], ...] | None:
        with self._lock:
            self._require_running()
            if not self._covered:
                return None
            return self._window.bounds(
                scope,
                after=after,
                observed_head=observed_head,
                partitions=partitions,
            )

    def _require_running(self) -> None:
        if self._closed or self._worker is None or self.failed:
            raise RuntimeError("published replay cache is unavailable") from self._failure

    def close(self) -> None:
        self._stopped.set()
        worker = self._worker
        if worker is not None:
            worker.join(5)
        with self._lock:
            if not self._closed:
                self._closed = True
                self._tail.close()
                self._checkpoints.clear()
                self._admissions.clear()
        if worker is not None and worker.is_alive():
            raise RuntimeError("published replay cache did not stop")
