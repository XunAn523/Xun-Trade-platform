"""Bounded single-order reduction of a contiguous committed Journal batch.

The persistent read worker loads only the affected order, not all historical
orders. Semantic checks are the existing recovery reducer's checks, not a second
looser state machine. This module cannot append a fact or call an Adapter.
"""

from __future__ import annotations

from dataclasses import dataclass

import msgspec

from ..domain import ConsumerEventKind, ConsumerOrderEvent
from .order_book import (
    OmsFactKind,
    OmsJournalCommit,
    OmsJournalFact,
    OmsOrderBookError,
    OmsOrderEvent,
    OrderBook,
    OrderBookOrder,
)
from .recovery import OmsRecoverySeedV1
from .recovery_checkpoint import OmsCheckpointState, decode_checkpoint
from .verified_seed_cache import AdmissionSeedTransaction


class _NoWrites:
    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        raise OmsOrderBookError("read projection attempted a Journal write")


class _ProjectionBook(OrderBook):
    def __init__(
        self,
        *,
        reuse_seed: bool = False,
        seed_transaction: AdmissionSeedTransaction | None = None,
        seed_identity: tuple[str, str | None] | None = None,
    ) -> None:
        super().__init__(journal=_NoWrites(), persist_recovery_seed=True)
        self._reuse_seed = reuse_seed
        self._seed_initial: OrderBookOrder | None = None
        self._seed_encoded: bytes | None = None
        self._seed_transaction, self._seed_identity = seed_transaction, seed_identity

    def _decode_recovery_admission(self, encoded: bytes) -> OmsRecoverySeedV1:
        if self._seed_transaction is None:
            return super()._decode_recovery_admission(encoded)
        if not self._reuse_seed or self._seed_identity is None or not self._seed_identity[1]:
            raise OmsOrderBookError("admission proof projection identity is missing")
        return self._seed_transaction.resolve(
            encoded, account_id=self._seed_identity[0], oms_order_id=self._seed_identity[1]
        )

    def _checkpoint_admission_seed(self, initial: OrderBookOrder) -> bytes:
        if not self._reuse_seed:
            return super()._checkpoint_admission_seed(initial)
        if self._seed_initial is None:
            encoded = (
                self._seed_encoded
                if self._seed_encoded is not None
                else super()._checkpoint_admission_seed(initial)
            )
            self._seed_initial, self._seed_encoded = initial, encoded
            return encoded
        # Retain ONLY the original immutable admission instance in this one
        # transaction-owned single-order book, never a caller-provided key/cache.
        if initial is not self._seed_initial or self._seed_encoded is None:
            raise OmsOrderBookError("transaction projection admission instance changed")
        return self._seed_encoded

    def remember_verified_seed(self, encoded: bytes) -> None:
        """Only AFTER the original canonical decoder/reducer has succeeded.

        Its decoder already re-builds and compares these exact bytes. Do not
        re-build that same immutable admission again for checkpoint export.
        This remains private to one no-write, single-order transaction book.
        """
        if (
            not self._reuse_seed
            or not self._recovered_journal_id
            or len(self._accounts) != 1
            or type(encoded) is not bytes
            or not 0 < len(encoded) <= 64 * 1024
            or (self._seed_encoded is not None and self._seed_encoded != encoded)
        ):
            raise OmsOrderBookError("transaction verified admission seed changed")
        account = next(iter(self._accounts.values()))
        if len(account.orders) != 1 or len(account.deduplication) != 1:
            raise OmsOrderBookError("transaction seed view is not single-order")
        self._seed_encoded = encoded

    def restore_committed_batch(
        self, facts: tuple[OmsJournalFact, ...], commit: OmsJournalCommit
    ) -> OmsOrderEvent | None:
        event = super().restore_committed_batch(facts, commit)
        if self._reuse_seed and facts[0].kind is OmsFactKind.ORDER_ATTRIBUTED:
            # Original replay has just canonical-verified and identity-bound
            # this admission. Failed replay never reaches the cache assignment.
            encoded = facts[0].recovery_payload
            if encoded is None:
                raise OmsOrderBookError("verified admission has no recovery seed")
            self.remember_verified_seed(encoded)
        return event

    def advance_foreign_order_gap(self, commit: OmsJournalCommit) -> None:
        if (
            not isinstance(self._journal, _NoWrites)
            or self._live_started
            or self._recovered_journal_id != commit.journal_id
            or commit.first_lsn <= self._recovered_lsn
        ):
            raise OmsOrderBookError("transaction projection range/lifecycle changed")
        # This read-only single-order view has no state for foreign orders. The
        # enclosing store proves all skipped global batches, just as the reference
        # reducer replaces its persisted checkpoint watermark before each replay.
        self._recovered_lsn = commit.first_lsn - 1


@dataclass(frozen=True)
class OrderProjectionReduction:
    checkpoint: bytes
    latest: ConsumerOrderEvent
    event: ConsumerOrderEvent | None


def reduce_order_projection(
    facts: tuple[OmsJournalFact, ...],
    commit: OmsJournalCommit,
    *,
    previous: bytes | None,
    maximum_order_events: int = 64,
) -> OrderProjectionReduction:
    _validate_input(facts, commit, maximum_order_events)
    book = _restore_projection_book(facts, commit, previous, maximum_order_events)
    return _reduce_and_export(book, facts, commit, maximum_order_events)


def _validate_input(
    facts: tuple[OmsJournalFact, ...], commit: OmsJournalCommit, maximum_order_events: int
) -> None:
    if (
        type(maximum_order_events) is not int
        or not 1 <= maximum_order_events <= 1024
        or not facts
        or len(facts) != commit.fact_count
        or commit.first_lsn < 1
    ):
        raise OmsOrderBookError("projection batch/capacity is invalid")


def _restore_projection_book(
    facts: tuple[OmsJournalFact, ...],
    commit: OmsJournalCommit,
    previous: bytes | None,
    maximum_order_events: int,
    *,
    reuse_seed: bool = False,
    seed_transaction: AdmissionSeedTransaction | None = None,
) -> _ProjectionBook:
    book = _ProjectionBook(
        reuse_seed=reuse_seed,
        seed_transaction=seed_transaction,
        seed_identity=(facts[0].account_id, facts[0].oms_order_id),
    )
    if previous is not None:
        state = decode_checkpoint(previous, maximum_bytes=256 * 1024)
        if (
            state.journal_id != commit.journal_id
            or state.watermark >= commit.first_lsn
            or len(state.orders) != 1
            or len(state.events) > maximum_order_events
            or any(event.journal_lsn > state.watermark for event in state.events)
        ):
            raise OmsOrderBookError("projection predecessor scope/capacity is invalid")
        if not reuse_seed:
            initial = OmsRecoverySeedV1.decode_and_verify(state.orders[0].admission_seed).order
            if (
                initial.order.oms_order_id != facts[0].oms_order_id
                or initial.order.account_id != facts[0].account_id
            ):
                raise OmsOrderBookError("projection predecessor belongs to another order")
        state = msgspec.structs.replace(state, watermark=commit.first_lsn - 1)
        book.restore_recovery_checkpoint(
            msgspec.json.encode(state), expected_journal_id=commit.journal_id
        )
        if reuse_seed:
            # The ORIGINAL restore above already canonical-validates the seed
            # and every state/event/reservation invariant. Bind that verified
            # view now, rather than decoding the same external seed twice.
            key = facts[0].oms_order_id
            if key is None or book.order(key).order.account_id != facts[0].account_id:
                raise OmsOrderBookError("projection predecessor belongs to another order")
            book.remember_verified_seed(state.orders[0].admission_seed)
    elif commit.first_lsn > 1:
        book.restore_recovery_checkpoint(
            msgspec.json.encode(
                OmsCheckpointState(1, commit.journal_id, commit.first_lsn - 1, (), ())
            ),
            expected_journal_id=commit.journal_id,
        )
    return book


def _reduce_and_export(
    book: OrderBook,
    facts: tuple[OmsJournalFact, ...],
    commit: OmsJournalCommit,
    maximum_order_events: int,
) -> OrderProjectionReduction:
    event = book.restore_committed_batch(facts, commit)
    encoded = book.export_recovery_checkpoint()
    state = decode_checkpoint(encoded, maximum_bytes=256 * 1024)
    if len(state.orders) != 1 or not 1 <= len(state.events) <= maximum_order_events:
        raise OmsOrderBookError("projection order history exceeds its bounded checkpoint")
    order_id = facts[0].oms_order_id
    if order_id is None:
        raise OmsOrderBookError("projection has no order identity")
    current = book.order(order_id)
    latest = event or book.last_order_event(order_id)
    projected = ConsumerOrderEvent(
        f"order:{latest.event_id}",
        ConsumerEventKind.ORDER,
        2,
        latest.account_id,
        current.order.trading_day,
        latest.journal_id,
        latest.journal_lsn,
        current.order.consumer_trace,
        latest.oms_order_id,
        latest.execution_id,
        latest.order_state,
        latest.order_revision,
    )
    return OrderProjectionReduction(encoded, projected, projected if event is not None else None)


class TransactionOrderProjection:
    """One bounded order reducer, owned and discarded by ONE SQL transaction.

    Initial persisted bytes undergo the original full checkpoint/seed checks.
    Later states come only from that same reducer, not SQL or a mutable external
    cache. The store must still verify every global batch/CRC/fingerprint/range.
    This object has a no-writes Journal and can never become a trading book.
    """

    def __init__(
        self,
        *,
        previous: bytes | None,
        maximum_order_events: int,
        seed_transaction: AdmissionSeedTransaction | None = None,
    ):
        self._previous, self._maximum = previous, maximum_order_events
        self._book: _ProjectionBook | None = None
        self._scope: tuple[str, str, str | None, str | None] | None = None
        self._failed = False
        self._seed_transaction = seed_transaction

    def reduce(
        self, facts: tuple[OmsJournalFact, ...], commit: OmsJournalCommit
    ) -> OrderProjectionReduction:
        if self._failed:
            raise OmsOrderBookError("transaction projection reducer previously failed")
        try:
            _validate_input(facts, commit, self._maximum)
            first = facts[0]
            scope = (commit.journal_id, first.account_id, first.oms_order_id, first.execution_id)
            if self._book is None:
                self._book = _restore_projection_book(
                    facts,
                    commit,
                    self._previous,
                    self._maximum,
                    reuse_seed=True,
                    seed_transaction=self._seed_transaction,
                )
                self._scope = scope
                self._previous = None  # Do not retain a second historical byte cache.
            else:
                book = self._book
                if scope != self._scope:
                    raise OmsOrderBookError("transaction projection order/range changed")
                # Same single-order view as the reference reducer's checkpoint
                # watermark replacement. Foreign-order source batches have been
                # validated/applied by the enclosing store before reaching here.
                # Never expose this optimization on the live/recovery writer API.
                book.advance_foreign_order_gap(commit)
            return _reduce_and_export(self._book, facts, commit, self._maximum)
        except BaseException:
            self._failed = True
            self._book = None
            self._previous = None
            raise
