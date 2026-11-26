"""Opt-in bounded live OMS state with exact cold handoff, not a runtime gate.

Full historical reads and startup recovery require the long-runtime read model;
this live reducer must not be wired to the benchmark's full-history projection.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from itertools import islice
from threading import RLock

import msgspec

from ..domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerStrategyBinding,
    DecisionContext,
    OmsOrderState,
    RiskSnapshot,
)
from ..domain._validation import require_nonnegative
from .cold_intent import (
    ColdArchiveReceipt,
    ColdIntentKey,
    ColdIntentResolution,
    ColdIntentStore,
)
from .committed_replay_window import (
    CommittedReplayEntry,
    CommittedReplayPage,
    CommittedReplayWindow,
)
from .order_book import (
    IntentAdmission,
    IntentAdmissionOutcome,
    OmsFactJournal,
    OmsFactKind,
    OmsIntentKey,
    OmsJournalCommit,
    OmsJournalFact,
    OmsOrderBookError,
    OmsOrderEvent,
    OmsOrderEventKind,
    OmsOrderIntent,
    OrderBook,
    OrderBookOrder,
    ReservationRequirement,
    intent_hash_for,
)
from .recovery import OmsRecoverySeedV1
from .recovery_checkpoint import (
    EventCheckpointRow,
    OmsCheckpointState,
    OrderCheckpointRow,
    decode_checkpoint,
)


class ColdIntentLookupRequired(OmsOrderBookError):
    def __init__(self, key: ColdIntentKey) -> None:
        super().__init__("exact cold lookup/revalidation required before admission")
        self.key = key


class OmsHotCapacityFull(OmsOrderBookError):
    pass


class OmsHistoryRequiresColdReader(OmsOrderBookError):
    pass


@dataclass(frozen=True)
class TerminalArchiveCandidate:
    initial: OrderBookOrder
    current: OrderBookOrder
    events: tuple[OmsOrderEvent, ...]
    _encoded: bytes | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def key(self) -> ColdIntentKey:
        trace = self.initial.order.consumer_trace
        return ColdIntentKey(
            self.initial.order.account_id, trace.strategy_id or "", trace.strategy_order_id or ""
        )

    def encode(self) -> bytes:
        """Run in maintenance, after immutable references were copied under the lock."""
        if self._encoded is not None:
            return self._encoded
        current, last = self.current, self.events[-1]
        encoded = msgspec.json.encode(
            OmsCheckpointState(
                1,
                last.journal_id,
                last.journal_lsn,
                (
                    OrderCheckpointRow(
                        OmsRecoverySeedV1.build(self.initial).encoded,
                        current.order.state.value,
                        current.order.order_revision,
                        current.filled_quantity,
                        tuple(sorted(current.trade_keys)),
                        current.reservation.reservation_revision,
                    ),
                ),
                tuple(
                    EventCheckpointRow(
                        e.event_id,
                        e.kind.value,
                        e.account_id,
                        e.oms_order_id,
                        e.execution_id,
                        e.order_state.value,
                        e.order_revision,
                        e.journal_lsn,
                        e.source_key,
                    )
                    for e in self.events
                ),
            )
        )
        object.__setattr__(self, "_encoded", encoded)
        return encoded


class RetainedOrderBook(OrderBook):
    def __init__(
        self,
        *,
        journal: OmsFactJournal,
        cold_index: ColdIntentStore,
        maximum_hot_orders: int = 50_000,
        replay_cache_events: int = 16_384,
        maximum_order_events: int = 64,
        maximum_accounts: int = 64,
    ) -> None:
        if any(
            type(v) is not int or v < 1
            for v in (
                maximum_hot_orders,
                replay_cache_events,
                maximum_order_events,
                maximum_accounts,
            )
        ):
            raise OmsOrderBookError("retained OMS capacity is invalid")
        if maximum_order_events < 3:
            raise OmsOrderBookError("order capacity must reserve cancel and terminal evidence")
        super().__init__(journal=journal, persist_recovery_seed=True)
        self._cold = cold_index
        self._maximum = maximum_hot_orders
        self._cache_events, self._maximum_order_events = replay_cache_events, maximum_order_events
        self._retention_lock = RLock()
        self._hot_accounts: dict[str, str] = {}
        self._maximum_accounts = maximum_accounts
        self._recovery_imported = False
        self._histories: dict[str, list[OmsOrderEvent]] = {}
        self._event_counts: dict[str, int] = {}
        self._consumer_replay_enabled = False
        self._consumer_replay_failed = False
        self._consumer_replay: dict[str, CommittedReplayWindow] = {}

    def enable_committed_consumer_replay(self) -> None:
        """Startup-only opt-in, after verified recovery and before live writes."""
        if self._live_started or self._consumer_replay_enabled:
            raise OmsOrderBookError("replay window requires pre-live initialization")
        if self._cache_events > 1_000_000:
            raise OmsOrderBookError("replay window exceeds its bounded capacity")
        self._consumer_replay_enabled = True

    def committed_replay_uses_journal(self, journal: OmsFactJournal) -> bool:
        return self._journal is journal

    def committed_replay_page(
        self, scope: tuple[str, str, str], *, after: int, high: int, limit: int
    ) -> CommittedReplayPage | None:
        with self._accounts_lock:
            account = self._accounts.get(scope[2])
        if account is None:
            return None
        with account.lock:
            window = self._consumer_replay.get(scope[2])
            if not self._consumer_replay_enabled or self._consumer_replay_failed or window is None:
                return None
            return window.page(scope, after=after, high=high, limit=limit)

    def committed_replay_bounds(
        self, scope: tuple[str, str, str], *, after: int, observed_head: int, partitions: int
    ) -> tuple[tuple[int, int, int], ...] | None:
        with self._accounts_lock:
            account = self._accounts.get(scope[2])
        if account is None:
            return None
        with account.lock:
            window = self._consumer_replay.get(scope[2])
            if not self._consumer_replay_enabled or self._consumer_replay_failed or window is None:
                return None
            return window.bounds(
                scope, after=after, observed_head=observed_head, partitions=partitions
            )

    def _account(self, account_id: str):
        with self._accounts_lock:
            if account_id not in self._accounts and len(self._accounts) >= self._maximum_accounts:
                raise OmsHotCapacityFull("OMS account capacity reached before Journal commit")
            account = super()._account(account_id)
        with account.lock:
            if isinstance(account.events, list):
                if account.events:
                    raise OmsHistoryRequiresColdReader("cannot convert an unbounded loaded account")
                account.events = deque(maxlen=self._cache_events)
        return account

    def admit(
        self,
        *,
        intent: OmsOrderIntent,
        account_binding: ConsumerAccountBinding,
        strategy_binding: ConsumerStrategyBinding,
        attribution_binding: AttributionBinding,
        risk_snapshot: RiskSnapshot,
        decision_context: DecisionContext,
        reservation_requirement: ReservationRequirement,
        now_monotonic_ns: int,
        cold_resolution: ColdIntentResolution | None = None,
    ) -> IntentAdmission:
        require_nonnegative(now_monotonic_ns, "OMS admission time")
        self._validate_admission_inputs(
            intent,
            account_binding,
            strategy_binding,
            attribution_binding,
            risk_snapshot,
            decision_context,
            reservation_requirement,
            now_monotonic_ns,
        )
        # The exact immutable intent is hashed once, outside the account lock.
        # A resumed cold request enters admit again and repeats all validation.
        intent_hash = intent_hash_for(intent)
        account = self._account(intent.account_id)
        key = ColdIntentKey(
            intent.account_id,
            intent.consumer_trace.strategy_id or "",
            intent.consumer_trace.strategy_order_id or "",
        )
        with account.lock:
            hot_key = OmsIntentKey(key.account_id, key.strategy_id, key.strategy_order_id)
            if hot_key not in account.deduplication:
                resolution = (
                    cold_resolution if cold_resolution is not None else self._cold.probe(key)
                )
                if (
                    resolution is None
                    or resolution.key != key
                    or not self._cold.validate(resolution)
                ):
                    raise ColdIntentLookupRequired(key)
                if resolution.checkpoint is not None:
                    return self._replay_cold(intent, resolution, intent_hash=intent_hash)
            with self._retention_lock:
                if (
                    hot_key not in account.deduplication
                    and len(self._hot_accounts) >= self._maximum
                ):
                    raise OmsHotCapacityFull("hot OMS capacity reached before Journal commit")
                self._live_started = True
                result = self._admit_to_account(
                    account=account,
                    intent=intent,
                    intent_hash=intent_hash,
                    attribution_binding=attribution_binding,
                    risk_snapshot=risk_snapshot,
                    decision_context=decision_context,
                    reservation_requirement=reservation_requirement,
                    now_monotonic_ns=now_monotonic_ns,
                )
                if result.outcome is IntentAdmissionOutcome.ACCEPTED:
                    assert result.event is not None
                    self._hot_accounts[result.order.order.oms_order_id] = intent.account_id
                    self._histories[result.order.order.oms_order_id] = []
                    self._capture(result.event)
                return result

    def _replay_cold(
        self, intent: OmsOrderIntent, resolution: ColdIntentResolution, *, intent_hash: str
    ) -> IntentAdmission:
        assert resolution.checkpoint is not None
        state = decode_checkpoint(resolution.checkpoint)
        initial = OmsRecoverySeedV1.decode_and_verify(state.orders[0].admission_seed).order
        if initial.order.intent_hash != intent_hash:
            fact = self._fact(
                OmsFactKind.IDEMPOTENCY_CONFLICT,
                account_id=intent.account_id,
                oms_order_id=initial.order.oms_order_id,
                execution_id=initial.execution.execution_id,
                attributes={
                    "existing_intent_hash": initial.order.intent_hash,
                    "incoming_intent_hash": intent_hash,
                    "strategy_id": resolution.key.strategy_id,
                    "strategy_order_id": resolution.key.strategy_order_id,
                },
            )
            return IntentAdmission(
                IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT,
                intent_hash,
                initial,
                None,
                self._commit((fact,)),
            )
        row = state.events[-1]
        from .order_book import OmsOrderEventKind

        event = OmsOrderEvent(
            row.event_id,
            OmsOrderEventKind(row.kind),
            row.account_id,
            row.oms_order_id,
            row.execution_id,
            OmsOrderState(row.order_state),
            row.order_revision,
            state.journal_id,
            row.journal_lsn,
            row.source_key,
        )
        return IntentAdmission(
            IntentAdmissionOutcome.IDEMPOTENT, intent_hash, initial, None, None, event
        )

    def _capture(self, event: OmsOrderEvent) -> None:
        self._histories[event.oms_order_id].append(event)
        self._event_counts[event.account_id] = self._event_counts.get(event.account_id, 0) + 1
        if self._consumer_replay_enabled and not self._consumer_replay_failed:
            try:
                window = self._consumer_replay.get(event.account_id)
                if window is None:
                    # Recovery rows lack immutable trace/day for retired orders.
                    # Never claim coverage before the verified recovery fence.
                    window = CommittedReplayWindow(
                        capacity=self._cache_events, floor=self._recovered_lsn
                    )
                    self._consumer_replay[event.account_id] = window
                order = self._accounts[event.account_id].orders[event.oms_order_id].order
                window.append(CommittedReplayEntry(event, order.consumer_trace, order.trading_day))
            except Exception:
                # The source commit already succeeded. Cache failure must not
                # turn it into a retryable failed order or a partial hot range.
                self._consumer_replay_failed = True

    def _transition(
        self,
        *,
        oms_order_id: str,
        target: OmsOrderState,
        first_fact_kind: OmsFactKind | None = None,
        additional_state_attributes: Mapping[str, str] | None = None,
    ) -> OmsOrderEvent | None:
        account = self._account_for_order(oms_order_id)
        with account.lock:
            terminal = target in {
                OmsOrderState.FILLED,
                OmsOrderState.CANCELED,
                OmsOrderState.REJECTED,
            }
            reserve = 0 if terminal else (1 if target is OmsOrderState.CANCEL_PENDING else 2)
            if (
                account.orders[oms_order_id].order.state is not target
                and len(self._histories[oms_order_id]) >= self._maximum_order_events - reserve
            ):
                raise OmsHotCapacityFull("order history capacity reached before Journal commit")
            result = super()._transition(
                oms_order_id=oms_order_id,
                target=target,
                first_fact_kind=first_fact_kind,
                additional_state_attributes=additional_state_attributes,
            )
            if result is not None:
                self._capture(result)
            return result

    def apply_trade(
        self, *, oms_order_id: str, trade_key: str, quantity: int
    ) -> OmsOrderEvent | None:
        account = self._account_for_order(oms_order_id)
        with account.lock:
            full_fill = quantity == account.orders[oms_order_id].leaves_quantity
            if trade_key not in account.orders[oms_order_id].trade_keys and len(
                self._histories[oms_order_id]
            ) >= self._maximum_order_events - (0 if full_fill else 2):
                raise OmsHotCapacityFull("order history capacity reached before Journal commit")
            result = super().apply_trade(
                oms_order_id=oms_order_id, trade_key=trade_key, quantity=quantity
            )
            if result is not None:
                self._capture(result)
            return result

    def prepare_terminal_archive(self, oms_order_id: str) -> TerminalArchiveCandidate:
        account = self._account_for_order(oms_order_id)
        with account.lock:
            current = account.orders[oms_order_id]
            if current.reservation.is_active or current.order.state not in {
                OmsOrderState.FILLED,
                OmsOrderState.REJECTED,
                OmsOrderState.CANCELED,
            }:
                raise OmsOrderBookError("unresolved/active order cannot retire")
            trace = current.order.consumer_trace
            initial = account.deduplication[
                OmsIntentKey(
                    current.order.account_id, trace.strategy_id or "", trace.strategy_order_id or ""
                )
            ].order
            return TerminalArchiveCandidate(initial, current, tuple(self._histories[oms_order_id]))

    def maintenance_order_ids(
        self, *, limit: int = 32, exclude: frozenset[str] = frozenset()
    ) -> tuple[str, ...]:
        """Oldest retained identities, copied with a small fixed work budget.

        Intended for a SINGLE maintenance owner, not a historical query or
        dispatch authorization. Never exposes mutable dictionaries or scans the
        cold index. Unknown/unresolved orders must be retained, never evicted.
        """
        if type(limit) is not int or not 1 <= limit <= 128:
            raise OmsOrderBookError("maintenance page exceeds its budget")
        if (
            type(exclude) is not frozenset
            or len(exclude) > 1024
            or any(type(key) is not str for key in exclude)
        ):
            raise OmsOrderBookError("maintenance in-flight identities exceed their budget")
        with self._retention_lock:
            # Inspect at most limit + in-flight identities, not the full book.
            return tuple(
                islice(
                    (
                        key
                        for key in islice(self._hot_accounts, limit + len(exclude))
                        if key not in exclude
                    ),
                    limit,
                )
            )

    def retire_terminal(
        self, candidate: TerminalArchiveCandidate, receipt: ColdArchiveReceipt
    ) -> None:
        self.retire_terminals((candidate,), receipt)

    def retire_terminals(
        self, candidates: tuple[TerminalArchiveCandidate, ...], receipt: ColdArchiveReceipt
    ) -> None:
        """One immutable positive proof, O(n) validation, per-order hot fencing.

        No SQL or encoding under account locks. Failure may leave a subset
        already safely retired (all covered by the same FULL commit); never
        removes an unverified order. This is not a cross-account atomic batch.
        """
        if type(candidates) is not tuple or not 1 <= len(candidates) <= 128:
            raise OmsOrderBookError("terminal retirement batch exceeds capacity")
        if not self._cold.validate(receipt):
            raise ColdIntentLookupRequired(candidates[0].key)
        if len(receipt.keys) != len(receipt.digests):
            raise OmsOrderBookError("archive receipt coverage is malformed")
        covered = dict(zip(receipt.keys, receipt.digests, strict=True))
        if len(covered) != len(receipt.keys) or len(
            {c.current.order.oms_order_id for c in candidates}
        ) != len(candidates):
            raise OmsOrderBookError("archive receipt/candidates repeat identities")
        for candidate in candidates:
            if candidate.key not in covered:
                raise ColdIntentLookupRequired(candidate.key)
            if covered[candidate.key] != sha256(candidate.encode()).digest():
                raise OmsOrderBookError("archive receipt covers another payload")
        for candidate in candidates:
            order_id = candidate.current.order.oms_order_id
            account = self._account_for_order(order_id)
            with account.lock:
                if (
                    account.orders[order_id] != candidate.current
                    or tuple(self._histories[order_id]) != candidate.events
                ):
                    raise OmsOrderBookError("terminal changed after archive capture")
                account.reservations.forget_released(order_id)
                del account.orders[order_id]
                key = candidate.key
                del account.deduplication[
                    OmsIntentKey(key.account_id, key.strategy_id, key.strategy_order_id)
                ]
                del self._histories[order_id]
                with self._retention_lock:
                    del self._hot_accounts[order_id]

    def last_order_event(self, oms_order_id: str) -> OmsOrderEvent:
        account = self._account_for_order(oms_order_id)
        with account.lock:
            return self._histories[oms_order_id][-1]

    def event_count(self, account_id: str) -> int:
        account = self._account(account_id)
        with account.lock:
            return self._event_counts.get(account_id, 0)

    def events(self, account_id: str) -> tuple[OmsOrderEvent, ...]:
        return self.events_from(account_id, 0)

    def events_from(self, account_id: str, start_index: int) -> tuple[OmsOrderEvent, ...]:
        account = self._account(account_id)
        with account.lock:
            count = self._event_counts.get(account_id, 0)
            floor = count - len(account.events)
            if not floor <= start_index <= count:
                raise OmsHistoryRequiresColdReader(
                    "requested events exceed the retained hot window"
                )
            return tuple(islice(account.events, start_index - floor, None))

    def restore_committed_batch(self, facts: tuple[OmsJournalFact, ...], commit: OmsJournalCommit):
        raise OmsHistoryRequiresColdReader(
            "bounded startup requires the cold-aware recovery coordinator"
        )

    def restore_recovery_checkpoint(self, encoded: bytes, *, expected_journal_id: str) -> None:
        raise OmsHistoryRequiresColdReader(
            "full historical checkpoint cannot load into bounded live state"
        )

    def export_recovery_checkpoint(self) -> bytes:
        raise OmsHistoryRequiresColdReader(
            "bounded checkpoint requires cold coverage and active-only state"
        )

    def restore_cold_covered_checkpoint(
        self,
        encoded: bytes | None,
        *,
        expected_journal_id: str,
        event_counts: dict[str, int],
        recent_events: tuple[EventCheckpointRow, ...],
        maximum_checkpoint_bytes: int = 64 * 1024**2,
    ) -> None:
        """Trusted startup coordinator only, after proving complete cold coverage.

        The payload is active-only; replay counts/window cover hot AND cold
        orders. This does not enable general full-history restore on this book.
        No live writer may be running while the fresh book is installed.
        """
        if self._live_started or self._accounts or self._recovery_imported or self._recovered_lsn:
            raise OmsOrderBookError("cold-covered restore requires fresh startup state")
        if (
            type(maximum_checkpoint_bytes) is not int
            or not 262144 <= maximum_checkpoint_bytes <= 256 * 1024**2
            or len(event_counts) > self._maximum_accounts
            or any(
                type(k) is not str or not 1 <= len(k) <= 128 or type(v) is not int or v < 0
                for k, v in event_counts.items()
            )
            or len(recent_events) > self._cache_events
        ):
            raise OmsOrderBookError("cold-covered restore exceeds its budget")
        state = (
            None
            if encoded is None
            else decode_checkpoint(encoded, maximum_bytes=maximum_checkpoint_bytes)
        )
        if state is None and (event_counts or recent_events):
            raise OmsOrderBookError("empty checkpoint cannot contain replay coverage")
        if state is not None and (
            len(state.orders) > self._maximum
            or any(row.state in {"FILLED", "CANCELED", "REJECTED"} for row in state.orders)
        ):
            raise OmsOrderBookError("active checkpoint contains retired/over-capacity orders")
        verifier = OrderBook(journal=self._journal, persist_recovery_seed=True)
        if encoded is not None:
            OrderBook.restore_recovery_checkpoint(
                verifier, encoded, expected_journal_id=expected_journal_id
            )
        histories: dict[str, list[OmsOrderEvent]] = {}
        hot_accounts: dict[str, str] = {}
        for account_id, account in verifier._accounts.items():
            if account_id not in event_counts:
                raise OmsOrderBookError("active account is missing replay coverage")
            for event in account.events:
                histories.setdefault(event.oms_order_id, []).append(event)
            for order_id in account.orders:
                if len(histories.get(order_id, ())) > self._maximum_order_events:
                    raise OmsOrderBookError("active order history exceeds its budget")
                hot_accounts[order_id] = account_id
            account.events = deque(maxlen=self._cache_events)
        last = 0
        window_counts: dict[str, int] = {}
        for row in recent_events:
            if (
                state is None
                or not last < row.journal_lsn <= state.watermark
                or row.account_id not in event_counts
                or row.event_id != f"oms-event:{expected_journal_id}:{row.journal_lsn}"
            ):
                raise OmsOrderBookError("replay window scope/order is invalid")
            account = verifier._account(row.account_id)
            if isinstance(account.events, list):
                account.events = deque(maxlen=self._cache_events)
            account.events.append(
                OmsOrderEvent(
                    row.event_id,
                    OmsOrderEventKind(row.kind),
                    row.account_id,
                    row.oms_order_id,
                    row.execution_id,
                    OmsOrderState(row.order_state),
                    row.order_revision,
                    expected_journal_id,
                    row.journal_lsn,
                    row.source_key,
                )
            )
            window_counts[row.account_id] = window_counts.get(row.account_id, 0) + 1
            last = row.journal_lsn
        if any(event_counts.get(k, 0) < v for k, v in window_counts.items()):
            raise OmsOrderBookError("replay count is below its window")
        if state is not None and any(count > state.watermark for count in event_counts.values()):
            raise OmsOrderBookError("replay count exceeds Journal coverage")
        # Accounts with no retained recent event still carry their absolute count.
        for account_id in event_counts:
            account = verifier._account(account_id)
            if isinstance(account.events, list):
                account.events = deque(maxlen=self._cache_events)
        self._accounts = verifier._accounts
        self._histories, self._hot_accounts = histories, hot_accounts
        self._event_counts = dict(event_counts)
        self._recovered_lsn = 0 if state is None else state.watermark
        self._recovered_journal_id = expected_journal_id
        self._recovery_imported = True

    @property
    def recovery_is_quiescent(self) -> bool:
        return self._recovery_imported and not self._live_started
