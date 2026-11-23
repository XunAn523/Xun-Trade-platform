"""P0 model-based tests for durable replay and future OMS compatibility.

The Journal and gateway machines exercise production code.  The OMS machine
uses the test-only reference model so M1 can attach the real order book to the
same transitions without changing the approved invariants.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.gateway import CommandState, OrderGateway, OrderIntent
from emt_gateway.journal import FileJournal, InMemoryJournal, JournalAppend
from hypothesis import settings
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule
from tests.contracts.oms_reference import ReferenceOrderBook, ReferenceOrderState

_STATEFUL_SETTINGS = settings(max_examples=30, stateful_step_count=24, deadline=None)


class _StatefulTrace:
    def __init__(self) -> None:
        self.traces: dict[str, dict[str, object]] = {}

    def begin(self, correlation_id: str, **identity: object) -> None:
        self.traces[correlation_id] = {"stages": ["C0"], "outcome": None}

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None:
        del timestamp_ns
        self.traces[correlation_id]["stages"].append(stage)  # type: ignore[index]  # heterogeneous test trace

    def complete(self, correlation_id: str, outcome: str = "ok", reason: str | None = None) -> None:
        del reason
        self.traces[correlation_id]["outcome"] = outcome

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None:
        del metric, value, timestamp_ns


class _StatefulNative:
    def __init__(self) -> None:
        self.next_id = 100
        self.order_calls: list[tuple[str, str]] = []

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        self.order_calls.append((command_id, client_order_id))
        native_id = self.next_id
        self.next_id += 1
        return native_id

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        del command_id, order_emt_id
        raise AssertionError("this state machine only exercises order callback delivery")

    def get_last_error_code(self) -> int:
        return 0


class JournalReplayStateMachine(RuleBasedStateMachine):
    """Append, restart and torn-tail recovery must preserve the verified prefix."""

    def __init__(self) -> None:
        super().__init__()
        self._temporary_directory = TemporaryDirectory()
        self._path = Path(self._temporary_directory.name) / "stateful.emtj"
        self._journal = FileJournal(self._path)
        self._expected: list[tuple[int, str, str, str, str]] = []
        self._counter = 0

    @rule()
    def append_one(self) -> None:
        self._counter += 1
        record = self._journal.append(
            kind="CALL_STARTED",
            command_id=f"command-{self._counter}",
            correlation_id=f"correlation-{self._counter}",
            payload_hash=_digest(f"payload-{self._counter}"),
        )
        self._expected.append(
            (
                record.lsn,
                record.kind,
                record.command_id or "",
                record.correlation_id,
                record.payload_hash,
            )
        )

    @rule()
    def append_atomic_batch(self) -> None:
        self._counter += 1
        command_id = f"command-{self._counter}"
        correlation_id = f"correlation-{self._counter}"
        entries = (
            JournalAppend(
                "CALL_STARTED", command_id, correlation_id, _digest(f"call-{self._counter}")
            ),
            JournalAppend(
                "NATIVE_ACCEPTED", command_id, correlation_id, _digest(f"result-{self._counter}")
            ),
        )
        records = self._journal.append_batch(entries)
        self._expected.extend(
            (
                record.lsn,
                record.kind,
                record.command_id or "",
                record.correlation_id,
                record.payload_hash,
            )
            for record in records
        )

    @rule()
    def restart(self) -> None:
        self._journal.close()
        self._journal = FileJournal(self._path)
        self._assert_verified_prefix()

    @rule()
    def recover_torn_tail_after_restart(self) -> None:
        self._journal.close()
        with self._path.open("ab") as raw_file:
            raw_file.write(b"incomplete-stateful-tail")
        self._journal = FileJournal(self._path)
        self._assert_verified_prefix()

    @invariant()
    def recovered_records_match_the_model(self) -> None:
        self._assert_verified_prefix()

    def teardown(self) -> None:
        self._journal.close()
        self._temporary_directory.cleanup()

    def _assert_verified_prefix(self) -> None:
        actual = [
            (
                record.lsn,
                record.kind,
                record.command_id or "",
                record.correlation_id,
                record.payload_hash,
            )
            for record in self._journal.records()
        ]
        assert actual == self._expected
        assert [record[0] for record in actual] == list(range(1, len(actual) + 1))


class CallbackReplayStateMachine(RuleBasedStateMachine):
    """Duplicate and arbitrary-order callbacks cannot replay a native send."""

    orders = Bundle("orders")

    def __init__(self) -> None:
        super().__init__()
        self.trace = _StatefulTrace()
        self.native = _StatefulNative()
        self.command_journal = InMemoryJournal()
        self.event_journal = InMemoryJournal()
        self.gateway = OrderGateway(
            native=self.native,
            command_journal=self.command_journal,
            event_journal=self.event_journal,
            trace=self.trace,
            callback_capacity=1_024,
        )
        self.order_count = 0
        self.callback_count = 0

    @rule(target=orders)
    def submit_order(self) -> tuple[str, int, str]:
        self.order_count += 1
        client_order_id = f"stateful-order-{self.order_count}"
        result = self.gateway.submit_order(OrderIntent(client_order_id, _digest(client_order_id)))
        assert result.state is CommandState.NATIVE_ACCEPTED
        assert result.native_id is not None
        return client_order_id, result.native_id, result.correlation_id

    @rule(order=orders)
    def duplicate_order_intent_is_idempotent(self, order: tuple[str, int, str]) -> None:
        client_order_id, native_id, correlation_id = order
        result = self.gateway.submit_order(OrderIntent(client_order_id, _digest(client_order_id)))
        assert (result.native_id, result.correlation_id, result.state) == (
            native_id,
            correlation_id,
            CommandState.NATIVE_ACCEPTED,
        )

    @rule(order=orders)
    def deliver_callback_in_any_order_or_as_a_duplicate(self, order: tuple[str, int, str]) -> None:
        _, native_id, correlation_id = order
        callback_id = self.gateway.callbacks.receive(
            "OnOrderEvent",
            profile="ordinary",
            workload="steady",
            order_emt_id=native_id,
        )
        assert callback_id is not None
        delivery = self.gateway.callbacks.normalize_one()
        assert delivery is not None
        self.gateway.callbacks.ack(delivery.delivery_id)
        self.callback_count += 1
        stages = self.trace.traces[correlation_id]["stages"]
        assert isinstance(stages, list)
        assert stages.count("C5") <= 1 and stages.count("C6") <= 1 and stages.count("C7") <= 1

    @invariant()
    def callbacks_do_not_create_native_side_effects_or_overflow(self) -> None:
        assert not self.gateway.callbacks.disarmed
        assert len(self.native.order_calls) == self.order_count
        assert len(self.command_journal.records()) == self.order_count * 2
        assert len(self.event_journal.records()) == self.callback_count


class OmsReferenceStateMachine(RuleBasedStateMachine):
    """M1 reference transitions for order state, reservations and trade de-duplication."""

    orders = Bundle("oms_orders")
    trades = Bundle("oms_trades")

    def __init__(self) -> None:
        super().__init__()
        self.book = ReferenceOrderBook()
        self.counter = 0

    @rule(target=orders)
    def reserve_new_order(self) -> str:
        self.counter += 1
        order_id = f"oms-order-{self.counter}"
        order = self.book.submit(order_id=order_id, quantity=100)
        assert order.state is ReferenceOrderState.LIVE
        return order_id

    @rule(order_id=orders)
    def duplicate_intent_keeps_the_same_reservation(self, order_id: str) -> None:
        before = self.book.order(order_id)
        assert self.book.submit(order_id=order_id, quantity=100) == before

    @rule(target=trades, order_id=orders)
    def apply_partial_or_final_trade(self, order_id: str) -> tuple[str, str] | None:
        order = self.book.order(order_id)
        if not order.reserves_quantity:
            return None
        self.counter += 1
        execution_key = f"execution-{self.counter}"
        quantity = min(25, order.leaves_quantity)
        assert self.book.apply_trade(
            order_id=order_id, execution_key=execution_key, quantity=quantity
        )
        return order_id, execution_key

    @rule(trade=trades)
    def duplicate_trade_is_ignored(self, trade: tuple[str, str] | None) -> None:
        if trade is None:
            return
        order_id, execution_key = trade
        assert not self.book.apply_trade(order_id=order_id, execution_key=execution_key, quantity=1)

    @rule(order_id=orders)
    def cancel_open_order(self, order_id: str) -> None:
        order = self.book.order(order_id)
        if order.reserves_quantity:
            assert self.book.cancel(order_id=order_id).state is ReferenceOrderState.CANCELED

    @invariant()
    def reservations_are_exact_and_no_order_overfills(self) -> None:
        expected = 0
        for order in self.book.orders():
            assert 0 <= order.filled_quantity <= order.quantity
            assert order.leaves_quantity >= 0
            if order.reserves_quantity:
                expected += order.leaves_quantity
            if order.state is ReferenceOrderState.FILLED:
                assert order.leaves_quantity == 0
        assert self.book.reservations() == expected


TestJournalReplayStateMachine = JournalReplayStateMachine.TestCase
TestJournalReplayStateMachine.settings = _STATEFUL_SETTINGS

TestCallbackReplayStateMachine = CallbackReplayStateMachine.TestCase
TestCallbackReplayStateMachine.settings = _STATEFUL_SETTINGS

TestOmsReferenceStateMachine = OmsReferenceStateMachine.TestCase
TestOmsReferenceStateMachine.settings = _STATEFUL_SETTINGS


def _digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()
