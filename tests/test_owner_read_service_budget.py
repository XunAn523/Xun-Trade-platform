from __future__ import annotations

from threading import Event, Thread
from time import monotonic_ns, sleep
from types import SimpleNamespace

from xuanling_tradecore.consumer.owner_read_ipc import OwnerReadServiceBudgetV1


def test_priority_read_gets_bounded_head_start_without_waiting_for_active_snapshot() -> None:
    signal = SimpleNamespace(value=0)
    budget = OwnerReadServiceBudgetV1(
        priority_head_start_ns=100_000_000,
        read_service_signal=signal,
    )
    stopped = Event()
    release_snapshot = Event()
    priority_entered = Event()
    release_priority = Event()
    second_snapshot_entered = Event()
    deadline = monotonic_ns() + 5_000_000_000

    def first_snapshot() -> None:
        with budget.turn("ReadOrderSnapshotPage", deadline_ns=deadline, stopped=stopped):
            release_snapshot.wait(2)

    def replay() -> None:
        with budget.turn("PollEvents", deadline_ns=deadline, stopped=stopped):
            priority_entered.set()
            release_priority.wait(2)

    def second_snapshot() -> None:
        with budget.turn("ReadOrderSnapshotPage", deadline_ns=deadline, stopped=stopped):
            second_snapshot_entered.set()

    first = Thread(target=first_snapshot)
    priority = Thread(target=replay)
    second = Thread(target=second_snapshot)
    first.start()
    sleep(0.01)
    priority.start()
    assert priority_entered.wait(1)
    assert signal.value == 1
    second.start()
    sleep(0.01)
    assert not second_snapshot_entered.is_set()

    # The fixed head start ends independently of the long priority handler,
    # and the first snapshot never had to finish before replay could begin.
    assert second_snapshot_entered.wait(1)
    release_priority.set()
    release_snapshot.set()
    for thread in (first, priority, second):
        thread.join(1)
        assert not thread.is_alive()
    assert signal.value == 0
