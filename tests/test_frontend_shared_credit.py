import multiprocessing as mp

import pytest
from xuanling_tradecore.consumer.frontend_shared_credit import (
    FrontendSharedCreditError,
    FrontendSharedCreditV1,
)


def _spawn_credit_worker(credit, worker_id, count, control):
    acquired = 0
    for _ in range(count):
        acquired += int(credit.try_acquire(worker_id))
    control.send(acquired)
    command = control.recv()
    if command == "release":
        for _ in range(acquired):
            credit.release(worker_id)
    elif command == "fence":
        credit.fence()
    control.close()


def test_one_hard_total_and_exact_worker_ownership():
    credit = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=4, worker_count=2)
    assert all(credit.try_acquire(0) for _ in range(4))
    assert not credit.try_acquire(1)
    assert credit.snapshot().held_by_worker == (4, 0)
    credit.release(0)
    assert credit.try_acquire(1)
    assert credit.snapshot().held_by_worker == (3, 1)
    with pytest.raises(FrontendSharedCreditError, match="unowned"):
        for _ in range(2):
            credit.release(1)
    assert credit.fenced


def test_dimension_and_drain_contracts():
    context = mp.get_context("spawn")
    with pytest.raises(FrontendSharedCreditError):
        FrontendSharedCreditV1(context, capacity=129, worker_count=1)
    credit = FrontendSharedCreditV1(context, capacity=128, worker_count=4)
    assert credit.try_acquire(2)
    with pytest.raises(FrontendSharedCreditError, match="not drained"):
        credit.require_worker_drained(2)
    credit.release(2)
    credit.require_worker_drained(2)
    credit.require_all_drained()


def test_restricted_class_reserves_existing_capacity_without_waiting():
    credit = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=4, worker_count=2)
    assert credit.maybe_restricted_available(3)
    assert credit.try_acquire(0, restricted_limit=3)
    assert credit.try_acquire(1, restricted_limit=3)
    assert credit.try_acquire(0, restricted_limit=3)
    assert not credit.maybe_restricted_available(3)
    assert not credit.try_acquire(1, restricted_limit=3)
    assert credit.snapshot().held == credit.snapshot().restricted_held == 3
    assert credit.try_acquire(1)
    assert credit.snapshot().held == 4
    with pytest.raises(FrontendSharedCreditError, match="invalid"):
        credit.try_acquire(0, restricted_limit=0)
    for worker_id, count in enumerate(credit.snapshot().held_by_worker):
        restricted = credit.snapshot().restricted_held_by_worker[worker_id]
        for _ in range(restricted):
            credit.release(worker_id, restricted=True)
        for _ in range(count - restricted):
            credit.release(worker_id)
    credit.require_all_drained()


def test_spawned_workers_share_one_capacity_and_drain_normally():
    context = mp.get_context("spawn")
    credit = FrontendSharedCreditV1(context, capacity=8, worker_count=2)
    parent0, child0 = context.Pipe()
    parent1, child1 = context.Pipe()
    workers = (
        context.Process(target=_spawn_credit_worker, args=(credit, 0, 8, child0)),
        context.Process(target=_spawn_credit_worker, args=(credit, 1, 8, child1)),
    )
    for worker in workers:
        worker.start()
    child0.close()
    child1.close()
    acquired = parent0.recv() + parent1.recv()
    assert acquired == 8
    assert credit.snapshot().held == 8
    parent0.send("release")
    parent1.send("release")
    for worker in workers:
        worker.join(10)
        assert worker.exitcode == 0
    credit.require_all_drained()
    parent0.close()
    parent1.close()


def test_dead_worker_credit_is_fenced_not_reclaimed():
    context = mp.get_context("spawn")
    credit = FrontendSharedCreditV1(context, capacity=2, worker_count=1)
    parent, child = context.Pipe()
    worker = context.Process(target=_spawn_credit_worker, args=(credit, 0, 1, child))
    worker.start()
    child.close()
    assert parent.recv() == 1
    worker.kill()
    worker.join(10)
    assert worker.exitcode != 0
    credit.fence()
    assert not credit.try_acquire(0)
    with pytest.raises(FrontendSharedCreditError, match="not drained"):
        credit.require_all_drained()
    parent.close()
