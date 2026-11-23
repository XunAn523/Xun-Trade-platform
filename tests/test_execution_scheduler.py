from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread

from xuanling_tradecore.domain import (
    CancelDeadlinePolicy,
    CancelRequest,
    ConsumerTrace,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    IngressAdmission,
    IngressLane,
    IngressRejection,
    IngressRejectionCode,
    SchedulerAdmissionExpiredError,
    SchedulerStateError,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _trace(
    *, strategy_order_id: str = "strategy-order-1", account_id: str = "account-1"
) -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id=strategy_order_id,
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _instruction(
    *, strategy_order_id: str = "strategy-order-1", account_id: str = "account-1"
) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"execution-{strategy_order_id}",
        oms_order_id=f"oms-{strategy_order_id}",
        account_id=account_id,
        trading_day=TradingDayEpoch("2026-09-02"),
        consumer_trace=_trace(strategy_order_id=strategy_order_id, account_id=account_id),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=101,
        limit_price_ticks=100,
    )


def _cancel(*, cancel_request_id: str = "cancel-1", account_id: str = "account-1") -> CancelRequest:
    return CancelRequest(
        cancel_request_id=cancel_request_id,
        oms_order_id="oms-strategy-order-1",
        execution_id="execution-strategy-order-1",
        account_id=account_id,
        consumer_trace=_trace(account_id=account_id),
        reason="strategy requested cancel",
        requested_monotonic_ns=100,
    )


def _scheduler(
    *,
    new_capacity: int = 2,
    cancel_capacity: int = 1,
    maximum_inflight_new_orders: int | None = None,
) -> ExecutionScheduler:
    policy = ExecutionSchedulerPolicy(
        policy_id="scheduler-v1",
        version=1,
        policy_hash=SHA_A,
        new_order_capacity=new_capacity,
        cancel_capacity=cancel_capacity,
        new_order_high_watermark=max(1, new_capacity - 1),
        cancel_high_watermark=cancel_capacity,
        max_new_order_bytes=1024,
        max_cancel_bytes=512,
        maximum_inflight_new_orders=maximum_inflight_new_orders,
    )
    return ExecutionScheduler(
        account_id="account-1",
        process_epoch_id="boot-1:process-1",
        policy=policy,
        execution_deadline_policy=ExecutionDeadlinePolicy(
            policy_id="execution-deadline-v1",
            version=1,
            policy_hash=SHA_B,
            native_start_budget_ns=100,
        ),
        cancel_deadline_policy=CancelDeadlinePolicy(
            policy_id="cancel-deadline-v1",
            version=1,
            policy_hash=SHA_C,
            native_start_budget_ns=50,
        ),
    )


class ExecutionSchedulerTests(unittest.TestCase):
    def test_policy_rejects_invalid_watermarks(self) -> None:
        with self.assertRaises(ValueError):
            ExecutionSchedulerPolicy(
                policy_id="scheduler-v1",
                version=1,
                policy_hash=SHA_A,
                new_order_capacity=2,
                cancel_capacity=1,
                new_order_high_watermark=3,
                cancel_high_watermark=1,
                max_new_order_bytes=1024,
                max_cancel_bytes=512,
            )

    def test_full_new_order_lane_returns_structured_zero_oms_rejection(self) -> None:
        scheduler = _scheduler(new_capacity=2)
        accepted = [
            scheduler.admit_new_order(
                request_id=f"strategy-order-{index}",
                serialized_size_bytes=128,
                admitted_monotonic_ns=100 + index,
            )
            for index in range(2)
        ]
        self.assertTrue(all(isinstance(item, IngressAdmission) for item in accepted))
        rejected = scheduler.admit_new_order(
            request_id="strategy-order-overload",
            serialized_size_bytes=128,
            admitted_monotonic_ns=103,
        )
        self.assertIsInstance(rejected, IngressRejection)
        assert isinstance(rejected, IngressRejection)
        self.assertEqual(IngressRejectionCode.INGRESS_OVERLOADED, rejected.code)
        self.assertTrue(rejected.retryable)
        self.assertIsNone(rejected.oms_order_id)
        snapshot = scheduler.snapshot()
        self.assertEqual(2, snapshot.depths["new_order"])
        self.assertEqual(1, snapshot.counters["new_order_overloaded"])
        self.assertTrue(snapshot.new_order_high_watermark_reached)
        self.assertFalse(snapshot.cancel_high_watermark_reached)
        self.assertEqual(100, snapshot.execution_native_start_budget_ns)
        self.assertEqual(50, snapshot.cancel_native_start_budget_ns)

    def test_cancel_capacity_is_reserved_and_cancel_dequeues_before_new_order(self) -> None:
        scheduler = _scheduler(new_capacity=1, cancel_capacity=1)
        new_admission = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        self.assertIsInstance(new_admission, IngressAdmission)
        self.assertIsInstance(
            scheduler.admit_new_order(
                request_id="strategy-order-2",
                serialized_size_bytes=128,
                admitted_monotonic_ns=101,
            ),
            IngressRejection,
        )
        cancel_admission = scheduler.admit_cancel(
            cancel_request_id="cancel-1",
            serialized_size_bytes=64,
            admitted_monotonic_ns=102,
        )
        self.assertIsInstance(cancel_admission, IngressAdmission)
        assert isinstance(new_admission, IngressAdmission)
        assert isinstance(cancel_admission, IngressAdmission)
        scheduler.commit_new_order(new_admission, _instruction(), checked_monotonic_ns=103)
        scheduler.commit_cancel(cancel_admission, _cancel(), checked_monotonic_ns=103)

        first = scheduler.dequeue(checked_monotonic_ns=104)
        second = scheduler.dequeue(checked_monotonic_ns=105)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(IngressLane.CANCEL, first.lane)
        self.assertEqual(IngressLane.NEW_ORDER, second.lane)
        self.assertIsNone(scheduler.dequeue(checked_monotonic_ns=106))

    def test_size_and_duplicate_rejections_do_not_consume_capacity(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        too_large = scheduler.admit_new_order(
            request_id="strategy-order-big",
            serialized_size_bytes=1025,
            admitted_monotonic_ns=100,
        )
        self.assertIsInstance(too_large, IngressRejection)
        assert isinstance(too_large, IngressRejection)
        self.assertEqual(IngressRejectionCode.INGRESS_MESSAGE_TOO_LARGE, too_large.code)
        admitted = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=101,
        )
        self.assertIsInstance(admitted, IngressAdmission)
        duplicate = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=102,
        )
        self.assertIsInstance(duplicate, IngressRejection)
        assert isinstance(duplicate, IngressRejection)
        self.assertEqual(IngressRejectionCode.INGRESS_DUPLICATE_ACTIVE, duplicate.code)
        self.assertFalse(duplicate.retryable)
        self.assertEqual(1, scheduler.snapshot().depths["new_order"])

    def test_unqueued_expiry_consumes_reservation_without_native_dispatch(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        admission = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        assert isinstance(admission, IngressAdmission)
        with self.assertRaises(SchedulerAdmissionExpiredError):
            scheduler.commit_new_order(
                admission,
                _instruction(),
                checked_monotonic_ns=201,
            )
        self.assertEqual(0, scheduler.snapshot().depths["new_order"])
        retry = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=202,
        )
        self.assertIsInstance(retry, IngressAdmission)

    def test_atomic_new_order_ingress_queues_without_a_visible_reservation(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        decision = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
            instruction=_instruction(),
            checked_monotonic_ns=101,
        )
        self.assertIsInstance(decision, IngressAdmission)
        snapshot = scheduler.snapshot()
        self.assertEqual(0, snapshot.depths["new_order_reservations"])
        self.assertEqual(1, snapshot.depths["new_order_queued"])
        dispatched = scheduler.dequeue(checked_monotonic_ns=102)
        self.assertIsNotNone(dispatched)
        assert dispatched is not None
        self.assertEqual("strategy-order-1", dispatched.admission.request_id)
        self.assertEqual("execution-strategy-order-1", dispatched.command.execution_id)

    def test_atomic_new_order_rejection_has_no_reservation_or_command(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        first = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
            instruction=_instruction(),
            checked_monotonic_ns=100,
        )
        self.assertIsInstance(first, IngressAdmission)
        rejected = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-2",
            serialized_size_bytes=128,
            admitted_monotonic_ns=101,
            instruction=_instruction(strategy_order_id="strategy-order-2"),
            checked_monotonic_ns=101,
        )
        self.assertIsInstance(rejected, IngressRejection)
        assert isinstance(rejected, IngressRejection)
        self.assertEqual(IngressRejectionCode.INGRESS_OVERLOADED, rejected.code)
        snapshot = scheduler.snapshot()
        self.assertEqual(0, snapshot.depths["new_order_reservations"])
        self.assertEqual(1, snapshot.depths["new_order_queued"])
        self.assertEqual(1, snapshot.counters["new_order_overloaded"])

    def test_native_inflight_credit_survives_dequeue_until_explicit_release(self) -> None:
        scheduler = _scheduler(new_capacity=2, maximum_inflight_new_orders=1)
        first = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
            instruction=_instruction(),
            checked_monotonic_ns=100,
        )
        self.assertIsInstance(first, IngressAdmission)
        self.assertIsNotNone(scheduler.dequeue(checked_monotonic_ns=101))

        rejected = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-2",
            serialized_size_bytes=128,
            admitted_monotonic_ns=102,
            instruction=_instruction(strategy_order_id="strategy-order-2"),
            checked_monotonic_ns=102,
        )
        self.assertIsInstance(rejected, IngressRejection)
        assert isinstance(rejected, IngressRejection)
        self.assertEqual(IngressRejectionCode.INGRESS_OVERLOADED, rejected.code)
        snapshot = scheduler.snapshot()
        self.assertEqual(1, snapshot.depths["new_order_inflight"])
        self.assertEqual(1, snapshot.max_observed_depths["new_order_inflight"])
        self.assertEqual(1, snapshot.counters["new_order_inflight_limited"])

        self.assertTrue(scheduler.release_new_order_inflight("execution-strategy-order-1"))
        self.assertFalse(scheduler.release_new_order_inflight("execution-strategy-order-1"))
        retry = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-2",
            serialized_size_bytes=128,
            admitted_monotonic_ns=103,
            instruction=_instruction(strategy_order_id="strategy-order-2"),
            checked_monotonic_ns=103,
        )
        self.assertIsInstance(retry, IngressAdmission)

    def test_uncommitted_new_order_credit_is_returned_on_abandon(self) -> None:
        scheduler = _scheduler(new_capacity=2, maximum_inflight_new_orders=1)
        first = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        self.assertIsInstance(first, IngressAdmission)
        assert isinstance(first, IngressAdmission)
        scheduler.abandon(first)
        retry = scheduler.admit_new_order(
            request_id="strategy-order-2",
            serialized_size_bytes=128,
            admitted_monotonic_ns=101,
        )
        self.assertIsInstance(retry, IngressAdmission)
        self.assertEqual(1, scheduler.snapshot().counters["new_order_inflight_released"])

    def test_atomic_new_order_rejects_invalid_command_without_consuming_capacity(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        with self.assertRaisesRegex(SchedulerStateError, "strategy_order_id"):
            scheduler.admit_and_commit_new_order(
                request_id="strategy-order-1",
                serialized_size_bytes=128,
                admitted_monotonic_ns=100,
                instruction=_instruction(strategy_order_id="other-order"),
                checked_monotonic_ns=100,
            )
        self.assertEqual(0, scheduler.snapshot().depths["new_order"])

    def test_dispatch_wait_wakes_when_atomic_ingress_queues_command(self) -> None:
        scheduler = _scheduler(new_capacity=1)
        waiter_started = Event()
        observed: list[bool] = []

        def wait_for_command() -> None:
            waiter_started.set()
            observed.append(scheduler.wait_for_dispatch(timeout_seconds=1.0))

        waiter = Thread(target=wait_for_command)
        waiter.start()
        self.assertTrue(waiter_started.wait(timeout=1.0))
        decision = scheduler.admit_and_commit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
            instruction=_instruction(),
            checked_monotonic_ns=100,
        )
        self.assertIsInstance(decision, IngressAdmission)
        waiter.join(timeout=1.0)
        self.assertFalse(waiter.is_alive())
        self.assertEqual([True], observed)

    def test_dispatch_notification_is_coalesced_until_both_lanes_are_empty(self) -> None:
        scheduler = _scheduler(new_capacity=3)
        for index in range(2):
            request_id = f"strategy-order-{index}"
            self.assertIsInstance(
                scheduler.admit_and_commit_new_order(
                    request_id=request_id,
                    serialized_size_bytes=128,
                    admitted_monotonic_ns=100 + index,
                    instruction=_instruction(strategy_order_id=request_id),
                    checked_monotonic_ns=100 + index,
                ),
                IngressAdmission,
            )
        self.assertEqual(1, scheduler.snapshot().counters["dispatch_wake_notifications"])

        self.assertIsNotNone(scheduler.dequeue(checked_monotonic_ns=103))
        self.assertIsNotNone(scheduler.dequeue(checked_monotonic_ns=104))
        self.assertIsNone(scheduler.dequeue(checked_monotonic_ns=105))

        self.assertIsInstance(
            scheduler.admit_and_commit_new_order(
                request_id="strategy-order-2",
                serialized_size_bytes=128,
                admitted_monotonic_ns=106,
                instruction=_instruction(strategy_order_id="strategy-order-2"),
                checked_monotonic_ns=106,
            ),
            IngressAdmission,
        )
        self.assertEqual(2, scheduler.snapshot().counters["dispatch_wake_notifications"])

    def test_queued_expiry_is_reported_as_pre_native_and_never_hidden(self) -> None:
        scheduler = _scheduler()
        admission = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        assert isinstance(admission, IngressAdmission)
        scheduler.commit_new_order(admission, _instruction(), checked_monotonic_ns=150)
        dispatch = scheduler.dequeue(checked_monotonic_ns=201)
        self.assertIsNotNone(dispatch)
        assert dispatch is not None
        self.assertTrue(dispatch.expired_before_native)
        self.assertEqual(101, dispatch.queue_wait_ns)
        self.assertEqual(1, scheduler.snapshot().counters["new_order_expired_pre_native"])

    def test_abandon_and_foreign_or_stale_tickets_fail_closed(self) -> None:
        scheduler = _scheduler()
        admission = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        assert isinstance(admission, IngressAdmission)
        scheduler.abandon(admission)
        with self.assertRaises(SchedulerStateError):
            scheduler.abandon(admission)

        cancel_admission = scheduler.admit_cancel(
            cancel_request_id="cancel-1",
            serialized_size_bytes=64,
            admitted_monotonic_ns=100,
        )
        assert isinstance(cancel_admission, IngressAdmission)
        with self.assertRaises(SchedulerStateError):
            scheduler.commit_new_order(
                cancel_admission,
                _instruction(strategy_order_id="cancel-1"),
                checked_monotonic_ns=101,
            )
        with self.assertRaises(SchedulerStateError):
            scheduler.commit_cancel(
                cancel_admission,
                _cancel(account_id="account-2"),
                checked_monotonic_ns=101,
            )
        self.assertEqual(1, scheduler.snapshot().depths["cancel"])

        foreign_scheduler = _scheduler()
        with self.assertRaises(SchedulerStateError):
            foreign_scheduler.commit_cancel(
                cancel_admission,
                _cancel(),
                checked_monotonic_ns=101,
            )

    def test_backwards_clock_does_not_drop_queued_command(self) -> None:
        scheduler = _scheduler()
        admission = scheduler.admit_new_order(
            request_id="strategy-order-1",
            serialized_size_bytes=128,
            admitted_monotonic_ns=100,
        )
        assert isinstance(admission, IngressAdmission)
        scheduler.commit_new_order(admission, _instruction(), checked_monotonic_ns=100)
        with self.assertRaises(SchedulerStateError):
            scheduler.dequeue(checked_monotonic_ns=99)
        self.assertEqual(1, scheduler.snapshot().depths["new_order"])
        self.assertIsNotNone(scheduler.dequeue(checked_monotonic_ns=100))

    def test_concurrent_ingress_never_exceeds_declared_capacity(self) -> None:
        scheduler = _scheduler(new_capacity=8)

        def admit(index: int) -> IngressAdmission | IngressRejection:
            return scheduler.admit_new_order(
                request_id=f"strategy-order-{index}",
                serialized_size_bytes=128,
                admitted_monotonic_ns=100,
            )

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(admit, range(64)))
        accepted = [item for item in results if isinstance(item, IngressAdmission)]
        rejected = [item for item in results if isinstance(item, IngressRejection)]
        self.assertEqual(8, len(accepted))
        self.assertEqual(56, len(rejected))
        self.assertTrue(
            all(item.code is IngressRejectionCode.INGRESS_OVERLOADED for item in rejected)
        )
        snapshot = scheduler.snapshot()
        self.assertEqual(8, snapshot.depths["new_order"])
        self.assertEqual(8, snapshot.max_observed_depths["new_order"])
        self.assertEqual(56, snapshot.counters["new_order_overloaded"])
        with self.assertRaises(TypeError):
            snapshot.depths["new_order"] = 9  # type: ignore[reportIndexIssue] # Frozen evidence cannot be mutated.

    def test_atomic_concurrent_ingress_never_exceeds_declared_capacity(self) -> None:
        scheduler = _scheduler(new_capacity=8)

        def schedule(index: int) -> IngressAdmission | IngressRejection:
            request_id = f"strategy-order-{index}"
            return scheduler.admit_and_commit_new_order(
                request_id=request_id,
                serialized_size_bytes=128,
                admitted_monotonic_ns=100,
                instruction=_instruction(strategy_order_id=request_id),
                checked_monotonic_ns=100,
            )

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(schedule, range(64)))
        accepted = [item for item in results if isinstance(item, IngressAdmission)]
        rejected = [item for item in results if isinstance(item, IngressRejection)]
        self.assertEqual(8, len(accepted))
        self.assertEqual(56, len(rejected))
        self.assertTrue(
            all(item.code is IngressRejectionCode.INGRESS_OVERLOADED for item in rejected)
        )
        snapshot = scheduler.snapshot()
        self.assertEqual(0, snapshot.depths["new_order_reservations"])
        self.assertEqual(8, snapshot.depths["new_order_queued"])
        self.assertEqual(8, snapshot.max_observed_depths["new_order"])
        self.assertEqual(56, snapshot.counters["new_order_overloaded"])


if __name__ == "__main__":
    unittest.main()
