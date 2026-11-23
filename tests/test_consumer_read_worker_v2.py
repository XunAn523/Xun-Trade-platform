from __future__ import annotations

import time
import unittest
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import patch

from xuanling_tradecore.consumer import (
    ConsumerPagedSnapshotErrorV2,
    ConsumerSnapshotReadWorkerV2,
    ConsumerSnapshotVersionV2,
)
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerEventKind,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    TradingDayEpoch,
)


class ConsumerSnapshotReadWorkerV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.binding = ConsumerAccountBinding(
            "binding-1", 1, "consumer-1", "ca-1", "account-1", "sim", "EMT", "emt-1"
        )
        self.stream = CursorStreamRefV2("journal-1", 1)

    def _event(self, index: int, *, consumer_account_id: str = "ca-1") -> ConsumerOrderEvent:
        return ConsumerOrderEvent(
            f"event-{index}",
            ConsumerEventKind.ORDER,
            2,
            "account-1",
            TradingDayEpoch("2026-10-08"),
            "journal-1",
            index,
            ConsumerTrace(
                "consumer-1",
                consumer_account_id,
                "account-1",
                "strategy-1",
                f"strategy-order-{index}",
                "attribution-1",
                1,
                OrderOrigin.STRATEGY,
            ),
            f"oms-{index:04d}",
            f"execution-{index}",
            OmsOrderState.ACCEPTED,
            0,
        )

    def _await_version(
        self, worker: ConsumerSnapshotReadWorkerV2
    ) -> ConsumerSnapshotVersionV2:
        deadline = time.monotonic() + 10
        while True:
            try:
                return worker.capture_snapshot_version(
                    binding=self.binding, stream=self.stream
                )
            except ConsumerPagedSnapshotErrorV2 as exc:
                self.assertEqual("SNAPSHOT_NOT_READY", exc.code)
            if time.monotonic() >= deadline:
                self.fail("isolated snapshot version did not finish")
            time.sleep(0.01)

    def test_builds_single_flight_versions_in_an_isolated_process(self) -> None:
        worker = ConsumerSnapshotReadWorkerV2(chunk_size=2)
        worker.start()
        try:
            for index in (3, 1, 2):
                worker.publish(self._event(index))
            first = self._await_version(worker)
            self.assertEqual(
                ("oms-0001", "oms-0002", "oms-0003"),
                tuple(order.oms_order_id for order in first.orders),
            )
            self.assertEqual(1, worker.stats().build_submissions)
            self.assertEqual(first, self._await_version(worker))
            self.assertEqual(1, worker.stats().build_submissions)

            worker.publish(self._event(4))
            second = self._await_version(worker)
            self.assertEqual(4, len(second.orders))
            self.assertEqual(2, worker.stats().build_submissions)
            self.assertEqual(0, worker.stats().build_failures)
        finally:
            worker.close()

    def test_fails_closed_when_scope_changes(self) -> None:
        worker = ConsumerSnapshotReadWorkerV2()
        worker.start()
        try:
            worker.publish(self._event(1))
            worker.publish(self._event(2, consumer_account_id="other"))
            with self.assertRaises(ConsumerPagedSnapshotErrorV2) as raised:
                worker.capture_snapshot_version(binding=self.binding, stream=self.stream)
            self.assertEqual("SNAPSHOT_NOT_READY", raised.exception.code)
        finally:
            worker.close()

    def test_recovers_when_process_pool_breaks_during_submit(self) -> None:
        worker = ConsumerSnapshotReadWorkerV2()
        worker.start()
        original_submit = ProcessPoolExecutor.submit
        failed_once = False

        def fail_once(
            executor: ProcessPoolExecutor, function: object, *args: object, **kwargs: object
        ) -> object:
            nonlocal failed_once
            if not failed_once:
                failed_once = True
                raise BrokenProcessPool("injected worker loss")
            return original_submit(executor, function, *args, **kwargs)  # type: ignore[arg-type, return-value]

        try:
            worker.publish(self._event(1))
            with patch.object(ProcessPoolExecutor, "submit", new=fail_once):
                version = self._await_version(worker)
            self.assertEqual(("oms-0001",), tuple(order.oms_order_id for order in version.orders))
            self.assertEqual(1, worker.stats().build_failures)
        finally:
            worker.close()

    def test_disables_parent_gc_before_build_and_restores_it_on_close(self) -> None:
        worker = ConsumerSnapshotReadWorkerV2(
            disable_parent_automatic_gc_on_start=True
        )
        with (
            patch("xuanling_tradecore.consumer.read_worker_v2.gc.collect") as collect,
            patch("xuanling_tradecore.consumer.read_worker_v2.gc.freeze") as freeze,
            patch(
                "xuanling_tradecore.consumer.read_worker_v2.gc.isenabled",
                side_effect=(True, False),
            ),
            patch("xuanling_tradecore.consumer.read_worker_v2.gc.disable") as disable,
            patch("xuanling_tradecore.consumer.read_worker_v2.gc.enable") as enable,
            patch("xuanling_tradecore.consumer.read_worker_v2.gc.unfreeze") as unfreeze,
        ):
            worker.start()
            disable.assert_called_once_with()
            collect.assert_not_called()
            freeze.assert_not_called()
            try:
                worker.publish(self._event(1))
                version = self._await_version(worker)
                self.assertEqual(("oms-0001",), tuple(item.oms_order_id for item in version.orders))
                stats = worker.stats()
                self.assertEqual(0, stats.parent_gc_compactions)
                self.assertEqual(0, stats.parent_gc_frozen_objects)
                self.assertTrue(stats.parent_automatic_gc_disabled)
            finally:
                worker.close()
            unfreeze.assert_not_called()
            enable.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
