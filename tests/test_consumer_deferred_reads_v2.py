from __future__ import annotations

import selectors
import unittest
from contextvars import ContextVar
from threading import Event

from xuanling_tradecore.consumer.deferred_reads_v2 import BoundedDeferredReadsV2


class DeferredReadsTests(unittest.TestCase):
    def pool(self, capacity=2, maximum_response_bytes=32):
        pool = BoundedDeferredReadsV2(
            workers=1, capacity=capacity, maximum_response_bytes=maximum_response_bytes
        )
        pool.start()
        self.addCleanup(pool.close)
        return pool

    def ready(self, pool):
        with selectors.DefaultSelector() as selector:
            selector.register(pool.reader, selectors.EVENT_READ)
            self.assertTrue(selector.select(2), "completion did not wake the selector")

    def test_undelivered_results_keep_capacity_and_wake_selector(self):
        pool = self.pool(capacity=1)
        self.assertTrue(pool.offer(1, lambda: b"one"))
        self.ready(pool)
        self.assertFalse(pool.offer(2, lambda: b"two"))
        self.assertEqual(1, pool.outstanding)
        result = pool.take()
        self.assertEqual((1, b"one", None), (result.token, result.response, result.error))
        pool.clear_notification()
        self.assertEqual(0, pool.outstanding)
        self.assertTrue(pool.offer(2, lambda: b"two"))
        self.ready(pool)
        self.assertEqual(b"two", pool.take().response)

    def test_running_and_queued_work_share_total_budget(self):
        pool = self.pool()
        entered, release = Event(), Event()

        def run():
            entered.set()
            release.wait(2)
            return b"done"

        try:
            self.assertTrue(pool.offer(1, run))
            self.assertTrue(entered.wait(2))
            self.assertTrue(pool.offer(2, lambda: b"queued"))
            self.assertFalse(pool.offer(3, lambda: b"excess"))
        finally:
            release.set()
        pool.close()
        self.assertEqual([1, 2], [pool.take().token, pool.take().token])
        self.assertIsNone(pool.take())
        self.assertFalse(pool.offer(4, lambda: b"closed"))

    def test_context_is_captured_and_worker_error_does_not_kill_next_read(self):
        pool = self.pool()
        value = ContextVar("test_read_context", default="unset")
        value.set("captured")
        self.assertTrue(pool.offer(1, lambda: value.get().encode()))
        value.set("changed")
        self.ready(pool)
        self.assertEqual(b"captured", pool.take().response)
        pool.clear_notification()

        def fail():
            raise OSError("private detail")

        self.assertTrue(pool.offer(2, fail))
        self.ready(pool)
        result = pool.take()
        self.assertIsNone(result.response)
        self.assertIsInstance(result.error, OSError)
        pool.clear_notification()
        self.assertTrue(pool.offer(3, lambda: b"healthy"))
        self.ready(pool)
        self.assertEqual(b"healthy", pool.take().response)

    def test_oversize_result_is_failure_not_unbounded_delivery(self):
        pool = self.pool(maximum_response_bytes=4)
        pool.offer(1, lambda: b"12345")
        self.ready(pool)
        result = pool.take()
        self.assertIsNone(result.response)
        self.assertIsInstance(result.error, ValueError)

    def test_thread_local_abort_completes_token_instead_of_hanging_drain(self):
        pool = self.pool()

        def abort():
            raise SystemExit("not a process-level shutdown")

        pool.offer(1, abort)
        self.ready(pool)
        self.assertIsInstance(pool.take().error, RuntimeError)
        self.assertEqual(0, pool.outstanding)
        pool.close()

    def test_stuck_read_is_failed_drain_and_cannot_accept_more(self):
        pool = self.pool()
        entered, release = Event(), Event()

        def run():
            entered.set()
            release.wait(2)
            return b"done"

        pool.offer(1, run)
        self.assertTrue(entered.wait(2))
        try:
            with self.assertRaisesRegex(RuntimeError, "drain timed out"):
                pool.close(timeout=0.001)
            self.assertFalse(pool.offer(2, lambda: b"no"))
        finally:
            release.set()
            pool.close()
        self.assertEqual(b"done", pool.take().response)

    def test_limits_are_strict_and_finite(self):
        for workers, capacity in ((True, 2), (0, 2), (2, 1), (17, 32), (1, 1025)):
            with self.subTest(workers=workers, capacity=capacity), self.assertRaises(ValueError):
                BoundedDeferredReadsV2(
                    workers=workers, capacity=capacity, maximum_response_bytes=10
                )
