from __future__ import annotations

import unittest

from xuanling_tradecore.consumer import (
    ConsumerPagedSnapshotErrorV2,
    ConsumerSession,
    ConsumerSnapshotIndexV2,
    ConsumerSnapshotVersionV2,
    SnapshotCursorCodecV2,
    SnapshotCursorSigningKeyV2,
)
from xuanling_tradecore.consumer.stream_registry_v2 import (
    CursorStreamRefV2,
    CursorStreamRegistryV2,
)
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerApiVersion,
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    RedisStreamIdV1,
    TradingDayEpoch,
)


class _Reader:
    def __init__(self, orders: tuple[ConsumerOrderEvent, ...]) -> None:
        self.orders = orders
        self.calls = 0

    def capture_snapshot_version(self, **_kwargs: object) -> ConsumerSnapshotVersionV2:
        self.calls += 1
        return ConsumerSnapshotVersionV2(
            ConsumerEventPositionV2(RedisStreamIdV1(99, 0), 0), self.orders
        )


class ConsumerSnapshotIndexV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = 1_000
        self.binding = ConsumerAccountBinding(
            "binding-1", 1, "consumer-1", "ca-1", "account-1", "sim", "EMT", "emt-1"
        )
        self.stream = CursorStreamRefV2("journal-1", 1)
        self.registry = CursorStreamRegistryV2(self.stream, (self.stream,))
        self.session = ConsumerSession("session-1", "consumer-1", ConsumerApiVersion(2, 0), 7, 2)
        self.reader = _Reader(tuple(self._order(index) for index in (3, 1, 2)))

    def _index(
        self,
        *,
        maximum_active_snapshots: int = 8,
        page_payload_budget_bytes: int = 262_144,
    ) -> ConsumerSnapshotIndexV2:
        return ConsumerSnapshotIndexV2(
            registry_for=lambda _: self.registry,
            reader=self.reader,
            cursor_codec=SnapshotCursorCodecV2(
                SnapshotCursorSigningKeyV2("snapshot-key", b"s" * 32)
            ),
            wall_clock_ns=lambda: self.now,
            ttl_ns=100,
            maximum_active_snapshots=maximum_active_snapshots,
            page_payload_budget_bytes=page_payload_budget_bytes,
        )

    def _order(self, index: int) -> ConsumerOrderEvent:
        trace = ConsumerTrace(
            "consumer-1",
            "ca-1",
            "account-1",
            "strategy-1",
            f"strategy-order-{index}",
            "attribution-1",
            1,
            OrderOrigin.STRATEGY,
        )
        return ConsumerOrderEvent(
            f"event-{index}",
            ConsumerEventKind.ORDER,
            2,
            "account-1",
            TradingDayEpoch("2026-10-08"),
            "journal-1",
            index,
            trace,
            f"oms-{index}",
            f"execution-{index}",
            OmsOrderState.WORKING,
            1,
        )

    def test_pages_are_stable_retryable_and_expire(self) -> None:
        index = self._index()
        plan = index.create(session=self.session, binding=self.binding, page_size=2)
        self.assertEqual(3, plan.order_count)
        first = index.read_page(
            session=self.session, binding=self.binding, page_cursor=plan.page_cursor
        )
        retry = index.read_page(
            session=self.session, binding=self.binding, page_cursor=plan.page_cursor
        )
        self.assertEqual(first, retry)
        self.assertIs(first.order_payloads, retry.order_payloads)
        self.assertIs(first.next_page_cursor, retry.next_page_cursor)
        self.assertEqual(("oms-1", "oms-2"), tuple(item.oms_order_id for item in first.orders))
        self.assertEqual(
            ("event-1", "event-2"), tuple(item["event_id"] for item in first.order_payloads)
        )
        self.assertTrue(first.has_more)
        self.assertIsNotNone(first.next_page_cursor)

        self.reader.orders = (self._order(9),)
        final = index.read_page(
            session=self.session,
            binding=self.binding,
            page_cursor=first.next_page_cursor or "",
        )
        self.assertEqual(("oms-3",), tuple(item.oms_order_id for item in final.orders))
        self.assertFalse(final.has_more)
        self.assertIsNone(final.next_page_cursor)

        self.now = plan.expires_at_wall_ns
        with self.assertRaisesRegex(ConsumerPagedSnapshotErrorV2, "expired") as raised:
            index.read_page(
                session=self.session, binding=self.binding, page_cursor=plan.page_cursor
            )
        self.assertEqual("SNAPSHOT_EXPIRED", raised.exception.code)

    def test_live_codec_provider_rotates_signing_without_invalidating_old_pins(self):
        old = SnapshotCursorSigningKeyV2("old", b"o" * 32)
        codecs = [SnapshotCursorCodecV2(old)]
        index = ConsumerSnapshotIndexV2(
            registry_for=lambda _: self.registry,
            reader=self.reader,
            cursor_codec=codecs[0],
            cursor_codec_for=lambda: codecs[0],
            wall_clock_ns=lambda: self.now,
            ttl_ns=100,
        )
        try:
            first = index.create(session=self.session, binding=self.binding, page_size=2)
            codecs[0] = SnapshotCursorCodecV2(
                SnapshotCursorSigningKeyV2("new", b"n" * 32),
                retired_keys=(SnapshotCursorSigningKeyV2("old", b"o" * 32, 2000),),
            )
            second = index.create(session=self.session, binding=self.binding, page_size=2)
            assert codecs[0].verify(second.page_cursor, evaluated_wall_ns=self.now).key_id == "new"
            page = index.read_page(
                session=self.session, binding=self.binding, page_cursor=first.page_cursor
            )
            assert len(page.orders) == 2
        finally:
            index.close()

    def test_rejects_tampering_scope_capacity_and_page_bytes(self) -> None:
        index = self._index(maximum_active_snapshots=1)
        plan = index.create(session=self.session, binding=self.binding, page_size=2)
        with self.assertRaises(ConsumerPagedSnapshotErrorV2) as tampered:
            index.read_page(
                session=self.session,
                binding=self.binding,
                page_cursor=plan.page_cursor[:-1] + ("A" if plan.page_cursor[-1] != "A" else "B"),
            )
        self.assertEqual("CURSOR_INVALID", tampered.exception.code)

        other = ConsumerSession("session-2", "consumer-2", ConsumerApiVersion(2, 0), 7, 2)
        with self.assertRaises(ConsumerPagedSnapshotErrorV2) as crossed:
            index.read_page(session=other, binding=self.binding, page_cursor=plan.page_cursor)
        self.assertEqual("CURSOR_INVALID", crossed.exception.code)
        with self.assertRaises(ConsumerPagedSnapshotErrorV2) as full:
            index.create(session=self.session, binding=self.binding, page_size=2)
        self.assertEqual("RATE_LIMITED", full.exception.code)

        tiny = self._index(page_payload_budget_bytes=1)
        tiny_plan = tiny.create(session=self.session, binding=self.binding, page_size=1)
        with self.assertRaises(ConsumerPagedSnapshotErrorV2) as too_large:
            tiny.read_page(
                session=self.session, binding=self.binding, page_cursor=tiny_plan.page_cursor
            )
        self.assertEqual("FRAME_TOO_LARGE", too_large.exception.code)

    def test_empty_snapshot_finishes_with_one_empty_page(self) -> None:
        self.reader.orders = ()
        index = self._index()
        plan = index.create(session=self.session, binding=self.binding, page_size=100)
        page = index.read_page(
            session=self.session, binding=self.binding, page_cursor=plan.page_cursor
        )
        self.assertEqual(0, plan.order_count)
        self.assertEqual((), page.orders)
        self.assertEqual((), page.order_payloads)
        self.assertFalse(page.has_more)

    def test_cached_only_never_cleans_pins_or_builds_missing_page(self):
        from dataclasses import replace
        from unittest.mock import patch

        from xuanling_tradecore.consumer.snapshot_index_v2 import cached_snapshot_only

        index = self._index()
        plan = index.create(session=self.session, binding=self.binding, page_size=2)
        arguments = dict(session=self.session, binding=self.binding, page_cursor=plan.page_cursor)
        self.assertFalse(index.cached_page_ready(**arguments))
        with (
            cached_snapshot_only(),
            patch.object(
                index, "_expire_locked", side_effect=AssertionError("IO cleanup on socket owner")
            ),
        ):
            with self.assertRaises(ConsumerPagedSnapshotErrorV2) as missing:
                index.read_page(**arguments)
            self.assertEqual("SNAPSHOT_NOT_READY", missing.exception.code)
        warm = index.read_page(**arguments)
        self.assertTrue(index.cached_page_ready(**arguments))
        with (
            cached_snapshot_only(),
            patch.object(
                index, "_expire_locked", side_effect=AssertionError("IO cleanup on socket owner")
            ),
        ):
            self.assertEqual(warm, index.read_page(**arguments))
            self.assertFalse(
                index.cached_page_ready(
                    **(arguments | {"session": replace(self.session, consumer_id="other")})
                )
            )
            self.now += 100
            self.assertFalse(index.cached_page_ready(**arguments))
            with self.assertRaises(ConsumerPagedSnapshotErrorV2) as expired:
                index.read_page(**arguments)
            self.assertEqual("SNAPSHOT_EXPIRED", expired.exception.code)
        index.close()

    def test_cached_hint_reuses_one_verified_claim_but_delivery_still_rechecks(self):
        from unittest.mock import patch

        index = self._index()
        plan = index.create(session=self.session, binding=self.binding, page_size=2)
        arguments = dict(session=self.session, binding=self.binding, page_cursor=plan.page_cursor)
        index.read_page(**arguments)
        with patch.object(index._codec, "verify", wraps=index._codec.verify) as verify:
            self.assertTrue(index.cached_page_ready(**arguments))
            self.assertEqual(1, verify.call_count)
            self.assertIsNone(index.validate_page_delivery(**arguments))
            self.assertEqual(2, verify.call_count)
            self.now += 100
            self.assertFalse(index.cached_page_ready(**arguments))
            with self.assertRaises(ConsumerPagedSnapshotErrorV2):
                index.validate_page_delivery(**arguments)
        index.close()


if __name__ == "__main__":
    unittest.main()
