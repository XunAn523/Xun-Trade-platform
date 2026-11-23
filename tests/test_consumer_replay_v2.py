from __future__ import annotations

import unittest
from itertools import product
from types import SimpleNamespace
from typing import cast

from xuanling_tradecore.consumer import (
    CursorKeyRingV2,
    CursorSigningKeyV2,
    PositionedConsumerEventV2,
)
from xuanling_tradecore.consumer.replay_v2 import (
    ConsumerCursorReplayError,
    ConsumerCursorReplayServiceV2,
    ConsumerReplayPartitionBoundsV2,
    ConsumerReplaySliceV2,
)
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerEvent,
    ConsumerEventPositionV2,
    RedisStreamIdV1,
)
from xuanling_tradecore.domain._validation import DomainValidationError


class _Reader:
    def read_events(self, **kwargs: object) -> ConsumerReplaySliceV2:
        position = ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 0)
        binding = cast(ConsumerAccountBinding, kwargs["binding"])
        event = SimpleNamespace(
            journal_id="journal-1",
            account_id=binding.account_id,
            consumer_trace=SimpleNamespace(
                consumer_id=binding.consumer_id,
                consumer_account_id=binding.consumer_account_id,
                account_id=binding.account_id,
            ),
        )
        return ConsumerReplaySliceV2(
            (PositionedConsumerEventV2(position, cast(ConsumerEvent, event)),), position, False
        )


class ConsumerReplayV2Tests(unittest.TestCase):
    def test_replay_payload_cannot_cross_underlying_account_even_with_same_external_scope(self):
        binding = ConsumerAccountBinding("b", 1, "c", "ca", "a", "sim", "EMT", "i")
        stream = CursorStreamRefV2("journal-1", 1)
        page = _Reader().read_events(binding=binding)
        wrong = SimpleNamespace(
            journal_id="journal-1",
            account_id="foreign-account",
            consumer_trace=SimpleNamespace(
                consumer_id="c", consumer_account_id="ca", account_id="foreign-account"
            ),
        )
        item = PositionedConsumerEventV2(page.items[0].position, cast(ConsumerEvent, wrong))
        with self.assertRaises(ConsumerCursorReplayError):
            ConsumerCursorReplayServiceV2._validate_page(
                binding, stream, None, ConsumerReplaySliceV2((item,), item.position, False)
            )

    def test_linear_slice_validation_preserves_order_uniqueness_and_fences(self) -> None:
        positions = [ConsumerEventPositionV2(RedisStreamIdV1(i, 0), 0) for i in range(3)]
        event = cast(ConsumerEvent, SimpleNamespace())
        for length in range(5):
            for sequence in product(positions, repeat=length):
                for high in (None, *positions):
                    for more in (False, True):
                        valid = (
                            list(sequence) == sorted(sequence)
                            and len(set(sequence)) == len(sequence)
                            and (not more or bool(sequence))
                            and (
                                not sequence and not more
                                if high is None
                                else all(p <= high for p in sequence)
                            )
                        )
                        items = tuple(PositionedConsumerEventV2(p, event) for p in sequence)
                        if valid:
                            self.assertEqual(items, ConsumerReplaySliceV2(items, high, more).items)
                        else:
                            with self.assertRaises(DomainValidationError):
                                ConsumerReplaySliceV2(items, high, more)

    def test_partition_capabilities_bind_bounds_epoch_and_expiry(self) -> None:
        binding = ConsumerAccountBinding("b", 1, "c", "ca", "a", "sim", "EMT", "i")
        stream = CursorStreamRefV2("journal-1", 1)
        high = ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 0)
        ring = CursorKeyRingV2(current=CursorSigningKeyV2("k", b"x" * 32))
        clock = [10]

        class BoundsReader:
            bounds = (ConsumerReplayPartitionBoundsV2(None, high, 1),)

            def replay_partition_bounds(self, **kwargs):
                return self.bounds

        reader = BoundsReader()
        service = ConsumerCursorReplayServiceV2(
            key_ring_for=lambda _: ring,
            registry_for=lambda _: CursorStreamRegistryV2(stream, (stream,)),
            event_reader=_Reader(),
            partition_reader=reader,
            cursor_ttl_ns=100,
            wall_clock_ns=lambda: clock[0],
        )
        plan = service.issue_partitions(binding=binding, authorization_epoch=1, partition_count=4)
        token = plan.partitions[0].cursor_token
        claims = ring.verify(token, evaluated_wall_ns=10)
        self.assertIsNone(claims.after_position)
        self.assertEqual(high, claims.high_watermark_position)
        page = service.poll(binding=binding, authorization_epoch=1, token=token, limit=1)
        continued = ring.verify(page.next_cursor_token, evaluated_wall_ns=10)
        self.assertEqual(high, continued.after_position)
        self.assertEqual(high, continued.high_watermark_position)
        for epoch in (0, 2):
            with self.assertRaises(ConsumerCursorReplayError):
                service.poll(binding=binding, authorization_epoch=epoch, token=token, limit=1)
        other = ConsumerAccountBinding("b2", 1, "c2", "ca2", "a2", "sim", "EMT", "i")
        with self.assertRaises(ConsumerCursorReplayError):
            service.poll(binding=other, authorization_epoch=1, token=token, limit=1)
        for count in (0, 65, True, 1.5):
            with self.assertRaises(ConsumerCursorReplayError):
                service.issue_partitions(
                    binding=binding, authorization_epoch=1, partition_count=count
                )
        reader.bounds = (reader.bounds[0], reader.bounds[0])
        with self.assertRaises(ConsumerCursorReplayError):
            service.issue_partitions(binding=binding, authorization_epoch=1, partition_count=4)
        clock[0] = 110
        with self.assertRaisesRegex(ConsumerCursorReplayError, "cannot be used"):
            service.poll(binding=binding, authorization_epoch=1, token=token, limit=1)

    def test_binds_cursor_and_preserves_empty_page_token(self) -> None:
        binding = ConsumerAccountBinding("b", 1, "c", "ca", "a", "sim", "EMT", "i")
        stream = CursorStreamRefV2("journal-1", 1)
        service = ConsumerCursorReplayServiceV2(
            key_ring_for=lambda _: CursorKeyRingV2(current=CursorSigningKeyV2("k", b"x" * 32)),
            registry_for=lambda _: CursorStreamRegistryV2(stream, (stream,)),
            event_reader=_Reader(),
            cursor_ttl_ns=100,
            wall_clock_ns=lambda: 10,
        )
        token = service.issue_begin_cursor(binding=binding, authorization_epoch=1)
        page = service.poll(binding=binding, authorization_epoch=1, token=token, limit=1)
        self.assertEqual(1, len(page.items))
        with self.assertRaises(ConsumerCursorReplayError):
            service.poll(
                binding=binding, authorization_epoch=2, token=page.next_cursor_token, limit=1
            )
