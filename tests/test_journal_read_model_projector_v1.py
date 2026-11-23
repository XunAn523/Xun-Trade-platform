from __future__ import annotations

from dataclasses import dataclass
from unittest import TestCase
from uuid import UUID

from xuanling_tradecore.oms.sidecar_v1 import (
    OmsSidecarFactKind,
    OmsSidecarFactReaderV1,
    OmsSidecarFactV1,
)
from xuanling_tradecore.read_model import (
    BoundedReadModelProjectorQueueV1,
    CommittedReadModelFactV1,
    InMemoryReadModelProjectionStoreV1,
    JournalFirstProjectionErrorV1,
    JournalFirstReadModelProjectorV1,
    ReadModelCheckpointKeyV1,
    ReadModelCheckpointV1,
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
    ReadModelRowMutationV1,
    ReadModelTableV1,
)

_JOURNAL_ID = UUID("00000000-0000-0000-0000-000000000051")


def _fact(position: int) -> CommittedReadModelFactV1:
    fact = OmsSidecarFactV1(
        scope_id="scope-a",
        position=position,
        schema_version=1,
        activation_epoch=1,
        route_version=1,
        kind=OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
        execution_id="execution-a",
        canonical_client_order_ref="reference-a",
        payload_hash="a" * 64,
        observed_monotonic_ns=position,
    )
    return CommittedReadModelFactV1(
        sidecar_fact=fact,
        position=ReadModelPositionV1(_JOURNAL_ID, 1, 1, 1, position),
        source_fact_hash=fact.fingerprint,
    )


@dataclass
class _Mapper:
    calls: int = 0

    def map_committed_facts(self, *, facts, expected_checkpoint):
        self.calls += 1
        final = facts[-1]
        return ReadModelProjectionBatchV1(
            expected_checkpoint,
            ReadModelCheckpointV1(
                ReadModelCheckpointKeyV1("f3-test", "shard-a", "account-a"),
                "v1",
                final.position,
                final.source_fact_hash,
            ),
            (
                ReadModelRowMutationV1(
                    ReadModelTableV1.STRATEGIES,
                    {
                        "strategy_id": UUID("00000000-0000-0000-0000-000000000052"),
                        "revision": final.sidecar_fact.position,
                        "source_fact_hash": final.source_fact_hash,
                    },
                ),
            ),
        )


@dataclass
class _FailOnceStore:
    calls: int = 0

    def apply(self, batch):
        del batch
        self.calls += 1
        if self.calls == 1:
            raise ReadModelProjectionErrorV1("simulated PostgreSQL unavailable")
        return None


def _projector(*, expected_checkpoint=None, store=None):
    reader = OmsSidecarFactReaderV1(
        scope_id="scope-a",
        activation_epoch=1,
        route_version=1,
        initial_watermark=0 if expected_checkpoint is None else expected_checkpoint.fact_ordinal,
    )
    return JournalFirstReadModelProjectorV1(
        fact_reader=reader,
        mapper=_Mapper(),
        store=InMemoryReadModelProjectionStoreV1() if store is None else store,
        expected_checkpoint=expected_checkpoint,
    )


class JournalFirstReadModelProjectorV1Tests(TestCase):
    def test_only_contiguous_fact_suffix_can_advance_checkpoint(self) -> None:
        projector = _projector()
        first = _fact(1)
        second = _fact(2)
        projector.project((first, second))
        self.assertEqual(second.position, projector.checkpoint)
        with self.assertRaisesRegex(JournalFirstProjectionErrorV1, "duplicate or uncommitted"):
            projector.project((second,))

    def test_restart_from_checkpoint_accepts_the_next_journal_suffix(self) -> None:
        first = _fact(1)
        store = InMemoryReadModelProjectionStoreV1()
        _projector(store=store).project((first,))
        resumed = _projector(expected_checkpoint=first.position, store=store)
        resumed.project((_fact(2),))
        self.assertEqual(_fact(2).position, resumed.checkpoint)

    def test_bounded_handoff_never_waits_for_projector_or_database(self) -> None:
        health = []
        queue = BoundedReadModelProjectorQueueV1(
            projector=_projector(),
            capacity=1,
            on_health_change=health.append,
        )
        self.assertTrue(queue.offer((_fact(1),)))
        self.assertFalse(queue.offer((_fact(2),)))
        self.assertEqual(1, queue.health().rejected_batches)
        self.assertTrue(queue.drain_once())
        self.assertEqual(1, queue.health().completed_batches)
        self.assertTrue(health)

    def test_failed_background_projection_rewinds_reader_and_preserves_pending_fact(self) -> None:
        store = _FailOnceStore()
        queue = BoundedReadModelProjectorQueueV1(
            projector=_projector(store=store),
            capacity=1,
        )
        self.assertTrue(queue.offer((_fact(1),)))
        with self.assertRaisesRegex(ReadModelProjectionErrorV1, "unavailable"):
            queue.drain_once()
        failed = queue.health()
        self.assertEqual(1, failed.queued_batches)
        self.assertEqual("ReadModelProjectionErrorV1", failed.last_error_code)
        self.assertTrue(queue.drain_once())
        self.assertEqual(1, queue.health().completed_batches)


if __name__ == "__main__":
    import unittest

    unittest.main()
