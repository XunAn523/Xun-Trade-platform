from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from uuid import UUID

from xuanling_tradecore.read_model import (
    InMemoryReadModelProjectionStoreV1,
    ReadModelCheckpointKeyV1,
    ReadModelCheckpointV1,
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
    ReadModelRowMutationV1,
    ReadModelSnapshotReplacementV1,
    ReadModelTableV1,
)

_JOURNAL_ID = UUID("00000000-0000-0000-0000-000000000001")
_CONSUMER_ID = UUID("00000000-0000-0000-0000-000000000002")
_HASH_A = "a" * 64
_HASH_B = "b" * 64


def _position(ordinal: int) -> ReadModelPositionV1:
    return ReadModelPositionV1(_JOURNAL_ID, 1, 1, 1, ordinal)


def _checkpoint(ordinal: int, source_hash: str = _HASH_A) -> ReadModelCheckpointV1:
    return ReadModelCheckpointV1(
        key=ReadModelCheckpointKeyV1("f3-test", "scope-a", "account-scope-a"),
        projector_version="v1",
        position=_position(ordinal),
        source_fact_hash=source_hash,
    )


def _account(revision: int = 1, source_hash: str = _HASH_A) -> ReadModelRowMutationV1:
    return ReadModelRowMutationV1(
        ReadModelTableV1.ACCOUNTS,
        {
            "account_id": "account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "adapter-a",
            "consumer_id": _CONSUMER_ID,
            "account_display_name": "masked",
            "trading_day": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
            "trading_day_epoch": 1,
            "lifecycle": "DISARMED",
            "reconciliation_status": "REQUIRED",
            "active_gate_count": 1,
            "can_submit": False,
            "submit_block_reason_code": "DISARMED",
            "capability_hash": _HASH_A,
            "revision": revision,
            "source_fact_hash": source_hash,
            "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "projected_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    )


def _asset(snapshot_generation: int, source_hash: str = _HASH_A) -> ReadModelRowMutationV1:
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return ReadModelRowMutationV1(
        ReadModelTableV1.ASSETS,
        {
            "account_id": "account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "adapter-a",
            "trading_day": timestamp.date(),
            "trading_day_epoch": 1,
            "snapshot_generation": snapshot_generation,
            "currency": "CNY",
            "total_asset": Decimal("1"),
            "net_asset": Decimal("1"),
            "cash_balance": Decimal("1"),
            "available_cash": Decimal("1"),
            "frozen_cash": Decimal("0"),
            "market_value": Decimal("0"),
            "margin_used": Decimal("0"),
            "total_debt": Decimal("0"),
            "credit_limit": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "total_fee": Decimal("0"),
            "revision": snapshot_generation,
            "source_fact_hash": source_hash,
            "updated_at": timestamp,
            "projected_at": timestamp,
        },
    )


def _asset_snapshot(snapshot_generation: int) -> ReadModelSnapshotReplacementV1:
    return ReadModelSnapshotReplacementV1(
        table=ReadModelTableV1.ASSETS,
        scope={
            "adapter_instance_id": "adapter-a",
            "trading_day_epoch": 1,
            "account_id": "account-a",
            "venue_id": "EMT",
        },
        snapshot_generation=snapshot_generation,
        rows=(_asset(snapshot_generation),),
    )


class ReadModelProjectionV1Tests(TestCase):
    def test_replay_is_idempotent_and_conflicting_checkpoint_is_rejected(self) -> None:
        store = InMemoryReadModelProjectionStoreV1()
        batch = ReadModelProjectionBatchV1(None, _checkpoint(1), (_account(),))
        store.apply(batch)
        store.apply(batch)
        snapshot = store.snapshot(batch.checkpoint.key)
        self.assertEqual(
            _position(1), snapshot.checkpoint.position if snapshot.checkpoint else None
        )
        conflict = ReadModelProjectionBatchV1(None, _checkpoint(1, _HASH_B), (_account(),))
        with self.assertRaisesRegex(ReadModelProjectionErrorV1, "conflicting source hash"):
            store.apply(conflict)

    def test_failed_snapshot_does_not_publish_rows_or_advance_checkpoint(self) -> None:
        store = InMemoryReadModelProjectionStoreV1()
        first = ReadModelProjectionBatchV1(
            None, _checkpoint(1), (_account(),), (_asset_snapshot(1),)
        )
        store.apply(first)
        batch = ReadModelProjectionBatchV1(
            first.checkpoint.position,
            _checkpoint(2, _HASH_B),
            (_account(2, _HASH_B),),
            (_asset_snapshot(1),),
        )
        with self.assertRaisesRegex(ReadModelProjectionErrorV1, "not newer"):
            store.apply(batch)
        snapshot = store.snapshot(first.checkpoint.key)
        self.assertEqual(
            _position(1), snapshot.checkpoint.position if snapshot.checkpoint else None
        )
        self.assertEqual(
            1,
            snapshot.rows[ReadModelTableV1.ACCOUNTS][("account-a", "EMT", "adapter-a")]["revision"],
        )
        self.assertEqual(1, len(snapshot.rows[ReadModelTableV1.ASSETS]))

    def test_snapshot_replacement_requires_new_generation_and_keeps_only_current_rows(self) -> None:
        store = InMemoryReadModelProjectionStoreV1()
        first = ReadModelProjectionBatchV1(
            None, _checkpoint(1), (_account(),), (_asset_snapshot(1),)
        )
        store.apply(first)
        second = ReadModelProjectionBatchV1(
            first.checkpoint.position,
            _checkpoint(2, _HASH_B),
            (),
            (_asset_snapshot(2),),
        )
        store.apply(second)
        rows = store.snapshot(second.checkpoint.key).rows[ReadModelTableV1.ASSETS]
        self.assertEqual(1, len(rows))
        self.assertEqual(2, next(iter(rows.values()))["snapshot_generation"])
        same_generation = ReadModelProjectionBatchV1(
            second.checkpoint.position,
            _checkpoint(3, "c" * 64),
            (),
            (_asset_snapshot(2),),
        )
        with self.assertRaisesRegex(ReadModelProjectionErrorV1, "not newer"):
            store.apply(same_generation)

    def test_float_is_not_an_accepted_projection_scalar(self) -> None:
        with self.assertRaisesRegex(ReadModelProjectionErrorV1, "non-SQL scalar"):
            ReadModelRowMutationV1(
                ReadModelTableV1.STRATEGIES,
                {
                    "strategy_id": UUID("00000000-0000-0000-0000-000000000003"),
                    "revision": 1,
                    "source_fact_hash": _HASH_A,
                    "bad": 1.5,
                },
            )


if __name__ == "__main__":
    import unittest

    unittest.main()
