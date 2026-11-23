"""Lease-held startup composition for a persistent, no-Adapter OMS runtime.

This is a recovery component, not permission to enable the LONG_RUNNING server:
bounded projections, cold idempotency, retention and maintenance remain gates.
"""

from __future__ import annotations

import os
from pathlib import Path

from ..oms.mmap_fact_journal_v1 import MmapOmsFactRecordV1
from ..oms.order_book import OmsJournalCommit, OmsOrderBookError, OrderBook
from ..oms.recovery_checkpoint import JournalCheckpointStore, decode_checkpoint
from ..oms.segmented_fact_journal import SegmentedOmsFactJournal
from .runtime_identity import PersistentConsumerRuntimeIdentity
from .runtime_profile import ConsumerLongRunningPolicy


class PersistentOmsRuntime:
    """Recover once before accepting work; a failed bootstrap never returns a book."""

    def __init__(
        self,
        *,
        root: Path,
        scope_sha256: str,
        policy: ConsumerLongRunningPolicy,
        initialize: bool = False,
    ) -> None:
        if not initialize and not root.is_dir():
            raise OmsOrderBookError(
                "persistent runtime is missing; explicit initialization required"
            )
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
        if root.is_symlink() or (
            os.name == "posix"
            and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        ):
            raise OmsOrderBookError("persistent runtime root is unsafe")
        if initialize and any(root.iterdir()):
            raise OmsOrderBookError("persistent initialization requires an empty data root")
        self.identity = PersistentConsumerRuntimeIdentity(
            root=root / "identity",
            scope_sha256=scope_sha256,
            create=initialize,
        )
        self._closed = False
        journal: SegmentedOmsFactJournal | None = None
        try:
            if not initialize and not (root / "journal" / "segments.json").is_file():
                raise OmsOrderBookError("persistent runtime lost its Journal manifest")
            journal = SegmentedOmsFactJournal(
                root=root / "journal",
                journal_id=self.identity.journal_id,
                segment_frames=policy.segment_frames,
                spare_segments=policy.spare_segments,
                disk_budget_bytes=policy.journal_disk_budget_bytes,
            )
            self.journal = journal
            self.checkpoints = JournalCheckpointStore(root / "checkpoints")
            self.order_book = OrderBook(journal=journal, persist_recovery_seed=True)
            watermark = 0
            if (root / "checkpoints" / "checkpoints.json").exists():
                encoded = self.checkpoints.load_latest(expected_journal_id=journal.journal_id)
                watermark = decode_checkpoint(encoded).watermark
                self.order_book.restore_recovery_checkpoint(
                    encoded, expected_journal_id=journal.journal_id
                )
            pending: list[MmapOmsFactRecordV1] = []
            last_lsn = 0
            boundary_found = watermark == 0
            for record in journal.startup_records():
                last_lsn = record.lsn
                pending.append(record)
                if record.ordinal != record.batch_size - 1:
                    continue
                if pending[-1].lsn == watermark:
                    boundary_found = True
                if pending[0].lsn <= watermark < pending[-1].lsn:
                    raise OmsOrderBookError("checkpoint watermark splits a committed batch")
                if pending[0].lsn > watermark:
                    self.order_book.restore_committed_batch(
                        tuple(row.fact for row in pending),
                        OmsJournalCommit(
                            journal.journal_id, pending[0].lsn, pending[-1].lsn, len(pending)
                        ),
                    )
                pending.clear()
            if pending or not boundary_found or watermark > last_lsn:
                raise OmsOrderBookError("checkpoint is not covered by the verified Journal")
            self.recovered_watermark = last_lsn
        except BaseException:
            if journal is not None:
                journal.close()
            self.identity.close()
            raise

    def checkpoint_recovered_state(self) -> Path:
        """Publish only a quiescent startup reducer; live instances are rejected."""
        if self._closed:
            raise OmsOrderBookError("persistent runtime is closed")
        return self.checkpoints.publish(self.order_book.export_recovery_checkpoint())

    def close(self) -> None:
        if not self._closed:
            try:
                self.journal.close()
            finally:
                self.identity.close()
                self._closed = True
