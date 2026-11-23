from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.journal import (
    FileJournal,
    GroupCommitJournal,
    JournalAppend,
    JournalFailure,
    crc32c,
)

_HASH_A = "a" * 64
_HASH_B = "b" * 64


class FileJournalTests(unittest.TestCase):
    def test_crc32c_uses_castagnoli_reference_value(self) -> None:
        self.assertEqual(0xE3069283, crc32c(b"123456789"))

    def test_append_is_recovered_with_identical_identity_and_monotonic_lsn(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "journal" / "account.emtj"
            with FileJournal(path) as journal:
                first = journal.append(
                    kind="COMMAND_PREPARED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
                second = journal.append(
                    kind="CALL_STARTED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_B,
                )
                self.assertEqual((first, second), journal.records())
                self.assertIsNotNone(journal.last_flush_ns)

            with FileJournal(path) as recovered:
                records = recovered.records()
                self.assertEqual([1, 2], [record.lsn for record in records])
                self.assertEqual("COMMAND_PREPARED", records[0].kind)
                self.assertEqual("command-1", records[1].command_id)
                self.assertEqual(_HASH_B, records[1].payload_hash)

    def test_open_truncates_an_incomplete_tail_and_keeps_verified_prefix(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            with FileJournal(path) as journal:
                journal.append(
                    kind="COMMAND_PREPARED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
            valid_size = path.stat().st_size
            with path.open("ab") as raw_file:
                raw_file.write(b"torn-frame")

            with FileJournal(path) as recovered:
                self.assertEqual(1, len(recovered.records()))
            self.assertEqual(valid_size, path.stat().st_size)

    def test_open_truncates_a_crc_corrupt_frame(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            with FileJournal(path) as journal:
                journal.append(
                    kind="COMMAND_PREPARED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
            with path.open("r+b") as raw_file:
                raw_file.seek(-1, 2)
                original = raw_file.read(1)
                self.assertEqual(1, len(original))
                raw_file.seek(-1, 2)
                raw_file.write(bytes((original[0] ^ 0xFF,)))

            with FileJournal(path) as recovered:
                self.assertEqual((), recovered.records())
            self.assertEqual(0, path.stat().st_size)

    def test_rejects_non_digest_and_oversized_identity_before_writing(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            with FileJournal(path, max_record_bytes=16) as journal:
                with self.assertRaises(JournalFailure):
                    journal.append(
                        kind="COMMAND_PREPARED",
                        command_id="command-1",
                        correlation_id="correlation-1",
                        payload_hash="not-a-digest",
                    )
                with self.assertRaises(JournalFailure):
                    journal.append(
                        kind="COMMAND_PREPARED",
                        command_id="command-1",
                        correlation_id="correlation-1",
                        payload_hash=_HASH_A,
                    )
                self.assertEqual((), journal.records())

    def test_batch_append_uses_monotonic_lsns_and_recovers_all_records(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            with FileJournal(path) as journal:
                records = journal.append_batch(
                    (
                        JournalAppend("CALL_STARTED", "command-1", "correlation-1", _HASH_A),
                        JournalAppend("NATIVE_ACCEPTED", "command-1", "correlation-1", _HASH_A),
                    )
                )
                self.assertEqual([1, 2], [record.lsn for record in records])
                timing = journal.last_append_timing
                self.assertIsNotNone(timing)
                assert timing is not None
                self.assertEqual(2, timing.entry_count)
                self.assertGreaterEqual(timing.total_ns, timing.write_ns + timing.flush_ns)
                self.assertGreaterEqual(timing.total_ns, timing.validation_and_encoding_ns)
            with FileJournal(path) as recovered:
                self.assertEqual(records, recovered.records())

    def test_timing_history_is_bounded_and_disabled_by_default(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            with FileJournal(path) as disabled:
                disabled.append(
                    kind="CALL_STARTED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
                self.assertEqual((), disabled.timing_history())
            with FileJournal(path, timing_history_capacity=1) as journal:
                journal.append(
                    kind="CALL_STARTED",
                    command_id="command-2",
                    correlation_id="correlation-2",
                    payload_hash=_HASH_A,
                )
                journal.append(
                    kind="NATIVE_ACCEPTED",
                    command_id="command-2",
                    correlation_id="correlation-2",
                    payload_hash=_HASH_A,
                )
                history = journal.timing_history()
                self.assertEqual(1, len(history))
                self.assertEqual(1, history[0].entry_count)

    def test_group_commit_returns_only_durable_records(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            base = FileJournal(path)
            journal = GroupCommitJournal(base, max_batch_size=8, max_wait_seconds=0.001)
            try:
                records = journal.append_batch(
                    (
                        JournalAppend("CALL_STARTED", "command-1", "correlation-1", _HASH_A),
                        JournalAppend("NATIVE_ACCEPTED", "command-1", "correlation-1", _HASH_A),
                    )
                )
                self.assertEqual([1, 2], [record.lsn for record in records])
            finally:
                journal.close()
                base.close()

    def test_group_commit_can_use_a_durable_idle_direct_path(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            base = FileJournal(path)
            journal = GroupCommitJournal(
                base,
                max_batch_size=8,
                max_wait_seconds=0.001,
                direct_when_idle=True,
            )
            try:
                record = journal.append(
                    kind="CALL_STARTED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
                self.assertEqual(1, record.lsn)
                self.assertEqual((record,), base.records())
            finally:
                journal.close()
                base.close()

    def test_group_commit_can_flush_ready_fifo_without_a_collection_wait(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            base = FileJournal(path)
            journal = GroupCommitJournal(base, max_batch_size=8, max_wait_seconds=0.0)
            try:
                record = journal.append(
                    kind="CALL_STARTED",
                    command_id="command-1",
                    correlation_id="correlation-1",
                    payload_hash=_HASH_A,
                )
                self.assertEqual(1, record.lsn)
                self.assertEqual((record,), base.records())
            finally:
                journal.close()
                base.close()

    def test_group_commit_prioritises_pre_native_evidence_without_dropping_outcome(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            base = FileJournal(path)
            journal = GroupCommitJournal(
                base,
                max_batch_size=8,
                max_wait_seconds=0.001,
                priority_kinds=("CALL_STARTED",),
            )
            try:
                records = journal.append_batch(
                    (
                        JournalAppend("NATIVE_ACCEPTED", "command-1", "correlation-1", _HASH_A),
                        JournalAppend("CALL_STARTED", "command-2", "correlation-2", _HASH_B),
                    )
                )
                self.assertEqual([2, 1], [record.lsn for record in records])
                self.assertEqual(
                    ["CALL_STARTED", "NATIVE_ACCEPTED"],
                    [record.kind for record in base.records()],
                )
            finally:
                journal.close()
                base.close()
