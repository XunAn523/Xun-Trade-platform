from __future__ import annotations

import mmap
import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.persistence.mmap_pre_native_journal import (
    MmapCallbackObservedAppend,
    MmapJournalRecordKind,
    MmapPreNativeJournal,
    MmapPreNativeJournalError,
    scan_mmap_pre_native_journal_path,
)


class MmapPreNativeJournalTests(unittest.TestCase):
    def test_read_only_path_scan_validates_without_constructing_a_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "pre-native.journal"
            journal = MmapPreNativeJournal(path=path, capacity=2)
            journal.append_pre_native(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash="a" * 64,
                client_order_ref="17",
            )
            journal.close()

            scan = scan_mmap_pre_native_journal_path(path)

            self.assertTrue(scan.clean)
            self.assertEqual(1, scan.trusted_position)
            self.assertEqual("17", scan.records[0].client_order_ref)

    def test_reopen_recovers_only_complete_committed_frames(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "pre-native.journal"
            journal = MmapPreNativeJournal(path=path, capacity=4)
            started = journal.append_pre_native(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash="a" * 64,
                client_order_ref="17",
            )
            accepted = journal.append_native_accepted(
                monotonic_ns=11,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash="a" * 64,
                client_order_ref="17",
                native_order_id="91",
            )
            self.assertEqual(
                (started, accepted),
                journal.records(),
            )
            journal.close()

            reopened = MmapPreNativeJournal(path=path, capacity=4)
            self.assertEqual(
                [
                    MmapJournalRecordKind.ORDER_PRE_NATIVE,
                    MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
                ],
                [record.kind for record in reopened.records()],
            )
            reopened.close()

    def test_uncommitted_tail_is_not_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "pre-native.journal"
            journal = MmapPreNativeJournal(path=path, capacity=2)
            journal.append_pre_native(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash="a" * 64,
                client_order_ref="17",
            )
            journal.close()
            with path.open("r+b", buffering=0) as file:
                mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                mapped[4_096 + 512 - 8 : 4_096 + 512] = b"\0" * 8
                mapped.close()
            reopened = MmapPreNativeJournal(path=path, capacity=2)
            self.assertEqual((), reopened.records())
            self.assertTrue(reopened.recovery_required)
            with self.assertRaisesRegex(MmapPreNativeJournalError, "requires reconciliation"):
                reopened.append_pre_native(
                    monotonic_ns=11,
                    account_id="account-1",
                    execution_id="execution-2",
                    command_id="command-2",
                    payload_hash="a" * 64,
                    client_order_ref="18",
                )
            reopened.close()

    def test_rejects_another_writer_thread(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = MmapPreNativeJournal(path=Path(temporary) / "pre-native.journal", capacity=2)
            journal.append_pre_native(
                monotonic_ns=9,
                account_id="account-1",
                execution_id="execution-0",
                command_id="command-0",
                payload_hash="a" * 64,
                client_order_ref="16",
            )
            failures: list[BaseException] = []

            def write_from_another_thread() -> None:
                try:
                    journal.append_pre_native(
                        monotonic_ns=10,
                        account_id="account-1",
                        execution_id="execution-1",
                        command_id="command-1",
                        payload_hash="a" * 64,
                        client_order_ref="17",
                    )
                except BaseException as error:
                    failures.append(error)

            from threading import Thread

            thread = Thread(target=write_from_another_thread)
            thread.start()
            thread.join()
            self.assertEqual(1, len(failures))
            self.assertIsInstance(failures[0], MmapPreNativeJournalError)
            journal.close()

    def test_measurement_preparation_pretouches_and_preencodes_known_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = MmapPreNativeJournal(path=Path(temporary) / "pre-native.journal", capacity=4)
            preparation = journal.prepare_for_measurement(
                account_id="account-1",
                execution_ids=("execution-1",),
                command_ids=("command-1",),
                payload_hashes=("a" * 64,),
                client_order_refs=("17",),
            )
            self.assertGreater(preparation.page_count, 0)
            self.assertGreater(preparation.page_size, 0)
            self.assertTrue(preparation.static_account_cached)
            self.assertGreaterEqual(preparation.preloaded_fixed_field_count, 5)
            record = journal.append_pre_native(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash="a" * 64,
                client_order_ref="17",
            )
            self.assertEqual("execution-1", record.execution_id)
            journal.close()

    def test_callback_batch_commits_a_contiguous_prefix_with_per_frame_hooks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = MmapPreNativeJournal(path=Path(temporary) / "callback.journal", capacity=4)
            before: list[int] = []
            after: list[int] = []
            records = journal.append_callback_observed_batch(
                (
                    MmapCallbackObservedAppend(
                        monotonic_ns=10,
                        account_id="account-1",
                        execution_id="execution-1",
                        transition_id="transition-1",
                        payload_hash="a" * 64,
                        client_order_ref="client-1",
                        native_order_id="91",
                    ),
                    MmapCallbackObservedAppend(
                        monotonic_ns=11,
                        account_id="account-1",
                        execution_id="execution-2",
                        transition_id="transition-2",
                        payload_hash="b" * 64,
                        client_order_ref="client-2",
                        native_order_id="92",
                    ),
                ),
                on_before_commit=lambda record: before.append(record.sequence),
                on_committed=lambda record: after.append(record.sequence),
            )
            self.assertEqual([1, 2], [record.sequence for record in records])
            self.assertEqual([1, 2], before)
            self.assertEqual([1, 2], after)
            self.assertEqual(records, journal.records())
            journal.close()


if __name__ == "__main__":
    unittest.main()
