"""SSD archive must remain outside the Redis/native proof boundary."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.journal import FileJournal
from emt_gateway.journal_archive import AsyncFileJournalArchive


class AsyncFileJournalArchiveTests(unittest.TestCase):
    def test_close_flushes_queued_identity_records_without_hot_path_waits(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "archive.emtj"
            archive = AsyncFileJournalArchive(path, capacity=4, maximum_batch_size=2)
            self.assertTrue(
                archive.offer(
                    kind="ORDER_PRE_NATIVE",
                    command_id="command-1",
                    correlation_id="execution-1",
                    payload_hash="a" * 64,
                )
            )
            archive.close()
            self.assertEqual(1, archive.snapshot().persisted)
            with FileJournal(path) as restored:
                self.assertEqual("ORDER_PRE_NATIVE", restored.records()[0].kind)

    def test_closed_archive_rejects_only_the_replica_offer(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            archive = AsyncFileJournalArchive(Path(temporary_directory) / "archive.emtj")
            archive.close()
            self.assertFalse(
                archive.offer(
                    kind="ORDER_NATIVE_ACCEPTED",
                    command_id="command-1",
                    correlation_id="execution-1",
                    payload_hash="b" * 64,
                )
            )
            self.assertEqual(1, archive.snapshot().rejected)


if __name__ == "__main__":
    unittest.main()
