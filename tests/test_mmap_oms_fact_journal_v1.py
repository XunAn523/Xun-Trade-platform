from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.oms import (
    MmapOmsFactJournalError,
    MmapOmsFactJournalRecoveryRequired,
    MmapOmsFactJournalV1,
    OmsFactKind,
    OmsJournalCommit,
    OmsJournalFact,
)


def _fact(identifier: str) -> OmsJournalFact:
    return OmsJournalFact(
        fact_id=identifier,
        kind=OmsFactKind.ORDER_ATTRIBUTED,
        account_id="consumer-uds-benchmark",
        oms_order_id="order-1",
        execution_id="execution-1",
        attributes={"source": "test", "ordinal": identifier},
    )


class MmapOmsFactJournalV1Tests(unittest.TestCase):
    def test_committed_batch_reader_rejects_wrong_or_partial_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = MmapOmsFactJournalV1(
                path=Path(directory) / "facts.mmap", journal_id="journal-1", capacity=8
            )
            commit = journal.append_batch((_fact("fact-1"), _fact("fact-2")))
            records = journal.read_committed_batch(commit)
            self.assertEqual(("fact-1", "fact-2"), tuple(row.fact.fact_id for row in records))
            with self.assertRaisesRegex(MmapOmsFactJournalError, "outside"):
                journal.read_committed_batch(OmsJournalCommit("foreign", 1, 2, 2))
            with self.assertRaisesRegex(MmapOmsFactJournalError, "complete batch"):
                journal.read_committed_batch(OmsJournalCommit("journal-1", 1, 1, 1))
            self.assertFalse(journal.recovery_required)
            journal.close()

    def test_committed_batch_corruption_closes_future_admission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "facts.mmap"
            journal = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=8)
            commit = journal.append_batch((_fact("fact-1"), _fact("fact-2")))
            with path.open("r+b", buffering=0) as handle:
                handle.seek(4_096 + 4_096 * 2 - 8)
                handle.write(b"\0" * 8)
            with self.assertRaises(MmapOmsFactJournalRecoveryRequired):
                journal.read_committed_batch(commit)
            with self.assertRaises(MmapOmsFactJournalRecoveryRequired):
                journal.append_batch((_fact("fact-3"),))
            journal.close()

    def test_commits_full_facts_and_reopens_a_clean_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "facts.mmap"
            journal = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=8)
            commit = journal.append_batch((_fact("fact-1"), _fact("fact-2")))
            self.assertEqual((1, 2, 2), (commit.first_lsn, commit.final_lsn, commit.fact_count))
            scan = journal.scan()
            self.assertTrue(scan.clean)
            self.assertEqual(
                ("fact-1", "fact-2"), tuple(item.fact.fact_id for item in scan.records)
            )
            journal.close()

            reopened = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=8)
            follow_up = reopened.append_batch((_fact("fact-3"),))
            self.assertEqual((3, 3), (follow_up.first_lsn, follow_up.final_lsn))
            self.assertTrue(reopened.scan().clean)
            reopened.close()

    def test_utf8_fact_and_sparse_frame_padding_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "facts.mmap"
            fact = OmsJournalFact(
                fact_id="fact-unicode",
                kind=OmsFactKind.ORDER_ATTRIBUTED,
                account_id="账户-A",
                oms_order_id="order-1",
                execution_id="execution-1",
                attributes={"说明": "仿真委托"},
            )
            journal = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=2)
            journal.append_batch((fact,))
            scan = journal.scan()
            self.assertTrue(scan.clean)
            self.assertEqual(fact, scan.records[0].fact)
            journal.close()

            frame = path.read_bytes()[4_096 : 4_096 + 4_096]
            self.assertGreater(frame.count(b"\0"), 3_000)
            reopened = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=2)
            self.assertTrue(reopened.scan().clean)
            reopened.close()

    def test_torn_last_commit_word_closes_admission_after_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "facts.mmap"
            journal = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=8)
            journal.append_batch((_fact("fact-1"), _fact("fact-2")))
            journal.close()
            with path.open("r+b") as handle:
                handle.seek(4_096 + (4_096 * 2) - 8)
                handle.write(b"\0" * 8)
            reopened = MmapOmsFactJournalV1(path=path, journal_id="journal-1", capacity=8)
            self.assertFalse(reopened.scan().clean)
            with self.assertRaises(MmapOmsFactJournalRecoveryRequired):
                reopened.append_batch((_fact("fact-3"),))
            reopened.close()


if __name__ == "__main__":
    unittest.main()
