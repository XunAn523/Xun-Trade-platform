from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from xuanling_tradecore.runtime import (
    RuntimeFaultEvidenceStoreV1,
    RuntimeFaultGapPageFileInitializerV1,
    RuntimeFaultGapPageFileReaderV1,
    RuntimeFaultSupervisorV1,
)
from xuanling_tradecore.runtime.fault_monitor.gap_page import (
    RUNTIME_FAULT_GAP_PAGE_V1_BYTES,
    RuntimeFaultGapPageV1,
)


class RuntimeFaultSupervisorTests(unittest.TestCase):
    def test_runtime_startup_initializes_once_and_never_overwrites_active_gap(self) -> None:
        with TemporaryDirectory() as temporary:
            page_path = Path(temporary) / "fault-gap-page.v1"
            initializer = RuntimeFaultGapPageFileInitializerV1(page_path)
            inactive = RuntimeFaultGapPageV1(5, False, 0, 0, 0, 0, 10)
            self.assertTrue(initializer.initialize(inactive))
            active = RuntimeFaultGapPageV1(5, True, 7, 9, 3, 4, 11)
            page_path.write_bytes(active.encode())
            self.assertFalse(initializer.initialize(inactive))
            self.assertEqual(active, RuntimeFaultGapPageV1.decode(page_path.read_bytes()))

    def test_exports_each_gap_change_once_without_clearing_the_page(self) -> None:
        page = RuntimeFaultGapPageV1(1, True, 2, 4, 2, 8, 10).encode()
        evidence = []
        supervisor = RuntimeFaultSupervisorV1(
            read_page=lambda: page, persist_evidence=evidence.append, clock_ns=lambda: 20
        )
        first = supervisor.poll_once()
        self.assertIsNotNone(first)
        self.assertEqual(1, len(evidence))
        self.assertIsNone(supervisor.poll_once())
        self.assertEqual(20, supervisor.last_heartbeat_monotonic_ns)

    def test_bad_page_keeps_the_supervisor_heartbeat_available(self) -> None:
        heartbeats: list[int] = []
        supervisor = RuntimeFaultSupervisorV1(
            read_page=lambda: b"bad",
            persist_evidence=lambda _evidence: None,
            persist_heartbeat=heartbeats.append,
            clock_ns=lambda: 20,
        )
        with self.assertRaises(ValueError):
            supervisor.poll_once()
        self.assertEqual([20], heartbeats)

    def test_file_reader_and_evidence_store_keep_only_gap_metadata(self) -> None:
        page = RuntimeFaultGapPageV1(5, True, 7, 9, 3, 4, 10).encode()
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            page_path = root / "fault-gap-page.v1"
            page_path.write_bytes(page)
            evidence_store = RuntimeFaultEvidenceStoreV1(root / "evidence")
            supervisor = RuntimeFaultSupervisorV1(
                read_page=RuntimeFaultGapPageFileReaderV1(
                    page_path, page_bytes=RUNTIME_FAULT_GAP_PAGE_V1_BYTES
                ).read,
                persist_evidence=evidence_store.persist,
                persist_heartbeat=evidence_store.persist_heartbeat,
                clock_ns=lambda: 12,
            )
            self.assertIsNotNone(supervisor.poll_once())
            self.assertIn("dropped_count", (root / "evidence" / "gap-5-7-9.json").read_text())
            self.assertTrue((root / "evidence" / "fault-supervisor-heartbeat.v1.json").is_file())


if __name__ == "__main__":
    unittest.main()
