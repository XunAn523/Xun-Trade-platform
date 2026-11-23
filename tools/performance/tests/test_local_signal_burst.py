from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.local_signal_burst import (
    LocalSignalBurstError,
    LocalSignalBurstSettings,
    run_local_signal_burst,
)


class LocalSignalBurstTests(unittest.TestCase):
    def test_account_free_group_commit_round_collects_complete_local_paths(self) -> None:
        with TemporaryDirectory() as directory:
            report = run_local_signal_burst(
                LocalSignalBurstSettings(
                    runtime_dir=Path(directory),
                    signals_per_second=4,
                    rounds=1,
                    warmup_rounds=1,
                    journal_mode="group-commit",
                    group_commit_max_wait_ms=1.0,
                    group_commit_max_batch_size=4,
                    callback_batch_size=4,
                    callback_collect_wait_ms=0.1,
                    arrival_pattern="burst",
                )
            )
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertFalse(report["safety"]["order_entry"])
        self.assertEqual(
            "local_stub_reference", report["report_contract"]["evidence_classification"]
        )
        self.assertFalse(
            report["report_contract"]["execution_boundaries"]["native_fastspi_required"]["value"]
        )
        self.assertIsInstance(
            report["report_contract"]["execution_boundaries"]["native_fastspi_available"]["value"],
            bool,
        )
        self.assertEqual(4, report["run"]["accepted_signals"])
        self.assertGreater(
            report["run"]["total_elapsed_ns"], report["run"]["measurement_elapsed_ns"]
        )
        self.assertGreater(report["run"]["effective_signals_per_second"], 3.0)
        self.assertEqual(4, report["latency"]["C0_to_C4_signal_accept"]["count"])
        self.assertEqual(4, report["latency"]["C5_to_C7_callback_complete"]["count"])
        self.assertEqual(
            4,
            report["one_signal_per_second_local_gate"]["measurements"]["C5_to_C6_callback_durable"][
                "count"
            ],
        )
        self.assertFalse(report["run"]["callback_disarmed"])

    def test_settings_reject_above_requested_300_signal_rate(self) -> None:
        with self.assertRaisesRegex(LocalSignalBurstError, "signals_per_second"):
            LocalSignalBurstSettings(runtime_dir=Path("runtime"), signals_per_second=301).validate()

    def test_uniform_arrival_uses_one_second_rate_window(self) -> None:
        with TemporaryDirectory() as directory:
            report = run_local_signal_burst(
                LocalSignalBurstSettings(
                    runtime_dir=Path(directory),
                    signals_per_second=2,
                    rounds=1,
                    journal_mode="group-commit",
                    group_commit_max_wait_ms=1.0,
                    group_commit_max_batch_size=2,
                    callback_batch_size=2,
                    callback_collect_wait_ms=0.1,
                    arrival_pattern="uniform",
                )
            )
        self.assertEqual("uniform", report["run"]["arrival_pattern"])
        self.assertIsNone(report["run"]["concurrent_signals_per_round"])
        self.assertEqual(2, report["run"]["accepted_signals"])
