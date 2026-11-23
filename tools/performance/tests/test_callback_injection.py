from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.callback_injection import (
    CallbackInjectionError,
    CallbackInjectionSettings,
    run_callback_injection,
)


class CallbackInjectionTests(unittest.TestCase):
    def test_account_free_injection_is_complete_and_ineligible_for_counter_baselines(self) -> None:
        with TemporaryDirectory() as directory:
            trace_path = Path(directory) / "callback.injected.traces.jsonl"
            report = run_callback_injection(
                CallbackInjectionSettings(
                    iterations=2,
                    burst_width=2,
                    slow_consumer_delay_seconds=0.001,
                    trace_output=trace_path,
                    fixture_owner="fengyu",
                    runtime_dir=Path(directory) / "runtime",
                    require_native_fastspi=False,
                )
            )
            trace = trace_path.read_text(encoding="utf-8")
            first_event = json.loads(trace.splitlines()[0])
        self.assertTrue(
            all(item["pipeline_complete"] for item in report["callback_pipeline_evidence"])
        )
        self.assertTrue(
            all(item["dropped_callbacks"] == 0 for item in report["callback_pipeline_evidence"])
        )
        self.assertFalse(report["evidence_classification"]["counter_performance_eligible"])
        self.assertFalse(report["evidence_classification"]["release_gate_eligible"])
        self.assertTrue(report["evidence_classification"]["must_not_merge_with_emt_trace_evidence"])
        self.assertEqual("fengyu", report["fixture"]["owner"])
        self.assertEqual("callback.injected.traces.jsonl", report["run"]["trace_artifact"])
        self.assertEqual("file", report["run"]["journal_mode"])
        self.assertTrue(report["run"]["journal_artifact"].endswith(".emtj"))
        self.assertGreater(report["resource_metrics"]["journal_durable_lag_ns"]["count"], 0)
        self.assertNotIn("account", trace.lower())
        self.assertNotIn("password", trace.lower())
        self.assertEqual("artifact_classification", first_event["event_type"])
        self.assertEqual("controlled_gateway_injection", first_event["artifact_type"])
        self.assertTrue(
            all(item["not_a_counter_or_release_result"] for item in report["matrix_projection"])
        )

    def test_settings_are_bounded(self) -> None:
        for settings, message in (
            (CallbackInjectionSettings(iterations=0), "iterations"),
            (CallbackInjectionSettings(burst_width=1), "burst_width"),
            (
                CallbackInjectionSettings(slow_consumer_delay_seconds=0.0),
                "slow_consumer_delay_seconds",
            ),
            (CallbackInjectionSettings(journal_mode="file"), "runtime_dir"),
            (CallbackInjectionSettings(journal_mode="unknown"), "journal_mode"),
        ):
            with self.subTest(settings=settings):
                with self.assertRaisesRegex(CallbackInjectionError, message):
                    settings.validate()
