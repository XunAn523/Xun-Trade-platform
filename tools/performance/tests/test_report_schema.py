from __future__ import annotations

import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from perfbench.cli import main
from perfbench.report_schema import (
    PerformanceReportSchemaError,
    finalize_performance_report,
    validate_performance_report,
)


class PerformanceReportSchemaTests(unittest.TestCase):
    def test_cli_persists_the_common_contract_for_a_synthetic_artifact(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory) / "synthetic.json"
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "emt-gateway-perf",
                        "--driver",
                        "synthetic",
                        "--iterations",
                        "1",
                        "--output",
                        str(output),
                    ],
                ),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(0, main())
            report = json.loads(output.read_text(encoding="utf-8"))

        contract = report["report_contract"]
        self.assertEqual("emt-gateway-performance-report", contract["name"])
        self.assertEqual("synthetic", contract["requested_driver"])
        self.assertFalse(contract["execution_boundaries"]["sdk_loaded"]["value"])

    def test_read_only_execution_has_explicit_core_boundaries_and_missing_fastspi_evidence(
        self,
    ) -> None:
        report = finalize_performance_report(
            {"driver": "native-readonly", "safety": {"order_entry": False}},
            requested_driver="native-readonly",
        )

        contract = report["report_contract"]
        self.assertEqual("incomplete", contract["boundary_completeness"])
        self.assertEqual("emt_execution_observation", contract["evidence_classification"])
        self.assertTrue(contract["execution_boundaries"]["sdk_loaded"]["value"])
        self.assertTrue(contract["execution_boundaries"]["login"]["value"])
        self.assertTrue(contract["execution_boundaries"]["network"]["value"])
        self.assertFalse(contract["execution_boundaries"]["real_order_entry"]["value"])
        self.assertFalse(contract["execution_boundaries"]["native_fastspi_available"]["observed"])

    def test_preflight_cannot_claim_sdk_login_network_or_order_entry(self) -> None:
        report = finalize_performance_report(
            {"driver": "gateway-mutation-order-cancel-c0-c7", "executed": False, "safety": {}},
            requested_driver="gateway-mutation",
        )

        boundaries = report["report_contract"]["execution_boundaries"]
        self.assertFalse(boundaries["sdk_loaded"]["value"])
        self.assertFalse(boundaries["login"]["value"])
        self.assertFalse(boundaries["network"]["value"])
        self.assertFalse(boundaries["real_order_entry"]["value"])
        self.assertIsNone(boundaries["native_fastspi_available"]["value"])

    def test_external_fixture_execution_is_explicitly_incomplete_not_inferred(self) -> None:
        report = finalize_performance_report(
            {"driver": "gateway-fixture", "executed": True},
            requested_driver="gateway-fixture",
        )

        contract = report["report_contract"]
        self.assertEqual("incomplete", contract["boundary_completeness"])
        self.assertEqual("external_or_uncontrolled_execution", contract["evidence_classification"])
        self.assertFalse(contract["execution_boundaries"]["sdk_loaded"]["observed"])
        self.assertIsNone(contract["execution_boundaries"]["sdk_loaded"]["value"])

    def test_validation_rejects_boundary_completeness_tampering(self) -> None:
        report = finalize_performance_report(
            {"driver": "synthetic", "safety": {}},
            requested_driver="synthetic",
        )
        report["report_contract"]["boundary_completeness"] = "complete"

        with self.assertRaisesRegex(PerformanceReportSchemaError, "completeness"):
            validate_performance_report(report)
