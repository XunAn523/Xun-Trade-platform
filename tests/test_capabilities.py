from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_ROOT = ROOT / "tools" / "performance"
if str(PERFORMANCE_ROOT) not in sys.path:
    sys.path.insert(0, str(PERFORMANCE_ROOT))

from emt_gateway.capabilities import (
    OFFICIAL_TRADER_API,
    CapabilityStatus,
    inspect_trader_api,
    python_api_name,
    require_direct_capabilities,
)
from perfbench.catalog import TRADER_API


class CapabilityTests(unittest.TestCase):
    def test_runtime_catalog_is_the_official_86_method_catalog(self) -> None:
        self.assertEqual(86, len(OFFICIAL_TRADER_API))
        self.assertEqual(TRADER_API, OFFICIAL_TRADER_API)

    def test_complete_direct_binding_has_85_direct_methods_and_bridge_adapter(self) -> None:
        binding = type("Binding", (), {})
        for official_name in OFFICIAL_TRADER_API:
            if official_name != "RegisterSpi":
                setattr(binding, python_api_name(official_name), lambda: None)

        report = inspect_trader_api(binding)

        self.assertTrue(report.complete)
        self.assertEqual(85, report.direct_count)
        self.assertEqual(1, report.adapter_count)
        self.assertEqual(0, report.unsupported_count)
        self.assertEqual(CapabilityStatus.ADAPTER, report.entry_for("RegisterSpi").status)
        self.assertEqual("direct", report.as_dict()["entries"][0]["status"])  # type: ignore[index]  # serialized report is heterogeneous

    def test_missing_or_non_callable_methods_are_explicitly_unsupported(self) -> None:
        class PartialBinding:
            insertOrder = object()

            @staticmethod
            def cancelOrder() -> None:
                return None

        report = inspect_trader_api(PartialBinding)

        self.assertEqual(CapabilityStatus.UNSUPPORTED, report.entry_for("InsertOrder").status)
        self.assertEqual(CapabilityStatus.DIRECT, report.entry_for("CancelOrder").status)
        self.assertEqual(84, report.unsupported_count)
        with self.assertRaisesRegex(RuntimeError, "InsertOrder"):
            require_direct_capabilities(report, ["InsertOrder", "CancelOrder"])

    def test_unknown_official_name_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            python_api_name("OldVersionMethod")
