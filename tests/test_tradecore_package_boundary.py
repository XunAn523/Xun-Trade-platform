from __future__ import annotations

import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tools.quality.check_tradecore_boundary import (
    POLICY_PATH,
    ImportPolicy,
    scan_imports,
    validate_metadata,
    validate_repository,
)
from xuanling_tradecore.cli import main as tradecore_main

ROOT = Path(__file__).resolve().parents[1]


class TradeCorePackageBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = ImportPolicy.load(POLICY_PATH)

    def test_repository_satisfies_versioned_package_boundary(self) -> None:
        self.assertEqual([], validate_repository(ROOT))

    def test_legacy_import_is_rejected_outside_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "xuanling_tradecore" / "oms" / "bad.py"
            source.parent.mkdir(parents=True)
            source.write_text("from emt_gateway.gateway import OrderGateway\n", encoding="utf-8")

            violations = scan_imports(root, self.policy)

        self.assertEqual(["TCB101"], [violation.code for violation in violations])

    def test_legacy_import_is_allowed_only_in_compatibility_package(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "xuanling_tradecore" / "compatibility" / "bridge.py"
            source.parent.mkdir(parents=True)
            source.write_text("from emt_gateway.gateway import OrderGateway\n", encoding="utf-8")

            violations = scan_imports(root, self.policy)

        self.assertEqual([], violations)

    def test_generic_layer_cannot_import_vendor_or_compatibility_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "xuanling_tradecore" / "domain" / "bad.py"
            source.parent.mkdir(parents=True)
            source.write_text(
                "import vnemttrader\nfrom ..compatibility import LegacyOrderGatewayBridge\n",
                encoding="utf-8",
            )

            violations = scan_imports(root, self.policy)

        self.assertEqual(["TCB102", "TCB103"], sorted(item.code for item in violations))

    def test_adapter_layer_may_import_vendor_sdk_but_not_legacy_gateway(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "xuanling_tradecore" / "adapters" / "emt" / "adapter.py"
            source.parent.mkdir(parents=True)
            source.write_text(
                "import vnemttrader\nfrom emt_gateway.gateway import OrderGateway\n",
                encoding="utf-8",
            )

            violations = scan_imports(root, self.policy)

        self.assertEqual(["TCB101"], [violation.code for violation in violations])

    def test_distribution_or_cli_identity_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            package = root / "xuanling_tradecore"
            package.mkdir()
            (package / "__init__.py").write_text('__version__ = "0.1.0"\n', encoding="utf-8")
            (package / "cli.py").write_text("def main():\n    return 0\n", encoding="utf-8")
            hybrid_package = root / "xuntrade_omsems"
            hybrid_package.mkdir()
            (hybrid_package / "__init__.py").write_text('__version__ = "0.1.0"\n', encoding="utf-8")
            (hybrid_package / "cli.py").write_text("def main():\n    return 0\n", encoding="utf-8")
            (root / "pyproject.toml").write_text(
                """[project]
name = "wrong-name"
version = "0.1.0"

[project.scripts]
xuanling-tradecore = "wrong.module:main"

[tool.setuptools.packages.find]
include = ["xuanling_tradecore*", "xuntrade_omsems*"]
""",
                encoding="utf-8",
            )

            violations = validate_metadata(root, self.policy)

        self.assertEqual(
            ["TCB202", "TCB203", "TCB204"],
            sorted(item.code for item in violations),
        )

    def test_emt_and_qmt_cannot_import_each_other(self) -> None:
        for source_venue, target_venue in (("emt", "qmt"), ("qmt", "emt")):
            for statement in (
                f"from ..{target_venue}.adapter import Adapter\n",
                f"from .. import {target_venue}\n",
                f"import xuanling_tradecore.adapters.{target_venue}\n",
            ):
                with self.subTest(source=source_venue, statement=statement):
                    with tempfile.TemporaryDirectory() as temporary_directory:
                        root = Path(temporary_directory)
                        source = root / "xuanling_tradecore" / "adapters" / source_venue / "bad.py"
                        source.parent.mkdir(parents=True)
                        source.write_text(statement, encoding="utf-8")
                        violations = scan_imports(root, self.policy)
                    self.assertEqual(["TCB105"], [item.code for item in violations])

    def test_generic_core_cannot_bypass_adapter_contract(self) -> None:
        for layer in ("ems", "oms", "consumer", "adapters"):
            with self.subTest(layer=layer):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    root = Path(temporary_directory)
                    source = root / "xuanling_tradecore" / layer / "bad.py"
                    source.parent.mkdir(parents=True)
                    source.write_text(
                        "from xuanling_tradecore.adapters.qmt import QmtBrokerAdapter\n",
                        encoding="utf-8",
                    )
                    violations = scan_imports(root, self.policy)
                self.assertEqual(["TCB104"], [item.code for item in violations])

    def test_identity_cli_has_no_filesystem_or_legacy_import_side_effect(self) -> None:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary_directory:
            previous_directory = Path.cwd()
            before_modules = set(sys.modules)
            try:
                os.chdir(temporary_directory)
                with redirect_stdout(output):
                    self.assertEqual(0, tradecore_main(["--version"]))
                self.assertEqual([], list(Path(temporary_directory).iterdir()))
            finally:
                os.chdir(previous_directory)
            newly_loaded = set(sys.modules) - before_modules

        self.assertEqual("Xuanling TradeCore 0.1.0\n", output.getvalue())
        self.assertFalse(
            any(name == "emt_gateway" or name.startswith("emt_gateway.") for name in newly_loaded)
        )


if __name__ == "__main__":
    unittest.main()
