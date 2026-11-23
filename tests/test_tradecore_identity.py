from __future__ import annotations

import ast
import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

import xuanling_tradecore
from xuanling_tradecore.cli import main
from xuanling_tradecore.identity import (
    DEFAULT_RUNTIME_CONTROL_ROOT,
    LEGACY_COMPATIBILITY_PACKAGE,
    PROJECT_DISTRIBUTION,
    PROJECT_NAME,
    ProjectIdentityError,
    RuntimeControlRoot,
)
from xuanling_tradecore.runtime.fault_monitor import (
    RUNTIME_FAULT_GAP_PAGE_V1_BYTES,
    RuntimeFaultGapPageV1,
)

ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = ROOT / "xuanling_tradecore"


class TradeCoreIdentityTests(unittest.TestCase):
    def test_product_identity_and_control_root_are_vendor_neutral(self) -> None:
        self.assertEqual("Xuanling TradeCore", PROJECT_NAME)
        self.assertEqual("xuanling-tradecore", PROJECT_DISTRIBUTION)
        self.assertEqual("emt_gateway", LEGACY_COMPATIBILITY_PACKAGE)
        self.assertEqual(
            PurePosixPath("/var/lib/xuanling-tradecore/control"), DEFAULT_RUNTIME_CONTROL_ROOT
        )
        self.assertEqual("0.1.0", xuanling_tradecore.__version__)

    def test_control_root_addressing_is_pure_and_scope_bound(self) -> None:
        control_root = RuntimeControlRoot.default()
        scope_hash = "a" * 64
        self.assertEqual(
            PurePosixPath("/var/lib/xuanling-tradecore/control/accounts") / scope_hash,
            control_root.account_control_root(scope_hash),
        )
        with self.assertRaises(ProjectIdentityError):
            RuntimeControlRoot(PurePosixPath("relative/control"))
        with self.assertRaises(ProjectIdentityError):
            RuntimeControlRoot(PurePosixPath("/var/lib/xuanling-tradecore/not-control"))
        with self.assertRaises(ProjectIdentityError):
            control_root.account_control_root("A" * 64)

    def test_identity_cli_is_read_only_and_reports_the_product_version(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(0, main(["--version"]))
        self.assertEqual("Xuanling TradeCore 0.1.0\n", output.getvalue())

    def test_cli_runs_only_an_explicitly_disarmed_runtime_once(self) -> None:
        self.assertEqual(0, main(["runtime", "--disarmed", "--once"]))

    def test_cli_runs_the_non_trading_fault_supervisor_once(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            page = root / "gap-page"
            page.write_bytes(RuntimeFaultGapPageV1(1, False, 0, 0, 0, 0, 1).encode())
            self.assertEqual(
                0,
                main(
                    [
                        "fault-supervisor",
                        "--gap-page",
                        str(page),
                        "--evidence-root",
                        str(root / "evidence"),
                        "--once",
                    ]
                ),
            )
            self.assertEqual(RUNTIME_FAULT_GAP_PAGE_V1_BYTES, page.stat().st_size)
            self.assertTrue((root / "evidence" / "fault-supervisor-heartbeat.v1.json").is_file())

    def test_new_core_modules_do_not_import_vendor_or_legacy_packages(self) -> None:
        forbidden_roots = {"emt_gateway", "vnemttrader", "qmt"}
        for source in CORE_ROOT.rglob("*.py"):
            relative_parts = source.relative_to(CORE_ROOT).parts
            if "compatibility" in relative_parts:
                continue
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                imported_names: tuple[str, ...]
                if isinstance(node, ast.Import):
                    imported_names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported_names = (node.module,)
                else:
                    continue
                for imported_name in imported_names:
                    root_name = imported_name.split(".", maxsplit=1)[0]
                    self.assertNotIn(
                        root_name,
                        forbidden_roots,
                        f"{source.relative_to(ROOT)} imports forbidden root {root_name}",
                    )
