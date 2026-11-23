"""H0 persistent-memory Go/No-Go contract tests."""

from __future__ import annotations

import ast
import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from xuanling_tradecore.persistence import (
    H0DaxDevice,
    H0Evidence,
    H0EvidenceError,
    H0HostSnapshot,
    collect_h0_host_snapshot,
    evaluate_h0,
    load_h0_evidence,
)

ROOT = Path(__file__).resolve().parents[1]


def _snapshot(
    *, system: str = "Linux", os_release: str = "6.8.0", pmem2: bool = True
) -> H0HostSnapshot:
    return H0HostSnapshot(
        system=system,
        hostname="trade-host-1",
        os_release=os_release,
        dax_devices=(
            H0DaxDevice(
                sysfs_name="dax0.0",
                device_path="/dev/dax0.0",
                size_bytes=2 * 1024 * 1024 * 1024,
                is_character_device=True,
            ),
        ),
        pmem2_library_path="libpmem2.so.1" if pmem2 else None,
        pmem2_symbols=(
            frozenset(
                {
                    "pmem2_map_new",
                    "pmem2_get_persist_fn",
                    "pmem2_get_flush_fn",
                    "pmem2_get_drain_fn",
                }
            )
            if pmem2
            else frozenset()
        ),
    )


def _evidence(**overrides: object) -> H0Evidence:
    values: dict[str, object] = {
        "schema_version": 1,
        "host_id": "trade-host-1",
        "dax_device": "/dev/dax0.0",
        "dax_sysfs_name": "dax0.0",
        "namespace_uuid": "namespace-uuid-1",
        "persistence_domain": "EADR",
        "mapping_length_bytes": 1024 * 1024 * 1024,
        "physical_media_verified": True,
        "recovery_drill_id": "drill-2026-08-31-1",
        "recovery_drill_completed": True,
        "verified_at_utc": "2026-08-31T00:00:00Z",
    }
    values.update(overrides)
    return H0Evidence(**values)  # type: ignore[reportArgumentType] # Dynamic fixture overrides exercise runtime validation.


class H0PmemPreflightTests(unittest.TestCase):
    def test_only_a_native_linux_dax_pmem2_and_recovery_drill_can_be_h0_eligible(self) -> None:
        report = evaluate_h0(_snapshot(), _evidence())
        self.assertTrue(report.eligible)
        self.assertTrue(all(check.passed for check in report.checks))

    def test_wsl_missing_pmem2_or_incomplete_evidence_fail_closed(self) -> None:
        wsl = evaluate_h0(_snapshot(os_release="6.6.0-microsoft-standard-WSL2"), _evidence())
        self.assertFalse(wsl.eligible)
        self.assertIn(
            "NO_WSL_OR_VIRTUAL_DAX", {check.code for check in wsl.checks if not check.passed}
        )
        virtualized = evaluate_h0(replace(_snapshot(), hypervisor_present=True), _evidence())
        self.assertFalse(virtualized.eligible)
        self.assertIn(
            "NO_WSL_OR_VIRTUAL_DAX",
            {check.code for check in virtualized.checks if not check.passed},
        )
        no_pmem = evaluate_h0(_snapshot(pmem2=False), _evidence())
        self.assertFalse(no_pmem.eligible)
        self.assertIn(
            "LIBPMEM2_FLUSH_DRAIN", {check.code for check in no_pmem.checks if not check.passed}
        )
        no_drill = evaluate_h0(_snapshot(), _evidence(recovery_drill_completed=False))
        self.assertFalse(no_drill.eligible)
        self.assertIn(
            "RECOVERY_DRILL", {check.code for check in no_drill.checks if not check.passed}
        )

    def test_missing_or_mismatched_hardware_evidence_never_gets_a_go_result(self) -> None:
        without_evidence = evaluate_h0(_snapshot(), None)
        self.assertFalse(without_evidence.eligible)
        wrong_host = evaluate_h0(_snapshot(), _evidence(host_id="another-host"))
        self.assertFalse(wrong_host.eligible)
        bad_device = evaluate_h0(_snapshot(), _evidence(dax_device="/dev/dax9.9"))
        self.assertFalse(bad_device.eligible)

    def test_evidence_loader_rejects_unknown_or_missing_fields(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "h0-evidence.json"
            payload = _evidence().__dict__.copy()
            payload["unapproved"] = True
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(H0EvidenceError, "fields"):
                load_h0_evidence(path)

    def test_current_host_without_evidence_is_always_no_go(self) -> None:
        self.assertFalse(evaluate_h0(collect_h0_host_snapshot(), None).eligible)

    def test_probe_does_not_import_journal_sdk_database_or_network_layers(self) -> None:
        forbidden = {"emt_gateway", "vnemttrader", "psycopg", "sqlite3", "redis", "requests"}
        for source in (
            ROOT / "xuanling_tradecore" / "persistence" / "h0.py",
            ROOT / "tools" / "h0_pmem_preflight.py",
        ):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    names = (node.module,)
                else:
                    continue
                self.assertFalse(
                    {name.split(".", 1)[0] for name in names} & forbidden,
                    f"{source.relative_to(ROOT)} imports a forbidden runtime layer",
                )


if __name__ == "__main__":
    unittest.main()
