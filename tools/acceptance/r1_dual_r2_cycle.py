"""Produce bounded R1-A3 evidence while a separate system B stays live.

The coordinator does not import EMT or read accounts.  It holds a complete B
cycle in its own signed scope, then runs A's physical R2 crash, DISARMED
rehydration and approved exact-target cleanup.  Only aggregate evidence is
written; this cannot claim full R1, automatic recovery, or READY.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


class R1DualR2CycleError(RuntimeError):
    """The dual-system R2 drill cannot prove its bounded condition."""


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    expected_library_hashes: Path
    accounts_file: Path
    fixture: Path
    output: Path
    runtime_dir: Path
    trading_day: str
    system_a_client_id: int
    system_b_client_id: int
    callback_timeout_seconds: float
    system_b_hold_seconds: float
    system_a_namespace_release: Path
    system_a_namespace_trust: Path
    system_a_namespace_state: Path
    system_b_namespace_release: Path
    system_b_namespace_trust: Path
    system_b_namespace_state: Path
    runtime_release_hash: str
    system_a_bootstrap_approval_id: str
    system_b_bootstrap_approval_id: str
    bootstrap_approver_id: str
    cleanup_approval_id: str
    cleanup_approver_id: str

    def validate(self) -> None:
        if platform.system() != "Linux":
            raise R1DualR2CycleError("R1 dual R2 cycle requires Linux")
        if self.output.exists():
            raise R1DualR2CycleError("R1 dual R2 cycle refuses to replace evidence")
        for path in (
            self.sdk_root,
            self.expected_library_hashes,
            self.accounts_file,
            self.fixture,
            self.system_a_namespace_release,
            self.system_a_namespace_trust,
            self.system_b_namespace_release,
            self.system_b_namespace_trust,
        ):
            if not path.exists():
                raise R1DualR2CycleError("R1 dual R2 input path is unavailable")
        if (
            not 1 <= self.system_a_client_id <= 255
            or not 1 <= self.system_b_client_id <= 255
            or self.system_a_client_id == self.system_b_client_id
        ):
            raise R1DualR2CycleError("R1 dual R2 requires distinct EMT scopes")
        if not 20.0 <= self.callback_timeout_seconds <= 60.0:
            raise R1DualR2CycleError("R1 dual R2 callback timeout must be in 20..60 seconds")
        if not 90.0 <= self.system_b_hold_seconds <= 180.0:
            raise R1DualR2CycleError("R1 dual R2 B hold must be in 90..180 seconds")
        if len(self.runtime_release_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.runtime_release_hash
        ):
            raise R1DualR2CycleError("R1 dual R2 runtime hash is invalid")
        if (
            not all(
                (
                    self.system_a_bootstrap_approval_id,
                    self.system_b_bootstrap_approval_id,
                    self.bootstrap_approver_id,
                    self.cleanup_approval_id,
                    self.cleanup_approver_id,
                )
            )
            or self.system_a_bootstrap_approval_id == self.system_b_bootstrap_approval_id
        ):
            raise R1DualR2CycleError("R1 dual R2 approvals are invalid")


def _f2_base(settings: Settings, *, client_id: int, runtime_dir: Path, output: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "tools.acceptance.f2_controlled_execution_cycle",
        "--sdk-root",
        str(settings.sdk_root),
        "--expected-library-hashes",
        str(settings.expected_library_hashes),
        "--accounts-file",
        str(settings.accounts_file),
        "--fixture",
        str(settings.fixture),
        "--output",
        str(output),
        "--runtime-dir",
        str(runtime_dir),
        "--trading-day",
        settings.trading_day,
        "--session-client-id",
        str(client_id),
        "--account-profile",
        "ordinary",
        "--callback-timeout",
        str(settings.callback_timeout_seconds),
        "--r1-authoritative-recovery-probe",
        "--r1-runtime-release-hash",
        settings.runtime_release_hash,
        "--execute-simulated",
        "--isolated-native-process",
    ]


def _append_a_release(settings: Settings, command: list[str], *, bootstrap: bool) -> list[str]:
    command.extend(
        (
            "--r1-namespace-release",
            str(settings.system_a_namespace_release),
            "--r1-namespace-trust",
            str(settings.system_a_namespace_trust),
            "--r1-namespace-state",
            str(settings.system_a_namespace_state),
        )
    )
    if bootstrap:
        command.extend(
            (
                "--r1-bootstrap-approval-id",
                settings.system_a_bootstrap_approval_id,
                "--r1-bootstrap-approver-id",
                settings.bootstrap_approver_id,
            )
        )
    return command


def _b_command(settings: Settings, *, runtime_dir: Path, output: Path) -> list[str]:
    command = _f2_base(
        settings, client_id=settings.system_b_client_id, runtime_dir=runtime_dir, output=output
    )
    command.extend(
        (
            "--hold-before-cancel-seconds",
            str(settings.system_b_hold_seconds),
            "--r1-namespace-release",
            str(settings.system_b_namespace_release),
            "--r1-namespace-trust",
            str(settings.system_b_namespace_trust),
            "--r1-namespace-state",
            str(settings.system_b_namespace_state),
            "--r1-bootstrap-approval-id",
            settings.system_b_bootstrap_approval_id,
            "--r1-bootstrap-approver-id",
            settings.bootstrap_approver_id,
        )
    )
    return command


def _crash_command(settings: Settings, *, runtime_dir: Path, output: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "tools.acceptance.r1_r2_crash_boundary",
        "--sdk-root",
        str(settings.sdk_root),
        "--expected-library-hashes",
        str(settings.expected_library_hashes),
        "--accounts-file",
        str(settings.accounts_file),
        "--fixture",
        str(settings.fixture),
        "--output",
        str(output),
        "--runtime-dir",
        str(runtime_dir),
        "--trading-day",
        settings.trading_day,
        "--session-client-id",
        str(settings.system_a_client_id),
        "--callback-timeout",
        str(settings.callback_timeout_seconds),
        "--namespace-release",
        str(settings.system_a_namespace_release),
        "--namespace-trust",
        str(settings.system_a_namespace_trust),
        "--namespace-state",
        str(settings.system_a_namespace_state),
        "--runtime-release-hash",
        settings.runtime_release_hash,
        "--bootstrap-approval-id",
        settings.system_a_bootstrap_approval_id,
        "--bootstrap-approver-id",
        settings.bootstrap_approver_id,
    ]


def _wait_for_b_hold(
    process: subprocess.Popen[bytes], lifecycle_path: Path, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise R1DualR2CycleError("system B exited before protected hold")
        try:
            if "f2_hold_before_cancel_started" in lifecycle_path.read_text(encoding="ascii"):
                return
        except FileNotFoundError:
            pass
        time.sleep(0.05)
    raise R1DualR2CycleError("system B did not reach protected hold")


def _run(command: list[str], *, timeout: float) -> None:
    result = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[2],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise R1DualR2CycleError("R1 dual R2 child did not pass")


def _report(path: Path, checks: tuple[str, ...]) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise R1DualR2CycleError("R1 dual R2 child report is unavailable") from exc
    if not isinstance(raw, dict) or raw.get("acceptance_status") != "passed":
        raise R1DualR2CycleError("R1 dual R2 child report is not passed")
    report_checks = raw.get("checks")
    if not isinstance(report_checks, dict) or any(
        report_checks.get(name) is not True for name in checks
    ):
        raise R1DualR2CycleError("R1 dual R2 child report lacks checks")
    authority = raw.get("r1_authority")
    return {
        "acceptance_status": "passed",
        "checks": {name: True for name in checks},
        "foreign_reserved_order_count_a": authority.get("foreign_reserved_order_count_a")
        if isinstance(authority, dict)
        else None,
        "foreign_reserved_order_count_b": authority.get("foreign_reserved_order_count_b")
        if isinstance(authority, dict)
        else None,
    }


def _terminate(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _snapshot_crashed_r2_ledger(source: Path, destination: Path) -> Path:
    """Preserve the pre-rehydration R2 fact for an exact approved inverse action.

    Rehydration deliberately writes the venue identity into its working ledger.
    Manual cleanup must instead prove the untouched pre-native boundary, so it
    receives a byte-for-byte snapshot taken after the physical crash and before
    any recovery projector opens the working ledger.
    """

    if not source.is_file():
        raise R1DualR2CycleError("R1 dual R2 crashed route ledger is unavailable")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    if not destination.is_file() or destination.stat().st_size != source.stat().st_size:
        raise R1DualR2CycleError("R1 dual R2 pre-rehydration ledger snapshot failed")
    return destination


def run(settings: Settings) -> dict[str, Any]:
    """Perform A3 and emit aggregate evidence without raw trading payloads."""

    settings.validate()
    root = settings.runtime_dir
    root.mkdir(parents=True, exist_ok=True)
    b_runtime, a_crash_runtime = root / "system-b", root / "system-a-crash"
    a_recovery_runtime, a_cleanup_runtime = root / "system-a-recovery", root / "system-a-cleanup"
    b_report_path = root / "system-b-report.json"
    crash_report_path = root / "system-a-crash-report.json"
    recovery_report_path = root / "system-a-recovery-report.json"
    cleanup_report_path = root / "system-a-cleanup-report.json"
    b_process: subprocess.Popen[bytes] | None = None
    summaries: dict[str, dict[str, object] | None] = {
        "system_b": None,
        "system_a_crash": None,
        "system_a_recovery": None,
        "system_a_cleanup": None,
    }
    b_held_through_recovery = False
    failure_kind: str | None = None
    try:
        b_process = subprocess.Popen(
            _b_command(settings, runtime_dir=b_runtime, output=b_report_path),
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_b_hold(
            b_process,
            b_runtime / "f2-controlled-cycle" / "lifecycle-stages.log",
            settings.callback_timeout_seconds * 8,
        )
        _run(
            _crash_command(settings, runtime_dir=a_crash_runtime, output=crash_report_path),
            timeout=settings.callback_timeout_seconds * 8,
        )
        summaries["system_a_crash"] = _report(
            crash_report_path,
            (
                "insert_order_return_boundary_marker_fsynced",
                "worker_sigkill_before_native_accepted_persistence",
                "native_accepted_not_persisted",
            ),
        )
        crashed_ledger = (
            a_crash_runtime / "f2-controlled-cycle" / "journals" / "r1-route-ledger.mmap"
        )
        cleanup_source_ledger = _snapshot_crashed_r2_ledger(
            crashed_ledger,
            root / "system-a-cleanup-source" / "r1-route-ledger-pre-rehydrate.mmap",
        )
        recovery = _append_a_release(
            settings,
            _f2_base(
                settings,
                client_id=settings.system_a_client_id,
                runtime_dir=a_recovery_runtime,
                output=recovery_report_path,
            ),
            bootstrap=False,
        )
        recovery.extend(("--r1-recovery-route-ledger", str(crashed_ledger)))
        _run(recovery, timeout=settings.callback_timeout_seconds * 10)
        if b_process.poll() is not None:
            raise R1DualR2CycleError("system B ended before A rehydration")
        b_held_through_recovery = True
        summaries["system_a_recovery"] = _report(
            recovery_report_path,
            (
                "r1_authoritative_double_scan",
                "r1_recovery_disarmed",
                "r1_rehydration_persisted",
                "callback_gap_free",
            ),
        )
        cleanup = _append_a_release(
            settings,
            _f2_base(
                settings,
                client_id=settings.system_a_client_id,
                runtime_dir=a_cleanup_runtime,
                output=cleanup_report_path,
            ),
            bootstrap=False,
        )
        cleanup.extend(
            (
                "--r1-manual-cleanup-route-ledger",
                str(cleanup_source_ledger),
                "--r1-manual-cleanup-source-release",
                str(settings.system_a_namespace_release),
                "--r1-manual-cleanup-source-trust",
                str(settings.system_a_namespace_trust),
                "--recovery-approval-id",
                settings.cleanup_approval_id,
                "--recovery-approver-id",
                settings.cleanup_approver_id,
            )
        )
        _run(cleanup, timeout=settings.callback_timeout_seconds * 10)
        summaries["system_a_cleanup"] = _report(
            cleanup_report_path,
            (
                "r1_manual_target_double_scan",
                "r1_manual_cleanup_only",
                "cancel_through_guard_route_scheduler",
                "terminal_callback_released_inflight",
                "post_cleanup_unfinished_zero",
                "callback_gap_free",
            ),
        )
        b_process.wait(
            timeout=settings.system_b_hold_seconds + settings.callback_timeout_seconds * 4
        )
        summaries["system_b"] = _report(
            b_report_path,
            ("r1_authoritative_double_scan", "post_cleanup_unfinished_zero", "callback_gap_free"),
        )
    except (OSError, subprocess.TimeoutExpired, R1DualR2CycleError) as exc:
        failure_kind = type(exc).__name__
    finally:
        _terminate(b_process)
    passed = failure_kind is None and b_held_through_recovery
    report: dict[str, Any] = {
        "schema_version": "tradecore-r1-dual-r2-cycle-report-v1",
        "driver": "r1-dual-r2-cycle",
        "environment": "simulated",
        "acceptance_scope": "R1-A3-only",
        "acceptance_status": "passed" if passed else "failed",
        "full_r1_acceptance": False,
        "ready_claim": False,
        "automatic_recovery_claim": False,
        **summaries,
        "system_b_held_through_a_recovery": b_held_through_recovery,
        "failure_kind": failure_kind,
        "payloads_retained": False,
        "credentials_retained": False,
    }
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--expected-library-hashes", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--system-a-client-id", type=int, required=True)
    parser.add_argument("--system-b-client-id", type=int, required=True)
    parser.add_argument("--callback-timeout", type=float, default=60.0)
    parser.add_argument("--system-b-hold-seconds", type=float, default=150.0)
    parser.add_argument("--system-a-namespace-release", type=Path, required=True)
    parser.add_argument("--system-a-namespace-trust", type=Path, required=True)
    parser.add_argument("--system-a-namespace-state", type=Path, required=True)
    parser.add_argument("--system-b-namespace-release", type=Path, required=True)
    parser.add_argument("--system-b-namespace-trust", type=Path, required=True)
    parser.add_argument("--system-b-namespace-state", type=Path, required=True)
    parser.add_argument("--runtime-release-hash", required=True)
    parser.add_argument("--system-a-bootstrap-approval-id", required=True)
    parser.add_argument("--system-b-bootstrap-approval-id", required=True)
    parser.add_argument("--bootstrap-approver-id", required=True)
    parser.add_argument("--cleanup-approval-id", required=True)
    parser.add_argument("--cleanup-approver-id", required=True)
    args = parser.parse_args(argv)
    report = run(
        Settings(
            sdk_root=args.sdk_root,
            expected_library_hashes=args.expected_library_hashes,
            accounts_file=args.accounts_file,
            fixture=args.fixture,
            output=args.output,
            runtime_dir=args.runtime_dir,
            trading_day=args.trading_day,
            system_a_client_id=args.system_a_client_id,
            system_b_client_id=args.system_b_client_id,
            callback_timeout_seconds=args.callback_timeout,
            system_b_hold_seconds=args.system_b_hold_seconds,
            system_a_namespace_release=args.system_a_namespace_release,
            system_a_namespace_trust=args.system_a_namespace_trust,
            system_a_namespace_state=args.system_a_namespace_state,
            system_b_namespace_release=args.system_b_namespace_release,
            system_b_namespace_trust=args.system_b_namespace_trust,
            system_b_namespace_state=args.system_b_namespace_state,
            runtime_release_hash=args.runtime_release_hash,
            system_a_bootstrap_approval_id=args.system_a_bootstrap_approval_id,
            system_b_bootstrap_approval_id=args.system_b_bootstrap_approval_id,
            bootstrap_approver_id=args.bootstrap_approver_id,
            cleanup_approval_id=args.cleanup_approval_id,
            cleanup_approver_id=args.cleanup_approver_id,
        )
    )
    print(json.dumps({"acceptance_status": report["acceptance_status"]}))
    return 0 if report["acceptance_status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
