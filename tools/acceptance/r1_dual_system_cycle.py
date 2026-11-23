"""Run R1-A2's bounded two-process EMT simulator composition.

The coordinator never imports the EMT SDK and never reads account data.  It
starts two separate F2 worker processes with distinct signed client scopes,
runtime roots, namespace-release state and Journals.  System B is held only
after its own recovery evidence has been collected; system A then runs its
own controlled cycle while B's order remains active.  The coordinator rejects
the result if B reaches cleanup before A completes.  Both workers still
perform their own rate-limited cancellation and unfinished-order cleanup.

This is deliberately only R1-A2 evidence.  It cannot claim R1-A3's native
return crash boundary, callback replay, automatic recovery or READY.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SYSTEM_B_PREPARE_TIMEOUT_SECONDS = 900.0


class R1DualSystemCycleError(RuntimeError):
    """The two-process coordinator cannot obtain bounded A/B evidence."""


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
    account_profile: str = "ordinary"
    tight_burst_order_plan: Path | None = None

    def validate(self) -> None:
        if platform.system() != "Linux":
            raise R1DualSystemCycleError("R1 dual-system cycle requires Linux")
        for path in (self.sdk_root, self.expected_library_hashes, self.accounts_file, self.fixture):
            if not path.exists():
                raise R1DualSystemCycleError("R1 dual-system input path is unavailable")
        if self.account_profile != "ordinary":
            raise R1DualSystemCycleError("R1 dual-system mutation cycle requires ordinary profile")
        if (
            not 1 <= self.system_a_client_id <= 255
            or not 1 <= self.system_b_client_id <= 255
            or self.system_a_client_id == self.system_b_client_id
        ):
            raise R1DualSystemCycleError("R1 dual-system cycle requires two distinct EMT scopes")
        if not 5.0 <= self.callback_timeout_seconds <= 60.0:
            raise R1DualSystemCycleError("R1 dual-system callback timeout must be in 5..60 seconds")
        if not 90.0 <= self.system_b_hold_seconds <= 180.0:
            raise R1DualSystemCycleError("R1 dual-system B hold must be in 90..180 seconds")
        for path in (
            self.system_a_namespace_release,
            self.system_a_namespace_trust,
            self.system_b_namespace_release,
            self.system_b_namespace_trust,
        ):
            if not path.is_file():
                raise R1DualSystemCycleError("R1 dual-system public release input is unavailable")
        for path in (self.system_a_namespace_state, self.system_b_namespace_state):
            if path.exists() and not path.is_file():
                raise R1DualSystemCycleError("R1 dual-system namespace state is invalid")
        if len(self.runtime_release_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.runtime_release_hash
        ):
            raise R1DualSystemCycleError("R1 dual-system runtime release hash is invalid")
        if (
            not self.system_a_bootstrap_approval_id
            or not self.system_b_bootstrap_approval_id
            or self.system_a_bootstrap_approval_id == self.system_b_bootstrap_approval_id
            or not self.bootstrap_approver_id
        ):
            raise R1DualSystemCycleError("R1 dual-system bootstrap approvals are invalid")


def _f2_command(
    settings: Settings,
    *,
    client_id: int,
    runtime_dir: Path,
    output: Path,
    hold_before_cancel_seconds: float,
    namespace_release: Path,
    namespace_trust: Path,
    namespace_state: Path,
    bootstrap_approval_id: str,
) -> list[str]:
    command = [
        sys.executable,
        str(Path(__file__).with_name("f2_controlled_execution_cycle.py")),
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
        settings.account_profile,
        "--callback-timeout",
        str(settings.callback_timeout_seconds),
        "--hold-before-cancel-seconds",
        str(hold_before_cancel_seconds),
        "--r1-authoritative-recovery-probe",
        "--r1-namespace-release",
        str(namespace_release),
        "--r1-namespace-trust",
        str(namespace_trust),
        "--r1-namespace-state",
        str(namespace_state),
        "--r1-runtime-release-hash",
        settings.runtime_release_hash,
        "--r1-bootstrap-approval-id",
        bootstrap_approval_id,
        "--r1-bootstrap-approver-id",
        settings.bootstrap_approver_id,
        "--execute-simulated",
        "--isolated-native-process",
    ]
    if settings.tight_burst_order_plan is not None:
        command.extend(("--tight-burst-order-plan", str(settings.tight_burst_order_plan)))
    return command


def _wait_for_b_ready(
    *, process: subprocess.Popen[bytes], lifecycle_path: Path, timeout_seconds: float
) -> None:
    deadline = time.monotonic() + timeout_seconds
    marker = "f2_hold_before_cancel_started"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise R1DualSystemCycleError("system B exited before entering its hold window")
        try:
            stages = lifecycle_path.read_text(encoding="ascii")
        except FileNotFoundError:
            stages = ""
        if marker in stages:
            return
        time.sleep(0.05)
    raise R1DualSystemCycleError("system B did not reach its bounded hold window")


def _load_safe_worker_summary(path: Path) -> dict[str, object]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise R1DualSystemCycleError("R1 worker report is unavailable") from exc
    if not isinstance(report, dict):
        raise R1DualSystemCycleError("R1 worker report is invalid")
    checks = report.get("checks")
    authority = report.get("r1_authority")
    if not isinstance(checks, dict) or not isinstance(authority, dict):
        raise R1DualSystemCycleError("R1 worker report lacks bounded evidence")
    allowed_checks = (
        "r1_namespace_route_fenced",
        "r1_authoritative_double_scan",
        "r1_recovery_disarmed",
        "post_cleanup_unfinished_zero",
        "callback_gap_free",
    )
    required_authority = (
        "recovery_disposition",
        "foreign_reserved_order_count_a",
        "foreign_reserved_order_count_b",
        "recovery_certificate",
    )
    if any(name not in authority for name in required_authority):
        raise R1DualSystemCycleError("R1 worker report lacks foreign classification evidence")
    return {
        "acceptance_status": report.get("acceptance_status"),
        "checks": {name: checks.get(name) is True for name in allowed_checks},
        "recovery_disposition": authority["recovery_disposition"],
        "foreign_reserved_order_count_a": authority["foreign_reserved_order_count_a"],
        "foreign_reserved_order_count_b": authority["foreign_reserved_order_count_b"],
        "certificate": authority["recovery_certificate"],
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


def run(settings: Settings) -> dict[str, Any]:
    """Run the bounded A/B cycle and emit only safe aggregate evidence."""

    settings.validate()
    root = settings.runtime_dir
    root.mkdir(parents=True, exist_ok=True)
    a_root, b_root = root / "system-a", root / "system-b"
    a_report, b_report = root / "system-a-report.json", root / "system-b-report.json"
    b_process: subprocess.Popen[bytes] | None = None
    a_process: subprocess.Popen[bytes] | None = None
    failure_kind: str | None = None
    a_summary: dict[str, object] | None = None
    b_summary: dict[str, object] | None = None
    try:
        b_process = subprocess.Popen(
            _f2_command(
                settings,
                client_id=settings.system_b_client_id,
                runtime_dir=b_root,
                output=b_report,
                hold_before_cancel_seconds=settings.system_b_hold_seconds,
                namespace_release=settings.system_b_namespace_release,
                namespace_trust=settings.system_b_namespace_trust,
                namespace_state=settings.system_b_namespace_state,
                bootstrap_approval_id=settings.system_b_bootstrap_approval_id,
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_b_ready(
            process=b_process,
            lifecycle_path=b_root / "f2-controlled-cycle" / "lifecycle-stages.log",
            # The full R1 B worker performs several complete, serial EMT
            # scans before entering its hold window.  This controller bound is
            # deliberately independent from the per-query terminal bound: it
            # permits a large, healthy historical account to finish its
            # fail-closed scans without weakening their 60-second limit.
            timeout_seconds=_SYSTEM_B_PREPARE_TIMEOUT_SECONDS,
        )
        a_process = subprocess.Popen(
            _f2_command(
                settings,
                client_id=settings.system_a_client_id,
                runtime_dir=a_root,
                output=a_report,
                hold_before_cancel_seconds=0.0,
                namespace_release=settings.system_a_namespace_release,
                namespace_trust=settings.system_a_namespace_trust,
                namespace_state=settings.system_a_namespace_state,
                bootstrap_approval_id=settings.system_a_bootstrap_approval_id,
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        a_process.wait(timeout=settings.callback_timeout_seconds * 12)
        if b_process.poll() is not None:
            raise R1DualSystemCycleError("system B ended before system A completed")
        b_process.wait(timeout=settings.callback_timeout_seconds * 12)
        a_summary = _load_safe_worker_summary(a_report)
        b_summary = _load_safe_worker_summary(b_report)
        a_foreign_observed = (
            isinstance(a_summary["foreign_reserved_order_count_a"], int)
            and isinstance(a_summary["foreign_reserved_order_count_b"], int)
            and max(
                a_summary["foreign_reserved_order_count_a"],
                a_summary["foreign_reserved_order_count_b"],
            )
            > 0
        )
        if (
            a_process.returncode != 0
            or b_process.returncode != 0
            or a_summary["acceptance_status"] != "passed"
            or b_summary["acceptance_status"] != "passed"
            or not a_foreign_observed
        ):
            failure_kind = "R1DualSystemEvidenceIncomplete"
    except (OSError, subprocess.TimeoutExpired, R1DualSystemCycleError) as exc:
        failure_kind = type(exc).__name__
    finally:
        _terminate(a_process)
        _terminate(b_process)
    report: dict[str, Any] = {
        "schema_version": "tradecore-r1-dual-system-cycle-report-v1",
        "driver": "r1-dual-system-cycle",
        "environment": "simulated",
        "acceptance_scope": "R1-A2-only",
        "acceptance_status": "passed" if failure_kind is None else "failed",
        "full_r1_acceptance": False,
        "ready_claim": False,
        "automatic_recovery_claim": False,
        "system_a": a_summary,
        "system_b": b_summary,
        "failure_kind": failure_kind,
    }
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
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
    parser.add_argument("--callback-timeout", type=float, default=20.0)
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
    parser.add_argument("--tight-burst-order-plan", type=Path)
    args = parser.parse_args()
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
            tight_burst_order_plan=args.tight_burst_order_plan,
        )
    )
    print(json.dumps({"acceptance_status": report["acceptance_status"]}))
    return 0 if report["acceptance_status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
