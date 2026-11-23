"""Prove R1-A3's exact EMT R2 crash boundary in a disposable Linux worker.

The child is the normal F2 composed execution path.  Its only added hook is
the payload-free marker written after ``TraderApi.insertOrder`` has returned
and before ``NATIVE_ACCEPTED`` can be appended.  The hook fsyncs that marker
and sends SIGKILL to itself.  The parent then verifies the committed local
route-ledger prefix contains the pre-dispatch/call-started route but no native
acceptance receipt.

This proves only the physical R2 boundary.  It intentionally does *not*
claim that EMT query completion, public replay, rehydration, cleanup, R1, or
READY has passed; those remain separately fail-closed prerequisites.
"""

from __future__ import annotations

import argparse
import json
import platform
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from tools.acceptance.r1_release_preflight import load_namespace_trust_v1
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    FileNamespaceReleaseStateStoreV1,
    MmapRouteLedgerV2,
    NamespaceReleaseActivationCoordinatorV1,
    load_signed_namespace_release_v1_json,
)
from xuanling_tradecore.domain import TradingDayEpoch


class R1R2CrashBoundaryError(RuntimeError):
    """The R2 boundary process did not produce its narrowly scoped evidence."""


_LOGICAL_ACCOUNT_ID = "emt_01"
_BOUNDARY = "R2_AFTER_INSERT_RETURN_BEFORE_NATIVE_ACCEPTED_PERSISTENCE"


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    expected_library_hashes: Path
    accounts_file: Path
    fixture: Path
    output: Path
    runtime_dir: Path
    trading_day: str
    session_client_id: int
    callback_timeout_seconds: float
    namespace_release: Path
    namespace_trust: Path
    namespace_state: Path
    runtime_release_hash: str
    bootstrap_approval_id: str
    bootstrap_approver_id: str
    account_profile: str = "ordinary"
    tight_burst_order_plan: Path | None = None

    def validate(self) -> None:
        if platform.system() != "Linux":
            raise R1R2CrashBoundaryError("R1 R2 crash boundary requires Linux")
        for path in (
            self.sdk_root,
            self.expected_library_hashes,
            self.accounts_file,
            self.fixture,
            self.namespace_release,
            self.namespace_trust,
        ):
            if not path.exists():
                raise R1R2CrashBoundaryError("R1 R2 crash boundary input is unavailable")
        if self.output.exists():
            raise R1R2CrashBoundaryError("R1 R2 crash boundary refuses to replace evidence")
        if self.account_profile != "ordinary":
            raise R1R2CrashBoundaryError("R1 R2 crash boundary requires ordinary simulation")
        if not 1 <= self.session_client_id <= 255:
            raise R1R2CrashBoundaryError("R1 R2 crash boundary EMT scope is invalid")
        if not 5.0 <= self.callback_timeout_seconds <= 60.0:
            raise R1R2CrashBoundaryError("R1 R2 crash boundary timeout is invalid")
        if len(self.runtime_release_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.runtime_release_hash
        ):
            raise R1R2CrashBoundaryError("R1 R2 crash boundary runtime hash is invalid")
        if not self.bootstrap_approval_id or not self.bootstrap_approver_id:
            raise R1R2CrashBoundaryError("R1 R2 crash boundary bootstrap approval is required")


def _child_command(settings: Settings, *, marker: Path, worker_output: Path) -> list[str]:
    command = [
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
        str(worker_output),
        "--runtime-dir",
        str(settings.runtime_dir),
        "--trading-day",
        settings.trading_day,
        "--session-client-id",
        str(settings.session_client_id),
        "--account-profile",
        settings.account_profile,
        "--callback-timeout",
        str(settings.callback_timeout_seconds),
        "--r1-authoritative-recovery-probe",
        "--r1-namespace-release",
        str(settings.namespace_release),
        "--r1-namespace-trust",
        str(settings.namespace_trust),
        "--r1-namespace-state",
        str(settings.namespace_state),
        "--r1-runtime-release-hash",
        settings.runtime_release_hash,
        "--r1-bootstrap-approval-id",
        settings.bootstrap_approval_id,
        "--r1-bootstrap-approver-id",
        settings.bootstrap_approver_id,
        "--r1-crash-after-native-return",
        "--r1-crash-marker",
        str(marker),
        "--execute-simulated",
        "--isolated-native-process",
    ]
    if settings.tight_burst_order_plan is not None:
        command.extend(("--tight-burst-order-plan", str(settings.tight_burst_order_plan)))
    return command


def _load_marker(path: Path) -> bool:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(document, dict)
        and document
        == {
            "schema_version": 1,
            "boundary": _BOUNDARY,
            "observed_monotonic_ns": document.get("observed_monotonic_ns"),
            "payloads_retained": False,
            "credentials_retained": False,
        }
        and type(document["observed_monotonic_ns"]) is int
        and document["observed_monotonic_ns"] >= 0
    )


def _route_ledger_boundary_summary(settings: Settings, path: Path) -> dict[str, int]:
    """Read the crashed committed prefix under the same public release fence."""

    try:
        release = load_signed_namespace_release_v1_json(
            settings.namespace_release.read_text(encoding="utf-8")
        )
        trust = load_namespace_trust_v1(settings.namespace_trust)
        activated = NamespaceReleaseActivationCoordinatorV1(
            verifier=trust.verifier(),
            state_store=FileNamespaceReleaseStateStoreV1(settings.namespace_state),
        ).activate(
            release,
            expected_scope=EmtOrderClientNamespaceScope(
                _LOGICAL_ACCOUNT_ID,
                TradingDayEpoch(settings.trading_day),
                settings.session_client_id,
            ),
            expected_release_manifest_hash=settings.runtime_release_hash,
            expected_trust_anchor_hash=trust.trust_anchor_hash,
            now_wall_ns=time.time_ns(),
        )
        ledger = MmapRouteLedgerV2(path=path, capacity=64, directory=activated.directory)
    except BaseException as exc:
        raise R1R2CrashBoundaryError("R1 R2 route ledger is unavailable after crash") from exc
    try:
        facts = ledger.load_route_ledger().facts()
        records = ledger.scan_records()
    finally:
        ledger.close()
    return {
        "committed_record_count": len(records),
        "pre_dispatch_route_count": len(facts),
        "native_acceptance_count": sum(item.order_emt_id is not None for item in facts),
        "call_started_route_count": sum(item.call_started for item in facts),
    }


def run(settings: Settings) -> dict[str, Any]:
    """Run one actual R2 kill and emit a strictly bounded evidence report."""

    settings.validate()
    root = settings.runtime_dir
    root.mkdir(parents=True, exist_ok=True)
    marker = root / "evidence" / "r2-native-return-marker.json"
    worker_output = root / "worker-report-must-not-exist.json"
    if marker.exists() or worker_output.exists():
        raise R1R2CrashBoundaryError("R1 R2 crash runtime path is not clean")
    process: subprocess.Popen[bytes] | None = None
    failure_kind: str | None = None
    route_summary: dict[str, int] | None = None
    try:
        process = subprocess.Popen(
            _child_command(settings, marker=marker, worker_output=worker_output),
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process.wait(timeout=settings.callback_timeout_seconds * 8)
        if process.returncode != -signal.SIGKILL:
            raise R1R2CrashBoundaryError("R1 R2 worker did not exit by SIGKILL")
        if not _load_marker(marker):
            raise R1R2CrashBoundaryError("R1 R2 worker marker is missing or invalid")
        if worker_output.exists():
            raise R1R2CrashBoundaryError("R1 R2 worker unexpectedly reached normal reporting")
        route_summary = _route_ledger_boundary_summary(
            settings,
            root / "f2-controlled-cycle" / "journals" / "r1-route-ledger.mmap",
        )
        if route_summary != {
            "committed_record_count": 1,
            "pre_dispatch_route_count": 1,
            "native_acceptance_count": 0,
            "call_started_route_count": 1,
        }:
            raise R1R2CrashBoundaryError("R1 R2 route ledger does not prove the exact boundary")
    except (OSError, subprocess.TimeoutExpired, R1R2CrashBoundaryError) as exc:
        failure_kind = type(exc).__name__
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    report: dict[str, Any] = {
        "schema_version": "tradecore-r1-r2-crash-boundary-report-v1",
        "driver": "r1-r2-crash-boundary",
        "environment": "simulated",
        "acceptance_scope": "R1-A3-physical-boundary-only",
        "acceptance_status": "passed" if failure_kind is None else "failed",
        "full_r1_acceptance": False,
        "ready_claim": False,
        "automatic_recovery_claim": False,
        "checks": {
            "external_public_release_activated_before_sdk": failure_kind is None,
            "insert_order_return_boundary_marker_fsynced": failure_kind is None,
            "worker_sigkill_before_native_accepted_persistence": failure_kind is None,
            "pre_dispatch_and_call_started_committed": failure_kind is None,
            "native_accepted_not_persisted": failure_kind is None,
        },
        "route_ledger": route_summary,
        "not_completed": (
            "authoritative_query_completion_certificate",
            "public_restart_callback_replay",
            "r2_rehydration_and_manual_cleanup",
            "r1_a3_foreign_system_noninterference",
        ),
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
    parser.add_argument("--session-client-id", type=int, required=True)
    parser.add_argument("--callback-timeout", type=float, default=20.0)
    parser.add_argument("--namespace-release", type=Path, required=True)
    parser.add_argument("--namespace-trust", type=Path, required=True)
    parser.add_argument("--namespace-state", type=Path, required=True)
    parser.add_argument("--runtime-release-hash", required=True)
    parser.add_argument("--bootstrap-approval-id", required=True)
    parser.add_argument("--bootstrap-approver-id", required=True)
    parser.add_argument("--tight-burst-order-plan", type=Path)
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
            session_client_id=args.session_client_id,
            callback_timeout_seconds=args.callback_timeout,
            namespace_release=args.namespace_release,
            namespace_trust=args.namespace_trust,
            namespace_state=args.namespace_state,
            runtime_release_hash=args.runtime_release_hash,
            bootstrap_approval_id=args.bootstrap_approval_id,
            bootstrap_approver_id=args.bootstrap_approver_id,
            tight_burst_order_plan=args.tight_burst_order_plan,
        )
    )
    print(json.dumps({"acceptance_status": report["acceptance_status"]}))
    return 0 if report["acceptance_status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
