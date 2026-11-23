"""Linux-only physical process-kill recovery drill for F1 mmap Journals.

This drill never loads the EMT SDK and never sends a query, order or cancel.
It writes only payload-free local Journal facts in an isolated child, kills that
child with ``SIGKILL`` at documented recovery boundaries, then restarts a
strict reader in the parent.  The two mid-write cases deliberately leave a
non-committed body prefix in the next fixed frame before the kill.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
from dataclasses import dataclass
from multiprocessing.connection import Connection
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from xuanling_tradecore.persistence import (
    MmapJournalShardInputV2,
    MmapPreNativeJournal,
    MmapRecoveryCoordinatorV2,
)
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapJournalRecoveryStatus

_DIGEST = "f" * 64
_CAPACITY = 8
_HEADER_SIZE = 4_096
_FRAME_SIZE = 512


class F1PhysicalKillDrillError(RuntimeError):
    """The local, no-venue physical Journal drill could not complete."""


@dataclass(frozen=True)
class _Case:
    name: str
    expected_status: MmapJournalRecoveryStatus
    expected_trusted_records: int
    expected_reconciliation: str | None


_CASES = (
    _Case("C2_BEFORE", MmapJournalRecoveryStatus.CLEAN, 0, None),
    _Case("C2_AFTER_C4_BEFORE", MmapJournalRecoveryStatus.CLEAN, 2, "SUBMIT_UNKNOWN"),
    _Case("C4_AFTER_C5_BEFORE", MmapJournalRecoveryStatus.CLEAN, 3, "CALLBACK_PENDING"),
    _Case("CALLBACK_MID_WRITE", MmapJournalRecoveryStatus.UNTRUSTED_TAIL, 3, None),
    _Case("CANCEL_MID_WRITE", MmapJournalRecoveryStatus.UNTRUSTED_TAIL, 3, None),
)


def run(*, root: Path) -> dict[str, Any]:
    """Kill five local writers and prove strict restart behavior without venue I/O."""

    if platform.system() != "Linux":
        raise F1PhysicalKillDrillError("F1 physical kill drill requires Linux")
    root.mkdir(parents=True, exist_ok=True)
    from multiprocessing import get_context

    context = get_context("fork")
    outcomes: list[dict[str, Any]] = []
    for case in _CASES:
        journal_path = root / f"{case.name.lower()}.journal"
        parent, child = context.Pipe(duplex=False)
        process = context.Process(target=_writer_child, args=(journal_path, case.name, child))
        process.start()
        child.close()
        try:
            if not parent.poll(10.0) or parent.recv() != "ready":
                raise F1PhysicalKillDrillError("F1 kill-drill child did not reach its boundary")
            if process.pid is None:
                raise F1PhysicalKillDrillError("F1 kill-drill child has no process ID")
            os.kill(process.pid, signal.SIGKILL)
            process.join(timeout=10.0)
            if process.is_alive() or process.exitcode != -signal.SIGKILL:
                raise F1PhysicalKillDrillError("F1 kill-drill child did not terminate by SIGKILL")
        finally:
            parent.close()

        reopened = MmapPreNativeJournal(path=journal_path, capacity=_CAPACITY)
        try:
            scan = reopened.scan_recovery_prefix()
            result = MmapRecoveryCoordinatorV2().recover(
                (
                    MmapJournalShardInputV2(
                        scope_id="scope-f1",
                        shard_id=case.name.lower(),
                        journal=reopened,
                        route_directory_digest=_DIGEST,
                        writer_identity="f1-physical-kill-drill",
                    ),
                )
            )
            # The recovery API returns only an immutable gate snapshot. Its
            # ``admission_closed`` state is the source-of-truth assertion;
            # no recovery result exposes a submit or resend method.
            submit_rejected = result.gate.admission_closed
            reconciliation = tuple(item.kind.value for item in result.reconciliation_items)
            integrity_gate = any(
                event.reason.value == "JOURNAL_INTEGRITY" for event in result.gate.events
            )
            asserted = (
                scan.status is case.expected_status
                and scan.trusted_position == case.expected_trusted_records
                and (
                    case.expected_reconciliation is None
                    or case.expected_reconciliation in reconciliation
                )
                and submit_rejected
                and (
                    integrity_gate
                    if case.expected_status is not MmapJournalRecoveryStatus.CLEAN
                    else True
                )
            )
            outcomes.append(
                {
                    "case": case.name,
                    "child_terminated_by_sigkill": True,
                    "strict_scan_status": scan.status.value,
                    "trusted_record_count": scan.trusted_position,
                    "first_untrusted_sequence": scan.first_untrusted_sequence,
                    "reconciliation_kinds": list(reconciliation),
                    "integrity_gate": integrity_gate,
                    "runtime_state": result.gate.runtime_state,
                    "manual_reconciliation_required": result.gate.manual_reconciliation_required,
                    "submit_rejected": submit_rejected,
                    "asserted": asserted,
                }
            )
        finally:
            reopened.close()

    complete = all(bool(item["asserted"]) for item in outcomes)
    return {
        "schema_version": 1,
        "driver": "f1-mmap-physical-kill-drill-v1",
        "executed": True,
        "complete": complete,
        "safety": {
            "linux_only": True,
            "sdk_loaded": False,
            "login": False,
            "query": False,
            "order_submit": False,
            "cancel_submit": False,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "cases": outcomes,
    }


def _writer_child(path: Path, case_name: str, ready: Connection) -> None:
    """Write a case's last local state then wait for the parent SIGKILL."""

    journal = MmapPreNativeJournal(path=path, capacity=_CAPACITY)
    try:
        if case_name != "C2_BEFORE":
            _append_pre_native(journal)
            _append_call_started(journal)
        if case_name in {"C4_AFTER_C5_BEFORE", "CALLBACK_MID_WRITE", "CANCEL_MID_WRITE"}:
            _append_native_accepted(journal)
        if case_name in {"CALLBACK_MID_WRITE", "CANCEL_MID_WRITE"}:
            _write_noncommitted_frame_prefix(journal, sequence=4)
        ready.send("ready")
        signal.pause()
    finally:
        # SIGKILL does not execute this branch. Keeping it makes an accidental
        # non-SIGKILL child failure clean and therefore diagnosable by parent.
        journal.close()
        ready.close()


def _append_pre_native(journal: MmapPreNativeJournal) -> None:
    journal.append_pre_native(
        monotonic_ns=10,
        account_id="account-f1",
        execution_id="execution-f1",
        command_id="command-f1",
        payload_hash=_DIGEST,
        client_order_ref="ref-f1",
    )


def _append_call_started(journal: MmapPreNativeJournal) -> None:
    journal.append_call_started(
        monotonic_ns=11,
        account_id="account-f1",
        execution_id="execution-f1",
        command_id="command-f1",
        payload_hash=_DIGEST,
        client_order_ref="ref-f1",
    )


def _append_native_accepted(journal: MmapPreNativeJournal) -> None:
    journal.append_native_accepted(
        monotonic_ns=12,
        account_id="account-f1",
        execution_id="execution-f1",
        command_id="command-f1",
        payload_hash=_DIGEST,
        client_order_ref="ref-f1",
        native_order_id="native-f1",
    )


def _write_noncommitted_frame_prefix(journal: MmapPreNativeJournal, *, sequence: int) -> None:
    """Materialize a nonzero body prefix and deliberately omit the commit word."""

    offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
    journal._mapped[offset : offset + 16] = b"TCMF" + (b"\0" * 12)  # noqa: SLF001


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the local F1 mmap SIGKILL recovery drill")
    parser.add_argument("--execute-linux-drill", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.execute_linux_drill:
        raise F1PhysicalKillDrillError("--execute-linux-drill is required")
    with TemporaryDirectory(prefix="tradecore-f1-kill-") as temporary:
        report = run(root=Path(temporary))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"complete": report["complete"], "driver": report["driver"]}))
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
