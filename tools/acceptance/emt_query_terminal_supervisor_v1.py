"""Supervise a one-shot raw EMT query probe without retaining query payloads.

Some vendor-wrapper paths can terminate a child process after the native
request returns but before Python has unwound to its normal report writer.  A
parent process turns that otherwise invisible termination into an explicit,
payload-free ``terminal_verified=false`` result.  It does not reinterpret a
timeout, a signal, or a missing report as query completion.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Mapping, Sequence


class EmtQueryTerminalSupervisorError(RuntimeError):
    """The raw terminal probe cannot be supervised safely."""


_ALLOWED_CASES = frozenset(
    {
        "QueryIPOInfoList",
        "QueryYesterdayAsset",
        "QueryCreditPositionFullRate",
    }
)


def _progress_phase(path: Path) -> str | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    phase = value.get("phase") if isinstance(value, Mapping) else None
    return phase if isinstance(phase, str) and phase.isascii() and len(phase) <= 96 else None


def _missing_worker_report(
    *,
    api_name: str,
    profile: str,
    worker_return_code: int | None,
    worker_timed_out: bool,
    progress_phase: str | None,
) -> dict[str, object]:
    """Produce only safe control-flow facts when the child could not report."""

    return {
        "schema_version": 1,
        "driver": "emt-query-terminal-supervisor-v1",
        "safety": {
            "mode": "single-query-read-only",
            "order_entry": False,
            "cancellation": False,
            "fund_or_credit_mutation": False,
            "payload_persisted": False,
        },
        "result": {
            "api": api_name,
            "profile": profile,
            "worker_report_persisted": False,
            "worker_return_code": worker_return_code,
            "worker_timed_out": worker_timed_out,
            "payload_free_progress_phase": progress_phase,
            "terminal_verified": False,
            "failure_class": "native_worker_terminated_before_payload_free_report",
        },
    }


def _write_report(path: Path, report: Mapping[str, object]) -> None:
    if path.exists():
        raise EmtQueryTerminalSupervisorError("terminal probe output already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--query-fixture", type=Path, required=True)
    parser.add_argument("--profile", choices=("ordinary", "credit"), required=True)
    parser.add_argument("--query-case", choices=tuple(sorted(_ALLOWED_CASES)), required=True)
    parser.add_argument("--callback-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.output.exists():
        raise EmtQueryTerminalSupervisorError("terminal probe output already exists")
    if not 0 < args.callback_timeout <= 60:
        raise EmtQueryTerminalSupervisorError("callback timeout must be in 0..60")
    if args.query_case == "QueryCreditPositionFullRate" and args.profile != "credit":
        raise EmtQueryTerminalSupervisorError("credit full-rate probe requires credit profile")
    if args.query_case != "QueryCreditPositionFullRate" and args.profile != "ordinary":
        raise EmtQueryTerminalSupervisorError("ordinary query probe requires ordinary profile")

    command = (
        sys.executable,
        "-m",
        "tools.acceptance.emt_query_terminal_probe_v1",
        "--sdk-root",
        str(args.sdk_root),
        "--accounts-file",
        str(args.accounts_file),
        "--query-fixture",
        str(args.query_fixture),
        "--profile",
        args.profile,
        "--query-case",
        args.query_case,
        "--callback-timeout",
        str(args.callback_timeout),
        "--output",
        str(args.output),
    )
    timed_out = False
    try:
        completed = subprocess.run(  # noqa: S603 - fixed module invocation, no shell
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=args.callback_timeout + 10.0,
        )
        return_code: int | None = completed.returncode
    except subprocess.TimeoutExpired:
        timed_out, return_code = True, None

    if args.output.is_file():
        # The child report already contains the raw terminal evidence.  Never
        # rewrite it with supervisor metadata or weaken its contract.
        return 0
    progress = _progress_phase(args.output.parent / f"{args.query_case}-runtime" / "terminal-probe-progress.json")
    _write_report(
        args.output,
        _missing_worker_report(
            api_name=args.query_case,
            profile=args.profile,
            worker_return_code=return_code,
            worker_timed_out=timed_out,
            progress_phase=progress,
        ),
    )
    return 2


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
