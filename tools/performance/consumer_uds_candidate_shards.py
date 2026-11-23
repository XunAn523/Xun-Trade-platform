"""Isolate short-probe event loops; common absolute window, real HDR merge.

Only launches the public consumer probe, never the server/OMS or reset tools.
Use one finite-memory generator cgroup enclosing this parent AND its children.
This diagnostic launcher is not the full MIX/12h orchestration or evaluator.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

from hdrh.histogram import HdrHistogram
from perfbench.short_probe_contract import common_start, compatible_clocks, outcome_ledger
from tools.performance.consumer_uds_candidate_probe import diagnostic_checks, histogram_summary
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json, open_private


def merge_reports(reports: list[dict], *, start_ns: int, planned_each: int) -> dict:
    if not 1 <= len(reports) <= 8 or sum(r["planned_each"] for r in reports) != planned_each:
        raise ValueError("child planned totals do not cover the common window")
    counts: Counter[str] = Counter()
    clock = compatible_clocks(reports)
    ends = {report.get("end_ns") for report in reports}
    if len(ends) != 1:
        raise ValueError("short-probe end windows differ")
    histograms = {}
    maxima = {}
    namespaces = [report.get("sequence_base") for report in reports]
    revisions = {report.get("tool_revision") for report in reports}
    if len(revisions) != 1:
        raise ValueError("generator tool revisions differ")
    targets = {report.get("target_kind") for report in reports}
    if len(targets) != 1:
        raise ValueError("fixture and TradeCore results cannot be merged")
    child_release_clocks = [report.get("release_clock") for report in reports]
    if any(not isinstance(value, dict) for value in child_release_clocks):
        raise ValueError("generator release clock is missing")
    release_clocks = [dict(value) for value in child_release_clocks if isinstance(value, dict)]
    release_signatures = {
        json.dumps(
            {
                key: value.get(key)
                for key in (
                    "schema",
                    "completion_reserve_ns",
                    "first_release_ns",
                    "last_release_ns",
                    "observation_window_ns",
                )
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for value in release_clocks
    }
    if len(release_signatures) != 1:
        raise ValueError("generator release clocks differ")
    release_clock = release_clocks[0]
    if (
        release_clock.get("schema") != "completion-aware-100hz-v1"
        or release_clock.get("first_release_ns") != start_ns
        or sum(value.get("planned_each", -1) for value in release_clocks) != planned_each
    ):
        raise ValueError("generator release clock is invalid")
    known_namespaces = all(type(value) is int for value in namespaces)
    if known_namespaces and len(set(namespaces)) != len(namespaces):
        raise ValueError("duplicate generator business namespace")
    for report in reports:
        if report["start_ns"] != start_ns or report["formal_qualification"] is not False:
            raise ValueError("incompatible child window/profile")
        counts.update(report["counts"])
        if "outcome_ledger" in report:
            child_ledger = outcome_ledger(report["counts"], planned_each=report["planned_each"])
            if (
                child_ledger != report["outcome_ledger"]
                or child_ledger["unissued"] != report["unissued"]
            ):
                raise ValueError("child outcome ledger differs from counts")
        for name, encoded in report["hdr_encoded"].items():
            decoded = HdrHistogram.decode(encoded.encode("ascii"))
            if name in histograms:
                histograms[name].add(decoded)
            else:
                histograms[name] = decoded
        for name, value in report["raw_max_ns"].items():
            maxima[name] = max(maxima.get(name, 0), value)
    ledger = outcome_ledger(dict(counts), planned_each=planned_each)
    result = {
        "schema": "consumer-candidate-sharded-short-probe-v1",
        "formal_qualification": False,
        "start_ns": start_ns,
        "end_ns": ends.pop(),
        "clock": "time.monotonic_ns",
        "tool_revision": revisions.pop(),
        "target_kind": targets.pop(),
        "clock_identity_sha256": clock,
        "release_clock": {
            **release_clock,
            "planned_each": planned_each,
            "window_average_tps": sum(
                int(value["window_average_tps"]) for value in release_clocks
            ),
        },
        "planned_each": planned_each,
        "shards": len(reports),
        "counts": dict(counts),
        "unissued": ledger["unissued"],
        "transport_failed": any(r.get("transport_failed", r["failed"]) for r in reports),
        "request_failed": ledger["request_failed"],
        "outcome_ledger": ledger,
        "raw_max_ns": maxima,
        "histograms": {name: histogram_summary(h) for name, h in histograms.items()},
        "limitations": ["short diagnostic only", "not full qualification", "no trace join"],
    }
    result["diagnostic_checks"] = diagnostic_checks(
        counts=result["counts"],
        histograms=result["histograms"],
        planned_each=planned_each,
        unissued=result["unissued"],
        transport_failed=result["transport_failed"],
    )
    result["diagnostic_checks"]["ledger_conserved"] = ledger["conserved"]
    result["diagnostic_checks"]["clock_identity_known"] = clock is not None
    result["diagnostic_checks"]["window_known"] = result["end_ns"] is not None
    result["diagnostic_checks"]["namespaces_known"] = known_namespaces
    result["diagnostic_slo_passed"] = all(result["diagnostic_checks"].values())
    result["failed"] = not result["diagnostic_slo_passed"]
    return result


def main() -> int:
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument(
        "--cpu-groups", required=True, help="Semicolon-separated explicit CPU ID lists"
    )
    parser.add_argument("--sequence-base", type=int, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--lead-seconds", type=int, default=10)
    parser.add_argument("--start-ns", type=int)
    args = parser.parse_args()
    groups = args.cpu_groups.split(";")
    if (
        not 1 <= len(groups) <= 8
        or args.rate % len(groups)
        or not 1 <= args.duration <= 60
        or not 1 <= args.rate <= 2000
        or not 5 <= args.lead_seconds <= 60
        or any(
            not group or any(not v.isascii() or not v.isdecimal() for v in group.split(","))
            for group in groups
        )
    ):
        parser.error("invalid bounded shard/release profile")
    if not 0 <= args.sequence_base < 10**17 - len(groups) * 1_000_000:
        parser.error("shard business sequence namespaces exceed the bounded mock contract")
    cpus = [int(v) for group in groups for v in group.split(",")]
    if len(cpus) != len(set(cpus)):
        parser.error("generator shard CPU sets overlap")
    started = common_start(
        now_ns=time.monotonic_ns(), supplied_ns=args.start_ns, lead_seconds=args.lead_seconds
    )
    args.output.mkdir(mode=0o700, exist_ok=False)
    processes, handles = [], []
    script = Path(__file__).with_name("consumer_uds_candidate_probe.py")
    exits = []
    try:
        for index, group in enumerate(groups):
            error = os.fdopen(
                open_private(
                    args.output / f"shard-{index}.stderr", os.O_CREAT | os.O_EXCL | os.O_WRONLY
                ),
                "wb",
            )
            handles.append(error)
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(script),
                    "--config",
                    str(args.config),
                    "--output",
                    str(args.output / f"shard-{index}"),
                    "--duration",
                    str(args.duration),
                    "--rate",
                    str(args.rate // len(groups)),
                    "--sequence-base",
                    str(args.sequence_base + index * 1_000_000),
                    "--trading-day",
                    args.trading_day,
                    "--start-ns",
                    str(started),
                    "--cpus",
                    group,
                ],
                stdout=subprocess.DEVNULL,
                stderr=error,
            )
            processes.append(process)
        deadline = (
            time.monotonic() + max(0, (started - time.monotonic_ns()) / 1e9) + args.duration + 65
        )
        for process in processes:
            exits.append(process.wait(timeout=max(1, deadline - time.monotonic())))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()
    reports = []
    for index in range(len(groups)):
        path = args.output / f"shard-{index}" / "report.json"
        if not path.is_file() or path.stat().st_size > 2 * 1024**2:
            raise ValueError("missing/oversized child report; not a successful run")
        reports.append(json.loads(path.read_text()))
    report = merge_reports(reports, start_ns=started, planned_each=args.rate * args.duration)
    report["child_exit_codes"] = exits
    report["failed"] |= any(code != 0 for code in exits)
    report["diagnostic_slo_passed"] &= not report["failed"]
    atomic_runtime_json(args.output / "report.json", report)
    print(json.dumps(report, separators=(",", ":")))
    return 1 if report["failed"] or report["unissued"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
