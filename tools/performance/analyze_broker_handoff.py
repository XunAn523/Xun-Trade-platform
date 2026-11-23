"""Join one closed diagnostic candidate's frontend/broker/owner handoffs.

The output contains aggregate timings only. Request hashes and raw records stay
in the original private trace directory; this does not certify trading or SLA.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_LIMIT = 1_000_000


def _indexed(rows: list[dict[str, Any]], *, frontend: bool = False) -> dict[tuple[str, str], dict]:
    result: dict[tuple[str, str], dict] = {}
    for row in rows:
        if row.get("operation") != "SubmitSignal":
            continue
        if frontend and "ipc_frontend_offered_ns" not in row:
            raise ValueError("frontend Submit record lacks offered boundary")
        digest = row.get("request_id_sha256")
        generation = row.get("owner_generation" if frontend else "generation")
        if (
            not isinstance(digest, str)
            or not _HASH.fullmatch(digest)
            or not isinstance(generation, str)
            or not generation
            or len(generation) > 128
        ):
            raise ValueError("invalid redacted request identity")
        key = (generation, digest)
        if key in result:
            raise ValueError("duplicate redacted request identity")
        result[key] = row
        if len(result) > _LIMIT:
            raise ValueError("request analysis budget exceeded")
    return result


def _distribution(values: list[int]) -> dict[str, int]:
    if not values:
        raise ValueError("no paired requests in the measurement window")
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ns": ordered[(len(ordered) - 1) * 50 // 100],
        "p95_ns": ordered[(len(ordered) - 1) * 95 // 100],
        "p99_ns": ordered[(len(ordered) - 1) * 99 // 100],
        "p100_ns": ordered[-1],
    }


def summarize(
    frontend: list[dict[str, Any]],
    broker: list[dict[str, Any]],
    owner: list[dict[str, Any]],
    *,
    start_ns: int,
    end_ns: int,
) -> dict[str, Any]:
    if type(start_ns) is not int or type(end_ns) is not int or not 0 < start_ns < end_ns:
        raise ValueError("invalid monotonic window")
    f, b, o = _indexed(frontend, frontend=True), _indexed(broker), _indexed(owner)
    if not f or f.keys() != b.keys() or b.keys() != o.keys():
        raise ValueError("frontend, broker and owner coverage differs")
    spans: dict[str, list[int]] = {
        name: []
        for name in (
            "frontend_to_broker",
            "broker_decode_offer",
            "broker_offer_to_send_before",
            "send_syscall_bracket",
            "send_after_to_owner_lower",
            "send_before_to_owner_upper",
            "frontend_to_owner",
        )
    }
    reverse = slow = 0
    outstanding: list[int] = []
    for key in f:
        front, middle, end = f[key], b[key], o[key]
        if (
            type(middle.get("serial")) is not int
            or middle["serial"] < 1
            or type(end.get("serial")) is not int
            or middle["serial"] != end["serial"]
        ):
            raise ValueError("owner serial mismatch")
        times = [
            front.get("ipc_frontend_offered_ns"),
            middle.get("broker_frontend_received_ns"),
            middle.get("broker_owner_offered_ns"),
            middle.get("broker_final_send_before_ns"),
            middle.get("broker_final_send_after_ns"),
            end.get("ipc_owner_received_ns"),
        ]
        if any(type(t) is not int or t <= 0 for t in times):
            raise ValueError("missing or invalid timing boundary")
        a, c, d, before, after, received = times
        if not a <= c <= d <= before <= after or received < before:
            raise ValueError("timing order or clock identity mismatch")
        held = middle.get("broker_outstanding_before")
        if type(held) is not int or not 0 <= held < 128:
            raise ValueError("invalid broker outstanding count")
        if not start_ns <= a < end_ns:
            continue
        reverse += int(received < after)
        outstanding.append(held)
        slow += int(received - a >= 20_000_000)
        for name, value in (
            ("frontend_to_broker", c - a),
            ("broker_decode_offer", d - c),
            ("broker_offer_to_send_before", before - d),
            ("send_syscall_bracket", after - before),
            ("send_after_to_owner_lower", max(0, received - after)),
            ("send_before_to_owner_upper", received - before),
            ("frontend_to_owner", received - a),
        ):
            spans[name].append(value)
    return {
        "schema": "candidate-broker-handoff-analysis-v1",
        "formal_qualification": False,
        "paired_all_requests": len(f),
        "paired_window_requests": len(outstanding),
        "slow_over_20ms": slow,
        "owner_received_before_send_return": reverse,
        "broker_outstanding_before_max": max(outstanding, default=0),
        "intervals": {name: _distribution(values) for name, values in spans.items()},
        "limitations": [
            "candidate trace changes scheduling; no formal performance qualification",
            "send return and receiver observation may overlap across processes",
            "send-after/owner lower bound is clamped to zero when they overlap",
            "per-stage percentiles cannot be added or subtracted as paired latency",
            "hash chains detect accidental changes, not a maliciously rewritten manifest",
        ],
    }


def _load_group(
    root: Path, pattern: str, expected: int, clock: str, *, require_hash_chain: bool = False
) -> list[dict[str, Any]]:
    statuses = sorted(root.glob(pattern + "*.status.json"))
    if len(statuses) != expected:
        raise ValueError("missing or unexpected trace writers")
    rows: list[dict[str, Any]] = []
    for path in statuses:
        if path.is_symlink() or path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("unsafe or oversized trace status")
        status = json.loads(path.read_text(encoding="utf-8"))
        if (
            status.get("clock_identity_sha256") != clock
            or status.get("closed") is not True
            or status.get("drain_complete") is not True
            or status.get("writer_failed") is not False
            or status.get("queue_dropped") != 0
            or status.get("capacity_dropped") != 0
            or status.get("submitted") != status.get("written")
            or type(status.get("submitted")) is not int
            or type(status.get("written")) is not int
            or status["written"] < 0
            or status.get("status_provider_failed", False) is not False
        ):
            raise ValueError("trace status is incomplete or has a different clock")
        stem = path.name.removesuffix(".status.json")
        files = [
            p
            for p in root.glob(stem + "*.jsonl")
            if re.fullmatch(re.escape(stem) + r"(?:-\d{6})?\.jsonl", p.name)
        ]
        storage = status.get("trace_storage", {})
        if not isinstance(storage, dict):
            raise ValueError("invalid trace storage status")
        if storage.get("mode") == "rotating":
            manifest = root / (stem + ".manifest.json")
            verified_count = verify_closed_request_trace(manifest)
            value = json.loads(manifest.read_text(encoding="utf-8"))
            if (
                value.get("journal_id") != status.get("journal_id")
                or verified_count != status.get("written")
                or {entry["file"] for entry in value["segments"]} != {p.name for p in files}
            ):
                raise ValueError("trace manifest, status and segment set differ")
            # Use the manifest order, not filesystem enumeration order.
            files = [root / entry["file"] for entry in value["segments"]]
        elif require_hash_chain:
            raise ValueError("trace writer lacks a verified closed hash chain")
        found = 0
        for trace in files:
            if trace.is_symlink() or trace.stat().st_size > 512 * 1024 * 1024:
                raise ValueError("unsafe or oversized trace")
            with trace.open(encoding="utf-8") as source:
                for line in source:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("invalid trace record")
                    rows.append(record)
                    found += 1
                    if len(rows) > _LIMIT * 4:
                        raise ValueError("trace record budget exceeded")
        if found != status.get("written"):
            raise ValueError("trace file count differs from closed writer status")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-dir", type=Path, required=True)
    parser.add_argument("--window-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--require-hash-chain",
        action="store_true",
        help="reject every trace writer without a verified closed rotating manifest",
    )
    args = parser.parse_args()
    window = json.loads(args.window_report.read_text(encoding="utf-8"))
    clock = window.get("clock_identity_sha256")
    if (
        window.get("window_contract_valid") is not True
        or not isinstance(clock, str)
        or not _HASH.fullmatch(clock)
    ):
        raise ValueError("unverified window or clock")
    root = args.trace_dir
    result = summarize(
        _load_group(
            root, "frontend-diagnostic-", 8, clock, require_hash_chain=args.require_hash_chain
        ),
        _load_group(
            root, "broker-diagnostic-", 1, clock, require_hash_chain=args.require_hash_chain
        ),
        _load_group(root, "owner-ipc-", 1, clock, require_hash_chain=args.require_hash_chain),
        start_ns=window["start_ns"],
        end_ns=window["end_ns"],
    )
    result["clock_identity_sha256"] = clock
    result["trace_hash_chain_coverage"] = (
        "ALL_WRITERS_VERIFIED" if args.require_hash_chain else "NOT_ASSERTED"
    )
    result["window_start_ns"] = window["start_ns"]
    result["window_end_ns"] = window["end_ns"]
    with args.output.open("x", encoding="utf-8") as target:
        json.dump(result, target, sort_keys=True, separators=(",", ":"))
        target.write("\n")
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
