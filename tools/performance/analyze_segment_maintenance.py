"""Verify closed, private segment-maintenance diagnostics; never certify a performance SLA."""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from pathlib import Path

from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace

_PHASES = frozenset(
    {
        "operation",
        "ftruncate",
        "fallocate",
        "prefault",
        "prepare_flush",
        "prepare_fsync",
        "seal_validate",
        "seal_flush",
        "seal_fsync",
        "seal_digest",
    }
)


def analyze(manifest: Path, *, expected_clock: str) -> dict:
    if (
        not isinstance(expected_clock, str)
        or len(expected_clock) != 64
        or any(c not in "0123456789abcdef" for c in expected_clock)
    ):
        raise ValueError("invalid clock identity")
    count = verify_closed_request_trace(manifest)
    meta = json.loads(manifest.read_text(encoding="utf-8"))
    stem = manifest.name.removesuffix(".manifest.json")
    if stem == manifest.name or not stem.startswith("segment-maintenance-"):
        raise ValueError("unexpected maintenance manifest name")
    status_path = manifest.with_name(stem + ".status.json")
    if status_path.is_symlink() or status_path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("unsafe maintenance status")
    status = json.loads(status_path.read_text(encoding="utf-8"))
    if (
        status.get("clock_identity_sha256") != expected_clock
        or status.get("journal_id") != meta.get("journal_id")
        or status.get("closed") is not True
        or status.get("drain_complete") is not True
        or status.get("writer_failed") is not False
        or status.get("status_provider_failed", False) is not False
        or status.get("queue_dropped") != 0
        or status.get("capacity_dropped") != 0
        or status.get("submitted") != count
        or status.get("written") != count
    ):
        raise ValueError("maintenance trace status incomplete or incompatible")
    journal = meta.get("journal_id")
    if not isinstance(journal, str):
        raise ValueError("invalid maintenance journal")
    journal_digest = sha256(journal.encode()).hexdigest()
    open_phases = {}
    completed_phases = set()
    operations = {}
    durations: dict[str, list[int]] = {}
    failures = 0
    for entry in meta["segments"]:
        for line in (manifest.parent / entry["file"]).open(encoding="utf-8"):
            record = json.loads(line)
            serial, index = record.get("serial"), record.get("segment_index")
            operation, phase, state, at = (
                record.get("operation"),
                record.get("phase"),
                record.get("state"),
                record.get("at_ns"),
            )
            if (
                record.get("scope") != "candidate_segment_maintenance"
                or record.get("journal_id_sha256") != journal_digest
                or record.get("pid") != status.get("pid")
                or type(serial) is not int
                or serial < 1
                or type(index) is not int
                or index < 1
                or operation not in {"prepare", "seal"}
                or phase not in _PHASES
                or state not in {"BEGIN", "END", "FAILED"}
                or type(at) is not int
                or at < 1
            ):
                raise ValueError("invalid maintenance phase identity")
            identity = (serial, index, operation)
            key = (identity, phase)
            if phase == "operation" and state == "BEGIN":
                if serial in operations:
                    raise ValueError("duplicate maintenance operation serial")
                operations[serial] = identity
            elif operations.get(serial) != identity:
                raise ValueError("maintenance operation identity mismatch")
            if state == "BEGIN":
                if key in open_phases or key in completed_phases:
                    raise ValueError("duplicate maintenance phase begin")
                if phase != "operation" and (identity, "operation") not in open_phases:
                    raise ValueError("phase outside maintenance operation")
                open_phases[key] = at
            else:
                before = open_phases.pop(key, None)
                if before is None or at < before:
                    raise ValueError("unmatched or reversed maintenance phase")
                if phase == "operation" and any(k[0] == identity for k in open_phases):
                    raise ValueError("maintenance operation finished with open phase")
                completed_phases.add(key)
                if state == "FAILED":
                    failures += 1
                durations.setdefault(phase, []).append(at - before)
    if open_phases:
        raise ValueError("incomplete maintenance phase evidence")
    if len(operations) != len(durations.get("operation", [])):
        raise ValueError("incomplete maintenance operation evidence")
    return {
        "schema": "segment-maintenance-analysis-v1",
        "formal_qualification": False,
        "clock_identity_sha256": expected_clock,
        "records": count,
        "operations": len(operations),
        "failed_phases": failures,
        "phases": {
            phase: {"count": len(values), "max_ns": max(values), "total_ns": sum(values)}
            for phase, values in sorted(durations.items())
        },
        "limitations": ["maintenance trace alone does not prove request latency causation"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--clock-identity", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if len(args.clock_identity) != 64 or any(
        c not in "0123456789abcdef" for c in args.clock_identity
    ):
        raise ValueError("invalid clock identity")
    result = analyze(args.manifest, expected_clock=args.clock_identity)
    with args.output.open("x", encoding="utf-8") as target:
        json.dump(result, target, sort_keys=True, separators=(",", ":"))
        target.write("\n")
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
