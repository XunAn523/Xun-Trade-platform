"""Read-only report of actual server request timings; no client checkpoint claims."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

STAGES = {
    "frame_assembly": ("first_byte_observed_ns", "frame_ready_ns"),
    "ready_queue": ("frame_ready_ns", "dispatch_started_ns"),
    "session_and_decode": ("dispatch_started_ns", "frame_decoded_ns"),
    "schema_validation": ("frame_decoded_ns", "schema_validated_ns"),
    "cursor_authorization": ("schema_validated_ns", "cursor_authorized_ns"),
    "replay_read": ("cursor_authorized_ns", "replay_read_finished_ns"),
    "replay_validation": ("replay_read_finished_ns", "replay_validated_ns"),
    "cursor_sign": ("replay_validated_ns", "next_cursor_signed_ns"),
    "event_payload": ("next_cursor_signed_ns", "event_payload_built_ns"),
    "response_encode": ("handler_finished_ns", "response_encoded_ns"),
    "response_send": ("response_encoded_ns", "send_finished_ns"),
    "server_handling": ("frame_ready_ns", "send_finished_ns"),
}

# The lane scheduler must decode before enqueueing. Keep legacy records on
# their original layout instead of inventing times or sorting invalid pairs.
PRE_DISPATCH_STAGES = {
    name: pair
    for name, pair in STAGES.items()
    if name not in {"session_and_decode", "schema_validation", "ready_queue"}
}
PRE_DISPATCH_STAGES.update(
    {
        "frame_decode": ("frame_ready_ns", "frame_decoded_ns"),
        "ready_queue": ("frame_decoded_ns", "dispatch_started_ns"),
        "session_and_schema_validation": ("dispatch_started_ns", "schema_validated_ns"),
    }
)


def report(
    path: Path,
    *,
    operation: str = "PollEvents",
    start_ns: int = 0,
    end_ns: int = 2**63 - 1,
    expected_count: int | None = None,
) -> dict[str, Any]:
    status = json.loads(path.with_suffix(".status.json").read_text())
    rows = []
    all_count = 0
    for line in path.open(encoding="utf-8"):
        row = json.loads(line)
        all_count += 1
        completed = row.get("send_finished_ns", row.get("closed_ns", 0))
        if row.get("operation") == operation and start_ns <= completed < end_ns:
            rows.append(row)
    stage_values: dict[str, list[int]] = {name: [] for name in STAGES | PRE_DISPATCH_STAGES}
    for row in rows:
        layout = PRE_DISPATCH_STAGES if row.get("decode_before_dispatch") is True else STAGES
        for name, (begin, finish) in layout.items():
            if begin in row and finish in row:
                stage_values[name].append(row[finish] - row[begin])
    stages = {}
    for name, samples in stage_values.items():
        values = sorted(samples)
        stages[name] = {
            "count": len(values),
            "negative_pairs": sum(v < 0 for v in values),
            "milliseconds": {
                f"P{p}": values[math.ceil(len(values) * p / 100) - 1] / 1e6 if values else None
                for p in (50, 95, 99, 100)
            },
        }
    clean = (
        not status["writer_failed"]
        and status["queue_dropped"] == 0
        and status["capacity_dropped"] == 0
        and status["queue_depth"] == 0
        and status["submitted"] == status["written"] == all_count
    )
    coverage = (
        clean
        and expected_count is not None
        and len(rows) == expected_count
        and bool(rows)
        and all(r.get("outcome") == "sent" and r.get("response_kind") == "RESPONSE" for r in rows)
        and all(s["negative_pairs"] == 0 for s in stages.values())
    )
    return {
        "schema_version": "consumer-server-trace-report-v1",
        "operation": operation,
        "record_count": len(rows),
        "expected_count": expected_count,
        "trace_loss_free": clean,
        "request_count_coverage_pass": coverage,
        "coverage_note": "count only; join request_id_sha256 with client records before full acceptance",
        "checkpoint_evidence": False,
        "stages": stages,
        "writer_status": status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--operation", default="PollEvents")
    parser.add_argument("--start-ns", type=int, default=0)
    parser.add_argument("--end-ns", type=int, default=2**63 - 1)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = report(
        args.trace,
        operation=args.operation,
        start_ns=args.start_ns,
        end_ns=args.end_ns,
        expected_count=args.expected_count,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        # Never replace previous evidence.
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    main()
