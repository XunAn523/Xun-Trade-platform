"""Bounded outcome ledger and absolute clock contract for short public probes."""

from __future__ import annotations

import subprocess
from pathlib import Path

OPERATIONS = ("SubmitSignal", "ReadOrderSnapshotPage")
RESULTS = {
    0: "SUCCESS",
    100: "CONNECT_FAILED",
    101: "POST_SEND_NO_RESPONSE",
    102: "RESPONSE_INVALID",
    103: "BUSINESS_REJECTED",
    104: "CONTENT_MISMATCH",
    105: "SEND_UNCONFIRMED",
    106: "RESPONSE_TIMEOUT",
    107: "PRE_SEND_TIMEOUT",
}
TRANSPORT_RESULTS = frozenset({100, 101, 105, 106, 107})


def tool_revision() -> str | None:
    """Checkout identity only, NOT installed service payload identity."""
    try:
        result = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[3]), "rev-parse", "HEAD"],
            capture_output=True,
            timeout=5,
            check=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    return result if len(result) == 40 and all(c in "0123456789abcdef" for c in result) else None


def outcome_ledger(counts: dict, *, planned_each: int) -> dict:
    if (
        type(planned_each) is not int
        or planned_each < 1
        or any(not isinstance(key, str) for key in counts)
        or any(type(value) is not int or value < 0 for value in counts.values())
    ):
        raise ValueError("invalid probe count ledger")
    operations = {}
    for op in OPERATIONS:
        outcomes = {RESULTS[code]: counts.get(f"{op}.result.{code}", 0) for code in RESULTS}
        offered = counts.get(f"{op}.offered", 0)
        enqueued = counts.get(f"{op}.enqueued", 0)
        rejected = counts.get(f"{op}.release_rejected", 0)
        attempted = counts.get(f"{op}.release_attempted", 0)
        completed = counts.get(f"{op}.completed", 0)
        in_window = counts.get(f"{op}.completed_in_window", 0)
        late = counts.get(f"{op}.completed_after_window", 0)
        unissued = planned_each - offered
        known_results = {f"{op}.result.{code}" for code in RESULTS}
        unknown = any(
            key.startswith(f"{op}.result.") and key not in known_results for key in counts
        )
        conserved = (
            not unknown
            and f"{op}.release_attempted" in counts
            and 0 <= offered <= enqueued <= attempted <= planned_each
            and enqueued + rejected == attempted
            and sum(outcomes.values()) == offered
            and completed == outcomes["SUCCESS"] == in_window + late
            and counts.get("generator_unrecorded_error", 0) == 0
            and counts.get("evidence_write_errors", 0) == 0
        )
        operations[op] = {
            "planned": planned_each,
            "release_attempted": attempted,
            "enqueued": enqueued,
            "offered": offered,
            "release_rejected": rejected,
            "not_attempted": planned_each - attempted,
            "enqueued_not_started": enqueued - offered,
            "unissued": unissued,
            "completed_in_window": in_window,
            "completed_after_window": late,
            "outcomes": outcomes,
            "conserved": conserved,
        }
    return {
        "schema": "short-probe-outcome-ledger-v1",
        "operations": operations,
        "conserved": all(row["conserved"] for row in operations.values()),
        "unissued": sum(row["unissued"] for row in operations.values()),
        "transport_failed": any(
            counts.get(f"{op}.result.{code}", 0) for op in OPERATIONS for code in TRANSPORT_RESULTS
        ),
        "request_failed": any(
            counts.get(f"{op}.result.{code}", 0) for op in OPERATIONS for code in RESULTS if code
        ),
    }


def common_start(*, now_ns: int, supplied_ns: int | None, lead_seconds: int = 10) -> int:
    if type(now_ns) is not int or now_ns <= 0 or type(lead_seconds) is not int:
        raise ValueError("invalid short-probe start clock")
    if supplied_ns is None:
        if not 5 <= lead_seconds <= 60:
            raise ValueError("short-probe start lead outside budget")
        return now_ns + lead_seconds * 1_000_000_000
    if type(supplied_ns) is not int or not now_ns < supplied_ns <= now_ns + 60_000_000_000:
        raise ValueError("common start must be within the next sixty seconds")
    return supplied_ns


def compatible_clocks(reports: list[dict]) -> str | None:
    clocks = {report.get("clock_identity_sha256") for report in reports}
    if len(clocks) != 1:
        raise ValueError("short-probe clock identities differ")
    clock = clocks.pop()
    if clock is not None and (
        not isinstance(clock, str)
        or len(clock) != 64
        or any(value not in "0123456789abcdef" for value in clock)
    ):
        raise ValueError("invalid short-probe clock identity")
    return clock


def common_window(load: dict, events: dict) -> dict:
    """Validate frozen boundaries only; never infer source/FULL closure from RPCs."""
    clock = compatible_clocks([load, events])
    start, end = load.get("start_ns"), load.get("end_ns")
    revision = load.get("tool_revision")
    if (
        clock is None
        or type(start) is not int
        or type(end) is not int
        or not 0 < start < end <= start + 60_000_000_000
        or events.get("start_ns") != start
        or events.get("end_ns") != end
        or load.get("formal_qualification") is not False
        or events.get("formal_qualification") is not False
        or load.get("clock") != "time.monotonic_ns"
        or events.get("clock") != "time.monotonic_ns"
        or not isinstance(revision, str)
        or len(revision) != 40
        or any(c not in "0123456789abcdef" for c in revision)
        or events.get("tool_revision") != revision
        or load.get("target_kind") != "TRADECORE_PUBLIC_UDS"
        or events.get("target_kind") != "TRADECORE_PUBLIC_UDS"
    ):
        raise ValueError("missing or incompatible common measurement window")
    return {
        "schema": "consumer-common-short-window-v1",
        "start_ns": start,
        "end_ns": end,
        "clock_identity_sha256": clock,
        "window_contract_valid": True,
        "tool_revision": revision,
        "formal_qualification": False,
        "producer_join_status": "NOT_MEASURED",
        "performance_status": "NOT_EVALUATED",
    }
