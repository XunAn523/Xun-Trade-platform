"""Offline source-commit interval to consumer FULL-checkpoint join.

Default covers consumed events only; optional final status closes only the fresh
no-Adapter Mock source, NOT general producer/venue authority or MIX/12h
qualification. Private cursor DB is leased/read-only; tokens, order identities
and payloads never enter the bounded derived index or paired timing output.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import struct
from contextlib import closing
from hashlib import sha256
from pathlib import Path

from hdrh.histogram import HdrHistogram
from xuanling_tradecore.consumer.mock_lifecycle import MOCK_PRODUCER_MODEL
from xuanling_tradecore.domain import ConsumerEventPositionV2
from xuanling_tradecore.persistence.commit_timing import RECORD, verify_commit_timing
from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace
from xuanling_tradecore.persistence.runtime_files import (
    ExclusiveRuntimeLease,
    atomic_runtime_json,
    open_private,
    require_private_runtime_file,
)

PAIR = struct.Struct("<5Q")
MAXIMUM_RECORDS = 1_000_000


def _verify_multi_frontend_components(lifecycle: dict[str, object]) -> None:
    components = lifecycle.get("lifecycle_components")
    # Match the runtime publisher's finite topologies: 2..8 frontends,
    # acceptor/write broker/owner, one or two isolated read planes, and an
    # optional read broker. Direct READ omits the latter, never a read plane.
    if not isinstance(components, list) or not 6 <= len(components) <= 14:
        raise ValueError("multi-frontend lifecycle topology is unavailable")
    encoded = json.dumps(components, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if lifecycle.get("lifecycle_components_sha256") != sha256(encoded).hexdigest():
        raise ValueError("multi-frontend lifecycle topology hash changed")
    roles: list[str] = []
    process_pids: list[int] = []
    for component in components:
        if (
            not isinstance(component, dict)
            or not isinstance(component.get("role"), str)
            or component.get("kind") not in {"THREAD", "PROCESS"}
            or type(component.get("pid")) is not int
            or component["pid"] < 1
            or component.get("exit_code") != 0
            or type(component.get("exit_code")) is not int
            or component.get("shutdown_verified") is not True
        ):
            raise ValueError("multi-frontend lifecycle component is incomplete")
        roles.append(component["role"])
        if component["kind"] == "PROCESS":
            process_pids.append(component["pid"])
    frontends = sorted(role for role in roles if role.startswith("public-frontend-"))
    read_roles = set(roles) & {
        "owner-read-plane", "owner-replay-plane", "owner-snapshot-plane"
    }
    if read_roles not in (
        {"owner-read-plane"},
        {"owner-replay-plane", "owner-snapshot-plane"},
    ):
        raise ValueError("multi-frontend lifecycle read-plane role closure is incomplete")
    required = {
        "public-acceptor",
        "write-broker",
        "oms-owner",
        *read_roles,
        *frontends,
    }
    if "read-broker" in roles:
        required.add("read-broker")
    if (
        set(roles) != required
        or len(set(roles)) != len(roles)
        or len(set(process_pids)) != len(process_pids)
        or not 2 <= len(frontends) <= 8
        or frontends != [f"public-frontend-{index}" for index in range(len(frontends))]
    ):
        raise ValueError("multi-frontend lifecycle role closure is incomplete")


def _json(path, maximum=4 * 1024**2):
    require_private_runtime_file(path)
    if path.stat().st_size > maximum:
        raise ValueError("oversized join metadata")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("invalid join metadata")
    return value


def _segment_budget(root, entries, maximum_bytes):
    if not isinstance(entries, list) or len(entries) > 4096:
        raise ValueError("invalid short join segment budget")
    records = size = 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("invalid short join segment")
        count, length, name = entry.get("records"), entry.get("bytes"), entry.get("file")
        if (
            type(count) is not int
            or not 0 <= count <= MAXIMUM_RECORDS
            or type(length) is not int
            or not 0 <= length <= maximum_bytes
            or not isinstance(name, str)
            or Path(name).name != name
        ):
            raise ValueError("invalid short join segment metadata")
        path = root / name
        require_private_runtime_file(path)
        if path.stat().st_size != length:
            raise ValueError("short join segment size changed")
        records += count
        size += length
        if records > MAXIMUM_RECORDS or size > maximum_bytes:
            raise ValueError("short join segment budget exceeded")
    return records


def _fresh_mock_closure(source, status, pairs, consumer):
    """Narrow profile proof: fresh candidate, two complete two-fact batches/order.

    This is local Mock accounting only, never venue recovery or trading READY.
    The whole source must be projected, retired and durably consumed.
    """
    if (
        source.get("producer_model") != MOCK_PRODUCER_MODEL
        or status.get("mock_producer_model") != MOCK_PRODUCER_MODEL
    ):
        raise ValueError("source accounting is not the frozen Mock producer model")
    if any(
        status.get(name) is not wanted
        for name, wanted in (
            ("closed", True),
            ("drain_complete", True),
            ("writer_failed", False),
            ("status_provider_failed", False),
            ("candidate_only", True),
            ("full_qualification", False),
        )
    ):
        raise ValueError("candidate final trace is incomplete")
    if (
        any(
            type(status.get(k)) is not int or status[k] != 0
            for k in ("queue_dropped", "capacity_dropped")
        )
        or type(status.get("submitted")) is not int
        or status["submitted"] != status.get("written")
    ):
        raise ValueError("candidate final trace coverage failed")
    mock, journal = status.get("mock"), status.get("journal")
    projector, timing = status.get("projector"), status.get("source_commit_timing")
    if any(not isinstance(v, dict) for v in (mock, journal, projector, timing)):
        raise ValueError("missing candidate final accounting")
    orders = mock.get("retired")
    if (
        type(orders) is not int
        or orders < 1
        or mock.get("state") != "STOPPED"
        or mock.get("error_code") is not None
        or mock.get("native_calls") != 0
        or mock.get("pending_archives") != 0
        or any(type(mock.get(k)) is not int for k in ("native_calls", "pending_archives"))
        or any(type(mock.get(k)) is not int or mock[k] < 0 for k in ("filled", "rejected"))
        or mock["filled"] + mock["rejected"] != orders
    ):
        raise ValueError("mock lifecycle did not completely retire")
    if (
        source.get("first_lsn") != 1
        or source.get("observed_final_lsn") != orders * 4
        or pairs != orders * 2
        or source.get("written") != pairs
        or journal.get("journal_id") != source["journal_id"]
        or journal.get("next_lsn") != orders * 4 + 1
        or journal.get("maintenance_failed") is not False
        or journal.get("commit_timing_failed") is not False
        or projector.get("state") != "STOPPED"
        or projector.get("error_code") is not None
        or projector.get("projected_lsn") != orders * 4
        or projector.get("observed_committed_lsn") != orders * 4
        or timing.get("failed") is not False
        or timing.get("closed") is not True
        or timing.get("dropped") != 0
        or type(timing.get("dropped")) is not int
        or any(
            timing.get(k) != source.get(k)
            for k in ("written", "submitted", "observed_final_lsn", "first_lsn")
        )
    ):
        raise ValueError("fresh mock source/projection/timing totals do not close")
    (frontier,) = consumer.execute("SELECT frontier_position FROM meta WHERE id=1").fetchone()
    if consumer.execute("SELECT count(*) FROM parts WHERE done=0").fetchone() != (0,):
        raise ValueError("consumer signed partitions are not all checkpointed")
    expected = ConsumerEventPositionV2.from_external_id(frontier)
    if (
        expected.stream_id.redis_ms != orders * 4
        or expected.stream_id.redis_seq
        or expected.fact_ordinal
    ):
        raise ValueError("consumer durable frontier has not reached final source")
    return orders


def join_consumed(
    *,
    source_root: Path,
    checkpoint_root: Path,
    progress_root: Path,
    output: Path,
    final_status: Path | None = None,
    final_lifecycle: Path | None = None,
):
    if (final_status is None) != (final_lifecycle is None):
        raise ValueError("final status and post-runtime lifecycle proof are both required")
    source = _json(source_root / "manifest.json")
    count = source.get("written")
    if type(count) is not int or not 0 <= count <= MAXIMUM_RECORDS:
        raise ValueError("large-run join requires a separately reviewed capacity budget")
    if _segment_budget(source_root, source["segments"], MAXIMUM_RECORDS * RECORD.size) != count:
        raise ValueError("source segment/count budget mismatch")
    commit_count, _ = verify_commit_timing(source_root)
    progress = _json(progress_root / "report.json")
    clock = source.get("clock_identity_sha256")
    if (
        not isinstance(clock, str)
        or len(clock) != 64
        or any(c not in "0123456789abcdef" for c in clock)
        or clock != progress.get("clock_identity_sha256")
        or progress.get("clock") != "time.monotonic_ns"
        or progress.get("failed") is not False
    ):
        raise ValueError("unknown/mismatched clock or failed consumer evidence")
    trace_path = progress_root / "progress.manifest.json"
    trace = _json(trace_path)
    _segment_budget(progress_root, trace["segments"], 512 * 1024**2)
    rpc_count = verify_closed_request_trace(trace_path)
    if rpc_count > MAXIMUM_RECORDS or rpc_count != progress.get("requests"):
        raise ValueError("consumer progress count/budget mismatch")
    db_path = checkpoint_root / "consumer-progress.sqlite"
    lease_path = checkpoint_root / "consumer-owner.lock"
    require_private_runtime_file(db_path)
    require_private_runtime_file(lease_path)
    if db_path.stat().st_size > 64 * 1024**3 or output.exists() or output.is_symlink():
        raise ValueError("unsafe join input/output budget")
    output.mkdir(mode=0o700, exist_ok=False)
    index_path = output / "join-index.sqlite"
    os.close(open_private(index_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
    lower, upper = (HdrHistogram(1, 86_400_000_000_000, 3) for _ in range(2))
    with closing(ExclusiveRuntimeLease(lease_path)):
        consumer = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
        index = sqlite3.connect(index_path)
        try:
            consumer.execute("PRAGMA query_only=ON")
            consumer.execute("PRAGMA cache_size=-8192")
            consumer.execute("BEGIN")
            if consumer.execute("PRAGMA user_version").fetchone() != (1,):
                raise ValueError("unsupported private checkpoint schema")
            journal, expected = consumer.execute(
                "SELECT journal,unique_events FROM meta WHERE id=1"
            ).fetchone()
            if (
                journal != source["journal_id"]
                or type(expected) is not int
                or not 0 <= expected <= MAXIMUM_RECORDS
            ):
                raise ValueError("consumer/source identity or capacity mismatch")
            index.execute("PRAGMA cache_size=-8192")
            index.execute("PRAGMA synchronous=FULL")
            index.execute("PRAGMA max_page_count=131072")
            if index.execute("PRAGMA page_size").fetchone() != (4096,):
                raise ValueError("unexpected offline index page size")
            index.execute(
                "CREATE TABLE source(lsn INTEGER PRIMARY KEY,before_ns INTEGER,after_ns INTEGER,consumed INTEGER DEFAULT 0)"
            )
            index.execute(
                "CREATE TABLE pages(serial INTEGER PRIMARY KEY,checkpoint_ns INTEGER,expected INTEGER,seen INTEGER DEFAULT 0)"
            )
            for entry in source["segments"]:
                with (source_root / entry["file"]).open("rb") as raw:
                    batch = []
                    source_digest = sha256()
                    while body := raw.read(RECORD.size):
                        source_digest.update(body)
                        _, _, last, before, after, _ = RECORD.unpack(body)
                        if max(last, before, after) >= 2**63:
                            raise ValueError("join position/time exceeds signed storage")
                        batch.append((last, before, after))
                        if len(batch) == 512:
                            with index:
                                index.executemany(
                                    "INSERT INTO source(lsn,before_ns,after_ns) VALUES(?,?,?)",
                                    batch,
                                )
                            batch.clear()
                    if batch:
                        with index:
                            index.executemany(
                                "INSERT INTO source(lsn,before_ns,after_ns) VALUES(?,?,?)", batch
                            )
                    if source_digest.hexdigest() != entry["sha256"]:
                        raise ValueError("source timing changed while joining")
            pages, page_events = 0, 0
            for entry in trace["segments"]:
                with (progress_root / entry["file"]).open("rb") as raw:
                    progress_digest = sha256()
                    for line in raw:
                        progress_digest.update(line)
                        row = json.loads(line)
                        count = row.get("unique_checkpointed_events", 0)
                        if not count:
                            continue
                        serial, checkpoint = (
                            row.get("page_serial"),
                            row.get("progress_confirmed_ns"),
                        )
                        returned, called = (
                            row.get("response_returned_ns"),
                            row.get("call_started_ns"),
                        )
                        if (
                            row.get("result") != "COMPLETED"
                            or row.get("operation") != "PollEvents"
                            or any(
                                type(v) is not int
                                for v in (serial, checkpoint, count, returned, called)
                            )
                            or not 1 <= count <= 100
                            or serial < 1
                            or not 0 < called <= returned <= checkpoint
                        ):
                            raise ValueError("invalid checkpoint observation")
                        index.execute(
                            "INSERT INTO pages(serial,checkpoint_ns,expected) VALUES(?,?,?)",
                            (serial, checkpoint, count),
                        )
                        pages += 1
                        page_events += count
                        if pages > MAXIMUM_RECORDS:
                            raise ValueError("progress page budget exceeded")
                    if progress_digest.hexdigest() != entry["sha256"]:
                        raise ValueError("progress timing changed while joining")
            index.commit()
            if page_events != expected:
                raise ValueError(
                    "missing progress pages; resume requires complete earlier evidence"
                )
            pairs = 0
            digest = sha256()
            with os.fdopen(
                open_private(output / "paired.bin", os.O_CREAT | os.O_EXCL | os.O_WRONLY), "wb"
            ) as target:
                for position, serial in consumer.execute(
                    "SELECT position,page_serial FROM events ORDER BY rowid"
                ):
                    p = ConsumerEventPositionV2.from_external_id(position)
                    if p.stream_id.redis_seq or p.fact_ordinal:
                        raise ValueError("join requires canonical mmap positions")
                    event_lsn = p.stream_id.redis_ms
                    timing = index.execute(
                        "SELECT before_ns,after_ns FROM source WHERE lsn=?", (event_lsn,)
                    ).fetchone()
                    page = index.execute(
                        "SELECT checkpoint_ns FROM pages WHERE serial=?", (serial,)
                    ).fetchone()
                    if (
                        timing is None
                        or page is None
                        or page[0] < timing[1]
                        or page[0] - timing[0] > 86_400_000_000_000
                    ):
                        raise ValueError("missing/reversed/out-of-budget source/checkpoint pair")
                    pairs += 1
                    if (
                        index.execute(
                            "UPDATE source SET consumed=1 WHERE lsn=? AND consumed=0", (event_lsn,)
                        ).rowcount
                        != 1
                    ):
                        raise ValueError("duplicate source event position in checkpoint evidence")
                    if pairs > MAXIMUM_RECORDS:
                        raise ValueError("pair output budget exceeded")
                    body = PAIR.pack(pairs, event_lsn, timing[0], timing[1], page[0])
                    target.write(body)
                    digest.update(body)
                    lower.record_value(max(1, page[0] - timing[1]))
                    upper.record_value(max(1, page[0] - timing[0]))
                    index.execute("UPDATE pages SET seen=seen+1 WHERE serial=?", (serial,))
                target.flush()
                os.fsync(target.fileno())
            if pairs != expected or index.execute(
                "SELECT count(*) FROM pages WHERE expected!=seen"
            ).fetchone() != (0,):
                raise ValueError("consumer page/event coverage mismatch")
            index.commit()
            closed_orders = None
            if final_status is not None:
                if final_lifecycle is None:
                    raise ValueError("missing final runtime lifecycle")
                lifecycle = _json(final_lifecycle)
                if (
                    lifecycle.get("schema")
                    not in {
                        "consumer-candidate-final-lifecycle-v2",
                        "consumer-process-owner-lifecycle-v1",
                        "consumer-multi-frontend-lifecycle-v1",
                    }
                    or lifecycle.get("runtime_closed") is not True
                    or lifecycle.get("all_managed_workers_exited_zero") is not True
                    or lifecycle.get("all_owned_descendants_exited_zero") is not True
                    or lifecycle.get("owned_helper_exit_status") != "VERIFIED"
                    or lifecycle.get("formal_qualification") is not False
                    or lifecycle.get("producer_model") != MOCK_PRODUCER_MODEL
                    or lifecycle.get("journal_id") != source["journal_id"]
                    or lifecycle.get("final_lsn") != source["observed_final_lsn"]
                    or lifecycle.get("trace_status_sha256")
                    != sha256(final_status.read_bytes()).hexdigest()
                    or lifecycle.get("source_timing_manifest_sha256")
                    != sha256((source_root / "manifest.json").read_bytes()).hexdigest()
                ):
                    raise ValueError("final runtime lifecycle/hash coverage is unavailable")
                if lifecycle.get("schema") == "consumer-multi-frontend-lifecycle-v1":
                    _verify_multi_frontend_components(lifecycle)
                helper = lifecycle.get("helper_exit")
                if (
                    not isinstance(helper, dict)
                    or helper.get("schema") != "candidate-owned-helper-exit-v1"
                    or helper.get("helper_kind") != "cpython310-resource-tracker"
                    or helper.get("ownership_verified") is not True
                    or helper.get("forced_kill") is not False
                    or helper.get("exit_code") != 0
                    or type(helper.get("exit_code")) is not int
                    or helper.get("remaining_descendants") != 0
                    or type(helper.get("remaining_descendants")) is not int
                    or helper.get("clock_identity_sha256") != clock
                    or any(
                        type(helper.get(key)) is not int or helper[key] < 1
                        for key in ("pid", "parent_pid", "start_ticks", "started_ns", "exited_ns")
                    )
                    or helper["pid"] == helper["parent_pid"]
                    or helper["started_ns"] > helper["exited_ns"]
                    or type(lifecycle.get("observed_monotonic_ns")) is not int
                    or helper["exited_ns"] > lifecycle["observed_monotonic_ns"]
                ):
                    raise ValueError("final runtime owned-helper exit evidence is unavailable")
                closed_orders = _fresh_mock_closure(source, _json(final_status), pairs, consumer)
        finally:
            consumer.close()
            index.close()
    report = {
        "schema": "consumer-source-clock-consumed-join-v1",
        "failed": False,
        "formal_qualification": False,
        "producer_closure_status": "MATCHED_FRESH_MOCK_SOURCE"
        if closed_orders is not None
        else "NOT_EVALUATED",
        "coverage": "FRESH_MOCK_SOURCE_ONLY"
        if closed_orders is not None
        else "CONSUMED_EVENTS_ONLY",
        "closed_mock_orders": closed_orders,
        "matched_events": pairs,
        "checkpoint_pages": pages,
        "source_batches": commit_count,
        "clock_identity_sha256": clock,
        "paired_record_bytes": PAIR.size,
        "paired_bytes": pairs * PAIR.size,
        "paired_sha256": digest.hexdigest(),
        "latency_bounds_ns": {
            name: {
                "count": h.total_count,
                **{f"p{p}": h.get_value_at_percentile(p) for p in (50, 95, 99, 100)},
            }
            for name, h in (("lower_after_commit_observed", lower), ("upper_before_append", upper))
        },
        "limitations": [
            "without final status only consumed events; closure option is fresh no-Adapter Mock only",
            "not full MIX/12h or throughput qualification",
            "commit time is an interval, not physical SSD durability",
            "single-run checkpoint/progress bundle; maximum one million records",
        ],
    }
    atomic_runtime_json(output / "report.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-root", "checkpoint-root", "progress-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument(
        "--final-status",
        type=Path,
        help="Optional closed candidate trace status for fresh Mock closure",
    )
    parser.add_argument(
        "--final-lifecycle", type=Path, help="Post-runtime verified child-exit marker"
    )
    args = parser.parse_args()
    try:
        result = join_consumed(
            source_root=args.source_root,
            checkpoint_root=args.checkpoint_root,
            progress_root=args.progress_root,
            output=args.output,
            final_status=args.final_status,
            final_lifecycle=args.final_lifecycle,
        )
    except Exception:
        print(
            json.dumps(
                {
                    "failed": True,
                    "error_code": "SOURCE_CHECKPOINT_JOIN_UNAVAILABLE",
                    "formal_qualification": False,
                }
            )
        )
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
