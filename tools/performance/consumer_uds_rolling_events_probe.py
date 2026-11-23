"""Short public-UDS rolling event/checkpoint diagnostic, NOT L6/12h qualification.

No reset, broker, private server book or cursor manipulation. Raw progress
timings and token-free reports are separate from the private durable cursor DB.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from hashlib import sha256
from pathlib import Path

from hdrh.histogram import HdrHistogram
from perfbench.consumer_event_checkpoint import (
    ConcurrentRollingConsumerEventProbe,
    ConsumerEventCheckpoint,
    RollingConsumerEventProbe,
)
from perfbench.short_probe_contract import common_start, tool_revision
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.domain import ConsumerEventPositionV2
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity
from xuanling_tradecore.persistence.rotating_request_trace import RotatingRequestTraceFile
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json


class ObservedClient:
    """Only real call/return boundaries; no invented connect/send timestamps."""

    def __init__(self, client):
        self.client, self.last = client, None
        self.sequence = 0

    def call(self, request):
        self.sequence += 1
        self.last = {
            "trace_sequence": self.sequence,
            "operation": request["operation"],
            "request_id_sha256": sha256(request["request_id"].encode()).hexdigest(),
            "call_started_ns": time.monotonic_ns(),
            "response_returned_ns": 0,
            "progress_confirmed_ns": 0,
            "unique_checkpointed_events": 0,
            "replayed_events": 0,
            "page_serial": 0,
            "result": "FAILED",
        }
        try:
            result = self.client.call(request)
        except Exception:
            self.last["failure_observed_ns"] = time.monotonic_ns()
            raise
        self.last["response_returned_ns"] = time.monotonic_ns()
        if result.get("message_type") == "error":
            code = result.get("code")
            self.last["server_error_code"] = (
                code
                if isinstance(code, str)
                and code
                in {
                    "CURSOR_INVALID",
                    "CURSOR_EXPIRED",
                    "CURSOR_RECOVERY_UNAVAILABLE",
                    "JOURNAL_GAP",
                    "INTERNAL_ERROR",
                    "RATE_LIMITED",
                    "UNAUTHORIZED",
                    "CAPABILITY_UNAVAILABLE",
                }
                else "UNRECOGNIZED_CODE"
            )
        return result


def _drain_target_reached(checkpoint, target_lsn):
    # A target is only a stop condition, never permission to create/change a
    # cursor. Only the FULL-checkpointed signed frontier can satisfy it.
    if checkpoint.pending():
        return False
    _, position = checkpoint.frontier()
    if position is None:
        return False
    value = ConsumerEventPositionV2.from_external_id(position)
    return (
        value.stream_id.redis_seq == value.fact_ordinal == 0
        and value.stream_id.redis_ms >= target_lsn
    )


def run_probe(
    *,
    config,
    output,
    checkpoint_root,
    initialize,
    duration,
    client=None,
    drain_through_lsn=None,
    drain_timeout_seconds=30,
    start_ns=None,
    partition_concurrency=1,
):
    if (
        type(duration) is not int
        or not 1 <= duration <= 60
        or config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
        or (
            drain_through_lsn is not None
            and (type(drain_through_lsn) is not int or not 1 <= drain_through_lsn <= 2**63 - 1)
        )
        or type(drain_timeout_seconds) is not int
        or not 1 <= drain_timeout_seconds <= 60
        or type(partition_concurrency) is not int
        or partition_concurrency not in {1, 2, 4}
    ):
        raise ValueError("short event probe requires an explicit bounded no-Adapter scope")
    scheduled = (
        common_start(now_ns=time.monotonic_ns(), supplied_ns=start_ns)
        if start_ns is not None
        else None
    )
    revision = tool_revision()
    output.mkdir(mode=0o700, exist_ok=False)
    scope = sha256(
        json.dumps(
            {
                "consumer_id": config.consumer_id,
                "consumer_account_id": config.consumer_account_id,
                "venue": config.venue_id,
                "adapter": config.adapter_instance_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    checkpoint = ConsumerEventCheckpoint(
        root=checkpoint_root, scope_sha256=scope, initialize=initialize
    )
    sink = serial_probe = concurrent_probe = None
    failed, error_code = False, None
    written = in_window = replayed = requests = 0
    measured_requests = drain_requests = completed_requests_in_window = 0
    histograms = {}
    initial = checkpoint.stats()["unique_checkpointed_events"]
    started = time.monotonic_ns() if scheduled is None else scheduled
    ends = started + duration * 1_000_000_000
    drain_started = drain_finished = None
    drain_reached = False
    drain_deadline = ends + drain_timeout_seconds * 1_000_000_000
    observed = ObservedClient(
        client
        if client is not None
        else ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, timeout_seconds=1))
    )
    try:
        sink = RotatingRequestTraceFile(
            output / "progress.jsonl",
            journal_id="consumer-progress-" + uuid.uuid4().hex,
            segment_bytes=16 * 1024**2,
        )
        if partition_concurrency == 1:
            serial_probe = RollingConsumerEventProbe(
                client=observed,
                checkpoint=checkpoint,
                consumer_account_id=config.consumer_account_id,
            )
        else:
            concurrent_probe = ConcurrentRollingConsumerEventProbe(
                client=observed.client,
                checkpoint=checkpoint,
                consumer_account_id=config.consumer_account_id,
                maximum_inflight=partition_concurrency,
            )
        while time.monotonic_ns() < started:
            time.sleep(min(0.01, max(0, started - time.monotonic_ns()) / 1_000_000_000))
        while True:
            now = time.monotonic_ns()
            if now >= ends:
                if drain_through_lsn is None:
                    break
                if drain_started is None:
                    drain_started = now
                if now >= drain_deadline:
                    failed, error_code = True, "FINAL_EVENT_DRAIN_TIMEOUT"
                    drain_finished = now
                    break
                if _drain_target_reached(checkpoint, drain_through_lsn):
                    drain_reached = True
                    drain_finished = now
                    break
            observed.last = None
            rows = []
            no_progress = True
            try:
                if partition_concurrency == 1:
                    assert serial_probe is not None
                    page = serial_probe.step()
                    row = observed.last
                    if row is None:
                        raise ValueError("step did not make its expected public RPC")
                    row["progress_confirmed_ns"] = (
                        time.monotonic_ns() if page is None else page.checkpointed_ns
                    )
                    row["result"] = "COMPLETED"
                    if page is not None:
                        row.update(
                            unique_checkpointed_events=page.unique_events,
                            replayed_events=page.replayed_events,
                            page_serial=page.page_serial,
                        )
                    rows.append(row)
                    no_progress = page is None
                else:
                    assert concurrent_probe is not None
                    pages = concurrent_probe.step()
                    for call in concurrent_probe.last_calls:
                        rows.append(
                            {
                                "trace_sequence": requests + len(rows) + 1,
                                "operation": call.operation,
                                "request_id_sha256": sha256(
                                    call.request_id.encode()
                                ).hexdigest(),
                                "call_started_ns": call.started_ns,
                                "response_returned_ns": call.returned_ns,
                                "progress_confirmed_ns": call.checkpointed_ns,
                                "unique_checkpointed_events": call.unique_events,
                                "replayed_events": call.replayed_events,
                                "page_serial": call.page_serial,
                                "result": "COMPLETED",
                            }
                        )
                    no_progress = not pages
                for row in rows:
                    row["phase"] = (
                        "MEASURE" if row["call_started_ns"] < ends else "FINAL_DRAIN"
                    )
                    if row["unique_checkpointed_events"]:
                        if checkpoint.last_page_stages is not None:
                            row["checkpoint_stages"] = dict(checkpoint.last_page_stages)
                        replayed += row["replayed_events"]
                        if row["progress_confirmed_ns"] < ends:
                            in_window += row["unique_checkpointed_events"]
                    for metric, elapsed in (
                        ("call", row["response_returned_ns"] - row["call_started_ns"]),
                        (
                            "call_to_progress",
                            row["progress_confirmed_ns"] - row["call_started_ns"],
                        ),
                    ):
                        if not 0 <= elapsed <= 60_000_000_000:
                            raise ValueError(
                                "event diagnostic observation outside histogram range"
                            )
                        key = row["operation"] + "." + metric
                        if row["phase"] == "FINAL_DRAIN":
                            key = "FINAL_DRAIN." + key
                        if key not in histograms:
                            histograms[key] = HdrHistogram(1, 60_000_000_000, 3)
                        histograms[key].record_value(max(1, elapsed))
            except Exception:
                failed, error_code = True, "PUBLIC_RPC_OR_CHECKPOINT_FAILED"
                if observed.last is not None:
                    observed.last["result"] = "FAILED"
                    observed.last["failure_code"] = error_code
                    rows.append(observed.last)
            for row in rows:
                row.setdefault("phase", "MEASURE" if row["call_started_ns"] < ends else "FINAL_DRAIN")
                requests += 1
                if row["phase"] == "MEASURE":
                    measured_requests += 1
                else:
                    drain_requests += 1
                if row["result"] == "COMPLETED" and 0 < row["progress_confirmed_ns"] < ends:
                    completed_requests_in_window += 1
                size = sink.append_record(row, maximum_remaining=2 * 1024**3 - written)
                if not size:
                    failed, error_code = True, "EVENT_PROGRESS_EVIDENCE_QUOTA"
                written += size
            if failed:
                break
            # No new frozen interval is available: bounded idle polling, not
            # an unbounded BEGIN replay or a busy loop. Background producers
            # are never paused, and existing partitions have no artificial wait.
            if no_progress and not checkpoint.pending():
                deadline = ends if time.monotonic_ns() < ends else drain_deadline
                time.sleep(min(0.05, max(0, deadline - time.monotonic_ns()) / 1e9))
    except Exception:
        failed, error_code = True, "EVENT_PROGRESS_EVIDENCE_FAILED"
    finally:
        if concurrent_probe is not None:
            concurrent_probe.close()
        try:
            stats = checkpoint.stats()
        except Exception:
            stats = {"checkpoint_state_available": False}
        try:
            checkpoint.close()
        except Exception:
            failed, error_code = True, "EVENT_CHECKPOINT_CLOSE_FAILED"
        if sink is not None:
            try:
                sink.close()
            except Exception:
                failed, error_code = True, "EVENT_PROGRESS_CLOSE_FAILED"
    report = {
        "schema": "consumer-rolling-events-short-probe-v1",
        "target_kind": "TRADECORE_PUBLIC_UDS",
        "formal_qualification": False,
        "performance_status": "NOT_EVALUATED",
        "producer_join_status": "NOT_MEASURED",
        "clock": "time.monotonic_ns",
        "tool_revision": revision,
        "clock_identity_sha256": monotonic_clock_identity(),
        "failed": failed,
        "error_code": error_code,
        "native_calls": 0,
        "partition_concurrency": partition_concurrency,
        "start_ns": started,
        "end_ns": ends,
        "finished_monotonic_ns": time.monotonic_ns(),
        "final_drain": {
            "requested": drain_through_lsn is not None,
            "target_lsn": drain_through_lsn,
            "target_is_authority": False,
            "timeout_seconds": drain_timeout_seconds if drain_through_lsn is not None else None,
            "started_ns": drain_started,
            "finished_ns": drain_finished,
            "reached": drain_reached if drain_through_lsn is not None else None,
            "unique_after_window": (
                stats["unique_checkpointed_events"] - initial - in_window
                if "unique_checkpointed_events" in stats
                else None
            ),
        },
        "requests": requests,
        "requests_started_in_window": measured_requests,
        "requests_completed_in_window": completed_requests_in_window,
        "requests_after_window": drain_requests,
        "unique_checkpointed_in_window": in_window,
        "replayed_events_not_counted": replayed,
        "new_unique_checkpointed_events": stats.get("unique_checkpointed_events", initial)
        - initial,
        "private_checkpoint": stats,
        "progress_evidence_bytes": written,
        "histograms": {
            key: {
                "count": h.total_count,
                "p50_ns": h.get_value_at_percentile(50),
                "p95_ns": h.get_value_at_percentile(95),
                "p99_ns": h.get_value_at_percentile(99),
                "p100_ns": h.get_max_value(),
            }
            for key, h in histograms.items()
        },
        "limitations": [
            "short component diagnostic only",
            "no production timestamp/lag proof",
            "drain target is only a stop condition; offline source total/lifecycle join still required",
            "not full MIX/12h qualification",
        ],
    }
    atomic_runtime_json(output / "report.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--drain-through-lsn", type=int)
    parser.add_argument("--drain-timeout-seconds", type=int, default=30)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--partition-concurrency", type=int, choices=(1, 2, 4), default=1)
    args = parser.parse_args()
    values = args.cpus.split(",")
    if not all(v.isascii() and v.isdecimal() for v in values):
        parser.error("explicit existing CPU IDs required")
    cpus = {int(v) for v in values}
    if not cpus <= os.sched_getaffinity(0):
        parser.error("event observer CPU is outside its existing cgroup grant")
    os.sched_setaffinity(0, cpus)
    report = run_probe(
        config=load_consumer_uds_server_config_v2(args.config),
        output=args.output,
        checkpoint_root=args.checkpoint_root,
        initialize=not args.resume,
        duration=args.duration,
        drain_through_lsn=args.drain_through_lsn,
        drain_timeout_seconds=args.drain_timeout_seconds,
        start_ns=args.start_ns,
        partition_concurrency=args.partition_concurrency,
    )
    print(json.dumps(report, separators=(",", ":")))
    return int(report["failed"])


if __name__ == "__main__":
    raise SystemExit(main())
