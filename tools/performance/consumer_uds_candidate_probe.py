"""Short consumer-only persistent-candidate diagnostic; NOT L6/12h acceptance.

Open-loop SubmitSignal + fixed snapshot pages, fresh AF_UNIX connections,
streamed timings/HDR and explicit release/response errors. No server reset,
book access, native SDK call or infinite buffering. Used BEFORE long gates.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import struct
import time
from collections import Counter
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from hdrh.histogram import HdrHistogram
from perfbench.longrun_evidence import ConsumerEvidenceWriter, ConsumerTimingRecord
from perfbench.short_probe_contract import outcome_ledger, tool_revision
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json

_MAX_LOAD_WORKERS = 4_096
_WINDOW_COMPLETION_RESERVE_NS = 60_000_000


def valid_trading_day(value: str) -> bool:
    """Keep malformed load fixtures outside the server and its evidence set."""
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def request(operation, request_id, payload):
    return {
        "message_type": "request",
        "schema_version": 2,
        "operation": operation,
        "request_id": request_id,
        "payload": payload,
    }


def signal_payload(config, sequence, day):
    return {
        "consumer_account_id": config.consumer_account_id,
        "strategy_id": config.strategy_id + "-H",
        "strategy_order_id": f"LR-{sequence}",
        "trading_day": day,
        "symbol": "TEST",
        "market": "SIM",
        "side": "BUY",
        "price_type": "LIMIT",
        "business_type": "CASH",
        "quantity": 1,
        "protection_price_ticks": 1,
    }


async def wait_seed_projection(client, *, base, order_id, journal_id, lsn):
    """Non-pinning PUBLIC cold audit fence, outside the measurement window.

    Replay bounds may legitimately be a committed-memory prefix. GetOrder may
    also be hot. Neither proves the SQL snapshot descriptor covers the seed.
    The candidate audit port always verifies its indexed plan against source.
    """
    for attempt in range(100):
        result = client.call(
            request(
                "GetOrderAuditTrail",
                f"projection-fence-{base}-{attempt}",
                {"oms_order_id": order_id},
            )
        )
        payload = result.get("payload")
        trail = payload.get("audit_trail") if isinstance(payload, dict) else None
        if (
            result.get("status") == "COMPLETED"
            and isinstance(trail, list)
            and any(
                isinstance(row, dict)
                and row.get("oms_order_id") == order_id
                and row.get("journal_id") == journal_id
                and type(row.get("journal_lsn")) is int
                and row["journal_lsn"] >= lsn
                for row in trail
            )
        ):
            return
        await asyncio.sleep(0.05)
    raise ValueError("public seed audit projection fence did not become available")


def histogram_summary(h):
    return {
        "count": h.total_count,
        "p50_ns": h.get_value_at_percentile(50),
        "p95_ns": h.get_value_at_percentile(95),
        "p99_ns": h.get_value_at_percentile(99),
        "p999_ns": h.get_value_at_percentile(99.9),
        "p100_ns": h.get_max_value(),
    }


def diagnostic_checks(*, counts, histograms, planned_each, unissued, transport_failed):
    """Conservative SHORT-window checks, never the L6/12h qualification.

    Completing drain after T1 is not sustained-window throughput. Likewise a
    transport-success result must not hide generator queue/SLO violations.
    Missing metrics fail closed; merged HDRs are evaluated, not mean quantiles.
    """
    checks = {"transport": not transport_failed, "all_released": unissued == 0}
    for op, p95, p99 in (
        ("SubmitSignal", 20_000_000, 60_000_000),
        ("ReadOrderSnapshotPage", 10_000_000, 40_000_000),
    ):
        checks[f"{op}.offered"] = counts.get(f"{op}.offered", 0) == planned_each
        checks[f"{op}.completed"] = counts.get(f"{op}.completed", 0) == planned_each
        checks[f"{op}.completed_in_window"] = (
            counts.get(f"{op}.completed_in_window", 0) == planned_each
        )
        latency = histograms.get(f"{op}.scheduled", {})
        queue = histograms.get(f"{op}.generator_queue", {})
        checks[f"{op}.scheduled_slo"] = (
            latency.get("count") == planned_each
            and 0 < latency.get("p95_ns", 0) <= p95
            and 0 < latency.get("p99_ns", 0) <= p99
        )
        checks[f"{op}.generator_queue_slo"] = (
            queue.get("count") == planned_each and 0 < queue.get("p99_ns", 0) <= 10_000_000
        )
    return checks


def tick_count(tick: int, rate: int) -> int:
    if type(tick) is not int or tick < 0 or type(rate) is not int or not 1 <= rate <= 2000:
        raise ValueError("short-probe release budget is invalid")
    return ((tick + 1) * rate) // 100 - (tick * rate) // 100


def planned_tick_ns(
    start_ns: int,
    tick: int,
    total_ticks: int,
    *,
    completion_reserve_ns: int = _WINDOW_COMPLETION_RESERVE_NS,
) -> int:
    """Spread every offered tick inside T0..T1 while reserving response time.

    The old 100 Hz clock placed the final batch at T1-10 ms even though the
    frozen request SLO permits a 60 ms Submit P99. That made an otherwise
    conforming final request fail the stricter all-completed-by-T1 gate. The
    count and 60-second observation window remain unchanged; this function
    only makes the completion budget explicit and deterministic.
    """

    window_ns = total_ticks * 10_000_000
    if (
        type(start_ns) is not int
        or start_ns <= 0
        or type(tick) is not int
        or type(total_ticks) is not int
        or not 0 <= tick < total_ticks
        or total_ticks < 2
        or type(completion_reserve_ns) is not int
        or not 10_000_000 <= completion_reserve_ns < window_ns
    ):
        raise ValueError("invalid completion-aware release clock")
    release_span_ns = window_ns - completion_reserve_ns
    return start_ns + tick * release_span_ns // (total_ticks - 1)


class ExchangeProgress(tuple):
    """Three measured boundaries, plus a stable local failure code (no exception text)."""

    failure_code: int | None

    def __new__(cls, values: list[int], failure_code: int | None = None) -> "ExchangeProgress":
        result = super().__new__(cls, values)
        result.failure_code = failure_code
        return result


async def exchange(socket_path, frame):
    progress = [0, 0, 0]
    received_full = False

    async def invoke():
        nonlocal received_full
        writer = None
        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
            progress[0] = time.monotonic_ns()
            writer.write(frame)
            await writer.drain()
            progress[1] = time.monotonic_ns()
            header = await reader.readexactly(HEADER_SIZE_BYTES)
            size = struct.unpack_from(">I", header, 24)[0]
            if HEADER_SIZE_BYTES + size > MAX_FRAME_BYTES:
                raise ValueError("response exceeds frame budget")
            body = header + await reader.readexactly(size)
            received_full = True
            result = decode_frame(body)
            progress[2] = time.monotonic_ns()
            if result.header.correlation_id != struct.unpack_from(">Q", frame, 16)[0]:
                raise ValueError("response correlation mismatch")
            return dict(result.payload), ExchangeProgress(progress)
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    try:
        # One deadline/task boundary per request, not per socket await. Extra
        # wait_for tasks between connect and send created a measured client
        # scheduling gap after the server had already accepted the connection.
        return await asyncio.wait_for(invoke(), timeout=1)
    except Exception as error:
        # Preserve measured partial boundaries; never fabricate send or decode.
        if received_full or isinstance(error, ValueError):
            code = 102
        elif isinstance(error, asyncio.TimeoutError):
            code = 106 if progress[1] else 105 if progress[0] else 107
        else:
            code = 101 if progress[1] else 105 if progress[0] else 100
        return None, ExchangeProgress(progress, code)


async def wait_until_ns(planned: int) -> None:
    # Event loops may wake a timer slightly early. Observe the real clock again;
    # never release early and then clamp a negative queue delay into a success.
    delay = (planned - time.monotonic_ns()) / 1e9
    while delay > 0:
        await asyncio.sleep(delay)
        delay = (planned - time.monotonic_ns()) / 1e9


async def run_window(
    *,
    config,
    output: Path,
    duration: int,
    rate: int,
    workers: int,
    base: int,
    day: str,
    start_ns: int | None = None,
    fixture_only: bool = False,
):
    if (
        getattr(config, "venue_id", None) != "NO_ADAPTER_BENCHMARK"
        or getattr(config, "adapter_instance_id", None) != "disabled-no-dispatch"
        or type(duration) is not int
        or not 1 <= duration <= 60
        or type(rate) is not int
        or not 1 <= rate <= 2000
        or type(workers) is not int
        or not 1 <= workers <= _MAX_LOAD_WORKERS
        or type(base) is not int
        or not 0 <= base < 10**17
        or type(fixture_only) is not bool
    ):
        raise ValueError("short load probe requires bounded explicit no-Adapter scope")
    counts: Counter[str] = Counter()
    revision = tool_revision()
    histograms = {
        (op, metric): HdrHistogram(1, 60_000_000_000, 3)
        for op in ("SubmitSignal", "ReadOrderSnapshotPage")
        for metric in ("scheduled", "call", "generator_queue")
    }
    work: asyncio.Queue[Any] = asyncio.Queue(maxsize=workers * 2)
    client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path))
    # Prepare a small stable public dataset outside the measurement window.
    last_seed_lsn = 0
    last_seed_id = last_seed_journal = None
    for n in range(100):
        result = cast(
            dict[str, Any],
            client.call(
                request(
                    "SubmitSignal", f"seed-{base}-{n}", signal_payload(config, base + n, day)
                )
            ),
        )
        if result.get("status") != "ACCEPTED":
            raise ValueError("public seed was not accepted")
        last_seed_lsn = result.get("payload", {}).get("journal_lsn")
        if type(last_seed_lsn) is not int or last_seed_lsn < 1:
            raise ValueError("public seed lacks its durable position")
        last_seed_id = result.get("payload", {}).get("oms_order_id")
        last_seed_journal = result.get("payload", {}).get("journal_id")
        if not isinstance(last_seed_id, str) or not isinstance(last_seed_journal, str):
            raise ValueError("public seed lacks its durable identity")
    # Creating snapshots is not a readiness probe: successful partial snapshots
    # occupy immutable pins until TTL. Poll a non-pinning public read watermark
    # before allocating exactly ONE snapshot, including with concurrent shards.
    target = ConsumerEventPositionV2(RedisStreamIdV1(last_seed_lsn, 0), 0)
    await wait_seed_projection(
        client, base=base, order_id=last_seed_id, journal_id=last_seed_journal, lsn=last_seed_lsn
    )
    result = cast(
        dict[str, Any],
        client.call(
            request(
                "CreateOrderSnapshot",
                f"prepare-{base}",
                {"consumer_account_id": config.consumer_account_id, "page_size": 100},
            )
        ),
    )
    plan = result.get("payload")
    if (
        result.get("status") != "COMPLETED"
        or not isinstance(plan, dict)
        or plan.get("order_count", 0) < 100
        or not isinstance(plan.get("snapshot_position"), str)
        or ConsumerEventPositionV2.from_external_id(plan["snapshot_position"]) < target
    ):
        raise ValueError("public seed snapshot did not become available after projection fence")
    token = plan["page_cursor"]
    warmed = cast(
        dict[str, Any],
        client.call(
            request("ReadOrderSnapshotPage", f"warm-page-{base}", {"page_cursor": token})
        ),
    )
    warm_payload = warmed.get("payload")
    if (
        warmed.get("status") != "COMPLETED"
        or not isinstance(warm_payload, dict)
        or warm_payload.get("count") != 100
    ):
        raise ValueError("public snapshot page warmup did not complete")
    warm_body = cast(dict[str, Any], warm_payload)
    frozen_page = {
        name: warm_body[name]
        for name in (
            "snapshot_id",
            "consumer_account_id",
            "journal_id",
            "stream_generation",
            "snapshot_position",
            "count",
            "has_more",
            "next_page_cursor",
        )
    }
    warm_orders_digest = sha256(
        json.dumps(warm_body["orders"], sort_keys=True, separators=(",", ":")).encode()
    ).digest()
    raw = ConsumerEvidenceWriter(root=output / "raw")
    started = time.monotonic_ns() + 500_000_000 if start_ns is None else start_ns
    if started <= time.monotonic_ns():
        raw.close()
        raise ValueError("common start gate expired during public preparation")
    ends = started + duration * 1_000_000_000
    failed = asyncio.Event()
    actual_max: dict[str, int] = {}

    async def consume():
        while True:
            job = await work.get()
            try:
                if job is None:
                    return
                seq, planned, op = job
                request_id = f"probe-{base}-{op}-{seq}"
                payload = (
                    signal_payload(config, base + 100 + seq, day)
                    if op == "SubmitSignal"
                    else {"page_cursor": token}
                )
                frame = encode_frame(MessageKind.REQUEST, seq + 1, request(op, request_id, payload))
                call_ns = time.monotonic_ns()
                counts[f"{op}.offered"] += 1
                response, progress = await exchange(str(config.socket_path), frame)
                connected, sent, decoded = progress
                validated = time.monotonic_ns()
                status, result_lsn, result_digest, count = 0, 0, b"\0" * 32, 0
                if response is None:
                    status = getattr(progress, "failure_code", None) or (
                        102 if decoded else 101 if sent else 105 if connected else 100
                    )
                elif response.get("request_id") != request_id:
                    status = 102
                elif op == "SubmitSignal":
                    body = response.get("payload")
                    if response.get("status") != "ACCEPTED" or not isinstance(body, dict):
                        status = 103
                    else:
                        result_lsn = body["journal_lsn"]
                        result_digest = sha256(body["oms_order_id"].encode()).digest()
                        count = 1
                else:
                    body = response.get("payload")
                    if (
                        response.get("status") != "COMPLETED"
                        or not isinstance(body, dict)
                        or body.get("count") != 100
                    ):
                        status = 103
                    else:
                        count = 100
                        result_digest = sha256(
                            json.dumps(
                                body["orders"], sort_keys=True, separators=(",", ":")
                            ).encode()
                        ).digest()
                        if result_digest != warm_orders_digest or any(
                            body.get(name) != value for name, value in frozen_page.items()
                        ):
                            status = 104  # validated immutable page differs from its warm cut
                counts[f"{op}.result.{status}"] += 1
                if status == 0:
                    counts[f"{op}.completed"] += 1
                    if validated < ends:
                        counts[f"{op}.completed_in_window"] += 1
                    else:
                        counts[f"{op}.completed_after_window"] += 1
                    for name, elapsed in (
                        ("scheduled", validated - planned),
                        ("call", validated - call_ns),
                        ("generator_queue", call_ns - planned),
                    ):
                        if elapsed < 0 or elapsed > 60_000_000_000:
                            raise ValueError("histogram observation outside frozen range")
                        histograms[(op, name)].record_value(max(1, elapsed))
                        actual_max[f"{op}.{name}"] = max(actual_max.get(f"{op}.{name}", 0), elapsed)
                else:
                    failed.set()
                    code = None if response is None else response.get("code")
                    known = {
                        "RATE_LIMITED",
                        "INTERNAL_ERROR",
                        "SNAPSHOT_NOT_READY",
                        "CURSOR_EXPIRED",
                        "CAPABILITY_UNAVAILABLE",
                        "SCHEMA_VALIDATION_FAILED",
                        "AUDIT_EVIDENCE_UNAVAILABLE",
                    }
                    counts[
                        f"server_code.{code if isinstance(code, str) and code in known else 'OTHER'}"
                    ] += 1
                try:
                    raw.append(
                        ConsumerTimingRecord(
                            seq,
                            planned,
                            call_ns,
                            connected,
                            sent,
                            decoded,
                            validated,
                            ends,
                            result_lsn,
                            int(op != "SubmitSignal"),
                            1,
                            status,
                            count,
                            sha256(request_id.encode()).digest(),
                            result_digest,
                        )
                    )
                except Exception:
                    counts["evidence_write_errors"] += 1
                    raise
            except Exception:
                # An unexpected runner failure invalidates the run. It must not
                # be mislabeled as a server capacity or success measurement.
                counts["generator_unrecorded_error"] += 1
                failed.set()
            finally:
                work.task_done()

    tasks = [asyncio.create_task(consume()) for _ in range(workers)]
    generator_gc_enabled_before = gc.isenabled()
    generator_gc_collected_before = gc.collect()
    gc.disable()
    sequence = 0
    try:
        total_ticks = duration * 100
        for tick in range(total_ticks):
            if failed.is_set():
                break
            planned = planned_tick_ns(started, tick, total_ticks)
            await wait_until_ns(planned)
            for _ in range(tick_count(tick, rate)):
                for op in ("SubmitSignal", "ReadOrderSnapshotPage"):
                    counts[f"{op}.release_attempted"] += 1
                    try:
                        work.put_nowait((sequence, planned, op))
                        counts[f"{op}.enqueued"] += 1
                    except asyncio.QueueFull:
                        counts["release_errors"] += 1
                        counts[f"{op}.release_rejected"] += 1
                        failed.set()
                    sequence += 1
        await asyncio.wait_for(work.join(), timeout=60)
    finally:
        try:
            for _ in tasks:
                await work.put(None)
            await asyncio.gather(*tasks)
            raw.close()
        finally:
            if generator_gc_enabled_before:
                gc.enable()
    ledger = outcome_ledger(dict(counts), planned_each=duration * rate)
    result = {
        "schema": "consumer-persistent-candidate-short-probe-v1",
        "target_kind": "CONTROLLED_GENERATOR_FIXTURE" if fixture_only else "TRADECORE_PUBLIC_UDS",
        "formal_qualification": False,
        "profile": "fresh-per-request",
        "planned_each": duration * rate,
        "sequence_base": base,
        "start_ns": started,
        "end_ns": ends,
        "release_clock": {
            "schema": "completion-aware-100hz-v1",
            "completion_reserve_ns": _WINDOW_COMPLETION_RESERVE_NS,
            "first_release_ns": started,
            "last_release_ns": planned_tick_ns(started, total_ticks - 1, total_ticks),
            "observation_window_ns": duration * 1_000_000_000,
            "planned_each": duration * rate,
            "window_average_tps": rate,
        },
        "clock": "time.monotonic_ns",
        "tool_revision": revision,
        "clock_identity_sha256": monotonic_clock_identity(),
        "counts": dict(counts),
        "transport_failed": ledger["transport_failed"],
        "request_failed": ledger["request_failed"],
        "outcome_ledger": ledger,
        "unissued": ledger["unissued"],
        "not_attempted": 2 * duration * rate - sequence,
        "generator_gc": {
            "enabled_before": generator_gc_enabled_before,
            "collected_before": generator_gc_collected_before,
            "disabled_during_window": True,
            "enabled_after": gc.isenabled(),
        },
        "histograms": {
            f"{op}.{metric}": histogram_summary(h) for (op, metric), h in histograms.items()
        },
        "raw_max_ns": actual_max,
        "hdr_encoded": {
            f"{op}.{metric}": h.encode().decode("ascii") for (op, metric), h in histograms.items()
        },
        "snapshot_preparation": {
            "orders": plan["order_count"],
            "stale": plan.get("stale"),
            "projection_lag": plan.get("projection_lag"),
            "public_page_warmups": 1,
        },
        "limitations": [
            "short diagnostic only",
            "no rolling event checkpoint",
            "no server trace join or full SLO evaluator",
        ],
    }
    result["diagnostic_checks"] = diagnostic_checks(
        counts=result["counts"],
        histograms=result["histograms"],
        planned_each=result["planned_each"],
        unissued=result["unissued"],
        transport_failed=result["transport_failed"],
    )
    result["diagnostic_checks"]["ledger_conserved"] = ledger["conserved"]
    result["diagnostic_checks"]["clock_identity_known"] = (
        result["clock_identity_sha256"] is not None
    )
    result["diagnostic_slo_passed"] = all(result["diagnostic_checks"].values())
    result["failed"] = not result["diagnostic_slo_passed"]
    atomic_runtime_json(output / "report.json", result)
    return result


def main():
    import resource

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--sequence-base", type=int, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--cpus", help="Explicit CPU IDs for THIS generator only")
    args = parser.parse_args()
    if (
        not 1 <= args.duration <= 60
        or not 1 <= args.rate <= 2000
        or not 1 <= args.workers <= _MAX_LOAD_WORKERS
    ):
        parser.error("short-probe duration/rate/workers outside supported budget")
    if not 0 <= args.sequence_base < 10**17:
        parser.error("sequence namespace outside fixed mock contract")
    if not valid_trading_day(args.trading_day):
        parser.error("trading-day must be ISO YYYY-MM-DD")
    if args.cpus:
        if not all(v.isascii() and v.isdecimal() for v in args.cpus.split(",")):
            parser.error("generator CPU IDs must be explicit decimal IDs")
        os.sched_setaffinity(0, {int(v) for v in args.cpus.split(",")})
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard < 30000:
        parser.error("load generator RLIMIT_NOFILE hard limit is too small")
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(soft, 30000), hard))
    args.output.mkdir(mode=0o700, exist_ok=False)
    result = asyncio.run(
        run_window(
            config=load_consumer_uds_server_config_v2(args.config),
            output=args.output,
            duration=args.duration,
            rate=args.rate,
            workers=args.workers,
            base=args.sequence_base,
            day=args.trading_day,
            start_ns=args.start_ns,
        )
    )
    print(json.dumps(result, separators=(",", ":")))
    return 1 if result["failed"] or result["unissued"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
