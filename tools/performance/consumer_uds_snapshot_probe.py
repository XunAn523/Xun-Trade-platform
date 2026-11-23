"""Controlled large-account Consumer V2 paged-snapshot load probe.

This is a server-local, no-Adapter probe. It measures fixed-version page RPCs;
it does not claim SubmitSignal, PollEvents checkpoint, or mixed S5 acceptance.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import struct
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from time import monotonic_ns
from typing import Any

from xuanling_tradecore.consumer.client_v2 import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
)
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)


@dataclass(frozen=True)
class _Settings:
    socket_path: Path
    token: str
    run_id: str
    round_id: str
    duration_seconds: int
    target_qps: int
    workers: int
    expected_page_count: int


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _bounded_queue_capacity(workers: int, batch: int) -> int:
    if workers <= 0 or batch <= 0:
        raise ValueError("worker and batch counts must be positive")
    return max(batch, workers * 2)


async def _exchange(
    *,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    settings: _Settings,
    sequence: int,
) -> tuple[bool, int]:
    request = {
        "message_type": "request",
        "schema_version": 2,
        "operation": "ReadOrderSnapshotPage",
        "request_id": f"s5-{settings.round_id}-{settings.run_id}-{sequence}",
        "payload": {"page_cursor": settings.token},
    }
    writer.write(encode_frame(MessageKind.REQUEST, sequence + 1, request))
    await writer.drain()
    header = await reader.readexactly(HEADER_SIZE_BYTES)
    length = struct.unpack_from(">I", header, 24)[0]
    response = decode_frame(header + await reader.readexactly(length))
    body = response.payload.get("payload")
    return (
        response.payload.get("status") == "COMPLETED"
        and isinstance(body, dict)
        and body.get("count") == settings.expected_page_count,
        monotonic_ns(),
    )


async def _run_window(settings: _Settings) -> dict[str, object]:
    batch = settings.target_qps // 100
    if batch * 100 != settings.target_qps:
        raise RuntimeError("target QPS must be divisible by 100 for the 10 ms release clock")
    queue_capacity = _bounded_queue_capacity(settings.workers, batch)
    queue: asyncio.Queue[tuple[int, int] | None] = asyncio.Queue(maxsize=queue_capacity)
    latencies: list[float] = []
    queue_delays: list[float] = []
    completion_times: list[int] = []
    errors = 0

    async def worker() -> None:
        nonlocal errors
        reader, writer = await asyncio.open_unix_connection(str(settings.socket_path))
        requests_on_connection = 0
        try:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return
                sequence, released_ns = item
                if requests_on_connection >= 1_000:
                    writer.close()
                    await writer.wait_closed()
                    reader, writer = await asyncio.open_unix_connection(
                        str(settings.socket_path)
                    )
                    requests_on_connection = 0
                try:
                    call_start_ns = monotonic_ns()
                    queue_delays.append((call_start_ns - released_ns) / 1_000_000)
                    completed, done_ns = await _exchange(
                        reader=reader,
                        writer=writer,
                        settings=settings,
                        sequence=sequence,
                    )
                    errors += int(not completed)
                    latencies.append((done_ns - released_ns) / 1_000_000)
                    completion_times.append(done_ns)
                    requests_on_connection += 1
                except (OSError, ValueError, asyncio.IncompleteReadError):
                    errors += 1
                finally:
                    queue.task_done()
        finally:
            writer.close()
            await writer.wait_closed()

    tasks = [asyncio.create_task(worker()) for _ in range(settings.workers)]
    await asyncio.sleep(0.1)
    start_ns = monotonic_ns()
    batches = settings.duration_seconds * 100
    sequence = 0
    release_errors = 0
    max_queue_depth = 0
    for batch_index in range(batches):
        target_ns = start_ns + batch_index * 10_000_000
        delay = (target_ns - monotonic_ns()) / 1_000_000_000
        if delay > 0:
            await asyncio.sleep(delay)
        released_ns = monotonic_ns()
        for _ in range(batch):
            try:
                queue.put_nowait((sequence, released_ns))
                max_queue_depth = max(max_queue_depth, queue.qsize())
            except asyncio.QueueFull:
                release_errors += 1
            sequence += 1
    window_end_ns = start_ns + settings.duration_seconds * 1_000_000_000
    await queue.join()
    finished_ns = monotonic_ns()
    for _ in tasks:
        await queue.put(None)
    await asyncio.gather(*tasks)
    completed_in_window = sum(done <= window_end_ns for done in completion_times)
    return {
        "offered": sequence,
        "completed": len(latencies),
        "completed_in_window": completed_in_window,
        "completed_qps": completed_in_window / settings.duration_seconds,
        "errors": errors + release_errors,
        "release_errors": release_errors,
        "queue_capacity": queue_capacity,
        "max_queue_depth": max_queue_depth,
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": {
            "p50": _percentile(queue_delays, 0.50),
            "p95": _percentile(queue_delays, 0.95),
            "p99": _percentile(queue_delays, 0.99),
            "p100": _percentile(queue_delays, 1.00),
        },
        "drain_ms": max(0.0, (finished_ns - window_end_ns) / 1_000_000),
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "p100": _percentile(latencies, 1.00),
        },
    }


def main() -> None:
    try:
        import resource
    except ModuleNotFoundError as exc:
        raise RuntimeError("snapshot probe requires Linux resource limits") from exc

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--round", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--qps", type=int, default=5_000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--expected-orders", type=int, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if min(args.duration, args.qps, args.workers, args.page_size, args.expected_orders) <= 0:
        raise RuntimeError("snapshot probe numeric arguments must be positive")
    soft_nofile, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_nofile < args.workers + 64:
        raise RuntimeError("generator RLIMIT_NOFILE is below the worker requirement")
    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("snapshot probe requires the no-Adapter benchmark binding")
    client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 30))
    run_id = uuid.uuid4().hex
    started_ns = monotonic_ns()
    plan: dict[str, Any] | None = None
    attempts = 0
    while attempts < 1_200:
        attempts += 1
        response = client.create_order_snapshot(
            request_id=f"s5-plan-{run_id}-{attempts}",
            consumer_account_id=config.consumer_account_id,
            page_size=args.page_size,
        )
        if response.get("status") == "COMPLETED":
            raw = response.get("payload")
            if isinstance(raw, dict):
                plan = raw
                break
        if response.get("code") != "SNAPSHOT_NOT_READY":
            raise RuntimeError(f"snapshot plan failed: {response.get('code')}")
        time.sleep(0.02)
    if plan is None or plan.get("order_count") != args.expected_orders:
        raise RuntimeError("snapshot plan order count is not the frozen dataset")
    snapshot_build_ms = (monotonic_ns() - started_ns) / 1_000_000
    token = plan.get("page_cursor")
    if not isinstance(token, str):
        raise RuntimeError("snapshot plan cursor is unavailable")
    settings = _Settings(
        config.socket_path,
        token,
        run_id,
        args.round,
        args.duration,
        args.qps,
        args.workers,
        min(args.page_size, args.expected_orders),
    )
    result = asyncio.run(_run_window(settings))
    result.update(
        {
            "profile": "UDS-S5-S3-PAGED-FRESH",
            "revision": args.revision,
            "round": args.round,
            "duration_seconds": args.duration,
            "target_qps": args.qps,
            "workers": args.workers,
            "page_size": args.page_size,
            "snapshot_order_count": args.expected_orders,
            "snapshot_build_ms": snapshot_build_ms,
            "snapshot_cursor_sha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
        }
    )
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
