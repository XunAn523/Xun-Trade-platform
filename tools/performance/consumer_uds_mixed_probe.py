"""Process-isolated S1/S2/S3 mixed-window probe for the no-Adapter UDS service.

The service must already contain the frozen seed dataset and a published paged
snapshot version.  The probe prepares signed replay/snapshot tokens before the
window, then runs independent OS processes per traffic class.  The high-rate S1
and S3 classes can be sharded across processes so a single load-generator
event loop/GIL cannot be mistaken for a TradeCore capacity limit.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import json
import math
import multiprocessing
import os
import queue
import socket
import struct
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from threading import Thread
from time import monotonic_ns
from typing import Any, Callable

from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.consumer.unix_v2 import ConsumerWireStreamDecoderV2
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2


@dataclass(frozen=True)
class _Common:
    socket_path: str
    consumer_account_id: str
    strategy_id: str
    trading_day: str
    run_id: str
    start_ns: int
    duration_seconds: int
    output: str
    connection_mode: str


@dataclass(frozen=True)
class _StartGate:
    ready: Any
    release: Any
    start_ns: Any


def _await_start_gate(common: _Common, gate: _StartGate) -> tuple[_Common, float]:
    ready_ns = monotonic_ns()
    gate.ready.set()
    if not gate.release.wait(30):
        raise TimeoutError("load-generator start gate was not released")
    start_ns = int(gate.start_ns.value)
    lead_ms = (start_ns - ready_ns) / 1_000_000
    if lead_ms <= 0:
        raise RuntimeError("load-generator start gate was released too late")
    return replace(common, start_ns=start_ns), lead_ms


class _WindowClient:
    def __init__(self, path: str, *, persistent: bool, timeout: float = 10.0) -> None:
        self._path = path
        self._persistent = persistent
        self._timeout = timeout
        self._socket: socket.socket | None = None
        self._decoder: ConsumerWireStreamDecoderV2 | None = None
        self._correlation = 0
        self._requests = 0

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
        self._socket = None
        self._decoder = None
        self._requests = 0

    def call(self, request: dict[str, object]) -> tuple[dict[str, Any], int]:
        if self._socket is None or self._requests >= 1_000:
            self.close()
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.settimeout(self._timeout)
            connection.connect(self._path)
            self._socket = connection
            self._decoder = ConsumerWireStreamDecoderV2()
        self._correlation += 1
        correlation = self._correlation
        assert self._socket is not None
        assert self._decoder is not None
        self._socket.sendall(encode_frame(MessageKind.REQUEST, correlation, request))
        while True:
            chunk = self._socket.recv(65_536)
            if not chunk:
                raise ConnectionError("Consumer UDS closed before returning a response")
            for frame in self._decoder.feed(chunk):
                if frame.header.correlation_id != correlation:
                    raise ValueError("Consumer UDS response correlation mismatch")
                self._requests += 1
                response = dict(frame.payload)
                done_ns = monotonic_ns()
                if not self._persistent:
                    self.close()
                return response, done_ns


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _latency_summary(values: list[float]) -> dict[str, float | None]:
    return {
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "p100": _percentile(values, 1.00),
    }


def _checkpoint_stage_durations_ms(
    scheduled_ns: int,
    call_ns: int,
    response_ns: int,
    write_ns: int,
    sync_ns: int,
    done_ns: int,
) -> dict[str, float]:
    """Per-poll wall-clock boundaries; never infer disk service time from fsync."""
    times = (scheduled_ns, call_ns, response_ns, write_ns, sync_ns, done_ns)
    if any(type(value) is not int for value in times) or any(
        left > right for left, right in zip(times, times[1:])
    ):
        raise ValueError("checkpoint timestamps are not monotonic")
    return {
        "scheduled_to_call": (call_ns - scheduled_ns) / 1_000_000,
        "call_to_response": (response_ns - call_ns) / 1_000_000,
        "response_to_write": (write_ns - response_ns) / 1_000_000,
        "write_call": (sync_ns - write_ns) / 1_000_000,
        "fdatasync_call": (done_ns - sync_ns) / 1_000_000,
        "scheduled_to_checkpoint": (done_ns - scheduled_ns) / 1_000_000,
    }


def _write_complete_checkpoint(fd: int, data: bytes) -> None:
    """A short write is a failed checkpoint, never a durable acknowledgement."""
    if os.write(fd, data) != len(data):
        raise OSError("incomplete checkpoint write")


def _wait_until(target_ns: int) -> None:
    while True:
        remaining = target_ns - monotonic_ns()
        if remaining <= 0:
            return
        if remaining > 1_000_000:
            time.sleep((remaining - 500_000) / 1_000_000_000)


def _request(operation: str, request_id: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "message_type": "request",
        "schema_version": 2,
        "operation": operation,
        "request_id": request_id,
        "payload": payload,
    }


def _bounded_queue_capacity(workers: int, per_tick: int) -> int:
    """Keep at most two worker waves; never hide seconds of generator debt."""

    if workers <= 0 or per_tick <= 0:
        raise ValueError("worker and release counts must be positive")
    return max(per_tick, workers * 2)


def _resolve_s3_queue_capacity(
    workers: int,
    per_tick: int,
    override: int | None,
) -> int:
    baseline = _bounded_queue_capacity(workers, per_tick)
    if override is None:
        return baseline
    if override < baseline:
        raise ValueError("S3 queue capacity cannot be below the worker-wave baseline")
    if override > per_tick * 10:
        raise ValueError("S3 queue capacity cannot buffer more than 100 ms of releases")
    return override


def _partition_cpu_lane(cpus: tuple[int, ...], processes: int) -> tuple[tuple[int, ...], ...]:
    if processes <= 0:
        raise ValueError("process count must be positive")
    if len(cpus) < processes:
        raise ValueError("each process requires at least one dedicated CPU")
    return tuple(tuple(cpus[index::processes]) for index in range(processes))


def _run_parallel_window(
    *,
    common: _Common,
    workers: int,
    per_tick: int,
    operation: Callable[[int], tuple[dict[str, object], Callable[[dict[str, Any]], bool]]],
    start_gate: _StartGate,
) -> dict[str, object]:
    queue_capacity = _bounded_queue_capacity(workers, per_tick)
    work: queue.Queue[tuple[int, int] | None] = queue.Queue(maxsize=queue_capacity)
    latencies: list[float] = []
    queue_delays: list[float] = []
    completion_times: list[int] = []
    errors: list[str] = []
    accepted_values: list[object] = []

    def worker() -> None:
        client = _WindowClient(
            common.socket_path,
            persistent=common.connection_mode == "persistent-per-vu",
        )
        try:
            while True:
                item = work.get()
                try:
                    if item is None:
                        return
                    sequence, released_ns = item
                    request, validate = operation(sequence)
                    call_start_ns = monotonic_ns()
                    queue_delays.append((call_start_ns - released_ns) / 1_000_000)
                    response, done_ns = client.call(request)
                    if not validate(response):
                        errors.append(str(response.get("code", response.get("status", "ERROR"))))
                        continue
                    payload = response.get("payload")
                    if isinstance(payload, dict) and "journal_lsn" in payload:
                        accepted_values.append(payload["journal_lsn"])
                    latencies.append((done_ns - released_ns) / 1_000_000)
                    completion_times.append(done_ns)
                except Exception as exc:  # evidence must retain transport failures
                    errors.append(type(exc).__name__)
                    client.close()
                finally:
                    work.task_done()
        finally:
            client.close()

    threads = [Thread(target=worker, daemon=True) for _ in range(workers)]
    for thread in threads:
        thread.start()
    common, ready_before_start_ms = _await_start_gate(common, start_gate)
    _wait_until(common.start_ns)
    sequence = 0
    release_errors = 0
    max_queue_depth = 0
    for tick in range(common.duration_seconds * 100):
        planned_ns = common.start_ns + tick * 10_000_000
        _wait_until(planned_ns)
        for _ in range(per_tick):
            try:
                work.put_nowait((sequence, planned_ns))
                max_queue_depth = max(max_queue_depth, work.qsize())
            except queue.Full:
                release_errors += 1
            sequence += 1
    window_end_ns = common.start_ns + common.duration_seconds * 1_000_000_000
    work.join()
    finished_ns = monotonic_ns()
    for _ in threads:
        work.put(None)
    for thread in threads:
        thread.join()
    return {
        "offered": sequence,
        "completed": len(latencies),
        "completed_in_window": sum(value <= window_end_ns for value in completion_times),
        "errors": len(errors) + release_errors,
        "error_counts": _counts(errors),
        "release_errors": release_errors,
        "queue_capacity": queue_capacity,
        "max_queue_depth": max_queue_depth,
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": _latency_summary(queue_delays),
        "unique_journal_lsns": len(set(accepted_values)) if accepted_values else None,
        "drain_ms": max(0.0, (finished_ns - window_end_ns) / 1_000_000),
        "latency_ms": _latency_summary(latencies),
        "_latency_samples_ms": latencies,
        "_queue_delay_samples_ms": queue_delays,
        "_accepted_values": accepted_values,
        "ready_before_start_ms": ready_before_start_ms,
    }


def _run_s1(
    common: _Common,
    workers: int,
    target_tps: int,
    shard_index: int,
    start_gate: _StartGate,
) -> dict[str, object]:
    if target_tps % 100:
        raise ValueError("S1 target TPS must be divisible by 100")

    def operation(sequence: int):
        request = _request(
            "SubmitSignal",
            f"s5-s1-{common.run_id}-{shard_index}-{sequence}",
            {
                "consumer_account_id": common.consumer_account_id,
                "strategy_id": common.strategy_id,
                "strategy_order_id": f"s5-s1-{common.run_id}-{shard_index}-{sequence}",
                "trading_day": common.trading_day,
                "symbol": "TEST",
                "market": "SIM",
                "side": "BUY",
                "price_type": "LIMIT",
                "business_type": "CASH",
                "quantity": 1,
                "protection_price_ticks": 1,
            },
        )
        return request, lambda response: response.get("status") == "ACCEPTED"

    result = _run_parallel_window(
        common=common,
        workers=workers,
        per_tick=target_tps // 100,
        operation=operation,
        start_gate=start_gate,
    )
    result.update(
        profile="S1",
        target_tps=target_tps,
        workers=workers,
        shard_index=shard_index,
    )
    return result


def _aggregate_s1_shards(shards: list[dict[str, Any]]) -> dict[str, object]:
    if not shards:
        raise ValueError("at least one S1 shard is required")
    latencies = [float(value) for shard in shards for value in shard.pop("_latency_samples_ms", [])]
    queue_delays = [
        float(value) for shard in shards for value in shard.pop("_queue_delay_samples_ms", [])
    ]
    accepted_values = [value for shard in shards for value in shard.pop("_accepted_values", [])]
    error_counts: dict[str, int] = {}
    for shard in shards:
        for name, count in dict(shard.get("error_counts", {})).items():
            error_counts[name] = error_counts.get(name, 0) + int(count)
    affinities = sorted(
        {int(cpu) for shard in shards for cpu in list(shard.get("load_generator_cpu_affinity", []))}
    )
    return {
        "profile": "S1",
        "target_tps": sum(int(shard["target_tps"]) for shard in shards),
        "processes": len(shards),
        "workers": sum(int(shard["workers"]) for shard in shards),
        "offered": sum(int(shard["offered"]) for shard in shards),
        "completed": sum(int(shard["completed"]) for shard in shards),
        "completed_in_window": sum(int(shard["completed_in_window"]) for shard in shards),
        "errors": sum(int(shard["errors"]) for shard in shards),
        "error_counts": error_counts,
        "release_errors": sum(int(shard["release_errors"]) for shard in shards),
        "queue_capacity": sum(int(shard["queue_capacity"]) for shard in shards),
        "max_queue_depth": sum(int(shard["max_queue_depth"]) for shard in shards),
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": _latency_summary(queue_delays),
        "unique_journal_lsns": len(set(accepted_values)),
        "drain_ms": max(float(shard["drain_ms"]) for shard in shards),
        "latency_ms": _latency_summary(latencies),
        "load_generator_automatic_gc_disabled": all(
            shard.get("load_generator_automatic_gc_disabled") is True for shard in shards
        ),
        "load_generator_cpu_affinity": affinities,
        "ready_before_start_ms": min(float(shard["ready_before_start_ms"]) for shard in shards),
        "shards": shards,
    }


def _run_s3(
    common: _Common,
    workers: int,
    target_qps: int,
    page_cursor: str,
    expected_page_count: int,
    queue_capacity_override: int | None,
    shard_index: int,
    start_gate: _StartGate,
) -> dict[str, object]:
    if target_qps % 100:
        raise ValueError("S3 target QPS must be divisible by 100")
    result = asyncio.run(
        _run_s3_async(
            common=common,
            workers=workers,
            per_tick=target_qps // 100,
            page_cursor=page_cursor,
            expected_page_count=expected_page_count,
            queue_capacity_override=queue_capacity_override,
            shard_index=shard_index,
            start_gate=start_gate,
        )
    )
    result.update(
        profile="S3",
        target_qps=target_qps,
        workers=workers,
        page_cursor_sha256=hashlib.sha256(page_cursor.encode("ascii")).hexdigest(),
        shard_index=shard_index,
    )
    return result


async def _run_s3_async(
    *,
    common: _Common,
    workers: int,
    per_tick: int,
    page_cursor: str,
    expected_page_count: int,
    queue_capacity_override: int | None,
    shard_index: int,
    start_gate: _StartGate,
) -> dict[str, object]:
    """Use one event loop, matching the already accepted S3 single-stream probe."""

    queue_capacity = _resolve_s3_queue_capacity(
        workers,
        per_tick,
        queue_capacity_override,
    )
    work: asyncio.Queue[tuple[int, int] | None] = asyncio.Queue(maxsize=queue_capacity)
    latencies: list[float] = []
    queue_delays: list[float] = []
    completion_times: list[int] = []
    errors: list[str] = []

    async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_unix_connection(common.socket_path)

    async def worker() -> None:
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        requests = 0

        async def close() -> None:
            nonlocal reader, writer, requests
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
            reader = None
            writer = None
            requests = 0

        try:
            while True:
                item = await work.get()
                try:
                    if item is None:
                        return
                    sequence, released_ns = item
                    if writer is None or requests >= 1_000:
                        await close()
                        reader, writer = await connect()
                    assert reader is not None and writer is not None
                    request = _request(
                        "ReadOrderSnapshotPage",
                        f"s5-s3-{common.run_id}-{shard_index}-{sequence}",
                        {"page_cursor": page_cursor},
                    )
                    call_start_ns = monotonic_ns()
                    queue_delays.append((call_start_ns - released_ns) / 1_000_000)
                    writer.write(encode_frame(MessageKind.REQUEST, sequence + 1, request))
                    await writer.drain()
                    header = await reader.readexactly(HEADER_SIZE_BYTES)
                    length = struct.unpack_from(">I", header, 24)[0]
                    if HEADER_SIZE_BYTES + length > MAX_FRAME_BYTES:
                        raise ValueError("oversize snapshot response")
                    response = decode_frame(header + await reader.readexactly(length))
                    done_ns = monotonic_ns()
                    payload = response.payload.get("payload")
                    if (
                        response.payload.get("status") != "COMPLETED"
                        or not isinstance(payload, dict)
                        or payload.get("count") != expected_page_count
                    ):
                        errors.append(
                            str(response.payload.get("code", response.payload.get("status")))
                        )
                        continue
                    requests += 1
                    latencies.append((done_ns - released_ns) / 1_000_000)
                    completion_times.append(done_ns)
                except Exception as exc:
                    errors.append(type(exc).__name__)
                    await close()
                finally:
                    if common.connection_mode == "fresh-per-request":
                        await close()
                    work.task_done()
        finally:
            await close()

    tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    await asyncio.sleep(0.1)
    common, ready_before_start_ms = _await_start_gate(common, start_gate)
    _wait_until(common.start_ns)
    sequence = 0
    release_errors = 0
    max_queue_depth = 0
    for tick in range(common.duration_seconds * 100):
        planned_ns = common.start_ns + tick * 10_000_000
        delay = (planned_ns - monotonic_ns()) / 1_000_000_000
        if delay > 0:
            await asyncio.sleep(delay)
        for _ in range(per_tick):
            try:
                work.put_nowait((sequence, planned_ns))
                max_queue_depth = max(max_queue_depth, work.qsize())
            except asyncio.QueueFull:
                release_errors += 1
            sequence += 1
    window_end_ns = common.start_ns + common.duration_seconds * 1_000_000_000
    await work.join()
    finished_ns = monotonic_ns()
    for _ in tasks:
        await work.put(None)
    await asyncio.gather(*tasks)
    return {
        "offered": sequence,
        "completed": len(latencies),
        "completed_in_window": sum(value <= window_end_ns for value in completion_times),
        "errors": len(errors) + release_errors,
        "error_counts": _counts(errors),
        "release_errors": release_errors,
        "queue_capacity": queue_capacity,
        "max_queue_depth": max_queue_depth,
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": _latency_summary(queue_delays),
        "unique_journal_lsns": None,
        "drain_ms": max(0.0, (finished_ns - window_end_ns) / 1_000_000),
        "latency_ms": _latency_summary(latencies),
        "_latency_samples_ms": latencies,
        "_queue_delay_samples_ms": queue_delays,
        "ready_before_start_ms": ready_before_start_ms,
    }


def _aggregate_s3_shards(shards: list[dict[str, Any]]) -> dict[str, object]:
    if not shards:
        raise ValueError("at least one S3 shard is required")
    latencies = [float(value) for shard in shards for value in shard.pop("_latency_samples_ms", [])]
    queue_delays = [
        float(value) for shard in shards for value in shard.pop("_queue_delay_samples_ms", [])
    ]
    error_counts: dict[str, int] = {}
    for shard in shards:
        for name, count in dict(shard.get("error_counts", {})).items():
            error_counts[name] = error_counts.get(name, 0) + int(count)
    affinities = sorted(
        {int(cpu) for shard in shards for cpu in list(shard.get("load_generator_cpu_affinity", []))}
    )
    return {
        "profile": "S3",
        "target_qps": sum(int(shard["target_qps"]) for shard in shards),
        "processes": len(shards),
        "workers": sum(int(shard["workers"]) for shard in shards),
        "offered": sum(int(shard["offered"]) for shard in shards),
        "completed": sum(int(shard["completed"]) for shard in shards),
        "completed_in_window": sum(int(shard["completed_in_window"]) for shard in shards),
        "errors": sum(int(shard["errors"]) for shard in shards),
        "error_counts": error_counts,
        "release_errors": sum(int(shard["release_errors"]) for shard in shards),
        "queue_capacity": sum(int(shard["queue_capacity"]) for shard in shards),
        "max_queue_depth": sum(int(shard["max_queue_depth"]) for shard in shards),
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": _latency_summary(queue_delays),
        "unique_journal_lsns": None,
        "drain_ms": max(float(shard["drain_ms"]) for shard in shards),
        "latency_ms": _latency_summary(latencies),
        "load_generator_automatic_gc_disabled": all(
            shard.get("load_generator_automatic_gc_disabled") is True for shard in shards
        ),
        "load_generator_cpu_affinity": affinities,
        "page_cursor_sha256": shards[0]["page_cursor_sha256"],
        "ready_before_start_ms": min(float(shard["ready_before_start_ms"]) for shard in shards),
        "shards": shards,
    }


def _run_s2(
    common: _Common,
    target_events_per_second: int,
    limit: int,
    partitions: list[dict[str, Any]],
    checkpoint_path: str,
    start_gate: _StartGate,
) -> dict[str, object]:
    if target_events_per_second % limit:
        raise ValueError("S2 target events per second must be divisible by PollEvents limit")
    requests_per_second = target_events_per_second // limit
    if 100 % requests_per_second:
        raise ValueError("S2 request rate must divide the 100 Hz release clock")
    queue_capacity_per_partition = 2
    queues = [queue.Queue(maxsize=queue_capacity_per_partition) for _ in partitions]
    latencies: list[float] = []
    queue_delays: list[float] = []
    completion_times: list[int] = []
    event_ids: list[str] = []
    errors: list[str] = []
    checkpoint_stages: list[dict[str, object]] = []
    checkpoint_fd = os.open(checkpoint_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)

    def worker(index: int) -> None:
        token = str(partitions[index]["cursor"])
        client = _WindowClient(
            common.socket_path,
            persistent=common.connection_mode == "persistent-per-vu",
        )
        previous_position: str | None = None
        partition_failed = False
        try:
            while True:
                item = queues[index].get()
                try:
                    if item is None:
                        return
                    if partition_failed:
                        errors.append("CHECKPOINT_PARTITION_FAILED_CLOSED")
                        continue
                    sequence, released_ns = item
                    call_start_ns = monotonic_ns()
                    queue_delays.append((call_start_ns - released_ns) / 1_000_000)
                    response, response_ns = client.call(
                        _request(
                            "PollEvents",
                            f"s5-s2-{common.run_id}-{index}-{sequence}",
                            {"cursor": token, "limit": limit},
                        )
                    )
                    payload = response.get("payload")
                    if response.get("status") != "COMPLETED" or not isinstance(payload, dict):
                        errors.append(str(response.get("code", "POLL_ERROR")))
                        partition_failed = True
                        continue
                    events = payload.get("events")
                    next_cursor = payload.get("next_cursor")
                    if not isinstance(events, list) or not isinstance(next_cursor, str):
                        errors.append("INVALID_POLL_RESPONSE")
                        partition_failed = True
                        continue
                    ids: list[str] = []
                    next_position = previous_position
                    for event in events:
                        if not isinstance(event, dict):
                            raise ValueError("PollEvents returned a non-object event")
                        event_id = event.get("event_id")
                        position = event.get("position")
                        if not isinstance(event_id, str) or not isinstance(position, str):
                            raise ValueError("PollEvents omitted event identity")
                        if next_position is not None and _position_key(
                            position
                        ) <= _position_key(next_position):
                            raise ValueError("PollEvents partition order is not increasing")
                        next_position = position
                        ids.append(event_id)
                    checkpoint = (
                        json.dumps(
                            {
                                "partition": index,
                                "sequence": sequence,
                                "count": len(ids),
                                "last_position": next_position,
                                "event_ids_sha256": hashlib.sha256(
                                    "\n".join(ids).encode("utf-8")
                                ).hexdigest(),
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        + b"\n"
                    )
                    write_ns = monotonic_ns()
                    _write_complete_checkpoint(checkpoint_fd, checkpoint)
                    sync_ns = monotonic_ns()
                    sync_cpu_ns = time.thread_time_ns()
                    os.fdatasync(checkpoint_fd)
                    sync_cpu_elapsed_ns = time.thread_time_ns() - sync_cpu_ns
                    done_ns = monotonic_ns()
                    token = next_cursor
                    previous_position = next_position
                    checkpoint_stages.append(
                        {
                            "sequence": sequence,
                            "partition": index,
                            "event_count": len(ids),
                            "scheduled_ns": released_ns,
                            "call_ns": call_start_ns,
                            "response_ns": response_ns,
                            "write_ns": write_ns,
                            "sync_ns": sync_ns,
                            "done_ns": done_ns,
                            "fdatasync_thread_cpu_ms": sync_cpu_elapsed_ns / 1_000_000,
                            **_checkpoint_stage_durations_ms(
                                released_ns, call_start_ns, response_ns, write_ns, sync_ns, done_ns
                            ),
                        }
                    )
                    event_ids.extend(ids)
                    latencies.extend([(done_ns - released_ns) / 1_000_000] * len(ids))
                    completion_times.extend([done_ns] * len(ids))
                except Exception as exc:
                    errors.append(type(exc).__name__)
                    partition_failed = True
                    client.close()
                finally:
                    queues[index].task_done()
        finally:
            client.close()

    threads = [Thread(target=worker, args=(index,), daemon=True) for index in range(len(queues))]
    for thread in threads:
        thread.start()
    common, ready_before_start_ms = _await_start_gate(common, start_gate)
    _wait_until(common.start_ns)
    sequence = 0
    release_errors = 0
    max_queue_depth = 0
    tick_step = 100 // requests_per_second
    total_ticks = common.duration_seconds * 100
    for tick in range(0, total_ticks, tick_step):
        planned_ns = common.start_ns + tick * 10_000_000
        _wait_until(planned_ns)
        target = queues[sequence % len(queues)]
        try:
            target.put_nowait((sequence, planned_ns))
            max_queue_depth = max(max_queue_depth, target.qsize())
        except queue.Full:
            release_errors += 1
        sequence += 1
    window_end_ns = common.start_ns + common.duration_seconds * 1_000_000_000
    for pending in queues:
        pending.join()
    finished_ns = monotonic_ns()
    for pending in queues:
        pending.put(None)
    for thread in threads:
        thread.join()
    os.close(checkpoint_fd)
    return {
        "profile": "S2",
        "target_events_per_second": target_events_per_second,
        "poll_limit": limit,
        "workers": len(threads),
        "offered_poll_requests": sequence,
        "checkpointed_events": len(event_ids),
        "checkpointed_in_window": sum(value <= window_end_ns for value in completion_times),
        "unique_events": len(set(event_ids)),
        "errors": len(errors) + release_errors,
        "error_counts": _counts(errors),
        "release_errors": release_errors,
        "queue_capacity_per_partition": queue_capacity_per_partition,
        "max_queue_depth_per_partition": max_queue_depth,
        "call_start_samples": len(queue_delays),
        "offered_to_call_start_ms": _latency_summary(queue_delays),
        "drain_ms": max(0.0, (finished_ns - window_end_ns) / 1_000_000),
        "checkpoint_latency_ms": _latency_summary(latencies),
        "checkpoint_stage_scope": "per_poll_wall_clock_including_empty_pages",
        "checkpoint_stage_samples": checkpoint_stages,
        "checkpoint_stage_latency_ms": {
            name: _latency_summary([float(row[name]) for row in checkpoint_stages])
            for name in (
                "scheduled_to_call",
                "call_to_response",
                "response_to_write",
                "write_call",
                "fdatasync_call",
                "fdatasync_thread_cpu_ms",
                "scheduled_to_checkpoint",
            )
        },
        "ready_before_start_ms": ready_before_start_ms,
    }


def _position_key(value: str) -> tuple[int, int, int]:
    position = ConsumerEventPositionV2.from_external_id(value)
    return (
        position.stream_id.redis_ms,
        position.stream_id.redis_seq,
        position.fact_ordinal,
    )


def _counts(values: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return result


def _parse_cpu_set(value: str) -> tuple[int, ...]:
    cpus: set[int] = set()
    for token in value.split(","):
        part = token.strip()
        if not part:
            raise ValueError("CPU affinity contains an empty component")
        if "-" in part:
            begin_text, separator, end_text = part.partition("-")
            if not separator or not begin_text.isdigit() or not end_text.isdigit():
                raise ValueError("CPU affinity range is invalid")
            begin, end = int(begin_text), int(end_text)
            if begin > end:
                raise ValueError("CPU affinity range is descending")
            cpus.update(range(begin, end + 1))
        elif part.isdigit():
            cpus.add(int(part))
        else:
            raise ValueError("CPU affinity component is invalid")
    if not cpus:
        raise ValueError("CPU affinity cannot be empty")
    return tuple(sorted(cpus))


def _resolve_cpu_lanes(
    s1: str | None,
    s2: str | None,
    s3: str | None,
    *,
    allowed: set[int] | None = None,
) -> dict[str, tuple[int, ...]] | None:
    raw = {"s1": s1, "s2": s2, "s3": s3}
    if all(value is None for value in raw.values()):
        return None
    if any(value is None for value in raw.values()):
        raise ValueError("S1, S2 and S3 CPU affinities must be provided together")
    lanes = {name: _parse_cpu_set(value) for name, value in raw.items() if value is not None}
    seen: set[int] = set()
    for name in ("s1", "s2", "s3"):
        lane = set(lanes[name])
        if seen & lane:
            raise ValueError("S1, S2 and S3 CPU affinities must not overlap")
        seen.update(lane)
    permitted = set(os.sched_getaffinity(0)) if allowed is None else allowed
    if not seen <= permitted:
        raise ValueError("generator CPU affinity widens the current cgroup CPU set")
    return lanes


def _child(
    target: Callable[..., dict[str, object]],
    output: str,
    cpu_affinity: tuple[int, ...] | None,
    *args: object,
) -> None:
    automatic_gc_was_enabled = gc.isenabled()
    if automatic_gc_was_enabled:
        gc.disable()
    result: dict[str, object]
    try:
        if cpu_affinity is not None:
            os.sched_setaffinity(0, cpu_affinity)
        result = target(*args)
    except Exception as exc:
        result = {
            "fatal_error": type(exc).__name__,
            "detail": str(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        if automatic_gc_was_enabled:
            gc.enable()
    result["load_generator_automatic_gc_disabled"] = automatic_gc_was_enabled
    result["load_generator_cpu_affinity"] = sorted(os.sched_getaffinity(0))
    Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _control_call(
    client: ConsumerUnixClientV2, operation: str, request_id: str, payload: dict[str, object]
) -> dict[str, Any]:
    response = dict(client.call(_request(operation, request_id, payload)))
    body = response.get("payload")
    if response.get("status") != "COMPLETED" or not isinstance(body, dict):
        raise RuntimeError(f"{operation} failed: {response.get('code', response.get('status'))}")
    return body


def _evaluate(
    s1: dict[str, Any],
    s2: dict[str, Any],
    s3: dict[str, Any],
    duration: int,
    *,
    s1_tps: int = 2_000,
    s2_eps: int = 5_000,
    s3_qps: int = 5_000,
) -> dict[str, object]:
    s1_latency = s1.get("latency_ms", {})
    s2_latency = s2.get("checkpoint_latency_ms", {})
    s3_latency = s3.get("latency_ms", {})
    checks = {
        "s1_count": s1.get("completed") == s1_tps * duration and _bounded_drain(s1, 60.0),
        "s1_errors": s1.get("errors") == 0,
        "s1_latency": _under(s1_latency, 20.0, 60.0),
        "s1_generator": _generator_healthy(s1, "offered"),
        "s2_count": s2.get("checkpointed_events") == s2_eps * duration and _bounded_drain(s2, 40.0),
        "s2_unique": s2.get("unique_events") == s2.get("checkpointed_events"),
        "s2_errors": s2.get("errors") == 0,
        "s2_latency": _under(s2_latency, 10.0, 40.0),
        "s2_generator": _generator_healthy(s2, "offered_poll_requests"),
        "s3_count": s3.get("completed") == s3_qps * duration and _bounded_drain(s3, 40.0),
        "s3_errors": s3.get("errors") == 0,
        "s3_latency": _under(s3_latency, 10.0, 40.0),
        "s3_generator": _generator_healthy(s3, "offered"),
    }
    return {"checks": checks, "passed": all(checks.values())}


def _under(values: object, p95_limit: float, p99_limit: float) -> bool:
    return (
        isinstance(values, dict)
        and isinstance(values.get("p95"), (int, float))
        and isinstance(values.get("p99"), (int, float))
        and values["p95"] <= p95_limit
        and values["p99"] <= p99_limit
    )


def _bounded_drain(values: dict[str, Any], limit_ms: float) -> bool:
    drain = values.get("drain_ms")
    return isinstance(drain, (int, float)) and 0 <= drain <= limit_ms


def _generator_healthy(values: dict[str, Any], offered_name: str) -> bool:
    queue_delay = values.get("offered_to_call_start_ms")
    return (
        values.get("release_errors") == 0
        and values.get("call_start_samples") == values.get(offered_name)
        and isinstance(queue_delay, dict)
        and isinstance(queue_delay.get("p99"), (int, float))
        and queue_delay["p99"] <= 10.0
    )


def _profile_name(connection_mode: str) -> str:
    if connection_mode == "fresh-per-request":
        return "UDS-S5-MIX-PAGED-FRESH"
    if connection_mode == "persistent-per-vu":
        return "UDS-S5-MIX-PAGED-PERSISTENT"
    raise ValueError("unsupported Consumer UDS connection mode")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--s1-tps", type=int, default=2_000)
    parser.add_argument("--s2-eps", type=int, default=5_000)
    parser.add_argument("--s3-qps", type=int, default=5_000)
    parser.add_argument("--expected-orders", type=int, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--round", required=True)
    parser.add_argument(
        "--connection-mode",
        choices=("fresh-per-request", "persistent-per-vu"),
        required=True,
    )
    parser.add_argument("--s1-cpus", help="Dedicated Linux CPU set for the S1 process")
    parser.add_argument("--s2-cpus", help="Dedicated Linux CPU set for the S2 process")
    parser.add_argument("--s3-cpus", help="Dedicated Linux CPU set for the S3 process")
    parser.add_argument("--s1-workers", type=int, default=64)
    parser.add_argument("--s1-processes", type=int, default=1)
    parser.add_argument("--s3-workers", type=int, default=64)
    parser.add_argument("--s3-processes", type=int, default=1)
    parser.add_argument(
        "--s3-queue-capacity",
        type=int,
        help="Bounded S3 release queue; capped at 100 ms of scheduled requests",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if (
        args.duration <= 0
        or args.expected_orders <= 0
        or not 1 <= args.s1_workers <= 512
        or not 1 <= args.s1_processes <= 20
        or not 1 <= args.s3_workers <= 512
        or not 1 <= args.s3_processes <= 32
    ):
        raise RuntimeError(
            "duration/orders must be positive, worker counts must be 1..512, "
            "S1 process count must be 1..20, and S3 process count must be 1..32"
        )
    if args.s1_workers % args.s1_processes:
        raise RuntimeError("S1 workers must divide evenly across S1 processes")
    if (
        args.s1_tps <= 0
        or args.s1_tps % args.s1_processes
        or (args.s1_tps // args.s1_processes) % 100
    ):
        raise RuntimeError("S1 target TPS must divide into 100 Hz process shards")
    if args.s3_workers % args.s3_processes:
        raise RuntimeError("S3 workers must divide evenly across S3 processes")
    if (
        args.s3_qps <= 0
        or args.s3_qps % args.s3_processes
        or (args.s3_qps // args.s3_processes) % 100
    ):
        raise RuntimeError("S3 target QPS must divide into 100 Hz process shards")
    if args.s2_eps < 100 or args.s2_eps % 100 or 100 % (args.s2_eps // 100):
        raise RuntimeError("S2 target EPS must divide into the 100 Hz poll release clock")
    if args.s2_eps * args.duration > args.expected_orders:
        raise RuntimeError("S2 target exceeds the frozen replay dataset")
    if args.s3_queue_capacity is not None and (args.s3_queue_capacity % args.s3_processes):
        raise RuntimeError("S3 queue capacity must divide evenly across S3 processes")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("mixed probe requires the no-Adapter benchmark binding")
    cpu_lanes = _resolve_cpu_lanes(args.s1_cpus, args.s2_cpus, args.s3_cpus)
    s3_cpu_lanes = (
        None if cpu_lanes is None else _partition_cpu_lane(cpu_lanes["s3"], args.s3_processes)
    )
    s1_cpu_lanes = (
        None if cpu_lanes is None else _partition_cpu_lane(cpu_lanes["s1"], args.s1_processes)
    )
    run_id = uuid.uuid4().hex
    control = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 30))
    snapshot = _control_call(
        control,
        "CreateOrderSnapshot",
        f"s5-mixed-{run_id}-snapshot",
        {"consumer_account_id": config.consumer_account_id, "page_size": 100},
    )
    if snapshot.get("order_count") != args.expected_orders:
        raise RuntimeError("snapshot plan does not match the frozen order count")
    page_cursor = snapshot.get("page_cursor")
    if not isinstance(page_cursor, str):
        raise RuntimeError("snapshot plan omitted page cursor")
    replay = _control_call(
        control,
        "CreateReplayPartitions",
        f"s5-mixed-{run_id}-replay",
        {"consumer_account_id": config.consumer_account_id, "partition_count": 32},
    )
    partitions = replay.get("partitions")
    if replay.get("event_count") != args.expected_orders or not isinstance(partitions, list):
        raise RuntimeError("replay plan does not match the frozen event count")
    start_ns = 0
    common = _Common(
        str(config.socket_path),
        config.consumer_account_id,
        config.strategy_id,
        args.trading_day,
        run_id,
        start_ns,
        args.duration,
        str(args.output_dir),
        args.connection_mode,
    )
    files = {"s2": args.output_dir / "s2.json"}
    s1_files = [
        args.output_dir / f"s1-shard-{index:02d}.json" for index in range(args.s1_processes)
    ]
    s3_files = [
        args.output_dir / f"s3-shard-{index:02d}.json" for index in range(args.s3_processes)
    ]
    ctx = multiprocessing.get_context("spawn")
    shared_start_ns = ctx.Value("q", 0)
    release_gate = ctx.Event()
    start_gates = [
        _StartGate(ctx.Event(), release_gate, shared_start_ns)
        for _ in range(args.s1_processes + 1 + args.s3_processes)
    ]
    processes = [
        ctx.Process(
            target=_child,
            args=(
                _run_s2,
                str(files["s2"]),
                None if cpu_lanes is None else cpu_lanes["s2"],
                common,
                args.s2_eps,
                100,
                partitions,
                str(args.output_dir / "s2-checkpoints.ndjson"),
                start_gates[args.s1_processes],
            ),
        ),
    ]
    s1_workers_per_process = args.s1_workers // args.s1_processes
    s1_tps_per_process = args.s1_tps // args.s1_processes
    for index, output in enumerate(s1_files):
        processes.append(
            ctx.Process(
                target=_child,
                args=(
                    _run_s1,
                    str(output),
                    None if s1_cpu_lanes is None else s1_cpu_lanes[index],
                    common,
                    s1_workers_per_process,
                    s1_tps_per_process,
                    index,
                    start_gates[index],
                ),
            )
        )
    s3_workers_per_process = args.s3_workers // args.s3_processes
    s3_qps_per_process = args.s3_qps // args.s3_processes
    s3_queue_per_process = (
        None if args.s3_queue_capacity is None else args.s3_queue_capacity // args.s3_processes
    )
    for index, output in enumerate(s3_files):
        processes.append(
            ctx.Process(
                target=_child,
                args=(
                    _run_s3,
                    str(output),
                    None if s3_cpu_lanes is None else s3_cpu_lanes[index],
                    common,
                    s3_workers_per_process,
                    s3_qps_per_process,
                    page_cursor,
                    100,
                    s3_queue_per_process,
                    index,
                    start_gates[args.s1_processes + 1 + index],
                ),
            )
        )
    for process in processes:
        process.start()
    for gate in start_gates:
        if not gate.ready.wait(30):
            for process in processes:
                process.kill()
                process.join()
            raise RuntimeError("mixed probe child failed to reach the start gate")
    start_ns = monotonic_ns() + 2_000_000_000
    shared_start_ns.value = start_ns
    release_gate.set()
    for process in processes:
        process.join(args.duration + 120)
        if process.is_alive():
            process.kill()
            process.join()
            raise RuntimeError("mixed probe child exceeded its bounded drain time")
        if process.exitcode != 0:
            raise RuntimeError(f"mixed probe child exited with {process.exitcode}")
    results = {"s2": json.loads(files["s2"].read_text(encoding="utf-8"))}
    s1_shards = [json.loads(path.read_text(encoding="utf-8")) for path in s1_files]
    results["s1"] = _aggregate_s1_shards(s1_shards)
    (args.output_dir / "s1.json").write_text(
        json.dumps(results["s1"], sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    s3_shards = [json.loads(path.read_text(encoding="utf-8")) for path in s3_files]
    results["s3"] = _aggregate_s3_shards(s3_shards)
    (args.output_dir / "s3.json").write_text(
        json.dumps(results["s3"], sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    if any("fatal_error" in result for result in results.values()):
        raise RuntimeError("mixed probe child reported a fatal error")
    summary = {
        "profile": _profile_name(args.connection_mode),
        "connection_mode": args.connection_mode,
        "revision": args.revision,
        "round": args.round,
        "run_id": run_id,
        "duration_seconds": args.duration,
        "target_rates": {"s1_tps": args.s1_tps, "s2_eps": args.s2_eps, "s3_qps": args.s3_qps},
        "snapshot_order_count": args.expected_orders,
        "generator_cpu_lanes": (
            None if cpu_lanes is None else {name: list(cpus) for name, cpus in cpu_lanes.items()}
        ),
        "generator_workers": {"s1": args.s1_workers, "s2": 32, "s3": args.s3_workers},
        "generator_processes": {
            "s1": args.s1_processes,
            "s2": 1,
            "s3": args.s3_processes,
        },
        "snapshot_id_sha256": hashlib.sha256(str(snapshot["snapshot_id"]).encode()).hexdigest(),
        "replay_plan_sha256": hashlib.sha256(
            json.dumps(
                {
                    "journal_id": replay.get("journal_id"),
                    "stream_generation": replay.get("stream_generation"),
                    "high_watermark_position": replay.get("high_watermark_position"),
                    "event_count": replay.get("event_count"),
                    "partition_count": len(partitions),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "common_start_ns": start_ns,
        "settings": asdict(replace(common, start_ns=start_ns)),
        **results,
    }
    summary["acceptance"] = _evaluate(
        results["s1"],
        results["s2"],
        results["s3"],
        args.duration,
        s1_tps=args.s1_tps,
        s2_eps=args.s2_eps,
        s3_qps=args.s3_qps,
    )
    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
