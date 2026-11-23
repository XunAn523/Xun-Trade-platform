"""Same-host, no-Adapter Consumer V2 burst probe (not a formal benchmark runner)."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import struct
import uuid
from collections import Counter
from contextlib import suppress
from datetime import date
from pathlib import Path
from time import monotonic_ns
from typing import Any

from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)

MAX_BURST_COUNT = 16_384


def _valid_trading_day(value: str) -> bool:
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * percentile) - 1]


async def _exchange(
    *, path: Path, frame: bytes, sample: dict[str, Any], expected_page_count: int | None = None
) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(str(path))
    sample["connected_ns"] = monotonic_ns()
    try:
        writer.write(frame)
        await writer.drain()
        sample["sent_ns"] = monotonic_ns()
        header = await reader.readexactly(HEADER_SIZE_BYTES)
        payload_length = struct.unpack_from(">I", header, 24)[0]
        if HEADER_SIZE_BYTES + payload_length > MAX_FRAME_BYTES:
            raise ValueError("oversize response")
        response = decode_frame(header + await reader.readexactly(payload_length))
        sample["done_ns"] = monotonic_ns()
        sample["status"] = str(response.payload.get("status", "ERROR"))
        sample["kind"] = response.header.message_kind.name
        payload = response.payload.get("payload")
        if isinstance(payload, dict):
            sample["journal_lsn"] = payload.get("journal_lsn")
            sample["page_count"] = payload.get("count")
        if (
            expected_page_count is not None
            and sample["status"] == "COMPLETED"
            and sample.get("page_count") != expected_page_count
        ):
            sample["status"] = "INVALID_SNAPSHOT_PAGE"
        if response.header.message_kind is MessageKind.ERROR:
            sample["status"] = str(response.payload.get("code", "WIRE_ERROR"))
        return sample
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def _main(args: argparse.Namespace) -> None:
    try:
        import resource
    except ModuleNotFoundError as exc:
        raise RuntimeError("burst probe requires Linux resource limits") from exc
    soft_nofile, _hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_nofile < args.count + 64:
        raise RuntimeError(
            f"generator RLIMIT_NOFILE={soft_nofile} is below required {args.count + 64}; "
            "raise the soft limit before this burst"
        )
    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("burst probe requires the no-Adapter benchmark binding")
    run_id = uuid.uuid4().hex
    release = asyncio.Event()
    page_cursor: str | None = None
    snapshot_preparation_ms: float | None = None
    if args.operation == "ReadOrderSnapshotPage":
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 30))
        prepared_start_ns = monotonic_ns()
        for attempt in range(1200):
            response = client.create_order_snapshot(
                request_id=f"burst-snapshot-{run_id}-{attempt}",
                consumer_account_id=config.consumer_account_id,
                page_size=100,
            )
            if response.get("status") == "COMPLETED":
                break
            if response.get("code") != "SNAPSHOT_NOT_READY":
                raise RuntimeError("burst snapshot preparation failed")
            if monotonic_ns() - prepared_start_ns >= 30_000_000_000:
                raise RuntimeError("burst snapshot preparation timed out")
            await asyncio.sleep(.02)
        snapshot_preparation_ms = (monotonic_ns() - prepared_start_ns) / 1_000_000
        payload = response.get("payload")
        if response.get("status") != "COMPLETED" or not isinstance(payload, dict):
            raise RuntimeError("burst snapshot must be prepared before common release")
        page_cursor = payload.get("page_cursor")
        if not isinstance(page_cursor, str) or int(payload.get("order_count", 0)) < 100:
            raise RuntimeError("burst snapshot requires a signed cursor and at least 100 orders")
    ready: asyncio.Queue[None] = asyncio.Queue()

    async def one(index: int) -> dict[str, Any]:
        request = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "SubmitSignal",
            "request_id": f"burst-{run_id}-{index}",
            "payload": {
                "consumer_account_id": config.consumer_account_id,
                "strategy_id": config.strategy_id,
                "strategy_order_id": f"burst-{run_id}-{index}",
                "trading_day": args.trading_day,
                "symbol": "TEST",
                "market": "SIM",
                "side": "BUY",
                "price_type": "LIMIT",
                "business_type": "CASH",
                "quantity": 1,
                "protection_price_ticks": 1,
            },
        }
        if args.operation == "ReadOrderSnapshotPage":
            request["operation"] = args.operation
            request["payload"] = {"page_cursor": page_cursor}
        frame = encode_frame(MessageKind.REQUEST, index + 1, request)
        await ready.put(None)
        await release.wait()
        sample: dict[str, Any] = {"sample_seq": index + 1, "start_ns": monotonic_ns()}
        try:
            return await asyncio.wait_for(
                _exchange(
                    path=config.socket_path,
                    frame=frame,
                    sample=sample,
                    expected_page_count=100 if args.operation == "ReadOrderSnapshotPage" else None,
                ),
                args.timeout,
            )
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
            sample["done_ns"] = monotonic_ns()
            sample["status"] = type(exc).__name__
            sample["stage"] = "connect" if "connected_ns" not in sample else "response"
            if isinstance(exc, OSError):
                sample["errno"] = exc.errno
            return sample

    tasks = [asyncio.create_task(one(index)) for index in range(args.count)]
    for _ in range(args.count):
        await ready.get()
    release_ns = monotonic_ns()
    release.set()
    samples = await asyncio.gather(*tasks)
    end_ns = monotonic_ns()
    if args.output is not None:
        args.output.write_text(
            "".join(json.dumps(sample, sort_keys=True) + "\n" for sample in samples),
            encoding="utf-8",
        )
    latencies = [
        (sample["done_ns"] - sample["start_ns"]) / 1_000_000
        for sample in samples
        if sample["status"] == ("ACCEPTED" if args.operation == "SubmitSignal" else "COMPLETED")
    ]
    lsns = [sample.get("journal_lsn") for sample in samples if sample["status"] == "ACCEPTED"]
    response_latency_ms = {
        "p50": _percentile(latencies, 0.50),
        "p95": _percentile(latencies, 0.95),
        "p99": _percentile(latencies, 0.99),
        "p100": _percentile(latencies, 1.00),
    }
    print(
        json.dumps(
            {
                "profile": "NO_ADAPTER_BENCHMARK_BURST_PROBE",
                "operation": args.operation,
                "release_model": "common_release_before_connect",
                "snapshot_preparation_ms": snapshot_preparation_ms,
                "offered": args.count,
                "connected": sum("connected_ns" in sample for sample in samples),
                "sent": sum("sent_ns" in sample for sample in samples),
                "statuses": dict(Counter(str(sample["status"]) for sample in samples)),
                "unique_accepted_lsn": len(set(lsns))
                if args.operation == "SubmitSignal" and all(lsn is not None for lsn in lsns)
                else None,
                "batch_wall_ms": (end_ns - release_ns) / 1_000_000,
                "response_latency_ms": response_latency_ms,
                "accepted_latency_ms": response_latency_ms
                if args.operation == "SubmitSignal"
                else None,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument(
        "--count",
        type=int,
        choices=range(1, MAX_BURST_COUNT + 1),
        required=True,
        help="Common-release width; 2,000 is the release target; larger widths measure headroom",
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument(
        "--operation", choices=("SubmitSignal", "ReadOrderSnapshotPage"), default="SubmitSignal"
    )
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    if not _valid_trading_day(arguments.trading_day):
        parser.error("trading-day must be ISO YYYY-MM-DD")
    asyncio.run(_main(arguments))
