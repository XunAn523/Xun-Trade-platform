"""Isolated controlled responder to qualify the LOAD GENERATOR, not TradeCore.

Never opens a server config, Journal, real account, Adapter or SDK. Synthetic
receipts are explicitly fixture-only; they have NO durability/recovery meaning.
Run as an unprivileged user inside a finite CPU/memory generator cgroup.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import struct
import time
from pathlib import Path
from types import SimpleNamespace

from consumer_uds_candidate_probe import run_window
from consumer_uds_candidate_shards import merge_reports
from consumer_uds_common_window_probe import validate_cpu_groups
from xuanling_tradecore.consumer.payload_v2 import order_event_payload
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)
from xuanling_tradecore.domain import (
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    RedisStreamIdV1,
    TradingDayEpoch,
)
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json

JOURNAL = "GENERATOR-FIXTURE-NOT-A-DURABLE-JOURNAL"


def frozen_page(day: str) -> dict:
    orders = []
    for n in range(100):
        trace = ConsumerTrace(
            "fixture",
            "fixture",
            "fixture",
            "fixture-H",
            f"LR-{n}",
            "fixture",
            1,
            OrderOrigin.STRATEGY,
        )
        event = ConsumerOrderEvent(
            f"evt-{n:032x}",
            ConsumerEventKind.ORDER,
            2,
            "fixture",
            TradingDayEpoch(day),
            JOURNAL,
            n + 1,
            trace,
            f"oms-{n:032x}",
            f"exec-{n:032x}",
            OmsOrderState.FILLED,
            2,
        )
        orders.append(order_event_payload(event))
    return dict(
        snapshot_id="GENERATOR-FIXTURE-PAGE",
        consumer_account_id="fixture",
        journal_id=JOURNAL,
        stream_generation=1,
        snapshot_position=ConsumerEventPositionV2(
            RedisStreamIdV1(1_000_000, 0), 0
        ).to_external_id(),
        count=100,
        orders=orders,
        has_more=False,
        next_page_cursor=None,
    )


def response(value: dict, *, serial: int, page: dict) -> dict:
    operation, payload = value["operation"], value["payload"]
    status = "COMPLETED"
    if operation == "SubmitSignal":
        status = "ACCEPTED"
        body = dict(journal_id=JOURNAL, journal_lsn=serial, oms_order_id=f"fixture-order-{serial}")
    elif operation == "GetOrderAuditTrail":
        order = payload["oms_order_id"]
        if not isinstance(order, str) or not order.startswith("fixture-order-"):
            raise ValueError("fixture only accepts its own synthetic identity")
        lsn = int(order.removeprefix("fixture-order-"))
        body = dict(audit_trail=[dict(oms_order_id=order, journal_id=JOURNAL, journal_lsn=lsn)])
    elif operation == "CreateOrderSnapshot":
        body = dict(
            order_count=100,
            page_cursor="FIXTURE-NOT-A-SIGNED-SERVER-TOKEN",
            snapshot_position=page["snapshot_position"],
            stale=False,
            projection_lag=0,
        )
    elif operation == "ReadOrderSnapshotPage":
        if payload.get("page_cursor") != "FIXTURE-NOT-A-SIGNED-SERVER-TOKEN":
            raise ValueError("fixture cursor mismatch")
        body = page
    else:
        raise ValueError("fixture does not implement real replay or trading")
    return dict(
        message_type="response",
        schema_version=2,
        request_id=value["request_id"],
        status=status,
        payload=body,
    )


def _responder(channel, socket_path: str, cpu: int, day: str):
    async def serve():
        os.sched_setaffinity(0, {cpu})
        page = frozen_page(day)
        active, serial, rejected = set(), 0, 0

        async def handle(reader, writer):
            nonlocal serial, rejected
            task = asyncio.current_task()
            if len(active) >= 2048:
                rejected += 1
                writer.close()
                return
            active.add(task)
            try:

                async def invoke():
                    nonlocal serial
                    header = await reader.readexactly(HEADER_SIZE_BYTES)
                    size = struct.unpack_from(">I", header, 24)[0]
                    if HEADER_SIZE_BYTES + size > MAX_FRAME_BYTES:
                        raise ValueError("fixture frame budget")
                    frame = decode_frame(header + await reader.readexactly(size))
                    if frame.header.message_kind is not MessageKind.REQUEST:
                        raise ValueError("fixture requires request frames")
                    serial += 1
                    reply = response(dict(frame.payload), serial=serial, page=page)
                    writer.write(
                        encode_frame(MessageKind.RESPONSE, frame.header.correlation_id, reply)
                    )
                    await writer.drain()

                await asyncio.wait_for(invoke(), timeout=1)
            except (ValueError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
                rejected += 1
            finally:
                writer.close()
                await writer.wait_closed()
                active.discard(task)

        server = await asyncio.start_unix_server(handle, path=socket_path, backlog=8192)
        os.chmod(socket_path, 0o600)
        channel.send("FIXTURE_READY")
        try:
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline and not channel.poll():
                await asyncio.sleep(0.01)
            if not channel.poll() or channel.recv() != "STOP_FIXTURE":
                raise ValueError("fixture bounded lifecycle expired")
        finally:
            server.close()
            await server.wait_closed()
            if active:
                await asyncio.wait_for(asyncio.gather(*active), timeout=2)
            channel.send(dict(served_requests=serial, rejected=rejected))

    try:
        asyncio.run(serve())
    finally:
        channel.close()


def _load(config, output, cpu_group, start, rate, duration, base, day):
    os.sched_setaffinity(0, {int(v) for v in cpu_group.split(",")})
    output.mkdir(mode=0o700)
    result = asyncio.run(
        run_window(
            config=config,
            output=output,
            duration=duration,
            rate=rate,
            workers=128,
            base=base,
            day=day,
            start_ns=start,
            fixture_only=True,
        )
    )
    raise SystemExit(int(result["failed"]))


def run_fixture(*, output: Path, groups: str, fixture_cpu: int, duration: int, day: str) -> dict:
    validate_cpu_groups(groups, str(fixture_cpu), os.sched_getaffinity(0))
    cpus = groups.split(";")
    if (
        type(duration) is not int
        or not 1 <= duration <= 30
        or 2000 % len(cpus)
        or not output.is_absolute()
    ):
        raise ValueError("invalid fixture budget")
    output.mkdir(mode=0o700, exist_ok=False)
    config = SimpleNamespace(
        socket_path=output / "fixture.sock",
        consumer_account_id="fixture",
        strategy_id="fixture",
        venue_id="NO_ADAPTER_BENCHMARK",
        adapter_instance_id="disabled-no-dispatch",
    )
    context = mp.get_context("spawn")
    parent, child = context.Pipe()
    server = context.Process(
        target=_responder, args=(child, str(config.socket_path), fixture_cpu, day)
    )
    workers = []
    result = dict(
        schema="consumer-generator-fixture-v1",
        target_kind="CONTROLLED_GENERATOR_FIXTURE",
        tradecore_performance_status="NOT_MEASURED",
        formal_qualification=False,
        passed=False,
    )
    try:
        server.start()
        child.close()
        if not parent.poll(10) or parent.recv() != "FIXTURE_READY":
            raise ValueError("fixture startup failed")
        start = time.monotonic_ns() + 10_000_000_000
        for n, group in enumerate(cpus):
            worker = context.Process(
                target=_load,
                args=(
                    config,
                    output / f"shard-{n}",
                    group,
                    start,
                    2000 // len(cpus),
                    duration,
                    n * 1_000_000,
                    day,
                ),
            )
            worker.start()
            workers.append(worker)
        deadline = time.monotonic() + 10 + duration + 65
        for worker in workers:
            worker.join(max(0, deadline - time.monotonic()))
        if any(worker.is_alive() for worker in workers):
            raise ValueError("fixture generator deadline")
        reports = [
            json.loads((output / f"shard-{n}" / "report.json").read_text())
            for n in range(len(cpus))
        ]
        merged = merge_reports(reports, start_ns=start, planned_each=2000 * duration)
        result["load"] = merged
        result["passed"] = not merged["failed"] and all(worker.exitcode == 0 for worker in workers)
    except Exception as error:
        result["failure_class"] = type(error).__name__
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(2)
            if worker.is_alive():
                worker.kill()
                worker.join(2)
        if server.pid is not None and server.is_alive():
            parent.send("STOP_FIXTURE")
            if parent.poll(5):
                result["fixture"] = parent.recv()
            server.join(5)
        if server.pid is not None and server.is_alive():
            server.terminate()
            server.join(2)
        if server.pid is not None and server.is_alive():
            server.kill()
            server.join(2)
        result["child_exits"] = [worker.exitcode for worker in workers]
        result["fixture_exit"] = server.exitcode
        result["passed"] &= server.exitcode == 0 and result.get("fixture", {}).get("rejected") == 0
        parent.close()
        child.close()
    atomic_runtime_json(output / "report.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu-groups", required=True)
    parser.add_argument("--fixture-cpu", type=int, required=True)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--trading-day", required=True)
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("fixture must run unprivileged inside its generator cgroup")
    result = run_fixture(
        output=args.output,
        groups=args.cpu_groups,
        fixture_cpu=args.fixture_cpu,
        duration=args.duration,
        day=args.trading_day,
    )
    print(json.dumps(result, separators=(",", ":")))
    return int(not result["passed"])


if __name__ == "__main__":
    raise SystemExit(main())
