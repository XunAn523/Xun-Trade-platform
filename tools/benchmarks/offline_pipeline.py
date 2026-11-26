"""Offline component-chain load: batch routing -> scheduler -> fsync -> fake broker -> ACK.

No SDK, credentials, live account or network service is loaded. This measures the
local composition, not a production OMS risk/fence deployment or real C0-C7 SLA.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from time import perf_counter_ns, sleep

from emt_gateway.gateway import CommandState, OrderGateway, OrderIntent
from emt_gateway.journal import FileJournal
from xuanling_tradecore.domain import (
    CancelDeadlinePolicy,
    ConsumerTrace,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    AccountFenceScope,
    AccountSessionShardBinding,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    IngressRejection,
)
from xuntrade_omsems.compute import backend_info


def distribution(values: list[int]) -> dict[str, int]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50_ns": 0, "p95_ns": 0, "p99_ns": 0, "max_ns": 0}
    return {
        "count": len(values),
        **{f"p{p}_ns": ordered[math.ceil(len(values) * p / 100) - 1] for p in (50, 95, 99)},
        "max_ns": ordered[-1],
    }


def make_router(capacity: int) -> AccountSessionShardRouter:
    shards = tuple(
        AccountSessionShardBinding(
            shard_id=f"offline-{i}",
            account_id="offline-account",
            venue_id="PAPER",
            adapter_instance_id="offline-broker",
            adapter_client_scope=str(i),
            ownership_scope_hash=AccountFenceScope("offline-account", "PAPER", str(i)).scope_hash,
            mapping_manifest_hash="a" * 64,
            capability_manifest_hash="b" * 64,
            journal_plan_hash="c" * 64,
            journal_routing_epoch=1,
            journal_shard_id=i,
        )
        for i in range(3)
    )
    plan = AccountSessionShardPlan("offline-plan", 1, "d" * 64, "offline-account", shards)
    schedulers = {
        shard.shard_id: ExecutionScheduler(
            account_id="offline-account",
            process_epoch_id="offline-process",
            policy=ExecutionSchedulerPolicy(
                "offline", 1, "a" * 64, capacity, capacity, capacity, capacity, 4096, 4096
            ),
            execution_deadline_policy=ExecutionDeadlinePolicy("offline", 1, "b" * 64, 3600 * 10**9),
            cancel_deadline_policy=CancelDeadlinePolicy("offline", 1, "c" * 64, 3600 * 10**9),
        )
        for shard in shards
    }
    return AccountSessionShardRouter(plan=plan, schedulers=schedulers)


def instruction(index: int) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"offline-{index}",
        oms_order_id=f"offline-oms-{index}",
        account_id="offline-account",
        trading_day=TradingDayEpoch("2026-11-26"),
        consumer_trace=ConsumerTrace(
            "offline-consumer",
            "offline-consumer-account",
            "offline-account",
            "offline-strategy",
            f"offline-request-{index}",
            "offline-binding",
            1,
            OrderOrigin.STRATEGY,
        ),
        symbol="OFFLINE",
        market="TEST",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=1,
        protection_price_ticks=100,
        limit_price_ticks=100,
    )


class _Journal:
    def __init__(self, journal: FileJournal):
        self.delegate = journal
        self.last = None
        self.flush: list[int] = []

    def append(self, **fields):
        record = self.delegate.append(**fields)
        self.last = record
        self.flush.append(self.delegate.last_append_timing.flush_ns)
        return record


class _Trace:
    def __init__(self):
        self.stages: dict[str, dict[str, int]] = {}

    def begin(self, correlation_id, **identity):
        self.stages[correlation_id] = {"C0": perf_counter_ns()}

    def mark(self, correlation_id, stage, timestamp_ns=None):
        self.stages[correlation_id][stage] = (
            perf_counter_ns() if timestamp_ns is None else timestamp_ns
        )

    def complete(self, *args, **kwargs):
        pass

    def record_metric(self, *args, **kwargs):
        pass


class _Broker:
    def __init__(self, journal: _Journal, delay_us: int):
        self.journal, self.delay_us, self.calls = journal, delay_us, 0

    def insert_order(self, *, command_id, client_order_id):
        if self.journal.last.kind != "CALL_STARTED" or self.journal.last.command_id != command_id:
            raise RuntimeError("fake broker reached before durable CALL_STARTED")
        if self.delay_us:
            sleep(self.delay_us / 10**6)
        self.calls += 1
        return self.calls

    def get_last_error_code(self):
        return 0


def run_pipeline(*, orders=200, batch_size=32, capacity=128, broker_delay_us=0, workers=1):
    if not 1 <= orders <= 100000 or not 1 <= batch_size <= 4096 or not 1 <= capacity <= 100000:
        raise ValueError("invalid offline workload bounds")
    if not 0 <= broker_delay_us <= 1000000 or type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("invalid broker delay or workers")
    router = make_router(capacity)
    lanes = router.route_execution_ids(
        account_id="offline-account", execution_ids=tuple(f"lane-{i}" for i in range(32))
    )
    schedulers = {lane.binding.shard_id: lane.scheduler for lane in lanes}
    if len(schedulers) != 3:
        raise RuntimeError("offline routing fixture did not cover all lanes")
    wake, done = Event(), Event()
    errors: list[BaseException] = []
    starts: dict[str, int] = {}
    latency, queue_wait, routing = [], [], []
    completed = rejected = 0
    with TemporaryDirectory(prefix="xuntrade-offline-") as directory:
        with (
            FileJournal(Path(directory) / "commands.emtj") as command_file,
            FileJournal(Path(directory) / "events.emtj") as event_file,
        ):
            commands, events = _Journal(command_file), _Journal(event_file)
            broker, trace = _Broker(commands, broker_delay_us), _Trace()
            gateway = OrderGateway(
                native=broker,
                command_journal=commands,
                event_journal=events,
                trace=trace,
                callback_capacity=16,
            )

            def drain():
                nonlocal completed
                try:
                    while True:
                        wake.clear()
                        progress = False
                        for scheduler in schedulers.values():
                            dispatch = scheduler.dequeue(checked_monotonic_ns=perf_counter_ns())
                            if dispatch is None:
                                continue
                            progress = True
                            if dispatch.expired_before_native:
                                raise RuntimeError("offline command expired")
                            item = dispatch.command
                            queue_wait.append(dispatch.queue_wait_ns)
                            result = gateway.submit_order(OrderIntent(item.execution_id, "a" * 64))
                            if result.state is not CommandState.NATIVE_ACCEPTED:
                                raise RuntimeError("offline submit did not succeed")
                            gateway.callbacks.receive(
                                "OnOrderEvent",
                                profile="ordinary",
                                workload="steady",
                                order_emt_id=result.native_id,
                            )
                            delivery = gateway.callbacks.normalize_one()
                            if delivery is None:
                                raise RuntimeError("offline callback missing")
                            gateway.callbacks.ack(delivery.delivery_id)
                            latency.append(perf_counter_ns() - starts[item.execution_id])
                            scheduler.release_new_order_inflight(item.execution_id)
                            completed += 1
                        if not progress:
                            if done.is_set():
                                return
                            wake.wait(0.05)
                except BaseException as exc:
                    errors.append(exc)

            started = perf_counter_ns()
            worker = Thread(target=drain, name="offline-dispatch")
            worker.start()
            try:
                for offset in range(0, orders, batch_size):
                    batch_started = perf_counter_ns()
                    batch = tuple(
                        instruction(i) for i in range(offset, min(orders, offset + batch_size))
                    )
                    selected = router.route_executions(batch, workers=workers)
                    routing.append(perf_counter_ns() - batch_started)
                    for item, lane in zip(batch, selected, strict=True):
                        starts[item.execution_id] = batch_started
                        now = perf_counter_ns()
                        decision = lane.scheduler.admit_and_commit_new_order(
                            request_id=item.consumer_trace.strategy_order_id,
                            serialized_size_bytes=256,
                            admitted_monotonic_ns=now,
                            checked_monotonic_ns=now,
                            instruction=item,
                        )
                        rejected += isinstance(decision, IngressRejection)
                    wake.set()
                    if errors:
                        raise RuntimeError("offline worker failed") from errors[0]
            finally:
                done.set()
                wake.set()
                worker.join()
            if errors:
                raise RuntimeError("offline worker failed") from errors[0]
            elapsed = perf_counter_ns() - started
            if completed + rejected != orders or broker.calls != completed:
                raise RuntimeError("offline workload accounting mismatch")
            c0_c7 = [
                row["C7"] - row["C0"]
                for row in trace.stages.values()
                if "C0" in row and "C4" in row and "C7" in row
            ]
            return {
                "measured_at_utc": datetime.now(timezone.utc).isoformat(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "batch_size": batch_size,
                "capacity_per_lane": capacity,
                "routing_workers": workers,
                "arrival_profile": "unpaced bounded burst",
                "scope": "offline components only; simulated broker; no live risk/fence/SDK/network",
                "backend": backend_info(),
                "offered": orders,
                "completed": completed,
                "rejected": rejected,
                "fake_broker_calls": broker.calls,
                "elapsed_ns": elapsed,
                "throughput_per_second": completed * 10**9 / elapsed,
                "latency": distribution(latency),
                "queue_wait": distribution(queue_wait),
                "routing_and_domain_batch": distribution(routing),
                "journal_flush": distribution(commands.flush + events.flush),
                "gateway_c0_c7": distribution(c0_c7),
                "broker_delay_us": broker_delay_us,
            }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--capacity", type=int, default=128)
    parser.add_argument("--broker-delay-us", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run_pipeline(
        orders=args.orders,
        batch_size=args.batch_size,
        capacity=args.capacity,
        broker_delay_us=args.broker_delay_us,
        workers=args.workers,
    )
    payload = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
