"""Offline component-chain load: batch routing -> scheduler -> fsync -> fake broker -> ACK.

No SDK, credentials, live account or network service is loaded. This measures the
local composition, not a production OMS risk/fence deployment or real C0-C7 SLA.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from time import perf_counter_ns, process_time_ns, sleep

from emt_gateway.gateway import CommandState, OrderGateway, OrderIntent
from emt_gateway.journal import FileJournal
from tools.quality.run_acceptance import revision
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


def arrival_ns(started: int, index: int, rate: float) -> int:
    """Fixed open-loop schedule: completions never move the next arrival."""
    return started + round(index * 10**9 / rate)


def storage_info(directory: Path) -> dict:
    info = {"path": str(directory), "free_bytes": shutil.disk_usage(directory).free}
    if platform.system() == "Linux":
        try:
            result = subprocess.run(
                [
                    "findmnt",
                    "--json",
                    "--target",
                    str(directory),
                    "--output",
                    "TARGET,FSTYPE,SOURCE",
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if result.returncode == 0:
                info["mount"] = json.loads(result.stdout)
        except (OSError, ValueError, subprocess.TimeoutExpired):
            info["mount"] = "unavailable"
    return info


def peak_rss_bytes() -> int | None:
    """Process lifetime peak, including imports/measurement storage; not a live OMS RSS limit."""
    if platform.system() == "Linux":
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (name, ctypes.c_size_t)
                for name in (
                    "PeakWorkingSetSize",
                    "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage",
                    "PagefileUsage",
                    "PeakPagefileUsage",
                )
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        process = ctypes.windll.kernel32.GetCurrentProcess
        process.restype = wintypes.HANDLE
        memory = ctypes.windll.psapi.GetProcessMemoryInfo
        memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        memory.restype = wintypes.BOOL
        if memory(process(), ctypes.byref(counters), counters.cb):
            return counters.PeakWorkingSetSize
    return None


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


def run_pipeline(
    *,
    orders=200,
    batch_size=32,
    capacity=128,
    broker_delay_us=0,
    workers=1,
    target_rate=0.0,
    deadline_ms=None,
    journal_root=None,
):
    if not 1 <= orders <= 100000 or not 1 <= batch_size <= 4096 or not 1 <= capacity <= 100000:
        raise ValueError("invalid offline workload bounds")
    if not 0 <= broker_delay_us <= 1000000 or type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("invalid broker delay or workers")
    if not math.isfinite(target_rate) or not 0 <= target_rate <= 10**6:
        raise ValueError("target rate must be finite and between zero and 1000000")
    if deadline_ms is not None and (not math.isfinite(deadline_ms) or deadline_ms <= 0):
        raise ValueError("deadline must be finite and positive")
    if target_rate and (orders - 1) / target_rate > 1800:
        raise ValueError("paced offer window exceeds 30 minutes")
    source = revision()
    native = importlib.util.find_spec("xuntrade_omsems._native")
    native_sha256 = (
        hashlib.sha256(Path(native.origin).read_bytes()).hexdigest()
        if native and native.origin
        else None
    )
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
    lateness, rejection_latency, completions = [], [], []
    completed = rejected = 0
    with TemporaryDirectory(prefix="xuntrade-offline-", dir=journal_root) as directory:
        storage = storage_info(Path(directory))
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
                            finished = perf_counter_ns()
                            latency.append(finished - starts[item.execution_id])
                            completions.append(finished)
                            scheduler.release_new_order_inflight(item.execution_id)
                            completed += 1
                        if not progress:
                            if done.is_set():
                                return
                            wake.wait(0.05)
                except BaseException as exc:
                    errors.append(exc)

            started = perf_counter_ns()
            cpu_started = process_time_ns()
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
                    for index, (item, lane) in enumerate(zip(batch, selected, strict=True), offset):
                        scheduled = (
                            arrival_ns(started, index, target_rate)
                            if target_rate
                            else batch_started
                        )
                        if target_rate:
                            remaining = scheduled - perf_counter_ns()
                            if remaining > 0:
                                sleep(remaining / 10**9)
                        starts[item.execution_id] = scheduled
                        now = perf_counter_ns()
                        lateness.append(max(0, now - scheduled))
                        decision = lane.scheduler.admit_and_commit_new_order(
                            request_id=item.consumer_trace.strategy_order_id,
                            serialized_size_bytes=256,
                            admitted_monotonic_ns=now,
                            checked_monotonic_ns=now,
                            instruction=item,
                        )
                        if isinstance(decision, IngressRejection):
                            rejected += 1
                            rejection_latency.append(perf_counter_ns() - scheduled)
                        wake.set()
                    if errors:
                        raise RuntimeError("offline worker failed") from errors[0]
                offered_until = perf_counter_ns()
            finally:
                done.set()
                wake.set()
                worker.join()
            if errors:
                raise RuntimeError("offline worker failed") from errors[0]
            elapsed = perf_counter_ns() - started
            cpu_elapsed = process_time_ns() - cpu_started
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
                "processor": platform.processor(),
                "logical_cpus": os.cpu_count(),
                "cpu_affinity": sorted(os.sched_getaffinity(0))
                if hasattr(os, "sched_getaffinity")
                else None,
                "source": source,
                "native_binary_sha256": native_sha256,
                "environment_kind": "wsl"
                if "microsoft" in platform.release().lower()
                else platform.system().lower(),
                "storage": storage,
                "batch_size": batch_size,
                "capacity_per_lane": capacity,
                "routing_workers": workers,
                "arrival_profile": "open-loop fixed rate"
                if target_rate
                else "unpaced bounded burst",
                "target_rate_per_second": target_rate,
                "scheduled_offer_window_ns": round(orders * 10**9 / target_rate)
                if target_rate
                else None,
                "last_scheduled_arrival_ns": round((orders - 1) * 10**9 / target_rate)
                if target_rate
                else None,
                "actual_offer_window_ns": offered_until - started,
                "actual_offered_per_second": (orders - 1) * 10**9 / (offered_until - started)
                if orders > 1
                else None,
                "scope": "offline components only; simulated broker; no live risk/fence/SDK/network",
                "backend": backend_info(),
                "offered": orders,
                "completed": completed,
                "rejected": rejected,
                "fake_broker_calls": broker.calls,
                "elapsed_ns": elapsed,
                "process_cpu_ns": cpu_elapsed,
                "process_lifetime_peak_rss_bytes": peak_rss_bytes(),
                "rss_scope": "whole process, including imports and bounded benchmark history; not steady-state OMS memory",
                "completion_ratio": completed / orders,
                "rejection_ratio": rejected / orders,
                "throughput_per_second": completed * 10**9 / elapsed,
                "completed_during_offer": sum(t <= offered_until for t in completions),
                "completed_during_offer_per_second": sum(t <= offered_until for t in completions)
                * 10**9
                / (offered_until - started),
                "drain_ns": max(
                    0, (max(completions) if completions else offered_until) - offered_until
                ),
                "deadline_ms": deadline_ms,
                "within_deadline": sum(t <= deadline_ms * 10**6 for t in latency)
                if deadline_ms is not None
                else None,
                "within_deadline_ratio_of_offered": sum(t <= deadline_ms * 10**6 for t in latency)
                / orders
                if deadline_ms is not None
                else None,
                "latency_population": "completed only; scheduled arrival through durable callback ACK; rejected requests excluded",
                "latency": distribution(latency),
                "generator_lateness": distribution(lateness),
                "rejection_latency": distribution(rejection_latency),
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
    parser.add_argument(
        "--target-rate", type=float, default=0, help="orders/s; zero is unpaced burst"
    )
    parser.add_argument(
        "--deadline-ms", type=float, help="experimental measurement threshold, not a production SLA"
    )
    parser.add_argument(
        "--journal-root", type=Path, help="existing directory on the storage device to measure"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run_pipeline(
        orders=args.orders,
        batch_size=args.batch_size,
        capacity=args.capacity,
        broker_delay_us=args.broker_delay_us,
        workers=args.workers,
        target_rate=args.target_rate,
        deadline_ms=args.deadline_ms,
        journal_root=args.journal_root,
    )
    payload = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
