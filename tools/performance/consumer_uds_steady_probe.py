"""Open-loop fresh-connection UDS diagnostic; never dispatches to an Adapter.

This complements (does not replace) the external three-round acceptance runner.
It preserves scheduled, sent and completed timestamps and never retries rejects.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import uuid
from collections import Counter
from pathlib import Path
from time import monotonic_ns

from consumer_uds_burst_probe import _exchange, _percentile
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame


async def run(args: argparse.Namespace) -> None:
    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("steady probe requires the no-Adapter benchmark binding")
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < args.max_inflight + 64:
        raise RuntimeError("generator file descriptor limit is too low")
    run_id = uuid.uuid4().hex
    # systemctl reports process start before bind completes. Prove the actual
    # authorized request path is ready before starting either ramp or window.
    preflight_frame = encode_frame(
        MessageKind.REQUEST,
        0,
        {
            "message_type": "request",
            "schema_version": 2,
            "operation": "ListAccounts",
            "request_id": f"preflight-{run_id}",
            "payload": {},
        },
    )
    ready_deadline = monotonic_ns() + 5_000_000_000
    while True:
        try:
            preflight = await asyncio.wait_for(
                _exchange(path=config.socket_path, frame=preflight_frame, sample={}), 1.0
            )
        except (FileNotFoundError, ConnectionRefusedError):
            if monotonic_ns() >= ready_deadline:
                raise RuntimeError("UDS listener did not become ready before the ramp") from None
            await asyncio.sleep(0.05)
        else:
            if preflight["status"] != "COMPLETED":
                raise RuntimeError("UDS ListAccounts preflight failed")
            break
    samples = []
    pending: set[asyncio.Task[None]] = set()
    total = args.rate * (args.seconds + args.ramp)
    start = monotonic_ns()
    window_start = start + args.ramp * 1_000_000_000
    window_end = window_start + args.seconds * 1_000_000_000

    async def one(index: int, planned: int) -> None:
        sample = {
            "sample_seq": index + 1,
            "planned_ns": planned,
            "phase": "measurement" if planned >= window_start else "ramp",
            "start_ns": monotonic_ns(),
        }
        request = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "SubmitSignal",
            "request_id": f"steady-{run_id}-{index}",
            "payload": {
                "consumer_account_id": config.consumer_account_id,
                "strategy_id": config.strategy_id,
                "strategy_order_id": f"steady-{run_id}-{index}",
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
        try:
            await asyncio.wait_for(
                _exchange(
                    path=config.socket_path,
                    frame=encode_frame(MessageKind.REQUEST, index + 1, request),
                    sample=sample,
                ),
                args.timeout,
            )
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as exc:
            sample["done_ns"] = monotonic_ns()
            sample["status"] = type(exc).__name__
        samples.append(sample)

    for index in range(total):
        planned = start + index * 1_000_000_000 // args.rate
        delay = (planned - monotonic_ns()) / 1_000_000_000
        if delay > 0:
            await asyncio.sleep(delay)
        if len(pending) >= args.max_inflight:
            samples.append(
                {
                    "sample_seq": index + 1,
                    "planned_ns": planned,
                    "phase": "measurement" if planned >= window_start else "ramp",
                    "start_ns": monotonic_ns(),
                    "done_ns": monotonic_ns(),
                    "status": "GENERATOR_OVERLOADED",
                }
            )
            continue
        task = asyncio.create_task(one(index, planned))
        pending.add(task)
        task.add_done_callback(pending.discard)
        # Catch-up work is bounded and must let socket completions run.
        if index % 32 == 0:
            await asyncio.sleep(0)
    if pending:
        await asyncio.gather(*pending)
    measurement = [s for s in samples if s["phase"] == "measurement"]
    accepted = [s for s in measurement if s["status"] == "ACCEPTED"]
    latencies = [(s["done_ns"] - s["start_ns"]) / 1_000_000 for s in accepted]
    lsns = [s.get("journal_lsn") for s in accepted]
    summary = {
        "profile": "NO_ADAPTER_UDS_STEADY_DIAGNOSTIC",
        "rate": args.rate,
        "seconds": args.seconds,
        "ramp_seconds": args.ramp,
        "window_start_ns": window_start,
        "window_end_ns": window_end,
        "offered_measurement": len(measurement),
        "measurement_statuses": dict(Counter(s["status"] for s in measurement)),
        "ramp_statuses": dict(Counter(s["status"] for s in samples if s["phase"] == "ramp")),
        "preflight": "ListAccounts COMPLETED before ramp",
        "measurement_sent_in_window": sum(
            window_start <= s.get("sent_ns", 0) < window_end for s in measurement
        ),
        "accepted_tps_in_window": sum(s["done_ns"] < window_end for s in accepted) / args.seconds,
        "unique_accepted_lsn": len(set(lsns)) if all(v is not None for v in lsns) else None,
        "accepted_latency_ms": {
            f"p{p}": _percentile(latencies, p / 100) for p in (50, 95, 99, 100)
        },
        "generator_lateness_p99_ms": _percentile(
            [(s["start_ns"] - s["planned_ns"]) / 1_000_000 for s in measurement], 0.99
        ),
    }
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (args.output / "samples.ndjson").write_text(
        "".join(json.dumps(s) + "\n" for s in sorted(samples, key=lambda s: s["sample_seq"])),
        encoding="utf-8",
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--rate", type=int, default=2_000)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--ramp", type=int, default=2)
    parser.add_argument("--max-inflight", type=int, default=256)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (
        0 < args.rate <= 10_000
        and 0 < args.seconds <= 120
        and 0 <= args.ramp <= 10
        and 0 < args.max_inflight <= 1_024
        and 0 < args.timeout <= 60
    ):
        parser.error("probe limits are outside the bounded diagnostic range")
    asyncio.run(run(args))
