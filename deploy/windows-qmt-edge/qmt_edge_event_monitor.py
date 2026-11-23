"""Durably verify fresh PROBE init/timer/optional stop, without QMT actions."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from pathlib import Path

from xuanling_tradecore.adapters.qmt.edge_config import QmtEdgeConfigV1
from xuanling_tradecore.adapters.qmt.edge_runtime import QmtEdgeRuntime
from xuanling_tradecore.adapters.qmt.native_engine_client import QmtNativeEventPending
from xuanling_tradecore.adapters.qmt.native_engine_contract import RING_ABI_VERSION
from xuanling_tradecore.adapters.qmt.probe_evidence import (
    QmtProbeEvidence,
    decode_durable_native_frame,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--seconds", required=True, type=float)
    parser.add_argument("--min-timer-samples", default=3, type=int)
    parser.add_argument("--max-timer-gap-seconds", default=5.0, type=float)
    parser.add_argument("--expect-stop", action="store_true")
    arguments = parser.parse_args()
    runtime: QmtEdgeRuntime | None = None
    try:
        if not math.isfinite(arguments.seconds) or not 0 < arguments.seconds <= 3600:
            raise RuntimeError("invalid duration")
        gap = arguments.max_timer_gap_seconds
        if not math.isfinite(gap) or not 0 < gap <= arguments.seconds:
            raise RuntimeError("invalid maximum timer gap")
        evidence = QmtProbeEvidence(
            min_timer_samples=arguments.min_timer_samples,
            max_timer_gap_ns=int(gap * 1e9),
            expect_stop=arguments.expect_stop,
        )
        config = QmtEdgeConfigV1.load(arguments.config)
        if not config.is_disarmed or config.native_policy is not None:
            raise RuntimeError("P0 monitor requires PROBE, mutation=false, native_policy=null")
        runtime = QmtEdgeRuntime.open(config)
        if runtime.event_ring.snapshot().depth != 0 or runtime.command_ring.snapshot().depth != 0:
            raise RuntimeError("probe requires empty rings; preserve and review pending evidence")
        print(
            json.dumps(
                {
                    "status": "MONITOR_READY",
                    "ring_abi": RING_ABI_VERSION,
                    "generation_hex": config.generation.hex(),
                    "duration_seconds": arguments.seconds,
                    "min_timer_samples": evidence.min_timer_samples,
                    "max_timer_gap_ns": evidence.max_timer_gap_ns,
                    "expect_stop": evidence.expect_stop,
                    "stop_quiet_ns": evidence.stop_quiet_ns,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        deadline = time.monotonic() + arguments.seconds
        events = 0
        while time.monotonic() < deadline:
            try:
                record = runtime.native_client.drain_one_event()
            except QmtNativeEventPending:
                evidence.check_alive(time.monotonic_ns())
                time.sleep(0.002)
                continue
            frame = decode_durable_native_frame(record.payload)
            sample = evidence.observe(frame, received_ns=time.monotonic_ns())
            events += 1
            sample.update(
                delivery_id=record.delivery_id,
                payload_hash=record.payload_hash,
                status="NATIVE_EVENT_DURABLE",
            )
            print(json.dumps(sample, sort_keys=True), flush=True)
        result = evidence.finish(now_ns=time.monotonic_ns())
        result.update(events=events, status="MONITOR_COMPLETE")
        print(json.dumps(result, sort_keys=True), flush=True)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(
            json.dumps({"status": "MONITOR_BLOCKED", "reason": str(error)}, sort_keys=True),
            flush=True,
        )
        return 2
    finally:
        if runtime is not None:
            runtime.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
