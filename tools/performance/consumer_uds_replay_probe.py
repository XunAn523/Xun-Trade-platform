"""Read-only partition/trace deployment probe, NOT checkpointed S2 acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from time import monotonic_ns
from typing import Any

from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.replay_evidence_v2 import build_replay_partition_evidence_v2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.domain import ConsumerEventPositionV2


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--partitions", type=int, default=32, choices=range(1, 65))
    parser.add_argument("--limit", type=int, default=100, choices=range(1, 10001))
    parser.add_argument("--expected-events", type=int, required=True)
    parser.add_argument(
        "--contract-only",
        action="store_true",
        help="Export validated plan bounds without draining any events",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("probe requires no-Adapter binding")
    run_id = uuid.uuid4().hex

    def call(client: ConsumerUnixClientV2, operation: str, body: dict[str, Any], request_id: str):
        start = monotonic_ns()
        reply = client.call(
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": operation,
                "request_id": request_id,
                "payload": body,
            }
        )
        end = monotonic_ns()
        if reply.get("status") != "COMPLETED":
            raise RuntimeError(str(reply.get("code", "not completed")))
        return reply["payload"], {
            "request_id_sha256": hashlib.sha256(request_id.encode()).hexdigest(),
            "call_started_ns": start,
            "call_finished_ns": end,
            "latency_ns": end - start,
        }

    client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 120))
    plan, plan_sample = call(
        client,
        "CreateReplayPartitions",
        {
            "consumer_account_id": config.consumer_account_id,
            "partition_count": args.partitions,
        },
        f"probe-{run_id}-plan",
    )
    plan_evidence = build_replay_partition_evidence_v2(plan)
    plan_evidence["request_sample"] = plan_sample
    if plan["event_count"] != args.expected_events:
        raise RuntimeError("unexpected seed dataset")
    if args.contract_only:
        result = {
            "profile": "partition-contract-probe",
            "run_id": run_id,
            "checkpoint_evidence": False,
            "plan_evidence": plan_evidence,
        }
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(result, output, separators=(",", ":"))
        print(json.dumps(result))
        return
    if not plan["partitions"]:
        raise RuntimeError("empty seed dataset cannot be drained")
    barrier = Barrier(len(plan["partitions"]))

    def drain(partition):
        worker = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, 10))
        token = partition["cursor"]
        previous = (
            ConsumerEventPositionV2.from_external_id(partition["after_position"])
            if partition["after_position"]
            else None
        )
        high = ConsumerEventPositionV2.from_external_id(partition["high_watermark_position"])
        ids, rows = [], []
        barrier.wait(timeout=10)
        while True:
            body, row = call(
                worker,
                "PollEvents",
                {"cursor": token, "limit": args.limit},
                f"probe-{run_id}-{partition['partition_id']}-{len(rows)}",
            )
            if body["high_watermark_position"] != partition["high_watermark_position"]:
                raise RuntimeError("partition fence changed")
            for event in body["events"]:
                position = ConsumerEventPositionV2.from_external_id(event["position"])
                if (previous is not None and position <= previous) or position > high:
                    raise RuntimeError("invalid event order/bounds")
                ids.append(event["event_id"])
                previous = position
            row.update(partition_id=partition["partition_id"], event_count=len(body["events"]))
            rows.append(row)
            token = body["next_cursor"]
            if not body["has_more"]:
                break
        if len(ids) != partition["event_count"] or previous != high:
            raise RuntimeError("partition count/end mismatch")
        return ids, rows

    with ThreadPoolExecutor(max_workers=len(plan["partitions"])) as executor:
        results = list(executor.map(drain, plan["partitions"]))
    ids = [event for events, _ in results for event in events]
    rows = [row for _, samples in results for row in samples]
    if len(ids) != len(set(ids)) or len(ids) != args.expected_events:
        raise RuntimeError("duplicate or missing events")
    latencies = sorted(r["latency_ns"] for r in rows)
    result = {
        "profile": "partition-read-deployment-probe",
        "checkpoint_evidence": False,
        "run_id": run_id,
        "journal_id": plan["journal_id"],
        "plan_evidence": plan_evidence,
        "event_count": len(ids),
        "unique_event_count": len(set(ids)),
        "partitions": len(plan["partitions"]),
        "limit": args.limit,
        "rpc_count": len(rows),
        "samples": rows,
        "start_ns": min(r["call_started_ns"] for r in rows),
        "end_ns": max(r["call_finished_ns"] for r in rows),
        "rpc_latency_ms": {
            f"P{p}": latencies[math.ceil(len(latencies) * p / 100) - 1] / 1e6
            for p in (50, 95, 99, 100)
        },
    }
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(result, output, separators=(",", ":"))
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}))


if __name__ == "__main__":
    main()
