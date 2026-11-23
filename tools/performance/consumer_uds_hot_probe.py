"""Serial hot Consumer V2 UDS probe; it never manages or mutates the server.

The process behaves exactly like one upstream strategy consumer: every sample
uses the public XTC2 request/response contract over AF_UNIX and there is never
more than one request in flight.  It cannot restart/reset TradeCore, write its
Journal, inspect server traces, or dispatch an Adapter directly.
"""

from __future__ import annotations

import argparse
import json
import math
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns
from typing import Any

from perfbench.short_probe_contract import tool_revision
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.runtime_profile import ConsumerRuntimeProfile
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity

HOT_LATENCY_SLO_PROFILE = "fixed-multifrontend-fresh-request-v1"
HOT_P95_LIMIT_MS = 1.50
HOT_P99_LIMIT_MS = 3.00


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
        "p99_9": _percentile(values, 0.999) if len(values) >= 10_000 else None,
        "p100": _percentile(values, 1.00),
    }


def _latency_slo_passed(latency: dict[str, float | None], accepted: int, offered: int) -> bool:
    return (
        accepted == offered
        and latency["p95"] is not None
        and latency["p95"] <= HOT_P95_LIMIT_MS
        and latency["p99"] is not None
        and latency["p99"] <= HOT_P99_LIMIT_MS
    )


def _valid_trading_day(value: str) -> bool:
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _request(
    *,
    request_id: str,
    consumer_account_id: str,
    strategy_id: str,
    strategy_order_id: str,
    trading_day: str,
) -> dict[str, object]:
    return {
        "message_type": "request",
        "schema_version": 2,
        "operation": "SubmitSignal",
        "request_id": request_id,
        "payload": {
            "consumer_account_id": consumer_account_id,
            "strategy_id": strategy_id,
            "strategy_order_id": strategy_order_id,
            "trading_day": trading_day,
            "symbol": "TEST",
            "market": "SIM",
            "side": "BUY",
            "price_type": "LIMIT",
            "business_type": "CASH",
            "quantity": 1,
            "protection_price_ticks": 1,
        },
    }


@dataclass(frozen=True)
class HotCallObservation:
    status: str
    started_ns: int
    finished_ns: int
    journal_lsn: int | None

    @property
    def latency_ns(self) -> int:
        return self.finished_ns - self.started_ns


def _call_one(
    client: ConsumerUnixClientV2,
    request: dict[str, object],
) -> HotCallObservation:
    started_ns = monotonic_ns()
    try:
        response = client.call(request)
    except Exception as exc:  # retain the public client failure class, not payloads
        return HotCallObservation(type(exc).__name__, started_ns, monotonic_ns(), None)
    status = str(response.get("status", response.get("code", "INVALID_RESPONSE")))
    payload = response.get("payload")
    journal_lsn = payload.get("journal_lsn") if isinstance(payload, dict) else None
    if status == "ACCEPTED" and (
        response.get("request_id") != request.get("request_id")
        or response.get("message_type") != "response"
        or response.get("schema_version") != 2
        or not isinstance(payload, dict)
        or type(journal_lsn) is not int
        or journal_lsn < 1
        or not isinstance(payload.get("oms_order_id"), str)
        or not payload.get("oms_order_id")
        or not isinstance(payload.get("journal_id"), str)
        or not payload.get("journal_id")
    ):
        status, journal_lsn = "INVALID_RESPONSE", None
    finished_ns = monotonic_ns()
    return HotCallObservation(
        status,
        started_ns,
        finished_ns,
        journal_lsn if type(journal_lsn) is int and journal_lsn > 0 else None,
    )


def hot_request(config, *, run_id, sequence, phase, day, candidate, base):
    value = _request(
        request_id=f"hot-{phase}-{run_id}-{sequence}",
        consumer_account_id=config.consumer_account_id,
        strategy_id=config.strategy_id,
        strategy_order_id=f"hot-{phase}-{run_id}-{sequence}",
        trading_day=day,
    )
    if candidate:
        from consumer_uds_candidate_probe import signal_payload

        value["payload"] = signal_payload(config, base + sequence, day)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--warmup", type=int, default=10_000)
    parser.add_argument("--samples", type=int, default=10_000)
    parser.add_argument("--subround-size", type=int, default=1_000)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--round", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--persistent-candidate", action="store_true")
    parser.add_argument("--sequence-base", type=int)
    args = parser.parse_args()
    if not (
        0 <= args.warmup <= 100_000
        and 1 <= args.samples <= 100_000
        and 1 <= args.subround_size <= args.samples
        and args.samples % args.subround_size == 0
        and 0 < args.timeout <= 60
        and _valid_trading_day(args.trading_day)
    ):
        parser.error(
            "hot probe limits are invalid, samples do not divide into subrounds, "
            "or trading-day is not ISO YYYY-MM-DD"
        )

    config = load_consumer_uds_server_config_v2(args.config)
    if (
        config.venue_id != "NO_ADAPTER_BENCHMARK"
        or config.adapter_instance_id != "disabled-no-dispatch"
    ):
        raise RuntimeError("hot probe requires the no-Adapter benchmark binding")
    if args.persistent_candidate != (
        config.runtime_profile is ConsumerRuntimeProfile.LONG_RUNNING
    ) or (
        args.persistent_candidate
        and (
            args.sequence_base is None
            or not 0 <= args.sequence_base < 10**17 - args.warmup - args.samples
        )
    ):
        raise RuntimeError("persistent HOT requires explicit mock profile and bounded LR namespace")
    args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    client = ConsumerUnixClientV2(
        ConsumerUnixClientConfigV2(config.socket_path, timeout_seconds=args.timeout)
    )
    run_id = uuid.uuid4().hex
    revision = tool_revision()

    preflight = client.call(
        {
            "message_type": "request",
            "schema_version": 2,
            "operation": "ListAccounts",
            "request_id": f"hot-preflight-{run_id}",
            "payload": {},
        }
    )
    if preflight.get("status") != "COMPLETED":
        raise RuntimeError("Consumer ListAccounts preflight did not complete")

    warmup_statuses: Counter[str] = Counter()
    for sequence in range(args.warmup):
        observed = _call_one(
            client,
            hot_request(
                config,
                run_id=run_id,
                sequence=sequence,
                phase="warmup",
                day=args.trading_day,
                candidate=args.persistent_candidate,
                base=args.sequence_base,
            ),
        )
        warmup_statuses[observed.status] += 1
    if warmup_statuses != Counter({"ACCEPTED": args.warmup}):
        raise RuntimeError(f"hot probe warmup was not clean: {dict(warmup_statuses)}")

    samples: list[dict[str, Any]] = []
    started_ns = monotonic_ns()
    for sequence in range(args.samples):
        value = hot_request(
            config,
            run_id=run_id,
            sequence=sequence,
            phase="measure",
            day=args.trading_day,
            candidate=args.persistent_candidate,
            base=args.sequence_base + args.warmup if args.persistent_candidate else None,
        )
        observed = _call_one(
            client,
            value,
        )
        samples.append(
            {
                "sample_seq": sequence + 1,
                "subround": sequence // args.subround_size + 1,
                "call_started_ns": observed.started_ns,
                "call_finished_ns": observed.finished_ns,
                "latency_ns": observed.latency_ns,
                "status": observed.status,
                "journal_lsn": observed.journal_lsn,
                "request_id_sha256": sha256(str(value["request_id"]).encode()).hexdigest(),
            }
        )
    finished_ns = monotonic_ns()
    accepted = [sample for sample in samples if sample["status"] == "ACCEPTED"]
    latencies_ms = [float(sample["latency_ns"]) / 1_000_000 for sample in accepted]
    lsns = [sample["journal_lsn"] for sample in accepted]
    subrounds = []
    for index in range(args.samples // args.subround_size):
        group = samples[index * args.subround_size : (index + 1) * args.subround_size]
        group_accepted = [sample for sample in group if sample["status"] == "ACCEPTED"]
        subrounds.append(
            {
                "subround": index + 1,
                "offered": len(group),
                "accepted": len(group_accepted),
                "statuses": dict(Counter(str(sample["status"]) for sample in group)),
                "latency_ms": _latency_summary(
                    [float(sample["latency_ns"]) / 1_000_000 for sample in group_accepted]
                ),
            }
        )
    wall_seconds = (finished_ns - started_ns) / 1_000_000_000
    summary = {
        "profile": "UDS-HOT-1-CONSUMER-BLACKBOX",
        "persistent_candidate": args.persistent_candidate,
        "formal_qualification": False,
        "clock": "time.monotonic_ns",
        "clock_identity_sha256": monotonic_clock_identity(),
        "tool_revision": revision,
        "consumer_only": True,
        "server_management_capability": False,
        "adapter_dispatch_expected": 0,
        "revision": args.revision,
        "round": args.round,
        "run_id": run_id,
        "connection_mode": "fresh-per-request",
        "latency_slo": {
            "profile": HOT_LATENCY_SLO_PROFILE,
            "p95_limit_ms": HOT_P95_LIMIT_MS,
            "p99_limit_ms": HOT_P99_LIMIT_MS,
            "p99_9_and_p100_report_only": True,
        },
        "max_inflight": 1,
        "warmup": args.warmup,
        "warmup_statuses": dict(warmup_statuses),
        "offered": args.samples,
        "accepted": len(accepted),
        "statuses": dict(Counter(str(sample["status"]) for sample in samples)),
        "unique_accepted_lsn": len(set(lsns)) if all(lsn is not None for lsn in lsns) else None,
        "wall_seconds": wall_seconds,
        "completed_tps": len(accepted) / wall_seconds,
        "latency_ms": _latency_summary(latencies_ms),
        "subrounds": subrounds,
        "passed": len(accepted) == args.samples
        and all(lsn is not None for lsn in lsns)
        and len(set(lsns)) == args.samples,
    }
    summary["requests_passed"] = summary["passed"]
    latency = summary["latency_ms"]
    summary["latency_slo_passed"] = _latency_slo_passed(
        latency, len(accepted), args.samples
    )
    summary["passed"] = summary["requests_passed"] and summary["latency_slo_passed"]
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (args.output_dir / "samples.ndjson").write_text(
        "".join(json.dumps(sample, sort_keys=True) + "\n" for sample in samples),
        encoding="utf-8",
    )
    print(json.dumps(summary, sort_keys=True))
    if not summary["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
