"""Measure warmed, serial OMS HTTP hot paths without sending an EMT request.

The benchmark keeps one persistent HTTP connection per independently pinned OMS
shard and issues exactly one request at a time.  It is intentionally separate
from k6 throughput tests: it answers the low-concurrency caller-latency
question while preserving the real HTTP decode, actor acknowledgement and
immutable-snapshot response paths.
"""

from __future__ import annotations

import argparse
import http.client
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class _Endpoint:
    """A parsed http endpoint with its own reusable client connection."""

    name: str
    connection: http.client.HTTPConnection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        required=True,
        help="comma-separated OMS URLs; one persistent connection is used per URL",
    )
    parser.add_argument("--samples", type=int, default=10_000)
    parser.add_argument("--warmup", type=int, default=1_000)
    parser.add_argument("--seed-orders", type=int, default=1_024)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _percentiles_ns(values: list[int]) -> dict[str, int]:
    """Return nearest-rank percentiles, retaining integer monotonic-ns units."""

    if not values:
        raise ValueError("cannot summarize an empty sample")
    ordered = sorted(values)
    result: dict[str, int] = {"count": len(ordered), "min": ordered[0], "max": ordered[-1]}
    for label, fraction in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99), ("p100", 1.00)):
        index = max(0, (len(ordered) * fraction).__ceil__() - 1)
        result[label] = ordered[index]
    return result


def _git_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, cwd=Path(__file__).resolve().parents[2]
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _endpoints(raw_urls: str) -> list[_Endpoint]:
    endpoints: list[_Endpoint] = []
    for index, raw in enumerate(item.strip().rstrip("/") for item in raw_urls.split(",")):
        if not raw:
            continue
        parsed = urlparse(raw)
        if parsed.scheme != "http" or not parsed.hostname or parsed.path not in ("", "/"):
            raise ValueError(f"base URL must be a bare http URL: {raw!r}")
        endpoints.append(
            _Endpoint(
                name=f"shard-{index}",
                connection=http.client.HTTPConnection(
                    parsed.hostname, parsed.port or 80, timeout=10
                ),
            )
        )
    if not endpoints:
        raise ValueError("--base-url must contain at least one http URL")
    return endpoints


def _request(
    endpoint: _Endpoint,
    method: str,
    target: str,
    body: bytes | None,
    headers: dict[str, str],
    expected_status: int,
) -> int:
    started = perf_counter_ns()
    endpoint.connection.request(method, target, body=body, headers=headers)
    response = endpoint.connection.getresponse()
    response.read()
    elapsed = perf_counter_ns() - started
    if response.status != expected_status:
        raise RuntimeError(f"{endpoint.name} {method} {target} returned HTTP {response.status}")
    return elapsed


def _request_json(endpoint: _Endpoint, target: str) -> tuple[int, dict[str, Any]]:
    """Fetch one JSON endpoint while retaining its round-trip and payload."""

    started = perf_counter_ns()
    endpoint.connection.request("GET", target)
    response = endpoint.connection.getresponse()
    payload = response.read()
    elapsed = perf_counter_ns() - started
    if response.status != 200:
        raise RuntimeError(f"{endpoint.name} GET {target} returned HTTP {response.status}")
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise RuntimeError(f"{endpoint.name} GET {target} returned a non-object JSON payload")
    return elapsed, decoded


def _fact_body(serial: int) -> bytes:
    token = f"hot-{serial:08d}"
    return json.dumps(
        {
            "scope_id": "bench-scope-0",
            "execution_id": f"hot-execution-{token}",
            "canonical_client_order_ref": f"hot-client-ref-{token}",
            "kind": "PRE_NATIVE_COMMITTED",
            "order_state": "PRE_NATIVE_COMMITTED",
            "state_version": 1,
        },
        separators=(",", ":"),
    ).encode("ascii")


def _measure_fact(endpoints: list[_Endpoint], *, warmup: int, samples: int) -> dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-TradeCore-Perf-Ack": "minimal"}
    total = warmup + samples
    per_shard: dict[str, list[int]] = {endpoint.name: [] for endpoint in endpoints}
    raw_ns: list[int] = []
    for serial in range(total):
        endpoint = endpoints[serial % len(endpoints)]
        elapsed = _request(
            endpoint,
            "POST",
            "/perf/v1/oms/facts",
            _fact_body(serial),
            headers,
            expected_status=201,
        )
        if serial >= warmup:
            raw_ns.append(elapsed)
            per_shard[endpoint.name].append(elapsed)
    return {
        "end_to_end_http_ns": _percentiles_ns(raw_ns),
        "by_shard_ns": {
            name: _percentiles_ns(values) for name, values in per_shard.items() if values
        },
        "raw_ns": raw_ns,
    }


def _measure_snapshot(
    endpoints: list[_Endpoint], *, warmup: int, samples: int, seed_orders: int
) -> dict[str, Any]:
    total = warmup + samples
    per_shard: dict[str, list[int]] = {endpoint.name: [] for endpoint in endpoints}
    raw_ns: list[int] = []
    for serial in range(total):
        endpoint = endpoints[serial % len(endpoints)]
        path = f"/perf/v1/oms/orders/seed-{serial % seed_orders:06d}"
        elapsed = _request(endpoint, "GET", path, None, {}, expected_status=200)
        if serial >= warmup:
            raw_ns.append(elapsed)
            per_shard[endpoint.name].append(elapsed)
    return {
        "end_to_end_http_ns": _percentiles_ns(raw_ns),
        "by_shard_ns": {
            name: _percentiles_ns(values) for name, values in per_shard.items() if values
        },
        "raw_ns": raw_ns,
    }


def _hdr(endpoints: list[_Endpoint]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for endpoint in endpoints:
        elapsed, payload = _request_json(endpoint, "/perf/v1/metrics/hdr")
        if payload.get("format") != "HDRHistogramV1":
            raise RuntimeError(f"{endpoint.name} returned an unexpected HDR schema")
        result[endpoint.name] = {
            "metrics_endpoint_round_trip_ns": elapsed,
            "hdr": payload,
        }
    return result


def main() -> int:
    args = _parser().parse_args()
    if args.samples <= 0 or args.warmup < 0 or args.seed_orders <= 0:
        raise SystemExit(
            "--samples and --seed-orders must be positive; --warmup must be non-negative"
        )
    endpoints = _endpoints(args.base_url)
    try:
        report: dict[str, Any] = {
            "schema": "TradeCoreOmsLowConcurrencyHotV1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "git_revision": _git_revision(),
            "mode": "serial_one_in_flight_per_client",
            "connection_policy": "one persistent HTTP connection per shard; no request retry",
            "warmup_per_operation": args.warmup,
            "samples_per_operation": args.samples,
            "operations": {
                "fact_ingest": _measure_fact(endpoints, warmup=args.warmup, samples=args.samples),
                "immutable_snapshot_query": _measure_snapshot(
                    endpoints,
                    warmup=args.warmup,
                    samples=args.samples,
                    seed_orders=args.seed_orders,
                ),
            },
            "metrics_endpoint_round_trips": _hdr(endpoints),
        }
    except (OSError, RuntimeError, ValueError, http.client.HTTPException) as error:
        print(f"low-concurrency hot benchmark failed: {error}", file=sys.stderr)
        return 1
    finally:
        for endpoint in endpoints:
            endpoint.connection.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"schema": report["schema"], "output": str(args.output)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
