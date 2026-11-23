"""Execute the E4/S4 fail-closed lifecycle drills against one isolated shard."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def _request(
    url: str, *, body: dict[str, object] | None = None, key: str | None = None
) -> tuple[int, dict[str, object] | None]:
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-TradeCore-Perf-Fault-Key"] = key
    request = Request(url, data=data, headers=headers, method="POST" if body is not None else "GET")
    try:
        with urlopen(request, timeout=15) as response:  # noqa: S310 - operator-supplied loopback endpoint
            raw = response.read()
            return response.status, json.loads(raw.decode("utf-8")) if raw else None
    except HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw.decode("utf-8")) if raw else None


def _expect(actual: int, expected: int, label: str) -> dict[str, object]:
    if actual != expected:
        raise RuntimeError(f"{label}: expected HTTP {expected}, got {actual}")
    return {"check": label, "status": "PASS", "http_status": actual}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run BenchmarkStandardV1 E4/S4 fault drills")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--fault-key", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    checks: list[dict[str, object]] = []
    try:
        status, _ = _request(f"{base}/perf/v1/admin/ems/pause", body={}, key=args.fault_key)
        checks.append(_expect(status, 200, "E4 pause applied"))
        status, _ = _request(f"{base}/perf/v1/ems/plans", body={"request_id": "fault-paused"})
        checks.append(_expect(status, 503, "E4 admission fails closed while paused"))
        status, _ = _request(f"{base}/perf/v1/admin/ems/resume", body={}, key=args.fault_key)
        checks.append(_expect(status, 200, "E4 resume applied"))
        status, _ = _request(
            f"{base}/perf/v1/admin/ems/cancel-plan",
            body={"plan_id": "ems-seed-000000"},
            key=args.fault_key,
        )
        checks.append(_expect(status, 200, "E4 control-only cancel-plan applied"))

        status, _ = _request(
            f"{base}/perf/v1/strategy/events",
            body={
                "consumer_id": "fault-consumer",
                "scope_id": "scope-0",
                "sequence": 1,
                "event_id": "fault-e1",
            },
        )
        checks.append(_expect(status, 201, "S4 initial checkpoint"))
        status, _ = _request(
            f"{base}/perf/v1/admin/strategy/restart",
            body={"consumer_id": "fault-consumer"},
            key=args.fault_key,
        )
        checks.append(_expect(status, 200, "S4 restart preserves checkpoint"))
        status, _ = _request(
            f"{base}/perf/v1/strategy/events",
            body={
                "consumer_id": "fault-consumer",
                "scope_id": "scope-0",
                "sequence": 2,
                "event_id": "fault-e2",
            },
        )
        checks.append(_expect(status, 201, "S4 resumed checkpoint sequence"))
        status, _ = _request(f"{base}/perf/v1/admin/strategy/pause", body={}, key=args.fault_key)
        checks.append(_expect(status, 200, "S4 pause applied"))
        status, _ = _request(
            f"{base}/perf/v1/strategy/events",
            body={
                "consumer_id": "fault-consumer",
                "scope_id": "scope-0",
                "sequence": 3,
                "event_id": "fault-e3",
            },
        )
        checks.append(_expect(status, 503, "S4 consumer fails closed while paused"))
        status, _ = _request(f"{base}/perf/v1/admin/strategy/resume", body={}, key=args.fault_key)
        checks.append(_expect(status, 200, "S4 resume applied"))
        status, _ = _request(
            f"{base}/perf/v1/strategy/events",
            body={
                "consumer_id": "fault-consumer",
                "scope_id": "scope-0",
                "sequence": 3,
                "event_id": "fault-e3",
            },
        )
        checks.append(_expect(status, 201, "S4 no gap after resume"))
        report: dict[str, object] = {
            "schema": "TradeCoreEmsStrategyFaultV1",
            "status": "PASS",
            "checks": checks,
        }
    except (OSError, RuntimeError, ValueError) as error:
        report = {
            "schema": "TradeCoreEmsStrategyFaultV1",
            "status": "FAIL",
            "checks": checks,
            "error": str(error),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
