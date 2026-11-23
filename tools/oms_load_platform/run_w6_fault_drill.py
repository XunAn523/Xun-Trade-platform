"""Run replay, projector-pause and stale-snapshot W6 over the real HTTP API."""

from __future__ import annotations

import argparse
import json
from time import time_ns
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def _request(
    url: str,
    method: str,
    payload: dict[str, object] | None = None,
    *,
    fault_key: str | None = None,
) -> tuple[int, dict[str, Any]]:
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if fault_key is not None:
        headers["X-TradeCore-Perf-Fault-Key"] = fault_key
    request = Request(url, data=data, method=method, headers=headers)
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310 - supplied test base URL.
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def _expect(status: int, actual: int, description: str) -> None:
    if actual != status:
        raise RuntimeError(f"{description}: expected HTTP {status}, got {actual}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OMS W6 fault drill")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--fault-key", required=True)
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")
    scope = "bench-scope-0"
    serial = str(time_ns())
    fact = {
        "scope_id": scope,
        "execution_id": f"w6-execution-{serial}",
        "canonical_client_order_ref": f"w6-client-ref-{serial}",
    }
    baseline_status, _ = _request(f"{base_url}/perf/v1/oms/facts", "POST", fact)
    _expect(201, baseline_status, "baseline fact")
    replay_status, replay = _request(
        f"{base_url}/perf/v1/admin/fault/replay-verify",
        "POST",
        {"scope_id": scope},
        fault_key=args.fault_key,
    )
    _expect(200, replay_status, "replay verification")
    if replay.get("status") != "PASS":
        raise RuntimeError("replay verification reported a mismatch")
    pause_status, _ = _request(
        f"{base_url}/perf/v1/admin/fault/pause-projector",
        "POST",
        {"scope_id": scope},
        fault_key=args.fault_key,
    )
    _expect(200, pause_status, "projector pause")
    stale_status, _ = _request(f"{base_url}/perf/v1/oms/snapshot?require_fresh=true", "GET")
    _expect(503, stale_status, "stale snapshot gate")
    rejected_status, _ = _request(f"{base_url}/perf/v1/oms/facts", "POST", fact)
    _expect(503, rejected_status, "paused ingest fail closed")
    health_status, health = _request(f"{base_url}/perf/v1/oms/health", "GET")
    _expect(200, health_status, "paused health")
    if health.get("status") != "STALE":
        raise RuntimeError("paused health did not report STALE")
    resume_status, _ = _request(
        f"{base_url}/perf/v1/admin/fault/resume-projector",
        "POST",
        {"scope_id": scope},
        fault_key=args.fault_key,
    )
    _expect(200, resume_status, "projector resume")
    recovered_fact = dict(fact)
    recovered_fact["execution_id"] = f"w6-recovered-execution-{serial}"
    recovered_fact["canonical_client_order_ref"] = f"w6-recovered-client-ref-{serial}"
    recovered_status, _ = _request(f"{base_url}/perf/v1/oms/facts", "POST", recovered_fact)
    _expect(201, recovered_status, "resumed fact")
    final_status, final_replay = _request(
        f"{base_url}/perf/v1/admin/fault/replay-verify",
        "POST",
        {"scope_id": scope},
        fault_key=args.fault_key,
    )
    _expect(200, final_status, "post-recovery replay")
    if final_replay.get("status") != "PASS":
        raise RuntimeError("post-recovery replay reported a mismatch")
    print(
        json.dumps(
            {
                "schema": "TradeCoreOmsW6FaultResultV1",
                "status": "PASS",
                "steps": [
                    "replay",
                    "projector_pause",
                    "snapshot_stale",
                    "resume",
                    "replay_after_resume",
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
