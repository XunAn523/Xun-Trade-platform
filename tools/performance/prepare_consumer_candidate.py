"""Create a NEW private no-Adapter candidate run, never edit an existing run.

The operator subsequently makes consumer.json root-owned/readable by the
service group and starts a dedicated finite-memory systemd unit. This helper
does not use sudo, start processes, change production or grant qualification.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

from xuanling_tradecore.consumer.runtime_profile import ConsumerLongRunningPolicy
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json


def prepare(*, run_root: Path, peer_uid: int, peer_gid: int) -> dict[str, object]:
    if (
        not run_root.is_absolute()
        or run_root.exists()
        or run_root.is_symlink()
        or any(type(v) is not int or v < 0 for v in (peer_uid, peer_gid))
        or run_root.parent.is_symlink()
        or not run_root.parent.is_dir()
        or len(os.fsencode(run_root / "s")) >= 100
    ):
        raise ValueError("candidate requires a new absolute run root with a short socket path")
    run_root.mkdir(mode=0o700, exist_ok=False)
    data = run_root / "data"
    data.mkdir(mode=0o700)
    config = {
        "schema_version": 1,
        "socket_path": str(run_root / "s"),
        "journal_root": str(data),
        "journal_capacity": 16384,
        # Qualification uses fresh-per-request. Close after the whole reply;
        # do not retain thousands of completed peers solely to consume EOF.
        "max_requests_per_connection": 1,
        "peer_uid": peer_uid,
        "peer_gid": peer_gid,
        "consumer_id": "longrun-candidate",
        "login_name": "longrun-candidate",
        "legal_name": "longrun-candidate",
        "consumer_account_id": "longrun-mock",
        "account_id": "longrun-mock",
        "account_display_name": "No-Adapter qualification",
        "venue_id": "NO_ADAPTER_BENCHMARK",
        "adapter_instance_id": "disabled-no-dispatch",
        "strategy_id": "longrun",
        "runtime_profile": "LONG_RUNNING",
        "long_running_policy": asdict(ConsumerLongRunningPolicy()),
    }
    path = run_root / "consumer.json"
    atomic_runtime_json(path, config)
    result = {
        "schema": "consumer-candidate-preparation-v1",
        "state": "PREPARED_NOT_QUALIFIED",
        "measurement_started": False,
        "formal_qualification": False,
        "config": str(path),
        "config_sha256": sha256(path.read_bytes()).hexdigest(),
        "root": str(run_root),
        "native_calls_allowed": False,
        "required_systemd_properties": {"KillMode": "mixed", "TimeoutStopSec": "75s"},
        "required_next": [
            "root-owned config with service-group read permission",
            "isolated CPU/SMT and finite whole-service cgroup",
            "short diagnostic before formal qualification",
        ],
    }
    atomic_runtime_json(run_root / "preparation.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--peer-uid", type=int, required=True)
    parser.add_argument("--peer-gid", type=int, required=True)
    args = parser.parse_args()
    print(
        json.dumps(prepare(run_root=args.run_root, peer_uid=args.peer_uid, peer_gid=args.peer_gid))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
