"""Run the disarmed local Windows Edge prerequisite probe.

This ordinary CPython 3.10 process is not the Big QMT model.  It only creates
or opens the bounded local rings and Edge SQLite store, then prints a
payload-free readiness record.  It never starts a TCP listener or calls QMT.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from xuanling_tradecore.adapters.qmt.edge_config import QmtEdgeConfigError, QmtEdgeConfigV1
from xuanling_tradecore.adapters.qmt.edge_runtime import QmtEdgeRuntime
from xuanling_tradecore.adapters.qmt.edge_store import QmtEdgeStoreError
from xuanling_tradecore.adapters.qmt.native_engine_contract import (
    RING_ABI_VERSION,
    QmtNativeContractError,
    qmt_native_abi_digest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="TradeCore QMT Edge disarmed prerequisite probe")
    parser.add_argument("--config", required=True, type=Path)
    arguments = parser.parse_args()
    try:
        config = QmtEdgeConfigV1.load(arguments.config)
        if not config.is_disarmed:
            raise QmtEdgeConfigError("QMT Edge probe requires mode=PROBE and allow_mutation=false")
        runtime = QmtEdgeRuntime.open(config)
    except (OSError, QmtEdgeConfigError, QmtEdgeStoreError, QmtNativeContractError) as error:
        print(json.dumps({"status": "PROBE_BLOCKED", "reason": str(error)}, sort_keys=True))
        return 2
    try:
        snapshot = runtime.snapshot()
        print(
            json.dumps(
                {
                    "command_ring_capacity": snapshot.command_ring.capacity,
                    "event_ring_capacity": snapshot.event_ring.capacity,
                    "mode": snapshot.mode,
                    "mutation_allowed": snapshot.mutation_allowed,
                    "sqlite_version": ".".join(str(part) for part in snapshot.sqlite_version),
                    "status": "PROBE_READY_DISARMED",
                    "ring_abi": RING_ABI_VERSION,
                    "native_abi_digest": qmt_native_abi_digest(),
                    "generation_hex": config.generation.hex(),
                },
                sort_keys=True,
            )
        )
    finally:
        runtime.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
