"""Prepare a fresh ABI-2 PROBE configuration without altering old evidence."""

from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import sys
from pathlib import Path
from uuid import uuid4

from xuanling_tradecore.adapters.qmt.edge_config import QmtEdgeConfigV1
from xuanling_tradecore.adapters.qmt.native_engine_contract import (
    RING_ABI_VERSION,
    RING_HEADER_SIZE,
    RING_MAGIC,
)


def prepare(config_path: Path, output_path: Path) -> dict[str, object]:
    """Only create a new config; operators stop both owners before installing it."""

    config = QmtEdgeConfigV1.load(config_path)
    if not config.is_disarmed or config.native_policy is not None:
        raise RuntimeError("ring upgrade preparation requires PROBE without a native policy")
    if output_path.resolve() == config_path.resolve():
        raise RuntimeError("output must preserve the active configuration")
    # Fresh command cursors cannot inherit a previously traded database's
    # command sequence. This helper is only for the no-command P0 deployment.
    connection = sqlite3.connect(config.database_path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        if connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0]:
            raise RuntimeError("existing command history requires a reviewed recovery migration")
    finally:
        connection.close()
    prefix = struct.Struct("<8sHH32s16sII")
    for path in (config.command_ring_path, config.event_ring_path):
        with path.open("rb") as stream:
            raw = stream.read(RING_HEADER_SIZE)
        if len(raw) != RING_HEADER_SIZE:
            raise RuntimeError("legacy ring header is truncated")
        magic, abi, reserved, scope, generation, slot_size, capacity = prefix.unpack_from(raw)
        if (magic, abi, reserved) != (RING_MAGIC, 1, 0):
            raise RuntimeError("preparation requires two legacy ABI-1 rings")
        if (scope, generation, slot_size, capacity) != (
            config.scope_digest,
            config.generation,
            512,
            config.ring_capacity,
        ):
            raise RuntimeError("legacy ring identity does not match the active config")
        if path.stat().st_size != RING_HEADER_SIZE + slot_size * capacity:
            raise RuntimeError("legacy ring size does not match its header")
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    new_generation = uuid4().hex
    raw_config["generation_hex"] = new_generation
    for key, path in (
        ("command_ring_path", config.command_ring_path),
        ("event_ring_path", config.event_ring_path),
    ):
        new_path = path.parent / ("abi2-" + new_generation) / path.name
        if new_path.exists():
            raise RuntimeError("new ring path already exists")
        raw_config[key] = str(new_path)
    with output_path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(raw_config, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    return {
        "status": "PROBE_UPGRADE_CONFIG_PREPARED",
        "ring_abi": RING_ABI_VERSION,
        "generation_hex": new_generation,
        "database_preserved": True,
        "active_config_changed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output-config", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = prepare(args.config, args.output_config)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(json.dumps({"status": "PROBE_UPGRADE_BLOCKED", "reason": str(error)}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
