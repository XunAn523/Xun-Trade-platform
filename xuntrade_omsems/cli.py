"""Offline operational checks; no SDK loading, login or native trading calls."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from .compute import assess_orders, backend_info, crc32c, require_native, session_shards


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="xuntrade-omsems")
    parser.add_argument("command", choices=("info", "self-check"), nargs="?", default="info")
    parser.add_argument("--require-native", action="store_true")
    args = parser.parse_args(argv)
    if args.require_native:
        require_native()
    result: dict[str, object] = {"product": "XunTradeOMSEMS", "backend": backend_info()}
    if args.command == "self-check":
        from emt_gateway.journal import FileJournal

        if crc32c(b"123456789") != 0xE3069283:
            raise RuntimeError("CRC32C self-check failed")
        if session_shards(("execution-1",), 31) != (14,):
            raise RuntimeError("session routing self-check failed")
        if assess_orders(((100, 1250, 200000, 200000),)) != ((125000, 0),):
            raise RuntimeError("integer assessment self-check failed")
        with TemporaryDirectory(prefix="xuntrade-self-check-") as directory:
            path = Path(directory) / "offline.emtj"
            with FileJournal(path) as journal:
                expected = journal.append(
                    kind="OFFLINE_CHECK",
                    command_id="offline-1",
                    correlation_id="offline-1",
                    payload_hash="a" * 64,
                )
            with FileJournal(path) as journal:
                if journal.records() != (expected,):
                    raise RuntimeError("journal roundtrip self-check failed")
        result["checks"] = ["crc32c", "v1-session-route", "int64-assessment", "durable-journal"]
        result["status"] = "passed"
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0
