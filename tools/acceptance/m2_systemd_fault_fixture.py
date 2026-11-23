"""Isolated processes used only to verify M2 systemd fault policy.

This tool never loads TradeCore accounts, the EMT SDK, or credentials.  It is
started only in a transient, separately named systemd cgroup during M2
acceptance and deliberately supplies the two process behaviours systemd must
handle: an uncooperative SIGTERM target and bounded memory exhaustion.
"""

from __future__ import annotations

import argparse
import signal
from time import sleep


def _ignore_term() -> int:
    signal.signal(signal.SIGTERM, lambda _number, _frame: None)
    while True:
        sleep(1)


def _allocate(bytes_to_allocate: int) -> int:
    allocation = bytearray(bytes_to_allocate)
    for offset in range(0, len(allocation), 4096):
        allocation[offset] = 1
    while True:
        sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="M2 isolated systemd fault fixture")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ignore-term")
    oom = commands.add_parser("allocate")
    oom.add_argument("--bytes", type=int, default=256 * 1024 * 1024)
    arguments = parser.parse_args()
    if arguments.command == "ignore-term":
        return _ignore_term()
    if arguments.bytes <= 0:
        parser.error("--bytes must be positive")
    return _allocate(arguments.bytes)


if __name__ == "__main__":
    raise SystemExit(main())
