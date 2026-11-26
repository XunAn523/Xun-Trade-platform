"""Explicit CLI entry to the EMT-specific process composition."""

from pathlib import Path


def run_consumer_emt_simulator_v1(*, config_path: Path) -> int:
    from emt_gateway.consumer_simulator_runtime_v1 import run_consumer_emt_simulator_v1 as run

    return run(config_path=config_path)
