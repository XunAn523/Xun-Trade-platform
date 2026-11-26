"""TradeCore identity and explicitly disarmed runtime commands.

The runtime command has no account, adapter, Journal, or order composition.
It only hosts the fail-closed, signal-aware disarmed shell while the P0 EMT
compatibility CLI remains unchanged.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from time import monotonic_ns, sleep

from . import __version__
from .consumer.uds_server_v2 import run_consumer_uds_service_v2
from .identity import PROJECT_DISTRIBUTION, PROJECT_NAME
from .runtime import (
    DisarmedRuntimeProcessV1,
    RuntimeFaultEvidenceStoreV1,
    RuntimeFaultGapPageFileInitializerV1,
    RuntimeFaultGapPageFileReaderV1,
    RuntimeFaultSupervisorV1,
    RuntimeShutdownCoordinatorV1,
)
from .runtime.fault_monitor import RUNTIME_FAULT_GAP_PAGE_V1_BYTES, RuntimeFaultGapPageV1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROJECT_DISTRIBUTION,
        description="Display Xuanling TradeCore product identity without starting trading services.",
    )
    parser.add_argument(
        "--version", action="store_true", help="print the installed TradeCore version"
    )
    commands = parser.add_subparsers(dest="command")
    runtime = commands.add_parser("runtime", help="start the explicitly disarmed runtime shell")
    runtime.add_argument("--disarmed", action="store_true", required=True)
    runtime.add_argument("--fault-gap-page", type=Path)
    runtime.add_argument("--once", action="store_true", help=argparse.SUPPRESS)
    supervisor = commands.add_parser(
        "fault-supervisor", help="run the non-trading fault supervisor"
    )
    supervisor.add_argument("--gap-page", type=Path, required=True)
    supervisor.add_argument("--evidence-root", type=Path, required=True)
    supervisor.add_argument("--interval-ms", type=int, default=1_000)
    supervisor.add_argument("--once", action="store_true", help=argparse.SUPPRESS)
    consumer_uds = commands.add_parser(
        "consumer-uds", help="run the server-local Consumer V2 UDS with no Adapter dispatch"
    )
    consumer_uds.add_argument("--config", type=Path, required=True)
    consumer_uds.add_argument("--contract-root", type=Path, default=Path("contracts/consumer/v2"))
    simulator = commands.add_parser("consumer-emt-simulator", help="run operator-authorized EMT simulator submit-only UDS")
    simulator.add_argument("--config", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only identity CLI and return a process status."""

    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.version:
        print(f"{PROJECT_NAME} {__version__}")
    elif arguments.command == "runtime":
        if arguments.fault_gap_page is not None:
            RuntimeFaultGapPageFileInitializerV1(arguments.fault_gap_page).initialize(
                RuntimeFaultGapPageV1(
                    process_epoch=max(1, monotonic_ns()),
                    active=False,
                    first_sequence=0,
                    last_sequence=0,
                    dropped_count=0,
                    severity_bitmap=0,
                    heartbeat_monotonic_ns=monotonic_ns(),
                )
            )
        coordinator = RuntimeShutdownCoordinatorV1(
            stop_new_admissions=lambda _reason: None,
            drain_workers=(),
            release_ownership_fences=(),
        )
        DisarmedRuntimeProcessV1(coordinator).run(once=arguments.once)
    elif arguments.command == "fault-supervisor":
        if arguments.interval_ms <= 0:
            parser.error("--interval-ms must be positive")
        evidence_store = RuntimeFaultEvidenceStoreV1(arguments.evidence_root)
        reader = RuntimeFaultGapPageFileReaderV1(
            arguments.gap_page, page_bytes=RUNTIME_FAULT_GAP_PAGE_V1_BYTES
        )
        supervisor = RuntimeFaultSupervisorV1(
            read_page=reader.read,
            persist_evidence=evidence_store.persist,
            persist_heartbeat=evidence_store.persist_heartbeat,
        )
        if arguments.once:
            supervisor.poll_once()
        else:
            while True:
                try:
                    supervisor.poll_once()
                except (OSError, ValueError):
                    pass
                sleep(arguments.interval_ms / 1_000)
    elif arguments.command == "consumer-uds":
        return run_consumer_uds_service_v2(
            config_path=arguments.config, contract_root=arguments.contract_root
        )
    elif arguments.command == "consumer-emt-simulator":
        from .compatibility.simulator_cli import run_consumer_emt_simulator_v1

        # Dedicated native process: preserve already-released vendor wrappers
        # through process exit, avoiding their unsafe/hanging Python finalizer.
        try:
            code = run_consumer_emt_simulator_v1(config_path=arguments.config)
        except BaseException as exc:
            print("EMT_SIMULATOR_STOPPED:" + type(exc).__name__, file=sys.stderr, flush=True)
            os._exit(2)
        os._exit(code)
    else:
        parser.print_help()
    return 0
