from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path

from .baseline import baseline_template, evaluate_acceptance
from .callback_injection import (
    CallbackInjectionError,
    CallbackInjectionSettings,
    run_callback_injection,
)
from .driver import SyntheticGatewayDriver
from .fixture_driver import FixtureDriverError, FixtureDriverSettings, run_fixture_driver
from .gateway_ab import GatewayAbError, GatewayAbSettings, run_gateway_ab
from .gateway_controlled_order import (
    ControlledOrderError,
    ControlledOrderSettings,
    run_controlled_order_experiment,
)
from .gateway_credit_quota_pair import CreditQuotaPairSettings, run_credit_quota_pair
from .gateway_credit_repayment import (
    CreditQuotaTransferSettings,
    CreditRepaymentError,
    CreditRepaymentSettings,
    run_credit_quota_transfer,
    run_credit_repayment,
)
from .gateway_fund_transfer_pair import FundTransferPairSettings, run_fund_transfer_pair
from .gateway_mutation import GatewayMutationError, GatewayMutationSettings, run_gateway_mutation
from .gateway_option_actions import OptionActionError, OptionActionSettings, run_option_action
from .gateway_readonly import GatewayReadOnlySettings, run_gateway_readonly
from .gateway_report import build_gateway_report
from .gateway_trade_fill import TradeFillSettings, run_gateway_trade_fill
from .local_signal_burst import (
    LocalSignalBurstError,
    LocalSignalBurstSettings,
    run_local_signal_burst,
)
from .matrix import runbook_template, validate_runbook
from .mutation_fixture import MutationFixtureError, mutation_fixture_template
from .mutation_recovery import (
    MutationRecoveryError,
    MutationRecoverySettings,
    run_mutation_recovery,
)
from .native_readonly import NativeSettings, run_read_only
from .query_fixture import (
    QueryFixtureError,
    load_query_fixture,
    query_fixture_template,
    required_callback_capacity,
    select_query_cases,
)
from .query_semantic_fixture import (
    QuerySemanticFixtureError,
    load_query_semantic_fixture,
    query_semantic_fixture_template,
)
from .release_readiness import ReleaseReadinessError, build_release_readiness
from .report_schema import PerformanceReportSchemaError, finalize_performance_report
from .runner import run
from .session_fault import SessionFaultError, SessionFaultSettings, run_session_fault
from .simulated_accounts import load_simulated_accounts
from .vendor_sweep import VendorSweepSettings, run_vendor_sweep
from .workload_plan import workload_plan_template
from .workload_readiness import WorkloadReadinessError, build_workload_readiness


def _select_trace_inputs(
    trace_paths: list[Path],
    excluded_paths: list[Path] | None,
    exclusion_reason: str,
) -> tuple[list[Path], dict[str, object]]:
    """Return explicitly included trace paths and auditable exclusion metadata.

    A source trace may only be excluded when it was also supplied as an input,
    and a non-empty reason must be persisted in the resulting report.  This
    prevents a shell glob or a hand-edited aggregate from silently hiding a
    real counter-side failure.
    """

    resolved_inputs = {path.resolve() for path in trace_paths}
    resolved_exclusions = {path.resolve() for path in excluded_paths or []}
    unknown_exclusions = resolved_exclusions - resolved_inputs
    if unknown_exclusions:
        names = ", ".join(sorted(path.name for path in unknown_exclusions))
        raise ValueError(f"excluded trace inputs must also be supplied: {names}")
    if resolved_exclusions and not exclusion_reason.strip():
        raise ValueError("--trace-exclusion-reason is required when excluding trace input")
    if not resolved_exclusions and exclusion_reason.strip():
        raise ValueError("--trace-exclusion-reason requires at least one excluded trace input")

    selected = [path for path in trace_paths if path.resolve() not in resolved_exclusions]
    if not selected:
        raise ValueError("at least one trace input must remain after exclusions")
    return selected, {
        "input_batches": len(trace_paths),
        "included_batches": len(selected),
        "excluded_batches": len(resolved_exclusions),
        "excluded_trace_files": sorted(path.name for path in resolved_exclusions),
        "trace_exclusion_reason": exclusion_reason.strip(),
    }


def _collect_trace_inputs(
    trace_paths: list[Path] | None, trace_directories: list[Path] | None
) -> list[Path]:
    """Expand explicit trace files and reviewed archive directories once."""

    collected = list(trace_paths or [])
    for directory in trace_directories or []:
        if not directory.is_dir():
            raise ValueError(f"trace input directory does not exist: {directory}")
        collected.extend(sorted(directory.rglob("*.traces.jsonl")))
    if not collected:
        raise ValueError("at least one trace input file or directory is required")
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in collected:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(path)
    return unique


def main() -> int:
    parser = argparse.ArgumentParser(description="Run EMT gateway performance harness")
    parser.add_argument(
        "--driver",
        choices=(
            "synthetic",
            "native-readonly",
            "vendor-simulated-sweep",
            "runbook-template",
            "workload-plan-template",
            "gateway-trace-report",
            "baseline-template",
            "gateway-acceptance-report",
            "gateway-release-readiness",
            "gateway-workload-readiness",
            "gateway-fixture",
            "gateway-readonly",
            "query-fixture-template",
            "query-semantic-fixture-template",
            "gateway-query-fixture",
            "gateway-query-burst",
            "gateway-query-late",
            "gateway-query-semantics",
            "mutation-fixture-template",
            "gateway-mutation",
            "gateway-mutation-aggregate",
            "gateway-callback-injection",
            "gateway-ab-order",
            "gateway-mutation-recovery",
            "gateway-credit-repayment",
            "gateway-credit-quota-transfer",
            "gateway-credit-quota-pair",
            "gateway-fund-transfer-pair",
            "gateway-trade-fill",
            "gateway-controlled-order",
            "gateway-local-signal-burst",
            "gateway-option-action",
            "gateway-session-fault",
        ),
        default="synthetic",
    )
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument(
        "--readonly-iterations",
        type=int,
        default=1,
        help="1..10 low-rate iterations for gateway-readonly",
    )
    parser.add_argument(
        "--burst-rounds", type=int, default=25, help="1..25 rounds for gateway-query-burst"
    )
    parser.add_argument(
        "--burst-width",
        type=int,
        default=4,
        help="2..4 concurrent requests per gateway-query-burst lane",
    )
    parser.add_argument(
        "--late-iterations",
        type=int,
        default=10,
        help="1..100 sequential requests for gateway-query-late",
    )
    parser.add_argument(
        "--late-delay-ms",
        type=float,
        default=100.0,
        help="10..1000 post-C5 consumer delay for gateway-query-late",
    )
    parser.add_argument(
        "--semantic-iterations",
        type=int,
        default=100,
        help="1..100 sequential semantic repetitions for gateway-query-semantics",
    )
    parser.add_argument(
        "--callback-injection-iterations",
        type=int,
        default=100,
        help="1..100 local callback-injection rounds per case",
    )
    parser.add_argument(
        "--callback-injection-burst-width",
        type=int,
        default=4,
        help="2..8 callbacks queued before a local injected drain",
    )
    parser.add_argument(
        "--callback-injection-delay-ms",
        type=float,
        default=10.0,
        help="1..1000 local post-C5 consumer delay for callback injection",
    )
    parser.add_argument(
        "--callback-injection-journal",
        choices=("file", "memory"),
        default="file",
        help="file uses write-through FileJournal; memory is test-only",
    )
    parser.add_argument(
        "--allow-python-fastspi",
        action="store_true",
        help="allow the non-native callback ring for local development only",
    )
    parser.add_argument(
        "--isolate-after-report",
        action="store_true",
        help=(
            "acceptance-only: retain the EMT native wrapper, persist the report, "
            "then exit the dedicated process without running the vendor finalizer"
        ),
    )
    parser.add_argument(
        "--session-fault-cycles",
        type=int,
        default=1,
        help="1..3 locally proxied real-session disconnect cycles",
    )
    parser.add_argument(
        "--session-fault-timeout",
        type=float,
        default=12.0,
        help="3..30 seconds to await one real native session callback",
    )
    parser.add_argument(
        "--session-fault-blackout-seconds",
        type=float,
        default=0.0,
        help="0..30 seconds to reset/reject only this fixture's new proxy pairs",
    )
    parser.add_argument(
        "--session-fault-transport",
        choices=("loopback_proxy", "transparent_redirect"),
        default="loopback_proxy",
        help="loopback login endpoint or test-only per-process transparent redirect",
    )
    parser.add_argument(
        "--session-fault-proxy-port",
        type=int,
        default=0,
        help="fixed loopback port required by transparent session-fault redirect",
    )
    parser.add_argument(
        "--session-fault-acceptance-standard",
        choices=(
            "emt_2_27_client_rst_auto_reconnect_v1",
            "documented_callback_sequence_v1",
        ),
        default="emt_2_27_client_rst_auto_reconnect_v1",
        help="versioned session-fault pass rule; does not alter runtime recovery safeguards",
    )
    parser.add_argument("--semantic-workload", choices=("empty", "pagination"), default="empty")
    parser.add_argument(
        "--probe-known-callback-gaps",
        action="store_true",
        help="also probe documented no-terminal-callback cases; they stay outside C0-C7 baseline evidence",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--fixture-owner",
        default="",
        help="opaque performance-fixture owner used only when generating local fixture templates",
    )
    parser.add_argument(
        "--sdk-root", type=Path, help="EMT Python SDK root; required for native-readonly"
    )
    parser.add_argument(
        "--accounts-file",
        type=Path,
        help="ignored local simulated-account JSON; overrides host/port/account/password",
    )
    parser.add_argument(
        "--query-fixture",
        type=Path,
        help="ignored local read-only query-fixture JSON; required for gateway-query-fixture",
    )
    parser.add_argument(
        "--semantic-fixture",
        type=Path,
        help="ignored local query semantic fixture; required for gateway-query-semantics",
    )
    parser.add_argument(
        "--query-case",
        action="append",
        help="repeat to select only already-enabled local query-fixture cases; rejects unknown or disabled names",
    )
    parser.add_argument(
        "--mutation-fixture",
        type=Path,
        help="ignored local mutation fixture; required for gateway-mutation",
    )
    parser.add_argument(
        "--controlled-order-fixture",
        type=Path,
        help="ignored local four-scenario 000010 simulated order fixture; required for gateway-controlled-order",
    )
    parser.add_argument(
        "--controlled-order-scenario",
        action="append",
        help="repeat to run only approved named controlled-order scenarios; default runs all four",
    )
    parser.add_argument(
        "--verify-cancel-error",
        action="store_true",
        help=(
            "with one selected cancel scenario, issue one bounded duplicate cancel after terminal "
            "reconciliation and require OnCancelOrderError"
        ),
    )
    parser.add_argument(
        "--local-latency-gate-report",
        type=Path,
        help="passing account-free 1 Hz local-gate report required for controlled real order entry",
    )
    parser.add_argument(
        "--skip-local-latency-gate",
        action="store_true",
        help="explicitly bypass the default 1 Hz local gate for a controlled simulated order run; recorded in the report",
    )
    parser.add_argument(
        "--mutation-trace-input",
        type=Path,
        nargs="+",
        help="payload-free mutation trace JSONL files; required for gateway-mutation-aggregate",
    )
    parser.add_argument(
        "--trace-input-directory",
        type=Path,
        action="append",
        help="repeat to recursively include *.traces.jsonl from a reviewed archive directory",
    )
    parser.add_argument(
        "--exclude-trace-input",
        type=Path,
        action="append",
        help="repeat to explicitly exclude a supplied trace input; requires --trace-exclusion-reason",
    )
    parser.add_argument(
        "--trace-exclusion-reason",
        default="",
        help="immutable review reason persisted when trace inputs are explicitly excluded",
    )
    parser.add_argument(
        "--trace-input",
        type=Path,
        nargs="+",
        help="one or more payload-free C0-C7 JSONL trace files; required for gateway-trace-report",
    )
    parser.add_argument(
        "--runbook",
        type=Path,
        help="account-free matrix manifest to validate before a gateway-trace-report",
    )
    parser.add_argument(
        "--workload-plan",
        type=Path,
        help="account-free non-steady fixture control plan; required for external gateway-fixture execution",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="performance-baseline JSON for acceptance or release-readiness evaluation",
    )
    parser.add_argument(
        "--require-release-ready",
        action="store_true",
        help="with gateway-release-readiness, return nonzero after writing the report when any gate blocks release",
    )
    parser.add_argument(
        "--execute-simulated",
        action="store_true",
        help="explicitly permit the external simulated fixture process",
    )
    parser.add_argument(
        "--execute-simulated-mutations",
        action="store_true",
        help="explicitly permit the bounded local order/cancel simulated fixture",
    )
    parser.add_argument(
        "--mutation-recovery-query-only",
        action="store_true",
        help="for gateway-mutation-recovery, query unfinished orders without issuing cancellation",
    )
    parser.add_argument(
        "--mutation-recovery-isolate-after-report",
        action="store_true",
        help=(
            "after a durable gateway-mutation-recovery report, terminate the one-shot process "
            "without running the vendor Python binding finalizer"
        ),
    )
    parser.add_argument(
        "--fixture-command",
        nargs="+",
        help="reviewed external gateway fixture command; credentials remain outside this tool",
    )
    parser.add_argument("--host", default=os.getenv("EMT_PERF_HOST"))
    parser.add_argument("--port", type=int, default=int(os.getenv("EMT_PERF_PORT", "0")))
    parser.add_argument("--account", default=os.getenv("EMT_PERF_ACCOUNT"))
    parser.add_argument("--client-id", type=int, default=int(os.getenv("EMT_PERF_CLIENT_ID", "81")))
    parser.add_argument("--local-ip", default=os.getenv("EMT_PERF_LOCAL_IP", ""))
    parser.add_argument("--password-env", default="EMT_PERF_PASSWORD")
    parser.add_argument("--callback-timeout", type=float, default=8.0)
    parser.add_argument(
        "--callback-capacity",
        type=int,
        default=4096,
        help="1..65536 callback ring capacity for gateway read-only query drivers",
    )
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument(
        "--mutation-cycles",
        type=int,
        default=1,
        help="1..10 sequential controlled order/cancel cycles",
    )
    parser.add_argument(
        "--ab-cycles",
        type=int,
        default=1,
        help="1..10 same-session native-direct versus durable-gateway order/cancel cycles",
    )
    parser.add_argument(
        "--credit-repayment-cycles",
        type=int,
        default=1,
        help="1..30 sequential direct-credit-repayment cycles",
    )
    parser.add_argument(
        "--credit-quota-pair-cycles",
        type=int,
        default=30,
        help="1..30 equal-and-opposite credit-quota transfer pairs",
    )
    parser.add_argument(
        "--fund-transfer-pair-cycles",
        type=int,
        default=30,
        help="1..30 equal-and-opposite ordinary fund-transfer pairs",
    )
    parser.add_argument(
        "--trade-fill-cycles",
        type=int,
        default=100,
        help="1..100 sequential simulated best-or-cancel sell fills",
    )
    parser.add_argument(
        "--signal-rate",
        type=int,
        default=300,
        help="1..300 account-free local signals per one-second window",
    )
    parser.add_argument(
        "--signal-rounds",
        type=int,
        default=10,
        help="1..600 measured one-second local signal rounds",
    )
    parser.add_argument(
        "--signal-warmup-rounds",
        type=int,
        default=0,
        help="0..60 excluded one-second warmup rounds",
    )
    parser.add_argument(
        "--signal-arrival",
        choices=("uniform", "burst"),
        default="uniform",
        help="uniform is sustained QPS; burst releases the whole one-second quota concurrently",
    )
    parser.add_argument(
        "--signal-journal-mode",
        choices=("write-through", "group-commit"),
        default="group-commit",
        help="local-signal journal strategy; group-commit preserves durable-before-native/ACK",
    )
    parser.add_argument(
        "--signal-group-wait-ms",
        type=float,
        default=0.0,
        help="0..10 group-commit collection bound; 0 flushes the ready FIFO immediately",
    )
    parser.add_argument(
        "--signal-group-size",
        type=int,
        default=128,
        help="1..1024 group-commit maximum command records per flush",
    )
    parser.add_argument(
        "--signal-callback-batch-size",
        type=int,
        default=64,
        help="1..1024 callbacks durably flushed per drain",
    )
    parser.add_argument(
        "--signal-callback-collect-wait-ms",
        type=float,
        default=0.0,
        help="0..10 callback microbatch collection bound",
    )
    parser.add_argument("--profile", choices=("ordinary", "credit", "option"), default="ordinary")
    args = parser.parse_args()
    isolate_supported_drivers = {
        "gateway-readonly",
        "gateway-query-fixture",
        "gateway-query-burst",
        "gateway-query-late",
        "gateway-query-semantics",
        "gateway-session-fault",
        "gateway-mutation",
        "gateway-credit-repayment",
        "gateway-trade-fill",
        "gateway-controlled-order",
        "gateway-credit-quota-pair",
        "gateway-fund-transfer-pair",
    }
    if args.isolate_after_report and args.driver not in isolate_supported_drivers:
        parser.error(
            "--isolate-after-report is limited to reviewed one-shot EMT acceptance drivers"
        )
    if args.verify_cancel_error and args.driver != "gateway-controlled-order":
        parser.error("--verify-cancel-error is limited to gateway-controlled-order")
    retained_native_references: list[object] = []

    if args.driver == "synthetic":
        report = run(SyntheticGatewayDriver(), iterations=args.iterations)
    elif args.driver == "runbook-template":
        report = runbook_template(owner=args.fixture_owner)
    elif args.driver == "workload-plan-template":
        report = workload_plan_template(owner=args.fixture_owner)
    elif args.driver in {
        "gateway-trace-report",
        "gateway-acceptance-report",
        "gateway-release-readiness",
        "gateway-workload-readiness",
    }:
        if not args.trace_input and not args.trace_input_directory:
            parser.error("--trace-input or --trace-input-directory is required for trace reporting")
        if args.runbook is not None:
            try:
                manifest = json.loads(args.runbook.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                parser.error(f"cannot read --runbook: {exc}")
            try:
                validate_runbook(manifest)
            except ValueError as exc:
                parser.error(str(exc))
        events: list[dict[str, object]] = []
        try:
            trace_inputs = _collect_trace_inputs(args.trace_input, args.trace_input_directory)
            trace_paths, trace_provenance = _select_trace_inputs(
                trace_inputs,
                args.exclude_trace_input,
                args.trace_exclusion_reason,
            )
            for trace_path in trace_paths:
                for line_number, line in enumerate(
                    trace_path.read_text(encoding="utf-8").splitlines(), start=1
                ):
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        parser.error(
                            f"trace input {trace_path.name} line {line_number} must be a JSON object"
                        )
                    events.append(event)
        except (OSError, json.JSONDecodeError) as exc:
            parser.error(f"cannot read --trace-input: {exc}")
        try:
            report = build_gateway_report(events)
        except ValueError as exc:
            parser.error(f"invalid trace input: {exc}")
        report["batches"] = len(trace_paths)
        trace_provenance["trace_input_directories"] = [
            str(path) for path in args.trace_input_directory or []
        ]
        report["trace_provenance"] = trace_provenance
        if args.driver == "gateway-acceptance-report":
            if args.baseline is None:
                parser.error("--baseline is required for gateway-acceptance-report")
            try:
                baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
                if not isinstance(baseline, dict):
                    parser.error("--baseline must contain a JSON object")
                report = evaluate_acceptance(report, baseline)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                parser.error(f"cannot evaluate acceptance: {exc}")
        elif args.driver == "gateway-release-readiness":
            baseline: dict[str, object] | None = None
            if args.baseline is not None:
                try:
                    raw_baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    parser.error(f"cannot read --baseline: {exc}")
                if not isinstance(raw_baseline, dict):
                    parser.error("--baseline must contain a JSON object")
                baseline = raw_baseline
            try:
                report = build_release_readiness(report, baseline)
            except ReleaseReadinessError as exc:
                parser.error(str(exc))
        elif args.driver == "gateway-workload-readiness":
            if args.workload_plan is None:
                parser.error("--workload-plan is required for gateway-workload-readiness")
            try:
                workload_plan = json.loads(args.workload_plan.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                parser.error(f"cannot read --workload-plan: {exc}")
            if not isinstance(workload_plan, dict):
                parser.error("--workload-plan must contain a JSON object")
            runbook: dict[str, object] | None = None
            if args.runbook is not None:
                try:
                    raw_runbook = json.loads(args.runbook.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    parser.error(f"cannot read --runbook: {exc}")
                if not isinstance(raw_runbook, dict):
                    parser.error("--runbook must contain a JSON object")
                runbook = raw_runbook
            try:
                report = build_workload_readiness(report, workload_plan, runbook)
            except WorkloadReadinessError as exc:
                parser.error(str(exc))
    elif args.driver == "baseline-template":
        report = baseline_template()
    elif args.driver == "query-fixture-template":
        report = query_fixture_template()
    elif args.driver == "query-semantic-fixture-template":
        report = query_semantic_fixture_template(owner=args.fixture_owner)
    elif args.driver == "mutation-fixture-template":
        report = mutation_fixture_template()
    elif args.driver == "gateway-callback-injection":
        try:
            report = run_callback_injection(
                CallbackInjectionSettings(
                    iterations=args.callback_injection_iterations,
                    burst_width=args.callback_injection_burst_width,
                    slow_consumer_delay_seconds=args.callback_injection_delay_ms / 1_000,
                    trace_output=args.output.with_name(f"{args.output.stem}.injected.traces.jsonl"),
                    fixture_owner=args.fixture_owner,
                    runtime_dir=args.output.parent / f"{args.output.stem}-runtime",
                    journal_mode=args.callback_injection_journal,
                    require_native_fastspi=not args.allow_python_fastspi,
                )
            )
        except CallbackInjectionError as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-session-fault":
        if args.sdk_root is None or args.accounts_file is None:
            parser.error("--sdk-root and --accounts-file are required for gateway-session-fault")
        if args.profile != "ordinary":
            parser.error("gateway-session-fault currently permits only --profile ordinary")
        if not args.fixture_owner.strip():
            parser.error("--fixture-owner is required for gateway-session-fault")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_session_fault(
                SessionFaultSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-session-fault-runtime",
                    accounts=accounts,
                    fixture_owner=args.fixture_owner,
                    cycles=args.session_fault_cycles,
                    callback_timeout_seconds=args.session_fault_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    disconnect_blackout_seconds=args.session_fault_blackout_seconds,
                    transport_mode=args.session_fault_transport,
                    proxy_listen_port=args.session_fault_proxy_port,
                    acceptance_standard=args.session_fault_acceptance_standard,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    require_native_fastspi=not args.allow_python_fastspi,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                    isolate_native_worker=args.isolate_after_report,
                )
            )
        except SessionFaultError as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-fixture":
        if args.runbook is None:
            parser.error("--runbook is required for gateway-fixture")
        try:
            report = run_fixture_driver(
                FixtureDriverSettings(
                    runbook=args.runbook,
                    trace_output=args.output.parent / "gateway-fixture-traces.jsonl",
                    command=tuple(args.fixture_command or ()),
                    workload_plan=args.workload_plan,
                    execute_simulated=args.execute_simulated,
                )
            )
        except FixtureDriverError as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-readonly":
        if args.sdk_root is None or args.accounts_file is None:
            parser.error("--sdk-root and --accounts-file are required for gateway-readonly")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_gateway_readonly(
                GatewayReadOnlySettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / f"gateway-readonly-{args.profile}-runtime",
                    accounts=accounts,
                    profile=args.profile,
                    iterations=args.readonly_iterations,
                    callback_timeout_seconds=args.callback_timeout,
                    callback_capacity=args.callback_capacity,
                    interval_seconds=args.interval,
                    probe_known_callback_gaps=args.probe_known_callback_gaps,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                ),
                retain_native_reference=(
                    retained_native_references.append if args.isolate_after_report else None
                ),
            )
        except RuntimeError as exc:
            parser.error(str(exc))
    elif args.driver in {"gateway-query-fixture", "gateway-query-burst", "gateway-query-late"}:
        if args.sdk_root is None or args.accounts_file is None or args.query_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --query-fixture are required for gateway query fixture drivers"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            minimum_callback_capacity = required_callback_capacity(
                args.query_fixture, profile=args.profile
            )
            if args.callback_capacity < minimum_callback_capacity:
                raise QueryFixtureError(
                    f"query fixture for {args.profile} requires --callback-capacity >= "
                    f"{minimum_callback_capacity} to preserve its terminal callback"
                )
            cases = select_query_cases(
                load_query_fixture(args.query_fixture, profile=args.profile),
                tuple(args.query_case) if args.query_case is not None else None,
            )
            report = run_gateway_readonly(
                GatewayReadOnlySettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent
                    / f"gateway-query-{args.driver.removeprefix('gateway-query-')}-{args.profile}-runtime",
                    accounts=accounts,
                    profile=args.profile,
                    iterations=(
                        args.burst_rounds
                        if args.driver == "gateway-query-burst"
                        else args.late_iterations
                        if args.driver == "gateway-query-late"
                        else args.readonly_iterations
                    ),
                    callback_timeout_seconds=args.callback_timeout,
                    callback_capacity=args.callback_capacity,
                    interval_seconds=args.interval,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    workload=(
                        "burst"
                        if args.driver == "gateway-query-burst"
                        else "late"
                        if args.driver == "gateway-query-late"
                        else "steady"
                    ),
                    burst_width=args.burst_width if args.driver == "gateway-query-burst" else 1,
                    late_delay_seconds=args.late_delay_ms / 1_000
                    if args.driver == "gateway-query-late"
                    else 0.0,
                ),
                cases=cases,
                known_callback_gaps=(),
                driver_name={
                    "gateway-query-fixture": "gateway-query-fixture-c0-c7",
                    "gateway-query-burst": "gateway-query-burst-c0-c7",
                    "gateway-query-late": "gateway-query-late-c0-c7",
                }[args.driver],
                retain_native_reference=(
                    retained_native_references.append if args.isolate_after_report else None
                ),
            )
        except (QueryFixtureError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-query-semantics":
        if args.sdk_root is None or args.accounts_file is None or args.semantic_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --semantic-fixture are required for gateway-query-semantics"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            cases = load_query_semantic_fixture(
                args.semantic_fixture,
                profile=args.profile,
                workload=args.semantic_workload,
            )
            report = run_gateway_readonly(
                GatewayReadOnlySettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent
                    / f"gateway-query-semantics-{args.semantic_workload}-{args.profile}-runtime",
                    accounts=accounts,
                    profile=args.profile,
                    iterations=args.semantic_iterations,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=args.interval,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    workload=args.semantic_workload,
                ),
                cases=cases,
                known_callback_gaps=(),
                driver_name="gateway-query-semantics-c0-c7",
                retain_native_reference=(
                    retained_native_references.append if args.isolate_after_report else None
                ),
            )
        except (QuerySemanticFixtureError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-mutation":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-mutation"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_gateway_mutation(
                GatewayMutationSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / f"gateway-mutation-{args.profile}-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    profile=args.profile,
                    cycles=args.mutation_cycles,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    execute_simulated=args.execute_simulated_mutations,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (MutationFixtureError, GatewayMutationError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-ab-order":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-ab-order"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_gateway_ab(
                GatewayAbSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / f"gateway-ab-order-{args.profile}-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    profile=args.profile,
                    cycles=args.ab_cycles,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                )
            )
        except (MutationFixtureError, GatewayAbError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-mutation-recovery":
        if args.sdk_root is None or args.accounts_file is None:
            parser.error(
                "--sdk-root and --accounts-file are required for gateway-mutation-recovery"
            )
        if args.profile != "ordinary":
            parser.error("gateway-mutation-recovery currently permits only --profile ordinary")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_mutation_recovery(
                MutationRecoverySettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-mutation-recovery-runtime",
                    accounts=accounts,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    report_output=args.output,
                    callback_timeout_seconds=args.callback_timeout,
                    cancel_unfinished_orders=not args.mutation_recovery_query_only,
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    isolate_after_report=args.mutation_recovery_isolate_after_report,
                )
            )
        except (MutationRecoveryError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-credit-repayment":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-credit-repayment"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_credit_repayment(
                CreditRepaymentSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-credit-repayment-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    cycles=args.credit_repayment_cycles,
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (CreditRepaymentError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-credit-quota-transfer":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-credit-quota-transfer"
            )
        if args.profile != "credit":
            parser.error("gateway-credit-quota-transfer requires --profile credit")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_credit_quota_transfer(
                CreditQuotaTransferSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-credit-quota-transfer-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    callback_timeout_seconds=args.callback_timeout,
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                )
            )
        except (CreditRepaymentError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-credit-quota-pair":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-credit-quota-pair"
            )
        if args.profile != "credit":
            parser.error("gateway-credit-quota-pair requires --profile credit")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_credit_quota_pair(
                CreditQuotaPairSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-credit-quota-pair-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    pairs=args.credit_quota_pair_cycles,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (CreditRepaymentError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-fund-transfer-pair":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-fund-transfer-pair"
            )
        if args.profile != "ordinary":
            parser.error("gateway-fund-transfer-pair requires --profile ordinary")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_fund_transfer_pair(
                FundTransferPairSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-fund-transfer-pair-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    pairs=args.fund_transfer_pair_cycles,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (CreditRepaymentError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-trade-fill":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-trade-fill"
            )
        if args.profile not in {"ordinary", "credit"}:
            parser.error("gateway-trade-fill requires --profile ordinary or credit")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_gateway_trade_fill(
                TradeFillSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / f"gateway-trade-fill-{args.profile}-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    profile=args.profile,
                    cycles=args.trade_fill_cycles,
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (MutationFixtureError, GatewayMutationError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-controlled-order":
        if (
            args.sdk_root is None
            or args.accounts_file is None
            or args.controlled_order_fixture is None
        ):
            parser.error(
                "--sdk-root, --accounts-file and --controlled-order-fixture are required for gateway-controlled-order"
            )
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_controlled_order_experiment(
                ControlledOrderSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-controlled-order-runtime",
                    accounts=accounts,
                    fixture=args.controlled_order_fixture,
                    scenario_ids=tuple(args.controlled_order_scenario or ()),
                    callback_timeout_seconds=args.callback_timeout,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    local_latency_gate_report=args.local_latency_gate_report,
                    skip_local_latency_gate=args.skip_local_latency_gate,
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                    verify_cancel_error=args.verify_cancel_error,
                    retain_native_reference=(
                        retained_native_references.append if args.isolate_after_report else None
                    ),
                )
            )
        except (ControlledOrderError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-local-signal-burst":
        try:
            report = run_local_signal_burst(
                LocalSignalBurstSettings(
                    runtime_dir=args.output.parent / "gateway-local-signal-burst-runtime",
                    signals_per_second=args.signal_rate,
                    rounds=args.signal_rounds,
                    warmup_rounds=args.signal_warmup_rounds,
                    journal_mode=args.signal_journal_mode,
                    group_commit_max_wait_ms=args.signal_group_wait_ms,
                    group_commit_max_batch_size=args.signal_group_size,
                    callback_batch_size=args.signal_callback_batch_size,
                    callback_collect_wait_ms=args.signal_callback_collect_wait_ms,
                    arrival_pattern=args.signal_arrival,
                )
            )
        except (LocalSignalBurstError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-option-action":
        if args.sdk_root is None or args.accounts_file is None or args.mutation_fixture is None:
            parser.error(
                "--sdk-root, --accounts-file and --mutation-fixture are required for gateway-option-action"
            )
        if args.profile != "option":
            parser.error("gateway-option-action requires --profile option")
        accounts = load_simulated_accounts(args.accounts_file)
        try:
            report = run_option_action(
                OptionActionSettings(
                    sdk_root=args.sdk_root,
                    runtime_dir=args.output.parent / "gateway-option-action-runtime",
                    accounts=accounts,
                    fixture=args.mutation_fixture,
                    trace_output=args.output.with_name(f"{args.output.stem}.traces.jsonl"),
                    callback_timeout_seconds=args.callback_timeout,
                    interval_seconds=max(args.interval, 1.0),
                    execute_simulated=args.execute_simulated_mutations,
                    require_native_fastspi=not args.allow_python_fastspi,
                )
            )
        except (OptionActionError, RuntimeError) as exc:
            parser.error(str(exc))
    elif args.driver == "gateway-mutation-aggregate":
        if not args.mutation_trace_input and not args.trace_input_directory:
            parser.error(
                "--mutation-trace-input or --trace-input-directory is required for gateway-mutation-aggregate"
            )
        events: list[dict[str, object]] = []
        try:
            trace_inputs = _collect_trace_inputs(
                args.mutation_trace_input, args.trace_input_directory
            )
            trace_paths, trace_provenance = _select_trace_inputs(
                trace_inputs,
                args.exclude_trace_input,
                args.trace_exclusion_reason,
            )
            for trace_path in trace_paths:
                for line_number, line in enumerate(
                    trace_path.read_text(encoding="utf-8").splitlines(), start=1
                ):
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        parser.error(
                            f"mutation trace {trace_path.name} line {line_number} must be a JSON object"
                        )
                    events.append(event)
            report = build_gateway_report(events)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            parser.error(f"cannot aggregate mutation traces: {exc}")
        report["driver"] = "gateway-mutation-aggregate"
        report["batches"] = len(trace_paths)
        trace_provenance["trace_input_directories"] = [
            str(path) for path in args.trace_input_directory or []
        ]
        report["trace_provenance"] = trace_provenance
    elif args.driver == "native-readonly":
        if args.sdk_root is None:
            parser.error("--sdk-root is required for native-readonly")
        if args.accounts_file is not None:
            accounts = load_simulated_accounts(args.accounts_file)
            host, port = accounts.host, accounts.port
            account, password = accounts.ordinary.account, accounts.ordinary.password
        else:
            host, port, account = args.host or "", args.port, args.account or ""
            password = os.getenv(args.password_env)
            if not password:
                password = getpass.getpass(f"{args.password_env}: ")
        settings = NativeSettings(
            sdk_root=args.sdk_root,
            host=host,
            port=port,
            account=account,
            password=password,
            client_id=args.client_id,
            local_ip=args.local_ip,
            callback_timeout_seconds=args.callback_timeout,
            interval_seconds=args.interval,
            runtime_dir=args.output.parent / "native-runtime",
        )
        report = run_read_only(settings, iterations=args.iterations)
    else:
        if args.sdk_root is None or args.accounts_file is None:
            parser.error("--sdk-root and --accounts-file are required for vendor-simulated-sweep")
        accounts = load_simulated_accounts(args.accounts_file)
        report = run_vendor_sweep(
            VendorSweepSettings(
                sdk_root=args.sdk_root,
                runtime_dir=args.output.parent / f"vendor-{args.profile}-runtime",
                accounts=accounts,
                profile=args.profile,
            )
        )
    # A mutation fixture is configuration, not a performance report.  Its
    # strict schema deliberately excludes the report-only ``driver`` field,
    # so it must bypass finalization and be emitted unchanged.  This command
    # never loads the SDK or carries credentials.
    if args.driver == "mutation-fixture-template":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"output": str(args.output), "driver": args.driver}, ensure_ascii=False))
        return 0
    try:
        report = finalize_performance_report(report, requested_driver=args.driver)
    except PerformanceReportSchemaError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {"output": str(args.output), "driver": report.get("driver", args.driver)},
            ensure_ascii=False,
        )
    )
    if (
        args.driver == "gateway-release-readiness"
        and args.require_release_ready
        and not report["release_ready"]
    ):
        return 2
    if args.isolate_after_report:
        # The reference remains live until this point, so CPython cannot run
        # the vendor wrapper destructor.  This is intentionally confined to
        # a throw-away acceptance process after its payload-free report is on
        # disk; it is never a production shutdown policy.
        os._exit(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
