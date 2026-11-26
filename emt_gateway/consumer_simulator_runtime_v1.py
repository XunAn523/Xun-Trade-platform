"""Executable single-account, submit-only Consumer to EMT simulator runtime.

The separate command requires protected simulator configuration and systemd
credentials. It never grants normal READY, asserts venue-wide uniqueness,
retries an uncertain order, or automatically reopens a consumed Journal.
"""

from __future__ import annotations

import json
import os
import secrets
import signal
import threading
import time
from bisect import bisect_left
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    ActivatedVenueMapping,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    MmapRouteLedgerV2,
    load_emt_disarmed_profile,
    load_emt_disarmed_signer_trust,
)
from xuanling_tradecore.adapters.emt.identity import EmtClientOrderRefScope
from xuanling_tradecore.adapters.emt.mapping import load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.adapters.emt.namespace_recovery import (
    VerifiedNamespaceBoundEmtClientOrderIdAllocator,
)
from xuanling_tradecore.adapters.emt.route_token import EmtAdapterRouteTokenCodec
from xuanling_tradecore.compatibility.emt_consumer.emt_callback_pump_v1 import (
    ManagedEmtCallbackPumpV1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_outcome_projection_v1 import (
    EmtCommittedOutcomeProjectorV1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_submit_composition_v1 import (
    ManagedEmtConsumerSubmitV1,
    ManagedEmtExecutionSessionV1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_submit_disposition_v1 import (
    MmapEmtSubmitDispositionJournalV1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_submit_route_preparer_v1 import (
    EmtConsumerSubmitRoutePreparerV1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_submit_uds_v1 import (
    build_consumer_emt_submit_uds_v1,
)
from xuanling_tradecore.compatibility.emt_consumer.emt_submit_worker_v1 import (
    EmtConsumerSubmitWorkerV1,
)
from xuanling_tradecore.consumer.base import ConsumerAuthorizationError
from xuanling_tradecore.consumer.cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from xuanling_tradecore.consumer.oms_operations_v2 import ConsumerOmsAdmissionContextV2
from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayServiceV2
from xuanling_tradecore.consumer.snapshot_v2 import ConsumerSnapshotProjectionV2
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.consumer.uds_server_v2 import _OrderBookProjectionV2
from xuanling_tradecore.domain import (
    AccountLifecycle,
    AttributionBinding,
    CancelDeadlinePolicy,
    ConsumerEventPositionV2,
    ConsumerPrincipal,
    DecisionContext,
    ExecutionDeadlinePolicy,
    RedisStreamIdV1,
    RiskSnapshot,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountExecutionRoutePlan,
    AccountExecutionRouter,
    AccountFenceScope,
    AccountOwnershipFence,
    ExecutionGuard,
    ExecutionGuardAdmissionGate,
    ExecutionGuardPolicy,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    ExecutionSessionRuntime,
    GuardMarketSnapshot,
    KillSwitchSnapshot,
    canonical_broker_capabilities_hash,
)
from xuanling_tradecore.oms import (
    MmapOmsFactJournalV1,
    OmsFactKind,
    OmsJournalFact,
    OrderBook,
    ReservationRequirement,
)
from xuanling_tradecore.oms.mmap_client_ref_journal_v1 import MmapClientRefJournalV1
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    MmapCallbackOutcomeJournal,
    MmapPreNativeJournal,
)

from .consumer_runtime_control_v1 import LinuxRootOwnedNamespaceFileV1
from .consumer_simulator_gate_v1 import load_emt_simulator_permit_v1
from .consumer_simulator_host_inputs_v1 import (
    SimulatorHostInputPathsV1,
    load_simulator_host_inputs_v1,
)
from .consumer_simulator_profile_v1 import load_simulator_profile_binding_v1
from .fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeObserver,
)
from .fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from .platform_loader import EmtPlatformLoader, SdkLoadSettings
from .provisioned_execution_factory_v1 import ProvisionedEmtExecutionProfileV1
from .simulator_session_supervisor_v1 import SimulatorSessionSupervisorV1
from .tradecore_consumer_session_v1 import build_provisioned_consumer_emt_adapter_v1
from .tradecore_mmap_journal import EmtMmapNativeCallJournal, PreparedEmtNativeRouteResolver

_NATIVE_RETAINED: list[object] = []


@dataclass(frozen=True)
class SimulatorRuntimeConfigV1:
    config_root: Path
    runtime_root: Path
    socket_path: Path
    contract_root: Path
    sdk_root: Path
    trading_day: TradingDayEpoch
    allowed_peers: frozenset[tuple[int, int]]
    capacity: int
    max_pending: int
    symbols: tuple[str, ...]
    owner_instance_id: str
    host_id: str


def load_simulator_runtime_config_v1(path: Path) -> SimulatorRuntimeConfigV1:
    data = json.loads(LinuxRootOwnedNamespaceFileV1(path).read())
    if not isinstance(data, dict) or set(data) != {
        "schema_version", "runtime_root", "socket_path", "contract_root", "sdk_root", "trading_day",
        "allowed_peers", "capacity", "max_pending", "symbols", "owner_instance_id", "host_id",
    } or type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("simulator runtime configuration schema is invalid")
    for key in ("runtime_root", "socket_path", "contract_root", "sdk_root"):
        if not isinstance(data[key], str) or not Path(data[key]).is_absolute() or ".." in Path(data[key]).parts:
            raise ValueError("simulator runtime requires absolute safe paths")
    if (
        type(data["capacity"]) is not int or not 16 <= data["capacity"] <= 100_000
        or type(data["max_pending"]) is not int or not 1 <= data["max_pending"] <= min(512, data["capacity"])
        or not isinstance(data["allowed_peers"], list) or not 1 <= len(data["allowed_peers"]) <= 32
        or any(not isinstance(row, list) or len(row) != 2 or any(type(x) is not int or x < 0 for x in row) for row in data["allowed_peers"])
        or not isinstance(data["symbols"], list) or not 1 <= len(data["symbols"]) <= 128
        or any(not isinstance(s, str) or len(s) != 6 or not s.isascii() or not s.isdecimal() for s in data["symbols"])
        or any(not isinstance(data[k], str) or not 1 <= len(data[k]) <= 64 for k in ("owner_instance_id", "host_id"))
    ):
        raise ValueError("simulator runtime capacity or identity is invalid")
    return SimulatorRuntimeConfigV1(
        path.parent, *(Path(data[k]) for k in ("runtime_root", "socket_path", "contract_root", "sdk_root")),
        TradingDayEpoch(data["trading_day"]), frozenset(tuple(row) for row in data["allowed_peers"]),
        data["capacity"], data["max_pending"], tuple(data["symbols"]), data["owner_instance_id"], data["host_id"],
    )


class _Audit:
    def __init__(self, journal, account):
        self.journal, self.account = journal, account

    def record_execution_rejected(self, instruction, decision):
        self.journal.append_batch((OmsJournalFact(
            "sim-reject:" + secrets.token_hex(16), OmsFactKind.ORDER_REJECTED, self.account,
            instruction.oms_order_id, instruction.execution_id, {"guard_code": decision.code.value},
        ),))


class _Observer:
    def __init__(self):
        self.event_count = 0
        self.gap_count = 0

    def on_broker_event(self, event):
        self.event_count += 1

    def on_callback_gap(self, evidence):
        self.gap_count += 1


class _Pacer:
    def __init__(self):
        self.next_ns = 0

    def __call__(self):
        now = time.monotonic_ns()
        if now < self.next_ns:
            time.sleep((self.next_ns - now) / 1e9)
        self.next_ns = time.monotonic_ns() + 2_100_000  # < 499 calls/s; one owner, no catch-up burst.


def build_simulator_runtime_v1(config: SimulatorRuntimeConfigV1, *, sdk_provider=None):
    """Construct a new, one-shot runtime; dependencies never come from requests."""

    root, public = config.runtime_root, config.config_root
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.lstat().st_uid != os.geteuid() or root.lstat().st_mode & 0o077:
        raise ValueError("simulator runtime root must be owner-private")
    # This marker is never automatically removed. A new boot after any prior
    # started runtime needs an operator-reviewed generation, not empty Journals.
    marker_fd = os.open(root / "started-once", os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    os.write(marker_fd, (str(os.getpid()) + "\n").encode())
    os.fsync(marker_fd)
    os.close(marker_fd)
    permit = load_emt_simulator_permit_v1(public / "permit.json")
    if not 1 <= permit.emt_client_id <= 127:
        raise ValueError("simulator native client id is outside this deployment compatibility bound")
    selected = load_simulator_profile_binding_v1(public / "profile.json").active_binding_and_profile(
        binding_revision_id="emt-simulator-binding", now_wall_ns=time.time_ns()
    )
    account_id = selected.binding.account_id
    control_root, journals = root / "control", root / "journals"
    for directory in (control_root, control_root / "accounts", journals):
        directory.mkdir(mode=0o700, exist_ok=True)
    scope = AccountFenceScope(account_id, "EMT", str(permit.emt_client_id))
    (control_root / "accounts" / scope.scope_hash).mkdir(mode=0o700, exist_ok=True)
    fence = AccountOwnershipFence(
        runtime_control_root=control_root, scope=scope, instance_id=config.owner_instance_id,
        host_id=config.host_id, process_started_monotonic_ns=time.monotonic_ns(),
        journal_id="emt-sim-consumer", journal_root=journals, stream_generation=1,
    )
    fence.acquire(acquired_monotonic_ns=time.monotonic_ns())
    mono, wall = time.monotonic_ns(), time.time_ns()
    ttl = min(permit.expires_wall_ns - wall, 7 * 86_400_000_000_000)
    if ttl <= 0:
        raise ValueError("simulator permit has expired")
    risk = RiskSnapshot("operator-simulator-budget", account_id, config.trading_day, "0" * 64, mono, mono + ttl, 10**15, 10**15, {s: 10**12 for s in config.symbols})
    inputs = load_simulator_host_inputs_v1(
        paths=SimulatorHostInputPathsV1(
            public / "profile.json", public / "namespace-release.json", public / "namespace-trust.json",
            root / "namespace-state.json", public / "permit.json",
            Path(os.environ["CREDENTIALS_DIRECTORY"]), "emt-simulator-account",
        ),
        binding_revision_id=selected.binding.binding_revision_id, trading_day=config.trading_day,
        risk=risk, require_ownership=fence.require_held, now_wall_ns=time.time_ns, now_monotonic_ns=time.monotonic_ns,
    )
    gate, control = inputs.gate, inputs.control
    for name in ("scope-profile.json", "scope-public.json", "scope-revocations.json", "sdk-hashes.json"):
        LinuxRootOwnedNamespaceFileV1(public / name).read()
    signed_profile = load_emt_disarmed_profile(public / "scope-profile.json")
    trust = load_emt_disarmed_signer_trust(public_key_path=public / "scope-public.json", revocations_path=public / "scope-revocations.json")
    trust.verify_profile(signed_profile, now_wall_ns=wall)
    matches = [s for s in signed_profile.signed_scope_ownerships if s.ownership.account_id == account_id and s.ownership.adapter_client_scope == str(permit.emt_client_id)]
    if len(matches) != 1:
        raise ValueError("simulator signed scope selection is ambiguous")
    signed = matches[0]
    manifest = replace(load_builtin_emt_legacy_sim_manifest(), scope_ownership=signed.ownership)
    # Explicit simulator authorization; preserve false probe flags truthfully.
    mapping = ActivatedVenueMapping(manifest, RuntimeVenueProfile(
        fingerprint=manifest.fingerprint, capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities(account_id, True, False, False),
        scope_ownership=signed.ownership, route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({trust.signer_id}), route_token_probe_succeeded=False,
    ))
    binding = control.consumer_bindings.accounts[0]
    execution_binding = AccountExecutionBinding(
        binding.binding_id, binding.binding_version, account_id, "EMT", binding.adapter_instance_id,
        str(permit.emt_client_id), scope.scope_hash, manifest.manifest_hash,
        canonical_broker_capabilities_hash(manifest.capabilities), control.namespace.state.release_manifest_hash, 1, 0,
    )
    capacity = config.capacity
    oms = MmapOmsFactJournalV1(path=journals / "oms.mmap", journal_id="emt-sim-oms", capacity=capacity * 8)
    book = OrderBook(journal=oms, persist_recovery_seed=True)
    ref_root = root / "references"
    bootstrap_refs = not ref_root.exists()
    ref_root.mkdir(mode=0o700, exist_ok=True)
    ref_scope = EmtClientOrderRefScope(account_id, config.trading_day, binding.adapter_instance_id, permit.emt_client_id)
    refs = MmapClientRefJournalV1(root=ref_root, scope=ref_scope.canonical_scope, capacity=capacity, bootstrap=bootstrap_refs)
    allocator = VerifiedNamespaceBoundEmtClientOrderIdAllocator(emt_scope=ref_scope, directory=control.namespace.directory, journal=refs)
    allocator.recover_from_journal()
    resolver = PreparedEmtNativeRouteResolver(maximum_pending_routes=capacity)
    preparer = EmtConsumerSubmitRoutePreparerV1(
        oms_journal=oms, client_ref_journal=refs, allocator=allocator, activated_release=control.namespace,
        expected_release_manifest_hash=control.namespace.state.release_manifest_hash,
        consumer_binding=binding, execution_binding=execution_binding,
        journal_binding=AccountJournalBinding(account_id, "EMT", f"xuanling:journal:{account_id}:", 0),
        codec=EmtAdapterRouteTokenCodec(manifest.route_token_binding), resolver=resolver,
        admission_authorized=gate.admission_permitted, close_admission=lambda reason: gate.close(),
        wall_clock_ns=time.time_ns, monotonic_ns=time.monotonic_ns, native_start_budget_ns=60_000_000_000,
    )
    stream = CursorStreamRefV2(oms.journal_id, 1)
    registry = CursorStreamRegistryV2(stream, (stream,))
    projection = _OrderBookProjectionV2(order_book=book, binding=binding, stream=stream)
    publish_lock = threading.Lock()

    def publish(event):
        # Both admission and SDK drain may publish. Reconstruct their exact
        # committed order from the book rather than requiring notification order.
        with publish_lock:
            items = projection._items()
            position = ConsumerEventPositionV2(RedisStreamIdV1(event.journal_lsn, 0), 0)
            index = bisect_left(projection._replay_positions, position)
            if index >= len(items) or items[index] != (position, event):
                raise ValueError("simulator event projection does not match committed OMS state")

    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book, adapter_instance_id=binding.adapter_instance_id,
        adapter_client_scope=str(permit.emt_client_id), publish=publish,
        close_admission=lambda reason: gate.close(),
    )
    native = MmapPreNativeJournal(path=journals / "native.mmap", capacity=capacity * 3)
    callbacks = MmapCallbackOutcomeJournal(
        journals={"events": MmapPreNativeJournal(path=journals / "callback.mmap", capacity=capacity * 16)},
        cancel_command_journals={"disabled": MmapPreNativeJournal(path=journals / "cancel-disabled.mmap", capacity=16)},
    )
    route_ledger = MmapRouteLedgerV2(path=journals / "routes.mmap", capacity=capacity * 2, directory=control.namespace.directory)
    outcomes = FastSpiV2RouteOutcomeDirectory(config=FastSpiV2RouteOutcomeDirectoryConfig(account_id, binding.adapter_instance_id, str(permit.emt_client_id), capacity))
    cancel_outcomes = FastSpiV2CancelOutcomeObserver(
        config=FastSpiV2CancelOutcomeDirectoryConfig(account_id, binding.adapter_instance_id, str(permit.emt_client_id), 16), journal=callbacks,
    )
    native_journal = EmtMmapNativeCallJournal(
        route_resolver=resolver, order_outcome_directory=outcomes, cancel_outcome_observer=cancel_outcomes,
        mmap_pre_native_journals={"simulator": native}, mmap_callback_outcomes=callbacks,
        mmap_route_ledger=route_ledger, committed_outcome_hook=projector, recovery_submit_guard=gate.require_account,
    )
    digest = sha256(b"tradecore-emt-simulator-submit-only").hexdigest()
    scheduler = ExecutionScheduler(
        account_id=account_id, process_epoch_id="sim-" + secrets.token_hex(8),
        policy=ExecutionSchedulerPolicy("simulator", 1, digest, config.max_pending, 1, config.max_pending, 1, config.max_pending * 4096, 4096, 1000),
        execution_deadline_policy=ExecutionDeadlinePolicy("sim-submit", 1, digest, 60_000_000_000),
        cancel_deadline_policy=CancelDeadlinePolicy("disabled", 1, digest, 1_000_000),
    )
    router = AccountExecutionRouter(plan=AccountExecutionRoutePlan("simulator", 1, digest, (execution_binding,)), schedulers={account_id: scheduler})
    provider = sdk_provider or EmtPlatformLoader(SdkLoadSettings(config.sdk_root, expected_hashes=json.loads(LinuxRootOwnedNamespaceFileV1(public / "sdk-hashes.json").read())))
    supervisor = SimulatorSessionSupervisorV1(
        simulator_gate=gate, binding=execution_binding, activated_mapping=mapping, ownership_fence=fence,
        adapter_factory=lambda: build_provisioned_consumer_emt_adapter_v1(
            mapping=mapping, runtime_profile=ProvisionedEmtExecutionProfileV1(control.profile_binding.profile, root / "sdk-logs", permit.emt_client_id),
            control_binding=control.profile_binding.binding, secret_provider=inputs.secret_provider, sdk_provider=provider,
            signed_scope_ownership=signed, scope_ownership_verifier=trust.verifier_at(now_wall_ns=time.time_ns()),
            preparer=preparer, native_journal=native_journal, projector=projector,
            release_native_inflight=scheduler.release_new_order_inflight,
            expected_api_version="2.27.0", session_epoch=1, now_wall_ns=time.time_ns(), simulator_gate=gate,
            native_reference_retainer=_NATIVE_RETAINED.append,
        ),
    )
    audit_journal = MmapOmsFactJournalV1(path=journals / "audit.mmap", journal_id="sim-audit", capacity=capacity)
    strategies = control.consumer_bindings.strategies
    guard = ExecutionGuardAdmissionGate(ExecutionGuard(ExecutionGuardPolicy(
        "simulator-static-policy-not-market-data", 1, digest, account_id, 1_000_000_000,
        499, {s.strategy_id: 499 for s in strategies}, {s: 499 for s in config.symbols},
        {"BUY": 499, "SELL": 499}, 10_000, 100, 10**10,
    )), _Audit(audit_journal, account_id))
    runtime = ExecutionSessionRuntime(binding=execution_binding, router=router, scheduler=scheduler, supervisor=supervisor, guard_admission=guard)
    dispositions = MmapOmsFactJournalV1(path=journals / "dispatch.mmap", journal_id="sim-dispatch", capacity=capacity)
    sink = MmapEmtSubmitDispositionJournalV1(journal=dispositions, account_id=account_id, binding_id=binding.binding_id, binding_generation=binding.binding_version)

    def market(instruction):
        now = time.monotonic_ns()
        return GuardMarketSnapshot("simulator-static-price-policy", instruction.symbol, instruction.market, instruction.limit_price_ticks, 1, 10**8, now, now + 60_000_000_000)

    worker = EmtConsumerSubmitWorkerV1(
        preparer=preparer, runtime=runtime, native_journal=native_journal, lane_id="simulator",
        market_for_execution=market, kill_switches_for_account=lambda _: KillSwitchSnapshot(0),
        serialized_size_for_execution=lambda _: 1024, record_disposition=sink.record,
        now_monotonic_ns=time.monotonic_ns, max_pending=config.max_pending, max_seen=capacity, before_dispatch=_Pacer(),
    )
    managed = ManagedEmtConsumerSubmitV1(worker)
    pump = ManagedEmtCallbackPumpV1(runtime=runtime, failure_target=managed, poll_seconds=0.001)
    observer = _Observer()
    replay_keys = CursorKeyRingV2(current=CursorSigningKeyV2("simulator-runtime", secrets.token_bytes(32)))
    replay = ConsumerCursorReplayServiceV2(key_ring_for=lambda _: replay_keys, registry_for=lambda _: registry, event_reader=projection, partition_reader=projection, cursor_ttl_ns=3_600_000_000_000)

    def context_for(candidate, strategy_id, request):
        strategy = next(s for s in strategies if s.binding_id == strategy_id)
        if candidate != binding or request.symbol not in config.symbols or request.side not in {"BUY", "SELL"} or request.market not in {"SH", "SZ"} or request.trading_day != config.trading_day or request.business_type != "CASH" or request.price_type != "LIMIT" or request.limit_price_ticks is None or request.limit_price_ticks <= 0 or not 1 <= request.quantity <= 100 or request.execution_parameters:
            raise ConsumerAuthorizationError("signal is outside simulator submit-only profile")
        now = time.monotonic_ns()
        decision = DecisionContext("sim-" + secrets.token_hex(16), account_id, config.trading_day, risk.risk_snapshot_id, risk.rule_bundle_hash, request.protection_price_ticks, "sim-fee", "sim-margin", "sim-reservation", now)
        attribution = AttributionBinding("sim-attribution:" + strategy.strategy_id, binding.binding_id, strategy.binding_id, binding.consumer_id, binding.consumer_account_id, strategy.strategy_id, binding.binding_version)
        requirement = (
            ReservationRequirement(request.symbol, cash_minor=request.quantity * request.limit_price_ticks)
            if request.side == "BUY" else ReservationRequirement(request.symbol, sellable_quantity=request.quantity)
        )
        return ConsumerOmsAdmissionContextV2(strategy, attribution, risk, decision, requirement)

    def account_view(candidate):
        view = gate.account_view(candidate)
        if view.lifecycle is AccountLifecycle.SIMULATOR_ACTIVE and (managed.failed or not pump.healthy()):
            return replace(view, lifecycle=AccountLifecycle.DISARMED)
        return view

    service = build_consumer_emt_submit_uds_v1(
        socket_path=config.socket_path, contract_root=config.contract_root,
        principal=ConsumerPrincipal(binding.consumer_id, "emt-simulator", "operator-authorized-simulator"),
        allowed_peers=config.allowed_peers, binding_snapshot=control.consumer_bindings, account_view_for=account_view,
        order_book=book, managed_worker=managed, admission_context_for=context_for, now_monotonic_ns=time.monotonic_ns,
        on_order_event_committed=publish, snapshot_for=ConsumerSnapshotProjectionV2(registry_for=lambda _: registry, reader=projection), event_replay=replay,
        managed_session=ManagedEmtExecutionSessionV1(runtime=runtime, observer=observer, now_wall_ns=time.time_ns),
        callback_pump=pump, runtime_gate=gate, managed_gate=gate,
        socket_mode=0o660,
    )
    return service, inputs, managed, pump, observer, (native_journal, audit_journal, dispositions, refs, oms), fence


def run_consumer_emt_simulator_v1(*, config_path: Path) -> int:
    config = load_simulator_runtime_config_v1(config_path)
    service, inputs, worker, pump, observer, resources, fence = build_simulator_runtime_v1(config)
    stop = threading.Event()
    failed = []

    def status_monitor():
        while not stop.wait(0.5):
            healthy = inputs.gate.admission_permitted() and not worker.failed and pump.healthy() and not observer.gap_count
            record = {"schema_version": 1, "pid": os.getpid(), "simulator_only": True, "admission_open": healthy, "broker_event_count": observer.event_count, "callback_gap_count": observer.gap_count, "updated_wall_ns": time.time_ns()}
            temporary = config.runtime_root / "health.tmp"
            temporary.write_text(json.dumps(record), encoding="utf-8")
            temporary.replace(config.runtime_root / "health.json")
            if not healthy:
                failed.append(True)
                inputs.gate.close()
                service.request_stop()
                return

    previous = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, lambda _sig, _frame: service.request_stop())
    monitor = threading.Thread(target=status_monitor, name="simulator-health")
    try:
        service.start()
        monitor.start()
        service.serve_forever()
    finally:
        stop.set()
        if monitor.is_alive():
            monitor.join(2)
        service.stop()
        for resource in resources:
            resource.close()
        fence.release()
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return 2 if failed else 0
