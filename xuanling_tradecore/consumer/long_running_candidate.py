"""Isolated persistent no-Adapter service for qualification, NOT a release gate.

This explicit entry point does not register LONG_RUNNING in the production
builder. Only a complete same-revision qualification may do that. No broker
credential, SDK, automatic dispatch or trading READY is available here.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import signal
import sys
import uuid
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from threading import Event, get_ident
from time import monotonic_ns
from typing import Any, Callable

from ..domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
    DecisionContext,
    RiskSnapshot,
)
from ..oms import ReservationRequirement
from ..persistence.commit_timing import CommitTimingWriter
from ..persistence.runtime_files import atomic_runtime_json
from ..read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from ..read_model.process_order_projector import ProcessOrderProjector
from ..read_model.process_projection_reads import ProcessProjectionReads
from ..read_model.versioned_snapshot_reader import VersionedConsumerSnapshotReader
from .base import ConsumerOrderSignal
from .bindings_v2 import ConsumerBindingSnapshotV2
from .candidate_affinity import (
    preserve_candidate_affinity,
    set_thread_affinity_exact,
    temporary_thread_affinity,
    validate_role_cpus,
)
from .candidate_helper import OwnedCandidateTracker
from .candidate_systemd import require_coordinated_candidate_unit
from .cold_admission_v2 import ConsumerAdmissionCapacityV2
from .cold_aware_oms_runtime import ColdAwareOmsRuntime
from .dispatch_v2 import ConsumerWireDispatcherV2
from .gateway_v2 import ConsumerGatewayV2
from .mock_lifecycle import MOCK_PRODUCER_MODEL, MockOrderLifecycle, mock_sequence
from .oms_operations_v2 import ConsumerOmsAdmissionContextV2, OrderBookConsumerOperationsV2
from .projection_reads_v2 import PROJECTION_READ_OPERATIONS
from .qualification_bootstrap import initialize_new_qualification_runtime
from .reactor_budget import FixedReactorBudgets
from .replay_v2 import ConsumerCursorReplayServiceV2
from .request_trace_v2 import BoundedRequestTraceWriterV2, mark_request_failure
from .runtime_health import (
    RuntimeAdmissionGuard,
    RuntimeAdmissionUnavailable,
    RuntimeGcObserver,
    RuntimeResourceSample,
    RuntimeResourceSampler,
    sample_cgroup_resources,
)
from .runtime_profile import ConsumerRuntimeProfile
from .service_v2 import ConsumerUnixServiceV2
from .snapshot_index_v2 import ConsumerSnapshotIndexV2
from .uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    consumer_no_adapter_account_view,
    consumer_registry_for_binding,
    load_consumer_request_schema,
    load_consumer_uds_server_config_v2,
    require_safe_consumer_runtime_root,
)
from .unix_v2 import ConsumerUnixPeerV2, ConsumerUnixSocketError, ConsumerUnixSocketListenerV2
from .wire_v2 import load_contract_identity


def candidate_mock_batch_size(raw: str | None) -> int:
    """Constrain an opt-in no-Adapter diagnostic; default behavior stays 128."""
    if raw is None:
        return 128
    if raw not in {"32", "128"}:
        raise ValueError("invalid candidate mock batch diagnostic")
    return int(raw)


def candidate_binding_scope(
    config: ConsumerUdsServerConfigV2,
) -> tuple[ConsumerAccountBinding, tuple[ConsumerStrategyBinding, ...], str]:
    """Pure immutable configuration: safe in a launcher without an OMS/writer."""
    account = ConsumerAccountBinding(
        "consumer-uds-account-v1",
        1,
        config.consumer_id,
        config.consumer_account_id,
        config.account_id,
        config.account_display_name,
        config.venue_id,
        config.adapter_instance_id,
    )
    strategies = tuple(
        ConsumerStrategyBinding(
            f"consumer-uds-strategy-{name}",
            1,
            config.consumer_id,
            config.consumer_account_id,
            account.binding_id,
            f"{config.strategy_id}-{name}",
        )
        for name in ("L", "H", *(f"F{n:02d}" for n in range(20)))
    )
    scope = sha256(
        json.dumps(
            {"account": asdict(account), "strategies": [asdict(s) for s in strategies]},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return account, strategies, scope


def process_owner_trace_budgets(
    config: ConsumerUdsServerConfigV2,
    *,
    projection_commit_diagnostics: bool,
) -> tuple[int, int, int, int, int]:
    """Source, stage, owner-IPC, private, public trace budgets.

    The complete-process owner keeps an empty private listener trace for its
    lifecycle contract, while the public frontend receives the remaining
    request-trace budget.  The five caps always sum to the frozen policy cap.
    """
    policy = config.long_running_policy
    if policy is None:
        raise ConsumerUnixSocketError("process owner trace budgets require LONG_RUNNING")
    source = policy.trace_disk_budget_bytes // 4
    stage = policy.trace_disk_budget_bytes // 8 if projection_commit_diagnostics else 0
    owner_ipc = policy.trace_disk_budget_bytes // 8
    available = policy.trace_disk_budget_bytes - source - stage - owner_ipc
    private = min(policy.trace_segment_bytes, available // 8)
    frontend = available - private
    if private < 1 or frontend < policy.trace_segment_bytes:
        raise ConsumerUnixSocketError("process owner trace budget is insufficient")
    return source, stage, owner_ipc, private, frontend


class LongRunningConsumerCandidate:
    @preserve_candidate_affinity
    def __init__(
        self,
        *,
        config: ConsumerUdsServerConfigV2,
        contract_root: Path,
        initialize: bool,
        sample_resources: Callable[[], RuntimeResourceSample],
        maximum_database_bytes: int = 8 * 1024**3,
        role_cpus: tuple[int, ...] | None = None,
        projection_read_cpus: tuple[int, ...] | None = None,
        projection_transaction_batches: int = 16,
        committed_replay_window: bool = False,
        stage_diagnostics: bool = False,
        fixed_reactor_budgets: bool = False,
        background_cpus: tuple[int, ...] | None = None,
        reuse_projection_read_connections: bool = False,
        optimize_projection_transactions: bool = False,
        projection_commit_diagnostics: bool = False,
        reuse_verified_admission_seeds: bool = False,
        scoped_cold_lookup_epochs: bool = False,
        isolate_submit_execution: bool = False,
        owner_submission_ipc_only: bool = False,
        owner_read_ipc_only: bool = False,
        external_read_plane: bool = False,
        projection_read_service_pause_ns: int = 0,
        projection_read_service_signal: Any | None = None,
    ) -> None:
        if (
            sys.platform != "linux"
            or config.runtime_profile is not ConsumerRuntimeProfile.LONG_RUNNING
            or config.long_running_policy is None
            or config.venue_id != "NO_ADAPTER_BENCHMARK"
            or config.adapter_instance_id != "disabled-no-dispatch"
            or not gc.isenabled()
            or type(projection_transaction_batches) is not int
            or not 1 <= projection_transaction_batches <= 256
            or type(committed_replay_window) is not bool
            or type(stage_diagnostics) is not bool
            or type(projection_commit_diagnostics) is not bool
            or (stage_diagnostics and projection_commit_diagnostics)
            or type(fixed_reactor_budgets) is not bool
            or type(reuse_projection_read_connections) is not bool
            or type(optimize_projection_transactions) is not bool
            or type(reuse_verified_admission_seeds) is not bool
            or type(scoped_cold_lookup_epochs) is not bool
            or type(isolate_submit_execution) is not bool
            or type(owner_submission_ipc_only) is not bool
            or type(owner_read_ipc_only) is not bool
            or type(external_read_plane) is not bool
            or type(projection_read_service_pause_ns) is not int
            or not 0 <= projection_read_service_pause_ns <= 10_000_000
            or (owner_read_ipc_only and not owner_submission_ipc_only)
            or (external_read_plane and (not owner_read_ipc_only or projection_read_cpus is None))
            or (owner_submission_ipc_only and isolate_submit_execution)
            or (reuse_verified_admission_seeds and not optimize_projection_transactions)
            or (reuse_projection_read_connections and projection_read_cpus is None)
        ):
            raise ConsumerUnixSocketError(
                "candidate requires explicit persistent no-Adapter profile"
            )
        require_safe_consumer_runtime_root(config.journal_root)
        validate_role_cpus(role_cpus, projection_read_cpus, background_cpus)
        self.role_cpus = role_cpus
        self.background_cpus = background_cpus
        self.reuse_projection_read_connections = reuse_projection_read_connections
        self.projection_transaction_batches = projection_transaction_batches
        self.optimize_projection_transactions = optimize_projection_transactions
        self.reuse_verified_admission_seeds = reuse_verified_admission_seeds
        self.scoped_cold_lookup_epochs = scoped_cold_lookup_epochs
        self.isolate_submit_execution = isolate_submit_execution
        self.owner_submission_ipc_only = owner_submission_ipc_only
        self.external_read_plane = external_read_plane
        self.ipc_trace: BoundedRequestTraceWriterV2 | None = None
        self.committed_replay_window = committed_replay_window
        self.stage_diagnostics = stage_diagnostics
        self.projection_commit_diagnostics = projection_commit_diagnostics
        self.reactor_budget_policy = FixedReactorBudgets() if fixed_reactor_budgets else None
        self.stage_trace: BoundedRequestTraceWriterV2 | None = None
        if role_cpus is not None:
            set_thread_affinity_exact(
                set(background_cpus) if background_cpus is not None else {role_cpus[0]}
            )
        policy = config.long_running_policy
        self._started = self._closed = False
        self._serve_owner: int | None = None
        self._serve_done = Event()
        self._serve_done.set()
        self._sample = sample_resources
        self.commit_timing: CommitTimingWriter | None = None
        account, strategies, scope_hash = candidate_binding_scope(config)
        self.account, self.strategies, self.scope_sha256 = account, strategies, scope_hash
        try:
            self.runtime = runtime = ColdAwareOmsRuntime(
                root=config.journal_root / "persistent",
                scope_sha256=scope_hash,
                policy=policy,
                initialize=initialize,
                maximum_database_bytes=maximum_database_bytes,
                process_cold_validation=True,
                scoped_cold_lookup_epochs=scoped_cold_lookup_epochs,
                cold_owner_cpu=role_cpus[2] if role_cpus is not None else None,
                segment_owner_cpu=role_cpus[3]
                if role_cpus is not None and len(role_cpus) == 4
                else None,
            )
            trace_root = config.journal_root / "traces"
            trace_root.mkdir(mode=0o700, exist_ok=True)
            require_safe_consumer_runtime_root(trace_root)
            stages = trace_root / "stages"
            if stage_diagnostics or projection_commit_diagnostics:
                stages.mkdir(mode=0o700, exist_ok=True)
                require_safe_consumer_runtime_root(stages)
            self.projector = ProcessOrderProjector(
                journal=runtime.journal,
                root=runtime.projection_root,
                maximum_database_bytes=maximum_database_bytes,
                cpu=role_cpus[1] if role_cpus is not None else None,
                maximum_transaction_batches=projection_transaction_batches,
                stage_trace_path=stages / ("projector-" + uuid.uuid4().hex + ".jsonl")
                if stage_diagnostics or projection_commit_diagnostics
                else None,
                stage_trace_budget=policy.trace_disk_budget_bytes // 16,
                optimize_transactions=optimize_projection_transactions,
                commit_only_diagnostics=projection_commit_diagnostics,
                reuse_verified_admission_seeds=reuse_verified_admission_seeds,
                read_service_pause_ns=projection_read_service_pause_ns,
                read_service_signal=projection_read_service_signal,
            )
            if committed_replay_window:
                runtime.order_book.enable_committed_consumer_replay()
            mock_batch = candidate_mock_batch_size(os.getenv("TRADECORE_DIAGNOSTIC_MOCK_BATCH"))
            self.mock = MockOrderLifecycle(
                book=runtime.order_book, cold=runtime.cold_index, batch_size=mock_batch,
                yield_every={"1": 8, "single": 1}.get(
                    os.getenv("TRADECORE_DIAGNOSTIC_MOCK_YIELD", ""), 0
                ),
                timing_diagnostics=os.getenv("TRADECORE_DIAGNOSTIC_OWNER_CPU") == "1",
                nice_increment=3 if os.getenv("TRADECORE_DIAGNOSTIC_MOCK_NICE") == "1" else 0,
            )
            self.guard = RuntimeAdmissionGuard(
                policy=policy, disk_reserve_bytes=policy.disk_reserve_bytes
            )
            self.sampler = RuntimeResourceSampler(guard=self.guard, sample=sample_resources)
            self.gc_observer = RuntimeGcObserver()
            registry = runtime.identity.registry
            stream = registry.current
            self.projection_reads = (
                ProcessProjectionReads(
                    root=runtime.projection_root,
                    journal_id=runtime.journal.journal_id,
                    profile_hash=runtime.journal.profile_hash,
                    maximum_database_bytes=maximum_database_bytes,
                    cpus=projection_read_cpus,
                    reuse_connections=reuse_projection_read_connections,
                )
                if projection_read_cpus is not None and not external_read_plane
                else None
            )
            reader = ConsumerOrderProjectionReaderV2(
                root=runtime.projection_root,
                journal=runtime.journal,
                stream=stream,
                current_stream=lambda: runtime.identity.registry.current,
                health_for=self.projector.health,
                maximum_database_bytes=maximum_database_bytes,
                process_reads=self.projection_reads,
                committed_replay_book=runtime.order_book if committed_replay_window else None,
            )
            snapshot_reader = VersionedConsumerSnapshotReader(
                root=runtime.projection_root,
                stream=stream,
                profile_hash=runtime.journal.profile_hash,
                health_for=self.projector.health,
                maximum_database_bytes=maximum_database_bytes,
            )
            by_id = {s.binding_id: s for s in strategies}
            attributions = {
                strategy.binding_id: AttributionBinding(
                    f"mock-{strategy.binding_id}",
                    account.binding_id,
                    strategy.binding_id,
                    config.consumer_id,
                    config.consumer_account_id,
                    strategy.strategy_id,
                    1,
                )
                for strategy in strategies
            }

            def admission(
                binding: ConsumerAccountBinding, strategy_id: str, request: ConsumerOrderSignal
            ) -> ConsumerOmsAdmissionContextV2:
                try:
                    self.guard.require_admission()
                except RuntimeAdmissionUnavailable:
                    mark_request_failure("runtime_admission", self.guard.state().value)
                    raise ConsumerAdmissionCapacityV2(
                        "candidate runtime protection is active"
                    ) from None
                try:
                    if (
                        self.mock.health().state != "RUNNING"
                        or self.projector.health().state != "RUNNING"
                        or (self.commit_timing is not None and self.commit_timing.failed)
                        or runtime.journal.commit_timing_failed
                    ):
                        raise ValueError("maintenance failed")
                    mock_sequence(request.strategy_order_id)
                    strategy = by_id[strategy_id]
                    if binding != account:
                        raise ValueError("binding mismatch")
                except Exception:
                    mark_request_failure("candidate_maintenance", "UNAVAILABLE")
                    raise ConsumerAdmissionCapacityV2(
                        "candidate admission is unavailable"
                    ) from None
                now = monotonic_ns()
                risk_id = f"consumer-mock-risk-{request.trading_day.value}"
                risk = RiskSnapshot(
                    risk_id,
                    account.account_id,
                    request.trading_day,
                    "0" * 64,
                    now,
                    now + 60_000_000_000,
                    10**18,
                    10**18,
                )
                decision = DecisionContext(
                    f"mock-decision-{uuid.uuid4().hex}",
                    account.account_id,
                    request.trading_day,
                    risk_id,
                    "0" * 64,
                    request.protection_price_ticks,
                    "mock-fee",
                    "mock-margin",
                    "mock-reservation",
                    now,
                )
                attribution = attributions[strategy.binding_id]
                return ConsumerOmsAdmissionContextV2(
                    strategy,
                    attribution,
                    risk,
                    decision,
                    ReservationRequirement(
                        request.symbol,
                        cash_minor=request.quantity
                        * max(1, request.limit_price_ticks or request.protection_price_ticks),
                    ),
                )

            operations = OrderBookConsumerOperationsV2(
                order_book=runtime.order_book,
                admission_context_for=admission,
                cold_resolver=runtime.cold_index,
                cold_order_reader=reader,
                # Legacy full snapshot isn't a long-running read contract.
                # Clients use CreateOrderSnapshot / ReadOrderSnapshotPage.
            )
            self.snapshots = ConsumerSnapshotIndexV2(
                registry_for=lambda binding: consumer_registry_for_binding(
                    binding, account, registry
                ),
                reader=snapshot_reader,
                cursor_codec=runtime.identity.snapshot_codec(),
                cursor_codec_for=runtime.identity.snapshot_codec,
                maximum_retained_bytes=policy.snapshot_budget_bytes,
                maximum_page_cache_bytes=policy.snapshot_budget_bytes * 7 // 8,
            )
            replay = ConsumerCursorReplayServiceV2(
                key_ring_for=lambda binding: (
                    runtime.identity.event_codec() if binding == account else _wrong_binding()
                ),
                registry_for=lambda binding: consumer_registry_for_binding(
                    binding, account, registry
                ),
                cursor_ttl_ns=3_600_000_000_000,
                event_reader=reader,
                partition_reader=reader,
            )
            bindings = ConsumerBindingSnapshotV2(1, (account,), strategies)
            gateway = ConsumerGatewayV2(
                binding_snapshot=lambda: bindings,
                account_view_for=lambda binding: consumer_no_adapter_account_view(binding, account),
                wire_contract=load_contract_identity(contract_root),
                event_replay=replay,
                operations=operations,
                paged_snapshots=self.snapshots,
                projection_read_delivery_guard=reader.validate_delivery,
            )
            peers = frozenset(
                ((config.peer_uid, config.peer_gid), *config.additional_peer_credentials)
            )
            verified_principal = ConsumerPrincipal(
                config.consumer_id, config.login_name, config.legal_name
            )

            def principal(peer: ConsumerUnixPeerV2) -> ConsumerPrincipal:
                if (peer.uid, peer.gid) not in peers:
                    raise ConsumerUnixSocketError("candidate peer is unauthorized")
                return verified_principal

            self.service = ConsumerUnixServiceV2(
                ConsumerUnixSocketListenerV2(
                    socket_path=config.socket_path,
                    gateway=gateway,
                    principal_for_peer=principal,
                    request_schema=load_consumer_request_schema(contract_root),
                    request_handler=ConsumerWireDispatcherV2(gateway),
                    max_requests_per_connection=config.max_requests_per_connection,
                    listen_backlog=config.listen_backlog,
                    remove_verified_stale_socket=True,
                    parallel_read_operations=PROJECTION_READ_OPERATIONS
                    | frozenset(
                        {
                            "CreateOrderSnapshot",
                            "ReadOrderSnapshotPage",
                        }
                    )
                    | (
                        frozenset({"ListAccounts"})
                        if isolate_submit_execution
                        else frozenset[str]()
                    ),
                    socket_mode=config.socket_mode,
                    inline_cached_snapshot_pages=True,
                    allow_socket_writes=not owner_submission_ipc_only,
                    allow_socket_reads=not owner_read_ipc_only,
                ),
                frame_ready_ingress=True,
                request_reactor=True,
                pending_capacity=config.pending_capacity,
                preframe_capacity=config.preframe_capacity,
                deferred_read_workers=0 if owner_read_ipc_only else 4,
                deferred_read_capacity=32,
                cold_admission_capacity=128,
                submit_execution_capacity=min(128, config.pending_capacity)
                if isolate_submit_execution
                else 0,
            )
            if self.reactor_budget_policy is not None:
                self.service.set_reactor_turn_budgets(self.reactor_budget_policy)
            if self.background_cpus is not None:
                self.service.set_background_thread_cpus(self.background_cpus)
            if owner_submission_ipc_only:
                (
                    source_budget,
                    stage_budget,
                    ipc_budget,
                    private_trace_budget,
                    _frontend_trace_budget,
                ) = process_owner_trace_budgets(
                    config,
                    projection_commit_diagnostics=stage_diagnostics
                    or projection_commit_diagnostics,
                )
            else:
                source_budget = policy.trace_disk_budget_bytes // 4
                stage_budget = (
                    policy.trace_disk_budget_bytes // 8
                    if stage_diagnostics or projection_commit_diagnostics
                    else 0
                )
                ipc_budget = 0
                private_trace_budget = (
                    policy.trace_disk_budget_bytes - source_budget - stage_budget
                )
            self.commit_timing = CommitTimingWriter(
                root=trace_root / ("source-commits-" + uuid.uuid4().hex),
                journal_id=runtime.journal.journal_id,
                first_lsn=runtime.journal.read_bound().committed_lsn + 1,
                maximum_bytes=source_budget,
                producer_model=MOCK_PRODUCER_MODEL,
            )
            runtime.journal.bind_commit_observer(self.commit_timing.observe)
            self.trace = BoundedRequestTraceWriterV2(
                trace_root / (runtime.journal.journal_id + "-" + uuid.uuid4().hex + ".jsonl"),
                journal_id=runtime.journal.journal_id,
                rotation_bytes=policy.trace_segment_bytes,
                max_bytes=private_trace_budget,
            )
            self.service.set_request_trace(self.trace)
            if owner_submission_ipc_only:
                self.ipc_trace = BoundedRequestTraceWriterV2(
                    trace_root / ("owner-ipc-" + uuid.uuid4().hex + ".jsonl"),
                    journal_id=runtime.journal.journal_id,
                    rotation_bytes=policy.trace_segment_bytes,
                    max_bytes=ipc_budget,
                    record_schema="consumer-owner-ipc-trace-v1",
                )
            if stage_diagnostics:
                self.stage_trace = BoundedRequestTraceWriterV2(
                    stages / ("reactor-" + uuid.uuid4().hex + ".jsonl"),
                    journal_id=runtime.journal.journal_id,
                    capacity=4096,
                    rotation_bytes=16 * 1024**2,
                    max_bytes=stage_budget // 2,
                    record_schema="consumer-stage-trace-v1",
                )
                self.service.set_stage_trace(self.stage_trace)
            self.trace.set_status_provider(
                lambda: {
                    "candidate_only": True,
                    "mock_producer_model": MOCK_PRODUCER_MODEL,
                    "full_qualification": False,
                    "role_cpus": self.role_cpus,
                    "background_cpus": self.background_cpus,
                    "stage_diagnostics": self.stage_diagnostics,
                    "projection_commit_diagnostics": self.projection_commit_diagnostics,
                    "reuse_verified_admission_seeds": self.reuse_verified_admission_seeds,
                    "scoped_cold_lookup_epochs": self.scoped_cold_lookup_epochs,
                    "submit_execution_mode": "THREAD_SINGLE_OWNER"
                    if self.isolate_submit_execution
                    else "INHERITED_IPC_SINGLE_OWNER"
                    if self.owner_submission_ipc_only
                    else "INLINE_REACTOR",
                    "reactor_fixed_budgets": asdict(self.reactor_budget_policy)
                    if self.reactor_budget_policy is not None
                    else None,
                    "projector": asdict(self.projector.health()),
                    "projection_transaction_batches": self.projection_transaction_batches,
                    "optimize_projection_transactions": self.optimize_projection_transactions,
                    "projection_reads": self.projection_reads.stats()
                    if self.projection_reads
                    else {"mode": "thread"},
                    "committed_replay_window": self.committed_replay_window,
                    "gil_switch_interval_ns": round(sys.getswitchinterval() * 1_000_000_000),
                    "mock": asdict(self.mock.health()),
                    "mock_batch_timing": self.mock.timing_evidence(),
                    "cold": asdict(runtime.cold_index.stats()),
                    "journal": asdict(runtime.journal.stats()),
                    "source_commit_timing": self.commit_timing.stats()
                    if self.commit_timing
                    else {},
                    "runtime_health": self.guard.evidence(),
                    "gc": {
                        "enabled": gc.isenabled(),
                        "thresholds": gc.get_threshold(),
                        "stable_graph_frozen": self.gc_observer.stable_graph_frozen,
                        "prefreeze_collected": self.gc_observer.prefreeze_collected,
                        "frozen_objects": self.gc_observer.frozen_objects,
                        "frozen_at_ns": self.gc_observer.frozen_at_ns,
                        "count": self.gc_observer.count,
                        "total_pause_ns": self.gc_observer.total_pause_ns,
                        "maximum_pause_ns": self.gc_observer.maximum_pause_ns,
                        "pause_evidence": self.gc_observer.pause_evidence(),
                    },
                    "ingress": asdict(self.service.ingress_stats()),
                }
            )
        except BaseException:
            self.close()
            raise

    def start(self) -> None:
        if self._started or self._closed:
            raise ConsumerUnixSocketError("candidate cannot restart")
        with temporary_thread_affinity(self.background_cpus):
            self.guard.observe(self._sample())
            self.guard.require_admission()
            self.projector.start()
            if self.projection_reads is not None:
                self.projection_reads.start()
            self.gc_observer.start()
            self.mock.start()
            self.sampler.start()
            self.service.start()
            self._started = True

    def serve_forever(self) -> None:
        if not self._started:
            raise ConsumerUnixSocketError("candidate is not started")
        self._serve_owner = get_ident()
        self._serve_done.clear()
        try:
            with temporary_thread_affinity(
                (self.role_cpus[0],)
                if self.background_cpus is not None and self.role_cpus is not None
                else None
            ):
                self.service.serve_forever()
        finally:
            self._serve_owner = None
            self._serve_done.set()

    @property
    def owner_submission_listener(self) -> ConsumerUnixSocketListenerV2:
        if not self.owner_submission_ipc_only:
            raise ConsumerUnixSocketError("inherited submission port is not enabled")
        return self.service.owner_submission_listener

    def close(self) -> None:
        if self._closed:
            return
        if hasattr(self, "service"):
            self.service.request_stop()
            if self._serve_owner is not None and self._serve_owner != get_ident():
                if not self._serve_done.wait(timeout=10):
                    raise ConsumerUnixSocketError("candidate reactor drain did not finish")
            self.service.stop()
            if not self.service.submission_shutdown_verified:
                # An execution owner may still reference the book/Journal.
                # Do not close its files or publish a clean managed exit.
                raise ConsumerUnixSocketError("submission owner shutdown was not verified")
        failures: list[BaseException] = []
        final_projection_failed = False
        for name in (
            "sampler",
            "mock",
            "projection_reads",
            "projector",
            "snapshots",
            "gc_observer",
            "commit_timing",
            "stage_trace",
            "ipc_trace",
            "trace",
        ):
            component = getattr(self, name, None)
            if component is not None:
                if name == "projector" and self._started:
                    try:
                        component.drain(final_lsn=self.runtime.journal.read_bound().committed_lsn)
                    except Exception:
                        # Still close all owned resources. A failed final fence
                        # must not publish lifecycle success or become a retry.
                        final_projection_failed = True
                try:
                    component.close()
                except BaseException as exc:
                    failures.append(exc)
        if failures:
            # In-flight owners may still reference source files. Keep the
            # runtime lease until a later successful drain or process exit.
            raise ConsumerUnixSocketError("candidate component drain failed")
        if self.commit_timing is not None and self.commit_timing.failed:
            raise ConsumerUnixSocketError("candidate source timing evidence failed")
        if self.stage_trace is not None and not self.stage_trace.coverage_verified:
            raise ConsumerUnixSocketError("candidate reactor stage evidence incomplete")
        if self.ipc_trace is not None and not self.ipc_trace.coverage_verified:
            raise ConsumerUnixSocketError("candidate owner IPC evidence incomplete")
        if hasattr(self, "runtime"):
            self.runtime.close()
        if self._started:
            mock = self.mock.health()
            if (
                final_projection_failed
                or not self.runtime.shutdown_verified
                or not self.projector.shutdown_verified
                or (
                    self.projection_reads is not None
                    and not self.projection_reads.shutdown_verified
                )
                or mock.state != "STOPPED"
                or mock.error_code is not None
            ):
                self._closed = True  # Resource cleanup completed, evidence is permanently failed.
                raise ConsumerUnixSocketError("candidate child exit was not verified")
            status = self.trace.path.with_suffix(".status.json")
            timing = self.commit_timing
            if timing is None:
                raise ConsumerUnixSocketError("candidate source timing evidence is missing")
            manifest = timing.manifest_path
            atomic_runtime_json(
                self.trace.path.with_suffix(".managed-exit.json"),
                {
                    "schema": "consumer-candidate-managed-exit-v1",
                    "runtime_closed": True,
                    "all_managed_workers_exited_zero": True,
                    "owned_helper_exit_status": "NOT_VERIFIED",
                    "formal_qualification": False,
                    "producer_model": MOCK_PRODUCER_MODEL,
                    "journal_id": self.runtime.journal.journal_id,
                    "final_lsn": self.runtime.journal.stats().next_lsn - 1,
                    "trace_status_sha256": sha256(status.read_bytes()).hexdigest(),
                    "source_timing_manifest_sha256": sha256(manifest.read_bytes()).hexdigest(),
                    **(
                        {
                            "owner_ipc_trace_name": self.ipc_trace.path.name,
                            "owner_ipc_trace_status_sha256": sha256(
                                self.ipc_trace.path.with_suffix(".status.json").read_bytes()
                            ).hexdigest(),
                        }
                        if self.ipc_trace is not None
                        else {}
                    ),
                    "observed_monotonic_ns": monotonic_ns(),
                },
            )
        self._closed = True

    def publish_owned_exit(self, helper: OwnedCandidateTracker) -> None:
        """Only standalone CLI can supply a proven, already drained helper."""
        if not self._closed or not self._started:
            raise ConsumerUnixSocketError("candidate managed drain is not verified")
        managed = json.loads(self.trace.path.with_suffix(".managed-exit.json").read_text())
        proof = helper.close()
        if managed.get("all_managed_workers_exited_zero") is not True:
            raise ConsumerUnixSocketError("candidate managed certificate unavailable")
        managed.update(
            schema="consumer-candidate-final-lifecycle-v2",
            owned_helper_exit_status="VERIFIED",
            all_owned_descendants_exited_zero=True,
            helper_exit=proof,
            observed_monotonic_ns=monotonic_ns(),
        )
        atomic_runtime_json(self.trace.path.with_suffix(".lifecycle.json"), managed)


def _wrong_binding():
    raise ConsumerUnixSocketError("candidate cursor binding is unavailable")


def configure_candidate_gil_switch(microseconds: int | None) -> None:
    """Explicit CLI experiment, before workers; never a production default.

    CPython's switch interval is interpreter-wide. Do not invoke from an
    embedded candidate or choose it dynamically during a measurement window.
    """
    if microseconds is None:
        return
    if type(microseconds) is not int or microseconds not in {100, 250, 500, 1000, 2000, 5000}:
        raise ValueError("candidate GIL interval is outside the reviewed experiment set")
    sys.setswitchinterval(microseconds / 1_000_000)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--contract-root", type=Path, required=True)
    parser.add_argument("--cgroup-root", type=Path, required=True)
    initialization = parser.add_mutually_exclusive_group()
    initialization.add_argument("--initialize", action="store_true")
    initialization.add_argument(
        "--initialize-if-new",
        action="store_true",
        help="Initialize only an absent/empty private runtime; otherwise verify and recover",
    )
    parser.add_argument(
        "--gil-switch-us",
        type=int,
        choices=(100, 250, 500, 1000, 2000, 5000),
        help="Explicit interpreter-local fixed experiment; default CPython interval unchanged",
    )
    parser.add_argument(
        "--role-cpus",
        help="Existing allowed physical CPUs: reactor,projection,cold[,segment]",
    )
    parser.add_argument(
        "--projection-read-cpus", help="Optional one/two additional fixed physical read CPUs"
    )
    parser.add_argument(
        "--background-cpus",
        help="Optional one/two separate fixed physical CPUs for parent background threads",
    )
    parser.add_argument(
        "--reuse-projection-read-connections",
        action="store_true",
        help="Explicit readonly-owner persistent-session candidate; default reopens per command",
    )
    parser.add_argument(
        "--committed-replay-window",
        action="store_true",
        help="Experimental bounded post-commit event references; uncovered ranges use SQL",
    )
    parser.add_argument(
        "--stage-diagnostics",
        action="store_true",
        help="Opt-in bounded internal stage evidence; requires overhead qualification",
    )
    parser.add_argument(
        "--projection-commit-diagnostics",
        action="store_true",
        help="Opt-in FULL transaction-range timings only; excludes per-operation stage probes",
    )
    parser.add_argument(
        "--fixed-reactor-budgets",
        action="store_true",
        help="Startup-frozen bounded reactor candidate; default legacy policy retained",
    )
    parser.add_argument(
        "--projection-transaction-batches",
        type=int,
        default=16,
        help="Already-arrived batches per FULL projection transaction (1..256; <=512 facts; no wait-to-fill)",
    )
    parser.add_argument(
        "--optimize-projection-transactions",
        action="store_true",
        help="Opt-in transaction-local projection reuse; FULL and all batch proofs retained",
    )
    parser.add_argument(
        "--reuse-verified-admission-seeds",
        action="store_true",
        help="Opt-in <=1024 immutable verified seeds; hash/bytes/scope on every hit; FULL unchanged",
    )
    parser.add_argument(
        "--scoped-cold-lookup-epochs",
        action="store_true",
        help="Opt-in bounded key-stripe lookup invalidation; FULL and retry budget unchanged",
    )
    parser.add_argument(
        "--isolate-submit-execution",
        action="store_true",
        help="Opt-in bounded single WRITE owner thread; shared ready credits; NOT GIL isolation",
    )
    args = parser.parse_args()
    role_cpus = None
    if args.role_cpus:
        if not all(value.isascii() and value.isdecimal() for value in args.role_cpus.split(",")):
            parser.error("role CPU IDs must be comma-separated decimal IDs")
        role_cpus = tuple(int(value) for value in args.role_cpus.split(","))
    require_coordinated_candidate_unit(args.cgroup_root)
    config = load_consumer_uds_server_config_v2(args.config)
    configure_candidate_gil_switch(args.gil_switch_us)
    read_cpus = None
    background_cpus = None
    if args.background_cpus:
        if not all(
            value.isascii() and value.isdecimal() for value in args.background_cpus.split(",")
        ):
            parser.error("background CPU IDs must be explicit decimal IDs")
        background_cpus = tuple(int(value) for value in args.background_cpus.split(","))
    if args.projection_read_cpus:
        if not all(
            value.isascii() and value.isdecimal() for value in args.projection_read_cpus.split(",")
        ):
            parser.error("projection read CPUs must be comma-separated decimal IDs")
        read_cpus = tuple(int(value) for value in args.projection_read_cpus.split(","))
    helper = OwnedCandidateTracker(cgroup_root=args.cgroup_root)
    candidate = None
    normal_stop = False
    try:
        candidate = LongRunningConsumerCandidate(
            config=config,
            contract_root=args.contract_root,
            initialize=(
                initialize_new_qualification_runtime(config.journal_root / "persistent")
                if args.initialize_if_new
                else args.initialize
            ),
            role_cpus=role_cpus,
            projection_read_cpus=read_cpus,
            projection_transaction_batches=args.projection_transaction_batches,
            committed_replay_window=args.committed_replay_window,
            stage_diagnostics=args.stage_diagnostics,
            projection_commit_diagnostics=args.projection_commit_diagnostics,
            fixed_reactor_budgets=args.fixed_reactor_budgets,
            background_cpus=background_cpus,
            reuse_projection_read_connections=args.reuse_projection_read_connections,
            optimize_projection_transactions=args.optimize_projection_transactions,
            reuse_verified_admission_seeds=args.reuse_verified_admission_seeds,
            scoped_cold_lookup_epochs=args.scoped_cold_lookup_epochs,
            isolate_submit_execution=args.isolate_submit_execution,
            sample_resources=lambda: sample_cgroup_resources(
                cgroup_root=args.cgroup_root,
                data_root=config.journal_root,
            ),
        )

        def stop(_number: int, _frame: object) -> None:
            candidate.service.request_stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, stop)
        candidate.start()
        candidate.serve_forever()
        normal_stop = True
        return 0
    finally:
        try:
            if candidate is not None:
                candidate.close()
        finally:
            helper.close()
        if normal_stop and candidate is not None:
            candidate.publish_owned_exit(helper)


if __name__ == "__main__":
    raise SystemExit(main())
