"""Real EMT-simulation burst benchmark through the complete TradeCore path.

By default the driver releases ``300`` or ``499`` already-built signals
concurrently into a bounded TradeCore scheduler, then drains that
queue through one real ``TradeCoreEmtRouteDispatcher`` per verified session
scope.  The mmap Journal distributes its single-writer native and callback
files across stable lanes while preserving exactly one SDK caller for an EMT
API/session.  No network datastore participates in C0--C7.

Only an explicit simulation flag permits side effects.  The emitted report is
payload-free and records nearest-rank P50/P95/P99/P100 for C0--C7.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import re
import secrets
import select
import time
from dataclasses import dataclass, replace
from hashlib import sha256
from math import ceil
from pathlib import Path
from queue import Empty, Full
from threading import (
    Barrier,
    BoundedSemaphore,
    BrokenBarrierError,
    Event,
    Lock,
    Thread,
    get_ident,
)
from typing import Any, Callable

from emt_gateway.fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeObserver,
)
from emt_gateway.fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.session import SessionSettings
from emt_gateway.tradecore_execution import EmtPlatformExecutionSessionFactory
from emt_gateway.tradecore_mmap_journal import (
    EmtMmapNativeCallJournal,
    PreparedEmtNativeRouteResolver,
    TradeCoreEmtRouteDispatcher,
)
from emt_gateway.tradecore_order_request import EmtTradeCoreOrderRequestFactory
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import EmtAdapterProfile
from xuanling_tradecore.domain import CancelDeadlinePolicy, ExecutionDeadlinePolicy, TradingDayEpoch
from xuanling_tradecore.ems import (
    AccountFenceScope,
    AccountSessionShardBinding,
    AccountSessionShardCallbackRouter,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
    CallbackDirectWriteEligibilityV1,
    CallbackPersistenceProfileV1,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    IngressRejection,
    canonical_broker_capabilities_hash,
    select_callback_persistence_profile_v1,
)
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
)
from xuanling_tradecore.persistence.mmap_callback_outcome_journal import MmapCallbackOutcomeJournal
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapPreNativeJournal

from .linux_runtime_telemetry import LinuxRuntimeTelemetry
from .simulated_accounts import load_simulated_accounts
from .tradecore_mmap_emt_support import (
    TradeCoreMmapEmtPerfError,
    _activated_test_manifest,
    _cancel_plan,
    _exit_isolated_native_process,
    _load_order_fixture,
    _Milestones,
    _normalized_trading_day,
    _stop_adapter,
    _submit_plan,
    load_tight_burst_order_plan,
    require_armed_simulated_fixture,
    require_armed_tight_burst_fixture,
)

_LOGICAL_ACCOUNT_ID = "emt_01"
_CLIENT_ID = 94
_MAX_SAFE_TEST_CLIENT_ORDER_REF = 900_000_000
_MIN_SAFE_TEST_CLIENT_ORDER_REF = 100_000_000
_CLEANUP_CANCEL_INTERVAL_SECONDS = 0.005
# The venue admits no more than 500 order-side actions in a rolling second.
# A full 499-order burst must therefore let its last submit leave that window
# before its separate, bounded one-shot cancellation control begins.
_EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS = 1.10
# EMT's simulation-environment limit is 500 simultaneous signals per second.
# Retain a one-signal margin for the full-acceptance stress baseline.
_APPROVED_BURST_WIDTHS = (300, 499)
# A separate mode establishes a warmed, globally serial low-concurrency
# baseline.  It is deliberately not a third burst size: every sample is
# released by the coordinator only after the preceding sample's C7 evidence
# and one bounded cleanup terminal fact have completed.
_HOT_SINGLE_ORDER_MIN_SAMPLES = 100
_HOT_SINGLE_ORDER_MAX_SAMPLES = 499
_HOT_SINGLE_ORDER_ACTION_INTERVAL_SECONDS = 0.010
_DEFAULT_INGRESS_CAPACITY = 1
_DEFAULT_INGRESS_WORKERS = 4
_MAX_INGRESS_WORKERS = 32
_MAX_NATIVE_INFLIGHT_ORDERS = 1_000
_SESSION_PREFLIGHT_SENTINEL_FIXTURE = {
    # This only supports typed route-table construction while a no-order
    # session preflight starts adapters. No producer, dispatcher, query or
    # cancel worker exists on that branch, and this value is never reported.
    "ticker": "000001",
    "market": 1,
    "side": 1,
    "price": 1.0,
    "quantity": 100,
}
_MAX_SESSION_SHARDS = 32
_MAX_MMAP_NATIVE_LANES = 8
_CALLBACK_PIPELINE_DEFAULT_MAX_ITEMS = 8
_CALLBACK_PIPELINE_EXPERIMENTAL_MAX_ITEMS = (4, 8, 12, 16)
_CALLBACK_PIPELINE_BACKLOG = 4
_CALLBACK_WAKEUP_MODES = ("threading-event", "eventfd")
_SUBMISSION_TOPOLOGIES = ("concurrent-sharded", "tight-single-producer")
_OVERLOAD_REJECTION_CODE = "INGRESS_OVERLOADED"
_BURST_STAGE_NAMES = (
    "C0_INGRESS",
    "C0P_BEFORE_TIGHT_SUBMIT",
    "C0A_AFTER_INGRESS_DECISION",
    "C0B_AFTER_NATIVE_DEQUEUE",
    "C0C_BEFORE_NATIVE_DISPATCH",
    "C1_BEFORE_ADAPTER",
    "C1A_BEFORE_NATIVE_JOURNAL",
    "C1B_AFTER_ROUTE_RESOLUTION",
    "C1C_AFTER_LOCAL_CALLBACK_REGISTRATION",
    "C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT",
    "C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT",
    "C1P_AFTER_CONTROLLED_DELEGATE",
    "C1Q_AFTER_ADAPTER_REQUIREMENTS",
    "C1R_AFTER_CLIENT_REFERENCE",
    "C1S_AFTER_MAPPED_REQUEST",
    "C2_AFTER_PRE_NATIVE_MMAP_COMMIT",
    "C3_BEFORE_SDK",
    "C4_AFTER_SDK",
    "C4A_AFTER_ACCEPTED_JOURNAL",
    "C5_CALLBACK_INGRESS",
    "C5D_PYTHON_DRAIN_BEGIN",
    "C5E_AFTER_SEALED_ROUTE",
    "C5A_AFTER_SHARD_ENQUEUE",
    "C5B_TARGET_WORKER_START",
    "C5B2_AFTER_ROUTE_STATE_VALIDATION",
    "C5B3_AFTER_CALLBACK_MMAP_HASH",
    "C5M0_BEFORE_CALLBACK_MMAP_COMMIT",
    "C5M1_AFTER_CALLBACK_MMAP_COMMIT",
    "C6_AFTER_CALLBACK_MMAP_COMMIT",
    "C7_AFTER_DOWNSTREAM",
)
_CALLBACK_MEASUREMENT_STAGES = (
    "C5_CALLBACK_INGRESS",
    "C5D_PYTHON_DRAIN_BEGIN",
    "C5E_AFTER_SEALED_ROUTE",
    "C5A_AFTER_SHARD_ENQUEUE",
    "C5B_TARGET_WORKER_START",
    "C5B2_AFTER_ROUTE_STATE_VALIDATION",
    "C5B3_AFTER_CALLBACK_MMAP_HASH",
    "C5M0_BEFORE_CALLBACK_MMAP_COMMIT",
    "C5M1_AFTER_CALLBACK_MMAP_COMMIT",
    "C6_AFTER_CALLBACK_MMAP_COMMIT",
    "C7_AFTER_DOWNSTREAM",
)
_BURST_STAGE_INDEX = {stage: index for index, stage in enumerate(_BURST_STAGE_NAMES)}


def _mmap_journal_frame_capacity(burst_width: int) -> int:
    """Reserve frames for the burst and its mandatory bounded cleanup.

    A resting test order can produce an initial callback, a cancel command
    outcome and an order-terminal callback.  Allocating only two frames per
    submitted order therefore leaves an exactly-full 499 run with no room for
    normal cleanup evidence.  File capacity is fixed and preallocated before
    C0; this budget changes neither measured frame writes nor the hot path.
    """

    if burst_width < 1:
        raise ValueError("mmap Journal burst width must be positive")
    return max((burst_width * 4) + 128, 1_024)


# These are fixed implementation messages only.  A burst report must not copy
# arbitrary exception text because native failures can contain callback
# payloads or account material.  Keeping this allow-list next to the report
# builder makes a real callback-state incompatibility diagnosable without
# weakening that boundary.
_SAFE_CALLBACK_OUTCOME_CODES = {
    "fastspi v2 native outcome transition id is invalid": "CALLBACK_TRANSITION_ID_INVALID",
    "fastspi v2 BrokerEvent has no proven M1 outcome": "CALLBACK_PROVEN_OUTCOME_MISSING",
    "fastspi v2 M1 outcome identity does not match its BrokerEvent": (
        "CALLBACK_OUTCOME_EVENT_IDENTITY_MISMATCH"
    ),
    "fastspi v2 order outcome lacks venue identity": "CALLBACK_OUTCOME_VENUE_IDENTITY_MISSING",
    "fastspi v2 M1 outcome venue order identity does not match": (
        "CALLBACK_OUTCOME_VENUE_IDENTITY_MISMATCH"
    ),
    "fastspi v2 M1 trade outcome omits the BrokerEvent trade key": (
        "CALLBACK_OUTCOME_TRADE_KEY_MISSING"
    ),
    "fastspi v2 route outcome directory config is invalid": "CALLBACK_ROUTE_DIRECTORY_CONFIG_INVALID",
    "fastspi v2 route registration conflicts": "CALLBACK_ROUTE_REGISTRATION_CONFLICT",
    "fastspi v2 route outcome directory is full": "CALLBACK_ROUTE_DIRECTORY_FULL",
    "fastspi v2 order callback lacks leaves quantity": "CALLBACK_ORDER_LEAVES_MISSING",
    "fastspi v2 order callback exceeds route quantity": "CALLBACK_ORDER_QUANTITY_EXCEEDS_ROUTE",
    "fastspi v2 order callback lacks proven cumulative trade history": (
        "CALLBACK_ORDER_TRADE_HISTORY_PENDING"
    ),
    "fastspi v2 order callback state is not M1 complete": "CALLBACK_ORDER_STATE_INCOMPLETE",
    "fastspi v2 FILLED callback does not conserve quantity": "CALLBACK_FILLED_QUANTITY_MISMATCH",
    "fastspi v2 trade callback lacks prior state, quantity or execution key": (
        "CALLBACK_TRADE_PRIOR_STATE_MISSING"
    ),
    "fastspi v2 trade callback reuses an execution key": "CALLBACK_TRADE_KEY_REUSED",
    "fastspi v2 trade callback exceeds route quantity": "CALLBACK_TRADE_QUANTITY_EXCEEDS_ROUTE",
    "fastspi v2 outcome callback lacks echoed client reference": "CALLBACK_CLIENT_REFERENCE_MISSING",
    "fastspi v2 outcome callback has no registered route": "CALLBACK_ROUTE_MISSING",
    "fastspi v2 callback identity does not match route": "CALLBACK_ROUTE_IDENTITY_MISMATCH",
    "fastspi v2 committed outcome lacks route reference": "CALLBACK_COMMITTED_ROUTE_REFERENCE_MISSING",
    "fastspi v2 committed outcome route disappeared": "CALLBACK_COMMITTED_ROUTE_MISSING",
    "fastspi v2 committed outcome has another route": "CALLBACK_COMMITTED_ROUTE_MISMATCH",
    "fastspi v2 callback replay changed its outcome": "CALLBACK_REPLAY_OUTCOME_CHANGED",
    "fastspi v2 registered route has another account": "CALLBACK_REGISTERED_ROUTE_ACCOUNT_MISMATCH",
    "fastspi v2 registered route has another venue": "CALLBACK_REGISTERED_ROUTE_VENUE_MISMATCH",
    "fastspi v2 registered route identity is invalid": "CALLBACK_REGISTERED_ROUTE_IDENTITY_INVALID",
    "fastspi v2 recovered route has another venue order identity": (
        "CALLBACK_RECOVERED_ROUTE_VENUE_IDENTITY_MISMATCH"
    ),
    "fastspi v2 callback source replay changed identity": "CALLBACK_SOURCE_REPLAY_IDENTITY_CHANGED",
}


class _ForeignCancelOrderDirectory:
    """Read-only-after-barrier external-order lookup for cleanup callbacks.

    Each process-isolated child publishes only its accepted external order IDs
    after C7. All children meet a cleanup barrier before the first cancel
    request. The native bridge can then prove that an otherwise-unroutable
    cancellation-error callback belongs to another scope without putting IPC
    into C0--C7.
    """

    def __init__(
        self,
        *,
        shared_order_ids: Any,
        own_slot_start: int,
        own_slot_count: int,
        shared_ready: Any | None = None,
    ) -> None:
        if own_slot_start < 0 or own_slot_count <= 0:
            raise ValueError("foreign cancel directory slot range is invalid")
        if own_slot_start + own_slot_count > len(shared_order_ids):
            raise ValueError("foreign cancel directory slot range exceeds its shared storage")
        self._shared_order_ids = shared_order_ids
        self._own_slot_start = own_slot_start
        self._own_slot_stop = own_slot_start + own_slot_count
        self._ready = False
        self._shared_ready = shared_ready

    def publish_owned_order_ids(self, order_ids: tuple[int, ...]) -> None:
        if len(order_ids) != self._own_slot_stop - self._own_slot_start:
            raise TradeCoreMmapEmtPerfError(
                "foreign cancel directory owned order count does not match its scope"
            )
        if any(type(order_id) is not int or order_id <= 0 for order_id in order_ids):
            raise TradeCoreMmapEmtPerfError("foreign cancel directory order ID is invalid")
        for offset, order_id in enumerate(order_ids, start=self._own_slot_start):
            self._publish_at(offset, order_id)

    def publish_owned_order_id(self, *, local_index: int, order_id: int) -> None:
        """Publish one sequential hot-baseline order in this scope's fixed range."""

        if not 0 <= local_index < self._own_slot_stop - self._own_slot_start:
            raise TradeCoreMmapEmtPerfError("foreign cancel directory local index is invalid")
        if type(order_id) is not int or order_id <= 0:
            raise TradeCoreMmapEmtPerfError("foreign cancel directory order ID is invalid")
        self._publish_at(self._own_slot_start + local_index, order_id)

    def _publish_at(self, offset: int, order_id: int) -> None:
        if self._shared_order_ids[offset] != 0:
            raise TradeCoreMmapEmtPerfError("foreign cancel directory slot is already published")
        self._shared_order_ids[offset] = order_id

    def mark_ready(self) -> None:
        """Enable foreign lookups only after every scope has published its IDs."""

        if self._shared_ready is not None:
            self._shared_ready.value = 1
        self._ready = True

    @property
    def ready(self) -> bool:
        if self._shared_ready is not None:
            return self._shared_ready.value == 1
        return self._ready

    def is_known_foreign_order_id(self, order_emt_id: int) -> bool:
        if not self.ready or type(order_emt_id) is not int or order_emt_id <= 0:
            return False
        for index, candidate in enumerate(self._shared_order_ids):
            if self._own_slot_start <= index < self._own_slot_stop:
                continue
            if candidate == order_emt_id:
                return True
        return False


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    accounts_file: Path
    fixture: Path
    output: Path
    runtime_dir: Path
    trading_day: str
    burst_width: int
    callback_timeout_seconds: float
    native_start_budget_seconds: float
    execute_simulated: bool
    diagnostic_file: Path | None = None
    ingress_capacity: int = _DEFAULT_INGRESS_CAPACITY
    ingress_workers: int = _DEFAULT_INGRESS_WORKERS
    native_inflight_limit: int = _MAX_NATIVE_INFLIGHT_ORDERS
    isolate_native_process: bool = False
    session_client_ids: tuple[int, ...] = (_CLIENT_ID,)
    same_session_insert_callers: int = 1
    mmap_native_lanes: int = 1
    disable_gc_during_measured_window: bool = False
    callback_pipeline_max_items: int = _CALLBACK_PIPELINE_DEFAULT_MAX_ITEMS
    native_worker_cpu: int | None = None
    ingress_producer_cpu: int | None = None
    callback_ingress_cpu: int | None = None
    callback_journal_cpu: int | None = None
    sdk_receive_cpu: int | None = None
    sdk_process_cpu: int | None = None
    callback_wakeup_mode: str = "threading-event"
    isolate_session_processes: bool = False
    _partitioned_scope_worker: bool = False
    _partition_client_order_ref_base: int | None = None
    # Populated only by the process-isolated parent before any SDK session
    # starts. It names other scope-owned references in the same measured
    # burst, allowing the native bridge to identify a foreign cancel-error
    # fan-out without relaxing handling of unknown callbacks.
    _known_foreign_cancel_client_order_ids: frozenset[int] = frozenset()
    _foreign_cancel_order_directory: _ForeignCancelOrderDirectory | None = None
    _foreign_cancel_cleanup_barrier: Any | None = None
    cleanup_cancel_interval_seconds: float = _CLEANUP_CANCEL_INTERVAL_SECONDS
    # A process-isolated 499 burst needs more than independent SDK sessions:
    # reusing the same affinity quartet for every child simply moves the C0A
    # contention from Python locks to the Linux scheduler.  These optional
    # ordered plans pin each *scope-local* process to a distinct native,
    # ingress-producer, callback-ingress, callback-journal and two EMT SDK
    # receive/process CPU roles.
    native_worker_cpus: tuple[int, ...] | None = None
    ingress_producer_cpus: tuple[int, ...] | None = None
    callback_ingress_cpus: tuple[int, ...] | None = None
    callback_journal_cpus: tuple[int, ...] | None = None
    sdk_receive_cpus: tuple[int, ...] | None = None
    sdk_process_cpus: tuple[int, ...] | None = None
    submission_topology: str = "concurrent-sharded"
    tight_burst_order_plan: Path | None = None
    # A no-order session-capacity preflight uses the same isolated adapter
    # startup path as a burst, but never releases a signal into the scheduler.
    # Its shorter parent-enforced deadline prevents a vendor login stall from
    # becoming an unbounded diagnostic process.
    session_preflight_only: bool = False
    session_preflight_timeout_seconds: float = 15.0
    _session_preflight_scope_count: int | None = None
    hot_single_order_samples: int = 0
    # O1 default: prebuild only pure-Python route mapping and canonical
    # request data after the session has started but before C0.  The dynamic
    # path remains an explicit A/B and rollback control.
    prebuild_submit_mapping: bool = True
    # An optional reviewed mapping is carried into every isolated child so
    # the platform loader verifies the exact SDK bytes before session start.
    # F2/F4 acceptance always supplies it; legacy exploratory measurements
    # remain explicit about lacking this release-binding gate.
    expected_library_hashes: dict[str, str] | None = None
    # P0-only diagnostic: count fixed order-side callback method entries
    # before bridge scalar extraction. Disabled for normal latency baselines.
    native_callback_diagnostics: bool = False
    # The direct candidate is a separately labelled, DISARMED-only A/B
    # composition. It bypasses a target SPSC handoff only after a scope-local
    # drain worker has been proven to be the sole callback-Journal writer.
    callback_persistence_profile: CallbackPersistenceProfileV1 = (
        CallbackPersistenceProfileV1.CONTROL
    )

    def validate(self) -> None:
        if not self.execute_simulated:
            raise TradeCoreMmapEmtPerfError("--execute-simulated is required")
        if self.submission_topology not in _SUBMISSION_TOPOLOGIES:
            raise TradeCoreMmapEmtPerfError("submission topology is invalid")
        _normalized_trading_day(self.trading_day)
        hot_single_order = self.hot_single_order_samples > 0
        if self.hot_single_order_samples < 0:
            raise TradeCoreMmapEmtPerfError("hot single-order sample count cannot be negative")
        if (
            hot_single_order
            and not self._partitioned_scope_worker
            and not (
                _HOT_SINGLE_ORDER_MIN_SAMPLES
                <= self.hot_single_order_samples
                <= _HOT_SINGLE_ORDER_MAX_SAMPLES
            )
        ):
            raise TradeCoreMmapEmtPerfError("hot single-order samples must be in 100..499")
        if hot_single_order and self.burst_width != self.hot_single_order_samples:
            raise TradeCoreMmapEmtPerfError("hot single-order sample count must equal burst width")
        if self._partitioned_scope_worker:
            if not 1 <= self.burst_width <= max(_APPROVED_BURST_WIDTHS):
                raise TradeCoreMmapEmtPerfError("partition worker burst width must be in 1..499")
        elif self.session_preflight_only:
            if not 1 <= self.burst_width <= max(_APPROVED_BURST_WIDTHS):
                raise TradeCoreMmapEmtPerfError("session preflight burst width must be in 1..499")
        elif not hot_single_order and self.burst_width not in _APPROVED_BURST_WIDTHS:
            raise TradeCoreMmapEmtPerfError("burst width must be 300 or 499")
        if not 10.0 <= self.callback_timeout_seconds <= 180.0:
            raise TradeCoreMmapEmtPerfError("callback timeout must be in 10..180 seconds")
        if not 60.0 <= self.native_start_budget_seconds <= 300.0:
            raise TradeCoreMmapEmtPerfError(
                "native start budget must be in 60..300 seconds for a burst run"
            )
        if not 1 <= self.ingress_capacity <= self.burst_width:
            raise TradeCoreMmapEmtPerfError("ingress capacity must be in 1..burst width")
        if not 1 <= self.ingress_workers <= _MAX_INGRESS_WORKERS:
            raise TradeCoreMmapEmtPerfError(f"ingress workers must be in 1..{_MAX_INGRESS_WORKERS}")
        if not 1 <= self.native_inflight_limit <= _MAX_NATIVE_INFLIGHT_ORDERS:
            raise TradeCoreMmapEmtPerfError(
                f"native inflight limit must be in 1..{_MAX_NATIVE_INFLIGHT_ORDERS}"
            )
        if not 1 <= len(self.session_client_ids) <= _MAX_SESSION_SHARDS:
            raise TradeCoreMmapEmtPerfError(
                f"session client scope count must be in 1..{_MAX_SESSION_SHARDS}"
            )
        if len(set(self.session_client_ids)) != len(self.session_client_ids):
            raise TradeCoreMmapEmtPerfError("session client scopes must be unique")
        if any(
            isinstance(client_id, bool)
            or not isinstance(client_id, int)
            or not 1 <= client_id <= 255
            for client_id in self.session_client_ids
        ):
            raise TradeCoreMmapEmtPerfError("session client scopes must be EMT uint8 values")
        if self.ingress_capacity < len(self.session_client_ids) and not (
            hot_single_order and self.isolate_session_processes
        ):
            raise TradeCoreMmapEmtPerfError(
                "ingress capacity must provide at least one reservation per session scope"
            )
        if not 1 <= self.same_session_insert_callers <= _MAX_INGRESS_WORKERS:
            raise TradeCoreMmapEmtPerfError(
                f"same-session InsertOrder callers must be in 1..{_MAX_INGRESS_WORKERS}"
            )
        if self.same_session_insert_callers > 1 and len(self.session_client_ids) != 1:
            raise TradeCoreMmapEmtPerfError(
                "same-session concurrent InsertOrder probing requires exactly one EMT session"
            )
        if self.same_session_insert_callers > 1 and self.native_inflight_limit < self.burst_width:
            raise TradeCoreMmapEmtPerfError(
                "same-session concurrent InsertOrder probing requires native inflight capacity for the burst"
            )
        if self.same_session_insert_callers != 1:
            raise TradeCoreMmapEmtPerfError(
                "the mmap Journal profile requires exactly one native caller per EMT session"
            )
        if not 1 <= self.mmap_native_lanes <= _MAX_MMAP_NATIVE_LANES:
            raise TradeCoreMmapEmtPerfError(
                f"mmap lane count must be in 1..{_MAX_MMAP_NATIVE_LANES}"
            )
        if self.mmap_native_lanes > 1 and len(self.session_client_ids) != 1:
            raise TradeCoreMmapEmtPerfError(
                "multiple mmap projection lanes currently require exactly one EMT session"
            )
        if self.mmap_native_lanes > 1 and self.same_session_insert_callers != 1:
            raise TradeCoreMmapEmtPerfError(
                "mmap lanes cannot combine with the direct caller probe"
            )
        if self.isolate_session_processes:
            if self._partitioned_scope_worker:
                raise TradeCoreMmapEmtPerfError(
                    "partition worker cannot recursively isolate session processes"
                )
            if len(self.session_client_ids) < 2 and not hot_single_order:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session mode requires at least two EMT sessions"
                )
            if self.same_session_insert_callers != 1:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session mode requires one caller per EMT session"
                )
            if self.mmap_native_lanes != 1:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session mode requires one mmap lane per session"
                )
        if self.session_preflight_only:
            if not self.isolate_session_processes and not self._partitioned_scope_worker:
                raise TradeCoreMmapEmtPerfError(
                    "session preflight requires process-isolated session mode"
                )
            if not 5.0 <= self.session_preflight_timeout_seconds <= 60.0:
                raise TradeCoreMmapEmtPerfError(
                    "session preflight timeout must be in 5..60 seconds"
                )
            if self._session_preflight_scope_count is not None and not (
                2 <= self._session_preflight_scope_count <= _MAX_SESSION_SHARDS
            ):
                raise TradeCoreMmapEmtPerfError(
                    "session preflight scope count must be in 2..maximum session scopes"
                )
        if hot_single_order:
            if self.session_preflight_only:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order baseline cannot combine with session preflight"
                )
            if not (self.isolate_session_processes or self._partitioned_scope_worker):
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order baseline requires process-isolated session mode"
                )
            if self.ingress_capacity != 1 or self.native_inflight_limit != 1:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order baseline requires one ingress and one native-inflight credit"
                )
            if self.ingress_workers != 1:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order baseline requires one ingress producer"
                )
        if self.submission_topology == "tight-single-producer":
            if self._partitioned_scope_worker or self.isolate_session_processes:
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode cannot use process-isolated session sharding"
                )
            if len(self.session_client_ids) != 1 or self.same_session_insert_callers != 1:
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode requires exactly one serial EMT session"
                )
            if self.mmap_native_lanes != 1:
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode requires one mmap native lane"
                )
            if self.ingress_workers != 1:
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode requires exactly one ingress producer"
                )
            if (
                self.ingress_capacity != self.burst_width
                or self.native_inflight_limit < self.burst_width
            ):
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode requires full-burst ingress and native capacity"
                )
            if self.tight_burst_order_plan is None:
                raise TradeCoreMmapEmtPerfError(
                    "tight single-producer mode requires an externally preflighted order plan"
                )
        if not 0.005 <= self.cleanup_cancel_interval_seconds <= 1.0:
            raise TradeCoreMmapEmtPerfError("cleanup cancel interval must be in .005..1 seconds")
        if self._partition_client_order_ref_base is not None:
            if not (
                _MIN_SAFE_TEST_CLIENT_ORDER_REF
                <= self._partition_client_order_ref_base
                <= _MAX_SAFE_TEST_CLIENT_ORDER_REF - self.burst_width + 1
            ):
                raise TradeCoreMmapEmtPerfError(
                    "partition client reference range is outside the safe test range"
                )
        if any(
            type(value) is not int
            or not _MIN_SAFE_TEST_CLIENT_ORDER_REF <= value <= _MAX_SAFE_TEST_CLIENT_ORDER_REF
            for value in self._known_foreign_cancel_client_order_ids
        ):
            raise TradeCoreMmapEmtPerfError(
                "foreign cancel callback client reference is outside the safe test range"
            )
        if self._partition_client_order_ref_base is not None:
            own_client_order_ids = range(
                self._partition_client_order_ref_base,
                self._partition_client_order_ref_base + self.burst_width,
            )
            if any(
                value in own_client_order_ids
                for value in self._known_foreign_cancel_client_order_ids
            ):
                raise TradeCoreMmapEmtPerfError(
                    "foreign cancel callback plan overlaps this scope's client references"
                )
        if self.callback_pipeline_max_items not in _CALLBACK_PIPELINE_EXPERIMENTAL_MAX_ITEMS:
            raise TradeCoreMmapEmtPerfError(
                "callback pipeline max items must be one of 4, 8, 12 or 16"
            )
        if not isinstance(self.callback_persistence_profile, CallbackPersistenceProfileV1):
            raise TradeCoreMmapEmtPerfError("callback persistence profile is invalid")
        if (
            self.callback_persistence_profile is CallbackPersistenceProfileV1.DIRECT_CANDIDATE
            and not (self.isolate_session_processes or self._partitioned_scope_worker)
        ):
            raise TradeCoreMmapEmtPerfError(
                "callback direct-write candidate requires process-isolated session scopes"
            )
        if self.callback_wakeup_mode not in _CALLBACK_WAKEUP_MODES:
            raise TradeCoreMmapEmtPerfError(
                "callback wakeup mode must be threading-event or eventfd"
            )
        if any(
            cpu is not None and (isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 0)
            for cpu in (
                self.native_worker_cpu,
                self.ingress_producer_cpu,
                self.callback_ingress_cpu,
                self.callback_journal_cpu,
                self.sdk_receive_cpu,
                self.sdk_process_cpu,
            )
        ):
            raise TradeCoreMmapEmtPerfError(
                "worker and EMT SDK CPU affinity values must be non-negative integers"
            )
        if (self.sdk_receive_cpu is None) != (self.sdk_process_cpu is None):
            raise TradeCoreMmapEmtPerfError(
                "EMT SDK receive and process CPUs must be configured together"
            )
        if self.sdk_receive_cpu is not None and self.sdk_receive_cpu == self.sdk_process_cpu:
            raise TradeCoreMmapEmtPerfError("EMT SDK receive and process CPUs must be distinct")
        _validate_process_isolated_cpu_plan(self)


def _validate_cpu_list(name: str, cpus: tuple[int, ...] | None) -> None:
    """Validate one explicit, payload-free Linux CPU affinity list."""

    if cpus is None:
        return
    if not cpus:
        raise TradeCoreMmapEmtPerfError(f"{name} cannot be empty")
    if any(isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 0 for cpu in cpus):
        raise TradeCoreMmapEmtPerfError(f"{name} must contain non-negative integer CPUs")
    if len(set(cpus)) != len(cpus):
        raise TradeCoreMmapEmtPerfError(f"{name} must not repeat a CPU")


def _validate_process_isolated_cpu_plan(settings: Settings) -> None:
    """Fail closed on an incomplete or falsely-isolated multi-process plan."""

    worker_lists = (
        ("native worker CPU plan", settings.native_worker_cpus),
        ("ingress producer CPU plan", settings.ingress_producer_cpus),
        ("callback ingress CPU plan", settings.callback_ingress_cpus),
        ("callback journal CPU plan", settings.callback_journal_cpus),
    )
    sdk_lists = (
        ("EMT SDK receive CPU plan", settings.sdk_receive_cpus),
        ("EMT SDK process CPU plan", settings.sdk_process_cpus),
    )
    named_lists = worker_lists + sdk_lists
    for name, cpus in named_lists:
        _validate_cpu_list(name, cpus)

    worker_configured = tuple(cpus for _name, cpus in worker_lists if cpus is not None)
    sdk_configured = tuple(cpus for _name, cpus in sdk_lists if cpus is not None)
    if not worker_configured and not sdk_configured:
        return
    if not settings.isolate_session_processes:
        raise TradeCoreMmapEmtPerfError(
            "per-scope worker CPU plans require process-isolated session mode"
        )
    if len(worker_configured) != len(worker_lists):
        raise TradeCoreMmapEmtPerfError(
            "process-isolated worker CPU plans require native, producer, ingress and journal lists"
        )
    if sdk_configured and len(sdk_configured) != len(sdk_lists):
        raise TradeCoreMmapEmtPerfError("EMT SDK CPU plans require both receive and process lists")
    if sdk_configured and not worker_configured:
        raise TradeCoreMmapEmtPerfError("EMT SDK CPU plans require the four worker CPU plans")
    configured = worker_configured + sdk_configured
    scope_count = len(settings.session_client_ids)
    if any(len(cpus) != scope_count for cpus in configured):
        raise TradeCoreMmapEmtPerfError(
            "every process-isolated worker CPU plan must contain one CPU per session scope"
        )
    if any(
        cpu is not None
        for cpu in (
            settings.native_worker_cpu,
            settings.ingress_producer_cpu,
            settings.callback_ingress_cpu,
            settings.callback_journal_cpu,
            settings.sdk_receive_cpu,
            settings.sdk_process_cpu,
        )
    ):
        raise TradeCoreMmapEmtPerfError(
            "use either scalar CPU assignments or the per-scope process CPU plan, not both"
        )
    flattened = tuple(cpu for cpus in configured for cpu in cpus)
    if len(set(flattened)) != len(flattened):
        raise TradeCoreMmapEmtPerfError(
            "process-isolated worker CPU plan must use distinct CPUs across every role and scope"
        )


def _scope_cpu_assignments(
    settings: Settings,
) -> tuple[tuple[int | None, int | None, int | None, int | None, int | None, int | None], ...]:
    """Return native, Python callback and EMT SDK CPU affinity per scope."""

    scope_count = len(settings.session_client_ids)
    if settings.native_worker_cpus is None:
        return (
            (
                settings.native_worker_cpu,
                settings.ingress_producer_cpu,
                settings.callback_ingress_cpu,
                settings.callback_journal_cpu,
                settings.sdk_receive_cpu,
                settings.sdk_process_cpu,
            ),
        ) * scope_count
    # ``Settings.validate`` has already verified the all-or-nothing contract.
    assert settings.ingress_producer_cpus is not None
    assert settings.callback_ingress_cpus is not None
    assert settings.callback_journal_cpus is not None
    return tuple(
        (
            settings.native_worker_cpus[index],
            settings.ingress_producer_cpus[index],
            settings.callback_ingress_cpus[index],
            settings.callback_journal_cpus[index],
            settings.sdk_receive_cpus[index] if settings.sdk_receive_cpus is not None else None,
            settings.sdk_process_cpus[index] if settings.sdk_process_cpus is not None else None,
        )
        for index in range(scope_count)
    )


class _BurstTiming:
    """Preallocated, payload-free timing slots for a fixed simultaneous burst.

    The benchmark knows every submit execution ID and client reference before
    C0.  Avoiding per-stage string-key dictionaries and a collector lock keeps
    the probe from manufacturing the C0->C1 tail it is meant to inspect.
    CPython's GIL protects individual slot writes; consumers only snapshot
    slots after dispatch or while waiting for callback progress.
    """

    def __init__(self, sample_count: int) -> None:
        self._rows: list[list[int | None]] = [
            [None] * len(_BURST_STAGE_NAMES) for _ in range(sample_count)
        ]
        self._submit_indices: dict[str, int] = {}
        self._callback_indices: dict[str, int] = {}

    def register(self, *, index: int, execution_id: str, client_order_ref: str) -> None:
        if not 0 <= index < len(self._rows):
            raise ValueError("burst timing sample index is invalid")
        if execution_id in self._submit_indices or client_order_ref in self._callback_indices:
            raise ValueError("burst timing identity is duplicated")
        self._submit_indices[execution_id] = index
        self._callback_indices[client_order_ref] = index

    def start_at(self, key: str, observed_ns: int) -> None:
        self._put_key(key, "C0_INGRESS", observed_ns)

    def on_tight_submit_begin(self, instruction: Any, observed_ns: int) -> None:
        """Record the serial strategy-equivalent call boundary for one item."""

        self._put_submit(instruction.execution_id, "C0P_BEFORE_TIGHT_SUBMIT", observed_ns)

    def on_ingress_decision(self, instruction: Any, observed_ns: int) -> None:
        self._put_submit(instruction.execution_id, "C0A_AFTER_INGRESS_DECISION", observed_ns)

    def on_native_dequeue(self, instruction: Any, observed_ns: int) -> None:
        """Record when the native worker has dequeued the admitted signal."""

        self._put_submit(instruction.execution_id, "C0B_AFTER_NATIVE_DEQUEUE", observed_ns)

    def on_native_dispatch_ready(self, instruction: Any, observed_ns: int) -> None:
        """Record native-lane local preparation immediately before dispatch."""

        self._put_submit(instruction.execution_id, "C0C_BEFORE_NATIVE_DISPATCH", observed_ns)

    def on_submit_dispatch(self, instruction: Any, observed_ns: int) -> None:
        self._put_submit(instruction.execution_id, "C1_BEFORE_ADAPTER", observed_ns)

    def on_cancel_dispatch(self, instruction: Any, observed_ns: int) -> None:
        del instruction, observed_ns

    def on_submit_stage(self, request: Any, stage: str, observed_ns: int) -> None:
        self._put_submit(request.instruction.execution_id, stage, observed_ns)

    def on_adapter_submit_stage(self, instruction: Any, stage: str, observed_ns: int) -> None:
        self._put_submit(instruction.execution_id, stage, observed_ns)

    def on_cancel_stage(self, request: Any, stage: str, observed_ns: int) -> None:
        del request, stage, observed_ns

    def on_adapter_cancel_stage(self, instruction: Any, stage: str, observed_ns: int) -> None:
        del instruction, stage, observed_ns

    def on_callback(self, event: Any, stage: str, observed_ns: int) -> None:
        identity = event.venue_order_identity
        client_order_ref = identity.client_order_ref if identity is not None else None
        self.on_callback_client_order_ref(client_order_ref, stage, observed_ns)

    def on_callback_client_order_ref(
        self, client_order_ref: str | None, stage: str, observed_ns: int
    ) -> None:
        if client_order_ref is None:
            return
        index = self._callback_indices.get(client_order_ref)
        self._put_index(index, stage, observed_ns)

    def consume(self, key: str) -> dict[str, int]:
        if key.startswith("submit:"):
            index = self._submit_indices.get(key.removeprefix("submit:"))
        elif key.startswith("callback:"):
            index = self._callback_indices.get(key.removeprefix("callback:"))
        else:
            return {}
        if index is None:
            return {}
        return {
            stage: observed_ns
            for stage, observed_ns in zip(_BURST_STAGE_NAMES, self._rows[index])
            if observed_ns is not None
        }

    def _put_key(self, key: str, stage: str, observed_ns: int) -> None:
        if not key.startswith("submit:"):
            raise ValueError("burst timing C0 key is not a submit key")
        self._put_index(self._submit_indices.get(key.removeprefix("submit:")), stage, observed_ns)

    def _put_submit(self, execution_id: str, stage: str, observed_ns: int) -> None:
        self._put_index(self._submit_indices.get(execution_id), stage, observed_ns)

    def _put_index(self, index: int | None, stage: str, observed_ns: int) -> None:
        if index is None or observed_ns < 0:
            return
        stage_index = _BURST_STAGE_INDEX.get(stage)
        if stage_index is not None and self._rows[index][stage_index] is None:
            # One order can legitimately emit working and terminal callbacks.
            # The benchmark measures the first complete callback path for its
            # pre-registered client reference; later callback traffic must not
            # race backwards over an already completed C5--C7 timestamp pair.
            self._rows[index][stage_index] = observed_ns


class _BurstObserver:
    """Payload-free callback completion tracker for the original submit burst."""

    def __init__(self, expected_client_refs: set[str]) -> None:
        self._expected_client_refs = frozenset(expected_client_refs)
        self._seen_client_refs: set[str] = set()
        self._gaps = 0
        self._changed = Event()
        self._lock = Lock()

    def on_broker_event(self, event) -> None:
        identity = event.venue_order_identity
        client_ref = identity.client_order_ref if identity is not None else None
        if client_ref not in self._expected_client_refs:
            return
        with self._lock:
            self._seen_client_refs.add(client_ref)
            self._changed.set()

    def on_callback_gap(self, evidence) -> None:
        del evidence
        with self._lock:
            self._gaps += 1
            self._changed.set()

    def wait_for_change(self, timeout_seconds: float) -> None:
        self._changed.wait(timeout_seconds)
        self._changed.clear()

    def snapshot(self) -> tuple[set[str], int]:
        with self._lock:
            return set(self._seen_client_refs), self._gaps


class _SpscCallbackRing:
    """A preallocated CPython SPSC ring for one callback scope.

    One adapter callback-drain thread is the only producer for a scope, while
    precisely one target Journal worker is its consumer.  The identity latches
    turn an accidental topology change into a fail-closed benchmark failure
    instead of silently applying an SPSC structure to multiple producers.
    CPython's GIL protects index publication; the Event is only a sleep/wake
    aid and never carries data ownership.
    """

    def __init__(self, *, capacity: int, wakeup_mode: str = "threading-event") -> None:
        if capacity < 1:
            raise ValueError("SPSC callback ring capacity is invalid")
        if wakeup_mode not in _CALLBACK_WAKEUP_MODES:
            raise ValueError("SPSC callback ring wakeup mode is invalid")
        self._capacity = capacity
        self._wakeup_mode = wakeup_mode
        self._slots: list[Any | None] = [None] * capacity
        self._read_index = 0
        self._write_index = 0
        self._producer_thread_id: int | None = None
        self._consumer_thread_id: int | None = None
        self._identity_lock = Lock()
        self._available = Event() if wakeup_mode == "threading-event" else None
        self._eventfd: int | None = None
        if wakeup_mode == "eventfd":
            self._eventfd = _new_eventfd()

    def put_nowait(self, item: Any) -> None:
        self._bind_producer()
        if self._write_index - self._read_index >= self._capacity:
            raise Full
        was_empty = self._write_index == self._read_index
        self._slots[self._write_index % self._capacity] = item
        self._write_index += 1
        if was_empty:
            self._signal_available()

    def get(self, *, timeout: float) -> Any:
        if timeout < 0:
            raise ValueError("SPSC callback ring timeout is invalid")
        self._bind_consumer()
        deadline = time.monotonic() + timeout
        while True:
            value = self._poll()
            if value is not _RING_EMPTY:
                return value
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self._wait_for_available(remaining):
                raise Empty

    def get_nowait(self) -> Any:
        self._bind_consumer()
        value = self._poll()
        if value is _RING_EMPTY:
            raise Empty
        return value

    def qsize(self) -> int:
        return self._write_index - self._read_index

    def _poll(self) -> Any:
        if self._read_index == self._write_index:
            return _RING_EMPTY
        index = self._read_index % self._capacity
        value = self._slots[index]
        if value is None:
            raise TradeCoreMmapEmtPerfError("SPSC callback ring publication is invalid")
        self._slots[index] = None
        self._read_index += 1
        if self._read_index == self._write_index:
            available = self._available
            if available is not None:
                available.clear()
                # A producer might have filled a slot just before clear().
                # In that case observe the indices once more before sleeping.
                if self._read_index != self._write_index:
                    available.set()
        return value

    def close(self) -> None:
        """Release an optional eventfd after its producer and consumer stop."""

        eventfd = self._eventfd
        self._eventfd = None
        if eventfd is not None:
            os.close(eventfd)

    def _signal_available(self) -> None:
        available = self._available
        if available is not None:
            available.set()
            return
        eventfd = self._eventfd
        if eventfd is None:
            raise TradeCoreMmapEmtPerfError("SPSC callback eventfd is closed")
        try:
            os.eventfd_write(eventfd, 1)
        except BlockingIOError:
            # The counter only signals non-empty state.  A saturated counter
            # already represents a pending wakeup and cannot lose an item.
            return

    def _wait_for_available(self, timeout: float) -> bool:
        available = self._available
        if available is not None:
            return available.wait(timeout)
        eventfd = self._eventfd
        if eventfd is None:
            return False
        # Clear an old empty->non-empty notification first, then re-check the
        # indices before sleeping.  With one producer and one consumer this
        # closes the clear/recheck/select race without a hot-path mutex.
        try:
            os.eventfd_read(eventfd)
        except BlockingIOError:
            pass
        if self._read_index != self._write_index:
            return True
        readable, _writable, _exceptional = select.select((eventfd,), (), (), timeout)
        return bool(readable)

    def _bind_producer(self) -> None:
        self._bind("producer")

    def _bind_consumer(self) -> None:
        self._bind("consumer")

    def _bind(self, role: str) -> None:
        thread_id = get_ident()
        field = "_producer_thread_id" if role == "producer" else "_consumer_thread_id"
        # The steady state already has the expected single owner.  Keep the
        # fail-closed bind check, but do not acquire a Python mutex for every
        # callback merely to re-check that invariant.
        if getattr(self, field) == thread_id:
            return
        with self._identity_lock:
            owner = getattr(self, field)
            if owner is None:
                setattr(self, field, thread_id)
            elif owner != thread_id:
                raise TradeCoreMmapEmtPerfError(
                    f"SPSC callback ring received a second {role} thread"
                )


def _new_eventfd() -> int:
    """Open a nonblocking Linux eventfd without silently falling back."""

    if platform.system() != "Linux" or not hasattr(os, "eventfd"):
        raise TradeCoreMmapEmtPerfError("callback eventfd wakeup requires Linux os.eventfd")
    return os.eventfd(0, os.EFD_NONBLOCK | os.EFD_CLOEXEC)


_RING_EMPTY = object()


def _pin_current_thread_to_cpu(cpu: int | None) -> None:
    """Pin the caller only when an explicit Linux CPU was approved for a run."""

    if cpu is None:
        return
    if platform.system() != "Linux" or not hasattr(os, "sched_setaffinity"):
        raise TradeCoreMmapEmtPerfError("worker CPU affinity requires Linux sched_setaffinity")
    try:
        os.sched_setaffinity(0, {cpu})
        if os.sched_getaffinity(0) != {cpu}:
            raise OSError("kernel did not retain requested affinity")
    except OSError as error:
        raise TradeCoreMmapEmtPerfError("worker CPU affinity could not be applied") from error


def _partition_worker_cpu_set(settings: Settings) -> tuple[int, ...] | None:
    """Return the inherited CPU set for one forked, SDK-owning scope.

    A process-isolated child is created before it loads EMT or starts any
    Python worker. Constraining its initial thread at that point means the
    SDK callback thread and all benchmark workers inherit the approved scope
    scope. The role-specific pins below still choose the exact core inside
    that set, but cannot accidentally let a vendor-created thread run on an
    unrelated host CPU.
    """

    if not settings._partitioned_scope_worker:
        return None
    worker_roles = (
        settings.native_worker_cpu,
        settings.ingress_producer_cpu,
        settings.callback_ingress_cpu,
        settings.callback_journal_cpu,
    )
    if any(cpu is None for cpu in worker_roles):
        # Session-capacity preflight makes no performance or CPU-isolation
        # claim.  It must therefore be able to establish whether an extra
        # EMT session can start before a host-specific CPU plan has
        # been selected.  A real process-isolated burst remains fail-closed.
        if settings.session_preflight_only:
            return None
        raise TradeCoreMmapEmtPerfError(
            "partition worker requires every role CPU before loading the SDK"
        )
    sdk_roles = (settings.sdk_receive_cpu, settings.sdk_process_cpu)
    if (sdk_roles[0] is None) != (sdk_roles[1] is None):
        raise TradeCoreMmapEmtPerfError(
            "EMT SDK receive and process CPUs must be configured together"
        )
    return tuple(sorted({int(cpu) for cpu in worker_roles + sdk_roles if cpu is not None}))


def _apply_partition_worker_cpu_set(settings: Settings) -> tuple[int, ...] | None:
    """Apply a forked scope's inherited CPU set before SDK construction."""

    cpu_set = _partition_worker_cpu_set(settings)
    if cpu_set is None:
        return None
    if platform.system() != "Linux" or not hasattr(os, "sched_setaffinity"):
        raise TradeCoreMmapEmtPerfError(
            "partition process CPU affinity requires Linux sched_setaffinity"
        )
    requested = set(cpu_set)
    try:
        # At this point the forked scope owns no EMT/native/Python workers.
        # Linux threads subsequently created by this initial thread inherit
        # this mask, including the vendor callback thread created during SDK
        # session startup.
        os.sched_setaffinity(os.getpid(), requested)
        actual = os.sched_getaffinity(os.getpid())
        if actual != requested:
            raise OSError("kernel did not retain requested partition CPU set")
    except OSError as error:
        raise TradeCoreMmapEmtPerfError(
            "partition process CPU affinity could not be applied"
        ) from error
    return tuple(sorted(actual))


class _ShardedCallbackObserver:
    """Route callbacks from native mmap lanes into their ordered target scope."""

    def __init__(
        self,
        *,
        callback_router: AccountSessionShardCallbackRouter,
        callback_lanes_by_client_ref: dict[str, _SpscCallbackRing],
        source_shard_id: str,
        direct_target_observer: Any | None,
        persistence_profile: CallbackPersistenceProfileV1,
        downstream: _BurstObserver,
        timing: _BurstTiming,
    ) -> None:
        self._callback_router = callback_router
        self._callback_lanes_by_client_ref = dict(callback_lanes_by_client_ref)
        self._source_shard_id = source_shard_id
        self._direct_target_observer = direct_target_observer
        self._persistence_profile = persistence_profile
        self._downstream = downstream
        self._timing = timing

    def on_broker_event(self, event: Any) -> None:
        # ``received_monotonic_ns`` is captured by the SDK-facing callback
        # bridge.  The timestamp below is the first Python instruction after
        # adapter draining delivers that event to this observer.
        self._timing.on_callback(
            event,
            "C5_CALLBACK_INGRESS",
            event.received_monotonic_ns,
        )
        self._timing.on_callback(event, "C5D_PYTHON_DRAIN_BEGIN", time.monotonic_ns())
        routed = self._callback_router.route_event(event)
        if routed is None:
            return
        identity = routed.event.venue_order_identity
        client_order_ref = identity.client_order_ref if identity is not None else None
        if client_order_ref is None:
            raise TradeCoreMmapEmtPerfError("callback route has no client reference")
        self._timing.on_callback(routed.event, "C5E_AFTER_SEALED_ROUTE", time.monotonic_ns())
        if self._persistence_profile is CallbackPersistenceProfileV1.DIRECT_CANDIDATE:
            if routed.binding.shard_id != self._source_shard_id:
                raise TradeCoreMmapEmtPerfError(
                    "direct callback writer cannot receive another scope's route"
                )
            target_observer = self._direct_target_observer
            if target_observer is None:
                raise TradeCoreMmapEmtPerfError("direct callback target observer is absent")
            # This is deliberately a scope-local *drain-worker* operation,
            # never an SDK callback-thread write. C5A remains the stable
            # handoff boundary but represents an in-thread direct handoff for
            # this separately labelled candidate instead of SPSC publication.
            self._timing.on_callback(
                routed.event,
                "C5A_AFTER_SHARD_ENQUEUE",
                time.monotonic_ns(),
            )
            self._timing.on_callback(
                routed.event,
                "C5B_TARGET_WORKER_START",
                time.monotonic_ns(),
            )
            target_observer.on_broker_events((routed.event,))
            return
        try:
            lane = self._callback_lanes_by_client_ref[client_order_ref]
        except KeyError as error:
            raise TradeCoreMmapEmtPerfError(
                "callback route target scope is absent from the lane composition"
            ) from error
        try:
            lane.put_nowait(routed)
        except Full:
            # A known callback that cannot enter its sole durable target lane
            # is an integrity gap, never a silent drop or a fallback session.
            self._downstream.on_callback_gap(None)
            return
        self._timing.on_callback(
            routed.event,
            "C5A_AFTER_SHARD_ENQUEUE",
            time.monotonic_ns(),
        )

    def on_callback_gap(self, evidence: Any) -> None:
        self._downstream.on_callback_gap(evidence)


def _drain_sharded_callback_lane(
    *,
    lane: _SpscCallbackRing,
    target_observer: Any,
    timing: _BurstTiming,
    stop: Event,
    pipeline_max_items: int,
) -> None:
    """Persist one target scope's callbacks in source arrival order.

    A batch is used only if an already-present backlog proves there is work to
    amortize. A deliberately small batch bounds one callback-worker turn;
    there is no timer or low-rate callback delay. Every callback frame is
    committed before C7 delivery.
    """

    while not stop.is_set():
        try:
            first = lane.get(timeout=0.25)
        except Empty:
            continue
        batch = [first]
        if lane.qsize() >= _CALLBACK_PIPELINE_BACKLOG:
            while len(batch) < pipeline_max_items:
                try:
                    batch.append(lane.get_nowait())
                except Empty:
                    break
        for routed in batch:
            timing.on_callback(
                routed.event,
                "C5B_TARGET_WORKER_START",
                time.monotonic_ns(),
            )
        target_observer.on_broker_events(tuple(routed.event for routed in batch))


@dataclass(frozen=True)
class _BurstPlan:
    """One prebuilt signal, fixed to one session and one mmap projection lane."""

    request_id: str
    instruction: Any
    route: Any
    shard_id: str
    native_lane_id: str


class _SameSessionConcurrentInflightGate:
    """Bound the explicit, experimental concurrent InsertOrder probe.

    This is intentionally narrower than :class:`ExecutionScheduler`: the
    probe submits already admitted, immutable plans directly to one EMT API to
    test the vendor's documented InsertOrder thread safety.  It still retains
    a credit until a terminal callback provides durable proof, so an
    experimental caller cannot turn an account limit into an unbounded native
    burst.
    """

    def __init__(self, *, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("same-session concurrent inflight capacity is invalid")
        self._capacity = capacity
        self._credits = BoundedSemaphore(capacity)
        self._active_execution_ids: set[str] = set()
        self._lock = Lock()

    def try_acquire(self, execution_id: str) -> bool:
        if not self._credits.acquire(blocking=False):
            return False
        with self._lock:
            if execution_id in self._active_execution_ids:
                self._credits.release()
                raise TradeCoreMmapEmtPerfError(
                    "same-session concurrent inflight execution is duplicated"
                )
            self._active_execution_ids.add(execution_id)
        return True

    def release(self, execution_id: str) -> bool:
        with self._lock:
            if execution_id not in self._active_execution_ids:
                return False
            self._active_execution_ids.remove(execution_id)
        self._credits.release()
        return True

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "capacity": self._capacity,
                "active": len(self._active_execution_ids),
            }


@dataclass
class _SessionShardRuntime:
    """Private composition for one EMT session and its isolated hot-path state."""

    shard_id: str
    client_id: int
    ownership: Any
    resolver: PreparedEmtNativeRouteResolver
    journal: EmtMmapNativeCallJournal
    scheduler: ExecutionScheduler
    native_lane_ids: tuple[str, ...]
    adapter: Any | None = None
    dispatcher: TradeCoreEmtRouteDispatcher | None = None
    native_callback_transport: "_NativeCallbackTransportCounters | None" = None


class _NativeCallbackTransportCounters:
    """Optional payload-free counts at the Python SDK bridge boundary."""

    _CALLBACK_NAMES = ("onOrderEvent", "onTradeEvent", "onCancelOrderError")

    def __init__(self) -> None:
        self._counts = {name: 0 for name in self._CALLBACK_NAMES}
        self._lock = Lock()

    def observe(self, callback_name: str) -> None:
        if callback_name not in self._counts:
            return
        with self._lock:
            self._counts[callback_name] += 1

    def snapshot(self) -> dict[str, int | bool]:
        with self._lock:
            counts = dict(self._counts)
        return {
            "enabled": True,
            "order_event_entries": counts["onOrderEvent"],
            "trade_event_entries": counts["onTradeEvent"],
            "cancel_error_entries": counts["onCancelOrderError"],
        }


@dataclass(frozen=True)
class _PartitionReleaseControl:
    """Fork-safe common C0 release shared by isolated session workers."""

    ready_event: Any
    release_event: Any
    release_ns: Any


@dataclass(frozen=True)
class _HotSingleOrderControl:
    """Fork-safe command/ack boundary for one globally serial sample lane.

    The parent owns sequencing.  A child can submit exactly one already
    prepared order only after receiving a ``release`` command, and it returns
    one payload-free acknowledgement only after that order's C7 and bounded
    cancel-terminal proof.  This makes the four-session baseline genuinely
    low-concurrency at the account boundary rather than merely one order per
    scope at the same time.
    """

    ready_event: Any
    command_queue: Any
    acknowledgement_queue: Any
    scope_name: str


def _run_partition_worker(
    settings: Settings,
    release_control: _PartitionReleaseControl | None,
    hot_single_order_control: _HotSingleOrderControl | None = None,
) -> None:
    """Run one SDK-owning worker without allowing recursive orchestration."""

    failure_phase = "BEFORE_SETTINGS_VALIDATION"

    def mark_startup_phase(phase: str) -> None:
        nonlocal failure_phase
        failure_phase = phase

    try:
        run(
            settings,
            release_control=release_control,
            hot_single_order_control=hot_single_order_control,
            startup_phase_observer=mark_startup_phase,
        )
    except BaseException as error:
        # A child may fail after it has opened the native SDK.  Never let its
        # unbounded traceback (which can contain vendor callback text) escape
        # to the coordinator log.  The parent treats this bounded report as a
        # failed P0 run and preserves the fail-closed outcome.
        failure = _partition_worker_failure_report(
            settings,
            _failure_signature(error),
            failure_phase,
        )
        try:
            settings.output.parent.mkdir(parents=True, exist_ok=True)
            settings.output.write_text(
                json.dumps(failure, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        finally:
            os._exit(2)


def _run_process_isolated_session_burst(settings: Settings) -> dict[str, Any]:
    """Probe one serial EMT API per independent Python process.

    The coordinator intentionally does not load the SDK.  It creates a
    process-local full TradeCore composition for every signed scope, releases
    all scope-local SPSC producers at one shared monotonic C0, then merges the
    payload-free stage rows from persisted child reports. Before child startup
    it seals each child with the exact client-reference ranges owned by the
    other scopes. This permits the bridge to count and suppress a known foreign
    ``onCancelOrderError`` fan-out; an unknown callback still fails closed and
    no callback is reassigned across processes.
    """

    if settings.hot_single_order_samples:
        return _run_process_isolated_hot_single_order_baseline(settings)
    if platform.system() != "Linux":
        raise TradeCoreMmapEmtPerfError(
            "process-isolated EMT session probing currently requires Linux fork"
        )
    from multiprocessing import get_context

    context = get_context("fork")
    scope_ids = tuple(settings.session_client_ids)
    scope_names = tuple(f"scope-{index + 1}" for index in range(len(scope_ids)))
    scope_cpu_assignments = _scope_cpu_assignments(settings)
    scope_counts = _split_limit(settings.burst_width, scope_names)
    base_client_order_ref = _allocate_client_order_ref_range(settings.burst_width)
    all_client_order_ids = frozenset(
        range(base_client_order_ref, base_client_order_ref + settings.burst_width)
    )
    release_ns = context.Value("q", 0)
    release_event = context.Event()
    foreign_cancel_order_ids = context.RawArray("Q", settings.burst_width)
    foreign_cancel_cleanup_barrier = context.Barrier(len(scope_ids))
    # Keep the parent-side ready event with its process.  A child sets it only
    # after its SDK session, local SPSC producer pool and local barrier are all
    # ready to accept the common C0 release.  Do not use report files as a
    # readiness protocol: reports are deliberately written only after C7 and
    # cleanup have completed.
    children: list[tuple[str, int, int, Any, Path, Any]] = []
    next_client_order_ref = base_client_order_ref
    next_foreign_cancel_slot = 0

    def wait_for_ready(
        workers: list[tuple[str, int, int, Any, Path, Any]], *, timeout_seconds: float
    ) -> None:
        """Wait for a bounded worker set without exposing vendor diagnostics."""

        ready_deadline = time.monotonic() + timeout_seconds
        while not all(ready_event.is_set() for *_child, ready_event in workers):
            for scope_name, _client_id, _width, process, _output, _ready_event in workers:
                if not process.is_alive():
                    raise TradeCoreMmapEmtPerfError(
                        f"process-isolated session worker exited before ready:{scope_name}"
                    )
            if time.monotonic() >= ready_deadline:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session workers did not reach their local barriers"
                )
            time.sleep(0.01)

    try:
        for scope_name, client_id, cpu_assignment in zip(
            scope_names, scope_ids, scope_cpu_assignments, strict=True
        ):
            width = scope_counts[scope_name]
            (
                native_worker_cpu,
                ingress_producer_cpu,
                callback_ingress_cpu,
                callback_journal_cpu,
                sdk_receive_cpu,
                sdk_process_cpu,
            ) = cpu_assignment
            output = settings.runtime_dir / f"process-{scope_name}.report.json"
            scope_client_order_ids = frozenset(
                range(next_client_order_ref, next_client_order_ref + width)
            )
            foreign_cancel_directory = _ForeignCancelOrderDirectory(
                shared_order_ids=foreign_cancel_order_ids,
                own_slot_start=next_foreign_cancel_slot,
                own_slot_count=width,
            )
            child_settings = replace(
                settings,
                output=output,
                runtime_dir=settings.runtime_dir / f"process-{scope_name}",
                diagnostic_file=(
                    settings.runtime_dir / f"process-{scope_name}.milestones.json"
                    if settings.diagnostic_file is not None
                    else None
                ),
                burst_width=width,
                ingress_capacity=width,
                native_inflight_limit=width,
                # Persisted workers use the same isolated exit boundary for a
                # no-order preflight and a measured burst.  The vendor SDK's
                # Python-level teardown is not bounded; after the report is
                # durable, process exit contains it without allowing a stale
                # session to delay capacity evidence indefinitely.
                isolate_native_process=True,
                session_client_ids=(client_id,),
                mmap_native_lanes=1,
                isolate_session_processes=False,
                _partitioned_scope_worker=True,
                _partition_client_order_ref_base=next_client_order_ref,
                _known_foreign_cancel_client_order_ids=(
                    all_client_order_ids - scope_client_order_ids
                ),
                _foreign_cancel_order_directory=foreign_cancel_directory,
                _foreign_cancel_cleanup_barrier=foreign_cancel_cleanup_barrier,
                _session_preflight_scope_count=(
                    len(scope_ids) if settings.session_preflight_only else None
                ),
                native_worker_cpu=native_worker_cpu,
                ingress_producer_cpu=ingress_producer_cpu,
                callback_ingress_cpu=callback_ingress_cpu,
                callback_journal_cpu=callback_journal_cpu,
                sdk_receive_cpu=sdk_receive_cpu,
                sdk_process_cpu=sdk_process_cpu,
                native_worker_cpus=None,
                ingress_producer_cpus=None,
                callback_ingress_cpus=None,
                callback_journal_cpus=None,
                sdk_receive_cpus=None,
                sdk_process_cpus=None,
                # Three scope-local cancel loops must remain below one shared
                # EMT 500 action/s window after the post-submit cooldown.
                cleanup_cancel_interval_seconds=max(
                    settings.cleanup_cancel_interval_seconds, 0.008
                ),
            )
            next_client_order_ref += width
            next_foreign_cancel_slot += width
            ready_event = context.Event()
            control = _PartitionReleaseControl(ready_event, release_event, release_ns)
            process = context.Process(
                target=_run_partition_worker,
                args=(child_settings, control),
                name=f"tradecore-emt-{scope_name}",
                daemon=False,
            )
            process.start()
            children.append((scope_name, client_id, width, process, output, ready_event))
            if settings.session_preflight_only:
                # EMT login itself can serialize at the account boundary.  A
                # capacity preflight therefore builds one real session at a
                # time while retaining the earlier sessions, proving the
                # final live-session count without mistaking a login lock for
                # a session-count limit.  The future C0 burst is unchanged.
                wait_for_ready(
                    [children[-1]], timeout_seconds=settings.session_preflight_timeout_seconds
                )

        if not settings.session_preflight_only:
            wait_for_ready(children, timeout_seconds=settings.native_start_budget_seconds)

        release_ns.value = time.monotonic_ns()
        release_event.set()
        completion_deadline = time.monotonic() + (
            max(15.0, settings.session_preflight_timeout_seconds)
            if settings.session_preflight_only
            else settings.native_start_budget_seconds + settings.callback_timeout_seconds + 30.0
        )
        for _scope_name, _client_id, _width, process, _output, _ready_event in children:
            remaining = completion_deadline - time.monotonic()
            process.join(max(0.0, remaining))
        if any(process.is_alive() for _, _, _, process, _output, _ready_event in children):
            raise TradeCoreMmapEmtPerfError(
                "process-isolated session worker exceeded hard deadline"
            )

        child_reports: list[tuple[str, int, int, dict[str, Any]]] = []
        for scope_name, client_id, width, process, output, _ready_event in children:
            if not output.is_file():
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session worker did not persist a report"
                )
            try:
                report = json.loads(output.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session report is unreadable"
                ) from error
            if process.exitcode not in {0, 2}:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session worker exited outside the report contract"
                )
            child_reports.append((scope_name, client_id, width, report))
        startup_failed_children = [
            (scope_name, width, child_report)
            for scope_name, _client_id, width, child_report in child_reports
            # A normal, completed measurement can itself be an acceptance
            # failure and still contain useful C0--C7 evidence.  Only the
            # synthetic child failure envelope has this fixed phase field.
            if isinstance(child_report.get("failure_phase"), str)
        ]
        report = (
            _process_isolated_child_failure_report(settings, startup_failed_children)
            if startup_failed_children
            else (
                _aggregate_process_isolated_preflight_reports(settings, child_reports)
                if settings.session_preflight_only
                else _aggregate_process_isolated_reports(settings, child_reports)
            )
        )
    except BaseException as error:
        report = _process_isolated_failure_report(settings, type(error).__name__)
    finally:
        release_event.set()
        for _scope_name, _client_id, _width, process, _output, _ready_event in children:
            # A worker still blocked inside vendor login remains contained to
            # this child and is then terminated; it never owns an order path.
            if process.is_alive() and settings.session_preflight_only:
                process.join(timeout=2.0)
            if process.is_alive():
                process.terminate()
            process.join(timeout=1.0)

    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def _run_process_isolated_hot_single_order_baseline(settings: Settings) -> dict[str, Any]:
    """Measure one warmed order at a time across one or more live sessions.

    All child processes establish their real SDK sessions before the first
    sample.  The coordinator then issues exactly one release token at a time
    in round-robin scope order.  It waits for the child acknowledgement before
    releasing the next token, so no account-wide submission overlap is hidden
    behind the session sharding that is being measured.
    """

    if platform.system() != "Linux":
        raise TradeCoreMmapEmtPerfError(
            "process-isolated hot single-order baseline currently requires Linux fork"
        )
    from multiprocessing import get_context

    context = get_context("fork")
    scope_ids = tuple(settings.session_client_ids)
    scope_names = tuple(f"scope-{index + 1}" for index in range(len(scope_ids)))
    scope_cpu_assignments = _scope_cpu_assignments(settings)
    scope_counts = _split_limit(settings.hot_single_order_samples, scope_names)
    base_client_order_ref = _allocate_client_order_ref_range(settings.hot_single_order_samples)
    hot_foreign_cancel_order_ids = context.RawArray("Q", settings.hot_single_order_samples)
    # The coordinator releases exactly one sample at a time. It enables this
    # payload-free cross-scope lookup before worker startup; each concrete
    # external ID is still inserted only after that sample's C7 and before its
    # cleanup cancel, so it is outside the measured hot path.
    hot_foreign_cancel_ready = context.RawValue("b", 1)
    children: list[tuple[str, int, int, Any, Path, Any, Any]] = []
    acknowledgement_queue = context.Queue()
    next_client_order_ref = base_client_order_ref
    next_foreign_cancel_slot = 0

    def wait_for_ready(
        workers: list[tuple[str, int, int, Any, Path, Any, Any]], *, timeout_seconds: float
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while not all(ready_event.is_set() for *_prefix, ready_event, _queue in workers):
            for scope_name, _client_id, _width, process, _output, _ready_event, _queue in workers:
                if not process.is_alive():
                    raise TradeCoreMmapEmtPerfError(
                        f"hot single-order worker exited before ready:{scope_name}"
                    )
            if time.monotonic() >= deadline:
                raise TradeCoreMmapEmtPerfError("hot single-order workers did not finish warmup")
            time.sleep(0.01)

    try:
        for scope_name, client_id, cpu_assignment in zip(
            scope_names, scope_ids, scope_cpu_assignments, strict=True
        ):
            width = scope_counts[scope_name]
            (
                native_worker_cpu,
                ingress_producer_cpu,
                callback_ingress_cpu,
                callback_journal_cpu,
                sdk_receive_cpu,
                sdk_process_cpu,
            ) = cpu_assignment
            output = settings.runtime_dir / f"hot-single-{scope_name}.report.json"
            command_queue = context.Queue(maxsize=1)
            foreign_cancel_directory = _ForeignCancelOrderDirectory(
                shared_order_ids=hot_foreign_cancel_order_ids,
                own_slot_start=next_foreign_cancel_slot,
                own_slot_count=width,
                shared_ready=hot_foreign_cancel_ready,
            )
            child_settings = replace(
                settings,
                output=output,
                runtime_dir=settings.runtime_dir / f"hot-single-{scope_name}",
                diagnostic_file=(
                    settings.runtime_dir / f"hot-single-{scope_name}.milestones.json"
                    if settings.diagnostic_file is not None
                    else None
                ),
                burst_width=width,
                hot_single_order_samples=width,
                ingress_capacity=1,
                ingress_workers=1,
                native_inflight_limit=1,
                isolate_native_process=True,
                session_client_ids=(client_id,),
                mmap_native_lanes=1,
                isolate_session_processes=False,
                _partitioned_scope_worker=True,
                _partition_client_order_ref_base=next_client_order_ref,
                _foreign_cancel_order_directory=foreign_cancel_directory,
                native_worker_cpu=native_worker_cpu,
                ingress_producer_cpu=ingress_producer_cpu,
                callback_ingress_cpu=callback_ingress_cpu,
                callback_journal_cpu=callback_journal_cpu,
                sdk_receive_cpu=sdk_receive_cpu,
                sdk_process_cpu=sdk_process_cpu,
                native_worker_cpus=None,
                ingress_producer_cpus=None,
                callback_ingress_cpus=None,
                callback_journal_cpus=None,
                sdk_receive_cpus=None,
                sdk_process_cpus=None,
            )
            next_client_order_ref += width
            next_foreign_cancel_slot += width
            ready_event = context.Event()
            control = _HotSingleOrderControl(
                ready_event=ready_event,
                command_queue=command_queue,
                acknowledgement_queue=acknowledgement_queue,
                scope_name=scope_name,
            )
            process = context.Process(
                target=_run_partition_worker,
                args=(child_settings, None, control),
                name=f"tradecore-emt-hot-single-{scope_name}",
                daemon=False,
            )
            process.start()
            children.append(
                (scope_name, client_id, width, process, output, ready_event, command_queue)
            )
            # Session login may serialize at the account boundary.  Establish
            # each scope in turn while retaining earlier live sessions, then
            # begin timing only after the final scope's callback/native workers
            # report ready.
            wait_for_ready([children[-1]], timeout_seconds=settings.native_start_budget_seconds)

        remaining_by_scope = {scope_name: width for scope_name, _id, width, *_rest in children}
        ordered_children = list(children)
        last_ack_ns = 0
        for sample_index in range(settings.hot_single_order_samples):
            eligible = [child for child in ordered_children if remaining_by_scope[child[0]] > 0]
            if not eligible:
                raise TradeCoreMmapEmtPerfError("hot single-order scope schedule is incomplete")
            child = eligible[sample_index % len(eligible)]
            scope_name, _client_id, _width, process, _output, _ready_event, command_queue = child
            if not process.is_alive():
                raise TradeCoreMmapEmtPerfError(
                    f"hot single-order worker exited before release:{scope_name}"
                )
            if last_ack_ns:
                elapsed_ns = time.monotonic_ns() - last_ack_ns
                minimum_ns = int(_HOT_SINGLE_ORDER_ACTION_INTERVAL_SECONDS * 1_000_000_000)
                if elapsed_ns < minimum_ns:
                    time.sleep((minimum_ns - elapsed_ns) / 1_000_000_000)
            command_queue.put("release", timeout=5.0)
            try:
                acknowledged_scope, acknowledged_ok = acknowledgement_queue.get(
                    timeout=settings.callback_timeout_seconds + 15.0
                )
            except Empty as error:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order child did not acknowledge one bounded sample"
                ) from error
            if acknowledged_scope != scope_name or acknowledged_ok is not True:
                # A child writes its bounded failure report through the normal
                # worker wrapper.  Give that small local write a chance to
                # complete before the coordinator's final containment logic
                # terminates a still-live SDK process.
                process.join(timeout=2.0)
                raise TradeCoreMmapEmtPerfError("hot single-order child reported a failed sample")
            remaining_by_scope[scope_name] -= 1
            last_ack_ns = time.monotonic_ns()

        completion_deadline = time.monotonic() + settings.callback_timeout_seconds + 30.0
        for _scope_name, _client_id, _width, process, _output, _ready_event, _queue in children:
            process.join(max(0.0, completion_deadline - time.monotonic()))
        if any(process.is_alive() for *_prefix, process, _output, _ready_event, _queue in children):
            raise TradeCoreMmapEmtPerfError("hot single-order worker exceeded completion deadline")

        child_reports: list[tuple[str, int, int, dict[str, Any]]] = []
        for scope_name, client_id, width, process, output, _ready_event, _queue in children:
            if not output.is_file():
                raise TradeCoreMmapEmtPerfError("hot single-order worker did not persist a report")
            try:
                child_report = json.loads(output.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order worker report is unreadable"
                ) from error
            if process.exitcode not in {0, 2}:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order worker exited outside the report contract"
                )
            child_reports.append((scope_name, client_id, width, child_report))
        report = _aggregate_process_isolated_reports(settings, child_reports)
        report["driver"] = "tradecore-mmap-emt-hot-single-order-c0-c7-process-isolated"
        report["execution_model"]["logical_simultaneous_signals"] = 1
        report["execution_model"]["producer_start"] = (
            "all session scopes are warmed first; the parent releases one order only after "
            "the preceding order has C7 and cleanup-terminal evidence"
        )
        report["hot_single_order_baseline"] = {
            "enabled": True,
            "samples": settings.hot_single_order_samples,
            "account_wide_submission_overlap": 0,
            "coordinator": "one parent release token, one child acknowledgement per completed sample",
            "inter_sample_action_interval_ms": _HOT_SINGLE_ORDER_ACTION_INTERVAL_SECONDS * 1_000,
            "per_sample_cleanup_post_submit_minimum_ms": (
                _EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS * 1_000
            ),
            "scope_sample_counts": {
                scope_name: width for scope_name, _client_id, width, *_rest in children
            },
        }
        report["safety"]["cleanup"] = (
            "each sample receives one bounded cancel after C7; the next submission is not "
            "released until the terminal callback releases its inflight credit"
        )
    except BaseException as error:
        report = _process_isolated_failure_report(settings, type(error).__name__)
        report["driver"] = "tradecore-mmap-emt-hot-single-order-c0-c7-process-isolated"
    finally:
        for (
            _scope_name,
            _client_id,
            _width,
            process,
            _output,
            _ready_event,
            command_queue,
        ) in children:
            if process.is_alive():
                try:
                    command_queue.put_nowait("stop")
                except BaseException:
                    pass
                process.terminate()
            process.join(timeout=1.0)

    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def _summary_map(samples: list[dict[str, int]]) -> dict[str, Any]:
    """Rebuild all published percentile views from payload-free child rows."""

    segments = (
        ("C0_INGRESS", "C0A_AFTER_INGRESS_DECISION"),
        ("C0P_BEFORE_TIGHT_SUBMIT", "C0A_AFTER_INGRESS_DECISION"),
        ("C0A_AFTER_INGRESS_DECISION", "C0B_AFTER_NATIVE_DEQUEUE"),
        ("C0B_AFTER_NATIVE_DEQUEUE", "C0C_BEFORE_NATIVE_DISPATCH"),
        ("C0C_BEFORE_NATIVE_DISPATCH", "C1_BEFORE_ADAPTER"),
        ("C0A_AFTER_INGRESS_DECISION", "C1_BEFORE_ADAPTER"),
        ("C0_INGRESS", "C1_BEFORE_ADAPTER"),
        ("C1_BEFORE_ADAPTER", "C1B_AFTER_ROUTE_RESOLUTION"),
        ("C1B_AFTER_ROUTE_RESOLUTION", "C1C_AFTER_LOCAL_CALLBACK_REGISTRATION"),
        ("C1C_AFTER_LOCAL_CALLBACK_REGISTRATION", "C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT"),
        ("C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT", "C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT", "C2_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C1_BEFORE_ADAPTER", "C2_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C2_AFTER_PRE_NATIVE_MMAP_COMMIT", "C3_BEFORE_SDK"),
        ("C3_BEFORE_SDK", "C4_AFTER_SDK"),
        ("C4_AFTER_SDK", "C4A_AFTER_ACCEPTED_JOURNAL"),
        ("C4_AFTER_SDK", "C5_CALLBACK_INGRESS"),
        ("C5_CALLBACK_INGRESS", "C5D_PYTHON_DRAIN_BEGIN"),
        ("C5D_PYTHON_DRAIN_BEGIN", "C5E_AFTER_SEALED_ROUTE"),
        ("C5E_AFTER_SEALED_ROUTE", "C5A_AFTER_SHARD_ENQUEUE"),
        ("C5_CALLBACK_INGRESS", "C5A_AFTER_SHARD_ENQUEUE"),
        ("C5A_AFTER_SHARD_ENQUEUE", "C5B_TARGET_WORKER_START"),
        ("C5B_TARGET_WORKER_START", "C5B2_AFTER_ROUTE_STATE_VALIDATION"),
        ("C5B2_AFTER_ROUTE_STATE_VALIDATION", "C5B3_AFTER_CALLBACK_MMAP_HASH"),
        ("C5B3_AFTER_CALLBACK_MMAP_HASH", "C5M0_BEFORE_CALLBACK_MMAP_COMMIT"),
        ("C5M0_BEFORE_CALLBACK_MMAP_COMMIT", "C5M1_AFTER_CALLBACK_MMAP_COMMIT"),
        ("C5M1_AFTER_CALLBACK_MMAP_COMMIT", "C6_AFTER_CALLBACK_MMAP_COMMIT"),
        ("C6_AFTER_CALLBACK_MMAP_COMMIT", "C7_AFTER_DOWNSTREAM"),
    )
    segment_summaries = {
        f"{left}->{right}": _summary(_durations(samples, left, right)) for left, right in segments
    }
    return {
        "segments_ns": segment_summaries,
        "batch_completion": {
            "C0_to_last_C4_after_sdk": _batch_completion_summary(samples, "C4_AFTER_SDK"),
            "C0_to_last_C4A_after_accepted_journal": _batch_completion_summary(
                samples, "C4A_AFTER_ACCEPTED_JOURNAL"
            ),
            "C0_to_last_C7_after_downstream": _batch_completion_summary(
                samples, "C7_AFTER_DOWNSTREAM"
            ),
            "first_C5_to_last_C7_after_downstream": _batch_first_to_last_summary(
                samples,
                start_stage="C5_CALLBACK_INGRESS",
                completion_stage="C7_AFTER_DOWNSTREAM",
            ),
            "first_C0P_to_last_C4_after_sdk": _batch_first_to_last_summary(
                samples,
                start_stage="C0P_BEFORE_TIGHT_SUBMIT",
                completion_stage="C4_AFTER_SDK",
            ),
            "first_C0P_to_last_C7_after_downstream": _batch_first_to_last_summary(
                samples,
                start_stage="C0P_BEFORE_TIGHT_SUBMIT",
                completion_stage="C7_AFTER_DOWNSTREAM",
            ),
        },
        "ingress_decision_C0_to_C0A_all_signals_ns": _summary(
            _durations(samples, "C0_INGRESS", "C0A_AFTER_INGRESS_DECISION")
        ),
        "ingress_decision_C0_to_C0A_admitted_ns": _summary(
            [
                stages["C0A_AFTER_INGRESS_DECISION"] - stages["C0_INGRESS"]
                for stages in samples
                if "C0_INGRESS" in stages
                and "C0A_AFTER_INGRESS_DECISION" in stages
                and "C1_BEFORE_ADAPTER" in stages
                and stages["C0A_AFTER_INGRESS_DECISION"] >= stages["C0_INGRESS"]
            ]
        ),
        "ingress_decision_C0_to_C0A_rejected_before_C1_ns": _summary(
            [
                stages["C0A_AFTER_INGRESS_DECISION"] - stages["C0_INGRESS"]
                for stages in samples
                if "C0_INGRESS" in stages
                and "C0A_AFTER_INGRESS_DECISION" in stages
                and "C1_BEFORE_ADAPTER" not in stages
                and stages["C0A_AFTER_INGRESS_DECISION"] >= stages["C0_INGRESS"]
            ]
        ),
        "tight_submit_C0P_to_C4_ns": _summary(
            _durations(samples, "C0P_BEFORE_TIGHT_SUBMIT", "C4_AFTER_SDK")
        ),
        "tight_submit_C0P_to_C7_ns": _summary(
            _durations(samples, "C0P_BEFORE_TIGHT_SUBMIT", "C7_AFTER_DOWNSTREAM")
        ),
        "project_internal_submit_C0_to_C4_ns": _summary(
            _durations(samples, "C0_INGRESS", "C4_AFTER_SDK")
        ),
        "project_internal_submit_C0_to_C4A_ns": _summary(
            _durations(samples, "C0_INGRESS", "C4A_AFTER_ACCEPTED_JOURNAL")
        ),
        "emt_callback_wait_C4_to_C5_ns": _summary(
            _durations(samples, "C4_AFTER_SDK", "C5_CALLBACK_INGRESS")
        ),
        "project_internal_callback_C5_to_C7_ns": _summary(
            _durations(samples, "C5_CALLBACK_INGRESS", "C7_AFTER_DOWNSTREAM")
        ),
        "end_to_end_C0_to_C7_ns": _summary(
            _durations(samples, "C0_INGRESS", "C7_AFTER_DOWNSTREAM")
        ),
    }


def _batch_completion_summary(
    samples: list[dict[str, int]], completion_stage: str
) -> dict[str, Any]:
    """Measure a whole burst from its shared C0 to its final completion stage.

    This is deliberately a single wall-clock duration, not another percentile.
    It is only published when every planned signal has a C0 and completion
    timestamp and all C0 values are identical.  A partial/overload run must
    never turn its last completed order into a misleading "batch complete"
    result.
    """

    starts = [stages.get("C0_INGRESS") for stages in samples]
    completions = [stages.get(completion_stage) for stages in samples]
    common_start_values = {value for value in starts if value is not None}
    complete = (
        bool(samples)
        and all(value is not None for value in starts)
        and all(value is not None for value in completions)
        and len(common_start_values) == 1
    )
    duration_ns: int | None = None
    if complete:
        start_ns = next(iter(common_start_values))
        assert start_ns is not None
        final_completion_ns = max(value for value in completions if value is not None)
        if final_completion_ns >= start_ns:
            duration_ns = final_completion_ns - start_ns
        else:
            complete = False
    return {
        "signals_target": len(samples),
        "signals_completed": sum(value is not None for value in completions),
        "shared_C0": len(common_start_values) == 1 and bool(common_start_values),
        "complete": complete,
        "duration_ns": duration_ns,
        "measurement": f"shared C0_INGRESS to final {completion_stage}",
    }


def _batch_first_to_last_summary(
    samples: list[dict[str, int]], *, start_stage: str, completion_stage: str
) -> dict[str, Any]:
    """Measure one batch window from its first source event to its last completion.

    Unlike C0, an EMT callback C5 is intentionally asynchronous and has a
    distinct timestamp for each order.  The window therefore starts at the
    earliest source timestamp and is emitted only when every planned sample
    has a monotonic source/completion pair.  This prevents a partial callback
    set from looking like a completed batch drain.
    """

    starts = [stages.get(start_stage) for stages in samples]
    completions = [stages.get(completion_stage) for stages in samples]
    complete = (
        bool(samples)
        and all(value is not None for value in starts)
        and all(value is not None for value in completions)
        and all(
            completion >= start
            for start, completion in zip(starts, completions, strict=True)
            if start is not None and completion is not None
        )
    )
    duration_ns: int | None = None
    if complete:
        first_start_ns = min(value for value in starts if value is not None)
        last_completion_ns = max(value for value in completions if value is not None)
        if last_completion_ns >= first_start_ns:
            duration_ns = last_completion_ns - first_start_ns
        else:
            complete = False
    return {
        "signals_target": len(samples),
        "signals_started": sum(value is not None for value in starts),
        "signals_completed": sum(value is not None for value in completions),
        "complete": complete,
        "duration_ns": duration_ns,
        "measurement": f"earliest {start_stage} to final {completion_stage}",
    }


def _sum_count_maps(reports: list[dict[str, Any]], key: str) -> dict[str, int]:
    totals: dict[str, int] = {}
    for report in reports:
        raw = report.get("outcomes", {}).get(key, {})
        if not isinstance(raw, dict):
            raise TradeCoreMmapEmtPerfError("process-isolated report count map is invalid")
        for name, count in raw.items():
            if not isinstance(name, str) or isinstance(count, bool) or not isinstance(count, int):
                raise TradeCoreMmapEmtPerfError("process-isolated report count entry is invalid")
            totals[name] = totals.get(name, 0) + count
    return totals


def _aggregate_process_isolated_preflight_reports(
    settings: Settings, child_reports: list[tuple[str, int, int, dict[str, Any]]]
) -> dict[str, Any]:
    """Merge bounded no-order adapter-start evidence from isolated scopes.

    This deliberately has no timing samples.  Its only claim is that each
    independently forked worker constructed, started and explicitly stopped
    the same EMT adapter used by a future burst without making a market-facing
    call.  A preflight never substitutes for a throughput or callback test.
    """

    if not child_reports:
        raise TradeCoreMmapEmtPerfError("process-isolated preflight report set is empty")
    per_scope: dict[str, dict[str, Any]] = {}
    for scope_name, _client_id, _width, report in child_reports:
        preflight = report.get("session_preflight")
        outcomes = report.get("outcomes")
        verification = report.get("verification")
        if (
            not isinstance(preflight, dict)
            or not isinstance(outcomes, dict)
            or not isinstance(verification, dict)
            or report.get("driver") != "tradecore-mmap-emt-session-preflight-process-isolated-child"
        ):
            raise TradeCoreMmapEmtPerfError("session preflight child report contract is invalid")
        if outcomes.get("outbound_mutation_attempted") != 0:
            raise TradeCoreMmapEmtPerfError("session preflight reported an outbound mutation")
        per_scope[scope_name] = {
            "complete": bool(report.get("complete")),
            "adapter_started": outcomes.get("adapter_started"),
            "isolated_worker_exit": outcomes.get("isolated_worker_exit"),
            "outbound_mutation_attempted": outcomes.get("outbound_mutation_attempted"),
            "worker_cpu_affinity": report.get("execution_model", {}).get("worker_cpu_affinity", {}),
            "accepted": bool(verification.get("accepted")),
        }
    complete = all(
        scope["complete"]
        and scope["adapter_started"] == 1
        and scope["isolated_worker_exit"] == 1
        and scope["outbound_mutation_attempted"] == 0
        and scope["accepted"]
        for scope in per_scope.values()
    )
    return {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-session-preflight-process-isolated",
        "executed": True,
        "complete": complete,
        "protected_overload_complete": False,
        # Keep the original intended capacity visible, while making it
        # impossible for readers to mistake this no-order check for a 499 run.
        "burst_width": 0,
        "requested_burst_width": settings.burst_width,
        "session_preflight": {
            "mode": "adapter-start-isolated-exit-no-order",
            "session_worker_count": len(child_reports),
            "parent_hard_timeout_seconds": settings.session_preflight_timeout_seconds,
            "per_scope": per_scope,
        },
        "execution_model": {
            "process_isolated_sessions": True,
            "session_worker_count": len(child_reports),
            "native_dispatch": "not entered",
            "worker_cpu_affinity": {
                scope_name: scope["worker_cpu_affinity"] for scope_name, scope in per_scope.items()
            },
        },
        "outcomes": {
            "signals_target": 0,
            "adapter_started": sum(
                int(scope["adapter_started"] or 0) for scope in per_scope.values()
            ),
            "isolated_worker_exit": sum(
                int(scope["isolated_worker_exit"] or 0) for scope in per_scope.values()
            ),
            "outbound_mutation_attempted": 0,
        },
        "safety": {
            "simulated_only": True,
            "no_order_submit": True,
            "no_order_cancel": True,
            "no_trading_query": True,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "verification": {
            "full_acceptance_complete": False,
            "protected_overload_complete": False,
            "accepted": complete,
        },
        "output_name": settings.output.name,
    }


def _aggregate_process_isolated_reports(
    settings: Settings, child_reports: list[tuple[str, int, int, dict[str, Any]]]
) -> dict[str, Any]:
    """Merge independently executed sessions without averaging percentiles."""

    if not child_reports:
        raise TradeCoreMmapEmtPerfError("process-isolated session report set is empty")
    reports = [report for _name, _client_id, _width, report in child_reports]
    samples: list[dict[str, int]] = []
    per_scope: dict[str, dict[str, Any]] = {}
    for scope_name, _client_id, width, report in child_reports:
        rows = report.get("payload_free_timing_samples_ns")
        outcomes = report.get("outcomes")
        verification = report.get("verification")
        if (
            not isinstance(rows, list)
            or len(rows) != width
            or not isinstance(outcomes, dict)
            or not isinstance(verification, dict)
            or report.get("burst_width") != width
        ):
            raise TradeCoreMmapEmtPerfError("process-isolated session report contract is invalid")
        if any(not isinstance(row, dict) for row in rows):
            raise TradeCoreMmapEmtPerfError("process-isolated timing row is invalid")
        scope_callback_stage_coverage = _callback_stage_coverage(rows)
        scope_native_callback_transport = _reported_native_callback_transport(outcomes)
        scope_callback_stage_coverage["origin_classification"] = _callback_origin_classification(
            scope_callback_stage_coverage, scope_native_callback_transport
        )
        samples.extend(rows)
        raw_execution_model = report.get("execution_model", {})
        if not isinstance(raw_execution_model, dict):
            raise TradeCoreMmapEmtPerfError("process-isolated execution model is invalid")
        raw_affinity = raw_execution_model.get("worker_cpu_affinity", {})
        if not isinstance(raw_affinity, dict):
            raise TradeCoreMmapEmtPerfError(
                "process-isolated session worker affinity report is invalid"
            )
        raw_submit_route_preparation = raw_execution_model.get("submit_route_preparation", {})
        if not isinstance(raw_submit_route_preparation, dict):
            raise TradeCoreMmapEmtPerfError(
                "process-isolated submit route preparation report is invalid"
            )
        raw_runtime_attribution = report.get("runtime_attribution", {})
        if not isinstance(raw_runtime_attribution, dict):
            raise TradeCoreMmapEmtPerfError(
                "process-isolated session runtime attribution report is invalid"
            )
        raw_callback_substage_measurement = outcomes.get("callback_substage_measurement", {})
        if not isinstance(raw_callback_substage_measurement, dict):
            raise TradeCoreMmapEmtPerfError(
                "process-isolated session callback substage measurement is invalid"
            )
        per_scope[scope_name] = {
            "signals_target": width,
            "complete": bool(report.get("complete")),
            "native_accepted": outcomes.get("native_accepted"),
            "callback_completed_C7": outcomes.get("callback_completed_C7"),
            "callback_gap_reason_counts": outcomes.get("callback_gap_reason_counts", {}),
            "verified_foreign_cancel_callback_count": outcomes.get(
                "verified_foreign_cancel_callback_count", 0
            ),
            "foreign_cancel_directory_ready": outcomes.get("foreign_cancel_directory_ready", False),
            "callback_substage_measurement": raw_callback_substage_measurement,
            "callback_stage_coverage": scope_callback_stage_coverage,
            "native_callback_transport": scope_native_callback_transport,
            "cleanup_cancel_accepted": outcomes.get("cleanup_cancel_accepted"),
            "cleanup_terminal_callback_complete": outcomes.get(
                "cleanup_terminal_callback_complete"
            ),
            "cleanup_terminal_inflight_remaining": outcomes.get(
                "cleanup_terminal_inflight_remaining"
            ),
            "accepted": bool(verification.get("accepted")),
            "worker_cpu_affinity": raw_affinity,
            "submit_route_preparation": raw_submit_route_preparation,
            # These counters come from overlapping scope-local windows. Keep
            # them separate rather than creating an invented aggregate rate.
            "runtime_attribution": raw_runtime_attribution,
        }
    if len(samples) != settings.burst_width:
        raise TradeCoreMmapEmtPerfError("process-isolated timing sample count is incomplete")

    full_path = _summary_map(samples)
    callback_stage_coverage = _callback_stage_coverage(samples)
    native_callback_transport = _aggregate_native_callback_transport(reports)
    callback_stage_coverage["origin_classification"] = _callback_origin_classification(
        callback_stage_coverage, native_callback_transport
    )
    outcomes = {
        "signals_target": settings.burst_width,
        "producer_error_kinds": _sum_count_maps(reports, "producer_error_kinds"),
        "producer_rejection_kinds": _sum_count_maps(reports, "producer_rejection_kinds"),
        "expired_before_native": sum(
            int(report["outcomes"].get("expired_before_native", 0)) for report in reports
        ),
        "native_accepted": sum(
            int(report["outcomes"].get("native_accepted", 0)) for report in reports
        ),
        "dispatch_error_kinds": _sum_count_maps(reports, "dispatch_error_kinds"),
        "callback_completed_C7": sum(
            int(report["outcomes"].get("callback_completed_C7", 0)) for report in reports
        ),
        "callback_seen_downstream": sum(
            int(report["outcomes"].get("callback_seen_downstream", 0)) for report in reports
        ),
        "callback_complete_C5_to_C7_pairs": full_path["project_internal_callback_C5_to_C7_ns"][
            "count"
        ],
        "callback_substage_measurement": _callback_substage_measurement(samples),
        "callback_stage_coverage": callback_stage_coverage,
        "native_callback_transport": native_callback_transport,
        "callback_gaps": sum(int(report["outcomes"].get("callback_gaps", 0)) for report in reports),
        "callback_gap_reason_counts": _sum_count_maps(reports, "callback_gap_reason_counts"),
        "verified_foreign_cancel_callback_count": sum(
            int(report["outcomes"].get("verified_foreign_cancel_callback_count", 0))
            for report in reports
        ),
        "foreign_cancel_directory_ready": all(
            bool(report["outcomes"].get("foreign_cancel_directory_ready")) for report in reports
        ),
        "callback_error_kinds": _sum_count_maps(reports, "callback_error_kinds"),
        "cleanup_cancel_attempted": sum(
            int(report["outcomes"].get("cleanup_cancel_attempted", 0)) for report in reports
        ),
        "cleanup_cancel_accepted": sum(
            int(report["outcomes"].get("cleanup_cancel_accepted", 0)) for report in reports
        ),
        "cleanup_terminal_callback_complete": all(
            bool(report["outcomes"].get("cleanup_terminal_callback_complete")) for report in reports
        ),
        "cleanup_terminal_inflight_remaining": sum(
            int(report["outcomes"].get("cleanup_terminal_inflight_remaining", 0))
            for report in reports
        ),
        "cleanup_error_kinds": _sum_count_maps(reports, "cleanup_error_kinds"),
        "per_scope": per_scope,
    }
    full_acceptance_complete = (
        all(bool(report.get("complete")) for report in reports)
        and not outcomes["producer_error_kinds"]
        and not outcomes["producer_rejection_kinds"]
        and outcomes["expired_before_native"] == 0
        and outcomes["native_accepted"] == settings.burst_width
        and not outcomes["dispatch_error_kinds"]
        and outcomes["callback_completed_C7"] == settings.burst_width
        and outcomes["callback_seen_downstream"] == settings.burst_width
        and outcomes["callback_complete_C5_to_C7_pairs"] == settings.burst_width
        and outcomes["callback_substage_measurement"]["complete"]
        and outcomes["callback_stage_coverage"]["complete"]
        and outcomes["callback_gaps"] == 0
        and not outcomes["callback_error_kinds"]
        and outcomes["foreign_cancel_directory_ready"]
        and outcomes["cleanup_cancel_attempted"] == settings.burst_width
        and outcomes["cleanup_cancel_accepted"] == settings.burst_width
        and outcomes["cleanup_terminal_callback_complete"]
        and outcomes["cleanup_terminal_inflight_remaining"] == 0
        and not outcomes["cleanup_error_kinds"]
    )
    first = reports[0]
    return {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-burst-c0-c7-process-isolated",
        "executed": True,
        "complete": full_acceptance_complete,
        "protected_overload_complete": False,
        "burst_width": settings.burst_width,
        "percentile_method": "nearest-rank: rank(q)=ceil(q*n), P100=max",
        "persistence_profile": first.get("persistence_profile", {}),
        "runtime_attribution": {
            "aggregation": "not combined across overlapping worker windows; inspect per-scope counters",
            "per_scope": {
                scope_name: scope_report["runtime_attribution"]
                for scope_name, scope_report in per_scope.items()
            },
        },
        "runtime_control": {
            "gc": first.get("runtime_control", {}).get("gc", {}),
            "worker_cpu_affinity": {
                scope_name: scope_report["worker_cpu_affinity"]
                for scope_name, scope_report in per_scope.items()
            },
        },
        "measurement_boundaries": first.get("measurement_boundaries", {}),
        "execution_model": {
            "logical_simultaneous_signals": settings.burst_width,
            "process_isolated_sessions": True,
            "session_worker_count": len(child_reports),
            "producer_start": "each scope-local SPSC producer pool waits at a shared cross-process C0 release",
            "native_dispatch": "one independent Python process, EMT API/session and serial native caller per attested scope",
            "callback_ownership": "callbacks remain in the owning session process; missing local sealed routes fail closed",
            "per_scope": per_scope,
            "submit_route_preparation": {
                "prepared_before_C0": True,
                "sealed_lock_free_lookup_at_C1": True,
                "per_scope": {
                    scope_name: scope_report["submit_route_preparation"]
                    for scope_name, scope_report in per_scope.items()
                },
            },
            "worker_cpu_affinity": {
                "per_scope_plan": {
                    scope_name: scope_report["worker_cpu_affinity"]
                    for scope_name, scope_report in per_scope.items()
                },
                "application": (
                    "each scope applies its process CPU set before EMT SDK load; Python role "
                    "workers and requested EMT SDK threads then use dedicated CPUs within it"
                ),
                "strictly_disjoint": settings.native_worker_cpus is not None,
            },
        },
        "full_path": full_path,
        "payload_free_timing_samples_ns": samples,
        "callback_note": first.get("callback_note", ""),
        "outcomes": outcomes,
        "safety": {
            "simulated_only": True,
            "automatic_retry": False,
            "overload": "all 499 signals are split before C0 and retain a bounded owning session process",
            "cleanup": "each scope serially cancels its own native-accepted orders after the shared action-rate cooldown",
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "verification": {
            "full_acceptance_complete": full_acceptance_complete,
            "protected_overload_complete": False,
            "accepted": full_acceptance_complete,
        },
        "output_name": settings.output.name,
    }


def _process_isolated_failure_report(settings: Settings, failure_kind: str) -> dict[str, Any]:
    """Persist a bounded coordinator failure without leaking child diagnostics."""

    return {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-burst-c0-c7-process-isolated",
        "executed": True,
        "complete": False,
        "protected_overload_complete": False,
        "burst_width": settings.burst_width,
        "failure_kind": failure_kind,
        "full_path": {"segments_ns": {}},
        "outcomes": {"signals_target": settings.burst_width},
        "safety": {
            "simulated_only": True,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "verification": {
            "full_acceptance_complete": False,
            "protected_overload_complete": False,
            "accepted": False,
        },
        "output_name": settings.output.name,
    }


_PARTITION_STARTUP_FAILURE_PHASES = frozenset(
    {
        "BEFORE_SETTINGS_VALIDATION",
        "CPU_AFFINITY_PENDING",
        "CPU_AFFINITY_APPLIED",
        "MILESTONES_STARTED",
    }
)


def _process_isolated_child_failure_report(
    settings: Settings, failed_children: list[tuple[str, int, dict[str, Any]]]
) -> dict[str, Any]:
    """Merge bounded child startup failures without inventing timing evidence.

    A child can fail before the first C0 milestone, for example when a real
    burst was deliberately started without its required disjoint CPU plan.
    Treating that as a malformed timing-report hides the safety gate and can
    lead an operator to misdiagnose it as a missing callback.  This envelope
    carries only fixed phases and the already-sanitised exception signature.
    """

    if not failed_children:
        raise ValueError("process-isolated child failure report requires a failed child")
    per_scope: dict[str, dict[str, Any]] = {}
    for scope_name, width, child_report in failed_children:
        phase = child_report.get("failure_phase")
        per_scope[scope_name] = {
            "signals_target": width,
            "failure_kind": _safe_partition_failure_kind(child_report.get("failure_kind")),
            "failure_phase": (
                phase if phase in _PARTITION_STARTUP_FAILURE_PHASES else "UNCLASSIFIED"
            ),
        }
    phases = {scope["failure_phase"] for scope in per_scope.values()}
    failure_stage = (
        "PRE_C0_CHILD_STARTUP"
        if phases <= {"BEFORE_SETTINGS_VALIDATION", "CPU_AFFINITY_PENDING"}
        else "CHILD_RUNTIME_FAILURE"
    )
    callback_classification = (
        "NOT_STARTED_PRE_C0"
        if failure_stage == "PRE_C0_CHILD_STARTUP"
        else "NOT_MEASURABLE_CHILD_RUNTIME_FAILURE"
    )
    return {
        **_process_isolated_failure_report(settings, "PROCESS_ISOLATED_CHILD_FAILURE"),
        "failure_stage": failure_stage,
        "outcomes": {
            "signals_target": settings.burst_width,
            "callback_stage_coverage": {
                "complete": False,
                "failure_classification": callback_classification,
            },
            "per_scope_failure": per_scope,
        },
    }


def _safe_partition_failure_kind(value: object) -> str:
    """Accept only the bounded signature generated by ``_failure_signature``."""

    if not isinstance(value, str):
        return "CHILD_FAILURE_UNCLASSIFIED"
    if re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*){0,3}"
        r"(?::(?:error_id=\d+|JOURNAL_[A-Z_]+|CALLBACK_[A-Z_]+))?",
        value,
    ):
        return value
    return "CHILD_FAILURE_UNCLASSIFIED"


def _partition_worker_failure_report(
    settings: Settings, failure_kind: str, failure_phase: str
) -> dict[str, Any]:
    """Use the same payload-free contract when an SDK-owning child fails."""

    return {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-burst-c0-c7-process-isolated-child",
        "executed": True,
        "complete": False,
        "protected_overload_complete": False,
        "burst_width": settings.burst_width,
        "failure_kind": _safe_partition_failure_kind(failure_kind),
        "failure_phase": (
            failure_phase if failure_phase in _PARTITION_STARTUP_FAILURE_PHASES else "UNCLASSIFIED"
        ),
        "outcomes": {"signals_target": settings.burst_width},
        "verification": {"accepted": False},
        "output_name": settings.output.name,
    }


def _session_preflight_child_report(
    *,
    settings: Settings,
    runtimes: dict[str, _SessionShardRuntime],
    process_cpu_set: tuple[int, ...] | None,
) -> dict[str, Any]:
    """Create a payload-free proof of one adapter startup and isolated exit.

    Callers invoke this only after the adapter starts and the parent opens the
    common exit release.  The report is persisted before the SDK-owning child
    exits through its normal isolation boundary; it does not claim a graceful
    in-process teardown, order flow or venue acceptance.
    """

    if len(runtimes) != 1:
        raise TradeCoreMmapEmtPerfError("preflight child must own exactly one session")
    scope_name = next(iter(runtimes))
    return {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-session-preflight-process-isolated-child",
        "executed": True,
        "complete": True,
        "protected_overload_complete": False,
        "burst_width": 0,
        "session_preflight": {
            "mode": "adapter-start-isolated-exit-no-order",
            "scope_name": scope_name,
            "parent_hard_timeout_seconds": settings.session_preflight_timeout_seconds,
        },
        "execution_model": {
            "process_isolated_sessions": True,
            "session_worker_count": 1,
            "native_dispatch": "not entered",
            "worker_cpu_affinity": {
                "process_cpu_set": list(process_cpu_set) if process_cpu_set is not None else [],
                "requested_role_cpus": {
                    "native_worker": settings.native_worker_cpu,
                    "ingress_producer": settings.ingress_producer_cpu,
                    "callback_ingress": settings.callback_ingress_cpu,
                    "callback_journal": settings.callback_journal_cpu,
                },
                "requested_emt_sdk_cpus": {
                    "receive": settings.sdk_receive_cpu,
                    "process": settings.sdk_process_cpu,
                },
                # This report is written only after adapter.start returns, so
                # an explicit requested pair proves the post-login SDK call
                # completed without exception.
                "sdk_cpu_affinity_applied": settings.sdk_receive_cpu is not None,
            },
        },
        "outcomes": {
            "signals_target": 0,
            "adapter_started": 1,
            "isolated_worker_exit": 1,
            "outbound_mutation_attempted": 0,
        },
        "safety": {
            "simulated_only": True,
            "no_order_submit": True,
            "no_order_cancel": True,
            "no_trading_query": True,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "verification": {
            "full_acceptance_complete": False,
            "protected_overload_complete": False,
            "accepted": True,
        },
        "output_name": settings.output.name,
    }


def run(
    settings: Settings,
    *,
    release_control: "_PartitionReleaseControl | None" = None,
    hot_single_order_control: "_HotSingleOrderControl | None" = None,
    startup_phase_observer: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    settings.validate()
    if startup_phase_observer is not None:
        startup_phase_observer("CPU_AFFINITY_PENDING")
    process_cpu_set = _apply_partition_worker_cpu_set(settings)
    if startup_phase_observer is not None:
        startup_phase_observer("CPU_AFFINITY_APPLIED")
    if settings.isolate_session_processes:
        return _run_process_isolated_session_burst(settings)
    # A timeout of the process that owns the SDK cannot execute its ``finally``
    # block.  Burst runs therefore persist only their sparse lifecycle markers
    # as they occur.  Timing samples remain in-memory until the measured run is
    # complete, so this adds no I/O to C0--C7.
    milestones = _Milestones(settings.diagnostic_file, flush_on_mark=True)
    milestones.mark("burst_run_started")
    if startup_phase_observer is not None:
        startup_phase_observer("MILESTONES_STARTED")
    accounts = load_simulated_accounts(settings.accounts_file)
    fixture, tight_burst_order_plan_evidence = _fixture_for_run(settings)
    trading_day = TradingDayEpoch(_normalized_trading_day(settings.trading_day))
    native_start_budget_ns = int(settings.native_start_budget_seconds * 1_000_000_000)
    binding = AccountJournalBinding(_LOGICAL_ACCOUNT_ID, "EMT", "xuanling:journal:emt_01:", 0)
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    callback_stop = Event()
    callback_lane_stop = Event()
    callback_errors: list[str] = []
    callback_error_lock = Lock()
    callback_threads: list[Thread] = []
    callback_lanes: dict[str, _SpscCallbackRing] = {}
    runtimes: dict[str, _SessionShardRuntime] = {}
    gc_was_enabled = gc.isenabled()
    gc_suspended = False

    try:
        warmup_started_ns = time.monotonic_ns()
        width = settings.burst_width
        timing = _BurstTiming(width)
        base_client_order_ref = (
            settings._partition_client_order_ref_base
            if settings._partition_client_order_ref_base is not None
            else _allocate_client_order_ref_range(width)
        )
        scope_specs: dict[str, tuple[int, Any, Any, Any, Any]] = {}
        bindings: list[AccountSessionShardBinding] = []
        journal_plan_hash = sha256(b"perf-emt-session-shard-journal-v1").hexdigest()
        for index, client_id in enumerate(settings.session_client_ids):
            shard_id = f"scope-{index + 1}"
            manifest, signed, verifier = _activated_test_manifest(
                logical_account_id=_LOGICAL_ACCOUNT_ID,
                client_id=client_id,
            )
            mapping = EmtAdapterProfile(manifest).activate(
                runtime=RuntimeVenueProfile(
                    fingerprint=manifest.fingerprint,
                    capabilities=manifest.capabilities,
                    account_capabilities=AccountTradingCapabilities(
                        _LOGICAL_ACCOUNT_ID, True, False, False
                    ),
                    scope_ownership=manifest.scope_ownership,
                    route_token_binding=manifest.route_token_binding,
                    trusted_scope_signer_ids=frozenset({signed.ownership.signer_id}),
                    route_token_probe_succeeded=True,
                ),
                activated_at_unix_s=int(time.time()),
            )
            ownership = manifest.scope_ownership
            assert ownership is not None
            bindings.append(
                AccountSessionShardBinding(
                    shard_id=shard_id,
                    account_id=_LOGICAL_ACCOUNT_ID,
                    venue_id="EMT",
                    adapter_instance_id=ownership.adapter_instance_id,
                    adapter_client_scope=str(client_id),
                    ownership_scope_hash=AccountFenceScope(
                        _LOGICAL_ACCOUNT_ID, "EMT", str(client_id)
                    ).scope_hash,
                    mapping_manifest_hash=manifest.manifest_hash,
                    capability_manifest_hash=canonical_broker_capabilities_hash(
                        manifest.capabilities
                    ),
                    journal_plan_hash=journal_plan_hash,
                    journal_routing_epoch=1,
                    journal_shard_id=0,
                )
            )
            scope_specs[shard_id] = (client_id, mapping, signed, verifier, ownership)
        plan = AccountSessionShardPlan(
            plan_id="perf-emt-session-shards-v1",
            version=1,
            plan_hash=sha256(
                (
                    "perf-emt-session-shards-v1:" + ",".join(map(str, settings.session_client_ids))
                ).encode("ascii")
            ).hexdigest(),
            account_id=_LOGICAL_ACCOUNT_ID,
            shards=tuple(bindings),
        )
        plans: list[_BurstPlan] = []
        for index in range(width):
            request_id = f"burst-{index:05d}"
            execution_suffix = f"{secrets.token_hex(16)}-{index}"
            shard_binding = plan.shard_for_execution_id(
                account_id=_LOGICAL_ACCOUNT_ID,
                execution_id=f"exec-{execution_suffix}",
            )
            ownership = scope_specs[shard_binding.shard_id][4]
            instruction, route = _submit_plan(
                cycle=index,
                fixture=fixture,
                binding=binding,
                trading_day=trading_day,
                ownership=ownership,
                client_order_ref=base_client_order_ref + index,
                strategy_order_id=request_id,
                native_start_budget_ns=native_start_budget_ns,
                execution_suffix=execution_suffix,
            )
            plans.append(
                _BurstPlan(
                    request_id,
                    instruction,
                    route,
                    shard_binding.shard_id,
                    _native_lane_id(
                        shard_id=shard_binding.shard_id,
                        execution_id=instruction.execution_id,
                        lane_count=settings.mmap_native_lanes,
                    ),
                )
            )
        planned_counts = {
            shard_id: sum(plan.shard_id == shard_id for plan in plans) for shard_id in scope_specs
        }
        ingress_limits = _allocate_burst_capacity(settings.ingress_capacity, planned_counts)
        native_limits = _allocate_burst_capacity(settings.native_inflight_limit, planned_counts)
        cancel_limits = _allocate_burst_capacity(width, planned_counts)
        schedulers = {
            shard_id: _new_scheduler(
                width=width,
                ingress_capacity=ingress_limits[shard_id],
                native_start_budget_ns=native_start_budget_ns,
                native_inflight_limit=native_limits[shard_id],
                cancel_capacity=cancel_limits[shard_id],
                shard_id=shard_id,
            )
            for shard_id in scope_specs
        }
        shard_router = AccountSessionShardRouter(plan=plan, schedulers=schedulers)
        callback_router = AccountSessionShardCallbackRouter(plan=plan)
        sharded_callback_observers: dict[str, _ShardedCallbackObserver] = {}
        mmap_preparation_by_shard: dict[str, dict[str, int | bool | str]] = {}
        route_preparation_by_shard: dict[str, dict[str, int | bool | str]] = {}
        prepared_submits_by_execution: dict[str, Any] = {}

        def callback_observer_wrapper_for_source(
            source_shard_id: str,
        ) -> Callable[[Any], _ShardedCallbackObserver]:
            def callback_observer_wrapper(_downstream: Any) -> _ShardedCallbackObserver:
                try:
                    return sharded_callback_observers[source_shard_id]
                except KeyError as error:
                    raise TradeCoreMmapEmtPerfError(
                        "session-shard callback observer was not prepared before adapter start"
                    ) from error

            return callback_observer_wrapper

        for index, binding_spec in enumerate(plan.shards):
            shard_id = binding_spec.shard_id
            client_id, mapping, signed, verifier, ownership = scope_specs[shard_id]
            resolver = PreparedEmtNativeRouteResolver(maximum_pending_routes=(width * 2) + 128)
            journal_frame_capacity = _mmap_journal_frame_capacity(width)
            callback_mmap_journals = {
                f"{shard_id}:callback-{lane_index + 1}": MmapPreNativeJournal(
                    path=settings.runtime_dir
                    / f"mmap-callback-{shard_id}-lane-{lane_index + 1}.journal",
                    capacity=journal_frame_capacity,
                )
                for lane_index in range(settings.mmap_native_lanes)
            }
            cancel_command_mmap_journals = {
                f"{shard_id}:cancel-command": MmapPreNativeJournal(
                    path=settings.runtime_dir / f"mmap-cancel-command-{shard_id}.journal",
                    capacity=journal_frame_capacity,
                )
            }
            mmap_callback_outcomes = MmapCallbackOutcomeJournal(
                journals=callback_mmap_journals,
                cancel_command_journals=cancel_command_mmap_journals,
                callback_stage_observer=timing.on_callback_client_order_ref,
            )
            order_directory = FastSpiV2RouteOutcomeDirectory(
                config=FastSpiV2RouteOutcomeDirectoryConfig(
                    _LOGICAL_ACCOUNT_ID,
                    ownership.adapter_instance_id,
                    str(client_id),
                    maximum_routes=width + 128,
                )
            )
            cancel_observer = FastSpiV2CancelOutcomeObserver(
                config=FastSpiV2CancelOutcomeDirectoryConfig(
                    _LOGICAL_ACCOUNT_ID,
                    ownership.adapter_instance_id,
                    str(client_id),
                    maximum_cancel_routes=width + 128,
                ),
                journal=mmap_callback_outcomes,
            )
            native_lane_ids = tuple(
                f"{shard_id}:native-{lane_index + 1}"
                for lane_index in range(settings.mmap_native_lanes)
            )
            mmap_journals = {
                lane_id: MmapPreNativeJournal(
                    path=settings.runtime_dir
                    / f"mmap-pre-native-{shard_id}-lane-{lane_index + 1}.journal",
                    capacity=journal_frame_capacity,
                )
                for lane_index, lane_id in enumerate(native_lane_ids)
            }
            journal = EmtMmapNativeCallJournal(
                route_resolver=resolver,
                order_outcome_directory=order_directory,
                cancel_outcome_observer=cancel_observer,
                gate_stage_observer=timing.on_submit_stage,
                callback_route_stage_observer=timing.on_callback,
                maximum_cancel_callback_mappings=width + 128,
                mmap_pre_native_journals=mmap_journals,
                mmap_callback_outcomes=mmap_callback_outcomes,
            )
            if mmap_journals:
                preparations = [
                    mmap_journal.prepare_for_measurement(
                        account_id=_LOGICAL_ACCOUNT_ID,
                        execution_ids=(
                            item.instruction.execution_id
                            for item in plans
                            if item.native_lane_id == lane_id
                        ),
                        command_ids=(
                            item.route.command.command_id
                            for item in plans
                            if item.native_lane_id == lane_id
                        ),
                        payload_hashes=(
                            item.route.command.payload_hash
                            for item in plans
                            if item.native_lane_id == lane_id
                        ),
                        client_order_refs=(
                            item.route.command.client_order_ref
                            for item in plans
                            if item.native_lane_id == lane_id
                        ),
                    )
                    for lane_id, mmap_journal in mmap_journals.items()
                ]
                if mmap_callback_outcomes is not None:
                    preparations.extend(
                        mmap_callback_outcomes.prepare_for_measurement(
                            account_id=_LOGICAL_ACCOUNT_ID,
                            execution_ids=(
                                item.instruction.execution_id
                                for item in plans
                                if item.shard_id == shard_id
                            ),
                            command_ids=(
                                item.route.command.command_id
                                for item in plans
                                if item.shard_id == shard_id
                            ),
                            client_order_refs=(
                                item.route.command.client_order_ref
                                for item in plans
                                if item.shard_id == shard_id
                            ),
                        )
                    )
                mmap_preparation_by_shard[shard_id] = {
                    "journal_count": len(preparations),
                    "journal_frame_capacity": journal_frame_capacity,
                    "frame_budget": "four frames per submitted order plus 128 cleanup margin",
                    "page_count": sum(preparation.page_count for preparation in preparations),
                    "madvise_willneed_requested": all(
                        preparation.madvise_willneed_requested for preparation in preparations
                    ),
                    "static_account_cached": all(
                        preparation.static_account_cached for preparation in preparations
                    ),
                    "preloaded_fixed_field_count": sum(
                        preparation.preloaded_fixed_field_count for preparation in preparations
                    ),
                }
            native_callback_transport = (
                _NativeCallbackTransportCounters() if settings.native_callback_diagnostics else None
            )
            factory = EmtPlatformExecutionSessionFactory(
                sdk_provider=EmtPlatformLoader(
                    SdkLoadSettings(
                        sdk_root=settings.sdk_root,
                        expected_hashes=settings.expected_library_hashes,
                    )
                ),
                settings=SessionSettings(
                    host=accounts.host,
                    port=accounts.port,
                    account=accounts.ordinary.account,
                    password=accounts.ordinary.password,
                    client_id=client_id,
                    sdk_log_dir=settings.runtime_dir / f"sdk-logs-{shard_id}",
                    account_profile="ordinary",
                    sdk_log_level=0,
                    sdk_receive_cpu=settings.sdk_receive_cpu,
                    sdk_process_cpu=settings.sdk_process_cpu,
                ),
                order_request_factory=EmtTradeCoreOrderRequestFactory(),
                native_call_journal=journal,
                cancel_request_id_for_order=journal.cancel_request_id_for_order,
                cancel_target_identity_for_order=journal.cancel_target_identity_for_order,
                known_foreign_cancel_client_order_ids=(
                    settings._known_foreign_cancel_client_order_ids
                ),
                known_foreign_cancel_order_id=(
                    settings._foreign_cancel_order_directory.is_known_foreign_order_id
                    if settings._foreign_cancel_order_directory is not None
                    else None
                ),
                mutation_telemetry=timing,
                lifecycle_observer=milestones.mark,
                native_callback_observer=(
                    native_callback_transport.observe
                    if native_callback_transport is not None
                    else None
                ),
            )
            runtime = _SessionShardRuntime(
                shard_id=shard_id,
                client_id=client_id,
                ownership=ownership,
                resolver=resolver,
                journal=journal,
                scheduler=schedulers[shard_id],
                native_lane_ids=native_lane_ids,
                native_callback_transport=native_callback_transport,
            )
            adapter = EmtAdapterProfile.create_controlled_execution_adapter(
                mapping,
                session_factory=factory,
                client_order_ref_factory=resolver.client_order_ref_for,
                expected_api_version="2.27.0",
                expected_trading_day=trading_day,
                session_epoch=index + 1,
                signed_scope_ownership=signed,
                scope_ownership_verifier=verifier,
                event_ingress_capacity=max(8_192, width * 4),
                broker_event_observer_wrapper=callback_observer_wrapper_for_source(shard_id),
                lifecycle_observer=milestones.mark,
            )
            runtime.adapter = adapter
            runtimes[shard_id] = runtime
        # Build and freeze the entire route table before C0.  This moves route
        # construction and RLock acquisition out of the serial EMT worker;
        # ``submit_prepared`` below can only prove the already-admitted route.
        for burst_plan in plans:
            runtimes[burst_plan.shard_id].resolver.prepare_submit(
                burst_plan.route, burst_plan.instruction
            )
        for shard_id, runtime in runtimes.items():
            prepared_count = runtime.resolver.seal_submits()
            if prepared_count != planned_counts[shard_id]:
                raise TradeCoreMmapEmtPerfError(
                    "sealed EMT submit route table does not cover its session shard"
                )
            route_preparation_by_shard[shard_id] = {
                "prepared_count": prepared_count,
                "sealed": True,
                "preparation_boundary": "before_C0",
            }
        mmap_preparation_ns = time.monotonic_ns() - warmup_started_ns
        milestones.mark("mmap_journals_prepared")

        for burst_plan in plans:
            routed_binding = shard_router.route_execution(burst_plan.instruction).binding
            if routed_binding.shard_id != burst_plan.shard_id:
                raise TradeCoreMmapEmtPerfError("session shard selection is not deterministic")
            callback_router.register_pre_native(
                binding=routed_binding,
                client_order_ref=burst_plan.route.command.client_order_ref,
            )
        if callback_router.seal() != width:
            raise TradeCoreMmapEmtPerfError(
                "session-shard callback registrations do not cover the full burst"
            )
        for index, burst_plan in enumerate(plans):
            timing.register(
                index=index,
                execution_id=burst_plan.instruction.execution_id,
                client_order_ref=burst_plan.route.command.client_order_ref,
            )
        expected_client_refs = {plan.route.command.client_order_ref for plan in plans}
        observer = _BurstObserver(expected_client_refs)
        same_session_inflight = (
            _SameSessionConcurrentInflightGate(capacity=settings.native_inflight_limit)
            if settings.same_session_insert_callers > 1
            else None
        )
        target_observers = {
            shard_id: runtime.journal.wrap_broker_event_observer(
                observer,
                callback_stage_observer=timing.on_callback,
                native_inflight_release=(
                    same_session_inflight.release
                    if same_session_inflight is not None
                    else runtime.scheduler.release_new_order_inflight
                ),
                callback_ingress_is_external=True,
            )
            for shard_id, runtime in runtimes.items()
        }
        callback_lanes = {
            shard_id: _SpscCallbackRing(
                capacity=max(1_024, planned_counts[shard_id] * 4),
                wakeup_mode=settings.callback_wakeup_mode,
            )
            for shard_id in runtimes
        }
        callback_lanes_by_client_ref: dict[str, _SpscCallbackRing] = {}
        for burst_plan in plans:
            if burst_plan.native_lane_id not in runtimes[burst_plan.shard_id].native_lane_ids:
                raise TradeCoreMmapEmtPerfError("callback native lane crossed session shards")
            callback_lanes_by_client_ref[burst_plan.route.command.client_order_ref] = (
                callback_lanes[burst_plan.shard_id]
            )
        for shard_id in runtimes:
            selected_callback_profile = select_callback_persistence_profile_v1(
                requested=settings.callback_persistence_profile,
                eligibility=CallbackDirectWriteEligibilityV1(
                    scope_id=shard_id,
                    disarmed_simulated=settings.execute_simulated,
                    sealed_route_map=True,
                    source_scope_equals_target_scope=(
                        settings._partitioned_scope_worker and len(runtimes) == 1
                    ),
                    # The only direct writer is this shard's adapter-drain
                    # worker. The normal control profile still owns a separate
                    # target-lane worker and does not select this candidate.
                    sole_drain_writer=(settings._partitioned_scope_worker and len(runtimes) == 1),
                    # The benchmark downstream accepts only the pre-registered
                    # finite burst set; it has no unbounded callback inbox.
                    bounded_projector_inbox=True,
                    callback_journal_capacity_available=True,
                ),
            )
            sharded_callback_observers[shard_id] = _ShardedCallbackObserver(
                callback_router=callback_router,
                callback_lanes_by_client_ref=callback_lanes_by_client_ref,
                source_shard_id=shard_id,
                direct_target_observer=(
                    target_observers[shard_id]
                    if selected_callback_profile is CallbackPersistenceProfileV1.DIRECT_CANDIDATE
                    else None
                ),
                persistence_profile=selected_callback_profile,
                downstream=observer,
                timing=timing,
            )
        for runtime in runtimes.values():
            assert runtime.adapter is not None
            runtime.adapter.start(observer)
            runtime.dispatcher = TradeCoreEmtRouteDispatcher(
                execution_adapter=runtime.adapter,
                route_resolver=runtime.resolver,
                telemetry=timing,
            )
        if settings.prebuild_submit_mapping:
            prepared_started_ns = time.monotonic_ns()
            for burst_plan in plans:
                adapter = runtimes[burst_plan.shard_id].adapter
                if adapter is None:
                    raise TradeCoreMmapEmtPerfError(
                        "prepared EMT submit mapping requires a started adapter"
                    )
                prepared_submits_by_execution[burst_plan.instruction.execution_id] = (
                    adapter.prepare_submit(burst_plan.instruction)
                )
            if len(prepared_submits_by_execution) != width:
                raise TradeCoreMmapEmtPerfError(
                    "prepared EMT submit mapping does not cover the burst"
                )
            prepared_elapsed_ns = time.monotonic_ns() - prepared_started_ns
            for shard_id, runtime in runtimes.items():
                prepared_count = sum(plan.shard_id == shard_id for plan in plans)
                route_preparation_by_shard[shard_id] = {
                    **route_preparation_by_shard[shard_id],
                    "prepared_submit_mapping": True,
                    "prepared_submit_count": prepared_count,
                    "prepared_submit_before_C0_ns": prepared_elapsed_ns,
                    "prepared_submit_native_vendor_object": False,
                    "prepared_submit_side_effects": False,
                }
        if settings.session_preflight_only:
            if release_control is None:
                raise TradeCoreMmapEmtPerfError(
                    "session preflight requires the isolated parent coordinator"
                )
            # The parent only opens this barrier after every child has started
            # its real EMT adapter.  No producer, dispatcher submit, query or
            # cancel is created on this path.
            release_control.ready_event.set()
            # The parent establishes scopes serially to avoid treating an EMT
            # account-level login lock as a capacity limit.  A ready early
            # scope therefore allows one bounded start window for every
            # declared scope before the common stop release.
            parent_release_wait_seconds = settings.session_preflight_timeout_seconds * (
                settings._session_preflight_scope_count or len(settings.session_client_ids)
            )
            if not release_control.release_event.wait(parent_release_wait_seconds):
                raise TradeCoreMmapEmtPerfError(
                    "session preflight release did not arrive before its hard deadline"
                )
            report = _session_preflight_child_report(
                settings=settings,
                runtimes=runtimes,
                process_cpu_set=process_cpu_set,
            )
            settings.output.parent.mkdir(parents=True, exist_ok=True)
            settings.output.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            milestones.mark("session_preflight_report_written")
            if settings.isolate_native_process:
                _exit_isolated_native_process(report)
            return report
        plans_by_execution = {plan.instruction.execution_id: plan for plan in plans}

        def drain_callbacks(runtime: _SessionShardRuntime) -> None:
            try:
                _pin_current_thread_to_cpu(settings.callback_ingress_cpu)
                while not callback_stop.is_set():
                    try:
                        assert runtime.adapter is not None
                        runtime.adapter.drain_broker_events(wait_timeout_seconds=0.25)
                    except (
                        BaseException
                    ) as error:  # leave a payload-free failure reason in the report
                        with callback_error_lock:
                            callback_errors.append(_failure_signature(error))
                        return
            except BaseException as error:  # leave a payload-free failure reason in the report
                with callback_error_lock:
                    callback_errors.append(_failure_signature(error))
                return

        def drain_target_callbacks(lane_id: str, target_observer: Any) -> None:
            try:
                _pin_current_thread_to_cpu(settings.callback_journal_cpu)
                _drain_sharded_callback_lane(
                    lane=callback_lanes[lane_id],
                    target_observer=target_observer,
                    timing=timing,
                    stop=callback_lane_stop,
                    pipeline_max_items=settings.callback_pipeline_max_items,
                )
            except BaseException as error:
                with callback_error_lock:
                    callback_errors.append(_failure_signature(error))

        if settings.callback_persistence_profile is CallbackPersistenceProfileV1.CONTROL:
            for runtime in runtimes.values():
                target_observer = target_observers[runtime.shard_id]
                callback_thread = Thread(
                    target=drain_target_callbacks,
                    args=(runtime.shard_id, target_observer),
                    name=f"tradecore-emt-burst-callback-target-{runtime.shard_id}",
                    daemon=True,
                )
                callback_thread.start()
                callback_threads.append(callback_thread)
        for runtime in runtimes.values():
            callback_thread = Thread(
                target=drain_callbacks,
                args=(runtime,),
                name=f"tradecore-emt-burst-callback-{runtime.shard_id}",
                daemon=True,
            )
            callback_thread.start()
            callback_threads.append(callback_thread)
        milestones.mark("callback_worker_started")

        producer_errors: list[str] = []
        producer_rejections: list[str] = []
        rejected_ingress_latencies_ns: list[int] = []
        producer_error_lock = Lock()
        producer_rejection_lock = Lock()
        direct_same_session_probe = same_session_inflight is not None
        # The default topology has one producer per scope and one native
        # scheduler consumer per scope. The explicit same-session mode instead
        # probes the vendor's InsertOrder thread-safety claim using one API,
        # bounded direct credits and multiple callers. It is an experiment,
        # never the default reliable-dispatch topology.
        producer_partitions = (
            [plans]
            if settings.submission_topology == "tight-single-producer"
            else _partition_plans(plans, settings.same_session_insert_callers)
            if direct_same_session_probe
            else _partition_plans_by_shard(plans, tuple(runtimes))
        )
        release_ns = 0
        direct_ingress_ready = Event()

        def mark_signal_release() -> None:
            nonlocal release_ns
            if release_control is None:
                release_ns = time.monotonic_ns()
                return
            release_control.ready_event.set()
            if not release_control.release_event.wait(settings.native_start_budget_seconds):
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session release did not arrive before native start budget"
                )
            release_ns = int(release_control.release_ns.value)
            if release_ns <= 0:
                raise TradeCoreMmapEmtPerfError(
                    "process-isolated session release timestamp is invalid"
                )

        # The former probe created one OS thread per raw signal.  That makes
        # the benchmark itself the dominant C0->C1 queue.  A fixed source pool
        # preserves one logical simultaneous C0 while bounding Python/GIL
        # contention.  The single native EMT lane remains the only SDK caller.
        barrier = Barrier(
            len(producer_partitions) + 1,
            action=mark_signal_release,
            timeout=60.0,
        )
        producers_completed = Event()
        accepted: list[tuple[_BurstPlan, Any]] = []
        dispatch_errors: list[str] = []
        accepted_lock = Lock()
        dispatch_error_lock = Lock()
        expired_before_native = [0]

        def enqueue_signals(partition: list[_BurstPlan]) -> None:
            try:
                _pin_current_thread_to_cpu(settings.ingress_producer_cpu)
                barrier.wait()
            except BaseException as error:
                with producer_error_lock:
                    producer_errors.append(_failure_signature(error))
                return
            if direct_same_session_probe and not direct_ingress_ready.wait(timeout=60.0):
                with producer_error_lock:
                    producer_errors.append("DirectIngressReleaseTimeout")
                return
            for burst_plan in partition:
                request_id, instruction = burst_plan.request_id, burst_plan.instruction
                submit_key = f"submit:{instruction.execution_id}"
                if settings.submission_topology == "tight-single-producer":
                    timing.on_tight_submit_begin(instruction, time.monotonic_ns())
                if direct_same_session_probe:
                    if same_session_inflight is None:
                        raise TradeCoreMmapEmtPerfError(
                            "direct same-session probe has no inflight gate"
                        )
                    try:
                        if not same_session_inflight.try_acquire(instruction.execution_id):
                            decision_ns = time.monotonic_ns()
                            timing.on_ingress_decision(instruction, decision_ns)
                            with producer_rejection_lock:
                                producer_rejections.append(_OVERLOAD_REJECTION_CODE)
                                rejected_ingress_latencies_ns.append(decision_ns - release_ns)
                            continue
                        runtime = runtimes[burst_plan.shard_id]
                        assert runtime.dispatcher is not None
                        timing.on_native_dequeue(instruction, time.monotonic_ns())
                        timing.on_native_dispatch_ready(instruction, time.monotonic_ns())
                        receipt = runtime.dispatcher.submit_prepared(
                            route=burst_plan.route,
                            instruction=instruction,
                            prepared_submit=prepared_submits_by_execution.get(
                                instruction.execution_id
                            ),
                        )
                    except BaseException as error:
                        same_session_inflight.release(instruction.execution_id)
                        with dispatch_error_lock:
                            dispatch_errors.append(_failure_signature(error))
                        milestones.mark(
                            "native_dispatch_failed_after_"
                            + _last_submit_stage(timing, instruction)
                        )
                    else:
                        with accepted_lock:
                            accepted.append((burst_plan, receipt.venue_order_identity))
                    continue
                scheduler = runtimes[burst_plan.shard_id].scheduler
                try:
                    timing.start_at(submit_key, release_ns)
                    admitted_monotonic_ns = time.monotonic_ns()
                    admitted = scheduler.admit_and_commit_new_order(
                        request_id=request_id,
                        serialized_size_bytes=512,
                        admitted_monotonic_ns=admitted_monotonic_ns,
                        instruction=instruction,
                        checked_monotonic_ns=time.monotonic_ns(),
                    )
                    decision_ns = time.monotonic_ns()
                    timing.on_ingress_decision(instruction, decision_ns)
                    if isinstance(admitted, IngressRejection):
                        with producer_rejection_lock:
                            producer_rejections.append(admitted.code.value)
                            rejected_ingress_latencies_ns.append(decision_ns - release_ns)
                except BaseException as error:
                    timing.on_ingress_decision(instruction, time.monotonic_ns())
                    with producer_error_lock:
                        producer_errors.append(type(error).__name__)

        def drain_native_direct(runtime: _SessionShardRuntime) -> None:
            try:
                _pin_current_thread_to_cpu(settings.native_worker_cpu)
                while True:
                    scheduled = runtime.scheduler.dequeue(checked_monotonic_ns=time.monotonic_ns())
                    if scheduled is None:
                        if producers_completed.is_set():
                            return
                        runtime.scheduler.wait_for_dispatch(timeout_seconds=0.25)
                        continue
                    if scheduled.expired_before_native:
                        with dispatch_error_lock:
                            expired_before_native[0] += 1
                        continue
                    burst_plan = plans_by_execution[scheduled.command.execution_id]
                    timing.on_native_dequeue(burst_plan.instruction, time.monotonic_ns())
                    if (
                        burst_plan.shard_id != runtime.shard_id
                        or burst_plan.native_lane_id not in runtime.native_lane_ids
                    ):
                        raise TradeCoreMmapEmtPerfError(
                            "scheduled execution crossed its direct native session"
                        )
                    # A lane owns one mmap file but does not add an EMT SDK
                    # caller. One worker remains the only caller per API.
                    lane_context = runtime.journal.mmap_native_lane(burst_plan.native_lane_id)
                    with lane_context:
                        try:
                            assert runtime.dispatcher is not None
                            timing.on_native_dispatch_ready(
                                burst_plan.instruction, time.monotonic_ns()
                            )
                            receipt = runtime.dispatcher.submit_prepared(
                                route=burst_plan.route,
                                instruction=burst_plan.instruction,
                                prepared_submit=prepared_submits_by_execution.get(
                                    burst_plan.instruction.execution_id
                                ),
                            )
                        except BaseException as error:
                            runtime.scheduler.release_new_order_inflight(
                                burst_plan.instruction.execution_id
                            )
                            with dispatch_error_lock:
                                dispatch_errors.append(_failure_signature(error))
                            milestones.mark(
                                "native_dispatch_failed_after_"
                                + _last_submit_stage(timing, burst_plan.instruction)
                            )
                        else:
                            with accepted_lock:
                                accepted.append((burst_plan, receipt.venue_order_identity))
            except BaseException as error:
                with dispatch_error_lock:
                    dispatch_errors.append(_failure_signature(error))

        if settings.hot_single_order_samples:
            if hot_single_order_control is None:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order child has no parent sequencing control"
                )
            return _run_hot_single_order_worker(
                settings=settings,
                control=hot_single_order_control,
                process_cpu_set=process_cpu_set,
                milestones=milestones,
                timing=timing,
                plans=plans,
                runtimes=runtimes,
                binding=binding,
                observer=observer,
                callback_stop=callback_stop,
                callback_lane_stop=callback_lane_stop,
                callback_threads=callback_threads,
                callback_errors=callback_errors,
                callback_error_lock=callback_error_lock,
                drain_native_direct=drain_native_direct,
                producers_completed=producers_completed,
                accepted=accepted,
                accepted_lock=accepted_lock,
                dispatch_errors=dispatch_errors,
                dispatch_error_lock=dispatch_error_lock,
                expired_before_native=expired_before_native,
                producer_errors=producer_errors,
                producer_rejections=producer_rejections,
                rejected_ingress_latencies_ns=rejected_ingress_latencies_ns,
                mmap_preparation_ns=mmap_preparation_ns,
                mmap_preparation_by_shard=mmap_preparation_by_shard,
                route_preparation_by_shard=route_preparation_by_shard,
                gc_was_enabled=gc_was_enabled,
            )

        milestones.mark("concurrent_signal_producers_starting")
        # Admission and native SDK dispatch remain serial per API/session.
        # mmap lane affinity is intentionally applied inside that single worker
        # so each order keeps its own Journal and callback lane.
        # This is the safe pure-Python composition after the direct four-caller
        # same-session experiment failed to complete within its hard timeout.
        native_route_threads: list[Thread] = []
        native_lane_threads = (
            []
            if direct_same_session_probe
            else [
                Thread(
                    target=drain_native_direct,
                    args=(runtime,),
                    name=f"tradecore-emt-burst-native-{runtime.shard_id}",
                    daemon=True,
                )
                for runtime in runtimes.values()
            ]
        )
        for native_lane_thread in native_lane_threads:
            native_lane_thread.start()
        for native_route_thread in native_route_threads:
            native_route_thread.start()
        producer_threads = [
            Thread(
                target=enqueue_signals,
                args=(partition,),
                name=f"tradecore-emt-burst-ingress-{index}",
                daemon=True,
            )
            for index, partition in enumerate(producer_partitions)
        ]
        for producer_thread in producer_threads:
            producer_thread.start()
        # Counter collection is complete before C0 and never invokes a service.
        runtime_telemetry = LinuxRuntimeTelemetry()
        runtime_telemetry_before = runtime_telemetry.capture()
        if settings.disable_gc_during_measured_window and gc_was_enabled:
            gc.disable()
            gc_suspended = True
        try:
            try:
                barrier.wait()
            except BrokenBarrierError:
                with producer_error_lock:
                    producer_errors.append("BrokenBarrierError")
            else:
                if direct_same_session_probe:
                    # Model all raw signals as arriving at one shared ingress
                    # boundary.  The caller threads do not start dispatching
                    # until every immutable plan has its C0/C0A timestamps;
                    # therefore C0->C0A excludes test-loop/GIL scheduling.
                    ingress_decision_ns = time.monotonic_ns()
                    for burst_plan in plans:
                        timing.start_at(f"submit:{burst_plan.instruction.execution_id}", release_ns)
                        timing.on_ingress_decision(burst_plan.instruction, ingress_decision_ns)
                    direct_ingress_ready.set()
            if direct_same_session_probe:
                # Unblock workers in the exceptional barrier case as well so
                # joins cannot mask the actual diagnostic failure.
                direct_ingress_ready.set()
            for producer_thread in producer_threads:
                producer_thread.join()
        finally:
            producers_completed.set()
        for native_route_thread in native_route_threads:
            native_route_thread.join()
        for native_lane_thread in native_lane_threads:
            native_lane_thread.join()
        milestones.mark("concurrent_signal_producers_completed")
        milestones.mark("concurrent_ingress_native_dispatch_completed")

        accepted_client_refs = {
            identity.client_order_ref
            for _, identity in accepted
            if identity is not None and identity.client_order_ref is not None
        }
        callback_completed_refs = _wait_for_callback_stages(
            timing=timing,
            observer=observer,
            expected_client_refs=accepted_client_refs,
            timeout_seconds=settings.callback_timeout_seconds,
            callback_errors=callback_errors,
            callback_error_lock=callback_error_lock,
        )
        submit_callback_samples = _collect_samples(plans, timing)
        runtime_telemetry_report = runtime_telemetry.report(
            runtime_telemetry_before, runtime_telemetry.capture()
        )
        if gc_suspended:
            gc.enable()
            gc_suspended = False

        cleanup_attempted = 0
        cleanup_accepted = 0
        cleanup_errors: list[str] = []
        cleanup_terminal_callback_complete = False
        cleanup_terminal_inflight_remaining = len(accepted)
        foreign_cancel_directory_ready = False
        foreign_cancel_directory = settings._foreign_cancel_order_directory
        foreign_cancel_cleanup_barrier = settings._foreign_cancel_cleanup_barrier
        if foreign_cancel_directory is not None:
            try:
                foreign_cancel_directory.publish_owned_order_ids(
                    tuple(_positive_venue_order_id(identity) for _burst_plan, identity in accepted)
                )
                if foreign_cancel_cleanup_barrier is None:
                    raise TradeCoreMmapEmtPerfError("foreign cancel cleanup barrier is absent")
                foreign_cancel_cleanup_barrier.wait(timeout=settings.callback_timeout_seconds)
                foreign_cancel_directory.mark_ready()
                foreign_cancel_directory_ready = True
            except BaseException:
                if foreign_cancel_cleanup_barrier is not None:
                    try:
                        foreign_cancel_cleanup_barrier.abort()
                    except BaseException:
                        pass
                cleanup_errors.append("ForeignCancelDirectorySyncFailed")
        latest_native_return_ns = max(
            (
                timing.consume(f"submit:{burst_plan.instruction.execution_id}").get(
                    "C4_AFTER_SDK", 0
                )
                for burst_plan, _identity in accepted
            ),
            default=0,
        )
        cleanup_cooldown_wait_ns = _remaining_cleanup_cooldown_ns(
            latest_native_return_ns=latest_native_return_ns,
            now_ns=time.monotonic_ns(),
        )
        if cleanup_cooldown_wait_ns:
            # This is outside C0--C7 and occurs before any cancel request.
            # It avoids converting the 499-order verification itself into a
            # combined submit/cancel rate-window breach.  Each cleanup cancel
            # remains exactly one attempt; this is not a retry loop.
            time.sleep(cleanup_cooldown_wait_ns / 1_000_000_000)
        for index, (burst_plan, identity) in enumerate(accepted):
            instruction = burst_plan.instruction
            if identity is None:
                cleanup_errors.append("MissingVenueOrderIdentity")
                continue
            cleanup_attempted += 1
            try:
                milestones.mark("hot_single_order_cleanup_cancel_before_dispatch")
                cancel_instruction, cancel_route = _cancel_plan(
                    instruction=instruction,
                    target=identity,
                    target_route_payload_hash=burst_plan.route.command.payload_hash,
                    binding=binding,
                )
                runtime = runtimes[burst_plan.shard_id]
                assert runtime.dispatcher is not None
                runtime.dispatcher.cancel(route=cancel_route, instruction=cancel_instruction)
            except BaseException as error:
                cleanup_errors.append(_failure_signature(error))
            else:
                cleanup_accepted += 1
            # Cleanup is outside the measured C0--C7 submit/callback samples.
            # It deliberately sends one cancellation attempt at a bounded
            # cadence so an exchange-side cancel window cannot turn a clean
            # benchmark into a set of left-open simulation orders.  This is
            # pacing, not automatic retry.
            if index + 1 < len(accepted):
                time.sleep(settings.cleanup_cancel_interval_seconds)
        if cleanup_accepted == len(accepted) and not cleanup_errors:
            (
                cleanup_terminal_callback_complete,
                cleanup_terminal_inflight_remaining,
            ) = _wait_for_cleanup_terminal_callbacks(
                runtimes=runtimes,
                timeout_seconds=settings.callback_timeout_seconds,
                callback_errors=callback_errors,
                callback_error_lock=callback_error_lock,
            )
        if not cleanup_terminal_callback_complete:
            cleanup_errors.append("CleanupTerminalCallbackIncomplete")
        milestones.mark("bounded_cleanup_completed")

        # This snapshot must include terminal callback release after the
        # bounded cleanup, rather than only the pre-cleanup burst peak.
        scheduler_snapshots = {
            shard_id: runtime.scheduler.snapshot() for shard_id, runtime in runtimes.items()
        }
        callback_gap_reason_counts = _callback_gap_reason_counts(runtimes)
        verified_foreign_cancel_callback_count = _verified_foreign_cancel_callback_count(runtimes)

        # Do not leave the native SDK session running behind a written report.
        # The callback worker must stop first so it cannot drain an ingress
        # whose session is being closed.
        callback_stop.set()
        callback_lane_stop.set()
        for callback_thread in callback_threads:
            callback_thread.join(timeout=1.0)
        if not settings.isolate_native_process:
            for runtime in runtimes.values():
                if runtime.adapter is not None:
                    _stop_adapter(runtime.adapter, milestones)
                    runtime.adapter = None

        seen_refs, callback_gaps = observer.snapshot()
        with callback_error_lock:
            callback_error_kinds = list(callback_errors)
        report = _report(
            settings=settings,
            samples=submit_callback_samples,
            mmap_preparation_ns=mmap_preparation_ns,
            scheduler_snapshots=scheduler_snapshots,
            producer_error_kinds=producer_errors,
            producer_rejection_kinds=producer_rejections,
            rejected_ingress_latencies_ns=rejected_ingress_latencies_ns,
            dispatch_error_kinds=dispatch_errors,
            expired_before_native=expired_before_native[0],
            native_accepted=len(accepted),
            callback_completed=len(callback_completed_refs),
            callback_seen=len(seen_refs),
            callback_gaps=callback_gaps,
            callback_gap_reason_counts=callback_gap_reason_counts,
            verified_foreign_cancel_callback_count=verified_foreign_cancel_callback_count,
            foreign_cancel_directory_ready=foreign_cancel_directory_ready,
            callback_error_kinds=callback_error_kinds,
            cleanup_attempted=cleanup_attempted,
            cleanup_accepted=cleanup_accepted,
            cleanup_error_kinds=cleanup_errors,
            cleanup_cooldown_wait_ns=cleanup_cooldown_wait_ns,
            cleanup_terminal_callback_complete=cleanup_terminal_callback_complete,
            cleanup_terminal_inflight_remaining=cleanup_terminal_inflight_remaining,
            source_worker_count=len(producer_partitions),
            same_session_concurrent_inflight=(
                same_session_inflight.snapshot() if same_session_inflight is not None else None
            ),
            runtime_telemetry=runtime_telemetry_report,
            gc_control={
                "disabled_during_C0_to_C7": settings.disable_gc_during_measured_window,
                "was_enabled_before_window": gc_was_enabled,
            },
            mmap_preparation_by_shard=mmap_preparation_by_shard,
            route_preparation_by_shard=route_preparation_by_shard,
            tight_burst_order_plan_evidence=tight_burst_order_plan_evidence,
            process_cpu_set=process_cpu_set,
            native_callback_transport=_native_callback_transport(runtimes),
        )
        settings.output.parent.mkdir(parents=True, exist_ok=True)
        settings.output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        milestones.mark("report_written")
        if settings.isolate_native_process:
            _exit_isolated_native_process(report)
        return report
    finally:
        if gc_suspended:
            gc.enable()
        callback_stop.set()
        callback_lane_stop.set()
        for callback_thread in callback_threads:
            callback_thread.join(timeout=1.0)
        if not settings.isolate_native_process:
            for runtime in runtimes.values():
                if runtime.adapter is not None:
                    _stop_adapter(runtime.adapter, milestones)
        for runtime in runtimes.values():
            runtime.journal.close()
        for callback_lane in callback_lanes.values():
            callback_lane.close()
        milestones.flush()


def _fixture_for_run(settings: Settings) -> tuple[dict[str, Any], dict[str, str | bool] | None]:
    """Resolve a real order plan only for a path that can release an order.

    The process-isolated session preflight is intentionally zero-order: it
    only starts verified sessions and then exits.  Keeping it independent of
    ignored, deploy-local controlled-order fixture files makes this safety
    check reproducible without weakening the real burst requirement.
    """

    if settings.session_preflight_only:
        return dict(_SESSION_PREFLIGHT_SENTINEL_FIXTURE), None
    # A legacy controlled fixture has no tight-plan fingerprint, but it is
    # still capable of reaching InsertOrder.  Do not let that older shape
    # bypass the same explicit simulated-order authorization required by the
    # hash-bound tight-plan path below.
    require_armed_simulated_fixture(settings.fixture)
    if settings.tight_burst_order_plan is not None:
        tight_burst_order_plan = load_tight_burst_order_plan(
            settings.tight_burst_order_plan,
            burst_width=settings.burst_width,
        )
        require_armed_tight_burst_fixture(
            settings.fixture,
            expected_sha256=tight_burst_order_plan.source_fixture_sha256,
        )
        return (
            tight_burst_order_plan.request,
            {
                "selection": tight_burst_order_plan.selection_mode,
                "live_quote_preflight": (
                    tight_burst_order_plan.selection_mode == "external-read-only-market-preflight"
                ),
                "plan_sha256": tight_burst_order_plan.plan_sha256,
            },
        )
    return _load_order_fixture(settings.fixture), None


def _run_hot_single_order_worker(
    *,
    settings: Settings,
    control: _HotSingleOrderControl,
    process_cpu_set: tuple[int, ...] | None,
    milestones: _Milestones,
    timing: _BurstTiming,
    plans: list[_BurstPlan],
    runtimes: dict[str, _SessionShardRuntime],
    binding: AccountJournalBinding,
    observer: _BurstObserver,
    callback_stop: Event,
    callback_lane_stop: Event,
    callback_threads: list[Thread],
    callback_errors: list[str],
    callback_error_lock: Lock,
    drain_native_direct: Any,
    producers_completed: Event,
    accepted: list[tuple[_BurstPlan, Any]],
    accepted_lock: Lock,
    dispatch_errors: list[str],
    dispatch_error_lock: Lock,
    expired_before_native: list[int],
    producer_errors: list[str],
    producer_rejections: list[str],
    rejected_ingress_latencies_ns: list[int],
    mmap_preparation_ns: int,
    mmap_preparation_by_shard: dict[str, dict[str, int | bool | str]],
    route_preparation_by_shard: dict[str, dict[str, int | bool | str]],
    gc_was_enabled: bool,
) -> dict[str, Any]:
    """Run prebuilt single-order samples after every session is warm.

    C0 is taken in the owning child immediately before `admit_and_commit`.
    Parent queue transport is deliberately outside the measurement; it is the
    test coordinator, not a TradeCore production hop.  The parent does not
    send another release until this worker acknowledges C7 plus one terminal
    cleanup callback, maintaining account-wide concurrency of one.
    """

    if len(runtimes) != 1:
        raise TradeCoreMmapEmtPerfError("hot single-order worker must own one session scope")
    if len(plans) != settings.hot_single_order_samples:
        raise TradeCoreMmapEmtPerfError("hot single-order plan count is invalid")

    native_lane_threads = [
        Thread(
            target=drain_native_direct,
            args=(runtime,),
            name=f"tradecore-emt-hot-single-native-{runtime.shard_id}",
            daemon=True,
        )
        for runtime in runtimes.values()
    ]
    for native_lane_thread in native_lane_threads:
        native_lane_thread.start()
    milestones.mark("hot_single_order_workers_ready")

    runtime_telemetry = LinuxRuntimeTelemetry()
    runtime_telemetry_before = runtime_telemetry.capture()
    gc_suspended = False
    if settings.disable_gc_during_measured_window and gc_was_enabled:
        gc.disable()
        gc_suspended = True

    cleanup_attempted = 0
    cleanup_accepted = 0
    cleanup_errors: list[str] = []
    cleanup_action_pacing_wait_ns = 0
    cleanup_terminal_callback_complete = False
    cleanup_terminal_inflight_remaining = 0
    sample_failed = False
    foreign_cancel_directory = settings._foreign_cancel_order_directory

    def accepted_identity_for(plan: _BurstPlan) -> Any | None:
        with accepted_lock:
            for accepted_plan, identity in accepted:
                if accepted_plan.instruction.execution_id == plan.instruction.execution_id:
                    return identity
        return None

    def wait_for_c7_and_identity(plan: _BurstPlan) -> Any:
        deadline = time.monotonic() + settings.callback_timeout_seconds
        expected_ref = plan.route.command.client_order_ref
        while True:
            identity = accepted_identity_for(plan)
            stages = timing.consume(f"callback:{expected_ref}")
            if identity is not None and "C7_AFTER_DOWNSTREAM" in stages:
                return identity
            with dispatch_error_lock:
                if dispatch_errors:
                    raise TradeCoreMmapEmtPerfError("hot single-order native dispatch failed")
            with callback_error_lock:
                if callback_errors:
                    raise TradeCoreMmapEmtPerfError("hot single-order callback path failed")
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                raise TradeCoreMmapEmtPerfError("hot single-order C7 callback timed out")
            observer.wait_for_change(min(remaining_seconds, 0.05))

    try:
        # `ready` is intentionally set only once all local callback workers
        # and the serial native caller are live.  The parent can then warm the
        # next scope while this child keeps its SDK session open and idle.
        control.ready_event.set()
        milestones.mark("hot_single_order_parent_ready")
        for local_index, plan in enumerate(plans):
            try:
                command = control.command_queue.get(timeout=settings.native_start_budget_seconds)
            except Empty as error:
                raise TradeCoreMmapEmtPerfError(
                    "hot single-order parent did not release a prepared sample"
                ) from error
            if command != "release":
                raise TradeCoreMmapEmtPerfError("hot single-order control command is invalid")
            milestones.mark("hot_single_order_parent_release")

            submit_key = f"submit:{plan.instruction.execution_id}"
            c0_ns = time.monotonic_ns()
            timing.start_at(submit_key, c0_ns)
            try:
                scheduler = runtimes[plan.shard_id].scheduler
                admitted = scheduler.admit_and_commit_new_order(
                    request_id=plan.request_id,
                    serialized_size_bytes=512,
                    admitted_monotonic_ns=time.monotonic_ns(),
                    instruction=plan.instruction,
                    checked_monotonic_ns=time.monotonic_ns(),
                )
                decision_ns = time.monotonic_ns()
                timing.on_ingress_decision(plan.instruction, decision_ns)
                if isinstance(admitted, IngressRejection):
                    producer_rejections.append(admitted.code.value)
                    rejected_ingress_latencies_ns.append(decision_ns - c0_ns)
                    raise TradeCoreMmapEmtPerfError("hot single-order admission was rejected")
            except BaseException:
                if "C0A_AFTER_INGRESS_DECISION" not in timing.consume(submit_key):
                    timing.on_ingress_decision(plan.instruction, time.monotonic_ns())
                raise

            identity = wait_for_c7_and_identity(plan)
            milestones.mark("hot_single_order_C7_completed")
            if foreign_cancel_directory is not None:
                try:
                    foreign_cancel_directory.publish_owned_order_id(
                        local_index=local_index,
                        order_id=_positive_venue_order_id(identity),
                    )
                except BaseException:
                    # Preserve the mandatory one-shot cleanup even if the
                    # cross-scope callback directory cannot be published.
                    # The bounded report marks this as an acceptance failure.
                    cleanup_errors.append("ForeignCancelDirectoryPublishFailed")
            c4_ns = timing.consume(submit_key).get("C4_AFTER_SDK", 0)
            # The simulator can acknowledge the cancellation call before the
            # order-side terminal state is eligible for a callback if a cancel
            # immediately follows the first working C7.  Retain the same
            # post-submit 1.10 s stability window used by the accepted 499
            # cleanup; it is outside C0--C7 and prevents one sample's cleanup
            # from invalidating the next sample's hot-path evidence.
            minimum_interval_ns = int(_EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS * 1_000_000_000)
            if c4_ns:
                elapsed_ns = time.monotonic_ns() - c4_ns
                if elapsed_ns < minimum_interval_ns:
                    delay_ns = minimum_interval_ns - elapsed_ns
                    cleanup_action_pacing_wait_ns += delay_ns
                    time.sleep(delay_ns / 1_000_000_000)

            cleanup_attempted += 1
            try:
                cancel_instruction, cancel_route = _cancel_plan(
                    instruction=plan.instruction,
                    target=identity,
                    target_route_payload_hash=plan.route.command.payload_hash,
                    binding=binding,
                )
                runtime = runtimes[plan.shard_id]
                assert runtime.dispatcher is not None
                runtime.dispatcher.cancel(route=cancel_route, instruction=cancel_instruction)
            except BaseException as error:
                cleanup_errors.append(_failure_signature(error))
                raise
            cleanup_accepted += 1
            milestones.mark("hot_single_order_cleanup_cancel_accepted")
            terminal_complete, inflight_remaining = _wait_for_cleanup_terminal_callbacks(
                runtimes=runtimes,
                timeout_seconds=settings.callback_timeout_seconds,
                callback_errors=callback_errors,
                callback_error_lock=callback_error_lock,
            )
            if not terminal_complete:
                cleanup_errors.append("CleanupTerminalCallbackIncomplete")
                cleanup_terminal_inflight_remaining = inflight_remaining
                control.acknowledgement_queue.put((control.scope_name, False))
                sample_failed = True
                break
            milestones.mark("hot_single_order_cleanup_terminal")
            control.acknowledgement_queue.put((control.scope_name, True))

        # A bounded per-sample failure is a measurement failure, not a reason
        # to discard its already payload-free C0--C7/cleanup counters.  Emit
        # the ordinary report so the parent and subsequent diagnosis can
        # distinguish callback integrity from control-plane timeout.
        accepted_client_refs = {
            identity.client_order_ref
            for _plan, identity in accepted
            if identity is not None and identity.client_order_ref is not None
        }
        callback_completed_refs = _wait_for_callback_stages(
            timing=timing,
            observer=observer,
            expected_client_refs=accepted_client_refs,
            timeout_seconds=settings.callback_timeout_seconds,
            callback_errors=callback_errors,
            callback_error_lock=callback_error_lock,
        )
        if not sample_failed:
            cleanup_terminal_callback_complete, cleanup_terminal_inflight_remaining = (
                _wait_for_cleanup_terminal_callbacks(
                    runtimes=runtimes,
                    timeout_seconds=settings.callback_timeout_seconds,
                    callback_errors=callback_errors,
                    callback_error_lock=callback_error_lock,
                )
            )
        if not cleanup_terminal_callback_complete:
            cleanup_errors.append("CleanupTerminalCallbackIncomplete")
    except BaseException:
        # Do not leak vendor/error text through the parent queue.  The normal
        # child wrapper persists its existing bounded failure report; this
        # token only prevents the parent from mistaking failure for a hung
        # benchmark and deleting that report prematurely.
        try:
            control.acknowledgement_queue.put((control.scope_name, False))
        except BaseException:
            pass
        raise
    finally:
        producers_completed.set()
        for native_lane_thread in native_lane_threads:
            native_lane_thread.join(timeout=1.0)
        callback_stop.set()
        callback_lane_stop.set()
        for callback_thread in callback_threads:
            callback_thread.join(timeout=1.0)
        if gc_suspended:
            gc.enable()
        if not settings.isolate_native_process:
            for runtime in runtimes.values():
                if runtime.adapter is not None:
                    _stop_adapter(runtime.adapter, milestones)
                    runtime.adapter = None

    seen_refs, callback_gaps = observer.snapshot()
    callback_gap_reason_counts = _callback_gap_reason_counts(runtimes)
    verified_foreign_cancel_callback_count = _verified_foreign_cancel_callback_count(runtimes)
    with callback_error_lock:
        callback_error_kinds = list(callback_errors)
    with dispatch_error_lock:
        dispatch_error_kinds = list(dispatch_errors)
    samples = _collect_samples(plans, timing)
    report = _report(
        settings=settings,
        samples=samples,
        mmap_preparation_ns=mmap_preparation_ns,
        scheduler_snapshots={
            shard_id: runtime.scheduler.snapshot() for shard_id, runtime in runtimes.items()
        },
        producer_error_kinds=producer_errors,
        producer_rejection_kinds=producer_rejections,
        rejected_ingress_latencies_ns=rejected_ingress_latencies_ns,
        dispatch_error_kinds=dispatch_error_kinds,
        expired_before_native=expired_before_native[0],
        native_accepted=len(accepted),
        callback_completed=len(callback_completed_refs),
        callback_seen=len(seen_refs),
        callback_gaps=callback_gaps,
        callback_gap_reason_counts=callback_gap_reason_counts,
        verified_foreign_cancel_callback_count=verified_foreign_cancel_callback_count,
        foreign_cancel_directory_ready=(
            foreign_cancel_directory.ready if foreign_cancel_directory is not None else False
        ),
        callback_error_kinds=callback_error_kinds,
        cleanup_attempted=cleanup_attempted,
        cleanup_accepted=cleanup_accepted,
        cleanup_error_kinds=cleanup_errors,
        cleanup_cooldown_wait_ns=cleanup_action_pacing_wait_ns,
        cleanup_terminal_callback_complete=cleanup_terminal_callback_complete,
        cleanup_terminal_inflight_remaining=cleanup_terminal_inflight_remaining,
        source_worker_count=1,
        same_session_concurrent_inflight=None,
        runtime_telemetry=runtime_telemetry.report(
            runtime_telemetry_before, runtime_telemetry.capture()
        ),
        gc_control={
            "disabled_during_C0_to_C7": settings.disable_gc_during_measured_window,
            "was_enabled_before_window": gc_was_enabled,
        },
        mmap_preparation_by_shard=mmap_preparation_by_shard,
        route_preparation_by_shard=route_preparation_by_shard,
        tight_burst_order_plan_evidence=None,
        process_cpu_set=process_cpu_set,
        native_callback_transport=_native_callback_transport(runtimes),
    )
    report["hot_single_order_baseline"] = {
        "enabled": True,
        "samples": settings.hot_single_order_samples,
        "account_wide_submission_overlap": 0,
        "parent_release": "one release token per sample after all scopes report warm",
        "C0": "owning scope child immediately before scheduler admission",
        "inter_sample_action_interval_ms": _HOT_SINGLE_ORDER_ACTION_INTERVAL_SECONDS * 1_000,
        "per_sample_cleanup_post_submit_minimum_ms": (
            _EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS * 1_000
        ),
        "cleanup_action_pacing_wait_ms": cleanup_action_pacing_wait_ns / 1_000_000,
    }
    report["driver"] = "tradecore-mmap-emt-hot-single-order-c0-c7-child"
    report["execution_model"]["logical_simultaneous_signals"] = 1
    report["execution_model"]["producer_start"] = (
        "one parent release token; no next C0 until this sample has C7 and cleanup terminal proof"
    )
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    milestones.mark("hot_single_order_report_written")
    if settings.isolate_native_process:
        _exit_isolated_native_process(report)
    return report


def _allocate_client_order_ref_range(width: int) -> int:
    maximum_base = _MAX_SAFE_TEST_CLIENT_ORDER_REF - width + 1
    if maximum_base < _MIN_SAFE_TEST_CLIENT_ORDER_REF:
        raise TradeCoreMmapEmtPerfError("burst does not fit the safe test client reference range")
    return (
        secrets.randbelow(maximum_base - _MIN_SAFE_TEST_CLIENT_ORDER_REF + 1)
        + _MIN_SAFE_TEST_CLIENT_ORDER_REF
    )


def _split_limit(total: int, shard_ids: tuple[str, ...]) -> dict[str, int]:
    """Split one account-wide cap without silently exceeding it per session."""

    if total < 1 or not shard_ids or total < len(shard_ids):
        raise ValueError("session-shard capacity cannot give every session one slot")
    quotient, remainder = divmod(total, len(shard_ids))
    return {
        shard_id: quotient + (1 if index < remainder else 0)
        for index, shard_id in enumerate(shard_ids)
    }


def _allocate_burst_capacity(total: int, planned_counts: dict[str, int]) -> dict[str, int]:
    """Preserve one account cap while avoiding hash-skew false overloads.

    A burst's immutable execution identities are all known before C0.  If the
    requested account cap covers the burst, reserve each scope's exact planned
    count first, then distribute only the unused headroom.  This keeps the
    sum equal to the account cap and prevents an otherwise valid 300-signal
    run from rejecting a scope that received 108 stable-hash assignments.
    """

    if total < 1 or not planned_counts or any(count < 0 for count in planned_counts.values()):
        raise ValueError("session-shard burst capacity inputs are invalid")
    shard_ids = tuple(planned_counts)
    planned_total = sum(planned_counts.values())
    if total >= planned_total:
        capacities = dict(planned_counts)
        headroom = (
            _split_limit(total - planned_total, shard_ids)
            if total > planned_total
            else {shard_id: 0 for shard_id in shard_ids}
        )
        return {shard_id: capacities[shard_id] + headroom[shard_id] for shard_id in shard_ids}
    active_ids = tuple(shard_id for shard_id in shard_ids if planned_counts[shard_id] > 0)
    if not active_ids:
        raise ValueError("session-shard burst has no planned executions")
    capacities = {shard_id: 0 for shard_id in shard_ids}
    remaining = total
    while remaining:
        progressed = False
        for shard_id in active_ids:
            if remaining == 0:
                break
            if capacities[shard_id] < planned_counts[shard_id]:
                capacities[shard_id] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            raise ValueError("session-shard burst capacity cannot be allocated")
    return capacities


def _parse_session_client_ids(value: str) -> tuple[int, ...]:
    """Parse an explicit, non-secret ordered EMT client-scope list."""

    items = tuple(item.strip() for item in value.split(","))
    if not items or any(not item for item in items):
        raise argparse.ArgumentTypeError("session client scopes cannot be empty")
    try:
        parsed = tuple(int(item) for item in items)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "session client scopes must be comma-separated integers"
        ) from exc
    return parsed


def _parse_cpu_list(value: str) -> tuple[int, ...]:
    """Parse a non-empty ordered CPU list without probing host topology yet."""

    items = tuple(item.strip() for item in value.split(","))
    if not items or any(not item for item in items):
        raise argparse.ArgumentTypeError("worker CPU plan cannot be empty")
    try:
        cpus = tuple(int(item) for item in items)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "worker CPU plan must be comma-separated integers"
        ) from exc
    if any(cpu < 0 for cpu in cpus):
        raise argparse.ArgumentTypeError("worker CPU plan values must be non-negative")
    if len(set(cpus)) != len(cpus):
        raise argparse.ArgumentTypeError("worker CPU plan cannot repeat a CPU")
    return cpus


def _new_scheduler(
    *,
    width: int,
    ingress_capacity: int,
    native_start_budget_ns: int,
    native_inflight_limit: int,
    cancel_capacity: int,
    shard_id: str,
) -> ExecutionScheduler:
    policy_material = (
        "perf-burst-ingress-v4:"
        f"shard={shard_id}:new={ingress_capacity}:cancel={cancel_capacity}:inflight={native_inflight_limit}"
    )
    return ExecutionScheduler(
        account_id=_LOGICAL_ACCOUNT_ID,
        process_epoch_id=f"perf-burst-native-lane-v2:{shard_id}",
        policy=ExecutionSchedulerPolicy(
            policy_id="perf-burst-ingress-v4",
            version=4,
            policy_hash=sha256(policy_material.encode("ascii")).hexdigest(),
            new_order_capacity=ingress_capacity,
            cancel_capacity=cancel_capacity,
            new_order_high_watermark=ingress_capacity,
            cancel_high_watermark=cancel_capacity,
            max_new_order_bytes=1024,
            max_cancel_bytes=1024,
            maximum_inflight_new_orders=native_inflight_limit,
        ),
        execution_deadline_policy=ExecutionDeadlinePolicy(
            policy_id="perf-burst-native-start-v1",
            version=1,
            policy_hash=sha256(b"perf-burst-native-start-v1").hexdigest(),
            native_start_budget_ns=native_start_budget_ns,
        ),
        cancel_deadline_policy=CancelDeadlinePolicy(
            policy_id="perf-burst-cancel-start-v1",
            version=1,
            policy_hash=sha256(b"perf-burst-cancel-start-v1").hexdigest(),
            native_start_budget_ns=native_start_budget_ns,
        ),
    )


def _partition_plans(plans: list[_BurstPlan], worker_count: int) -> list[list[_BurstPlan]]:
    """Round-robin immutable signals across a bounded source-worker pool."""

    if worker_count < 1:
        raise ValueError("worker_count must be positive")
    if not plans:
        return []
    partitions = [[] for _ in range(min(worker_count, len(plans)))]
    for index, plan in enumerate(plans):
        partitions[index % len(partitions)].append(plan)
    return partitions


def _partition_plans_by_shard(
    plans: list[_BurstPlan], shard_ids: tuple[str, ...]
) -> list[list[_BurstPlan]]:
    """Build one bounded SPSC producer partition for every declared scope."""

    if not shard_ids or len(shard_ids) != len(set(shard_ids)):
        raise ValueError("session-shard producer topology is invalid")
    partitions = {shard_id: [] for shard_id in shard_ids}
    for plan in plans:
        try:
            partitions[plan.shard_id].append(plan)
        except KeyError as error:
            raise TradeCoreMmapEmtPerfError(
                "prebuilt burst plan is absent from the session-shard producer topology"
            ) from error
    return [partitions[shard_id] for shard_id in shard_ids]


def _native_lane_id(*, shard_id: str, execution_id: str, lane_count: int) -> str:
    """Select a stable per-session mmap projection lane without a hot-path map."""

    if not shard_id or not execution_id or lane_count < 1:
        raise ValueError("native lane routing inputs are invalid")
    lane_index = (
        int.from_bytes(sha256(f"{shard_id}:{execution_id}".encode("ascii")).digest()[:8], "big")
        % lane_count
    )
    return f"{shard_id}:native-{lane_index + 1}"


def _wait_for_callback_stages(
    *,
    timing: _BurstTiming,
    observer: _BurstObserver,
    expected_client_refs: set[str],
    timeout_seconds: float,
    callback_errors: list[str],
    callback_error_lock: Lock,
) -> set[str]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        completed = {
            client_ref
            for client_ref in expected_client_refs
            if "C7_AFTER_DOWNSTREAM" in timing.consume(f"callback:{client_ref}")
        }
        if completed == expected_client_refs:
            return completed
        with callback_error_lock:
            if callback_errors:
                return completed
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            return completed
        observer.wait_for_change(min(remaining_seconds, 0.25))


def _collect_samples(plans: list[_BurstPlan], timing: _BurstTiming) -> list[dict[str, int]]:
    samples: list[dict[str, int]] = []
    for test_index, burst_plan in enumerate(plans):
        stages = timing.consume(f"submit:{burst_plan.instruction.execution_id}")
        stages.update(timing.consume(f"callback:{burst_plan.route.command.client_order_ref}"))
        stages["test_index"] = test_index
        samples.append(stages)
    return samples


def _positive_venue_order_id(identity: Any) -> int:
    """Return one canonical external order ID without retaining its value in reports."""

    value = getattr(identity, "venue_order_id", None)
    if not isinstance(value, str) or not value.isdecimal():
        raise TradeCoreMmapEmtPerfError("accepted order has no canonical venue order identity")
    parsed = int(value)
    if parsed <= 0:
        raise TradeCoreMmapEmtPerfError("accepted order has an invalid venue order identity")
    return parsed


def _remaining_cleanup_cooldown_ns(*, latest_native_return_ns: int, now_ns: int) -> int:
    """Return the post-submit wait needed before one-shot cleanup cancels."""

    if latest_native_return_ns <= 0 or now_ns <= latest_native_return_ns:
        return int(_EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS * 1_000_000_000)
    elapsed_ns = now_ns - latest_native_return_ns
    required_ns = int(_EMT_POST_SUBMIT_CLEANUP_COOLDOWN_SECONDS * 1_000_000_000)
    return max(0, required_ns - elapsed_ns)


def _callback_gap_reason_counts(
    runtimes: dict[str, _SessionShardRuntime],
) -> dict[str, int]:
    """Aggregate payload-free malformed-callback reasons before session close."""

    counts: dict[str, int] = {}
    for runtime in runtimes.values():
        adapter = runtime.adapter
        if adapter is None:
            continue
        for reason, count in adapter.snapshot().callback_gap_reason_counts.items():
            if not isinstance(count, int) or count < 1:
                raise TradeCoreMmapEmtPerfError("callback gap reason count is invalid")
            counts[reason] = counts.get(reason, 0) + count
    return counts


def _verified_foreign_cancel_callback_count(
    runtimes: dict[str, _SessionShardRuntime],
) -> int:
    """Return static-plan foreign cancellation fan-out seen before session close."""

    total = 0
    for runtime in runtimes.values():
        adapter = runtime.adapter
        if adapter is None:
            continue
        count = adapter.snapshot().verified_foreign_cancel_callback_count
        if not isinstance(count, int) or count < 0:
            raise TradeCoreMmapEmtPerfError("verified foreign cancel callback count is invalid")
        total += count
    return total


def _wait_for_cleanup_terminal_callbacks(
    *,
    runtimes: dict[str, _SessionShardRuntime],
    timeout_seconds: float,
    callback_errors: list[str],
    callback_error_lock: Lock,
) -> tuple[bool, int]:
    """Wait outside C0--C7 until every accepted order releases its credit.

    A successful ``CancelOrder`` invocation is not terminal evidence.  The
    scheduler releases a new-order credit only after its durable terminal
    callback path completes, which lets the benchmark prove cleanup before it
    tears down the native session.
    """

    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = sum(
            runtime.scheduler.snapshot().depths["new_order_inflight"]
            for runtime in runtimes.values()
        )
        if remaining == 0:
            return True, 0
        with callback_error_lock:
            if callback_errors:
                return False, remaining
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            return False, remaining
        time.sleep(min(0.01, remaining_seconds))


def _native_callback_transport(
    runtimes: dict[str, _SessionShardRuntime],
) -> dict[str, int | bool]:
    """Return P0-only bridge-entry counts without retaining callback data."""

    counters = [
        runtime.native_callback_transport
        for runtime in runtimes.values()
        if runtime.native_callback_transport is not None
    ]
    if not counters:
        return {
            "enabled": False,
            "order_event_entries": 0,
            "trade_event_entries": 0,
            "cancel_error_entries": 0,
        }
    if len(counters) != len(runtimes):
        raise TradeCoreMmapEmtPerfError(
            "native callback diagnostics must be configured for every session shard"
        )
    snapshots = [counter.snapshot() for counter in counters]
    return {
        "enabled": True,
        "order_event_entries": sum(int(snapshot["order_event_entries"]) for snapshot in snapshots),
        "trade_event_entries": sum(int(snapshot["trade_event_entries"]) for snapshot in snapshots),
        "cancel_error_entries": sum(
            int(snapshot["cancel_error_entries"]) for snapshot in snapshots
        ),
    }


def _reported_native_callback_transport(outcomes: dict[str, Any]) -> dict[str, int | bool]:
    """Read a backward-compatible, payload-free child transport summary."""

    disabled = {
        "enabled": False,
        "order_event_entries": 0,
        "trade_event_entries": 0,
        "cancel_error_entries": 0,
    }
    raw = outcomes.get("native_callback_transport")
    if raw is None:
        return disabled
    if not isinstance(raw, dict) or type(raw.get("enabled")) is not bool:
        raise TradeCoreMmapEmtPerfError("native callback transport report is invalid")
    counts: dict[str, int | bool] = {"enabled": raw["enabled"]}
    for name in ("order_event_entries", "trade_event_entries", "cancel_error_entries"):
        value = raw.get(name)
        if type(value) is not int or value < 0:
            raise TradeCoreMmapEmtPerfError("native callback transport count is invalid")
        counts[name] = value
    return counts


def _aggregate_native_callback_transport(
    reports: list[dict[str, Any]],
) -> dict[str, int | bool]:
    transports = [_reported_native_callback_transport(report["outcomes"]) for report in reports]
    enabled = [transport for transport in transports if bool(transport["enabled"])]
    if not enabled:
        return {
            "enabled": False,
            "order_event_entries": 0,
            "trade_event_entries": 0,
            "cancel_error_entries": 0,
        }
    if len(enabled) != len(transports):
        raise TradeCoreMmapEmtPerfError(
            "native callback diagnostics differ across process-isolated session reports"
        )
    return {
        "enabled": True,
        "order_event_entries": sum(int(transport["order_event_entries"]) for transport in enabled),
        "trade_event_entries": sum(int(transport["trade_event_entries"]) for transport in enabled),
        "cancel_error_entries": sum(
            int(transport["cancel_error_entries"]) for transport in enabled
        ),
    }


def _callback_origin_classification(
    coverage: dict[str, Any], native_callback_transport: dict[str, int | bool]
) -> str:
    """Classify a missing C5 using optional pre-extraction bridge evidence."""

    if not bool(native_callback_transport["enabled"]):
        return "NOT_COLLECTED"
    if int(coverage["stage_counts"]["C5_CALLBACK_INGRESS"]) > 0:
        return "C5_REACHED"
    native_entries = sum(
        int(native_callback_transport[name])
        for name in ("order_event_entries", "trade_event_entries", "cancel_error_entries")
    )
    if native_entries == 0:
        return "NO_NATIVE_ORDER_SIDE_CALLBACK_TO_PYTHON_BRIDGE"
    return "NATIVE_CALLBACK_NOT_DELIVERED_TO_C5"


def _report(
    *,
    settings: Settings,
    samples: list[dict[str, int]],
    mmap_preparation_ns: int,
    scheduler_snapshots: dict[str, Any],
    producer_error_kinds: list[str],
    producer_rejection_kinds: list[str],
    rejected_ingress_latencies_ns: list[int],
    dispatch_error_kinds: list[str],
    expired_before_native: int,
    native_accepted: int,
    callback_completed: int,
    callback_seen: int,
    callback_gaps: int,
    callback_gap_reason_counts: dict[str, int],
    verified_foreign_cancel_callback_count: int,
    foreign_cancel_directory_ready: bool,
    callback_error_kinds: list[str],
    cleanup_attempted: int,
    cleanup_accepted: int,
    cleanup_error_kinds: list[str],
    cleanup_cooldown_wait_ns: int,
    cleanup_terminal_callback_complete: bool,
    cleanup_terminal_inflight_remaining: int,
    source_worker_count: int,
    same_session_concurrent_inflight: dict[str, int] | None,
    runtime_telemetry: dict[str, Any],
    gc_control: dict[str, bool],
    mmap_preparation_by_shard: dict[str, dict[str, int | bool | str]],
    route_preparation_by_shard: dict[str, dict[str, int | bool | str]],
    tight_burst_order_plan_evidence: dict[str, str | bool] | None,
    process_cpu_set: tuple[int, ...] | None,
    native_callback_transport: dict[str, int | bool],
) -> dict[str, Any]:
    hot_single_order = settings.hot_single_order_samples > 0
    scheduler_rollup = _scheduler_rollup(scheduler_snapshots)
    segments = (
        ("C0_INGRESS", "C0A_AFTER_INGRESS_DECISION"),
        ("C0P_BEFORE_TIGHT_SUBMIT", "C0A_AFTER_INGRESS_DECISION"),
        ("C0A_AFTER_INGRESS_DECISION", "C0B_AFTER_NATIVE_DEQUEUE"),
        ("C0B_AFTER_NATIVE_DEQUEUE", "C0C_BEFORE_NATIVE_DISPATCH"),
        ("C0C_BEFORE_NATIVE_DISPATCH", "C1_BEFORE_ADAPTER"),
        ("C0A_AFTER_INGRESS_DECISION", "C1_BEFORE_ADAPTER"),
        ("C0_INGRESS", "C1_BEFORE_ADAPTER"),
        ("C1_BEFORE_ADAPTER", "C1B_AFTER_ROUTE_RESOLUTION"),
        ("C1B_AFTER_ROUTE_RESOLUTION", "C1C_AFTER_LOCAL_CALLBACK_REGISTRATION"),
        ("C1C_AFTER_LOCAL_CALLBACK_REGISTRATION", "C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT"),
        ("C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT", "C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT", "C2_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C1_BEFORE_ADAPTER", "C2_AFTER_PRE_NATIVE_MMAP_COMMIT"),
        ("C2_AFTER_PRE_NATIVE_MMAP_COMMIT", "C3_BEFORE_SDK"),
        ("C3_BEFORE_SDK", "C4_AFTER_SDK"),
        ("C4_AFTER_SDK", "C4A_AFTER_ACCEPTED_JOURNAL"),
        ("C4_AFTER_SDK", "C5_CALLBACK_INGRESS"),
        ("C5_CALLBACK_INGRESS", "C5D_PYTHON_DRAIN_BEGIN"),
        ("C5D_PYTHON_DRAIN_BEGIN", "C5E_AFTER_SEALED_ROUTE"),
        ("C5E_AFTER_SEALED_ROUTE", "C5A_AFTER_SHARD_ENQUEUE"),
        ("C5_CALLBACK_INGRESS", "C5A_AFTER_SHARD_ENQUEUE"),
        ("C5A_AFTER_SHARD_ENQUEUE", "C5B_TARGET_WORKER_START"),
        ("C5B_TARGET_WORKER_START", "C5B2_AFTER_ROUTE_STATE_VALIDATION"),
        ("C5B2_AFTER_ROUTE_STATE_VALIDATION", "C5B3_AFTER_CALLBACK_MMAP_HASH"),
        ("C5B3_AFTER_CALLBACK_MMAP_HASH", "C5M0_BEFORE_CALLBACK_MMAP_COMMIT"),
        ("C5M0_BEFORE_CALLBACK_MMAP_COMMIT", "C5M1_AFTER_CALLBACK_MMAP_COMMIT"),
        ("C5M1_AFTER_CALLBACK_MMAP_COMMIT", "C6_AFTER_CALLBACK_MMAP_COMMIT"),
        ("C6_AFTER_CALLBACK_MMAP_COMMIT", "C7_AFTER_DOWNSTREAM"),
    )
    segment_values = {
        f"{left}->{right}": _durations(samples, left, right) for left, right in segments
    }
    full_path = {
        "segments_ns": {name: _summary(values) for name, values in segment_values.items()},
        "batch_completion": {
            "C0_to_last_C4_after_sdk": _batch_completion_summary(samples, "C4_AFTER_SDK"),
            "C0_to_last_C4A_after_accepted_journal": _batch_completion_summary(
                samples, "C4A_AFTER_ACCEPTED_JOURNAL"
            ),
            "C0_to_last_C7_after_downstream": _batch_completion_summary(
                samples, "C7_AFTER_DOWNSTREAM"
            ),
            "first_C5_to_last_C7_after_downstream": _batch_first_to_last_summary(
                samples,
                start_stage="C5_CALLBACK_INGRESS",
                completion_stage="C7_AFTER_DOWNSTREAM",
            ),
            "first_C0P_to_last_C4_after_sdk": _batch_first_to_last_summary(
                samples,
                start_stage="C0P_BEFORE_TIGHT_SUBMIT",
                completion_stage="C4_AFTER_SDK",
            ),
            "first_C0P_to_last_C7_after_downstream": _batch_first_to_last_summary(
                samples,
                start_stage="C0P_BEFORE_TIGHT_SUBMIT",
                completion_stage="C7_AFTER_DOWNSTREAM",
            ),
        },
        "ingress_decision_C0_to_C0A_all_signals_ns": _summary(
            _durations(samples, "C0_INGRESS", "C0A_AFTER_INGRESS_DECISION")
        ),
        "ingress_decision_C0_to_C0A_admitted_ns": _summary(
            [
                stages["C0A_AFTER_INGRESS_DECISION"] - stages["C0_INGRESS"]
                for stages in samples
                if "C0_INGRESS" in stages
                and "C0A_AFTER_INGRESS_DECISION" in stages
                and "C1_BEFORE_ADAPTER" in stages
                and stages["C0A_AFTER_INGRESS_DECISION"] >= stages["C0_INGRESS"]
            ]
        ),
        "ingress_decision_C0_to_C0A_rejected_before_C1_ns": _summary(
            [
                stages["C0A_AFTER_INGRESS_DECISION"] - stages["C0_INGRESS"]
                for stages in samples
                if "C0_INGRESS" in stages
                and "C0A_AFTER_INGRESS_DECISION" in stages
                and "C1_BEFORE_ADAPTER" not in stages
                and stages["C0A_AFTER_INGRESS_DECISION"] >= stages["C0_INGRESS"]
            ]
        ),
        "tight_submit_C0P_to_C4_ns": _summary(
            _durations(samples, "C0P_BEFORE_TIGHT_SUBMIT", "C4_AFTER_SDK")
        ),
        "tight_submit_C0P_to_C7_ns": _summary(
            _durations(samples, "C0P_BEFORE_TIGHT_SUBMIT", "C7_AFTER_DOWNSTREAM")
        ),
        "project_internal_submit_C0_to_C4_ns": _summary(
            _durations(samples, "C0_INGRESS", "C4_AFTER_SDK")
        ),
        "project_internal_submit_C0_to_C4A_ns": _summary(
            _durations(samples, "C0_INGRESS", "C4A_AFTER_ACCEPTED_JOURNAL")
        ),
        "emt_callback_wait_C4_to_C5_ns": _summary(
            _durations(samples, "C4_AFTER_SDK", "C5_CALLBACK_INGRESS")
        ),
        "project_internal_callback_C5_to_C7_ns": _summary(
            _durations(samples, "C5_CALLBACK_INGRESS", "C7_AFTER_DOWNSTREAM")
        ),
        "end_to_end_C0_to_C7_ns": _summary(
            _durations(samples, "C0_INGRESS", "C7_AFTER_DOWNSTREAM")
        ),
    }
    callback_timing_pair_count = full_path["project_internal_callback_C5_to_C7_ns"]["count"]
    callback_timing_complete = callback_timing_pair_count == settings.burst_width
    callback_substage_measurement = _callback_substage_measurement(samples)
    callback_stage_coverage = {
        **_callback_stage_coverage(samples),
    }
    callback_stage_coverage["origin_classification"] = _callback_origin_classification(
        callback_stage_coverage, native_callback_transport
    )
    full_acceptance_complete = (
        not producer_error_kinds
        and not producer_rejection_kinds
        and not dispatch_error_kinds
        and expired_before_native == 0
        and native_accepted == settings.burst_width
        and callback_completed == settings.burst_width
        and callback_timing_complete
        and callback_substage_measurement["complete"]
        and callback_stage_coverage["complete"]
        and callback_gaps == 0
        and not callback_error_kinds
        and (not settings._partitioned_scope_worker or foreign_cancel_directory_ready)
        and cleanup_attempted == native_accepted
        and cleanup_accepted == native_accepted
        and not cleanup_error_kinds
        and cleanup_terminal_callback_complete
    )
    c0_to_c1 = full_path["segments_ns"]["C0_INGRESS->C1_BEFORE_ADAPTER"]
    protected_overload_complete = _protected_overload_complete(
        settings=settings,
        scheduler_counters=scheduler_rollup["counters"],
        producer_error_kinds=producer_error_kinds,
        producer_rejection_kinds=producer_rejection_kinds,
        dispatch_error_kinds=dispatch_error_kinds,
        expired_before_native=expired_before_native,
        native_accepted=native_accepted,
        callback_completed=callback_completed,
        callback_seen=callback_seen,
        callback_gaps=callback_gaps,
        callback_error_kinds=callback_error_kinds,
        cleanup_attempted=cleanup_attempted,
        cleanup_accepted=cleanup_accepted,
        cleanup_error_kinds=cleanup_error_kinds,
        c0_to_c1_sample_count=c0_to_c1["count"],
    )
    return {
        "schema_version": 3,
        "driver": (
            "tradecore-mmap-emt-hot-single-order-c0-c7"
            if hot_single_order
            else "tradecore-mmap-emt-burst-c0-c7"
        ),
        "executed": True,
        "complete": full_acceptance_complete,
        "protected_overload_complete": protected_overload_complete,
        "burst_width": settings.burst_width,
        "percentile_method": "nearest-rank: rank(q)=ceil(q*n), P100=max",
        "persistence_profile": {
            "name": "MmapJournalProfileV1",
            "C2": "committed local PRE_NATIVE mmap frame",
            "C6": "committed local callback mmap frame and local state proof",
            "callback_persistence_profile": settings.callback_persistence_profile.value,
            "callback_handoff": (
                "scope-local callback drain worker direct durable write; no target SPSC handoff"
                if settings.callback_persistence_profile
                is CallbackPersistenceProfileV1.DIRECT_CANDIDATE
                else "source drain worker to target scope SPSC lane, then target Journal worker"
            ),
            "network_datastore_in_C1_to_C2_C5_to_C7": False,
            "preparation_ns": mmap_preparation_ns,
            "mmap_preparation": mmap_preparation_by_shard,
        },
        "runtime_attribution": runtime_telemetry,
        "runtime_control": {"gc": gc_control},
        "measurement_boundaries": {
            "C0P": (
                "per-order entry to the tight single-producer loop immediately before "
                "TradeCore ingress admission; present only in tight-single-producer mode"
            ),
            "C0A_to_C0B": (
                "admitted ingress to native-worker scheduler dequeue; includes queue wait and "
                "the time until the worker is scheduled to execute Python"
            ),
            "C0B_to_C0C": (
                "post-dequeue plan lookup, native-lane selection and local context preparation"
            ),
            "C0C_to_C1": (
                "prepared-route proof before TradeCore dispatcher telemetry marks adapter entry"
            ),
            "C5_to_C5D": (
                "SDK callback ingress timestamp to first Python callback-observer instruction"
            ),
            "C5D_to_C5E": (
                "sealed callback ownership lookup and rebound target-event construction"
            ),
            "C5E_to_C5A": (
                "scope-local direct callback handoff after sealed route; no SPSC publication"
                if settings.callback_persistence_profile
                is CallbackPersistenceProfileV1.DIRECT_CANDIDATE
                else "target SPSC lane lookup and callback publication"
            ),
            "C7": (
                "TradeCore downstream receipt after callback durability; it is not represented "
                "as an external Strategy on_order callback"
            ),
        },
        "execution_model": {
            "logical_simultaneous_signals": 1 if hot_single_order else settings.burst_width,
            "source_worker_threads": source_worker_count,
            "submission_topology": settings.submission_topology,
            "producer_start": (
                "one parent release token starts one already-warmed sample; the next token waits for C7 and cleanup terminal proof"
                if hot_single_order
                else "one producer executes all 499 prebuilt signals in one tight, unbatched loop after shared C0"
                if settings.submission_topology == "tight-single-producer"
                else "one shared C0 release and pre-admitted C0A timestamp before direct same-session callers dispatch"
                if settings.same_session_insert_callers > 1
                else "one shared C0 release timestamp after one SPSC producer per session scope is ready"
            ),
            "native_dispatch": (
                "EXPERIMENTAL: one EMT API/session receives direct concurrent InsertOrder callers; "
                "it is a vendor-capability probe, not the default dispatch topology"
                if settings.same_session_insert_callers > 1
                else "one serial EMT API/session caller with sharded mmap and callback "
                "lanes selected by stable execution-key affinity"
                if settings.mmap_native_lanes > 1
                else "single EMT session lane concurrent with bounded ingress"
                if len(settings.session_client_ids) == 1
                else "one dedicated EMT native session lane per attested scope, each concurrent with bounded ingress"
            ),
            "submit_route_preparation": {
                "prepared_before_C0": True,
                "sealed_lock_free_lookup_at_C1": True,
                "per_session_shard": route_preparation_by_shard,
            },
            "pre_native_fence": "local single-writer mmap process-crash fence",
            "direct_concurrent_native_calls": settings.same_session_insert_callers > 1,
            "same_session_insert_callers": settings.same_session_insert_callers,
            "mmap_native_lane_count": settings.mmap_native_lanes,
            "mmap_projection_lane_count": settings.mmap_native_lanes,
            "native_lane_routing": (
                "SHA-256(shard_id + execution_id) modulo lane count; one serial native worker "
                "binds the selected mmap Journal and its paired callback lane"
                if settings.mmap_native_lanes > 1
                else "one native lane per session scope"
            ),
            "attested_session_shard_count": len(settings.session_client_ids),
            "new_order_ingress_capacity": scheduler_rollup["capacities"]["new_order"],
            "maximum_native_inflight_orders": scheduler_rollup["capacities"]["new_order_inflight"],
            "overload_disposition": "reject before C1 with INGRESS_OVERLOADED",
            "event_ingress_capacity": max(8_192, settings.burst_width * 4),
            "native_start_budget_seconds": settings.native_start_budget_seconds,
            "callback_pipeline": {
                "backlog_threshold": _CALLBACK_PIPELINE_BACKLOG,
                "max_items": settings.callback_pipeline_max_items,
                "selection": (
                    "not used by direct candidate; the drain worker is the sole callback Journal writer"
                    if settings.callback_persistence_profile
                    is CallbackPersistenceProfileV1.DIRECT_CANDIDATE
                    else "fixed candidate; compare P95 across controlled 4/8/12/16 runs"
                ),
            },
            "callback_wakeup": {
                "mode": settings.callback_wakeup_mode,
                "selection": (
                    "Linux eventfd is an explicit A/B candidate; threading.Event remains "
                    "the portable baseline"
                ),
            },
            "worker_cpu_affinity": {
                "native_worker_cpu": settings.native_worker_cpu,
                "ingress_producer_cpu": settings.ingress_producer_cpu,
                "callback_ingress_cpu": settings.callback_ingress_cpu,
                "callback_journal_cpu": settings.callback_journal_cpu,
                "sdk_receive_cpu": settings.sdk_receive_cpu,
                "sdk_process_cpu": settings.sdk_process_cpu,
                "sdk_set_cpu_affinity_requested": settings.sdk_receive_cpu is not None,
                "process_cpu_set": list(process_cpu_set) if process_cpu_set is not None else None,
                "process_cpu_set_applied_before_sdk_load": process_cpu_set is not None,
                "application": (
                    "forked scope process CPU set applied before EMT SDK load; "
                    "Python role workers and EMT receive/process threads use reserved CPUs within that set"
                    if process_cpu_set is not None
                    else "explicit Linux thread affinity only"
                ),
            },
        },
        "tight_burst_preflight": tight_burst_order_plan_evidence,
        "hot_single_order_baseline": {
            "enabled": hot_single_order,
            "samples": settings.hot_single_order_samples if hot_single_order else 0,
            "account_wide_submission_overlap": 0 if hot_single_order else None,
        },
        "full_path": full_path,
        # The rows contain only stage timestamps—never execution IDs, account
        # values, order references or request payload.  Process-isolated P0
        # workers use them to recompute one exact 499-sample percentile set
        # rather than averaging per-process percentiles.
        "payload_free_timing_samples_ns": samples,
        "callback_note": (
            "C4->C5 is the asynchronous EMT callback wait and is not included in either "
            "project-internal total. C0->C7 is measured directly because C4A Journal work "
            "and an asynchronous callback can overlap; C5->C5D->C5E->C5A->C5B separates "
            "Python drain scheduling, sealed-route work, and either SPSC publication/target-lane "
            "queueing (control) or direct scope-local Journal handoff (direct candidate)."
        ),
        "outcomes": {
            "signals_target": settings.burst_width,
            "scheduler": {
                "max_observed_new_order_depth": scheduler_rollup["max_observed_depths"][
                    "new_order"
                ],
                "max_observed_new_order_inflight": scheduler_rollup["max_observed_depths"][
                    "new_order_inflight"
                ],
                "new_order_inflight_at_report": scheduler_rollup["depths"]["new_order_inflight"],
                "new_order_counters": dict(scheduler_rollup["counters"]),
                "per_session_shard": scheduler_rollup["per_session_shard"],
            },
            "same_session_concurrent_inflight": same_session_concurrent_inflight,
            "producer_error_kinds": _kind_counts(producer_error_kinds),
            "producer_rejection_kinds": _kind_counts(producer_rejection_kinds),
            "rejected_before_C1_ingress_latency_ns": _summary(rejected_ingress_latencies_ns),
            "expired_before_native": expired_before_native,
            "native_accepted": native_accepted,
            "dispatch_error_kinds": _kind_counts(dispatch_error_kinds),
            "callback_completed_C7": callback_completed,
            "callback_seen_downstream": callback_seen,
            "callback_complete_C5_to_C7_pairs": callback_timing_pair_count,
            "callback_substage_measurement": callback_substage_measurement,
            "callback_stage_coverage": callback_stage_coverage,
            "native_callback_transport": native_callback_transport,
            "callback_gaps": callback_gaps,
            "callback_gap_reason_counts": dict(sorted(callback_gap_reason_counts.items())),
            "verified_foreign_cancel_callback_count": verified_foreign_cancel_callback_count,
            "foreign_cancel_directory_ready": foreign_cancel_directory_ready,
            "callback_error_kinds": _kind_counts(callback_error_kinds),
            "cleanup_cancel_attempted": cleanup_attempted,
            "cleanup_cancel_accepted": cleanup_accepted,
            "cleanup_error_kinds": _kind_counts(cleanup_error_kinds),
            "cleanup_terminal_callback_complete": cleanup_terminal_callback_complete,
            "cleanup_terminal_inflight_remaining": cleanup_terminal_inflight_remaining,
            "cleanup_post_submit_cooldown_ms": cleanup_cooldown_wait_ns / 1_000_000,
            "cleanup_cancel_interval_ms": settings.cleanup_cancel_interval_seconds * 1_000,
        },
        "safety": {
            "simulated_only": True,
            "automatic_retry": False,
            "overload": "signals rejected at ingress do not enter the mmap Journal or EMT path",
            "cleanup": "one bounded serial cancel attempt for every native-accepted order",
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "verification": {
            "full_acceptance_complete": full_acceptance_complete,
            "protected_overload_complete": protected_overload_complete,
            "accepted": full_acceptance_complete or protected_overload_complete,
            "protected_overload_definition": (
                "all signals are accounted for; excess signals are only rejected as "
                "INGRESS_OVERLOADED before mmap Journal/EMT by queue or native-inflight "
                "capacity; every admitted signal reaches "
                "C7 and receives one accepted cleanup cancellation"
            ),
        },
        "output_name": settings.output.name,
    }


def _scheduler_rollup(snapshots: dict[str, Any]) -> dict[str, Any]:
    """Return payload-free account totals plus independently inspectable lanes."""

    if not snapshots:
        raise ValueError("session-shard scheduler snapshots are empty")
    mapping_fields = ("capacities", "depths", "max_observed_depths", "counters")
    result: dict[str, Any] = {field: {} for field in mapping_fields}
    per_session: dict[str, Any] = {}
    for shard_id, snapshot in snapshots.items():
        per_session[shard_id] = {
            "capacities": dict(snapshot.capacities),
            "depths": dict(snapshot.depths),
            "max_observed_depths": dict(snapshot.max_observed_depths),
            "counters": dict(snapshot.counters),
        }
        for field in mapping_fields:
            combined = result[field]
            for key, value in getattr(snapshot, field).items():
                combined[key] = combined.get(key, 0) + value
    result["per_session_shard"] = per_session
    return result


def _protected_overload_complete(
    *,
    settings: Settings,
    scheduler_counters: Any,
    producer_error_kinds: list[str],
    producer_rejection_kinds: list[str],
    dispatch_error_kinds: list[str],
    expired_before_native: int,
    native_accepted: int,
    callback_completed: int,
    callback_seen: int,
    callback_gaps: int,
    callback_error_kinds: list[str],
    cleanup_attempted: int,
    cleanup_accepted: int,
    cleanup_error_kinds: list[str],
    c0_to_c1_sample_count: int,
) -> bool:
    """Recognize a deliberately bounded, fail-closed overload run.

    This is deliberately distinct from full acceptance.  It never converts a
    native throttle or a missing callback/cleanup into a passing result.
    """

    scheduler_accepted = scheduler_counters.get("new_order_accepted", 0)
    return (
        min(settings.ingress_capacity, settings.native_inflight_limit) < settings.burst_width
        and not producer_error_kinds
        and bool(producer_rejection_kinds)
        and set(producer_rejection_kinds) == {_OVERLOAD_REJECTION_CODE}
        and scheduler_accepted + len(producer_rejection_kinds) == settings.burst_width
        and not dispatch_error_kinds
        and expired_before_native == 0
        and scheduler_accepted == native_accepted
        and c0_to_c1_sample_count == native_accepted
        and callback_completed == native_accepted
        and callback_seen == native_accepted
        and callback_gaps == 0
        and not callback_error_kinds
        and cleanup_attempted == native_accepted
        and cleanup_accepted == native_accepted
        and not cleanup_error_kinds
    )


def _durations(samples: list[dict[str, int]], left: str, right: str) -> list[int]:
    values: list[int] = []
    for stages in samples:
        if left not in stages or right not in stages:
            continue
        duration = stages[right] - stages[left]
        if duration >= 0:
            values.append(duration)
    return values


def _callback_substage_measurement(samples: list[dict[str, int]]) -> dict[str, Any]:
    """Require one monotonic timing pair for every callback substage.

    C5→C7 alone can remain present while a diagnostic substage is absent. A
    performance result must not label such a run complete because it cannot
    truthfully attribute the callback tail to its required boundaries.
    """

    target = len(samples)
    pair_counts = {
        f"{left}->{right}": len(_durations(samples, left, right))
        for left, right in zip(
            _CALLBACK_MEASUREMENT_STAGES,
            _CALLBACK_MEASUREMENT_STAGES[1:],
        )
    }
    return {
        "signals_target": target,
        "adjacent_pair_counts": pair_counts,
        "complete": bool(samples) and all(count == target for count in pair_counts.values()),
    }


def _callback_stage_coverage(samples: list[dict[str, int]]) -> dict[str, Any]:
    """Classify the first missing callback boundary without retaining payload.

    A missing C7 is not sufficiently actionable by itself: no matching broker
    callback, a Python drain failure, a sealed-route failure and a callback
    Journal delivery failure have different owners and different safe recovery
    actions.  The burst harness therefore records only fixed stage names and
    counts.  It deliberately does not copy native error text, client IDs or
    order references into the report.
    """

    target = len(samples)
    stage_counts = {
        stage: sum(1 for sample in samples if stage in sample)
        for stage in _CALLBACK_MEASUREMENT_STAGES
    }
    first_incomplete_stage = next(
        (stage for stage in _CALLBACK_MEASUREMENT_STAGES if stage_counts[stage] != target),
        None,
    )
    if not samples:
        failure_classification = "NO_CALLBACK_SAMPLES"
    elif first_incomplete_stage is None:
        failure_classification = "COMPLETE"
    elif first_incomplete_stage == "C5_CALLBACK_INGRESS":
        # C5 is recorded immediately when the benchmark's broker-event
        # observer receives a callback with a pre-registered client reference.
        # It does not claim why EMT/SDK did not supply such a callback.
        failure_classification = "NO_MATCHED_CALLBACK_TO_C5"
    else:
        failure_classification = "CALLBACK_PIPELINE_INCOMPLETE_BEFORE_" + first_incomplete_stage
    return {
        "signals_target": target,
        "stage_counts": stage_counts,
        "first_incomplete_stage": first_incomplete_stage,
        "failure_classification": failure_classification,
        "complete": bool(samples) and first_incomplete_stage is None,
    }


def _summary(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {
            "count": 0,
            "p50_ns": None,
            "p95_ns": None,
            "p99_ns": None,
            "p100_ns": None,
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ns": _nearest_rank(ordered, 0.50),
        "p95_ns": _nearest_rank(ordered, 0.95),
        "p99_ns": _nearest_rank(ordered, 0.99),
        "p100_ns": ordered[-1],
    }


def _nearest_rank(ordered: list[int], percentile: float) -> int:
    rank = max(1, ceil(len(ordered) * percentile))
    return ordered[rank - 1]


def _kind_counts(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _failure_signature(error: BaseException) -> str:
    """Keep a failure diagnostic useful without putting payloads in the report.

    A Journal-gate failure is wrapped before it reaches the scheduler.  Retain
    only the bounded exception-type chain and documented numeric/Journal code,
    or a fixed allow-listed callback-state code; never arbitrary exception
    text, account data or native payload.
    """

    chain: list[str] = []
    current: BaseException | None = error
    code = ""
    while current is not None and len(chain) < 4:
        chain.append(type(current).__name__)
        message = str(current)
        if not code:
            numeric = re.search(r"error_id=(\d+)", message)
            journal = re.search(r"\b(JOURNAL_[A-Z_]+)\b", message)
            if numeric is not None:
                code = f":error_id={numeric.group(1)}"
            elif journal is not None:
                code = f":{journal.group(1)}"
            elif message in _SAFE_CALLBACK_OUTCOME_CODES:
                code = f":{_SAFE_CALLBACK_OUTCOME_CODES[message]}"
        next_error = current.__cause__ or current.__context__
        if next_error is current:
            break
        current = next_error
    return ":".join(chain) + code


def _load_expected_library_hashes(path: Path) -> dict[str, str]:
    """Read the non-secret loader allowlist carried into isolated SDK workers."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TradeCoreMmapEmtPerfError("SDK hash manifest is unreadable") from exc
    if not isinstance(raw, dict) or not raw:
        raise TradeCoreMmapEmtPerfError("SDK hash manifest is empty")
    hashes: dict[str, str] = {}
    for name, digest in raw.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise TradeCoreMmapEmtPerfError("SDK hash manifest entry is invalid")
        hashes[name] = digest
    return hashes


def _last_submit_stage(timing: _BurstTiming, instruction: Any) -> str:
    """Return one payload-free failure boundary for an unsuccessful dispatch."""

    stages = timing.consume(f"submit:{instruction.execution_id}")
    if not stages:
        return "NO_STAGE"
    return max(stages.items(), key=lambda item: item[1])[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run real TradeCore mmap/EMT simulation burst")
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument(
        "--expected-library-hashes",
        type=Path,
        help="reviewed non-secret SDK hash allowlist required for F2/F4 acceptance",
    )
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument(
        "--burst-width",
        type=int,
        required=True,
        help="300 or 499 for a burst; equal --hot-single-order-samples for the serial baseline",
    )
    parser.add_argument(
        "--hot-single-order-samples",
        type=int,
        default=0,
        help=(
            "run 100..499 globally serial, fully warmed samples; requires --burst-width "
            "to equal this value and process-isolated session scope(s)"
        ),
    )
    parser.add_argument("--callback-timeout", type=float, default=120.0)
    parser.add_argument("--native-start-budget", type=float, default=180.0)
    parser.add_argument(
        "--ingress-capacity",
        type=int,
        default=_DEFAULT_INGRESS_CAPACITY,
        help=(
            "bounded signals admitted to the single native lane (default: 1); "
            "pass burst width only for a historical full-ingress baseline"
        ),
    )
    parser.add_argument(
        "--ingress-workers",
        type=int,
        default=_DEFAULT_INGRESS_WORKERS,
        help=f"fixed raw-signal worker count (default: {_DEFAULT_INGRESS_WORKERS})",
    )
    parser.add_argument(
        "--submission-topology",
        choices=_SUBMISSION_TOPOLOGIES,
        default="concurrent-sharded",
        help=(
            "concurrent-sharded preserves the existing scope topology; tight-single-producer "
            "uses one serial source loop for the entire 499-signal burst"
        ),
    )
    parser.add_argument(
        "--tight-burst-order-plan",
        type=Path,
        help=(
            "external no-credential market-preflight plan required by tight-single-producer; "
            "its values are never copied into the result"
        ),
    )
    parser.add_argument(
        "--native-inflight-limit",
        type=int,
        default=_MAX_NATIVE_INFLIGHT_ORDERS,
        help=(
            "maximum admitted new orders retained until no-native proof or a durable "
            "terminal callback (default: 1000; separate from the 499 signal/s burst ceiling)"
        ),
    )
    parser.add_argument(
        "--session-client-ids",
        type=_parse_session_client_ids,
        default=(_CLIENT_ID,),
        help=(
            "comma-separated, independently signed EMT client scopes; each gets its own "
            "native session and callback worker (default: one scope)"
        ),
    )
    parser.add_argument(
        "--same-session-insert-callers",
        type=int,
        default=1,
        help=(
            "explicit experimental caller count for concurrent InsertOrder on exactly one "
            "EMT API/session (default: 1, which retains the normal scheduler topology)"
        ),
    )
    parser.add_argument(
        "--mmap-native-lanes",
        type=int,
        default=1,
        help=("mmap/callback lane count for one serial EMT API/session (default: 1)"),
    )
    parser.add_argument(
        "--disable-gc-during-measured-window",
        action="store_true",
        help="disable CPython cyclic GC only from C0 release through completed C7 evidence",
    )
    parser.add_argument(
        "--callback-pipeline-max-items",
        type=int,
        choices=_CALLBACK_PIPELINE_EXPERIMENTAL_MAX_ITEMS,
        default=_CALLBACK_PIPELINE_DEFAULT_MAX_ITEMS,
        help="bounded callback batch candidate; production default is 8",
    )
    parser.add_argument(
        "--callback-persistence-profile",
        type=CallbackPersistenceProfileV1,
        choices=tuple(CallbackPersistenceProfileV1),
        default=CallbackPersistenceProfileV1.CONTROL,
        help=(
            "callback durable-write composition: control is the default SPSC target-lane path; "
            "direct-candidate is DISARMED, process-isolated, same-scope A/B only"
        ),
    )
    parser.add_argument("--native-worker-cpu", type=int)
    parser.add_argument("--ingress-producer-cpu", type=int)
    parser.add_argument("--callback-ingress-cpu", type=int)
    parser.add_argument("--callback-journal-cpu", type=int)
    parser.add_argument("--sdk-receive-cpu", type=int)
    parser.add_argument("--sdk-process-cpu", type=int)
    parser.add_argument(
        "--native-worker-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help=(
            "one native-worker CPU per process-isolated session scope; requires all six "
            "per-scope CPU plans and unique CPUs across every role"
        ),
    )
    parser.add_argument(
        "--ingress-producer-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help="one ingress-producer CPU per process-isolated session scope",
    )
    parser.add_argument(
        "--callback-ingress-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help="one callback-ingress CPU per process-isolated session scope",
    )
    parser.add_argument(
        "--callback-journal-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help="one callback-journal CPU per process-isolated session scope",
    )
    parser.add_argument(
        "--sdk-receive-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help="one dedicated EMT SDK receive CPU per process-isolated session scope",
    )
    parser.add_argument(
        "--sdk-process-cpus",
        type=_parse_cpu_list,
        metavar="CPU,...",
        help="one dedicated EMT SDK process CPU per process-isolated session scope",
    )
    parser.add_argument(
        "--callback-wakeup-mode",
        choices=_CALLBACK_WAKEUP_MODES,
        default="threading-event",
        help="callback target-lane wakeup primitive (default: threading-event)",
    )
    submit_mapping_group = parser.add_mutually_exclusive_group()
    submit_mapping_group.add_argument(
        "--prebuild-submit-mapping",
        dest="prebuild_submit_mapping",
        action="store_true",
        help=(
            "construct immutable mapped EMT submit descriptors after session start "
            "and before C0; vendor request construction remains after the mmap PreDispatch commit"
        ),
    )
    submit_mapping_group.add_argument(
        "--dynamic-submit-mapping",
        dest="prebuild_submit_mapping",
        action="store_false",
        help=(
            "rollback/A-B control: map the immutable EMT submit descriptor inside the native "
            "lane after C0"
        ),
    )
    parser.set_defaults(prebuild_submit_mapping=True)
    parser.add_argument("--execute-simulated", action="store_true")
    parser.add_argument(
        "--isolate-native-process",
        action="store_true",
        help="after a persisted simulated report, exit the SDK worker without Python finalization",
    )
    parser.add_argument(
        "--isolate-session-processes",
        action="store_true",
        help=(
            "P0 probe: create one independent Python process, EMT API/session and serial "
            "native caller per supplied signed scope, then release all scope-local producers "
            "at a common C0"
        ),
    )
    parser.add_argument(
        "--session-preflight-only",
        action="store_true",
        help=(
            "start and stop every process-isolated EMT adapter without submitting, cancelling "
            "or querying an order; requires --isolate-session-processes"
        ),
    )
    parser.add_argument(
        "--session-preflight-timeout",
        type=float,
        default=15.0,
        help="per-phase parent hard deadline in seconds for --session-preflight-only (default: 15)",
    )
    parser.add_argument(
        "--native-callback-diagnostics",
        action="store_true",
        help=(
            "P0 diagnostic only: count payload-free order-side callback method entries before "
            "bridge scalar extraction; do not use this profile for latency comparison"
        ),
    )
    parser.add_argument("--diagnostic-file", type=Path)
    args = parser.parse_args()
    report = run(
        Settings(
            args.sdk_root,
            args.accounts_file,
            args.fixture,
            args.output,
            args.runtime_dir,
            args.trading_day,
            args.burst_width,
            args.callback_timeout,
            args.native_start_budget,
            args.execute_simulated,
            args.diagnostic_file,
            args.ingress_capacity,
            args.ingress_workers,
            args.native_inflight_limit,
            args.isolate_native_process,
            args.session_client_ids,
            args.same_session_insert_callers,
            args.mmap_native_lanes,
            args.disable_gc_during_measured_window,
            args.callback_pipeline_max_items,
            args.native_worker_cpu,
            args.ingress_producer_cpu,
            args.callback_ingress_cpu,
            args.callback_journal_cpu,
            args.sdk_receive_cpu,
            args.sdk_process_cpu,
            args.callback_wakeup_mode,
            args.isolate_session_processes,
            native_worker_cpus=args.native_worker_cpus,
            ingress_producer_cpus=args.ingress_producer_cpus,
            callback_ingress_cpus=args.callback_ingress_cpus,
            callback_journal_cpus=args.callback_journal_cpus,
            sdk_receive_cpus=args.sdk_receive_cpus,
            sdk_process_cpus=args.sdk_process_cpus,
            submission_topology=args.submission_topology,
            tight_burst_order_plan=args.tight_burst_order_plan,
            session_preflight_only=args.session_preflight_only,
            session_preflight_timeout_seconds=args.session_preflight_timeout,
            hot_single_order_samples=args.hot_single_order_samples,
            prebuild_submit_mapping=args.prebuild_submit_mapping,
            expected_library_hashes=(
                _load_expected_library_hashes(args.expected_library_hashes)
                if args.expected_library_hashes is not None
                else None
            ),
            native_callback_diagnostics=args.native_callback_diagnostics,
            callback_persistence_profile=args.callback_persistence_profile,
        )
    )
    print(
        json.dumps(
            {
                "driver": report["driver"],
                "complete": report["complete"],
                "protected_overload_complete": report["protected_overload_complete"],
            }
        )
    )
    return 0 if report["verification"]["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
