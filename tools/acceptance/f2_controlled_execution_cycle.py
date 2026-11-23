"""F2's low-frequency EMT simulated submit/query/cancel acceptance cycle.

This driver deliberately exercises the *composed* path rather than calling an
adapter directly:

``Guard -> static route -> scheduler -> ExecutionSessionRuntime -> EMT``.

It is not a READY transition and accepts no live profile.  The only mutation
mode is an explicit simulated run using an existing local controlled-order
fixture.  Reports hold booleans, counts and hashes only: no endpoint,
credential, order, instrument, price, vendor row or native identifier leaves
the process.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import platform
import secrets
import shutil
import signal
import time
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock
from typing import Any, Callable

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeObserver,
)
from emt_gateway.fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.provisioned_execution_factory_v1 import (
    ProvisionedEmtExecutionProfileV1,
    ProvisionedEmtExecutionSessionFactoryV1,
)
from emt_gateway.session import SessionSettings, normalize_emt_trading_day
from emt_gateway.tradecore_execution import EmtPlatformExecutionSessionFactory
from emt_gateway.tradecore_mmap_journal import (
    EmtMmapNativeCallJournal,
    PreparedEmtNativeRouteResolver,
)
from emt_gateway.tradecore_order_request import EmtTradeCoreOrderRequestFactory
from tools.acceptance.r1_release_preflight import (
    R1ReleasePreflightError,
    load_namespace_trust_v1,
)
from tools.performance.perfbench.simulated_accounts import load_simulated_accounts
from tools.performance.perfbench.tradecore_mmap_emt_support import (
    _activated_test_manifest,
    _cancel_plan,
    _load_order_fixture,
    _submit_plan,
    load_tight_burst_order_plan,
)
from xuanling_tradecore.adapters import AccountTradingCapabilities, BrokerQuery, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtAuthoritativeOrderScanSeriesCollectorV1,
    EmtAuthoritativeRecoveryReconcilerV1,
    EmtControlledExecutionAdapter,
    EmtExecutionSdkSessionFactory,
    EmtNamespaceRecoveryError,
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    EmtRecoveryCertificateCollectorV1,
    ExecutionNamespaceFenceV1,
    FileNamespaceReleaseStateStoreV1,
    MmapRouteLedgerV2,
    NamespaceOwnership,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseActivationErrorV1,
    NamespaceReleaseBootstrapApprovalV1,
    OrderClientIdNamespaceDirectoryV1,
    OrderClientIdNamespaceLeaseV1,
    RecoveryDispositionV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
    load_signed_namespace_release_v1_json,
    load_single_native_acceptance_target_v1,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.adapters.emt.replay_witness_v1 import (
    issue_emt_replay_witness_v1,
    write_emt_replay_witness_proposal_v1,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
    SecretReferenceV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import (
    LinuxProtectedFileSecretProviderV1,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    CallbackGapEvidence,
    CancelDeadlinePolicy,
    ConsumerTrace,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountExecutionRoutePlan,
    AccountExecutionRouter,
    AccountFenceScope,
    AccountOwnershipFence,
    ExecutionDispatchOutcome,
    ExecutionGuard,
    ExecutionGuardAdmissionGate,
    ExecutionGuardPolicy,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    ExecutionSessionRuntime,
    ExecutionSessionRuntimeError,
    GuardDecision,
    GuardMarketSnapshot,
    IngressAdmission,
    KillSwitchSnapshot,
    SessionSupervisor,
    canonical_broker_capabilities_hash,
)
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    MmapCallbackOutcomeJournal,
    MmapPreNativeJournal,
    scan_mmap_pre_native_journal_path,
)
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapJournalRecordKind

_LOGICAL_ACCOUNT_ID = "emt_01"
_STRATEGY_ID = "f2-controlled-cycle"
_SESSION_EPOCH = 1
_MAX_SAFE_CLIENT_ORDER_REF = 900_000_000
_MIN_SAFE_CLIENT_ORDER_REF = 100_000_000
_EMT_ACTION_COOLDOWN_SECONDS = 1.10
_R1_ORDER_VISIBILITY_ATTEMPTS = 3
_R1_ORDER_VISIBILITY_WAIT_SECONDS = 0.10
_R1_AUTHORITY_PAGE_SIZE = 200
_R1_AUTHORITY_MAX_PAGES = 512
_R1_AUTHORITY_PAGE_DELAY_SECONDS = 0.025
# The vendor wrapper is retained only after an explicit native logout/release
# inside an F2 process that calls ``os._exit`` immediately after its report is
# durable.  This prevents its known blocking Python finalizer from delaying a
# normal interpreter shutdown and is never used by a long-running service.
_ISOLATED_NATIVE_REFERENCE_ANCHOR: list[object] = []


class F2ControlledExecutionCycleError(RuntimeError):
    """The F2 controlled cycle did not obtain complete, safe evidence."""


@dataclass(frozen=True)
class _OwnedSourceNativeAcceptanceTarget:
    """One source-Journal target that may be reconciled exactly once.

    This is deliberately a local, payload-free pairing of EMT's two routing
    scalars.  It is not a recovered order or a licence to scan/cancel an
    account.  The live path below still requires exactly one match from the
    authoritative unfinished-order query before it can create a cancellation.
    """

    order_client_id: int
    order_emt_id: int
    source_sequence: int


def _load_owned_source_native_acceptance_targets(
    path: Path,
) -> tuple[_OwnedSourceNativeAcceptanceTarget, ...]:
    """Extract unique accepted targets from a clean historical mmap Journal.

    The file is read through ``scan_mmap_pre_native_journal_path`` and is
    never opened for write access.  Duplicate client references, malformed
    scalar fields, torn tails, or an empty source all fail closed.  This
    helper intentionally does not log or serialize the native identifiers.
    """

    scan = scan_mmap_pre_native_journal_path(path)
    if not scan.clean:
        raise F2ControlledExecutionCycleError("F2 owned-source Journal tail is not clean")
    targets: dict[int, _OwnedSourceNativeAcceptanceTarget] = {}
    for record in scan.records:
        if record.kind != MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED:
            continue
        try:
            client_id = int(record.client_order_ref)
            native_id = int(record.native_order_id)
        except ValueError as exc:
            raise F2ControlledExecutionCycleError(
                "F2 owned-source Journal acceptance identity is invalid"
            ) from exc
        if (
            str(client_id) != record.client_order_ref
            or str(native_id) != record.native_order_id
            or not 1 <= client_id <= _MAX_SAFE_CLIENT_ORDER_REF
            or native_id <= 0
        ):
            raise F2ControlledExecutionCycleError(
                "F2 owned-source Journal acceptance identity is invalid"
            )
        candidate = _OwnedSourceNativeAcceptanceTarget(
            order_client_id=client_id,
            order_emt_id=native_id,
            source_sequence=record.sequence,
        )
        existing = targets.get(client_id)
        if existing is not None and existing != candidate:
            raise F2ControlledExecutionCycleError(
                "F2 owned-source Journal has an ambiguous client reference"
            )
        targets[client_id] = candidate
    if not targets:
        raise F2ControlledExecutionCycleError(
            "F2 owned-source Journal has no accepted native target"
        )
    return tuple(sorted(targets.values(), key=lambda item: item.order_client_id))


class _PreflightComplete(Exception):
    """Internal control flow after a clean, explicitly no-mutation probe."""


class _R1CrashAfterNativeReturnTelemetry:
    """One-shot R2 test hook, reachable only after ``insertOrder`` returns.

    It deliberately has no order identifiers, credentials or request payload.
    The marker is fully synced before SIGKILL, while the code path to
    ``record_submit_accepted`` has not yet been reached.  This class exists
    only in the disposable F2 acceptance subprocess; production composition
    never supplies it.
    """

    _BOUNDARY = "R2_AFTER_INSERT_RETURN_BEFORE_NATIVE_ACCEPTED_PERSISTENCE"

    def __init__(self, marker_path: Path) -> None:
        self._marker_path = marker_path
        self._fired = False

    def on_submit_stage(self, request: object, stage: str, observed_ns: int) -> None:
        del request
        if self._fired or stage != "C4_AFTER_SDK":
            return
        self._fired = True
        self._write_marker(observed_ns)
        os.kill(os.getpid(), signal.SIGKILL)

    def on_cancel_stage(self, request: object, stage: str, observed_ns: int) -> None:
        del request, stage, observed_ns

    def _write_marker(self, observed_ns: int) -> None:
        self._marker_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self._marker_path.parent, 0o700)
        payload = json.dumps(
            {
                "schema_version": 1,
                "boundary": self._BOUNDARY,
                "observed_monotonic_ns": observed_ns,
                "payloads_retained": False,
                "credentials_retained": False,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        descriptor = os.open(
            self._marker_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if os.name != "nt":
            parent_descriptor = os.open(
                self._marker_path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(parent_descriptor)
            finally:
                os.close(parent_descriptor)


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    expected_library_hashes: Path
    accounts_file: Path
    fixture: Path
    output: Path
    runtime_dir: Path
    trading_day: str
    session_client_id: int
    callback_timeout_seconds: float
    execute_simulated: bool
    provisioned_secret_lease: bool = False
    account_profile: str = "ordinary"
    tight_burst_order_plan: Path | None = None
    preflight_only: bool = False
    isolated_native_process: bool = False
    fault_mode: str | None = None
    r1_authoritative_recovery_probe: bool = False
    r1_authoritative_query_only: bool = False
    r1_replay_seed_only: bool = False
    hold_before_cancel_seconds: float = 0.0
    recovery_route_ledger: Path | None = None
    recovery_source_pre_native_journal: Path | None = None
    recovery_source_query_only: bool = False
    r1_namespace_release: Path | None = None
    r1_namespace_trust: Path | None = None
    r1_namespace_state: Path | None = None
    r1_runtime_release_hash: str | None = None
    r1_bootstrap_approval_id: str | None = None
    r1_bootstrap_approver_id: str | None = None
    r1_crash_after_native_return: bool = False
    r1_crash_marker: Path | None = None
    r1_recovery_route_ledger: Path | None = None
    r1_manual_cleanup_route_ledger: Path | None = None
    r1_manual_cleanup_source_release: Path | None = None
    r1_manual_cleanup_source_trust: Path | None = None
    r1_replay_witness_proposal_output: Path | None = None
    recovery_approval_id: str | None = None
    recovery_approver_id: str | None = None

    def validate(self) -> None:
        if not self.execute_simulated:
            raise F2ControlledExecutionCycleError("--execute-simulated is required")
        if not self.isolated_native_process:
            raise F2ControlledExecutionCycleError(
                "F2 controlled cycle requires --isolated-native-process"
            )
        if platform.system() != "Linux":
            raise F2ControlledExecutionCycleError("F2 controlled cycle requires Linux")
        if (
            not self.sdk_root.is_dir()
            or not self.expected_library_hashes.is_file()
            or not self.accounts_file.is_file()
            or not self.fixture.is_file()
        ):
            raise F2ControlledExecutionCycleError("F2 controlled cycle input path is unavailable")
        if not 1 <= self.session_client_id <= 2**31 - 1:
            raise F2ControlledExecutionCycleError("F2 EMT client scope is invalid")
        if self.account_profile not in ("ordinary", "credit"):
            raise F2ControlledExecutionCycleError("F2 account profile is unsupported")
        if self.account_profile == "credit" and not self.preflight_only:
            raise F2ControlledExecutionCycleError(
                "F2 credit profile is restricted to a query-only preflight"
            )
        if not 5.0 <= self.callback_timeout_seconds <= 60.0:
            raise F2ControlledExecutionCycleError("F2 callback timeout must be in 5..60 seconds")
        if self.fault_mode not in (None, "host-health-integrity", "host-health-capacity"):
            raise F2ControlledExecutionCycleError("F2 fault mode is unsupported")
        if self.fault_mode is not None and not self.preflight_only:
            raise F2ControlledExecutionCycleError(
                "F2 fault mode requires --preflight-only and never permits a mutation"
            )
        if (
            self.r1_authoritative_recovery_probe
            and self.preflight_only
            and not self.r1_authoritative_query_only
        ):
            raise F2ControlledExecutionCycleError(
                "R1 authoritative recovery probe requires the full controlled cycle"
            )
        if self.r1_authoritative_query_only and (
            not self.r1_authoritative_recovery_probe
            or not self.preflight_only
            or self.fault_mode is not None
            or self.r1_replay_seed_only
            or self.r1_crash_after_native_return
            or self.r1_recovery_route_ledger is not None
            or self.r1_manual_cleanup_route_ledger is not None
        ):
            raise F2ControlledExecutionCycleError(
                "R1 authoritative query-only mode requires the R1 read-only preflight"
            )
        if self.r1_replay_seed_only and (
            not self.r1_authoritative_recovery_probe
            or self.r1_recovery_route_ledger is not None
            or self.r1_manual_cleanup_route_ledger is not None
            or self.r1_crash_after_native_return
        ):
            raise F2ControlledExecutionCycleError(
                "R1 replay seed requires the normal external R1 controlled cycle"
            )
        if self.r1_replay_witness_proposal_output is not None:
            if (
                not self.r1_authoritative_recovery_probe
                or self.preflight_only
                or not external_r1_release_enabled(self)
                or self.r1_replay_witness_proposal_output.exists()
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 replay witness proposal requires a new full R1 probe output path"
                )
        if self.r1_authoritative_recovery_probe and not 1 <= self.session_client_id <= 255:
            raise F2ControlledExecutionCycleError(
                "R1 namespace probe requires an EMT uint8 client scope"
            )
        r1_release_inputs = (
            self.r1_namespace_release,
            self.r1_namespace_trust,
            self.r1_namespace_state,
            self.r1_runtime_release_hash,
        )
        if any(item is not None for item in r1_release_inputs):
            if not all(item is not None for item in r1_release_inputs):
                raise F2ControlledExecutionCycleError(
                    "F2 external R1 release requires release, trust, state and runtime hash"
                )
            if not self.r1_authoritative_recovery_probe:
                raise F2ControlledExecutionCycleError(
                    "F2 external R1 release requires the R1 authoritative recovery probe"
                )
            assert self.r1_namespace_release is not None
            assert self.r1_namespace_trust is not None
            assert self.r1_namespace_state is not None
            assert self.r1_runtime_release_hash is not None
            if not self.r1_namespace_release.is_file() or not self.r1_namespace_trust.is_file():
                raise F2ControlledExecutionCycleError("F2 external R1 release input is unavailable")
            if self.r1_namespace_state.exists() and not self.r1_namespace_state.is_file():
                raise F2ControlledExecutionCycleError("F2 external R1 release state is invalid")
            if not _is_sha256(self.r1_runtime_release_hash):
                raise F2ControlledExecutionCycleError("F2 external R1 runtime hash is invalid")
            state_store = FileNamespaceReleaseStateStoreV1(self.r1_namespace_state)
            if state_store.load() is None and (
                not self.r1_bootstrap_approval_id or not self.r1_bootstrap_approver_id
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 initial external R1 release requires bootstrap approval identity"
                )
        elif self.r1_bootstrap_approval_id is not None or self.r1_bootstrap_approver_id is not None:
            raise F2ControlledExecutionCycleError(
                "F2 R1 bootstrap approval cannot be supplied without an external release"
            )
        if self.r1_crash_after_native_return:
            if not external_r1_release_enabled(self):
                raise F2ControlledExecutionCycleError(
                    "F2 R2 crash boundary requires an external R1 release"
                )
            if self.r1_crash_marker is None or self.r1_crash_marker.exists():
                raise F2ControlledExecutionCycleError("F2 R2 crash marker is unavailable")
        elif self.r1_crash_marker is not None:
            raise F2ControlledExecutionCycleError(
                "F2 R2 crash marker requires the crash-boundary test flag"
            )
        if self.r1_recovery_route_ledger is not None:
            if (
                not external_r1_release_enabled(self)
                or not self.r1_authoritative_recovery_probe
                or self.preflight_only
                or self.fault_mode is not None
                or self.r1_crash_after_native_return
                or not self.r1_recovery_route_ledger.is_file()
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 R1 rehydration requires external release, full R1 mode and a route ledger"
                )
        if self.r1_manual_cleanup_route_ledger is not None:
            if (
                not external_r1_release_enabled(self)
                or not self.r1_authoritative_recovery_probe
                or self.preflight_only
                or self.fault_mode is not None
                or self.r1_crash_after_native_return
                or not self.r1_manual_cleanup_route_ledger.is_file()
                or self.r1_manual_cleanup_source_release is None
                or self.r1_manual_cleanup_source_trust is None
                or not self.r1_manual_cleanup_source_release.is_file()
                or not self.r1_manual_cleanup_source_trust.is_file()
                or not self.recovery_approval_id
                or not self.recovery_approver_id
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 R1 manual cleanup requires external release, full R1 mode, "
                    "a source release/trust, crashed route ledger and explicit approval"
                )
        elif (
            self.r1_manual_cleanup_source_release is not None
            or self.r1_manual_cleanup_source_trust is not None
        ):
            raise F2ControlledExecutionCycleError(
                "F2 manual cleanup source release is not applicable without manual cleanup"
            )
        if (
            sum(
                item is not None
                for item in (
                    self.recovery_route_ledger,
                    self.recovery_source_pre_native_journal,
                    self.r1_recovery_route_ledger,
                    self.r1_manual_cleanup_route_ledger,
                )
            )
            > 1
        ):
            raise F2ControlledExecutionCycleError(
                "F2 recovery and cleanup modes cannot be combined in one process"
            )
        if self.recovery_route_ledger is not None:
            if self.preflight_only or self.fault_mode is not None:
                raise F2ControlledExecutionCycleError(
                    "F2 targeted cleanup cannot combine with preflight or fault mode"
                )
            if self.r1_authoritative_recovery_probe:
                raise F2ControlledExecutionCycleError(
                    "F2 targeted cleanup cannot combine with an R1 evidence probe"
                )
            if not self.recovery_route_ledger.is_file():
                raise F2ControlledExecutionCycleError(
                    "F2 targeted cleanup route ledger is unavailable"
                )
            if not self.recovery_approval_id or not self.recovery_approver_id:
                raise F2ControlledExecutionCycleError(
                    "F2 targeted cleanup requires explicit manual approval identity"
                )
        if self.recovery_source_pre_native_journal is not None:
            if (
                self.fault_mode is not None
                or self.r1_authoritative_recovery_probe
                or self.r1_replay_seed_only
                or not self.recovery_source_pre_native_journal.is_file()
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 owned-source reconciliation has an incompatible mode"
                )
            if self.recovery_source_query_only:
                if (
                    not self.preflight_only
                    or self.recovery_approval_id is not None
                    or self.recovery_approver_id is not None
                ):
                    raise F2ControlledExecutionCycleError(
                        "F2 owned-source query-only mode requires preflight-only and no approval"
                    )
            elif (
                self.preflight_only
                or not self.recovery_approval_id
                or not self.recovery_approver_id
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 owned-source cleanup requires explicit approval and the normal isolated cycle"
                )
        elif self.recovery_source_query_only:
            raise F2ControlledExecutionCycleError(
                "F2 owned-source query-only mode requires a source Journal"
            )
        elif self.r1_manual_cleanup_route_ledger is None and self.recovery_route_ledger is None and (
            self.recovery_approval_id is not None or self.recovery_approver_id is not None
        ):
            raise F2ControlledExecutionCycleError(
                "F2 recovery approval cannot be supplied without a cleanup route ledger"
            )
        maximum_hold_seconds = (
            180.0
            if self.r1_authoritative_recovery_probe
            and self.isolated_native_process
            and self.account_profile == "ordinary"
            else 30.0
        )
        if not 0.0 <= self.hold_before_cancel_seconds <= maximum_hold_seconds:
            raise F2ControlledExecutionCycleError(
                "F2 hold-before-cancel duration exceeds the bounded mode limit"
            )
        try:
            normalize_emt_trading_day(self.trading_day)
        except Exception as exc:
            raise F2ControlledExecutionCycleError("F2 trading day is invalid") from exc


class _Audit:
    """Count only guard rejections; rejected payloads remain outside evidence."""

    def __init__(self) -> None:
        self.rejection_count = 0

    def record_execution_rejected(
        self, instruction: ExecutionInstruction, decision: GuardDecision
    ) -> None:
        del instruction, decision
        self.rejection_count += 1


class _Observer:
    """In-memory completion tracker; it never serializes a BrokerEvent."""

    def __init__(self) -> None:
        self._events: list[BrokerEvent] = []
        self._gaps = 0
        self._gap_producer_counts: dict[str, int] = {}
        self._changed = Event()
        self._lock = Lock()
        self._activity_observer: Callable[[str], None] | None = None

    def set_activity_observer(self, observer: Callable[[str], None]) -> None:
        self._activity_observer = observer

    def on_broker_event(self, event: BrokerEvent) -> None:
        self._observe_activity(f"f2_observer_event:{event.event_kind.value}")
        with self._lock:
            self._events.append(event)
            self._changed.set()

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self._observe_activity("f2_observer_callback_gap")
        with self._lock:
            self._gaps += 1
            self._gap_producer_counts[evidence.producer_id] = (
                self._gap_producer_counts.get(evidence.producer_id, 0) + 1
            )
            self._changed.set()

    def snapshot(self) -> tuple[tuple[BrokerEvent, ...], int]:
        with self._lock:
            return tuple(self._events), self._gaps

    @property
    def gap_producer_counts(self) -> dict[str, int]:
        """Return payload-free callback-gap origins for acceptance diagnosis."""

        with self._lock:
            return dict(sorted(self._gap_producer_counts.items()))

    def wait(self, timeout_seconds: float) -> None:
        self._changed.wait(timeout_seconds)
        self._changed.clear()

    def _observe_activity(self, stage: str) -> None:
        observer = self._activity_observer
        if observer is not None:
            observer(stage)


def _secure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def external_r1_release_enabled(settings: Settings) -> bool:
    return all(
        item is not None
        for item in (
            settings.r1_namespace_release,
            settings.r1_namespace_trust,
            settings.r1_namespace_state,
            settings.r1_runtime_release_hash,
        )
    )


def _load_expected_library_hashes(path: Path) -> dict[str, str]:
    """Load the reviewed non-secret SDK hash manifest for the F2 loader gate."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise F2ControlledExecutionCycleError("F2 SDK hash manifest is unreadable") from exc
    if not isinstance(raw, dict) or not raw:
        raise F2ControlledExecutionCycleError("F2 SDK hash manifest is empty")
    hashes: dict[str, str] = {}
    for name, digest in raw.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise F2ControlledExecutionCycleError("F2 SDK hash manifest entry is invalid")
        hashes[name] = digest
    return hashes


def _query(
    *,
    request_id: str,
    binding: AccountExecutionBinding,
    kind: str,
    page_size: int = 32,
    continuation: int = 0,
    capture_authoritative_order_rows: bool = False,
) -> BrokerQuery:
    return BrokerQuery(
        request_id=request_id,
        account_id=binding.account_id,
        venue_id=binding.venue_id,
        adapter_instance_id=binding.adapter_instance_id,
        query_kind=kind,
        page_size=page_size,
        continuation=continuation,
        capture_authoritative_order_rows=capture_authoritative_order_rows,
    )


def _query_terminal(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    kind: str,
    timeout_seconds: float,
    prior_count: int,
    allow_callback_gaps: bool = False,
) -> BrokerEvent:
    deadline = time.monotonic() + timeout_seconds
    while True:
        events, gaps = observer.snapshot()
        if gaps and not allow_callback_gaps:
            raise F2ControlledExecutionCycleError("F2 query callback gap observed")
        for event in events[prior_count:]:
            if (
                event.event_kind is BrokerEventKind.QUERY
                and event.normalized_summary.get("query_kind") == kind
            ):
                if event.normalized_summary.get("terminal") != "true":
                    raise F2ControlledExecutionCycleError(
                        "F2 query did not normalize terminal evidence"
                    )
                if event.normalized_summary.get("error_id") != "0":
                    raise F2ControlledExecutionCycleError("F2 query terminal returned an error")
                return event
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise F2ControlledExecutionCycleError("F2 query terminal callback timed out")
        runtime.drain_broker_events(wait_timeout_seconds=min(remaining, 0.25))
        observer.wait(min(remaining, 0.05))


def _wait_for_controlled_order_callback_echo(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    expected_client_order_ref: int,
    timeout_seconds: float,
    prior_count: int,
) -> bool:
    """Confirm the controlled callback echoes its pre-admitted client reference.

    This produces one Boolean only.  It deliberately neither serializes the
    native identifier nor uses an unmatched callback as a recovery fact.  The
    later authoritative scan remains the source of truth for R1; this small
    probe separates a callback-echo fault from a paged-query visibility fault.
    """

    deadline = time.monotonic() + timeout_seconds
    expected = str(expected_client_order_ref)
    while True:
        events, gaps = observer.snapshot()
        if gaps:
            raise F2ControlledExecutionCycleError("F2 order callback gap observed")
        for event in events[prior_count:]:
            if event.event_kind is not BrokerEventKind.ORDER:
                continue
            identity = event.venue_order_identity
            if identity is not None and identity.client_order_ref == expected:
                return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        runtime.drain_broker_events(wait_timeout_seconds=min(remaining, 0.25))
        observer.wait(min(remaining, 0.05))


def _collect_authoritative_unfinished_order_scan(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    scan_series: EmtAuthoritativeOrderScanSeriesCollectorV1,
    scan_id: str,
    request_id: str,
    timeout_seconds: float,
    allow_callback_gaps: bool = False,
) -> Any:
    """Read the current unfinished set without retaining a vendor payload.

    EMT's ``QueryUnfinishedOrders`` is account-scoped.  An account may be
    shared by independent systems, so its aggregate row count cannot decide
    whether this F2 run may proceed.  The opt-in collector keeps only the five
    reviewed order scalars while this single terminal query is in flight; the
    caller compares the exact generated ``order_client_id`` and immediately
    discards the scan after producing a Boolean outcome.
    """

    scan_series.begin(scan_id=scan_id)
    before_queries, _gaps = observer.snapshot()
    runtime.query(
        _query(
            request_id=request_id,
            binding=binding,
            kind="unfinished_orders",
            capture_authoritative_order_rows=True,
        )
    )
    event = _query_terminal(
        runtime=runtime,
        observer=observer,
        kind="unfinished_orders",
        timeout_seconds=timeout_seconds,
        prior_count=len(before_queries),
        allow_callback_gaps=allow_callback_gaps,
    )
    scan = scan_series.seal_current()
    if not scan.complete:
        raise F2ControlledExecutionCycleError(
            "F2 owned-target unfinished-order scan did not complete"
        )
    return scan, event


def _wait_for_target_unfinished_order_absent(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    scan_series: EmtAuthoritativeOrderScanSeriesCollectorV1,
    target_client_order_ref: int,
    timeout_seconds: float,
    request_prefix: str,
    allow_callback_gaps: bool = False,
) -> BrokerEvent:
    """Wait until this run's exact client reference leaves EMT unfinished work.

    A native cancel's synchronous acceptance is not a terminal event.  The
    cleanup code must therefore wait for a terminal unfinished-order snapshot
    instead of inferring completion from the submit inflight counter.  Other
    systems may share the same EMT account, so their open orders are neither
    an F2 failure nor cancellation targets.  It never retries the cancel or
    changes this run's exact pre-admitted target.
    """

    deadline = time.monotonic() + timeout_seconds
    ordinal = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise F2ControlledExecutionCycleError(
                "F2 cleanup did not receive an unfinished-order terminal callback"
            )
        scan, event = _collect_authoritative_unfinished_order_scan(
            runtime=runtime,
            observer=observer,
            binding=binding,
            scan_series=scan_series,
            scan_id=f"{request_prefix}-scan-{ordinal}",
            request_id=f"{request_prefix}-{ordinal}",
            timeout_seconds=remaining,
            allow_callback_gaps=allow_callback_gaps,
        )
        _events, gaps = observer.snapshot()
        if gaps and not allow_callback_gaps:
            raise F2ControlledExecutionCycleError("F2 cleanup query callback gap observed")
        if not any(order.order_client_id == target_client_order_ref for order in scan.orders):
            return event
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise F2ControlledExecutionCycleError("F2 cleanup query reports unfinished orders")
        ordinal += 1
        time.sleep(min(0.10, remaining))


def _collect_r1_certificate_queries(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    collector: EmtRecoveryCertificateCollectorV1,
    timeout_seconds: float,
    record_lifecycle: Callable[[str], None],
) -> None:
    """Collect the certificate's serial query family without retaining rows.

    A page whose bounded result is full must provide a new opaque continuation.
    Repeating a cursor, timing out, receiving a nonzero terminal code or
    exceeding the explicit page ceiling all fail this bounded recovery probe;
    none of them can be treated as an empty account snapshot.
    """

    for query_kind in sorted(collector.required_query_kinds):
        continuation = 0
        record_lifecycle(f"f2_r1_certificate_{query_kind}_started")
        # A recovery scan is outside C0--C7.  It must be bounded, but the
        # ordinary simulator account may contain more than 12,800 history
        # rows; use the documented 200-row page and a fixed 512-page ceiling.
        for ordinal in range(512):
            before_queries, _gaps = observer.snapshot()
            runtime.query(
                _query(
                    request_id=f"f2-r1-certificate-{query_kind}-{ordinal}",
                    binding=binding,
                    kind=query_kind,
                    page_size=200,
                    continuation=continuation,
                )
            )
            event = _query_terminal(
                runtime=runtime,
                observer=observer,
                kind=query_kind,
                timeout_seconds=timeout_seconds,
                prior_count=len(before_queries),
            )
            collector.record_terminal(event)
            next_continuation = collector.next_continuation(query_kind)
            if next_continuation is None:
                record_lifecycle(f"f2_r1_certificate_{query_kind}_completed")
                break
            if next_continuation == continuation:
                raise F2ControlledExecutionCycleError(
                    "R1 paged query returned a non-progressing continuation"
                )
            continuation = next_continuation
            # Pace recovery-only continuation requests to avoid turning the
            # session-local vendor query lane into a burst source.
            time.sleep(0.025)
        else:
            raise F2ControlledExecutionCycleError("R1 paged query exceeded the fixed page bound")


def _wait_for_terminal_cleanup(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    scheduler: ExecutionScheduler,
    timeout_seconds: float,
    allow_callback_gaps: bool = False,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        _events, gaps = observer.snapshot()
        if gaps and not allow_callback_gaps:
            raise F2ControlledExecutionCycleError("F2 order callback gap observed")
        if scheduler.snapshot().depths["new_order_inflight"] == 0:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise F2ControlledExecutionCycleError("F2 terminal cleanup callback timed out")
        runtime.drain_broker_events(wait_timeout_seconds=min(remaining, 0.25))
        observer.wait(min(remaining, 0.05))


def _admit_controlled_cancel(
    *,
    runtime: ExecutionSessionRuntime,
    resolver: PreparedEmtNativeRouteResolver,
    journal: EmtMmapNativeCallJournal,
    instruction: Any,
    target: Any,
    target_route_payload_hash: str,
    binding: AccountJournalBinding,
) -> None:
    """Run exactly one accepted F2 target through the composed cancel lane.

    The normal and exceptional paths share this operation.  Its prepared
    route binds the request to the single venue identity returned by the
    current process's submit; it never expands to account-wide cleanup.
    """

    # EMT action flow-control is account scoped.  F2 deliberately issues no
    # retry and waits before its one cancellation rather than broadening the
    # target set after any unrelated evidence-collection failure.
    time.sleep(_EMT_ACTION_COOLDOWN_SECONDS)
    cancel_instruction, cancel_route = _cancel_plan(
        instruction=instruction,
        target=target,
        target_route_payload_hash=target_route_payload_hash,
        binding=binding,
    )
    resolver.prepare_cancel(cancel_route, cancel_instruction)
    cancel_admission = runtime.admit_cancel(
        cancel_instruction,
        serialized_size_bytes=256,
        admitted_monotonic_ns=time.monotonic_ns(),
    )
    if not isinstance(cancel_admission, IngressAdmission):
        raise F2ControlledExecutionCycleError("F2 bounded controlled cancel was not admitted")
    with journal.mmap_native_lane("f2-native"):
        cancel_dispatch = runtime.dispatch_once(checked_monotonic_ns=time.monotonic_ns())
    if (
        cancel_dispatch is None
        or cancel_dispatch.outcome is not ExecutionDispatchOutcome.CANCEL_ACCEPTED
    ):
        raise F2ControlledExecutionCycleError("F2 composed cancel did not reach native acceptance")


def _guard(
    *,
    instruction: object,
    audit: _Audit,
) -> tuple[ExecutionGuardAdmissionGate, GuardMarketSnapshot]:
    symbol = getattr(instruction, "symbol")
    market = getattr(instruction, "market")
    side = getattr(instruction, "side")
    quantity = getattr(instruction, "quantity")
    price_ticks = getattr(instruction, "limit_price_ticks")
    now = time.monotonic_ns()
    policy = ExecutionGuardPolicy(
        policy_id="f2-controlled-sim-guard",
        version=1,
        policy_hash=sha256(b"f2-controlled-sim-guard-v1").hexdigest(),
        account_id=_LOGICAL_ACCOUNT_ID,
        frequency_window_ns=1_000_000_000,
        account_order_limit=1,
        strategy_order_limits={_STRATEGY_ID: 1},
        symbol_order_limits={symbol: 1},
        side_order_limits={side: 1},
        max_price_deviation_bps=0,
        max_order_quantity=quantity,
        max_order_notional_ticks=(price_ticks * quantity) + 1,
    )
    snapshot = GuardMarketSnapshot(
        snapshot_id="f2-controlled-sim-market",
        symbol=symbol,
        market=market,
        reference_price_ticks=price_ticks,
        minimum_price_ticks=price_ticks,
        maximum_price_ticks=price_ticks,
        captured_monotonic_ns=now,
        expires_monotonic_ns=now + 60_000_000_000,
    )
    return ExecutionGuardAdmissionGate(ExecutionGuard(policy), audit), snapshot


def _f2_report(
    *,
    status: str,
    checks: dict[str, bool],
    observer: _Observer | None,
    runtime: ExecutionSessionRuntime | None,
    supervisor: SessionSupervisor | None,
    scheduler: ExecutionScheduler | None,
    audit: _Audit | None,
    failure_kind: str | None,
    preflight_only: bool,
    fault_mode: str | None,
    account_profile: str,
    owned_source_cleanup: bool,
    owned_source_query_only: bool,
    lifecycle_stages: tuple[str, ...],
    r1_authority: dict[str, object] | None,
    callback_gap_reason_counts: dict[str, int],
    verified_foreign_cancel_callback_count: int,
    verified_foreign_namespace_callback_count: int,
    query_callback_gap_reason_counts: dict[str, int],
    query_authoritative_row_failure_counts: dict[str, int],
    provisioned_lease_evidence: dict[str, bool],
) -> dict[str, Any]:
    _events, gap_count = observer.snapshot() if observer is not None else ((), 0)
    runtime_snapshot = runtime.snapshot() if runtime is not None else None
    supervisor_snapshot = supervisor.snapshot() if supervisor is not None else None
    scheduler_snapshot = scheduler.snapshot() if scheduler is not None else None
    return {
        "schema_version": "tradecore-f2-controlled-cycle-report-v1",
        "driver": "f2-controlled-execution-cycle",
        "environment": "simulated",
        "account_profile": account_profile,
        "mode": (
            "owned-source-reconciliation-query-only"
            if owned_source_query_only
            else
            "owned-source-reconciliation-cleanup"
            if owned_source_cleanup
            else
            "r1-manual-r2-cleanup-only"
            if r1_authority is not None and r1_authority.get("manual_cleanup_only") is True
            else "r1-recovery-rehydrate-only"
            if r1_authority is not None and r1_authority.get("recovery_only") is True
            else "r1-replay-seed-only"
            if r1_authority is not None and r1_authority.get("replay_seed_only") is True
            else "r1-authoritative-query-only"
            if r1_authority is not None and r1_authority.get("query_only") is True
            else "fault-probe"
            if fault_mode is not None
            else "preflight-only"
            if preflight_only
            else "full-controlled-cycle"
        ),
        "fault_mode": fault_mode,
        "full_f2_acceptance": False,
        "acceptance_status": status,
        "checks": checks,
        "callback": {
            "normalized_event_count": len(_events),
            "gap_count": gap_count,
            "gap_producer_counts": observer.gap_producer_counts if observer is not None else {},
            "gap_reason_counts": dict(sorted(callback_gap_reason_counts.items())),
            "verified_foreign_cancel_callback_count": verified_foreign_cancel_callback_count,
            "verified_foreign_namespace_callback_count": (
                verified_foreign_namespace_callback_count
            ),
            "query_gap_reason_counts": dict(sorted(query_callback_gap_reason_counts.items())),
            "query_authoritative_row_failure_counts": dict(
                sorted(query_authoritative_row_failure_counts.items())
            ),
        },
        "runtime": (
            {
                "started": runtime_snapshot.started,
                "admission_closed": runtime_snapshot.admission_closed,
                "callback_gap_count": runtime_snapshot.callback_gap_count,
                "scheduler_depths": runtime_snapshot.scheduler_depths,
            }
            if runtime_snapshot is not None
            else None
        ),
        "supervisor": (
            {
                "state": supervisor_snapshot.state.value,
                "authorizes_new_orders": supervisor_snapshot.authorizes_new_orders,
            }
            if supervisor_snapshot is not None
            else None
        ),
        "scheduler": (
            {
                "new_order_inflight": scheduler_snapshot.depths["new_order_inflight"],
                "new_order_accepted": scheduler_snapshot.counters["new_order_accepted"],
                "cancel_accepted": scheduler_snapshot.counters["cancel_accepted"],
            }
            if scheduler_snapshot is not None
            else None
        ),
        "guard": {"rejection_count": audit.rejection_count if audit is not None else 0},
        "lifecycle_stages": list(lifecycle_stages),
        "failure_kind": failure_kind,
        "ready_claim": False,
        "automatic_recovery_claim": False,
        "payloads_retained": False,
        "credentials_retained": False,
        "provisioned_secret_lease": dict(sorted(provisioned_lease_evidence.items())),
        "r1_authority": r1_authority,
    }


def _required_check_names(settings: Settings) -> tuple[str, ...]:
    """Return only the acceptance checks applicable to the selected F2 mode.

    Host-health checks deliberately stay ``False`` for the normal controlled
    submit/query/cancel cycle: no fault is injected in that mode.  Treating
    those sentinel values as mandatory made an otherwise complete cycle report
    fail after the native cleanup had succeeded.  Keep the mode decision in
    one small pure helper so the live evidence path cannot silently broaden its
    acceptance contract.
    """

    common = (
        "linux_ownership_fence_held",
        "sdk_hash_gate_passed",
        "disarmed_signed_scope_started",
        "preflight_target_ref_absent",
        "callback_gap_free",
    )
    if settings.r1_recovery_route_ledger is not None:
        return (
            "linux_ownership_fence_held",
            "sdk_hash_gate_passed",
            "disarmed_signed_scope_started",
            "r1_namespace_route_fenced",
            "r1_authoritative_double_scan",
            "r1_recovery_disarmed",
            "r1_rehydration_persisted",
            "callback_gap_free",
        )
    if settings.r1_manual_cleanup_route_ledger is not None:
        # This exception path exists only to clean an already-crashed, exact
        # R2 target.  It never declares generic recovery, a namespace clean,
        # or new-order eligibility when unrelated remote rows are unknown.
        return (
            "linux_ownership_fence_held",
            "sdk_hash_gate_passed",
            "disarmed_signed_scope_started",
            "r1_manual_target_double_scan",
            "r1_manual_cleanup_only",
            "cancel_through_guard_route_scheduler",
            "terminal_callback_released_inflight",
            "post_cleanup_target_ref_absent",
            "callback_gap_free",
        )
    if settings.recovery_route_ledger is not None:
        # This is a one-time inverse-side-effect recovery command.  Its
        # precondition is intentionally nonzero unfinished work; success is
        # the exact-route cancellation plus the authoritative zero query.
        return (
            "linux_ownership_fence_held",
            "sdk_hash_gate_passed",
            "disarmed_signed_scope_started",
            "cancel_through_guard_route_scheduler",
            "terminal_callback_released_inflight",
            "post_cleanup_target_ref_absent",
            "callback_gap_free",
        )
    if settings.recovery_source_pre_native_journal is not None:
        # The source Journal only nominates potential TradeCore-owned targets;
        # it cannot itself prove their current venue state.  Require a clean
        # source and one exact authoritative unfinished-order intersection
        # before the composed inverse action may run.
        checks = (
            "linux_ownership_fence_held",
            "sdk_hash_gate_passed",
            "disarmed_signed_scope_started",
            "owned_source_journal_clean",
            "owned_source_unique_current_target",
        )
        if settings.recovery_source_query_only:
            return checks + ("callback_gap_free",)
        return checks + (
            "cancel_through_guard_route_scheduler",
            "terminal_callback_released_inflight",
            "post_cleanup_target_ref_absent",
            "callback_gap_free",
        )
    if settings.fault_mode is not None:
        return common + (
            "fault_admission_closed",
            "fault_read_only_query_terminal",
            "fault_mode_no_mutation",
        )
    if settings.preflight_only:
        return common
    if settings.r1_replay_seed_only:
        # This mode establishes one exact, cleaned-up source for the separate
        # RESTART capability proof. It does not claim the recovery certificate
        # or stable double scan, which remain a distinct R1 acceptance scope.
        return common + (
            "submit_through_guard_route_scheduler",
            "cancel_through_guard_route_scheduler",
            "terminal_callback_released_inflight",
            "post_cleanup_target_ref_absent",
        )
    checks = common + (
        "submit_through_guard_route_scheduler",
        "query_after_submit_terminal",
        "cancel_through_guard_route_scheduler",
        "terminal_callback_released_inflight",
        "post_cleanup_target_ref_absent",
    )
    if settings.r1_authoritative_recovery_probe:
        checks += (
            "r1_controlled_callback_echo",
            "r1_namespace_route_fenced",
            "r1_authoritative_double_scan",
            "r1_recovery_disarmed",
        )
    return checks


def _r1_namespace_release(
    *,
    trading_day: TradingDayEpoch,
    client_id: int,
    adapter_instance_id: str,
    own_order_client_id: int,
    state_path: Path,
):
    """Activate an isolated signed R1 release plus ambient foreign cover.

    The private key never leaves this process and no key, account, endpoint or
    native identifier is written to the F2 report.  The owned lease contains
    only this probe's generated reference; the two signed foreign leases cover
    every other valid value so pre-existing same-account simulator history is
    classified as foreign evidence rather than silently treated as local.
    The control-plane state path is unique to this bounded probe; production
    uses one reviewed release document and durable scope state instead.
    """

    now_wall_ns = time.time_ns()
    private_key = Ed25519PrivateKey.generate()
    signer_id = "tradecore-r1-ephemeral-sim-signer-v1"
    scope = EmtOrderClientNamespaceScope(_LOGICAL_ACCOUNT_ID, trading_day, client_id)
    if not _MIN_SAFE_CLIENT_ORDER_REF <= own_order_client_id <= _MAX_SAFE_CLIENT_ORDER_REF:
        raise ValueError("R1 probe client order reference is outside its safe range")

    def signed_lease(
        *,
        namespace_id: str,
        owner_system_id: str,
        lease_adapter_instance_id: str,
        range_start: int,
        range_end: int,
    ) -> SignedOrderClientIdNamespaceLeaseV1:
        lease = OrderClientIdNamespaceLeaseV1(
            scope=scope,
            namespace_id=namespace_id,
            owner_system_id=owner_system_id,
            adapter_instance_id=lease_adapter_instance_id,
            range_start=range_start,
            range_end=range_end,
            manifest_version=1,
            valid_from_wall_ns=now_wall_ns - 1_000_000_000,
            valid_until_wall_ns=now_wall_ns + 60_000_000_000,
            release_manifest_hash=sha256(b"tradecore-r1-simulated-acceptance-v1").hexdigest(),
            signer_id=signer_id,
        )
        unsigned = SignedOrderClientIdNamespaceLeaseV1(
            lease=lease,
            certificate_reference="ephemeral://tradecore-r1-simulated-acceptance",
            revocation_reference="ephemeral://none",
            signature_b64="AA==",
        )
        return replace(
            unsigned,
            signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
                "ascii"
            ),
        )

    signed = signed_lease(
        namespace_id="tradecore-r1-acceptance-own-v1",
        owner_system_id="tradecore-r1-acceptance",
        lease_adapter_instance_id=adapter_instance_id,
        range_start=own_order_client_id,
        range_end=own_order_client_id,
    )
    foreign = (
        signed_lease(
            namespace_id="tradecore-r1-acceptance-foreign-lower-v1",
            owner_system_id="external-simulator-history",
            lease_adapter_instance_id="external-simulator-history-v1",
            range_start=1,
            range_end=own_order_client_id - 1,
        ),
        signed_lease(
            namespace_id="tradecore-r1-acceptance-foreign-upper-v1",
            owner_system_id="external-simulator-history",
            lease_adapter_instance_id="external-simulator-history-v1",
            range_start=own_order_client_id + 1,
            range_end=4_294_967_295,
        ),
    )
    public = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    trusted_keys = {signer_id: public}
    verifier = EmtOrderClientNamespaceVerifier(trusted_keys, revoked_signer_ids=frozenset())
    runtime_release_hash = sha256(b"tradecore-r1-simulated-acceptance-v1").hexdigest()
    unsigned_release = SignedNamespaceReleaseV1(
        release_id="tradecore-r1-simulated-acceptance-release-v1",
        release_epoch=1,
        release_manifest_hash=runtime_release_hash,
        trust_anchor_hash=namespace_trust_anchor_hash_v1(
            trusted_keys, revoked_signer_ids=frozenset()
        ),
        valid_from_wall_ns=now_wall_ns - 1_000_000_000,
        valid_until_wall_ns=now_wall_ns + 60_000_000_000,
        own=signed,
        foreign=foreign,
        signer_id=signer_id,
        certificate_reference="ephemeral://tradecore-r1-simulated-acceptance",
        revocation_reference="ephemeral://none",
        signature_b64="AA==",
    )
    release = replace(
        unsigned_release,
        signature_b64=base64.b64encode(
            private_key.sign(unsigned_release.canonical_payload())
        ).decode("ascii"),
    )
    return NamespaceReleaseActivationCoordinatorV1(
        verifier=verifier,
        state_store=FileNamespaceReleaseStateStoreV1(state_path),
    ).activate(
        release,
        expected_scope=scope,
        expected_release_manifest_hash=runtime_release_hash,
        expected_trust_anchor_hash=release.trust_anchor_hash,
        now_wall_ns=now_wall_ns,
        bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
            approval_id="f2-r1-simulated-bootstrap-v1",
            release_hash=release.content_hash,
            approver_id="f2-simulated-control-plane",
            approved_at_wall_ns=now_wall_ns,
        ),
    )


def _activate_external_r1_namespace_release(
    *,
    settings: Settings,
    trading_day: TradingDayEpoch,
):
    """Activate a reviewed public R1 release before any SDK/session operation.

    This F2 path is intentionally unavailable unless every control-plane file
    was supplied.  The ephemeral release helper remains a unit/fixture path;
    a Linux acceptance run must pass this public release material explicitly.
    """

    assert settings.r1_namespace_release is not None
    assert settings.r1_namespace_trust is not None
    assert settings.r1_namespace_state is not None
    assert settings.r1_runtime_release_hash is not None
    try:
        release = load_signed_namespace_release_v1_json(
            settings.r1_namespace_release.read_text(encoding="utf-8")
        )
        trust = load_namespace_trust_v1(settings.r1_namespace_trust)
        state_store = FileNamespaceReleaseStateStoreV1(settings.r1_namespace_state)
        previous = state_store.load()
        now_wall_ns = time.time_ns()
        approval = (
            NamespaceReleaseBootstrapApprovalV1(
                approval_id=str(settings.r1_bootstrap_approval_id),
                release_hash=release.content_hash,
                approver_id=str(settings.r1_bootstrap_approver_id),
                approved_at_wall_ns=now_wall_ns,
            )
            if previous is None
            else None
        )
        coordinator = NamespaceReleaseActivationCoordinatorV1(
            verifier=trust.verifier(), state_store=state_store
        )
        activated = coordinator.activate(
            release,
            expected_scope=EmtOrderClientNamespaceScope(
                _LOGICAL_ACCOUNT_ID, trading_day, settings.session_client_id
            ),
            expected_release_manifest_hash=settings.r1_runtime_release_hash,
            expected_trust_anchor_hash=trust.trust_anchor_hash,
            now_wall_ns=now_wall_ns,
            bootstrap_approval=approval,
        )
    except (
        OSError,
        R1ReleasePreflightError,
        NamespaceReleaseActivationErrorV1,
        ValueError,
    ) as exc:
        raise F2ControlledExecutionCycleError("F2 external R1 release activation failed") from exc
    return activated, coordinator


def _load_manual_r2_source_directory(*, settings: Settings, trading_day: TradingDayEpoch) -> Any:
    """Verify a historical release used only to interpret an old R2 route.

    The active release binds the executable currently opening the EMT session.
    This separate, signed historical release is not activated and cannot
    allocate IDs or authorize a new order.  It only supplies the immutable
    directory hash which the crashed mmap fact was written under.
    """

    assert settings.r1_manual_cleanup_source_release is not None
    assert settings.r1_manual_cleanup_source_trust is not None
    try:
        release = load_signed_namespace_release_v1_json(
            settings.r1_manual_cleanup_source_release.read_text(encoding="utf-8")
        )
        trust = load_namespace_trust_v1(settings.r1_manual_cleanup_source_trust)
        expected_scope = EmtOrderClientNamespaceScope(
            _LOGICAL_ACCOUNT_ID, trading_day, settings.session_client_id
        )
        now_wall_ns = time.time_ns()
        if (
            release.scope != expected_scope
            or release.trust_anchor_hash != trust.trust_anchor_hash
            or not release.active_at(now_wall_ns)
        ):
            raise F2ControlledExecutionCycleError("F2 manual R2 source release is not current")
        verifier = trust.verifier()
        verifier.verify_detached(
            signer_id=release.signer_id,
            signature_b64=release.signature_b64,
            payload=release.canonical_payload(),
        )
        return OrderClientIdNamespaceDirectoryV1(
            own=release.own,
            foreign=release.foreign,
            verifier=verifier,
            wall_ns=now_wall_ns,
        )
    except (
        OSError,
        R1ReleasePreflightError,
        NamespaceReleaseActivationErrorV1,
        EmtNamespaceRecoveryError,
        ValueError,
    ) as exc:
        raise F2ControlledExecutionCycleError(
            "F2 manual R2 source release verification failed"
        ) from exc


def _collect_r1_authoritative_order_scan(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    scan_series: EmtAuthoritativeOrderScanSeriesCollectorV1,
    scan_id: str,
    request_prefix: str,
    timeout_seconds: float,
    record_lifecycle: Callable[[str], None],
) -> Any:
    """Collect one complete R1 order snapshot through EMT's cursor API.

    The raw one-shot Orders API can be unsuitable for a large account.  This
    helper keeps the only five recovery scalars in the opt-in collector while
    each 200-row page finishes.  A stalled callback, repeated/non-progressing
    reference, or 512-page overflow remains an explicit failure.
    """

    continuation = 0
    scan_series.begin(scan_id=scan_id)
    record_lifecycle(f"{request_prefix}_started")
    for ordinal in range(_R1_AUTHORITY_MAX_PAGES):
        before_queries, _gaps = observer.snapshot()
        runtime.query(
            _query(
                request_id=f"{request_prefix}-{ordinal}",
                binding=binding,
                kind="orders_by_page",
                page_size=_R1_AUTHORITY_PAGE_SIZE,
                continuation=continuation,
                capture_authoritative_order_rows=True,
            )
        )
        event = _query_terminal(
            runtime=runtime,
            observer=observer,
            kind="orders_by_page",
            timeout_seconds=timeout_seconds,
            prior_count=len(before_queries),
        )
        summary = event.normalized_summary
        if summary.get("page_has_more") == "false":
            scan = scan_series.seal_current()
            if not scan.complete:
                raise F2ControlledExecutionCycleError(
                    "R1 paged authoritative order scan did not complete"
                )
            record_lifecycle(f"{request_prefix}_completed")
            return scan
        if summary.get("page_has_more") != "true":
            raise F2ControlledExecutionCycleError(
                "R1 paged authoritative order scan lacks continuation state"
            )
        raw_reference = summary.get("page_reference")
        if type(raw_reference) is not str or not raw_reference.isdecimal():
            raise F2ControlledExecutionCycleError(
                "R1 paged authoritative order scan reference is invalid"
            )
        next_continuation = int(raw_reference)
        if next_continuation <= continuation:
            raise F2ControlledExecutionCycleError(
                "R1 paged authoritative order scan continuation did not progress"
            )
        continuation = next_continuation
        time.sleep(_R1_AUTHORITY_PAGE_DELAY_SECONDS)
    raise F2ControlledExecutionCycleError("R1 paged authoritative order scan exceeded page bound")


def _rehydrate_r1_r2_route_ledger(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    scan_series: EmtAuthoritativeOrderScanSeriesCollectorV1,
    directory: Any,
    route_ledger: MmapRouteLedgerV2,
    timeout_seconds: float,
    authority: dict[str, object],
    record_lifecycle: Callable[[str], None],
) -> None:
    """Perform the narrow R2 order-scan/replay step, still DISARMED.

    The complete R1 certificate additionally requires terminal semantics for
    positions and public ``RESTART`` replay.  This helper deliberately does
    neither, so it can prove a recovered native mapping without pretending to
    have passed P2 or released a new-order gate.
    """

    scans = [
        _collect_r1_authoritative_order_scan(
            runtime=runtime,
            observer=observer,
            binding=binding,
            scan_series=scan_series,
            scan_id=f"r1-r2-recovery-scan-{label}",
            request_prefix=f"f2-r1-r2-recovery-{label}",
            timeout_seconds=timeout_seconds,
            record_lifecycle=record_lifecycle,
        )
        for label in ("a", "b")
    ]
    first_scan, second_scan = scans
    authority["authoritative_scan_a"] = first_scan.digest
    authority["authoritative_scan_b"] = second_scan.digest
    authority["foreign_reserved_order_count_a"] = sum(
        directory.lookup(order.order_client_id).ownership.value == "FOREIGN_RESERVED"
        for order in first_scan.orders
    )
    authority["foreign_reserved_order_count_b"] = sum(
        directory.lookup(order.order_client_id).ownership.value == "FOREIGN_RESERVED"
        for order in second_scan.orders
    )
    recovery = EmtAuthoritativeRecoveryReconcilerV1(
        directory=directory,
        ledger=route_ledger.load_route_ledger(),
        rehydration_sink=route_ledger,
    ).reconcile(
        first_scan=first_scan,
        second_scan=second_scan,
        callback_gaps_closed=not observer.snapshot()[1],
    )
    authority["recovery_disposition"] = recovery.disposition.value
    authority["recovery_reason_codes"] = list(recovery.reason_codes)
    authority["authoritative_double_scan_complete"] = recovery.stable_double_scan
    authority["foreign_drift_detected"] = recovery.foreign_drift_detected
    authority["foreign_drift_order_count"] = recovery.foreign_drift_order_count
    if (
        recovery.disposition is not RecoveryDispositionV1.RECOVERED_DISARMED
        or len(recovery.rehydrated_execution_ids) != 1
    ):
        raise F2ControlledExecutionCycleError("R1 R2 rehydration did not remain uniquely DISARMED")
    recovered_facts = route_ledger.load_route_ledger().facts()
    if len(recovered_facts) != 1 or recovered_facts[0].order_emt_id is None:
        raise F2ControlledExecutionCycleError("R1 R2 rehydration was not durably projected")
    authority["r2_rehydration_persisted"] = True


def _select_manual_r2_cleanup_target(
    *,
    runtime: ExecutionSessionRuntime,
    observer: _Observer,
    binding: AccountExecutionBinding,
    scan_series: EmtAuthoritativeOrderScanSeriesCollectorV1,
    directory: Any,
    route_ledger: MmapRouteLedgerV2,
    timeout_seconds: float,
    authority: dict[str, object],
    record_lifecycle: Callable[[str], None],
) -> tuple[VenueOrderIdentity, str]:
    """Prove one crashed R2 target before an explicitly approved inverse action.

    This is intentionally narrower than R1 recovery: unrelated rows not covered
    by the historical release remain ``UNKNOWN_NAMESPACE`` and are never
    projected, cancelled, or used to claim a healthy namespace.  The one local
    PreDispatch fact is enough to identify the only target eligible for this
    operator-approved cleanup, but only after two complete scans agree on one
    exact native identity.
    """

    facts = route_ledger.load_route_ledger().facts()
    if len(facts) != 1:
        raise F2ControlledExecutionCycleError("F2 manual R2 cleanup requires one route fact")
    route_fact = facts[0]
    if (
        route_fact.scope != directory.scope
        or route_fact.namespace_manifest_hash != directory.manifest_hash
        or route_fact.order_emt_id is not None
        or not route_fact.call_started
        or directory.lookup(route_fact.order_client_id).ownership.value != "OWN"
    ):
        raise F2ControlledExecutionCycleError(
            "F2 manual R2 cleanup route fact is not an owned R2 boundary"
        )

    scans = [
        _collect_r1_authoritative_order_scan(
            runtime=runtime,
            observer=observer,
            binding=binding,
            scan_series=scan_series,
            scan_id=f"r1-manual-r2-cleanup-scan-{label}",
            request_prefix=f"f2-r1-manual-r2-cleanup-{label}",
            timeout_seconds=timeout_seconds,
            record_lifecycle=record_lifecycle,
        )
        for label in ("a", "b")
    ]
    first_scan, second_scan = scans
    first_matches = tuple(
        item for item in first_scan.orders if item.order_client_id == route_fact.order_client_id
    )
    second_matches = tuple(
        item for item in second_scan.orders if item.order_client_id == route_fact.order_client_id
    )
    authority["authoritative_scan_a"] = first_scan.digest
    authority["authoritative_scan_b"] = second_scan.digest
    authority["manual_unrelated_row_count_a"] = len(first_scan.orders) - len(first_matches)
    authority["manual_unrelated_row_count_b"] = len(second_scan.orders) - len(second_matches)
    authority["manual_target_match_count_a"] = len(first_matches)
    authority["manual_target_match_count_b"] = len(second_matches)
    if (
        not first_scan.complete
        or not second_scan.complete
        or len(first_matches) != 1
        or len(second_matches) != 1
        or first_matches[0] != second_matches[0]
        or observer.snapshot()[1]
    ):
        raise F2ControlledExecutionCycleError(
            "F2 manual R2 cleanup target is not uniquely stable across complete scans"
        )
    target = second_matches[0]
    authority["manual_target_stable"] = True
    authority["recovery_disposition"] = "MANUAL_CLEANUP_ONLY"
    authority["recovery_reason_codes"] = ("UNRELATED_UNKNOWN_NAMESPACE_RETAINED",)
    return (
        VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id=directory.own.adapter_instance_id,
            adapter_client_scope=str(directory.scope.emt_client_id),
            client_order_ref=str(target.order_client_id),
            venue_order_id=str(target.order_emt_id),
        ),
        route_fact.route_payload_hash,
    )


def run(settings: Settings) -> dict[str, Any]:
    settings.validate()
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    owned_source_targets = (
        _load_owned_source_native_acceptance_targets(
            settings.recovery_source_pre_native_journal
        )
        if settings.recovery_source_pre_native_journal is not None
        else ()
    )
    expected_library_hashes = _load_expected_library_hashes(settings.expected_library_hashes)
    accounts = load_simulated_accounts(settings.accounts_file)
    session_account = getattr(accounts, settings.account_profile)
    if settings.tight_burst_order_plan is None:
        fixture = _load_order_fixture(settings.fixture)
    else:
        fixture = load_tight_burst_order_plan(
            settings.tight_burst_order_plan, burst_width=1
        ).request
    trading_day = TradingDayEpoch(normalize_emt_trading_day(settings.trading_day))
    recovery_target = (
        load_single_native_acceptance_target_v1(settings.recovery_route_ledger)
        if settings.recovery_route_ledger is not None
        else None
    )
    manifest, signed, verifier = _activated_test_manifest(
        logical_account_id=_LOGICAL_ACCOUNT_ID,
        client_id=settings.session_client_id,
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
    cycle_root = settings.runtime_dir / "f2-controlled-cycle"
    external_r1_release = settings.r1_namespace_release is not None
    r1_activation_coordinator: NamespaceReleaseActivationCoordinatorV1 | None = None
    if settings.r1_authoritative_recovery_probe and external_r1_release:
        r1_release, r1_activation_coordinator = _activate_external_r1_namespace_release(
            settings=settings, trading_day=trading_day
        )
        if (
            settings.r1_recovery_route_ledger is not None
            or settings.r1_manual_cleanup_route_ledger is not None
            or settings.r1_authoritative_query_only
        ):
            # Recovery and manual inverse actions do not create a new order.
            # Do not burn a namespace value merely because a DISARMED worker
            # needs to reopen the signed scope around an existing route.
            client_order_ref = r1_release.state.high_watermark
        else:
            candidate_client_order_ref = r1_release.state.high_watermark + 1
            if candidate_client_order_ref > r1_release.directory.own.range_end:
                raise F2ControlledExecutionCycleError("F2 external R1 namespace is exhausted")
            # Reservation is a control-plane anti-rollback checkpoint completed
            # before the SDK is imported.  It never runs on C0--C7 and therefore
            # cannot turn a post-native failure into a duplicate client id.
            r1_release = r1_activation_coordinator.checkpoint_high_watermark(
                r1_release, high_watermark=candidate_client_order_ref
            )
            client_order_ref = candidate_client_order_ref
    elif recovery_target is not None:
        client_order_ref = recovery_target.order_client_id
        r1_release = None
    elif owned_source_targets:
        # This instruction is never submitted.  It supplies only the typed
        # cancel request shell after the exact historical target is selected
        # by the authoritative unfinished-order query below.
        client_order_ref = (
            secrets.randbelow(_MAX_SAFE_CLIENT_ORDER_REF - _MIN_SAFE_CLIENT_ORDER_REF)
            + _MIN_SAFE_CLIENT_ORDER_REF
        )
        r1_release = None
    elif settings.r1_authoritative_recovery_probe:
        client_order_ref = (
            secrets.randbelow(_MAX_SAFE_CLIENT_ORDER_REF - _MIN_SAFE_CLIENT_ORDER_REF)
            + _MIN_SAFE_CLIENT_ORDER_REF
        )
        r1_release = _r1_namespace_release(
            trading_day=trading_day,
            client_id=settings.session_client_id,
            adapter_instance_id=ownership.adapter_instance_id,
            own_order_client_id=client_order_ref,
            state_path=(
                cycle_root
                / "control"
                / "r1-namespace-release"
                / str(client_order_ref)
                / "state.json"
            ),
        )
    else:
        client_order_ref = (
            secrets.randbelow(_MAX_SAFE_CLIENT_ORDER_REF - _MIN_SAFE_CLIENT_ORDER_REF)
            + _MIN_SAFE_CLIENT_ORDER_REF
        )
        r1_release = None
    r1_directory = r1_release.directory if r1_release is not None else None
    r1_manual_source_directory = (
        _load_manual_r2_source_directory(settings=settings, trading_day=trading_day)
        if settings.r1_manual_cleanup_route_ledger is not None
        else None
    )
    # F2 must distinguish its exact generated client reference from unrelated
    # account work.  This collector is passed to the native bridge for a
    # single opt-in ``QueryUnfinishedOrders`` scan; it retains only the five
    # reviewed scalar fields and is sealed before any mutation.  R1 reuses the
    # same serial collector after its preflight scan is complete.
    authoritative_scan_series = EmtAuthoritativeOrderScanSeriesCollectorV1()
    r1_scan_series = (
        authoritative_scan_series if settings.r1_authoritative_recovery_probe else None
    )
    r1_certificate_collector: EmtRecoveryCertificateCollectorV1 | None = None
    journal_plan_hash = sha256(b"f2-controlled-cycle-journal-v1").hexdigest()
    binding = AccountExecutionBinding(
        binding_id="f2-controlled-sim-binding",
        binding_version=1,
        account_id=_LOGICAL_ACCOUNT_ID,
        venue_id="EMT",
        adapter_instance_id=ownership.adapter_instance_id,
        adapter_client_scope=ownership.adapter_client_scope,
        ownership_scope_hash=AccountFenceScope(
            _LOGICAL_ACCOUNT_ID, "EMT", ownership.adapter_client_scope
        ).scope_hash,
        mapping_manifest_hash=manifest.manifest_hash,
        capability_manifest_hash=canonical_broker_capabilities_hash(manifest.capabilities),
        journal_plan_hash=journal_plan_hash,
        journal_routing_epoch=1,
        journal_shard_id=0,
    )
    account_binding = AccountJournalBinding(
        _LOGICAL_ACCOUNT_ID,
        "EMT",
        "xuanling:journal:emt_01:",
        0,
    )
    control_root = cycle_root / "control"
    journal_root = cycle_root / "journals"
    scope_root = control_root / "accounts" / binding.ownership_scope_hash
    for directory in (control_root, control_root / "accounts", scope_root, journal_root):
        _secure_directory(directory)
    fence = AccountOwnershipFence(
        runtime_control_root=control_root.absolute(),
        scope=AccountFenceScope(_LOGICAL_ACCOUNT_ID, "EMT", ownership.adapter_client_scope),
        # The ephemeral simulator claim is signed for this bounded operator
        # identity; SessionSupervisor correctly rejects a fence owned by any
        # other instance before it can construct an Adapter.
        instance_id="fengyu",
        host_id="f2-controlled-sim-host",
        process_started_monotonic_ns=time.monotonic_ns(),
        journal_id="f2-controlled-cycle-journal",
        journal_root=journal_root.absolute(),
        stream_generation=1,
    )
    resolver = PreparedEmtNativeRouteResolver(maximum_pending_routes=16)
    pre_native = MmapPreNativeJournal(path=journal_root / "pre-native.journal", capacity=64)
    if r1_directory is not None:
        r1_route_ledger_path = (
            settings.r1_recovery_route_ledger
            if settings.r1_recovery_route_ledger is not None
            else settings.r1_manual_cleanup_route_ledger
            if settings.r1_manual_cleanup_route_ledger is not None
            else journal_root / "r1-route-ledger.mmap"
        )
        r1_route_ledger: MmapRouteLedgerV2 | None = MmapRouteLedgerV2(
            path=r1_route_ledger_path,
            capacity=64,
            directory=(
                r1_manual_source_directory
                if r1_manual_source_directory is not None
                else r1_directory
            ),
        )
    else:
        r1_route_ledger = None
    callback_journal = MmapCallbackOutcomeJournal(
        journals={
            "f2-callback": MmapPreNativeJournal(path=journal_root / "callback.journal", capacity=64)
        },
        cancel_command_journals={
            "f2-cancel": MmapPreNativeJournal(path=journal_root / "cancel.journal", capacity=64)
        },
    )
    order_directory = FastSpiV2RouteOutcomeDirectory(
        config=FastSpiV2RouteOutcomeDirectoryConfig(
            _LOGICAL_ACCOUNT_ID,
            ownership.adapter_instance_id,
            ownership.adapter_client_scope,
            maximum_routes=16,
        )
    )
    cancel_outcomes = FastSpiV2CancelOutcomeObserver(
        config=FastSpiV2CancelOutcomeDirectoryConfig(
            _LOGICAL_ACCOUNT_ID,
            ownership.adapter_instance_id,
            ownership.adapter_client_scope,
            maximum_cancel_routes=16,
        ),
        journal=callback_journal,
    )
    journal = EmtMmapNativeCallJournal(
        route_resolver=resolver,
        order_outcome_directory=order_directory,
        cancel_outcome_observer=cancel_outcomes,
        mmap_pre_native_journals={"f2-native": pre_native},
        mmap_callback_outcomes=callback_journal,
        mmap_route_ledger=r1_route_ledger,
    )
    scheduler = ExecutionScheduler(
        account_id=_LOGICAL_ACCOUNT_ID,
        process_epoch_id="f2-controlled-cycle-v1",
        policy=ExecutionSchedulerPolicy(
            policy_id="f2-controlled-cycle-scheduler",
            version=1,
            policy_hash=sha256(b"f2-controlled-cycle-scheduler-v1").hexdigest(),
            new_order_capacity=1,
            cancel_capacity=1,
            new_order_high_watermark=1,
            cancel_high_watermark=1,
            max_new_order_bytes=1024,
            max_cancel_bytes=1024,
            maximum_inflight_new_orders=1,
        ),
        execution_deadline_policy=ExecutionDeadlinePolicy(
            policy_id="f2-controlled-cycle-execution-deadline",
            version=1,
            policy_hash=sha256(b"f2-controlled-cycle-execution-deadline-v1").hexdigest(),
            native_start_budget_ns=5_000_000_000,
        ),
        cancel_deadline_policy=CancelDeadlinePolicy(
            policy_id="f2-controlled-cycle-cancel-deadline",
            version=1,
            policy_hash=sha256(b"f2-controlled-cycle-cancel-deadline-v1").hexdigest(),
            native_start_budget_ns=5_000_000_000,
        ),
    )
    router = AccountExecutionRouter(
        plan=AccountExecutionRoutePlan(
            plan_id="f2-controlled-cycle-route",
            version=1,
            plan_hash=sha256(b"f2-controlled-cycle-route-v1").hexdigest(),
            bindings=(binding,),
        ),
        schedulers={_LOGICAL_ACCOUNT_ID: scheduler},
    )
    observer = _Observer()
    audit = _Audit()
    lifecycle_stages: list[str] = []
    lifecycle_activity_stages: set[str] = set()
    lifecycle_marker = cycle_root / "lifecycle-stages.log"
    lifecycle_marker.write_text("", encoding="ascii")
    r1_row_failure_counts: dict[str, int] = {}
    r1_row_failure_lock = Lock()

    def record_r1_row_failure(failure_code: str) -> None:
        """Retain a bounded static extraction diagnosis for this R1 probe only."""

        with r1_row_failure_lock:
            r1_row_failure_counts[failure_code] = r1_row_failure_counts.get(failure_code, 0) + 1

    def record_lifecycle(stage: str) -> None:
        # F2 has a bounded start, query and teardown sequence.  Keep enough
        # room to distinguish every teardown boundary when a vendor session
        # stalls, while still retaining no business payload or unbounded log.
        if type(stage) is not str or len(stage) > 128 or len(lifecycle_stages) >= 64:
            return
        # A single paged EMT request can emit hundreds of identical row
        # callbacks.  Retain every distinct callback *shape* once, including
        # both terminal and nonterminal forms, but reserve the bounded evidence
        # budget for the scan's completion and teardown boundaries.
        if stage.startswith(("native_query_callback:", "f2_observer_event:")):
            if stage in lifecycle_activity_stages:
                return
            lifecycle_activity_stages.add(stage)
        lifecycle_stages.append(stage)
        # Session startup occurs outside C0--C7. Persisting this bounded,
        # payload-free marker lets an external timeout distinguish a loader,
        # login or teardown stall without retaining endpoint or SDK output.
        with lifecycle_marker.open("a", encoding="ascii") as handle:
            handle.write(stage + "\n")

    observer.set_activity_observer(record_lifecycle)

    runtime: ExecutionSessionRuntime | None = None
    supervisor: SessionSupervisor | None = None
    controlled_adapter: EmtControlledExecutionAdapter | None = None
    callback_gap_reason_counts: dict[str, int] = {}
    verified_foreign_cancel_callback_count = 0
    verified_foreign_namespace_callback_count = 0
    query_callback_gap_reason_counts: dict[str, int] = {}
    query_authoritative_row_failure_counts: dict[str, int] = {}
    namespace_fence: ExecutionNamespaceFenceV1 | None = None
    provisioned_secret_root: Path | None = None
    provisioned_secret_provider: LinuxProtectedFileSecretProviderV1 | None = None
    provisioned_secret_reference: SecretReferenceV1 | None = None
    provisioned_lease_evidence: dict[str, bool] = {
        "requested": settings.provisioned_secret_lease,
        "active_disarmed_profile_used": False,
        "session_opened_from_short_lease": False,
        "secret_reference_revoked": False,
        "secret_root_removed": False,
    }
    accepted_identity: Any | None = None
    accepted_instruction: Any | None = None
    accepted_route_payload_hash: str | None = None
    checks = {
        "linux_ownership_fence_held": False,
        "sdk_hash_gate_passed": False,
        "disarmed_signed_scope_started": False,
        "preflight_target_ref_absent": False,
        "submit_through_guard_route_scheduler": False,
        "query_after_submit_terminal": False,
        "cancel_through_guard_route_scheduler": False,
        "terminal_callback_released_inflight": False,
        "post_cleanup_target_ref_absent": False,
        "emergency_cleanup_attempted": False,
        "emergency_cleanup_succeeded": False,
        "callback_gap_free": False,
        "fault_admission_closed": False,
        "fault_read_only_query_terminal": False,
        "fault_mode_no_mutation": False,
        "r1_namespace_route_fenced": False,
        "r1_controlled_callback_echo": False,
        "r1_authoritative_double_scan": False,
        "r1_recovery_disarmed": False,
        "r1_rehydration_persisted": False,
        "r1_manual_target_double_scan": False,
        "r1_manual_cleanup_only": False,
        "owned_source_journal_clean": bool(owned_source_targets),
        "owned_source_unique_current_target": False,
    }
    failure_kind: str | None = None
    r1_authority: dict[str, object] | None = None
    if r1_directory is not None:
        assert r1_release is not None
        r1_authority = {
            "enabled": True,
            "recovery_only": settings.r1_recovery_route_ledger is not None,
            "manual_cleanup_only": settings.r1_manual_cleanup_route_ledger is not None,
            "replay_seed_only": settings.r1_replay_seed_only,
            "query_only": settings.r1_authoritative_query_only,
            "namespace_manifest_hash": r1_directory.manifest_hash,
            "manual_source_namespace_manifest_hash": (
                r1_manual_source_directory.manifest_hash
                if r1_manual_source_directory is not None
                else None
            ),
            "namespace_release_hash": r1_release.state.release_hash,
            "namespace_release_epoch": r1_release.state.release_epoch,
            "namespace_trust_anchor_hash": r1_release.state.trust_anchor_hash,
            "route_ledger_pre_dispatch": False,
            "controlled_order_callback_echoed": False,
            "authoritative_double_scan_complete": False,
            "authoritative_scan_a": None,
            "authoritative_scan_b": None,
            "matching_own_order_count_a": 0,
            "matching_own_order_count_b": 0,
            "foreign_drift_detected": False,
            "foreign_drift_order_count": 0,
            "foreign_reserved_order_count_a": 0,
            "foreign_reserved_order_count_b": 0,
            "recovery_certificate": None,
            "controlled_order_query_visibility_confirmed": False,
            "controlled_order_query_visibility_attempts": 0,
            "controlled_order_query_visibility_last_scan_order_count": None,
            "recovery_disposition": None,
            "recovery_reason_codes": [],
            "r2_rehydration_persisted": False,
            "manual_target_stable": False,
            "manual_target_match_count_a": 0,
            "manual_target_match_count_b": 0,
            "manual_unrelated_row_count_a": 0,
            "manual_unrelated_row_count_b": 0,
        }
    try:
        fence.acquire(acquired_monotonic_ns=time.monotonic_ns())
        checks["linux_ownership_fence_held"] = True
        if r1_directory is not None:
            namespace_fence = ExecutionNamespaceFenceV1(
                runtime_control_root=scope_root / "r1-namespace-fences",
                directory=r1_directory,
            )
            namespace_fence.acquire(instance_id="fengyu")
        instruction, route = _submit_plan(
            cycle=0,
            fixture=fixture,
            binding=account_binding,
            trading_day=trading_day,
            ownership=ownership,
            client_order_ref=client_order_ref,
            strategy_order_id="f2-strategy-order",
            execution_suffix=secrets.token_hex(16),
        )
        trace = instruction.consumer_trace
        instruction = replace(
            instruction,
            consumer_trace=ConsumerTrace(
                trace.consumer_id,
                trace.consumer_account_id,
                trace.account_id,
                _STRATEGY_ID,
                trace.strategy_order_id,
                trace.attribution_binding_id,
                trace.binding_version,
                trace.order_origin,
            ),
        )
        resolver.prepare_submit(route, instruction)
        guard_admission, market = _guard(instruction=instruction, audit=audit)
        if settings.r1_crash_after_native_return:
            assert settings.r1_crash_marker is not None
            crash_telemetry = _R1CrashAfterNativeReturnTelemetry(settings.r1_crash_marker)
        else:
            crash_telemetry = None
        foreign_cancel_client_order_lookup: Callable[[int], bool] | None = None
        if r1_directory is not None:

            def is_signed_foreign_cancel_client_order_id(
                client_order_id: int,
                *,
                directory: OrderClientIdNamespaceDirectoryV1 = r1_directory,
            ) -> bool:
                return (
                    directory.lookup(client_order_id).ownership
                    is NamespaceOwnership.FOREIGN_RESERVED
                )

            foreign_cancel_client_order_lookup = is_signed_foreign_cancel_client_order_id

        def build_native_factory(session_settings: SessionSettings) -> EmtPlatformExecutionSessionFactory:
            return EmtPlatformExecutionSessionFactory(
                sdk_provider=EmtPlatformLoader(
                    SdkLoadSettings(
                        sdk_root=settings.sdk_root,
                        expected_hashes=expected_library_hashes,
                    )
                ),
                settings=session_settings,
                order_request_factory=EmtTradeCoreOrderRequestFactory(),
                native_call_journal=journal,
                cancel_request_id_for_order=journal.cancel_request_id_for_order,
                cancel_target_identity_for_order=journal.cancel_target_identity_for_order,
                known_foreign_cancel_client_order_id=foreign_cancel_client_order_lookup,
                mutation_telemetry=crash_telemetry,
                lifecycle_observer=record_lifecycle,
                native_reference_retainer=_ISOLATED_NATIVE_REFERENCE_ANCHOR.append,
            )

        factory: EmtExecutionSdkSessionFactory
        if settings.provisioned_secret_lease:
            # This driver does not turn the short-lived test profile into an
            # operational readiness claim.  It only proves that a Linux
            # protected-file SecretProvider can lease a scope-bound EMT
            # credential into the outer native-session factory and that the
            # provider record is revoked/removed at the end of the isolated
            # acceptance process.  No reference, endpoint or material enters
            # the report.
            provisioned_secret_root = control_root / "provisioned-secret-root"
            _secure_directory(provisioned_secret_root)
            provisioned_secret_provider = LinuxProtectedFileSecretProviderV1(
                secret_root=provisioned_secret_root
            )
            profile_id = f"f2-provisioned-emt-{secrets.token_hex(16)}"
            credential_material = BrokerCredentialMaterialV1(
                broker_account=session_account.account,
                password=session_account.password,
                client_id=str(settings.session_client_id),
                connection=BrokerConnectionV1("BROKER_TCP", accounts.host, accounts.port),
            )
            secret_target = "linux-emt-secret-provider/f2-isolated-acceptance"
            provisioned_secret_reference = provisioned_secret_provider.store(
                target=secret_target,
                material=credential_material,
                adapter_instance_id=ownership.adapter_instance_id,
                account_scope=profile_id,
                idempotency_key=f"f2-provisioned-session:{profile_id}",
            )
            provisioned_profile = AdapterProfileRevisionV1(
                identity=AdapterProfileIdentityV1(
                    profile_id=profile_id,
                    adapter_kind=AdapterKindV1.EMT,
                    venue_id="EMT",
                    adapter_instance_id=ownership.adapter_instance_id,
                    account_alias="f2-isolated-simulated",
                ),
                revision=1,
                state=ProfileStateV1.ACTIVE_DISARMED,
                secret_reference=provisioned_secret_reference,
                material_digest=ProfileMaterialDigestV1(
                    credential_digest=credential_material.credential_digest(),
                    endpoint_digest=credential_material.connection.endpoint_digest(),
                    envelope_digest=sha256(profile_id.encode("ascii")).hexdigest(),
                    recipient_key_id="f2-isolated-provisioned-lease",
                    mapping_manifest_hash=manifest.manifest_hash,
                    capability_manifest_hash=canonical_broker_capabilities_hash(
                        manifest.capabilities
                    ),
                    release_manifest_hash=journal_plan_hash,
                ),
                created_wall_ns=time.time_ns(),
                validation_evidence_id="f2-isolated-disarmed-validation",
                approval_id="f2-isolated-operator-approval",
            )
            factory = ProvisionedEmtExecutionSessionFactoryV1(
                runtime_profile=ProvisionedEmtExecutionProfileV1(
                    profile=provisioned_profile,
                    sdk_log_dir=cycle_root / "sdk-logs",
                    expected_client_id=settings.session_client_id,
                    account_profile=settings.account_profile,
                    sdk_log_level=0,
                ),
                secret_provider=provisioned_secret_provider,
                factory_builder=build_native_factory,
            )
            provisioned_lease_evidence["active_disarmed_profile_used"] = True
        else:
            factory = build_native_factory(
                SessionSettings(
                    host=accounts.host,
                    port=accounts.port,
                    account=session_account.account,
                    password=session_account.password,
                    client_id=settings.session_client_id,
                    sdk_log_dir=cycle_root / "sdk-logs",
                    account_profile=settings.account_profile,
                    sdk_log_level=0,
                )
            )

        def adapter_factory() -> EmtControlledExecutionAdapter:
            nonlocal controlled_adapter
            controlled_adapter = EmtAdapterProfile.create_controlled_execution_adapter(
                mapping,
                session_factory=factory,
                client_order_ref_factory=resolver.client_order_ref_for,
                expected_api_version="2.27.0",
                expected_trading_day=trading_day,
                session_epoch=_SESSION_EPOCH,
                signed_scope_ownership=signed,
                scope_ownership_verifier=verifier,
                event_ingress_capacity=1024,
                broker_event_observer_wrapper=lambda downstream: journal.wrap_broker_event_observer(
                    downstream,
                    native_inflight_release=scheduler.release_new_order_inflight,
                ),
                lifecycle_observer=record_lifecycle,
                native_reference_retainer=_ISOLATED_NATIVE_REFERENCE_ANCHOR.append,
                activated_namespace_release=r1_release,
                authoritative_order_observer=authoritative_scan_series,
                authoritative_row_failure_observer=(
                    record_r1_row_failure if r1_scan_series is not None else None
                ),
            )
            return controlled_adapter

        supervisor = SessionSupervisor(
            binding=binding,
            activated_mapping=mapping,
            ownership_fence=fence,
            adapter_factory=adapter_factory,
        )
        runtime = ExecutionSessionRuntime(
            binding=binding,
            router=router,
            scheduler=scheduler,
            supervisor=supervisor,
            guard_admission=guard_admission,
        )
        runtime.start(observer, evaluated_wall_ns=time.time_ns())
        if settings.provisioned_secret_lease:
            provisioned_lease_evidence["session_opened_from_short_lease"] = True
        checks["sdk_hash_gate_passed"] = True
        checks["disarmed_signed_scope_started"] = True

        if settings.r1_recovery_route_ledger is not None:
            assert r1_authority is not None
            assert r1_directory is not None
            assert r1_route_ledger is not None
            assert r1_scan_series is not None
            _rehydrate_r1_r2_route_ledger(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=r1_scan_series,
                directory=r1_directory,
                route_ledger=r1_route_ledger,
                timeout_seconds=settings.callback_timeout_seconds,
                authority=r1_authority,
                record_lifecycle=record_lifecycle,
            )
            checks["r1_namespace_route_fenced"] = True
            checks["r1_authoritative_double_scan"] = True
            checks["r1_recovery_disarmed"] = True
            checks["r1_rehydration_persisted"] = True
            _events, gaps = observer.snapshot()
            if gaps or runtime.snapshot().callback_gap_count:
                raise F2ControlledExecutionCycleError(
                    "F2 R1 R2 rehydration observed a callback gap"
                )
            checks["callback_gap_free"] = True
            record_lifecycle("f2_r1_r2_rehydration_completed")
            raise _PreflightComplete

        if settings.r1_manual_cleanup_route_ledger is not None:
            assert r1_authority is not None
            assert r1_directory is not None
            assert r1_manual_source_directory is not None
            assert r1_route_ledger is not None
            assert r1_scan_series is not None
            target, target_route_payload_hash = _select_manual_r2_cleanup_target(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=r1_scan_series,
                directory=r1_manual_source_directory,
                route_ledger=r1_route_ledger,
                timeout_seconds=settings.callback_timeout_seconds,
                authority=r1_authority,
                record_lifecycle=record_lifecycle,
            )
            checks["r1_manual_target_double_scan"] = True
            checks["r1_manual_cleanup_only"] = True
            record_lifecycle("f2_r1_manual_r2_cleanup_before")
            _admit_controlled_cancel(
                runtime=runtime,
                resolver=resolver,
                journal=journal,
                instruction=instruction,
                target=target,
                target_route_payload_hash=target_route_payload_hash,
                binding=account_binding,
            )
            checks["cancel_through_guard_route_scheduler"] = True
            _wait_for_terminal_cleanup(
                runtime=runtime,
                observer=observer,
                scheduler=scheduler,
                timeout_seconds=settings.callback_timeout_seconds,
            )
            checks["terminal_callback_released_inflight"] = True
            if target.client_order_ref is None:
                raise F2ControlledExecutionCycleError(
                    "F2 manual R2 cleanup target lacks a client order reference"
                )
            _wait_for_target_unfinished_order_absent(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=authoritative_scan_series,
                target_client_order_ref=int(target.client_order_ref),
                timeout_seconds=settings.callback_timeout_seconds,
                request_prefix="f2-r1-manual-r2-post-cleanup-unfinished",
            )
            checks["post_cleanup_target_ref_absent"] = True
            _events, gaps = observer.snapshot()
            if gaps or runtime.snapshot().callback_gap_count:
                raise F2ControlledExecutionCycleError(
                    "F2 R1 manual R2 cleanup observed a callback gap"
                )
            checks["callback_gap_free"] = True
            record_lifecycle("f2_r1_manual_r2_cleanup_completed")
            raise _PreflightComplete

        record_lifecycle("f2_preflight_unfinished_query_before")
        preflight_scan, _preflight = _collect_authoritative_unfinished_order_scan(
            runtime=runtime,
            observer=observer,
            binding=binding,
            scan_series=authoritative_scan_series,
            scan_id="f2-preflight-unfinished-owned-target",
            request_id="f2-preflight-unfinished",
            timeout_seconds=settings.callback_timeout_seconds,
        )
        record_lifecycle("f2_preflight_unfinished_query_returned")
        if owned_source_targets:
            source_current_matches = tuple(
                candidate
                for candidate in owned_source_targets
                if any(
                    item.order_client_id == candidate.order_client_id
                    and item.order_emt_id == candidate.order_emt_id
                    for item in preflight_scan.orders
                )
            )
            if len(source_current_matches) != 1:
                raise F2ControlledExecutionCycleError(
                    "F2 owned-source cleanup requires one exact current authoritative target"
                )
            recovery_target = source_current_matches[0]
            checks["owned_source_unique_current_target"] = True
            record_lifecycle("f2_owned_source_exact_target_selected")
            if settings.recovery_source_query_only:
                _events, gaps = observer.snapshot()
                if gaps or runtime.snapshot().callback_gap_count:
                    raise F2ControlledExecutionCycleError(
                        "F2 owned-source query-only observed a callback gap"
                    )
                checks["callback_gap_free"] = True
                record_lifecycle("f2_owned_source_query_only_completed")
                raise _PreflightComplete
        elif recovery_target is None:
            if any(item.order_client_id == client_order_ref for item in preflight_scan.orders):
                raise F2ControlledExecutionCycleError(
                    "F2 preflight exact client reference is already unfinished"
                )
            checks["preflight_target_ref_absent"] = True

        if recovery_target is not None:
            recovery_matches = tuple(
                item
                for item in preflight_scan.orders
                if (
                    item.order_client_id == recovery_target.order_client_id
                    and item.order_emt_id == recovery_target.order_emt_id
                )
            )
            if len(recovery_matches) != 1:
                raise F2ControlledExecutionCycleError(
                    "F2 targeted cleanup exact client reference is not unfinished"
                )
            recovery_identity = VenueOrderIdentity(
                venue_id="EMT",
                adapter_instance_id=ownership.adapter_instance_id,
                adapter_client_scope=ownership.adapter_client_scope,
                client_order_ref=str(recovery_target.order_client_id),
                venue_order_id=str(recovery_target.order_emt_id),
            )
            record_lifecycle("f2_targeted_cleanup_before")
            _admit_controlled_cancel(
                runtime=runtime,
                resolver=resolver,
                journal=journal,
                instruction=instruction,
                target=recovery_identity,
                target_route_payload_hash=route.command.payload_hash,
                binding=account_binding,
            )
            checks["cancel_through_guard_route_scheduler"] = True
            _wait_for_terminal_cleanup(
                runtime=runtime,
                observer=observer,
                scheduler=scheduler,
                timeout_seconds=settings.callback_timeout_seconds,
            )
            checks["terminal_callback_released_inflight"] = True
            _wait_for_target_unfinished_order_absent(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=authoritative_scan_series,
                target_client_order_ref=recovery_target.order_client_id,
                timeout_seconds=settings.callback_timeout_seconds,
                request_prefix="f2-targeted-cleanup-unfinished",
            )
            checks["post_cleanup_target_ref_absent"] = True
            _events, gaps = observer.snapshot()
            if gaps or runtime.snapshot().callback_gap_count:
                raise F2ControlledExecutionCycleError("F2 targeted cleanup observed a callback gap")
            checks["callback_gap_free"] = True
            record_lifecycle("f2_targeted_cleanup_completed")
            raise _PreflightComplete
        if settings.fault_mode is not None:
            record_lifecycle(f"f2_fault_{settings.fault_mode}_before")
            if settings.fault_mode == "host-health-integrity":
                runtime.on_host_health_integrity("f2-acceptance", "controlled-probe")
            else:
                runtime.on_host_health_capacity("f2-acceptance", "controlled-probe")
            record_lifecycle(f"f2_fault_{settings.fault_mode}_latched")
            try:
                runtime.admit_submit(
                    instruction,
                    serialized_size_bytes=512,
                    market=market,
                    kill_switches=KillSwitchSnapshot(control_version=0),
                    admitted_monotonic_ns=time.monotonic_ns(),
                )
            except ExecutionSessionRuntimeError:
                checks["fault_admission_closed"] = True
            else:
                raise F2ControlledExecutionCycleError(
                    "F2 host-health fault admitted a new mutation"
                )
            before_queries, _gaps = observer.snapshot()
            record_lifecycle("f2_fault_query_after_latch_before")
            runtime.query(
                _query(
                    request_id="f2-fault-unfinished-after-latch",
                    binding=binding,
                    kind="unfinished_orders",
                )
            )
            record_lifecycle("f2_fault_query_after_latch_returned")
            _query_terminal(
                runtime=runtime,
                observer=observer,
                kind="unfinished_orders",
                timeout_seconds=settings.callback_timeout_seconds,
                prior_count=len(before_queries),
            )
            checks["fault_read_only_query_terminal"] = True
            scheduler_snapshot = scheduler.snapshot()
            if (
                scheduler_snapshot.counters["new_order_accepted"] != 0
                or scheduler_snapshot.counters["cancel_accepted"] != 0
                or scheduler_snapshot.depths["new_order_inflight"] != 0
            ):
                raise F2ControlledExecutionCycleError(
                    "F2 host-health fault reached a native mutation lane"
                )
            checks["fault_mode_no_mutation"] = True
        if settings.preflight_only:
            if settings.r1_authoritative_query_only:
                assert r1_scan_series is not None
                assert r1_authority is not None
                scan = _collect_r1_authoritative_order_scan(
                    runtime=runtime,
                    observer=observer,
                    binding=binding,
                    scan_series=r1_scan_series,
                    scan_id="r1-authoritative-query-only",
                    request_prefix="f2-r1-query-only",
                    timeout_seconds=settings.callback_timeout_seconds,
                    record_lifecycle=record_lifecycle,
                )
                r1_authority["query_only_authoritative_scan_complete"] = scan.complete
                r1_authority["query_only_authoritative_scan_order_count"] = len(scan.orders)
                r1_authority["query_only_authoritative_scan_digest"] = scan.digest
                if not scan.complete:
                    raise F2ControlledExecutionCycleError(
                        "R1 query-only authoritative scan did not complete"
                    )
            _events, gaps = observer.snapshot()
            if gaps or runtime.snapshot().callback_gap_count:
                raise F2ControlledExecutionCycleError("F2 preflight observed a callback gap")
            checks["callback_gap_free"] = True
            raise _PreflightComplete

        before_submit_events, _ = observer.snapshot()
        admitted = runtime.admit_submit(
            instruction,
            serialized_size_bytes=512,
            market=market,
            kill_switches=KillSwitchSnapshot(control_version=0),
            admitted_monotonic_ns=time.monotonic_ns(),
        )
        if not admitted.decision.accepted or not isinstance(
            admitted.authorized_result, IngressAdmission
        ):
            raise F2ControlledExecutionCycleError("F2 guard rejected its bounded controlled submit")
        with journal.mmap_native_lane("f2-native"):
            dispatched = runtime.dispatch_once(checked_monotonic_ns=time.monotonic_ns())
        if dispatched is None or dispatched.outcome is not ExecutionDispatchOutcome.SUBMIT_ACCEPTED:
            raise F2ControlledExecutionCycleError(
                "F2 composed submit did not reach native acceptance"
            )
        identity = (
            dispatched.submit_accepted.venue_order_identity if dispatched.submit_accepted else None
        )
        if identity is None or identity.venue_order_id is None:
            raise F2ControlledExecutionCycleError("F2 native submit lacks a venue identity")
        accepted_identity = identity
        accepted_instruction = instruction
        accepted_route_payload_hash = route.command.payload_hash
        checks["submit_through_guard_route_scheduler"] = True
        if r1_route_ledger is not None:
            route_ledger_after_submit = r1_route_ledger.load_route_ledger()
            if route_ledger_after_submit.route_for_client_id(client_order_ref) is None:
                raise F2ControlledExecutionCycleError(
                    "R1 route ledger lacks the controlled pre-dispatch fact"
                )
            assert r1_authority is not None
            r1_authority["route_ledger_pre_dispatch"] = True

        if r1_scan_series is not None and not settings.r1_replay_seed_only:
            assert r1_authority is not None
            r1_authority["controlled_order_callback_echoed"] = (
                _wait_for_controlled_order_callback_echo(
                    runtime=runtime,
                    observer=observer,
                    expected_client_order_ref=client_order_ref,
                    timeout_seconds=settings.callback_timeout_seconds,
                    prior_count=len(before_submit_events),
                )
            )
            if not r1_authority["controlled_order_callback_echoed"]:
                raise F2ControlledExecutionCycleError(
                    "R1 controlled order callback did not echo its pre-admitted client reference"
                )
            checks["r1_controlled_callback_echo"] = True
            for attempt in range(1, _R1_ORDER_VISIBILITY_ATTEMPTS + 1):
                record_lifecycle(f"f2_r1_visibility_query_{attempt}_before")
                visibility_scan = _collect_r1_authoritative_order_scan(
                    runtime=runtime,
                    observer=observer,
                    binding=binding,
                    scan_series=r1_scan_series,
                    scan_id=f"r1-authoritative-visibility-{attempt}",
                    request_prefix=f"f2-r1-visibility-{attempt}",
                    timeout_seconds=settings.callback_timeout_seconds,
                    record_lifecycle=record_lifecycle,
                )
                visible_count = sum(
                    order.order_client_id == client_order_ref for order in visibility_scan.orders
                )
                r1_authority["controlled_order_query_visibility_last_scan_order_count"] = len(
                    visibility_scan.orders
                )
                r1_authority["controlled_order_query_visibility_attempts"] = attempt
                if visibility_scan.complete and visible_count == 1:
                    r1_authority["controlled_order_query_visibility_confirmed"] = True
                    break
                time.sleep(_R1_ORDER_VISIBILITY_WAIT_SECONDS)
            if not r1_authority["controlled_order_query_visibility_confirmed"]:
                raise F2ControlledExecutionCycleError(
                    "R1 controlled order did not become uniquely visible in the bounded query window"
                )

        first_scan: Any | None = None
        if r1_scan_series is not None and not settings.r1_replay_seed_only:
            record_lifecycle("f2_post_submit_orders_query_before")
            first_scan = _collect_r1_authoritative_order_scan(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=r1_scan_series,
                scan_id="r1-authoritative-scan-a",
                request_prefix="f2-post-submit-orders",
                timeout_seconds=settings.callback_timeout_seconds,
                record_lifecycle=record_lifecycle,
            )
        elif not settings.r1_replay_seed_only:
            record_lifecycle("f2_post_submit_orders_query_before")
            before_queries, _gaps = observer.snapshot()
            runtime.query(
                _query(request_id="f2-post-submit-orders", binding=binding, kind="orders")
            )
            record_lifecycle("f2_post_submit_orders_query_returned")
            _query_terminal(
                runtime=runtime,
                observer=observer,
                kind="orders",
                timeout_seconds=settings.callback_timeout_seconds,
                prior_count=len(before_queries),
            )
        if not settings.r1_replay_seed_only:
            checks["query_after_submit_terminal"] = True
        if r1_scan_series is not None and not settings.r1_replay_seed_only:
            assert first_scan is not None
            r1_certificate_collector = EmtRecoveryCertificateCollectorV1(
                certificate_id="f2-r1-authoritative-recovery-certificate",
                account_id=binding.account_id,
                adapter_instance_id=binding.adapter_instance_id,
                adapter_client_scope=ownership.adapter_client_scope,
                trading_day=trading_day,
                account_profile=settings.account_profile,
            )
            _collect_r1_certificate_queries(
                runtime=runtime,
                observer=observer,
                binding=binding,
                collector=r1_certificate_collector,
                timeout_seconds=settings.callback_timeout_seconds,
                record_lifecycle=record_lifecycle,
            )
            record_lifecycle("f2_r1_second_orders_query_before")
            second_scan = _collect_r1_authoritative_order_scan(
                runtime=runtime,
                observer=observer,
                binding=binding,
                scan_series=r1_scan_series,
                scan_id="r1-authoritative-scan-b",
                request_prefix="f2-r1-second-orders",
                timeout_seconds=settings.callback_timeout_seconds,
                record_lifecycle=record_lifecycle,
            )
            assert r1_authority is not None
            r1_authority["authoritative_scan_a"] = {
                "complete": first_scan.complete,
                "order_count": len(first_scan.orders),
                "digest": first_scan.digest,
            }
            r1_authority["authoritative_scan_b"] = {
                "complete": second_scan.complete,
                "order_count": len(second_scan.orders),
                "digest": second_scan.digest,
            }
            if not first_scan.complete or not second_scan.complete:
                raise F2ControlledExecutionCycleError(
                    "R1 authoritative order query did not complete"
                )
            matching_own_orders_a = sum(
                order.order_client_id == client_order_ref for order in first_scan.orders
            )
            matching_own_orders_b = sum(
                order.order_client_id == client_order_ref for order in second_scan.orders
            )
            if matching_own_orders_a != 1 or matching_own_orders_b != 1:
                raise F2ControlledExecutionCycleError(
                    "R1 authoritative scans did not each echo one controlled order_client_id"
                )
            r1_authority["matching_own_order_count_a"] = matching_own_orders_a
            r1_authority["matching_own_order_count_b"] = matching_own_orders_b
            assert r1_directory is not None
            assert r1_route_ledger is not None
            r1_authority["foreign_reserved_order_count_a"] = sum(
                r1_directory.lookup(order.order_client_id).ownership.value == "FOREIGN_RESERVED"
                for order in first_scan.orders
            )
            r1_authority["foreign_reserved_order_count_b"] = sum(
                r1_directory.lookup(order.order_client_id).ownership.value == "FOREIGN_RESERVED"
                for order in second_scan.orders
            )
            recovery = EmtAuthoritativeRecoveryReconcilerV1(
                directory=r1_directory,
                ledger=r1_route_ledger.load_route_ledger(),
                rehydration_sink=r1_route_ledger,
            ).reconcile(
                first_scan=first_scan,
                second_scan=second_scan,
                callback_gaps_closed=not observer.snapshot()[1],
            )
            r1_authority["recovery_disposition"] = recovery.disposition.value
            r1_authority["recovery_reason_codes"] = list(recovery.reason_codes)
            r1_authority["authoritative_double_scan_complete"] = recovery.stable_double_scan
            r1_authority["foreign_drift_detected"] = recovery.foreign_drift_detected
            r1_authority["foreign_drift_order_count"] = recovery.foreign_drift_order_count
            if recovery.disposition is not RecoveryDispositionV1.RECOVERED_DISARMED:
                raise F2ControlledExecutionCycleError(
                    "R1 recovery did not remain RECOVERED_DISARMED"
                )
            certificate_result = r1_certificate_collector.build(
                # Public-topic subscription is not itself a replay-complete
                # proof.  The real restart/reconnect callback probe records
                # this as true only after the configured replay window has
                # terminated with no gap; this bounded submit cycle keeps it
                # deliberately false rather than overstating recovery scope.
                callback_replay_complete=False,
                callback_gaps_closed=not observer.snapshot()[1],
                stable_double_scan=recovery.stable_double_scan,
            )
            r1_authority["recovery_certificate"] = {
                "profile_id": certificate_result.profile.profile_id,
                "profile_hash": certificate_result.profile.profile_hash,
                "certificate_hash": certificate_result.certificate.certificate_hash,
                "query_digest_count": len(certificate_result.certificate.query_digests),
                "missing_query_kinds": list(certificate_result.missing_query_kinds),
                "pending_paged_query_kinds": list(certificate_result.pending_paged_query_kinds),
                "negative_confirmation_complete": certificate_result.negative_confirmation_complete,
                "incompleteness_reasons": list(certificate_result.incompleteness_reasons),
            }
            checks["r1_namespace_route_fenced"] = True
            checks["r1_authoritative_double_scan"] = True
            checks["r1_recovery_disarmed"] = True

        if settings.hold_before_cancel_seconds > 0:
            record_lifecycle("f2_hold_before_cancel_started")
            time.sleep(settings.hold_before_cancel_seconds)
            record_lifecycle("f2_hold_before_cancel_completed")
        _admit_controlled_cancel(
            runtime=runtime,
            resolver=resolver,
            journal=journal,
            instruction=instruction,
            target=identity,
            target_route_payload_hash=route.command.payload_hash,
            binding=account_binding,
        )
        checks["cancel_through_guard_route_scheduler"] = True
        _wait_for_terminal_cleanup(
            runtime=runtime,
            observer=observer,
            scheduler=scheduler,
            timeout_seconds=settings.callback_timeout_seconds,
        )
        checks["terminal_callback_released_inflight"] = True

        record_lifecycle("f2_post_cleanup_unfinished_query_before")
        _wait_for_target_unfinished_order_absent(
            runtime=runtime,
            observer=observer,
            binding=binding,
            scan_series=authoritative_scan_series,
            target_client_order_ref=client_order_ref,
            timeout_seconds=settings.callback_timeout_seconds,
            request_prefix="f2-post-cleanup-unfinished",
        )
        record_lifecycle("f2_post_cleanup_unfinished_query_returned")
        checks["post_cleanup_target_ref_absent"] = True
        _events, gaps = observer.snapshot()
        if gaps or runtime.snapshot().callback_gap_count:
            raise F2ControlledExecutionCycleError("F2 observed a callback gap")
        checks["callback_gap_free"] = True
    except _PreflightComplete:
        pass
    except BaseException as exc:
        failure_kind = type(exc).__name__
        # Once EMT has accepted this process's submit, all subsequent failure
        # paths must attempt the same one exact inverse side effect before the
        # session stops.  Preserve the original failure result; the report
        # contains only bounded cleanup state and no order identifiers.
        if (
            runtime is not None
            and accepted_identity is not None
            and accepted_instruction is not None
            and accepted_route_payload_hash is not None
            and not checks["cancel_through_guard_route_scheduler"]
        ):
            checks["emergency_cleanup_attempted"] = True
            try:
                record_lifecycle("f2_emergency_cleanup_before")
                _admit_controlled_cancel(
                    runtime=runtime,
                    resolver=resolver,
                    journal=journal,
                    instruction=accepted_instruction,
                    target=accepted_identity,
                    target_route_payload_hash=accepted_route_payload_hash,
                    binding=account_binding,
                )
                _wait_for_terminal_cleanup(
                    runtime=runtime,
                    observer=observer,
                    scheduler=scheduler,
                    timeout_seconds=settings.callback_timeout_seconds,
                    allow_callback_gaps=True,
                )
                _wait_for_target_unfinished_order_absent(
                    runtime=runtime,
                    observer=observer,
                    binding=binding,
                    scan_series=authoritative_scan_series,
                    target_client_order_ref=int(accepted_identity.client_order_ref),
                    timeout_seconds=settings.callback_timeout_seconds,
                    request_prefix="f2-emergency-post-cleanup-unfinished",
                    allow_callback_gaps=True,
                )
                checks["post_cleanup_target_ref_absent"] = True
                checks["emergency_cleanup_succeeded"] = True
                record_lifecycle("f2_emergency_cleanup_completed")
            except BaseException:
                record_lifecycle("f2_emergency_cleanup_failed")
    finally:
        if controlled_adapter is not None:
            # Snapshot the safe bridge reason vocabulary before stopping the
            # native session.  These values contain no vendor row, request
            # id, account, or credential and cannot weaken fail-closed gaps.
            controlled_snapshot = controlled_adapter.snapshot()
            callback_gap_reason_counts = dict(
                sorted(controlled_snapshot.callback_gap_reason_counts.items())
            )
            verified_foreign_cancel_callback_count = (
                controlled_snapshot.verified_foreign_cancel_callback_count
            )
            verified_foreign_namespace_callback_count = (
                controlled_snapshot.verified_foreign_namespace_callback_count
            )
            query_callback_gap_reason_counts = dict(
                sorted(controlled_snapshot.query_callback_gap_reason_counts.items())
            )
            query_authoritative_row_failure_counts = dict(
                sorted(controlled_snapshot.query_authoritative_row_failure_counts.items())
            )
        if runtime is not None:
            try:
                record_lifecycle("f2_runtime_stop_before")
                runtime.stop()
                record_lifecycle("f2_runtime_stop_returned")
            except BaseException:
                if failure_kind is None:
                    failure_kind = "RuntimeStopError"
        try:
            record_lifecycle("f2_ownership_release_before")
            fence.release()
            record_lifecycle("f2_ownership_release_returned")
        except BaseException:
            if failure_kind is None:
                failure_kind = "OwnershipFenceReleaseError"
        if namespace_fence is not None:
            try:
                namespace_fence.release()
            except BaseException:
                if failure_kind is None:
                    failure_kind = "NamespaceFenceReleaseError"
        record_lifecycle("f2_journal_close_before")
        journal.close()
        record_lifecycle("f2_journal_close_returned")
        if (
            provisioned_secret_provider is not None
            and provisioned_secret_reference is not None
        ):
            try:
                provisioned_secret_provider.revoke(
                    reference=provisioned_secret_reference,
                    reason_code="F2_ISOLATED_SESSION_COMPLETED",
                )
                provisioned_lease_evidence["secret_reference_revoked"] = True
            except BaseException:
                if failure_kind is None:
                    failure_kind = "ProvisionedSecretRevokeError"
        if provisioned_secret_root is not None:
            try:
                # The root was created in this exact per-run directory and
                # contains only a provider record for this isolated process.
                # Remove it after explicit revocation so the F2 evidence tree
                # never retains credentials after the report is emitted.
                shutil.rmtree(provisioned_secret_root)
                provisioned_lease_evidence["secret_root_removed"] = True
            except OSError:
                if failure_kind is None:
                    failure_kind = "ProvisionedSecretRootRemovalError"
    if r1_authority is not None:
        with r1_row_failure_lock:
            r1_authority["authoritative_row_extraction_failure_counts"] = dict(
                sorted(r1_row_failure_counts.items())
            )
    required_check_names = _required_check_names(settings)
    accepted = failure_kind is None and all(checks[name] for name in required_check_names)
    if settings.r1_replay_witness_proposal_output is not None and accepted:
        # Write this only after the controlled submit, the complete query
        # certificate, inverse action, and zero-unfinished verification have
        # all passed.  The document contains a digest of the two EMT ids, not
        # the ids themselves. It is an unsigned proposal: a separate offline
        # signer must bind it to the R1 release before a RESTART-only observer
        # can use it as evidence.
        try:
            if accepted_identity is None or accepted_identity.client_order_ref is None:
                raise F2ControlledExecutionCycleError(
                    "F2 accepted submit lacks order_client_id for replay witness"
                )
            if accepted_identity.venue_order_id is None:
                raise F2ControlledExecutionCycleError(
                    "F2 accepted submit lacks order_emt_id for replay witness"
                )
            if r1_release is None or r1_directory is None or r1_authority is None:
                raise F2ControlledExecutionCycleError(
                    "F2 replay witness lacks an activated external R1 namespace"
                )
            order_client_id = int(accepted_identity.client_order_ref)
            order_emt_id = int(accepted_identity.venue_order_id)
            if order_client_id != client_order_ref:
                raise F2ControlledExecutionCycleError(
                    "F2 accepted order_client_id disagrees with the signed namespace"
                )
            issued_at_wall_ns = time.time_ns()
            witness = issue_emt_replay_witness_v1(
                witness_id=f"f2-r1-replay-{secrets.token_hex(16)}",
                account_config_profile=settings.account_profile,
                # This value participates only in the in-memory digest.  It must
                # be the EMT account rather than TradeCore's logical binding id,
                # because the independent RESTART subscriber receives the real
                # credential profile when it validates the callback.
                account_id=session_account.account,
                adapter_instance_id=ownership.adapter_instance_id,
                adapter_client_scope=ownership.adapter_client_scope,
                trading_day=trading_day,
                namespace_release_hash=r1_release.state.release_hash,
                namespace_manifest_hash=r1_directory.manifest_hash,
                order_client_id=order_client_id,
                order_emt_id=order_emt_id,
                issued_at_wall_ns=issued_at_wall_ns,
                expires_at_wall_ns=issued_at_wall_ns + 30 * 60 * 1_000_000_000,
            )
            write_emt_replay_witness_proposal_v1(
                settings.r1_replay_witness_proposal_output, witness
            )
            r1_authority["replay_witness_proposal_sha256"] = witness.content_sha256
        except (OSError, ValueError, F2ControlledExecutionCycleError):
            failure_kind = "ReplayWitnessProposalIssueError"
            accepted = False
    report = _f2_report(
        status="passed" if accepted else "failed",
        checks=checks,
        observer=observer,
        runtime=runtime,
        supervisor=supervisor,
        scheduler=scheduler,
        audit=audit,
        failure_kind=failure_kind,
        preflight_only=settings.preflight_only,
        fault_mode=settings.fault_mode,
        account_profile=settings.account_profile,
        owned_source_cleanup=settings.recovery_source_pre_native_journal is not None,
        owned_source_query_only=settings.recovery_source_query_only,
        lifecycle_stages=tuple(lifecycle_stages),
        r1_authority=r1_authority,
        callback_gap_reason_counts=callback_gap_reason_counts,
        verified_foreign_cancel_callback_count=verified_foreign_cancel_callback_count,
        verified_foreign_namespace_callback_count=verified_foreign_namespace_callback_count,
        query_callback_gap_reason_counts=query_callback_gap_reason_counts,
        query_authoritative_row_failure_counts=query_authoritative_row_failure_counts,
        provisioned_lease_evidence=provisioned_lease_evidence,
    )
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--expected-library-hashes", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--session-client-id", type=int, required=True)
    parser.add_argument(
        "--account-profile",
        choices=("ordinary", "credit"),
        default="ordinary",
        help="choose the simulated credential profile; credit mode is query-only",
    )
    parser.add_argument("--callback-timeout", type=float, default=20.0)
    parser.add_argument("--tight-burst-order-plan", type=Path)
    parser.add_argument(
        "--recovery-route-ledger",
        type=Path,
        help=(
            "run a one-time exact cleanup target recovered from one committed F2 route ledger; "
            "cannot be combined with an R1 probe, fault mode, or preflight-only"
        ),
    )
    parser.add_argument(
        "--recovery-source-pre-native-journal",
        type=Path,
        help=(
            "one immutable historical mmap source Journal: select exactly one currently "
            "unfinished TradeCore-owned pair, cancel it once, and never retry"
        ),
    )
    parser.add_argument(
        "--recovery-source-query-only",
        action="store_true",
        help=(
            "read-only verification of one exact current owned-source target; requires "
            "--preflight-only and never sends a cancellation"
        ),
    )
    parser.add_argument(
        "--recovery-approval-id",
        help="explicit operator approval ID required for one recovered-target cleanup",
    )
    parser.add_argument(
        "--recovery-approver-id",
        help="explicit operator identity required for one recovered-target cleanup",
    )
    parser.add_argument(
        "--isolated-native-process",
        action="store_true",
        help="require a disposable process after explicit EMT logout/release",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="start the signed DISARMED session and prove a clean unfinished-order query; no mutation",
    )
    parser.add_argument(
        "--fault-mode",
        choices=("host-health-integrity", "host-health-capacity"),
        help="inject a post-login, no-mutation host-health latch; requires --preflight-only",
    )
    parser.add_argument(
        "--r1-authoritative-recovery-probe",
        action="store_true",
        help="exercise the signed namespace, route-ledger and stable full-order scans in DISARMED simulator mode",
    )
    parser.add_argument(
        "--r1-authoritative-query-only",
        action="store_true",
        help="perform one R1 authoritative paged order scan in a no-mutation preflight",
    )
    parser.add_argument(
        "--r1-replay-seed-only",
        action="store_true",
        help=(
            "issue one cleaned-up, signed-release-bound replay proposal without claiming "
            "the R1 paged recovery certificate or double-scan acceptance"
        ),
    )
    parser.add_argument(
        "--r1-replay-witness-proposal-output",
        type=Path,
        help=(
            "new hash-only proposal issued after a full successful external R1 cycle; "
            "an offline signer must bind it before RESTART verification"
        ),
    )
    parser.add_argument(
        "--r1-namespace-release",
        type=Path,
        help="reviewed public R1 namespace release; requires all external R1 control-plane arguments",
    )
    parser.add_argument(
        "--r1-namespace-trust",
        type=Path,
        help="strict public-key-only R1 namespace trust document",
    )
    parser.add_argument(
        "--r1-namespace-state",
        type=Path,
        help="durable, scope-exclusive R1 namespace release state outside the runtime root",
    )
    parser.add_argument(
        "--r1-runtime-release-hash",
        help="SHA-256 of the reviewed runtime release bound by the namespace release",
    )
    parser.add_argument(
        "--r1-bootstrap-approval-id",
        help="explicit approval ID used only when the external release state is first created",
    )
    parser.add_argument(
        "--r1-bootstrap-approver-id",
        help="explicit approver identity used only when the external release state is first created",
    )
    parser.add_argument(
        "--r1-crash-after-native-return",
        action="store_true",
        help=(
            "R2 drill only: SIGKILL the isolated worker after EMT insertOrder returns and "
            "before NATIVE_ACCEPTED persistence; requires external R1 release material"
        ),
    )
    parser.add_argument(
        "--r1-crash-marker",
        type=Path,
        help="new payload-free evidence marker written and fsynced immediately before the R2 SIGKILL",
    )
    parser.add_argument(
        "--r1-recovery-route-ledger",
        type=Path,
        help=(
            "R2 recovery only: reopen one crashed own R1 route ledger, obtain two authoritative "
            "order scans, and persist RECOVERY_REHYDRATED while remaining DISARMED"
        ),
    )
    parser.add_argument(
        "--r1-manual-cleanup-route-ledger",
        type=Path,
        help=(
            "manual inverse-action only: use two complete scans to bind one crashed own R2 "
            "route to one native order, then cancel it with an explicit approval; unrelated "
            "unknown rows remain unprojected and this never claims generic recovery"
        ),
    )
    parser.add_argument(
        "--r1-manual-cleanup-source-release",
        type=Path,
        help=(
            "historical public release whose directory hash is bound by the crashed route; "
            "manual cleanup only and never an active execution release"
        ),
    )
    parser.add_argument(
        "--r1-manual-cleanup-source-trust",
        type=Path,
        help="public trust document used only to verify the historical manual-cleanup release",
    )
    parser.add_argument(
        "--hold-before-cancel-seconds",
        type=float,
        default=0.0,
        help=(
            "bounded simulated-only hold after R1 evidence and before cleanup; ordinary "
            "mode is 0..30 seconds, isolated signed R1 is 0..180 seconds"
        ),
    )
    parser.add_argument("--execute-simulated", action="store_true")
    parser.add_argument(
        "--provisioned-secret-lease",
        action="store_true",
        help=(
            "F2-only: construct the native EMT session through one ACTIVE_DISARMED "
            "profile and Linux protected-file secret lease, then revoke/remove its "
            "per-run secret root before report emission"
        ),
    )
    args = parser.parse_args()
    report = run(
        Settings(
            sdk_root=args.sdk_root,
            expected_library_hashes=args.expected_library_hashes,
            accounts_file=args.accounts_file,
            fixture=args.fixture,
            output=args.output,
            runtime_dir=args.runtime_dir,
            trading_day=args.trading_day,
            session_client_id=args.session_client_id,
            callback_timeout_seconds=args.callback_timeout,
            execute_simulated=args.execute_simulated,
            provisioned_secret_lease=args.provisioned_secret_lease,
            account_profile=args.account_profile,
            tight_burst_order_plan=args.tight_burst_order_plan,
            preflight_only=args.preflight_only,
            isolated_native_process=args.isolated_native_process,
            fault_mode=args.fault_mode,
            r1_authoritative_recovery_probe=args.r1_authoritative_recovery_probe,
            r1_authoritative_query_only=args.r1_authoritative_query_only,
            r1_replay_seed_only=args.r1_replay_seed_only,
            r1_replay_witness_proposal_output=args.r1_replay_witness_proposal_output,
            hold_before_cancel_seconds=args.hold_before_cancel_seconds,
            recovery_route_ledger=args.recovery_route_ledger,
            recovery_source_pre_native_journal=args.recovery_source_pre_native_journal,
            recovery_source_query_only=args.recovery_source_query_only,
            r1_namespace_release=args.r1_namespace_release,
            r1_namespace_trust=args.r1_namespace_trust,
            r1_namespace_state=args.r1_namespace_state,
            r1_runtime_release_hash=args.r1_runtime_release_hash,
            r1_bootstrap_approval_id=args.r1_bootstrap_approval_id,
            r1_bootstrap_approver_id=args.r1_bootstrap_approver_id,
            r1_crash_after_native_return=args.r1_crash_after_native_return,
            r1_crash_marker=args.r1_crash_marker,
            r1_recovery_route_ledger=args.r1_recovery_route_ledger,
            r1_manual_cleanup_route_ledger=args.r1_manual_cleanup_route_ledger,
            r1_manual_cleanup_source_release=args.r1_manual_cleanup_source_release,
            r1_manual_cleanup_source_trust=args.r1_manual_cleanup_source_trust,
            recovery_approval_id=args.recovery_approval_id,
            recovery_approver_id=args.recovery_approver_id,
        )
    )
    summary = json.dumps({"acceptance_status": report["acceptance_status"]}) + "\n"
    os.write(1, summary.encode("utf-8"))
    # ``run`` has already written the complete bounded report.  The retained
    # vendor wrapper is deliberately never finalized in this isolated worker.
    os._exit(0 if report["acceptance_status"] == "passed" else 2)


if __name__ == "__main__":
    raise SystemExit(main())
