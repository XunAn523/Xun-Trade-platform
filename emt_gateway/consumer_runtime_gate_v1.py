"""Fail-closed cached control, account, recovery and risk admission fence.

Control-plane refresh and authority verification are outside the hot path.
This gate cannot create recovery evidence and starts closed. After any failure
it cannot be reopened without a new reconciled runtime instance.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Lock, RLock, Thread
from typing import Callable

from xuanling_tradecore.consumer.base import ConsumerAccountView
from xuanling_tradecore.domain import (
    AccountLifecycle,
    ReconciliationStatus,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    RiskSnapshot,
)

from .consumer_runtime_control_v1 import ConsumerEmtRuntimeControlSnapshotV1


class ConsumerEmtRuntimeGateErrorV1(RuntimeError):
    """Runtime evidence is missing, stale, untrusted or belongs to another scope."""


class ConsumerEmtRuntimeGateV1:
    def __init__(
        self,
        *,
        control: ConsumerEmtRuntimeControlSnapshotV1,
        recovery_profile: RecoveryEvidenceProfile,
        verify_recovery_authority: Callable[[RecoveryCompletenessCertificate], bool],
        now_wall_ns: Callable[[], int],
        now_monotonic_ns: Callable[[], int],
        refresh_ttl_ns: int,
        recovery_ttl_ns: int,
    ) -> None:
        if (
            not callable(verify_recovery_authority)
            or type(refresh_ttl_ns) is not int
            or type(recovery_ttl_ns) is not int
            or not 1 <= refresh_ttl_ns <= 60_000_000_000
            or not 1 <= recovery_ttl_ns <= 86_400_000_000_000
        ):
            raise ValueError("EMT runtime gate bounds are invalid")
        self._control = control
        self._recovery_profile = recovery_profile
        self._verify_recovery = verify_recovery_authority
        self._wall = now_wall_ns
        self._mono = now_monotonic_ns
        self._refresh_ttl = refresh_ttl_ns
        self._recovery_ttl = recovery_ttl_ns
        self._lock = RLock()
        self._failed = False
        self._valid_until_mono = 0
        self._refreshed_mono = 0
        self._risk: RiskSnapshot | None = None
        self._recovery_completed_wall = 0

    def refresh(
        self,
        *,
        control: ConsumerEmtRuntimeControlSnapshotV1,
        account_view: ConsumerAccountView,
        risk: RiskSnapshot,
        certificate: RecoveryCompletenessCertificate,
    ) -> None:
        """Install only a fully verified snapshot; any failed refresh latches closed."""

        try:
            mono, wall = self._mono(), self._wall()
            original = self._control
            binding = original.profile_binding.binding
            scope = original.namespace.directory.scope
            if (
                control.profile_binding != original.profile_binding
                or control.namespace.state != original.namespace.state
                or control.namespace_not_after_wall_ns != original.namespace_not_after_wall_ns
                or account_view.consumer_account_id != binding.consumer_account_id
                or account_view.venue_id != "EMT"
                or account_view.lifecycle is not AccountLifecycle.READY
                or account_view.reconciliation_status is not ReconciliationStatus.VALIDATED
                or bool(account_view.gates)
                or risk.account_id != binding.account_id
                or risk.trading_day != scope.trading_day
                or not risk.captured_monotonic_ns <= mono < risk.expires_monotonic_ns
                or certificate.account_id != binding.account_id
                or certificate.venue_id != "EMT"
                or certificate.adapter_instance_id != original.profile_binding.profile.identity.adapter_instance_id
                or certificate.adapter_client_scope != str(scope.emt_client_id)
                or certificate.trading_day != scope.trading_day
                or certificate.certificate_hash != certificate.content_sha256
                or certificate.incompleteness_reasons(self._recovery_profile)
                or not certificate.completed_at_wall_ns <= wall < original.namespace_not_after_wall_ns
                or wall - certificate.completed_at_wall_ns >= self._recovery_ttl
                or self._verify_recovery(certificate) is not True
            ):
                raise ConsumerEmtRuntimeGateErrorV1("EMT runtime evidence is not valid")
            with self._lock:
                if self._failed:
                    raise ConsumerEmtRuntimeGateErrorV1("EMT runtime gate is latched")
                self._valid_until_mono = min(mono + self._refresh_ttl, risk.expires_monotonic_ns)
                self._refreshed_mono = mono
                self._risk = risk
                self._recovery_completed_wall = certificate.completed_at_wall_ns
        except Exception as exc:
            with self._lock:
                self._failed = True
                self._risk = None
            raise ConsumerEmtRuntimeGateErrorV1("EMT runtime gate requires reconciliation") from exc

    def admission_permitted(self) -> bool:
        with self._lock:
            if self._failed or self._risk is None:
                return False
            mono, wall = self._mono(), self._wall()
            if (
                mono < self._refreshed_mono
                or mono >= self._valid_until_mono
                or wall < self._recovery_completed_wall
                or wall >= self._control.namespace_not_after_wall_ns
                or wall - self._recovery_completed_wall >= self._recovery_ttl
            ):
                self._failed = True
                self._risk = None
                return False
            return True

    def require_account(self, account_id: str) -> None:
        if account_id != self._control.profile_binding.binding.account_id or not self.admission_permitted():
            raise ConsumerEmtRuntimeGateErrorV1("EMT native submit is blocked")

    def risk_snapshot(self) -> RiskSnapshot:
        with self._lock:
            if not self.admission_permitted() or self._risk is None:
                raise ConsumerEmtRuntimeGateErrorV1("EMT risk snapshot is unavailable")
            return self._risk

    def invalidate(self) -> None:
        """Permanently close this runtime after a monitor or host failure."""

        with self._lock:
            self._failed = True
            self._risk = None


@dataclass(frozen=True)
class ConsumerEmtRuntimeGateEvidenceV1:
    control: ConsumerEmtRuntimeControlSnapshotV1
    account_view: ConsumerAccountView
    risk: RiskSnapshot
    certificate: RecoveryCompletenessCertificate


class ManagedConsumerEmtGateRefreshV1:
    """Refresh authoritative evidence outside C0–C7; no automatic rearming."""

    def __init__(
        self,
        *,
        gate: ConsumerEmtRuntimeGateV1,
        read_evidence: Callable[[], ConsumerEmtRuntimeGateEvidenceV1],
        refresh_interval_seconds: float,
        shutdown_timeout_seconds: float = 5.0,
    ) -> None:
        if (
            not callable(read_evidence)
            or not 0 < refresh_interval_seconds <= 30
            or not 0 < shutdown_timeout_seconds <= 60
            or int(refresh_interval_seconds * 1_000_000_000) >= gate._refresh_ttl
        ):
            raise ValueError("EMT gate refresh interval must be shorter than its TTL")
        self._gate = gate
        self._reader = read_evidence
        self._interval = refresh_interval_seconds
        self._timeout = shutdown_timeout_seconds
        self._stop = Event()
        self._lock = Lock()
        self._thread: Thread | None = None

    @property
    def gate(self) -> ConsumerEmtRuntimeGateV1:
        return self._gate

    def start(self) -> None:
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise ConsumerEmtRuntimeGateErrorV1("EMT gate refresh cannot restart")
            self._thread = Thread(target=self._run, name="consumer-emt-control-refresh")
            self._thread.start()

    def close(self) -> None:
        self._gate.invalidate()
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(self._timeout)
            if thread.is_alive():
                raise ConsumerEmtRuntimeGateErrorV1("EMT gate refresh did not stop")

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                evidence = self._reader()
                self._gate.refresh(
                    control=evidence.control,
                    account_view=evidence.account_view,
                    risk=evidence.risk,
                    certificate=evidence.certificate,
                )
                if self._stop.wait(self._interval):
                    break
        except BaseException:
            self._gate.invalidate()
            self._stop.set()
