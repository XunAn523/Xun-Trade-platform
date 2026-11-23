"""Production-shaped TradeCore QMT Adapter over the Windows Edge contract.

This module never imports a vendor Python SDK.  The only component permitted
to call a vendor API is the separately deployed Windows embedded Native Engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from time import monotonic_ns
from typing import Callable

from ...domain import (
    BrokerCapabilities,
    BrokerRecoveryEvidence,
    CallbackGapEvidence,
    ExecutionInstruction,
    RecoveryPlan,
    VenueOrderIdentity,
)
from ..base import (
    BrokerAdapterError,
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    SubmitAccepted,
)
from ..mapping import ActivatedVenueMapping
from .bridge_normalizer import QmtBridgeEventConflictError, QmtBridgeEventNormalizer
from .bridge_protocol import (
    QmtBridgeDelivery,
    QmtBridgeError,
    QmtBridgeFaultSource,
    QmtBridgeGapDelivery,
    QmtBridgePort,
    QmtSubmitCommand,
)
from .capability import QmtCapabilityActivation


class QmtBrokerAdapterError(BrokerAdapterError):
    """A QMT profile, Windows Edge, or delivery contract violation."""


class QmtMutationUnavailableError(QmtBrokerAdapterError):
    """A mutation was attempted while the QMT Adapter Path is disarmed."""


@dataclass(frozen=True)
class QmtBrokerAdapterSnapshot:
    """Inspectable local state; it makes no claim about a QMT venue outcome."""

    started: bool
    observer_attached: bool
    submit_attempts: int
    cancel_attempts: int
    query_attempts: int
    recovery_attempts: int
    acknowledged_delivery_id: int | None
    mutation_blocked: bool


class QmtBrokerAdapter:
    """One strict `BrokerAdapter` implementation backed by Windows Edge.

    The class remains independently testable through ``QmtBridgePort``.  A
    real mTLS implementation is injected only after P0 establishes the QMT
    capability manifest; no QMT SDK handle crosses this boundary.
    """

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        bridge: QmtBridgePort,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        capability_activation: QmtCapabilityActivation | None = None,
        mutation_enabled: bool = False,
    ) -> None:
        if mapping.manifest.venue_id != "QMT":
            raise QmtBrokerAdapterError("QMT Adapter requires a QMT mapping")
        ownership = mapping.manifest.scope_ownership
        if ownership is None or mapping.manifest.route_token_binding is None:
            raise QmtBrokerAdapterError("QMT mapping lacks scope or route-token evidence")
        self._mapping = mapping
        self._bridge = bridge
        self._client_order_ref_factory = client_order_ref_factory
        self._capability_activation = capability_activation
        if capability_activation is not None:
            manifest = capability_activation.manifest
            if (
                manifest.adapter_instance_id != ownership.adapter_instance_id
                or manifest.adapter_client_scope != ownership.adapter_client_scope
                or manifest.mapping_manifest_hash != mapping.manifest.manifest_hash
            ):
                raise QmtBrokerAdapterError(
                    "QMT capability activation belongs to another adapter profile"
                )
        if mutation_enabled and capability_activation is None:
            raise QmtMutationUnavailableError(
                "QMT mutation requires verified Windows Edge capability evidence"
            )
        self._mutation_enabled = mutation_enabled
        self._started = False
        self._observer: BrokerEventObserver | None = None
        self._submit_attempts = 0
        self._cancel_attempts = 0
        self._query_attempts = 0
        self._recovery_attempts = 0
        self._plans: dict[str, RecoveryPlan] = {}
        self._event_fingerprints: dict[str, str] = {}
        self._acknowledged_delivery_id: int | None = None
        self._last_delivery: QmtBridgeDelivery | None = None
        self._mutation_blocked = False
        self._delivery_lock = RLock()

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        return ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._mapping.runtime.capabilities

    @property
    def capability_available(self) -> bool:
        """Whether this bridge has a verified, scope-bound Edge capability proof.

        A false result is intentionally not a degraded route: a composition
        layer must expose it as ``CAPABILITY_UNAVAILABLE`` rather than select
        the adapter for a mutation.
        """

        return self._capability_activation is not None

    def start(self, observer: BrokerEventObserver) -> None:
        if self._started:
            raise QmtBrokerAdapterError("QMT Adapter is already started")
        self._observer = observer
        self._started = True
        try:
            if isinstance(self._bridge, QmtBridgeFaultSource):
                self._bridge.set_fault_sink(self._on_bridge_fault)
            self._bridge.open(self._on_delivery)
            health = self._bridge.health()
            if not health.ready:
                raise QmtBrokerAdapterError("QMT Edge health probe is not ready")
        except BaseException:
            try:
                self._bridge.close()
            finally:
                self._started = False
                self._observer = None
            raise

    def stop(self) -> None:
        try:
            if isinstance(self._bridge, QmtBridgeFaultSource):
                self._bridge.set_fault_sink(None)
            self._bridge.close()
        finally:
            self._started = False
            self._observer = None
            self._plans.clear()
            # Keep delivery identity and fault state across a transport restart.
            # Reopening a socket is neither reconciliation nor authorization.

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        with self._delivery_lock:
            try:
                return self._submit(instruction)
            except QmtBridgeError:
                self._mutation_blocked = True
                raise

    def _submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        self._require_started()
        self._require_mutation_enabled("submit")
        self._require_request_identity(
            account_id=instruction.account_id,
            venue_id="QMT",
            adapter_instance_id=self.adapter_instance_id,
        )
        client_order_ref = self._client_order_ref_factory(instruction)
        if not client_order_ref:
            raise QmtBrokerAdapterError("QMT client_order_ref factory returned empty text")
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        self._submit_attempts += 1
        receipt = self._bridge.submit(
            QmtSubmitCommand(
                instruction=instruction,
                mapped_order=self._mapping.map_execution(instruction),
                client_order_ref=client_order_ref,
                adapter_instance_id=self.adapter_instance_id,
                adapter_client_scope=ownership.adapter_client_scope,
            )
        )
        if receipt.execution_id != instruction.execution_id:
            raise QmtBrokerAdapterError("QMT Edge submit receipt has another execution")
        self._require_identity(receipt.venue_order_identity)
        return SubmitAccepted(
            execution_id=instruction.execution_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=receipt.venue_order_identity,
        )

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        with self._delivery_lock:
            try:
                return self._cancel(instruction)
            except QmtBridgeError:
                self._mutation_blocked = True
                raise

    def _cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        self._require_started()
        self._require_mutation_enabled("cancel")
        identity = instruction.venue_order_identity
        self._require_request_identity(
            account_id=instruction.cancel_request.account_id,
            venue_id=identity.venue_id,
            adapter_instance_id=identity.adapter_instance_id,
        )
        self._require_identity(identity)
        self._cancel_attempts += 1
        receipt = self._bridge.cancel(instruction.cancel_request, identity)
        if receipt.cancel_request_id != instruction.cancel_request.cancel_request_id:
            raise QmtBrokerAdapterError("QMT Edge cancel receipt has another request")
        self._require_identity(receipt.venue_order_identity)
        return CancelAccepted(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=receipt.venue_order_identity,
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        self._require_started()
        self._require_request_identity(
            account_id=request.account_id,
            venue_id=request.venue_id,
            adapter_instance_id=request.adapter_instance_id,
        )
        handle = self._bridge.query(request)
        self._require_handle(handle, request_id=request.request_id)
        self._query_attempts += 1
        return handle

    def begin_recovery(self, plan: RecoveryPlan, observer: BrokerEventObserver) -> RecoveryHandle:
        self._require_started()
        self._require_request_identity(
            account_id=plan.account_id,
            venue_id=plan.venue_id,
            adapter_instance_id=plan.adapter_instance_id,
        )
        if observer is not self._observer:
            raise QmtBrokerAdapterError("QMT recovery must use the attached observer")
        handle = self._bridge.begin_recovery(plan)
        self._require_handle(handle, request_id=plan.recovery_run_id)
        self._plans[plan.recovery_run_id] = plan
        self._recovery_attempts += 1
        return handle

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        plan = self._plans.get(handle.recovery_run_id)
        if plan is None:
            raise QmtBrokerAdapterError("QMT recovery handle is unknown")
        self._require_handle(handle, request_id=plan.recovery_run_id)
        evidence = self._bridge.collect_recovery_evidence(handle)
        if (
            evidence.recovery_run_id != plan.recovery_run_id
            or evidence.account_id != plan.account_id
            or evidence.venue_id != "QMT"
            or evidence.adapter_instance_id != self.adapter_instance_id
            or evidence.trading_day != plan.trading_day
        ):
            raise QmtBrokerAdapterError("QMT Edge recovery evidence has another profile")
        return evidence

    def snapshot(self) -> QmtBrokerAdapterSnapshot:
        return QmtBrokerAdapterSnapshot(
            started=self._started,
            observer_attached=self._observer is not None,
            submit_attempts=self._submit_attempts,
            cancel_attempts=self._cancel_attempts,
            query_attempts=self._query_attempts,
            recovery_attempts=self._recovery_attempts,
            acknowledged_delivery_id=self._acknowledged_delivery_id,
            mutation_blocked=self._mutation_blocked,
        )

    def _on_delivery(self, delivery: QmtBridgeDelivery) -> None:
        with self._delivery_lock:
            try:
                self._deliver_and_acknowledge(delivery)
            except BaseException:
                # Includes observer commit failures and uncertain ACK effects.
                # Recovery/query remain available, but mutations stay stopped.
                self._mutation_blocked = True
                raise

    def _on_bridge_fault(self) -> None:
        """Make transport loss visible through the same generic observer as EMT."""
        with self._delivery_lock:
            self._mutation_blocked = True
            observer = self._observer
            if observer is not None:
                sequence = 1 if self._last_delivery is None else self._last_delivery.delivery_id + 1
                observed = monotonic_ns()
                observer.on_callback_gap(
                    CallbackGapEvidence(
                        evidence_id=f"qmt-bridge-fault:{observed}",
                        account_id=self._account_id,
                        producer_id=self.adapter_instance_id,
                        first_rejected_sequence=sequence,
                        last_rejected_sequence=sequence,
                        rejected_count=1,
                        observed_monotonic_ns=observed,
                    )
                )

    def _deliver_and_acknowledge(self, delivery: QmtBridgeDelivery) -> None:
        observer = self._observer
        if not self._started or observer is None:
            raise QmtBridgeError("QMT Edge delivered before an observer was attached")
        previous_delivery = self._last_delivery
        if previous_delivery is not None:
            if delivery.delivery_id == previous_delivery.delivery_id:
                if not self._same_delivery(previous_delivery, delivery):
                    raise QmtBrokerAdapterError("QMT Edge reused a delivery id")
                # The observer already returned successfully. An ACK retry must
                # not project the same fact (including a gap) twice.
                self._bridge.acknowledge(delivery.delivery_id)
                self._acknowledged_delivery_id = delivery.delivery_id
                return
            if delivery.delivery_id != previous_delivery.delivery_id + 1:
                raise QmtBrokerAdapterError("QMT Edge delivery sequence is not contiguous")
            if self._acknowledged_delivery_id != previous_delivery.delivery_id:
                raise QmtBrokerAdapterError("QMT Edge advanced past an unacknowledged delivery")
        if isinstance(delivery, QmtBridgeGapDelivery):
            if delivery.evidence.account_id != self._account_id:
                raise QmtBrokerAdapterError("QMT Edge callback gap belongs to another account")
            self._mutation_blocked = True
            observer.on_callback_gap(delivery.evidence)
        else:
            event = delivery.event
            fingerprint = QmtBridgeEventNormalizer.fingerprint(event)
            previous = self._event_fingerprints.get(event.external_event_id)
            if previous is not None:
                if previous != fingerprint:
                    raise QmtBrokerAdapterError("QMT Edge reused an event id")
            else:
                try:
                    normalized = QmtBridgeEventNormalizer(
                        adapter_id=self.adapter_id,
                        adapter_instance_id=self.adapter_instance_id,
                        adapter_client_scope=self._scope,
                        account_id=self._account_id,
                    ).normalize(event)
                except QmtBridgeEventConflictError as error:
                    raise QmtBrokerAdapterError("QMT Edge event normalization failed") from error
                observer.on_broker_event(normalized)
                self._event_fingerprints[event.external_event_id] = fingerprint
        self._last_delivery = delivery
        self._bridge.acknowledge(delivery.delivery_id)
        self._acknowledged_delivery_id = delivery.delivery_id

    @staticmethod
    def _same_delivery(previous: QmtBridgeDelivery, current: QmtBridgeDelivery) -> bool:
        if isinstance(previous, QmtBridgeGapDelivery):
            return (
                isinstance(current, QmtBridgeGapDelivery) and previous.evidence == current.evidence
            )
        if isinstance(current, QmtBridgeGapDelivery):
            return False
        return (
            previous.event.external_event_id == current.event.external_event_id
            and QmtBridgeEventNormalizer.fingerprint(previous.event)
            == QmtBridgeEventNormalizer.fingerprint(current.event)
        )

    @property
    def _scope(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        return ownership.adapter_client_scope

    @property
    def _account_id(self) -> str:
        return self._mapping.runtime.account_capabilities.account_id

    def _require_started(self) -> None:
        if not self._started or self._observer is None:
            raise QmtBrokerAdapterError("QMT operations require an attached observer")

    def _require_mutation_enabled(self, operation: str) -> None:
        if self._mutation_blocked:
            raise QmtMutationUnavailableError(
                f"QMT {operation} is unavailable: bridge reconciliation is required"
            )
        if self._capability_activation is None:
            raise QmtMutationUnavailableError(
                f"QMT {operation} is unavailable: Windows Edge capability is unverified"
            )
        if not self._mutation_enabled:
            raise QmtMutationUnavailableError(
                f"QMT {operation} is unavailable until the production profile is accepted"
            )

    def _require_request_identity(
        self, *, account_id: str, venue_id: str, adapter_instance_id: str
    ) -> None:
        if (
            account_id != self._account_id
            or venue_id != "QMT"
            or adapter_instance_id != self.adapter_instance_id
        ):
            raise QmtBrokerAdapterError("QMT request belongs to another profile")

    def _require_identity(self, identity: VenueOrderIdentity | None) -> None:
        if identity is None:
            return
        if (
            identity.venue_id != "QMT"
            or identity.adapter_instance_id != self.adapter_instance_id
            or identity.adapter_client_scope != self._scope
        ):
            raise QmtBrokerAdapterError("QMT native identity belongs to another scope")

    def _require_handle(self, handle: QueryHandle | RecoveryHandle, *, request_id: str) -> None:
        returned = handle.request_id if isinstance(handle, QueryHandle) else handle.recovery_run_id
        if (
            returned != request_id
            or handle.adapter_id != self.adapter_id
            or handle.adapter_instance_id != self.adapter_instance_id
        ):
            raise QmtBrokerAdapterError("QMT Edge returned an invalid handle")
