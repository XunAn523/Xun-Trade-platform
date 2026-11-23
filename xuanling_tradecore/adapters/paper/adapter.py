"""Fail-closed TradeCore-side PaperBroker Adapter.

This contains connection/session fencing, mapping, normalized event ingress,
and recovery delegation.  It intentionally contains no matching, calendar,
fee, market-data, or order-book implementation.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass
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
from .connector import (
    PaperBrokerNativeEvent,
    PaperBrokerOrderRequest,
    PaperBrokerSessionPort,
)
from .dependency import PaperBrokerDependencyManifest
from .normalizer import PaperBrokerEventConflictError, PaperBrokerEventNormalizer


class PaperBrokerAdapterError(BrokerAdapterError):
    """A PaperBroker profile, connection, ingress, or identity safety failure."""


class PaperBrokerMutationUnavailableError(PaperBrokerAdapterError):
    """A mutating action was requested before a ready external session exists."""


@dataclass(frozen=True)
class PaperBrokerAdapterSnapshot:
    """Local connector state; it makes no claim about external broker state."""

    started: bool
    observer_attached: bool
    ingress_depth: int
    ingress_capacity: int
    rejected_event_count: int
    submit_attempts: int
    cancel_attempts: int
    query_attempts: int
    recovery_attempts: int


class PaperBrokerAdapter:
    """Connector/profile implementation that keeps external semantics at the edge."""

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        dependency: PaperBrokerDependencyManifest,
        session: PaperBrokerSessionPort,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        ingress_capacity: int = 1024,
    ) -> None:
        if mapping.manifest.venue_id != "PAPER":
            raise PaperBrokerAdapterError("PaperBroker Adapter requires a PAPER mapping")
        if ingress_capacity <= 0:
            raise PaperBrokerAdapterError("PaperBroker ingress capacity must be positive")
        ownership = mapping.manifest.scope_ownership
        if ownership is None or mapping.manifest.route_token_binding is None:
            raise PaperBrokerAdapterError("PaperBroker mapping lacks scope or route-token evidence")
        self._mapping = mapping
        self._dependency = dependency
        self._session = session
        self._client_order_ref_factory = client_order_ref_factory
        self._ingress_capacity = ingress_capacity
        self._started = False
        self._observer: BrokerEventObserver | None = None
        self._events: deque[PaperBrokerNativeEvent] = deque()
        self._event_fingerprints: dict[str, str] = {}
        self._rejected_event_count = 0
        self._submit_attempts = 0
        self._cancel_attempts = 0
        self._query_attempts = 0
        self._recovery_attempts = 0
        self._plans: dict[str, RecoveryPlan] = {}
        self._normalizer = PaperBrokerEventNormalizer(
            adapter_id=mapping.manifest.fingerprint.adapter_id,
            adapter_instance_id=ownership.adapter_instance_id,
            adapter_client_scope=ownership.adapter_client_scope,
            account_id=mapping.runtime.account_capabilities.account_id,
        )

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

    def start(self, observer: BrokerEventObserver) -> None:
        if self._started:
            raise PaperBrokerAdapterError("PaperBroker Adapter is already started")
        try:
            # A connector may replay an event synchronously from ``open``.  The
            # durable observer is therefore attached before any external work.
            self._observer = observer
            self._started = True
            runtime = self._session.open(self._enqueue_external_event)
            self._dependency.require_runtime_identity(runtime)
            health = self._session.health()
            if not health.ready:
                raise PaperBrokerAdapterError("PaperBroker health probe is not ready")
        except BaseException:
            try:
                self._session.close()
            finally:
                self._observer = None
                self._started = False
                self._events.clear()
                self._event_fingerprints.clear()
            raise

    def stop(self) -> None:
        try:
            self._session.close()
        finally:
            self._started = False
            self._observer = None
            self._events.clear()
            self._plans.clear()

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        self._require_started()
        self._require_request_identity(
            account_id=instruction.account_id,
            venue_id="PAPER",
            adapter_instance_id=self.adapter_instance_id,
        )
        client_order_ref = self._client_order_ref_factory(instruction)
        self._require_client_order_ref(client_order_ref)
        mapped = self._mapping.map_execution(instruction)
        self._submit_attempts += 1
        receipt = self._session.submit(
            PaperBrokerOrderRequest(
                instruction_id=instruction.execution_id,
                account_id=instruction.account_id,
                mapped_order=mapped,
                symbol=instruction.symbol,
                quantity=instruction.quantity,
                limit_price_ticks=instruction.limit_price_ticks,
                client_order_ref=client_order_ref,
            )
        )
        if self.capabilities.has_stable_venue_order_id and receipt.venue_order_id is None:
            raise PaperBrokerAdapterError("PaperBroker submit lacks declared stable venue order id")
        return SubmitAccepted(
            execution_id=instruction.execution_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=self._identity(
                client_order_ref=client_order_ref, venue_order_id=receipt.venue_order_id
            ),
        )

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        self._require_started()
        identity = instruction.venue_order_identity
        self._require_request_identity(
            account_id=instruction.cancel_request.account_id,
            venue_id=identity.venue_id,
            adapter_instance_id=identity.adapter_instance_id,
        )
        self._cancel_attempts += 1
        receipt = self._session.cancel(instruction)
        return CancelAccepted(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=self._identity(
                client_order_ref=identity.client_order_ref,
                venue_order_id=identity.venue_order_id,
                venue_cancel_id=receipt.venue_cancel_id,
            ),
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        self._require_started()
        self._require_request_identity(
            account_id=request.account_id,
            venue_id=request.venue_id,
            adapter_instance_id=request.adapter_instance_id,
        )
        handle = self._session.query(request)
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
            raise PaperBrokerAdapterError("PaperBroker recovery must use the attached observer")
        handle = self._session.begin_recovery(plan)
        self._require_handle(handle, request_id=plan.recovery_run_id)
        self._plans[plan.recovery_run_id] = plan
        self._recovery_attempts += 1
        return handle

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        plan = self._plans.get(handle.recovery_run_id)
        if plan is None:
            raise PaperBrokerAdapterError("PaperBroker recovery handle is unknown")
        self._require_handle(handle, request_id=plan.recovery_run_id)
        evidence = self._session.collect_recovery_evidence(handle)
        if (
            evidence.recovery_run_id != plan.recovery_run_id
            or evidence.account_id != plan.account_id
            or evidence.venue_id != "PAPER"
            or evidence.adapter_instance_id != self.adapter_instance_id
            or evidence.trading_day != plan.trading_day
        ):
            raise PaperBrokerAdapterError("PaperBroker recovery evidence has another profile")
        return evidence

    def drain_ingress(self) -> int:
        """Deliver queued normalized facts in arrival order to the durable observer."""

        self._require_started()
        observer = self._observer
        assert observer is not None
        delivered = 0
        while self._events:
            event = self._events.popleft()
            observer.on_broker_event(self._normalizer.normalize(event))
            delivered += 1
        return delivered

    def snapshot(self) -> PaperBrokerAdapterSnapshot:
        return PaperBrokerAdapterSnapshot(
            started=self._started,
            observer_attached=self._observer is not None,
            ingress_depth=len(self._events),
            ingress_capacity=self._ingress_capacity,
            rejected_event_count=self._rejected_event_count,
            submit_attempts=self._submit_attempts,
            cancel_attempts=self._cancel_attempts,
            query_attempts=self._query_attempts,
            recovery_attempts=self._recovery_attempts,
        )

    def _enqueue_external_event(self, event: PaperBrokerNativeEvent) -> None:
        if not self._started or self._observer is None:
            raise PaperBrokerAdapterError("PaperBroker event arrived without an attached observer")
        if event.account_id != self._mapping.runtime.account_capabilities.account_id:
            raise PaperBrokerAdapterError("PaperBroker event belongs to another profile")
        if event.client_order_ref is not None:
            self._require_client_order_ref(event.client_order_ref)
        fingerprint = self._normalizer.fingerprint(event)
        previous = self._event_fingerprints.get(event.external_event_id)
        if previous is not None:
            if previous != fingerprint:
                raise PaperBrokerEventConflictError(
                    "PaperBroker external event id conflicts with an earlier event"
                )
            return
        self._event_fingerprints[event.external_event_id] = fingerprint
        if len(self._events) >= self._ingress_capacity:
            self._rejected_event_count += 1
            sequence = self._rejected_event_count
            self._observer.on_callback_gap(
                CallbackGapEvidence(
                    evidence_id=f"paperbroker-ingress-overflow:{self.adapter_instance_id}:{sequence}",
                    account_id=self._mapping.runtime.account_capabilities.account_id,
                    producer_id=self.adapter_id,
                    first_rejected_sequence=sequence,
                    last_rejected_sequence=sequence,
                    rejected_count=1,
                    observed_monotonic_ns=event.received_monotonic_ns,
                )
            )
            return
        self._events.append(event)

    def _identity(
        self,
        *,
        client_order_ref: str | None,
        venue_order_id: str | None,
        venue_cancel_id: str | None = None,
    ) -> VenueOrderIdentity:
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        return VenueOrderIdentity(
            venue_id="PAPER",
            adapter_instance_id=ownership.adapter_instance_id,
            adapter_client_scope=ownership.adapter_client_scope,
            client_order_ref=client_order_ref,
            venue_order_id=venue_order_id,
            venue_cancel_id=venue_cancel_id,
        )

    def _require_started(self) -> None:
        if not self._started or self._observer is None:
            raise PaperBrokerMutationUnavailableError(
                "PaperBroker operations require a ready session and attached observer"
            )

    def _require_request_identity(
        self, *, account_id: str, venue_id: str, adapter_instance_id: str
    ) -> None:
        if (
            account_id != self._mapping.runtime.account_capabilities.account_id
            or venue_id != "PAPER"
            or adapter_instance_id != self.adapter_instance_id
        ):
            raise PaperBrokerAdapterError("PaperBroker request belongs to another profile")

    def _require_handle(self, handle: QueryHandle | RecoveryHandle, *, request_id: str) -> None:
        returned_id = (
            handle.request_id if isinstance(handle, QueryHandle) else handle.recovery_run_id
        )
        if (
            returned_id != request_id
            or handle.adapter_id != self.adapter_id
            or handle.adapter_instance_id != self.adapter_instance_id
        ):
            raise PaperBrokerAdapterError("PaperBroker session returned an invalid handle")

    def _require_client_order_ref(self, value: str) -> None:
        binding = self._mapping.manifest.route_token_binding
        assert binding is not None
        try:
            encoded = value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise PaperBrokerAdapterError("PaperBroker client order ref must be ASCII") from exc
        if (
            len(encoded) > binding.max_encoded_bytes
            or re.fullmatch(binding.allowed_characters, value) is None
        ):
            raise PaperBrokerAdapterError(
                "PaperBroker client order ref violates route-token binding"
            )
