"""No-login EMT BrokerAdapter staging boundary.

The future SDK/session bridge is intentionally not wired here.  This module
can delegate only query and recovery evidence to an injected read-only port;
submit and cancel are hard-rejected until the durable native-call boundary and
the S1-A EMT capability evidence are available.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic_ns
from typing import Callable, Protocol, runtime_checkable

from ...domain import (
    BrokerCapabilities,
    BrokerRecoveryEvidence,
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
from .event_ingress import EmtBrokerEventIngress
from .identity import EmtClientOrderIdAllocator
from .mutation import (
    EmtCancelRequest,
    EmtMutationPort,
    EmtSubmitRequest,
)


class EmtBrokerAdapterError(BrokerAdapterError):
    """The EMT Adapter profile or its read-only port violated its contract."""


class EmtMutationUnavailableError(EmtBrokerAdapterError):
    """A native EMT mutation was attempted before its authorized implementation exists."""


class EmtReadOnlyPort(Protocol):
    """The only staging seam allowed before the future EMT SDK bridge exists."""

    def query(self, request: BrokerQuery) -> QueryHandle:
        """Begin one account-scoped read-only query."""

        ...

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        """Begin one account-scoped read-only recovery collection."""

        ...

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        """Return explicitly complete or incomplete recovery evidence."""

        ...


EmtAdapterSubmitStageObserver = Callable[[ExecutionInstruction, str, int], None]


@runtime_checkable
class EmtCallbackIngressPort(Protocol):
    """Optional callback ingress lifecycle for a typed read-only native port."""

    def attach_observer(self, observer: BrokerEventObserver) -> None:
        """Attach the Adapter's durable observer before an SDK query can begin."""

    def detach_observer(self, observer: BrokerEventObserver) -> None:
        """Detach ingress and invalidate any unresolved query callbacks on stop."""

        ...


@dataclass(frozen=True)
class EmtBrokerAdapterSnapshot:
    """Inspectable local state; it is not a claim about an EMT SDK session."""

    started: bool
    observer_attached: bool
    submit_attempts: int
    cancel_attempts: int
    query_attempts: int
    recovery_attempts: int


@dataclass(frozen=True)
class PreparedEmtSubmit:
    """One side-effect-free EMT submit descriptor bound to one adapter activation.

    The descriptor deliberately contains the mapped domain request, not an EMT
    vendor request object.  It may be constructed before the native lane starts
    only after the adapter is active.  ``submit_prepared`` still delegates the
    native request construction to the mutation port after that port's
    PreDispatch Journal fence has committed.
    """

    instruction: ExecutionInstruction
    request: EmtSubmitRequest
    adapter_id: str
    adapter_instance_id: str
    activation_epoch: int
    _owner_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.request.instruction is not self.instruction:
            raise EmtBrokerAdapterError("prepared EMT request must retain its instruction identity")
        if self.activation_epoch <= 0:
            raise EmtBrokerAdapterError("prepared EMT submit activation epoch is invalid")

    @property
    def activation_owner(self) -> object:
        """Return the opaque activation identity used by the Adapter fence.

        This deliberately exposes identity comparison only.  Callers cannot
        derive credentials, an SDK handle, or mutable Adapter state from it.
        The public read-only seam avoids reaching across this value object's
        protected implementation field when an Adapter verifies a prepared
        submission after a session restart.
        """

        return self._owner_token


class EmtBrokerAdapter:
    """A strict EMT-shaped Adapter limited to an injected read-only port.

    No EMT SDK is imported, loaded, or logged into here.  The class exists so
    profile/Registry integration, exact identity fencing, and the future
    callback/recovery seam can be accepted independently of live credentials.
    """

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        read_only_port: EmtReadOnlyPort,
        mutation_port: EmtMutationPort | None = None,
        client_order_ref_factory: Callable[[ExecutionInstruction], str] | None = None,
        event_ingress: EmtBrokerEventIngress | None = None,
        submit_stage_observer: EmtAdapterSubmitStageObserver | None = None,
    ) -> None:
        if mapping.manifest.venue_id != "EMT":
            raise EmtBrokerAdapterError("EMT Adapter requires an EMT mapping")
        self._mapping = mapping
        self._read_only_port = read_only_port
        if (mutation_port is None) != (client_order_ref_factory is None):
            raise EmtBrokerAdapterError(
                "EMT mutation port and client-order reference factory must be configured together"
            )
        self._mutation_port = mutation_port
        self._client_order_ref_factory = client_order_ref_factory
        self._event_ingress = event_ingress
        self._submit_stage_observer = submit_stage_observer
        self._started = False
        self._observer: BrokerEventObserver | None = None
        self._submit_attempts = 0
        self._cancel_attempts = 0
        self._query_attempts = 0
        self._recovery_attempts = 0
        self._plans: dict[str, RecoveryPlan] = {}
        self._activation_epoch = 0
        self._prepared_submit_token: object | None = None

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtBrokerAdapterError("EMT mapping lacks scope ownership")
        return ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._mapping.runtime.capabilities

    def start(self, observer: BrokerEventObserver) -> None:
        if self._started:
            raise EmtBrokerAdapterError("EMT Adapter is already started")
        try:
            if self._event_ingress is not None:
                self._event_ingress.attach_observer(observer)
            if isinstance(self._read_only_port, EmtCallbackIngressPort):
                self._read_only_port.attach_observer(observer)
            self._observer = observer
            self._started = True
            self._activation_epoch += 1
            self._prepared_submit_token = object()
        except BaseException:
            if self._event_ingress is not None:
                try:
                    self._event_ingress.detach_observer(observer)
                except BaseException:
                    pass
            self._observer = None
            raise

    def stop(self) -> None:
        observer = self._observer
        try:
            if observer is not None and isinstance(self._read_only_port, EmtCallbackIngressPort):
                self._read_only_port.detach_observer(observer)
            if observer is not None and self._event_ingress is not None:
                self._event_ingress.detach_observer(observer)
        finally:
            self._started = False
            self._observer = None
            self._plans.clear()

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        prepared = self._prepare_submit(
            instruction,
            observe_stages=True,
            count_submit_attempt=True,
        )
        return self._dispatch_prepared(prepared, count_submit_attempt=False)

    def prepare_submit(self, instruction: ExecutionInstruction) -> PreparedEmtSubmit:
        """Build a reusable pure-Python submit descriptor for the active session.

        This is intentionally separate from :meth:`submit`: callers can run
        manifest mapping and canonical client-reference derivation before a
        measured burst, while the native mutation remains journal-fenced.
        No mutation port method is invoked here.
        """

        return self._prepare_submit(
            instruction,
            observe_stages=False,
            count_submit_attempt=False,
        )

    def submit_prepared(self, prepared: PreparedEmtSubmit) -> SubmitAccepted:
        """Dispatch one previously prepared request after activation fencing."""

        return self._dispatch_prepared(prepared, count_submit_attempt=True)

    def _dispatch_prepared(
        self, prepared: PreparedEmtSubmit, *, count_submit_attempt: bool
    ) -> SubmitAccepted:
        self._require_prepared_submit(prepared)
        mutation_port = self._mutation_port
        if mutation_port is None:
            raise EmtMutationUnavailableError(
                "EMT submit is unavailable until a controlled mutation port is accepted"
            )
        if count_submit_attempt:
            self._submit_attempts += 1
        receipt = mutation_port.submit(prepared.request)
        instruction = prepared.instruction
        return SubmitAccepted(
            execution_id=instruction.execution_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=self._identity(
                client_order_ref=str(prepared.request.order_client_id),
                venue_order_id=str(receipt.order_emt_id),
            ),
        )

    def _prepare_submit(
        self,
        instruction: ExecutionInstruction,
        *,
        observe_stages: bool,
        count_submit_attempt: bool,
    ) -> PreparedEmtSubmit:
        self._require_started()
        self._require_request_identity(
            account_id=instruction.account_id,
            venue_id="EMT",
            adapter_instance_id=self.adapter_instance_id,
        )
        if count_submit_attempt:
            self._submit_attempts += 1
        _mutation_port, factory = self._require_mutation_port(operation="submit")
        if observe_stages and self._submit_stage_observer is not None:
            self._submit_stage(instruction, "C1Q_AFTER_ADAPTER_REQUIREMENTS")
        client_order_ref = factory(instruction)
        order_client_id = EmtClientOrderIdAllocator.parse_client_order_ref(client_order_ref)
        if client_order_ref != str(order_client_id):
            raise EmtBrokerAdapterError("EMT order_client_id is not canonical decimal")
        if observe_stages and self._submit_stage_observer is not None:
            self._submit_stage(instruction, "C1R_AFTER_CLIENT_REFERENCE")
        request = EmtSubmitRequest(
            instruction=instruction,
            mapped_order=self._mapping.map_execution(instruction),
            order_client_id=order_client_id,
        )
        if observe_stages and self._submit_stage_observer is not None:
            self._submit_stage(instruction, "C1S_AFTER_MAPPED_REQUEST")
        token = self._prepared_submit_token
        if token is None:
            raise EmtBrokerAdapterError("EMT adapter activation has no prepared-submit token")
        return PreparedEmtSubmit(
            instruction=instruction,
            request=request,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            activation_epoch=self._activation_epoch,
            _owner_token=token,
        )

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        self._require_started()
        target = instruction.venue_order_identity
        self._require_request_identity(
            account_id=instruction.cancel_request.account_id,
            venue_id=target.venue_id,
            adapter_instance_id=target.adapter_instance_id,
        )
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        if target.adapter_client_scope != ownership.adapter_client_scope:
            raise EmtBrokerAdapterError("EMT cancel target belongs to another client scope")
        self._cancel_attempts += 1
        mutation_port, _factory = self._require_mutation_port(operation="cancel")
        order_emt_id = self._parse_order_emt_id(target.venue_order_id)
        receipt = mutation_port.cancel(
            EmtCancelRequest(
                cancel_request=instruction.cancel_request,
                order_emt_id=order_emt_id,
            )
        )
        return CancelAccepted(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=receipt.accepted_monotonic_ns,
            venue_order_identity=self._identity(
                client_order_ref=target.client_order_ref,
                venue_order_id=str(order_emt_id),
                venue_cancel_id=str(receipt.order_cancel_emt_id),
            ),
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        self._require_started()
        self._require_request_identity(
            account_id=request.account_id,
            venue_id=request.venue_id,
            adapter_instance_id=request.adapter_instance_id,
        )
        handle = self._read_only_port.query(request)
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
            raise EmtBrokerAdapterError("recovery must use the attached observer")
        handle = self._read_only_port.begin_recovery(plan)
        self._require_handle(handle, request_id=plan.recovery_run_id)
        self._plans[plan.recovery_run_id] = plan
        self._recovery_attempts += 1
        return handle

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        plan = self._plans.get(handle.recovery_run_id)
        if plan is None:
            raise EmtBrokerAdapterError("EMT recovery handle is unknown")
        self._require_handle(handle, request_id=plan.recovery_run_id)
        evidence = self._read_only_port.collect_recovery_evidence(handle)
        if (
            evidence.recovery_run_id != plan.recovery_run_id
            or evidence.account_id != plan.account_id
            or evidence.venue_id != "EMT"
            or evidence.adapter_instance_id != self.adapter_instance_id
            or evidence.trading_day != plan.trading_day
        ):
            raise EmtBrokerAdapterError("EMT read-only recovery evidence has another profile")
        return evidence

    def snapshot(self) -> EmtBrokerAdapterSnapshot:
        return EmtBrokerAdapterSnapshot(
            started=self._started,
            observer_attached=self._observer is not None,
            submit_attempts=self._submit_attempts,
            cancel_attempts=self._cancel_attempts,
            query_attempts=self._query_attempts,
            recovery_attempts=self._recovery_attempts,
        )

    def _require_started(self) -> None:
        if not self._started or self._observer is None:
            raise EmtBrokerAdapterError("EMT operations require an attached observer")

    def _require_prepared_submit(self, prepared: PreparedEmtSubmit) -> None:
        self._require_started()
        token = self._prepared_submit_token
        if (
            token is None
            or prepared.activation_owner is not token
            or prepared.activation_epoch != self._activation_epoch
            or prepared.adapter_id != self.adapter_id
            or prepared.adapter_instance_id != self.adapter_instance_id
        ):
            raise EmtBrokerAdapterError("prepared EMT submit belongs to another adapter activation")
        instruction = prepared.instruction
        request = prepared.request
        if request.instruction is not instruction:
            raise EmtBrokerAdapterError("prepared EMT submit request instruction identity changed")
        self._require_request_identity(
            account_id=instruction.account_id,
            venue_id="EMT",
            adapter_instance_id=self.adapter_instance_id,
        )

    def _require_request_identity(
        self, *, account_id: str, venue_id: str, adapter_instance_id: str
    ) -> None:
        account = self._mapping.runtime.account_capabilities.account_id
        if (
            account_id != account
            or venue_id != "EMT"
            or adapter_instance_id != self.adapter_instance_id
        ):
            raise EmtBrokerAdapterError("EMT read-only request belongs to another profile")

    def _require_handle(self, handle: QueryHandle | RecoveryHandle, *, request_id: str) -> None:
        returned_id = (
            handle.request_id if isinstance(handle, QueryHandle) else handle.recovery_run_id
        )
        if (
            returned_id != request_id
            or handle.adapter_id != self.adapter_id
            or handle.adapter_instance_id != self.adapter_instance_id
        ):
            raise EmtBrokerAdapterError("EMT read-only port returned an invalid handle")

    def _require_mutation_port(
        self, *, operation: str
    ) -> tuple[EmtMutationPort, Callable[[ExecutionInstruction], str]]:
        if self._mutation_port is None or self._client_order_ref_factory is None:
            raise EmtMutationUnavailableError(
                f"EMT {operation} is unavailable until a controlled mutation port is accepted"
            )
        return self._mutation_port, self._client_order_ref_factory

    def _submit_stage(self, instruction: ExecutionInstruction, stage: str) -> None:
        observer = self._submit_stage_observer
        if observer is None:
            return
        try:
            observer(instruction, stage, monotonic_ns())
        except BaseException:
            return

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
            venue_id="EMT",
            adapter_instance_id=ownership.adapter_instance_id,
            adapter_client_scope=ownership.adapter_client_scope,
            client_order_ref=client_order_ref,
            venue_order_id=venue_order_id,
            venue_cancel_id=venue_cancel_id,
        )

    @staticmethod
    def _parse_order_emt_id(value: str | None) -> int:
        if value is None:
            raise EmtBrokerAdapterError("EMT cancel target lacks venue order id")
        if not value.isascii() or not value.isdecimal() or value.startswith("0"):
            raise EmtBrokerAdapterError("EMT cancel target venue order id is not canonical")
        order_emt_id = int(value)
        if order_emt_id <= 0:
            raise EmtBrokerAdapterError("EMT cancel target venue order id must be positive")
        return order_emt_id
