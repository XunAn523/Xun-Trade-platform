"""File-backed mmap fencing for EMT calls and normalized callbacks.

The Python execution profile has one synchronous persistence boundary: a
complete mmap frame whose commit word is written last. It is deliberately a
same-host process-crash fence, not a promise of a flushed SSD write or of
host-power-loss recovery. No network datastore is imported or consulted by
this composition.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from threading import RLock, local
from time import monotonic_ns
from types import MappingProxyType
from typing import Callable, Iterator, Mapping, Protocol

from xuanling_tradecore.adapters import (
    BrokerEventObserver,
    CancelAccepted,
    CancelExecutionInstruction,
    SubmitAccepted,
)
from xuanling_tradecore.adapters.emt import (
    EmtCancelRequest,
    EmtSubmitRequest,
    MmapRouteLedgerV2,
    PreparedEmtSubmit,
    RouteLedgerFactV2,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    ExecutionInstruction,
    VenueOrderIdentity,
)
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    CancelRouteCommand,
    MmapCallbackOutcomeJournal,
    MmapPreNativeJournal,
    PreNativeRouteCommand,
)

from .fastspi_v2_cancel_outcome import FastSpiV2CancelOutcomeObserver
from .fastspi_v2_outcome import FastSpiV2NativeOutcomeObserver, FastSpiV2OutcomeResolution
from .fastspi_v2_route_outcome import FastSpiV2RouteOutcomeDirectory
from .journal_archive import JournalArchiveSink


class EmtMmapNativeCallJournalError(RuntimeError):
    """A request lacks the local proof required to call EMT."""


@dataclass(frozen=True)
class EmtSubmitRoute:
    """One admitted local route authorizing exactly one EMT submit request."""

    binding: AccountJournalBinding
    command: PreNativeRouteCommand
    venue_order_identity: VenueOrderIdentity

    def validate(self, request: EmtSubmitRequest) -> None:
        instruction = request.instruction
        if (
            self.binding.venue_id != "EMT"
            or self.binding.account_id != instruction.account_id
            or self.command.account_id != instruction.account_id
            or self.command.execution_id != instruction.execution_id
            or self.command.oms_order_id != instruction.oms_order_id
            or self.command.client_order_ref != str(request.order_client_id)
            or self.venue_order_identity.venue_id != "EMT"
            or self.venue_order_identity.client_order_ref != str(request.order_client_id)
        ):
            raise EmtMmapNativeCallJournalError("EMT submit route does not match its request")


@dataclass(frozen=True)
class EmtCancelRoute:
    """One admitted local route authorizing exactly one EMT cancel request."""

    binding: AccountJournalBinding
    command: CancelRouteCommand
    target_identity: VenueOrderIdentity

    def validate(self, request: EmtCancelRequest) -> None:
        cancel = request.cancel_request
        if (
            self.binding.venue_id != "EMT"
            or self.binding.account_id != cancel.account_id
            or self.command.account_id != cancel.account_id
            or self.command.cancel_request_id != cancel.cancel_request_id
            or self.command.oms_order_id != cancel.oms_order_id
            or self.command.execution_id != cancel.execution_id
            or self.target_identity.venue_id != "EMT"
            or self.target_identity.client_order_ref != self.command.target_client_order_ref
            or self.target_identity.venue_order_id != str(request.order_emt_id)
        ):
            raise EmtMmapNativeCallJournalError("EMT cancel route does not match its request")


class EmtNativeRouteResolver(Protocol):
    """Resolve routes prepared by TradeCore admission before the SDK call."""

    def resolve_submit(self, request: EmtSubmitRequest) -> EmtSubmitRoute: ...

    def resolve_cancel(self, request: EmtCancelRequest) -> EmtCancelRoute: ...


class PreparedEmtNativeRouteResolver:
    """Bounded hand-off table between admission and the native EMT caller."""

    def __init__(self, *, maximum_pending_routes: int = 4096) -> None:
        if not 1 <= maximum_pending_routes <= 1_000_000:
            raise ValueError("EMT prepared route capacity is invalid")
        self._maximum_pending_routes = maximum_pending_routes
        self._submits_by_execution: dict[str, EmtSubmitRoute] = {}
        self._submits_by_client_ref: dict[str, EmtSubmitRoute] = {}
        self._sealed_submits_by_execution: Mapping[str, EmtSubmitRoute] | None = None
        self._cancels_by_request_id: dict[str, EmtCancelRoute] = {}
        self._lock = RLock()

    def prepare_submit(self, route: EmtSubmitRoute, instruction: ExecutionInstruction) -> None:
        self._validate_submit_route_instruction(route, instruction)
        with self._lock:
            sealed = self._sealed_submits_by_execution
            if sealed is not None:
                if sealed.get(instruction.execution_id) == route:
                    return
                raise EmtMmapNativeCallJournalError("prepared EMT submit route table is sealed")
            command = route.command
            existing_execution = self._submits_by_execution.get(command.execution_id)
            existing_ref = self._submits_by_client_ref.get(command.client_order_ref)
            if existing_execution is not None or existing_ref is not None:
                if existing_execution == route and existing_ref == route:
                    return
                raise EmtMmapNativeCallJournalError("prepared EMT submit route conflicts")
            if self._pending_count() >= self._maximum_pending_routes:
                raise EmtMmapNativeCallJournalError("prepared EMT route table is full")
            self._submits_by_execution[command.execution_id] = route
            self._submits_by_client_ref[command.client_order_ref] = route

    def seal_submits(self) -> int:
        """Freeze submit lookup before a burst so native reads need no lock."""

        with self._lock:
            if self._sealed_submits_by_execution is None:
                self._sealed_submits_by_execution = MappingProxyType(
                    dict(self._submits_by_execution)
                )
            return len(self._sealed_submits_by_execution)

    def require_prepared_submit(
        self, route: EmtSubmitRoute, instruction: ExecutionInstruction
    ) -> None:
        self._validate_submit_route_instruction(route, instruction)
        if self._submit_route_for_execution(instruction.execution_id) != route:
            raise EmtMmapNativeCallJournalError(
                "prepared EMT submit route differs from the dispatch route"
            )

    def client_order_ref_for(self, instruction: ExecutionInstruction) -> str:
        route = self._submit_route_for_execution(instruction.execution_id)
        command = route.command
        if (
            command.account_id != instruction.account_id
            or command.oms_order_id != instruction.oms_order_id
            or command.execution_id != instruction.execution_id
        ):
            raise EmtMmapNativeCallJournalError(
                "prepared EMT client reference belongs to another execution"
            )
        return command.client_order_ref

    def prepare_cancel(
        self, route: EmtCancelRoute, instruction: CancelExecutionInstruction
    ) -> None:
        request = instruction.cancel_request
        command = route.command
        if (
            request.account_id != command.account_id
            or request.cancel_request_id != command.cancel_request_id
            or request.execution_id != command.execution_id
            or request.oms_order_id != command.oms_order_id
            or instruction.venue_order_identity != route.target_identity
        ):
            raise EmtMmapNativeCallJournalError(
                "prepared EMT cancel route does not match its execution instruction"
            )
        with self._lock:
            existing = self._cancels_by_request_id.get(command.cancel_request_id)
            if existing is not None:
                if existing == route:
                    return
                raise EmtMmapNativeCallJournalError("prepared EMT cancel route conflicts")
            if self._pending_count() >= self._maximum_pending_routes:
                raise EmtMmapNativeCallJournalError("prepared EMT route table is full")
            self._cancels_by_request_id[command.cancel_request_id] = route

    def resolve_submit(self, request: EmtSubmitRequest) -> EmtSubmitRoute:
        route = self._submit_route_for_execution(request.instruction.execution_id)
        if route.command.client_order_ref != str(request.order_client_id):
            raise EmtMmapNativeCallJournalError(
                "EMT submit request client reference was not pre-admitted"
            )
        return route

    def resolve_cancel(self, request: EmtCancelRequest) -> EmtCancelRoute:
        with self._lock:
            try:
                route = self._cancels_by_request_id[request.cancel_request.cancel_request_id]
            except KeyError as exc:
                raise EmtMmapNativeCallJournalError("EMT cancel route was not prepared") from exc
        if route.target_identity.venue_order_id != str(request.order_emt_id):
            raise EmtMmapNativeCallJournalError("EMT cancel request target was not pre-admitted")
        return route

    def release_submit(self, execution_id: str) -> None:
        with self._lock:
            if self._sealed_submits_by_execution is not None:
                raise EmtMmapNativeCallJournalError("sealed EMT submit routes cannot be released")
            route = self._submits_by_execution.pop(execution_id, None)
            if route is not None:
                self._submits_by_client_ref.pop(route.command.client_order_ref, None)

    def release_cancel(self, cancel_request_id: str) -> None:
        with self._lock:
            self._cancels_by_request_id.pop(cancel_request_id, None)

    def _pending_count(self) -> int:
        return len(self._submits_by_execution) + len(self._cancels_by_request_id)

    def _submit_route_for_execution(self, execution_id: str) -> EmtSubmitRoute:
        sealed = self._sealed_submits_by_execution
        if sealed is not None:
            try:
                return sealed[execution_id]
            except KeyError as exc:
                raise EmtMmapNativeCallJournalError(
                    "EMT execution has no prepared mmap submit route"
                ) from exc
        with self._lock:
            try:
                return self._submits_by_execution[execution_id]
            except KeyError as exc:
                raise EmtMmapNativeCallJournalError(
                    "EMT execution has no prepared mmap submit route"
                ) from exc

    @staticmethod
    def _validate_submit_route_instruction(
        route: EmtSubmitRoute, instruction: ExecutionInstruction
    ) -> None:
        command = route.command
        if (
            instruction.account_id != command.account_id
            or instruction.execution_id != command.execution_id
            or instruction.oms_order_id != command.oms_order_id
        ):
            raise EmtMmapNativeCallJournalError(
                "prepared EMT submit route does not match its execution instruction"
            )


class EmtControlledExecutionPort(Protocol):
    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted: ...

    def submit_prepared(self, prepared: PreparedEmtSubmit) -> SubmitAccepted: ...

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted: ...


class TradeCoreDispatchTelemetry(Protocol):
    def on_submit_dispatch(self, instruction: ExecutionInstruction, observed_ns: int) -> None: ...

    def on_cancel_dispatch(
        self, instruction: CancelExecutionInstruction, observed_ns: int
    ) -> None: ...


class TradeCoreEmtRouteDispatcher:
    """Compose pre-admitted TradeCore execution with the EMT adapter boundary."""

    def __init__(
        self,
        *,
        execution_adapter: EmtControlledExecutionPort,
        route_resolver: PreparedEmtNativeRouteResolver,
        telemetry: TradeCoreDispatchTelemetry | None = None,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._execution_adapter = execution_adapter
        self._route_resolver = route_resolver
        self._telemetry = telemetry
        self._clock = clock

    def submit(self, *, route: EmtSubmitRoute, instruction: ExecutionInstruction) -> SubmitAccepted:
        self._route_resolver.prepare_submit(route, instruction)
        return self._submit_prepared(route, instruction)

    def submit_prepared(
        self,
        *,
        route: EmtSubmitRoute,
        instruction: ExecutionInstruction,
        prepared_submit: PreparedEmtSubmit | None = None,
    ) -> SubmitAccepted:
        self._route_resolver.require_prepared_submit(route, instruction)
        if prepared_submit is not None and prepared_submit.instruction is not instruction:
            raise EmtMmapNativeCallJournalError(
                "prepared EMT submit descriptor has another execution instruction"
            )
        return self._submit_prepared(route, instruction, prepared_submit)

    def _submit_prepared(
        self,
        route: EmtSubmitRoute,
        instruction: ExecutionInstruction,
        prepared_submit: PreparedEmtSubmit | None = None,
    ) -> SubmitAccepted:
        self._submit_dispatch_stage(instruction)
        accepted = (
            self._execution_adapter.submit_prepared(prepared_submit)
            if prepared_submit is not None
            else self._execution_adapter.submit(instruction)
        )
        identity = accepted.venue_order_identity
        if (
            identity is None
            or accepted.execution_id != instruction.execution_id
            or identity.client_order_ref != route.command.client_order_ref
            or identity.venue_order_id is None
        ):
            raise EmtMmapNativeCallJournalError(
                "TradeCore EMT submit returned an identity outside its prepared route"
            )
        return accepted

    def cancel(
        self, *, route: EmtCancelRoute, instruction: CancelExecutionInstruction
    ) -> CancelAccepted:
        self._route_resolver.prepare_cancel(route, instruction)
        if self._telemetry is not None:
            try:
                self._telemetry.on_cancel_dispatch(instruction, self._clock())
            except BaseException:
                pass
        accepted = self._execution_adapter.cancel(instruction)
        identity = accepted.venue_order_identity
        if (
            identity is None
            or accepted.cancel_request_id != route.command.cancel_request_id
            or identity.venue_order_id != route.target_identity.venue_order_id
            or identity.venue_cancel_id is None
        ):
            raise EmtMmapNativeCallJournalError(
                "TradeCore EMT cancel returned an identity outside its prepared route"
            )
        return accepted

    def _submit_dispatch_stage(self, instruction: ExecutionInstruction) -> None:
        if self._telemetry is not None:
            try:
                self._telemetry.on_submit_dispatch(instruction, self._clock())
            except BaseException:
                pass


class EmtJournaledBrokerEventObserver(BrokerEventObserver):
    """Commit local callback frames before delivering them to downstream code."""

    def __init__(
        self,
        *,
        order_outcomes: FastSpiV2NativeOutcomeObserver,
        cancel_outcomes: FastSpiV2CancelOutcomeObserver,
        downstream: BrokerEventObserver,
        callback_stage_observer: Callable[[BrokerEvent, str, int], None] | None = None,
        native_inflight_release: Callable[[str], bool] | None = None,
        terminal_execution_id_resolver: Callable[[BrokerEvent], str | None] | None = None,
        is_owned_order_callback: Callable[[BrokerEvent], bool] | None = None,
        callback_ingress_is_external: bool = False,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._order_outcomes = order_outcomes
        self._cancel_outcomes = cancel_outcomes
        self._downstream = downstream
        self._callback_stage_observer = callback_stage_observer
        self._native_inflight_release = native_inflight_release
        self._terminal_execution_id_resolver = terminal_execution_id_resolver
        self._is_owned_order_callback = is_owned_order_callback
        self._callback_ingress_is_external = callback_ingress_is_external
        self._clock = clock

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.on_broker_events((event,))

    def on_broker_events(self, events: tuple[BrokerEvent, ...]) -> None:
        delivered = tuple(
            event
            for event in events
            if not (
                event.event_kind in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}
                and self._is_owned_order_callback is not None
                and not self._is_owned_order_callback(event)
            )
        )
        if not delivered:
            return
        if not self._callback_ingress_is_external:
            for event in delivered:
                self._stage(event, "C5_CALLBACK_INGRESS", event.received_monotonic_ns)
        states = tuple(
            event
            for event in delivered
            if event.event_kind in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}
        )
        if len(states) > 1:
            self._order_outcomes.on_broker_events(states)
        elif states:
            self._order_outcomes.on_broker_event(states[0])
        for event in delivered:
            self._cancel_outcomes.on_broker_event(event)
            self._release_terminal_inflight(event)
            self._stage(event, "C6_AFTER_CALLBACK_MMAP_COMMIT")
            self._downstream.on_broker_event(event)
            self._stage(event, "C7_AFTER_DOWNSTREAM")

    def on_callback_gap(self, evidence: object) -> None:
        self._downstream.on_callback_gap(evidence)

    def _stage(self, event: BrokerEvent, stage: str, observed_ns: int | None = None) -> None:
        if self._callback_stage_observer is not None:
            try:
                self._callback_stage_observer(
                    event, stage, self._clock() if observed_ns is None else observed_ns
                )
            except BaseException:
                pass

    def _release_terminal_inflight(self, event: BrokerEvent) -> None:
        if self._native_inflight_release is None or event.event_kind is not BrokerEventKind.ORDER:
            return
        if event.normalized_summary.get("ledger_state") not in {"FILLED", "CANCELED", "REJECTED"}:
            return
        execution_id = event.execution_id
        if execution_id is None and self._terminal_execution_id_resolver is not None:
            execution_id = self._terminal_execution_id_resolver(event)
        if execution_id is None:
            raise EmtMmapNativeCallJournalError(
                "terminal EMT callback has no execution ID for inflight release"
            )
        self._native_inflight_release(execution_id)


class EmtMmapNativeCallJournal:
    """Commit mmap PRE_NATIVE and callback frames around EMT side effects."""

    def __init__(
        self,
        *,
        route_resolver: EmtNativeRouteResolver,
        order_outcome_directory: FastSpiV2RouteOutcomeDirectory,
        cancel_outcome_observer: FastSpiV2CancelOutcomeObserver,
        mmap_pre_native_journals: Mapping[str, MmapPreNativeJournal],
        mmap_callback_outcomes: MmapCallbackOutcomeJournal,
        mmap_route_ledger: MmapRouteLedgerV2 | None = None,
        archive: JournalArchiveSink | None = None,
        clock: Callable[[], int] = monotonic_ns,
        gate_stage_observer: Callable[[EmtSubmitRequest, str, int], None] | None = None,
        callback_route_stage_observer: Callable[[BrokerEvent, str, int], None] | None = None,
        recovery_submit_guard: Callable[[str], None] | None = None,
        committed_outcome_hook: Callable[[BrokerEvent, FastSpiV2OutcomeResolution], None]
        | None = None,
        maximum_cancel_callback_mappings: int = 4096,
    ) -> None:
        if not 1 <= maximum_cancel_callback_mappings <= 1_000_000:
            raise ValueError("EMT cancel callback mapping capacity is invalid")
        if not mmap_pre_native_journals or any(not key for key in mmap_pre_native_journals):
            raise ValueError("mmap native Journal lane mapping cannot be empty")
        self._resolver = route_resolver
        self._order_outcome_directory = order_outcome_directory
        self._cancel_outcome_observer = cancel_outcome_observer
        self._mmap_pre_native_journals = dict(mmap_pre_native_journals)
        self._mmap_callback_outcomes = mmap_callback_outcomes
        self._mmap_route_ledger = mmap_route_ledger
        self._archive = archive
        self._clock = clock
        self._gate_stage_observer = gate_stage_observer
        self._recovery_submit_guard = recovery_submit_guard
        self._mmap_lane_context = local()
        self._mmap_lane_by_client_ref: dict[str, str] = {}
        self._mmap_lane_lock = RLock()
        self._cancel_callback_routes_by_order: dict[int, tuple[str, VenueOrderIdentity]] = {}
        self._cancel_callback_lock = RLock()
        self._maximum_cancel_callback_mappings = maximum_cancel_callback_mappings
        self._consumer_outcome_hook = committed_outcome_hook
        def on_outcome_committed(event: BrokerEvent, resolution: FastSpiV2OutcomeResolution) -> None:
            # Only a successfully journaled, route-proven event can advance
            # the route directory or downstream Consumer state.
            order_outcome_directory.record_outcome_committed(event, resolution)
            if committed_outcome_hook is not None:
                committed_outcome_hook(event, resolution)

        self._order_outcome_observer = FastSpiV2NativeOutcomeObserver(
            journal=mmap_callback_outcomes,
            resolve_outcome=order_outcome_directory.resolve_outcome,
            outcome_committed=on_outcome_committed,
            callback_stage_observer=callback_route_stage_observer,
        )

    @property
    def route_resolver(self) -> EmtNativeRouteResolver:
        """The resolver whose prepared identity this Journal commits."""

        return self._resolver

    @property
    def consumer_outcome_hook(self) -> Callable[[BrokerEvent, FastSpiV2OutcomeResolution], None] | None:
        """The exact post-commit callback hook installed at construction."""

        return self._consumer_outcome_hook

    @property
    def has_recovery_submit_guard(self) -> bool:
        return self._recovery_submit_guard is not None

    @property
    def recovery_submit_guard(self) -> Callable[[str], None] | None:
        return self._recovery_submit_guard

    def cancel_request_id_for_order(self, order_emt_id: int) -> str | None:
        if type(order_emt_id) is not int or order_emt_id <= 0:
            return None
        with self._cancel_callback_lock:
            item = self._cancel_callback_routes_by_order.get(order_emt_id)
            return item[0] if item else None

    def cancel_target_identity_for_order(self, order_emt_id: int) -> VenueOrderIdentity | None:
        if type(order_emt_id) is not int or order_emt_id <= 0:
            return None
        with self._cancel_callback_lock:
            item = self._cancel_callback_routes_by_order.get(order_emt_id)
            return item[1] if item else None

    def record_submit_started(self, request: EmtSubmitRequest) -> None:
        route = self._submit_route(request)
        # The prepared deadline originates at Consumer OMS admission, not
        # when the worker dequeues. Reject stale work at the final Journal
        # gate even if an intermediate scheduler used a later time base.
        if self._clock() >= route.command.native_start_deadline_ns:
            raise EmtMmapNativeCallJournalError("EMT submit expired before PRE_NATIVE")
        # Startup composition supplies the F1 recovery gate. It is optional
        # only for isolated legacy/unit constructions; a recovered runtime
        # must call it before route registration or any mmap/native effect.
        if self._recovery_submit_guard is not None:
            self._recovery_submit_guard(route.binding.account_id)
        self._gate_stage(request, "C1B_AFTER_ROUTE_RESOLUTION")
        self._order_outcome_directory.register_pre_native_route(
            binding=route.binding,
            command=route.command,
            venue_order_identity=route.venue_order_identity,
        )
        self._gate_stage(request, "C1C_AFTER_LOCAL_CALLBACK_REGISTRATION")
        lane_id, journal = self._current_lane()
        self._gate_stage(request, "C1M0_BEFORE_PRE_NATIVE_MMAP_COMMIT")
        pre_native = journal.append_pre_native(
            monotonic_ns=self._clock(),
            account_id=route.command.account_id,
            execution_id=route.command.execution_id,
            command_id=route.command.command_id,
            payload_hash=route.command.payload_hash,
            client_order_ref=route.command.client_order_ref,
        )
        self._gate_stage(request, "C1M1_AFTER_PRE_NATIVE_MMAP_COMMIT")
        if self._mmap_route_ledger is not None:
            self._record_route_ledger_pre_dispatch(route, request, journal, pre_native.sequence)
            self._gate_stage(request, "C1M1A_AFTER_ROUTE_LEDGER_MMAP_COMMIT")
        # ``CALL_STARTED`` is deliberately a distinct durable fact.  It is
        # written after the pre-native fence and before the caller can enter
        # ``TraderApi.InsertOrder``; restart recovery therefore classifies a
        # missing receipt as SUBMIT_UNKNOWN rather than guessing or resending.
        journal.append_call_started(
            monotonic_ns=self._clock(),
            account_id=route.command.account_id,
            execution_id=route.command.execution_id,
            command_id=route.command.command_id,
            payload_hash=route.command.payload_hash,
            client_order_ref=route.command.client_order_ref,
        )
        self._gate_stage(request, "C1M2_AFTER_CALL_STARTED_MMAP_COMMIT")
        with self._mmap_lane_lock:
            if route.command.client_order_ref in self._mmap_lane_by_client_ref:
                raise EmtMmapNativeCallJournalError("mmap Journal client reference is duplicated")
            self._mmap_lane_by_client_ref[route.command.client_order_ref] = lane_id
        self._archive_offer(
            "ORDER_PRE_NATIVE",
            route.command.command_id,
            route.command.execution_id,
            route.command.payload_hash,
        )
        self._archive_offer(
            "ORDER_CALL_STARTED",
            route.command.command_id,
            route.command.execution_id,
            route.command.payload_hash,
        )

    def record_submit_accepted(self, request: EmtSubmitRequest, receipt: object) -> None:
        route = self._submit_route(request)
        native_order_id = str(getattr(receipt, "order_emt_id"))
        accepted_ns = int(getattr(receipt, "accepted_monotonic_ns"))
        journal = self._lane_for_client_ref(route.command.client_order_ref)
        journal.append_native_accepted(
            monotonic_ns=accepted_ns,
            account_id=route.command.account_id,
            execution_id=route.command.execution_id,
            command_id=route.command.command_id,
            payload_hash=route.command.payload_hash,
            client_order_ref=route.command.client_order_ref,
            native_order_id=native_order_id,
        )
        if self._mmap_route_ledger is not None:
            self._mmap_route_ledger.append_native_accepted(
                order_client_id=request.order_client_id,
                order_emt_id=int(getattr(receipt, "order_emt_id")),
            )
        self._archive_offer(
            "ORDER_NATIVE_ACCEPTED",
            route.command.command_id,
            route.command.execution_id,
            _transition_id("emt-submit-accepted", route.command.payload_hash, native_order_id),
        )

    def record_cancel_started(self, request: EmtCancelRequest) -> None:
        route = self._cancel_route(request)
        self._mmap_callback_outcomes.append_cancel_started(
            route.binding, route.command, native_order_id=str(request.order_emt_id)
        )
        self._archive_offer(
            "CANCEL_STARTED",
            route.command.cancel_request_id,
            route.command.execution_id,
            route.command.payload_hash,
        )
        self._cancel_outcome_observer.register_cancel_route(
            binding=route.binding, command=route.command, target_identity=route.target_identity
        )
        self._register_cancel_callback_route(
            request.order_emt_id, route.command.cancel_request_id, route.target_identity
        )

    def record_cancel_accepted(self, request: EmtCancelRequest, receipt: object) -> None:
        route = self._cancel_route(request)
        native_cancel_id = str(getattr(receipt, "order_cancel_emt_id"))
        commit = self._mmap_callback_outcomes.append_cancel_native_accepted(
            route.binding, route.command, native_order_id=native_cancel_id
        )
        self._archive_offer(
            "CANCEL_NATIVE_ACCEPTED",
            route.command.cancel_request_id,
            route.command.execution_id,
            commit.transition_hash,
        )

    def wrap_broker_event_observer(
        self,
        downstream: BrokerEventObserver,
        *,
        callback_stage_observer: Callable[[BrokerEvent, str, int], None] | None = None,
        native_inflight_release: Callable[[str], bool] | None = None,
        callback_ingress_is_external: bool = False,
    ) -> EmtJournaledBrokerEventObserver:
        return EmtJournaledBrokerEventObserver(
            order_outcomes=self._order_outcome_observer,
            cancel_outcomes=self._cancel_outcome_observer,
            downstream=downstream,
            callback_stage_observer=callback_stage_observer,
            native_inflight_release=native_inflight_release,
            terminal_execution_id_resolver=self._order_outcome_directory.execution_id_for_callback,
            is_owned_order_callback=self._order_outcome_directory.has_registered_callback_route,
            callback_ingress_is_external=callback_ingress_is_external,
            clock=self._clock,
        )

    def close(self) -> None:
        try:
            self._mmap_callback_outcomes.close()
        finally:
            try:
                for journal in {
                    id(value): value for value in self._mmap_pre_native_journals.values()
                }.values():
                    journal.close()
            finally:
                if self._mmap_route_ledger is not None:
                    self._mmap_route_ledger.close()

    @contextmanager
    def mmap_native_lane(self, lane_id: str) -> Iterator[None]:
        if lane_id not in self._mmap_pre_native_journals:
            raise EmtMmapNativeCallJournalError("mmap native dispatch lane is unknown")
        previous = getattr(self._mmap_lane_context, "lane_id", None)
        self._mmap_lane_context.lane_id = lane_id
        try:
            yield
        finally:
            if previous is None:
                del self._mmap_lane_context.lane_id
            else:
                self._mmap_lane_context.lane_id = previous

    def _current_lane(self) -> tuple[str, MmapPreNativeJournal]:
        lane_id = getattr(self._mmap_lane_context, "lane_id", None)
        if lane_id is None and len(self._mmap_pre_native_journals) == 1:
            lane_id = next(iter(self._mmap_pre_native_journals))
        journal = self._mmap_pre_native_journals.get(lane_id)
        if journal is None:
            raise EmtMmapNativeCallJournalError("mmap native dispatch lane is not bound")
        return lane_id, journal

    def _lane_for_client_ref(self, client_order_ref: str) -> MmapPreNativeJournal:
        with self._mmap_lane_lock:
            lane_id = self._mmap_lane_by_client_ref.get(client_order_ref)
            journal = self._mmap_pre_native_journals.get(lane_id)
        if journal is None:
            raise EmtMmapNativeCallJournalError("mmap Journal acceptance has no native lane")
        return journal

    def _submit_route(self, request: EmtSubmitRequest) -> EmtSubmitRoute:
        try:
            route = self._resolver.resolve_submit(request)
        except BaseException as exc:
            raise EmtMmapNativeCallJournalError("EMT submit has no prepared mmap route") from exc
        route.validate(request)
        return route

    def _cancel_route(self, request: EmtCancelRequest) -> EmtCancelRoute:
        try:
            route = self._resolver.resolve_cancel(request)
        except BaseException as exc:
            raise EmtMmapNativeCallJournalError("EMT cancel has no prepared mmap route") from exc
        route.validate(request)
        return route

    def _record_route_ledger_pre_dispatch(
        self,
        route: EmtSubmitRoute,
        request: EmtSubmitRequest,
        journal: MmapPreNativeJournal,
        frame_sequence: int,
    ) -> None:
        """Write R1's strategy route proof before ``CALL_STARTED``/SDK entry.

        This opt-in companion Journal is purposefully absent from legacy
        profiles.  An R1 profile cannot fall back to a strategy-less fact:
        without this proof an R2 result must remain unresolved and no native
        call may begin.
        """

        assert self._mmap_route_ledger is not None
        directory = self._mmap_route_ledger.directory
        scope = directory.scope
        strategy_id = request.instruction.consumer_trace.strategy_id
        if (
            strategy_id is None
            or scope.account_id != route.command.account_id
            or scope.trading_day != request.instruction.trading_day
            or not directory.own.contains(request.order_client_id)
        ):
            raise EmtMmapNativeCallJournalError(
                "R1 route ledger cannot prove namespace, trading day, or strategy ownership"
            )
        self._mmap_route_ledger.append_pre_dispatch(
            RouteLedgerFactV2(
                scope=scope,
                order_client_id=request.order_client_id,
                namespace_id=directory.own.namespace_id,
                namespace_manifest_hash=directory.manifest_hash,
                owner_system_id=directory.own.owner_system_id,
                adapter_instance_id=directory.own.adapter_instance_id,
                strategy_id=strategy_id,
                execution_id=route.command.execution_id,
                journal_epoch=journal.journal_epoch,
                frame_sequence=frame_sequence,
                route_payload_hash=route.command.payload_hash,
            )
        )

    def _gate_stage(self, request: EmtSubmitRequest, stage: str) -> None:
        if self._gate_stage_observer is not None:
            try:
                self._gate_stage_observer(request, stage, self._clock())
            except BaseException:
                pass

    def _register_cancel_callback_route(
        self, order_emt_id: int, cancel_request_id: str, target_identity: VenueOrderIdentity
    ) -> None:
        if type(order_emt_id) is not int or order_emt_id <= 0:
            raise EmtMmapNativeCallJournalError("EMT cancel target has no positive native order id")
        if target_identity.venue_order_id != str(order_emt_id):
            raise EmtMmapNativeCallJournalError(
                "EMT cancel target identity does not match native order id"
            )
        candidate = (cancel_request_id, target_identity)
        with self._cancel_callback_lock:
            existing = self._cancel_callback_routes_by_order.get(order_emt_id)
            if existing is not None:
                if existing == candidate:
                    return
                raise EmtMmapNativeCallJournalError(
                    "EMT native order id is already bound to another cancel request"
                )
            if len(self._cancel_callback_routes_by_order) >= self._maximum_cancel_callback_mappings:
                raise EmtMmapNativeCallJournalError("EMT cancel callback mapping table is full")
            self._cancel_callback_routes_by_order[order_emt_id] = candidate

    def _archive_offer(
        self, kind: str, command_id: str, correlation_id: str, payload_hash: str
    ) -> None:
        if self._archive is not None:
            try:
                self._archive.offer(
                    kind=kind,
                    command_id=command_id,
                    correlation_id=correlation_id,
                    payload_hash=payload_hash,
                )
            except BaseException:
                pass


def _transition_id(prefix: str, *parts: str) -> str:
    return f"{prefix}:{sha256(chr(0).join(parts).encode('ascii')).hexdigest()}"
