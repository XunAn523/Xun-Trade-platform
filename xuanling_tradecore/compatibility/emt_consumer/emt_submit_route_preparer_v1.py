"""Prepare one Consumer-origin EMT route from committed OMS facts.

This does not start a service, invoke an Adapter, or change readiness. A
separate EMS owner must dispatch the prepared route inside its mmap lane.
"""

from __future__ import annotations

from hashlib import sha256
from threading import Lock
from typing import Callable

from emt_gateway.tradecore_mmap_journal import EmtSubmitRoute, PreparedEmtNativeRouteResolver

from ...adapters.emt.identity import EmtClientOrderIdAllocator
from ...adapters.emt.namespace_recovery import (
    NamespaceOwnership,
    VerifiedNamespaceBoundEmtClientOrderIdAllocator,
)
from ...adapters.emt.namespace_release_v1 import ActivatedNamespaceReleaseV1
from ...adapters.emt.route_token import EmtAdapterRouteTokenCodec
from ...adapters.identity import ClientOrderRefAllocationRequest
from ...adapters.route_token import Sha256AdapterRouteTokenFactory
from ...domain import ConsumerAccountBinding, VenueOrderIdentity
from ...ems.routing import AccountExecutionBinding
from ...oms import (
    MmapOmsFactJournalV1,
    OmsFactKind,
    OmsJournalCommit,
    OmsJournalFact,
    OrderBookOrder,
)
from ...oms.mmap_client_ref_journal_v1 import MmapClientRefJournalV1
from ...persistence import AccountJournalBinding, MmapAdmissionEnvelopeV1, PreNativeRouteCommand
from .emt_committed_admission_v1 import verify_emt_committed_admission_v1


class EmtSubmitRoutePreparationErrorV1(RuntimeError):
    """An OMS execution cannot be safely prepared for an EMT native call."""


class EmtConsumerSubmitRoutePreparerV1:
    """One serialized owner for signed-scope reference allocation and route preparation."""

    def __init__(
        self,
        *,
        oms_journal: MmapOmsFactJournalV1,
        client_ref_journal: MmapClientRefJournalV1,
        allocator: VerifiedNamespaceBoundEmtClientOrderIdAllocator,
        activated_release: ActivatedNamespaceReleaseV1,
        expected_release_manifest_hash: str,
        consumer_binding: ConsumerAccountBinding,
        execution_binding: AccountExecutionBinding,
        journal_binding: AccountJournalBinding,
        codec: EmtAdapterRouteTokenCodec,
        resolver: PreparedEmtNativeRouteResolver,
        admission_authorized: Callable[[], bool],
        close_admission: Callable[[str], None],
        wall_clock_ns: Callable[[], int],
        monotonic_ns: Callable[[], int],
        native_start_budget_ns: int,
    ) -> None:
        directory, state = activated_release.directory, activated_release.state
        scope = allocator.scope
        if (
            allocator.directory is not directory
            or allocator.journal is not client_ref_journal
            or state.scope != directory.scope
            or state.namespace_manifest_hash != directory.manifest_hash
            or state.release_manifest_hash != expected_release_manifest_hash
            or directory.own.release_manifest_hash != expected_release_manifest_hash
            or scope.account_id != directory.scope.account_id
            or scope.trading_day != directory.scope.trading_day
            or scope.adapter_client_scope != str(directory.scope.emt_client_id)
            or scope.adapter_instance_id != directory.own.adapter_instance_id
            or scope.venue_id != "EMT"
            or not consumer_binding.active
            or consumer_binding.binding_id != execution_binding.binding_id
            or consumer_binding.binding_version != execution_binding.binding_version
            or consumer_binding.account_id != scope.account_id
            or consumer_binding.venue_id != scope.venue_id
            or consumer_binding.adapter_instance_id != scope.adapter_instance_id
            or execution_binding.account_id != scope.account_id
            or execution_binding.venue_id != scope.venue_id
            or execution_binding.adapter_instance_id != scope.adapter_instance_id
            or execution_binding.adapter_client_scope != scope.adapter_client_scope
            or journal_binding.account_id != scope.account_id
            or journal_binding.venue_id != scope.venue_id
            or type(native_start_budget_ns) is not int
            or native_start_budget_ns <= 0
        ):
            raise EmtSubmitRoutePreparationErrorV1("EMT route composition scope is inconsistent")
        recovery = client_ref_journal.read_recovery(scope)
        if (
            max(recovery.committed_allocation_sequence, directory.own.range_start - 1)
            < state.high_watermark
        ):
            raise EmtSubmitRoutePreparationErrorV1(
                "client reference journal is behind release watermark"
            )
        client_ref_journal.claim_single_preparer()
        self._oms_journal = oms_journal
        self._refs = client_ref_journal
        self._allocator = allocator
        self._activated_release = activated_release
        self._directory = directory
        self._consumer = consumer_binding
        self._execution = execution_binding
        self._binding = journal_binding
        self._codec = codec
        self._resolver = resolver
        self._authorized = admission_authorized
        self._close = close_admission
        self._wall = wall_clock_ns
        self._clock = monotonic_ns
        self._budget = native_start_budget_ns
        self._lock = Lock()
        self._failed = False

    @property
    def resolver(self) -> PreparedEmtNativeRouteResolver:
        return self._resolver

    @property
    def activated_release(self) -> ActivatedNamespaceReleaseV1:
        """Return the exact verified release used by this route preparer."""

        return self._activated_release

    @property
    def oms_journal(self) -> MmapOmsFactJournalV1:
        return self._oms_journal

    @property
    def consumer_binding(self) -> ConsumerAccountBinding:
        return self._consumer

    @property
    def admission_authorizer(self) -> Callable[[], bool]:
        return self._authorized

    @property
    def execution_binding(self) -> AccountExecutionBinding:
        return self._execution

    def admission_permitted(self) -> bool:
        """Read a cached runtime gate and signed lease before OMS admission.

        The supplier must be a bounded, in-process read with no I/O. Do not
        acquire the serial route-preparation lock on the Consumer hot path;
        the owner rechecks the gate before any native side effect.
        """

        if self._failed:
            return False
        try:
            return self._authorized() is True and self._directory.own.active_at(self._wall())
        except Exception:
            return False

    def prepare(self, order: OrderBookOrder, commit: OmsJournalCommit) -> EmtSubmitRoute:
        """Verify source facts, commit one reference, then register one route.

        Existing references never trigger a retry: they require verified
        reconciliation. PRE_NATIVE is still committed by the native Journal
        immediately before the SDK call, not by this method.
        """

        with self._lock:
            if self._failed:
                raise EmtSubmitRoutePreparationErrorV1("EMT route preparer is latched")
            try:
                return self._prepare(order, commit)
            except Exception as exc:
                self._failed = True
                try:
                    self._close("consumer-emt-route-preparation-failed")
                except Exception as close_error:
                    raise EmtSubmitRoutePreparationErrorV1(
                        "EMT route failure and admission closure is unconfirmed"
                    ) from close_error
                raise EmtSubmitRoutePreparationErrorV1(
                    "EMT route preparation requires reconciliation"
                ) from exc

    def _prepare(self, order: OrderBookOrder, commit: OmsJournalCommit) -> EmtSubmitRoute:
        if self._authorized() is not True or not self._directory.own.active_at(self._wall()):
            raise ValueError("EMT submit gate or signed namespace is not active")
        trace = order.order.consumer_trace
        if (
            trace.consumer_id != self._consumer.consumer_id
            or trace.consumer_account_id != self._consumer.consumer_account_id
            or order.order.account_id != self._consumer.account_id
            or order.order.trading_day != self._allocator.scope.trading_day
        ):
            raise ValueError("consumer order is outside the bound EMT scope")
        deadline = order.order.created_monotonic_ns + self._budget
        if self._clock() >= deadline:
            raise ValueError("consumer execution expired before route preparation")
        evidence = verify_emt_committed_admission_v1(
            journal=self._oms_journal, commit=commit, expected=order
        )
        execution_id = order.execution.execution_id
        if not execution_id.isascii() or len(execution_id) > 128:
            raise ValueError("execution identity cannot be represented by EMT route")
        if self._refs.read_committed_lease(self._allocator.scope, execution_id) is not None:
            raise ValueError("existing client reference requires recovery; no automatic retry")
        lease = self._allocator.allocate(
            ClientOrderRefAllocationRequest(execution_id, self._allocator.scope)
        )
        value = EmtClientOrderIdAllocator.parse_client_order_ref(lease.client_order_ref)
        if self._directory.lookup(value).ownership is not NamespaceOwnership.OWN:
            raise ValueError("allocated client reference is outside the signed own range")
        token = Sha256AdapterRouteTokenFactory(self._directory.manifest_hash).create(
            execution_id=execution_id, scope=lease.scope, allocation_nonce=lease.allocation_sequence
        )
        if (
            self._codec.encode(token, allocated_client_order_ref=lease.client_order_ref)
            != lease.client_order_ref
        ):
            raise ValueError("EMT route token codec returned another client reference")
        started = OmsJournalFact(
            fact_id=f"consumer-call:{sha256(execution_id.encode()).hexdigest()}",
            kind=OmsFactKind.CALL_STARTED,
            account_id=order.order.account_id,
            oms_order_id=order.order.oms_order_id,
            execution_id=execution_id,
            attributes={
                "source_journal_id": commit.journal_id,
                "source_first_lsn": str(commit.first_lsn),
                "source_final_lsn": str(commit.final_lsn),
            },
        )
        command = PreNativeRouteCommand(
            account_id=order.order.account_id,
            command_id=execution_id,
            execution_id=execution_id,
            oms_order_id=order.order.oms_order_id,
            client_order_ref=lease.client_order_ref,
            route_token_digest=sha256(token.token_bytes).hexdigest(),
            binding_manifest_hash=self._execution.mapping_manifest_hash,
            intent_hash=order.order.intent_hash,
            decision_context_id=order.decision_context.decision_context_id,
            native_start_deadline_ns=deadline,
            fact_envelope=MmapAdmissionEnvelopeV1.build(
                account_id=order.order.account_id,
                facts=(*evidence.facts, started),
                maximum_fact_count=3,
                maximum_encoded_bytes=65_536,
            ),
            recovery_seed=evidence.seed,
        )
        identity = VenueOrderIdentity(
            "EMT",
            self._allocator.scope.adapter_instance_id,
            self._allocator.scope.adapter_client_scope,
            client_order_ref=lease.client_order_ref,
        )
        route = EmtSubmitRoute(self._binding, command, identity)
        self._resolver.prepare_submit(route, order.execution)
        return route
