"""Opt-in single-binding UDS composition for an already-fenced EMT worker.

This bootstrap does not load an SDK, configure credentials, prove recovery or
arm a broker account. Its caller owns runtime, risk and readiness evidence.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Protocol

from emt_gateway.consumer_simulator_gate_v1 import ConsumerEmtSimulatorGateV1

from ...consumer.base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerAccountView,
    ConsumerOrderSignal,
    ConsumerSession,
)
from ...consumer.bindings_v2 import ConsumerBindingSnapshotV2
from ...consumer.dispatch_v2 import ConsumerWireDispatcherV2
from ...consumer.gateway_v2 import ConsumerGatewayV2
from ...consumer.oms_operations_v2 import ConsumerOmsAdmissionContextV2
from ...consumer.replay_v2 import ConsumerCursorReplayServiceV2
from ...consumer.service_v2 import ConsumerUnixServiceV2
from ...consumer.uds_server_v2 import load_consumer_request_schema
from ...consumer.unix_v2 import (
    ConsumerUnixPeerV2,
    ConsumerUnixSocketError,
    ConsumerUnixSocketListenerV2,
)
from ...consumer.wire_v2 import load_contract_identity
from ...domain import (
    AccountLifecycle,
    ConsumerAccountBinding,
    ConsumerOrderEvent,
    ConsumerPrincipal,
    ReconciliationStatus,
)
from ...oms import OrderBook
from .emt_callback_pump_v1 import ManagedEmtCallbackPumpV1
from .emt_submit_composition_v1 import (
    ConsumerEmtRuntimeGatePortV1,
    ManagedEmtConsumerSubmitV1,
    ManagedEmtExecutionSessionV1,
    build_consumer_emt_submit_operations_v1,
)


class ManagedConsumerEmtGatePortV1(Protocol):
    @property
    def gate(self) -> ConsumerEmtRuntimeGatePortV1: ...
    def start(self) -> None: ...
    def close(self) -> None: ...


def build_consumer_emt_submit_uds_v1(
    *,
    socket_path: Path,
    contract_root: Path,
    principal: ConsumerPrincipal,
    allowed_peers: frozenset[tuple[int, int]],
    binding_snapshot: ConsumerBindingSnapshotV2,
    account_view_for: Callable[[ConsumerAccountBinding], ConsumerAccountView],
    order_book: OrderBook,
    managed_worker: ManagedEmtConsumerSubmitV1,
    admission_context_for: Callable[
        [ConsumerAccountBinding, str, ConsumerOrderSignal], ConsumerOmsAdmissionContextV2
    ],
    now_monotonic_ns: Callable[[], int],
    on_order_event_committed: Callable[[ConsumerOrderEvent], None] | None = None,
    snapshot_for: Callable[
        [ConsumerSession, ConsumerAccountBinding, str | None], ConsumerAccountOrderSnapshotV2
    ] | None = None,
    event_replay: ConsumerCursorReplayServiceV2 | None = None,
    managed_session: ManagedEmtExecutionSessionV1 | None = None,
    callback_pump: ManagedEmtCallbackPumpV1 | None = None,
    runtime_gate: ConsumerEmtRuntimeGatePortV1 | None = None,
    managed_gate: ManagedConsumerEmtGatePortV1 | None = None,
    socket_mode: int = 0o600,
) -> ConsumerUnixServiceV2:
    """Assemble one UDS and managed owner without substituting readiness evidence.

    ``account_view_for`` must be a current, verified, bounded in-process read.
    READY + VALIDATED admits the normal profile. An explicitly scoped
    simulator gate may admit SIMULATOR_ACTIVE without claiming recovery.
    This submit-only
    profile does not support cancellation. An optional replay service and
    publisher let a caller expose its own committed event projection; the
    caller must also connect that publisher to the EMT outcome projector and
    supply any required restart recovery. This is not a recovery certificate.
    """

    binding = managed_worker.worker.preparer.consumer_binding
    if (
        binding.venue_id != "EMT"
        or binding_snapshot.accounts != (binding,)
        or principal.consumer_id != binding.consumer_id
        or not principal.active
        or not allowed_peers
        or (event_replay is not None and on_order_event_committed is None)
        or ((runtime_gate is None) != (managed_gate is None))
        or (runtime_gate is not None and (
            managed_gate is None
            or managed_gate.gate is not runtime_gate
            or managed_session is None
            or callback_pump is None
            or getattr(managed_worker.worker.native_journal, "recovery_submit_guard", None)
            != runtime_gate.require_account
        ))
        or (managed_session is not None and managed_session.runtime is not managed_worker.worker.runtime)
        or (callback_pump is not None and (
            callback_pump.runtime is not managed_worker.worker.runtime
            or callback_pump.failure_target is not managed_worker
        ))
        or any(
            type(uid) is not int or type(gid) is not int or uid < 0 or gid < 0
            for uid, gid in allowed_peers
        )
    ):
        raise ConsumerUnixSocketError("EMT UDS scope or authorized peer set is invalid")
    binding_snapshot.require_principal(principal)

    def account_ready(candidate: ConsumerAccountBinding) -> bool:
        view = account_view_for(candidate)
        simulator_ready = (
            isinstance(runtime_gate, ConsumerEmtSimulatorGateV1)
            and view == runtime_gate.account_view(candidate)
            and view.lifecycle is AccountLifecycle.SIMULATOR_ACTIVE
            and view.reconciliation_status is ReconciliationStatus.NOT_REQUIRED
        )
        return (
            candidate == binding
            and view.consumer_account_id == binding.consumer_account_id
            and view.venue_id == "EMT"
            and (
                simulator_ready if isinstance(runtime_gate, ConsumerEmtSimulatorGateV1) else (
                    view.lifecycle is AccountLifecycle.READY
                    and view.reconciliation_status is ReconciliationStatus.VALIDATED
                )
            )
            and not view.gates
            and (callback_pump is None or callback_pump.healthy())
        )

    operations = build_consumer_emt_submit_operations_v1(
        order_book=order_book,
        managed_worker=managed_worker,
        admission_context_for=admission_context_for,
        account_ready=account_ready,
        snapshot_for=snapshot_for,
        on_order_event_committed=on_order_event_committed,
        runtime_gate=runtime_gate,
        now_monotonic_ns=now_monotonic_ns,
    )
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: binding_snapshot,
        account_view_for=account_view_for,
        wire_contract=load_contract_identity(contract_root),
        operations=operations,
        event_replay=event_replay,
    )

    def principal_for_peer(peer: ConsumerUnixPeerV2) -> ConsumerPrincipal:
        if (peer.uid, peer.gid) not in allowed_peers:
            raise ConsumerUnixSocketError("EMT UDS peer is not authorized")
        return principal

    listener = ConsumerUnixSocketListenerV2(
        socket_path=socket_path,
        gateway=gateway,
        principal_for_peer=principal_for_peer,
        request_schema=load_consumer_request_schema(contract_root),
        request_handler=ConsumerWireDispatcherV2(gateway),
        socket_mode=socket_mode,
        remove_verified_stale_socket=False,
    )
    # Start the signed/fenced session before its owner and listener. Shutdown
    # reverses this order; a worker which cannot be joined must not release
    # the session or its Journals underneath an in-flight native call.
    components = (
        *((managed_gate,) if managed_gate is not None else ()),
        *((managed_session,) if managed_session is not None else ()),
        *((callback_pump,) if callback_pump is not None else ()),
        managed_worker,
    )
    return ConsumerUnixServiceV2(listener, request_reactor=True, managed_components=components)
