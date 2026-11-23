"""Explicit no-Adapter public frontend: immutable auth, sockets and IPC only.

This builder never opens a Journal, cursor identity, Book, writer lease or SQL
database. All reads are prepared AND delivery-checked by the unique owner.
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import TYPE_CHECKING

from ..domain import ConsumerPrincipal
from .bindings_v2 import ConsumerBindingSnapshotV2
from .dispatch_v2 import ConsumerWireDispatcherV2
from .fair_queue_v2 import BoundedFairOperationQueueV2, ConsumerOperationLaneV2
from .frontend_handoff_listener import ConsumerHandoffSocketListenerV2
from .frontend_shared_credit import FrontendSharedCreditV1
from .gateway_v2 import ConsumerGatewayV2
from .long_running_candidate import candidate_binding_scope
from .owner_ipc_v1 import OwnerReadyV1
from .owner_reactor_ipc import ReactorOwnerPortV1
from .owner_read_reactor import ReactorOwnerReadsV1
from .service_v2 import ConsumerUnixServiceV2
from .uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    consumer_no_adapter_account_view,
    load_consumer_request_schema,
)
from .unix_v2 import ConsumerUnixPeerV2, ConsumerUnixSocketError, ConsumerUnixSocketListenerV2
from .wire_v2 import load_contract_identity

if TYPE_CHECKING:
    from .process_oms_owner import SpawnedOmsOwnerCandidate


def build_process_owner_frontend(
    *,
    config: ConsumerUdsServerConfigV2,
    contract_root: Path,
    owner: SpawnedOmsOwnerCandidate | None = None,
    handoff_channel: socket.socket | None = None,
    handoff_generation: str | None = None,
    broker_ready: OwnerReadyV1 | None = None,
    broker_write_channel: socket.socket | None = None,
    broker_read_channels: tuple[socket.socket, ...] | None = None,
    shared_credit: FrontendSharedCreditV1 | None = None,
    shared_read_slots: FrontendSharedCreditV1 | None = None,
    frontend_worker_id: int | None = None,
    direct_owner_reads: bool = False,
) -> ConsumerUnixServiceV2:
    broker_values = (
        broker_ready,
        broker_write_channel,
        broker_read_channels,
        shared_credit,
        shared_read_slots,
        frontend_worker_id,
    )
    brokered = all(value is not None for value in broker_values)
    if owner is not None and any(value is not None for value in broker_values):
        raise ConsumerUnixSocketError("frontend cannot combine direct owner and broker ports")
    if owner is None and not brokered:
        raise ConsumerUnixSocketError("frontend requires direct owner or complete broker ports")
    if owner is not None and not owner.has_read_ipc:
        raise ConsumerUnixSocketError("full public frontend requires authoritative read IPC")
    if brokered and (handoff_channel is None or handoff_generation is None):
        raise ConsumerUnixSocketError("brokered frontend requires sole-acceptor FD handoff")
    if direct_owner_reads and (
        not brokered
        or broker_read_channels is None
        or len(broker_read_channels) != 2
    ):
        raise ConsumerUnixSocketError(
            "direct owner READ requires dedicated replay and snapshot rendezvous"
        )
    account, strategies, _ = candidate_binding_scope(config)
    bindings = ConsumerBindingSnapshotV2(1, (account,), strategies)
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: bindings,
        account_view_for=lambda binding: consumer_no_adapter_account_view(binding, account),
        wire_contract=load_contract_identity(contract_root),
    )
    allowed = frozenset(((config.peer_uid, config.peer_gid), *config.additional_peer_credentials))
    principal = ConsumerPrincipal(config.consumer_id, config.login_name, config.legal_name)

    def authorize(peer: ConsumerUnixPeerV2) -> ConsumerPrincipal:
        if (peer.uid, peer.gid) not in allowed:
            raise ConsumerUnixSocketError("public owner frontend peer denied")
        return principal

    if (handoff_channel is None) != (handoff_generation is None):
        raise ConsumerUnixSocketError("frontend FD channel/generation must be supplied together")
    schema = load_consumer_request_schema(contract_root)
    dispatcher = ConsumerWireDispatcherV2(gateway)
    if handoff_channel is not None and handoff_generation is not None:
        listener: ConsumerUnixSocketListenerV2 = ConsumerHandoffSocketListenerV2(
            handoff_channel=handoff_channel,
            generation=handoff_generation,
            socket_path=config.socket_path,
            gateway=gateway,
            principal_for_peer=authorize,
            request_schema=schema,
            request_handler=dispatcher,
            max_requests_per_connection=config.max_requests_per_connection,
            socket_mode=config.socket_mode,
        )
    else:
        listener = ConsumerUnixSocketListenerV2(
            socket_path=config.socket_path,
            gateway=gateway,
            principal_for_peer=authorize,
            request_schema=schema,
            # The fallback cannot write because this gateway has operations=None.
            # Every reviewed public read/write is intercepted by the reactor.
            request_handler=dispatcher,
            max_requests_per_connection=config.max_requests_per_connection,
            listen_backlog=config.listen_backlog,
            remove_verified_stale_socket=True,
            socket_mode=config.socket_mode,
        )
    if owner is not None:
        owner_port_factory = owner.open_reactor_port
        owner_read_factory = owner.open_read_pool
    else:
        assert broker_ready is not None
        assert broker_write_channel is not None
        assert broker_read_channels is not None
        assert shared_credit is not None
        assert shared_read_slots is not None
        assert frontend_worker_id is not None
        opened_write = opened_reads = False

        def owner_port_factory(
            ready_queue: BoundedFairOperationQueueV2[socket.socket],
        ) -> ReactorOwnerPortV1[socket.socket]:
            nonlocal opened_write
            if opened_write:
                raise ConsumerUnixSocketError("broker WRITE port already transferred")
            opened_write = True
            return ReactorOwnerPortV1(
                broker_write_channel,
                ready=broker_ready,
                ready_queue=ready_queue,
                shared_credit=shared_credit,
                worker_id=frontend_worker_id,
            )

        def owner_read_factory(
            ready_queue: BoundedFairOperationQueueV2[socket.socket],
        ) -> ReactorOwnerReadsV1[socket.socket]:
            nonlocal opened_reads
            if opened_reads:
                raise ConsumerUnixSocketError("broker READ ports already transferred")
            opened_reads = True
            return ReactorOwnerReadsV1(
                broker_read_channels,
                ready_queue=ready_queue,
                shared_credit=shared_credit,
                shared_read_slots=shared_read_slots,
                worker_id=frontend_worker_id,
                release_ack_required=not direct_owner_reads,
                frontend_slot_count=2 if direct_owner_reads else len(broker_read_channels),
                channel_lanes=(
                    (
                        frozenset({ConsumerOperationLaneV2.REPLAY}),
                        frozenset(
                            {
                                ConsumerOperationLaneV2.SNAPSHOT,
                                ConsumerOperationLaneV2.OTHER,
                            }
                        ),
                    )
                    if direct_owner_reads
                    else None
                ),
            )

    return ConsumerUnixServiceV2(
        listener,
        frame_ready_ingress=True,
        request_reactor=True,
        pending_capacity=config.pending_capacity,
        preframe_capacity=config.preframe_capacity,
        owner_port_factory=owner_port_factory,
        owner_account_id=config.consumer_account_id,
        owner_read_factory=owner_read_factory,
    )
