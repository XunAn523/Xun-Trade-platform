"""Fixed inherited read rendezvous and online delivery checks; no public proxy.

The only place with cursor keys and authoritative snapshot/stream state remains
the owner. PREPARED bytes are not permission to disclose them. Every public send
attempt needs an owner GRANT from the unchanged canonical delivery validator.
"""

from __future__ import annotations

import select
import socket
from contextlib import contextmanager, nullcontext
from hashlib import sha256
from threading import Condition, Event
from time import monotonic_ns
from typing import TYPE_CHECKING, Any, Callable, Generator

from ..domain import ConsumerPrincipal
from .fair_queue_v2 import ConsumerOperationLaneV2, consumer_operation_lane_v2
from .owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadMessageV1,
    OwnerReadyV1,
    receive_owner_packet,
    send_owner_packet,
)
from .wire_v2 import MessageKind, decode_frame

if TYPE_CHECKING:
    from .uds_server_v2 import ConsumerUdsServerConfigV2
    from .unix_v2 import ConsumerUnixSocketListenerV2

_OWNER_READ_IDLE_POLL_SECONDS = 0.005


class OwnerReadServiceBudgetV1:
    """Give replay/control a bounded head start ahead of snapshot traffic.

    Admission credits still bound total work. This gate controls only the
    synchronous owner handler start: a priority READ begins without waiting for
    active snapshots and reserves a short window before another snapshot may
    begin. It does not monopolize the full SQL/replay call. Response transport
    and delivery rechecks stay outside the gate.
    """

    def __init__(
        self,
        priority_head_start_ns: int = 50_000,
        read_service_signal: Any | None = None,
    ) -> None:
        if type(priority_head_start_ns) is not int or not 1 <= priority_head_start_ns <= 100_000_000:
            raise ValueError("invalid owner READ head-start budget")
        self._condition = Condition()
        self._active_snapshots = 0
        self._priority_active = False
        self._priority_waiters = 0
        self._priority_head_start_ns = priority_head_start_ns
        self._snapshot_not_before_ns = 0
        self._read_service_signal = read_service_signal

    def _publish_priority(self) -> None:
        if self._read_service_signal is not None:
            self._read_service_signal.value = int(
                self._priority_active or self._priority_waiters > 0
            )

    @contextmanager
    def turn(
        self,
        operation: object,
        *,
        deadline_ns: int,
        stopped: Event,
    ) -> Generator[None, None, None]:
        priority = consumer_operation_lane_v2(operation) is not ConsumerOperationLaneV2.SNAPSHOT
        acquired = False
        with self._condition:
            if priority:
                self._priority_waiters += 1
                self._publish_priority()
            try:
                while True:
                    now = monotonic_ns()
                    unavailable = (
                        self._priority_active
                        if priority
                        else self._priority_waiters > 0 or now < self._snapshot_not_before_ns
                    )
                    if not unavailable:
                        break
                    remaining_ns = deadline_ns - now
                    if stopped.is_set() or remaining_ns <= 0:
                        raise OwnerProtocolError("owner READ service budget expired")
                    service_wait_ns = (
                        max(1, self._snapshot_not_before_ns - now)
                        if not priority and not self._priority_waiters
                        else 50_000_000
                    )
                    self._condition.wait(
                        min(remaining_ns, service_wait_ns) / 1_000_000_000
                    )
                if priority:
                    self._priority_active = True
                    self._snapshot_not_before_ns = max(
                        self._snapshot_not_before_ns,
                        monotonic_ns() + self._priority_head_start_ns,
                    )
                else:
                    self._active_snapshots += 1
                acquired = True
            finally:
                if priority:
                    self._priority_waiters -= 1
                    self._publish_priority()
                    self._condition.notify_all()
        try:
            yield
        finally:
            if acquired:
                with self._condition:
                    if priority:
                        self._priority_active = False
                        self._publish_priority()
                    else:
                        self._active_snapshots -= 1
                    self._condition.notify_all()


def serve_owner_reads(
    channel: socket.socket,
    *,
    listener: ConsumerUnixSocketListenerV2,
    original_config: ConsumerUdsServerConfigV2,
    ready: OwnerReadyV1,
    stopped: Event,
    fence: Callable[[BaseException], None],
    service_budget: OwnerReadServiceBudgetV1 | None = None,
    allowed_lanes: frozenset[ConsumerOperationLaneV2] | None = None,
) -> None:
    if allowed_lanes is None:
        allowed_lanes = frozenset(
            {
                ConsumerOperationLaneV2.REPLAY,
                ConsumerOperationLaneV2.SNAPSHOT,
                ConsumerOperationLaneV2.OTHER,
            }
        )
    if not allowed_lanes or ConsumerOperationLaneV2.WRITE in allowed_lanes:
        raise OwnerProtocolError("owner READ lane scope is invalid")
    last_serial = 0
    def receive():
        while not stopped.is_set():
            if select.select([channel], [], [], _OWNER_READ_IDLE_POLL_SECONDS)[0]:
                message = receive_owner_packet(channel, deadline_ns=monotonic_ns() + 5_000_000_000)
                if type(message) is not OwnerReadMessageV1:
                    raise OwnerProtocolError("owner read message kind mismatch")
                return message
        return None

    def send(message: OwnerReadMessageV1) -> None:
        send_owner_packet(channel, message, deadline_ns=monotonic_ns() + 5_000_000_000)

    try:
        while not stopped.is_set():
            request = receive()
            if request is None:
                return
            identity = request.identity
            authority, peer = identity.authority, identity.peer
            if request.phase != "REQUEST" or (
                identity.generation != ready.generation
                or identity.serial <= last_serial
                or authority.scope_sha256 != ready.scope_sha256
                or authority.consumer_id != original_config.consumer_id
                or authority.account_id != original_config.consumer_account_id
                or authority.strategy_id != "__READ_SCOPE__"
                or peer is None
                or (peer.uid, peer.gid)
                not in {
                    (original_config.peer_uid, original_config.peer_gid),
                    *original_config.additional_peer_credentials,
                }
            ):
                raise OwnerProtocolError("owner read original authority mismatch")
            frame = decode_frame(request.frame)
            operation = frame.payload.get("operation")
            if (
                frame.header.message_kind is not MessageKind.REQUEST
                or frame.payload.get("request_id") != identity.request_id
                or identity.business_key != "READ:" + str(operation)
                or consumer_operation_lane_v2(operation) not in allowed_lanes
                or monotonic_ns() >= request.deadline_ns
            ):
                raise OwnerProtocolError("owner read request association/deadline mismatch")
            last_serial = identity.serial
            principal = ConsumerPrincipal(
                authority.consumer_id, original_config.login_name, original_config.legal_name
            )
            with listener.authorize_owner_read(
                principal, frame, authorization_epoch=authority.authorization_epoch
            ) as session:
                budget = (
                    service_budget.turn(
                        frame.payload.get("operation"),
                        deadline_ns=request.deadline_ns,
                        stopped=stopped,
                    )
                    if service_budget is not None
                    else nullcontext()
                )
                with budget:
                    response = listener.response_for_owner_read(session, frame)
                    # The owner may grant the first immediate public send in the
                    # same reply. PREPARED was followed unconditionally by a
                    # CHECK/GRANT round trip even though no public byte could be
                    # written between those messages. Validate the exact bytes
                    # now instead; a partial/EAGAIN public write still requires a
                    # fresh CHECK below before another send attempt.
                    if listener.can_defer_read(frame):
                        response = listener.validate_deferred_delivery(session, frame, response)
                    else:
                        listener.validate_owner_frontend_delivery(session)
                    digest = sha256(response).digest()
                send(OwnerReadMessageV1(identity, "GRANT", response, digest))
                while not stopped.is_set():
                    check = receive()
                    if check is None:
                        return
                    if check.identity != identity:
                        raise OwnerProtocolError("owner read delivery identity changed")
                    if check.phase == "RELEASE":
                        break
                    if (
                        check.phase != "CHECK"
                        or check.response_sha256 != digest
                        or monotonic_ns() >= request.deadline_ns
                    ):
                        raise OwnerProtocolError("owner read delivery check invalid/expired")
                    if listener.can_defer_read(frame):
                        current = listener.validate_deferred_delivery(session, frame, response)
                    else:
                        listener.validate_owner_frontend_delivery(session)
                        current = response
                    changed = current != response
                    response, digest = current, sha256(current).digest()
                    send(OwnerReadMessageV1(identity, "GRANT", current if changed else b"", digest))
    except BaseException as error:
        if not stopped.is_set():
            fence(error)
    finally:
        channel.close()
