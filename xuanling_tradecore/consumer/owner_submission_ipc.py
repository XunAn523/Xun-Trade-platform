"""Single in-flight inherited WRITE channel, not another listener/ready queue.

The frontend must supply original kernel peer credentials, reserve its ORIGINAL
ready credit and acknowledge client delivery explicitly. This module neither
opens a public endpoint nor substitutes the private socket's uid for that peer.
"""

from __future__ import annotations

import os
import select
import socket
from concurrent.futures import TimeoutError as FutureTimeoutError
from hashlib import sha256
from threading import Event
from time import monotonic_ns, thread_time_ns
from typing import TYPE_CHECKING, Callable, Generic, TypeVar, cast

from ..domain import ConsumerPrincipal
from .base import ConsumerGatewayError
from .cold_admission_v2 import (
    ColdAdmissionRequestV2,
    ColdAdmissionResumeV2,
    ConsumerColdLookupRequiredV2,
    cold_admission_scope,
)
from .fair_queue_v2 import BoundedFairOperationQueueV2
from .owner_cpu_diagnostic import OwnerCpuDiagnostic
from .owner_ipc_v1 import (
    OwnerAuthorityV1,
    OwnerCreditLedgerV1,
    OwnerEventV1,
    OwnerProtocolError,
    OwnerReadyV1,
    OwnerRequestV1,
    receive_owner_packet,
    send_owner_packet,
    send_owner_packet_nonblocking_candidate,
)
from .request_trace_v2 import request_trace_scope
from .uds_server_v2 import ConsumerUdsServerConfigV2
from .unix_v2 import AuthorizedOwnerSubmissionV1, ConsumerUnixSocketError
from .wire_v2 import ConsumerWireError, DecodedFrameV2, MessageKind, decode_frame, encode_frame

if TYPE_CHECKING:
    from .long_running_candidate import LongRunningConsumerCandidate

_OWNER_SUBMISSION_IDLE_POLL_SECONDS = 0.005

ACCOUNT_SCOPE_STRATEGY = "__ACCOUNT_SCOPE__"
_T = TypeVar("_T")


def owner_request_fields(frame: DecodedFrameV2) -> tuple[str, str]:
    """Original business key, and strategy or explicit account-scope marker."""
    payload = frame.payload.get("payload")
    if not isinstance(payload, dict):
        raise OwnerProtocolError("owner request payload missing")
    body = cast(dict[str, object], payload)
    if frame.payload.get("operation") == "SubmitSignal":
        key, strategy = body.get("strategy_order_id"), body.get("strategy_id")
    elif frame.payload.get("operation") == "CancelOrder":
        key, strategy = body.get("oms_order_id"), ACCOUNT_SCOPE_STRATEGY
    else:
        raise OwnerProtocolError("owner WRITE operation required")
    if type(key) is not str or type(strategy) is not str or not key or not strategy:
        raise OwnerProtocolError("owner business routing identity missing")
    return key, strategy


def _cold_response(
    lease: AuthorizedOwnerSubmissionV1,
    frame: DecodedFrameV2,
    request: OwnerRequestV1,
    stopped: Event,
    trace: dict[str, object],
) -> bytes:
    resume = None
    lookups = 0

    def rejected(detail: str) -> bytes:
        trace.update(response_kind="ERROR", failure_stage="cold_admission", failure_code=detail)
        return encode_frame(
            MessageKind.ERROR,
            frame.header.correlation_id,
            {
                "message_type": "error",
                "schema_version": 2,
                "request_id": request.identity.request_id,
                "code": "RATE_LIMITED",
                "retryable": True,
                "detail": detail,
            },
        )

    while True:
        if stopped.is_set() or monotonic_ns() >= request.deadline_ns:
            # Only before a first attempt or while suspended by the canonical
            # strictly-pre-commit exception. Never retry after any other error.
            return rejected("SUBMIT_DEADLINE_OR_STOP_BEFORE_EFFECT")
        try:
            with cold_admission_scope(resume):
                return lease.run()
        except ConsumerColdLookupRequiredV2 as required:
            if lookups >= 3:
                return rejected("COLD_REVALIDATION_REQUIRED")
            remaining = (request.deadline_ns - monotonic_ns()) / 1_000_000_000
            if remaining <= 0:
                return rejected("COLD_DEADLINE")
            trace["cold_lookup_attempts"] = lookups = lookups + 1
            trace["cold_lookup_enqueued_ns"] = monotonic_ns()
            try:
                resolution = required.resolver.resolve(required.key).result(
                    timeout=min(5, remaining)
                )
            except FutureTimeoutError:
                return rejected("COLD_EVIDENCE_TIMEOUT")
            # A resolver/SQL failure other than a bounded lookup timeout is not
            # reclassified by exception text. It fences the owner below.
            if resolution.key != required.key or (
                resolution.checkpoint is not None
                and (
                    type(resolution.checkpoint) is not bytes
                    or len(resolution.checkpoint) > 256 * 1024
                )
            ):
                raise OwnerProtocolError("owner cold lookup scope mismatch")
            trace["cold_lookup_completed_ns"] = monotonic_ns()
            resume = ColdAdmissionResumeV2(
                ColdAdmissionRequestV2(required.key, required.resolver),
                resolution,
            )


def serve_owner_submissions(
    channel: socket.socket,
    *,
    candidate: LongRunningConsumerCandidate,
    original_config: ConsumerUdsServerConfigV2,
    ready: OwnerReadyV1,
    stopped: Event,
    fence: Callable[[], None],
) -> None:
    """One writer thread, no request queue, private reads use the other reactor."""
    last_serial = 0
    cpu_diagnostics = os.environ.get("TRADECORE_DIAGNOSTIC_OWNER_CPU") == "1"
    detail_diagnostics = os.environ.get("TRADECORE_DIAGNOSTIC_OWNER_DETAIL") == "1"
    nonblocking_candidate = os.environ.get("TRADECORE_DIAGNOSTIC_OWNER_SEND_NONBLOCKING") == "1"
    trace_writer = candidate.ipc_trace
    if trace_writer is None:
        fence()
        return
    profile = (
        OwnerCpuDiagnostic(
            original_config.journal_root / "traces" / f"owner-cpu-{ready.generation}.pstats"
        )
        if os.environ.get("TRADECORE_DIAGNOSTIC_OWNER_PROFILE") == "1"
        else None
    )
    try:
        if profile is not None:
            profile.start()
        while not stopped.is_set():
            if not select.select([channel], [], [], _OWNER_SUBMISSION_IDLE_POLL_SECONDS)[0]:
                continue
            incoming = receive_owner_packet(channel, deadline_ns=monotonic_ns() + 5_000_000_000)
            if type(incoming) is not OwnerRequestV1:
                raise OwnerProtocolError("unexpected owner business message")
            request = incoming
            identity = request.identity
            if identity.generation != ready.generation or identity.serial <= last_serial:
                raise OwnerProtocolError("owner business generation/serial mismatch")
            last_serial = identity.serial
            record: dict[str, object] = {
                "scope": "owner_business_ipc",
                "owner_pid": os.getpid(),
                "generation": ready.generation,
                "serial": identity.serial,
                "request_id_sha256": sha256(identity.request_id.encode()).hexdigest(),
                "operation": request.operation,
                "ipc_owner_received_ns": monotonic_ns(),
            }

            def emit(event: OwnerEventV1) -> None:
                deadline = monotonic_ns() + 5_000_000_000
                if nonblocking_candidate:
                    send_owner_packet_nonblocking_candidate(channel, event, deadline_ns=deadline)
                elif detail_diagnostics and event.phase == "COMPLETED":
                    send_owner_packet(channel, event, deadline_ns=deadline, diagnostic=record)
                else:
                    send_owner_packet(channel, event, deadline_ns=deadline)

            try:
                with request_trace_scope(record):
                    frame = decode_frame(request.request_frame)
                    key, strategy = owner_request_fields(frame)
                    authority, peer = identity.authority, identity.peer
                    if (
                        frame.header.message_kind is not MessageKind.REQUEST
                        or frame.payload.get("operation") != request.operation
                        or frame.payload.get("request_id") != identity.request_id
                        or key != identity.business_key
                        or strategy != authority.strategy_id
                        or authority.scope_sha256 != ready.scope_sha256
                        or authority.consumer_id != original_config.consumer_id
                        or authority.account_id != original_config.consumer_account_id
                        or peer is None
                        or (peer.uid, peer.gid)
                        not in {
                            (original_config.peer_uid, original_config.peer_gid),
                            *original_config.additional_peer_credentials,
                        }
                    ):
                        raise OwnerProtocolError("owner original principal/routing mismatch")
                    if (
                        request.operation == "SubmitSignal"
                        and cast(dict[str, object], frame.payload["payload"]).get(
                            "consumer_account_id"
                        )
                        != authority.account_id
                    ):
                        raise OwnerProtocolError("owner payload account mismatch")
                    principal = ConsumerPrincipal(
                        authority.consumer_id,
                        original_config.login_name,
                        original_config.legal_name,
                    )
                    try:
                        context = candidate.owner_submission_listener.authorize_owner_submission(
                            principal,
                            frame,
                            authorization_epoch=authority.authorization_epoch,
                        )
                        lease = context.__enter__()
                    except (ConsumerUnixSocketError, ConsumerWireError, ConsumerGatewayError):
                        emit(
                            OwnerEventV1(identity, "NOT_STARTED", monotonic_ns(), reason="PRECHECK")
                        )
                        record["outcome"] = "not_started_precheck"
                        continue
                    try:
                        observed = monotonic_ns()
                        if stopped.is_set() or observed >= request.deadline_ns:
                            reason = "STOPPED" if stopped.is_set() else "DEADLINE"
                            emit(OwnerEventV1(identity, "NOT_STARTED", observed, reason=reason))
                            record["outcome"] = "not_started_" + reason.lower()
                            continue
                        emit(OwnerEventV1(identity, "STARTED", observed))
                        record["ipc_owner_started_ns"] = observed
                        if cpu_diagnostics or detail_diagnostics:
                            record["submit_actor_wall_started_ns"] = monotonic_ns()
                            record["submit_actor_cpu_started_ns"] = thread_time_ns()
                        try:
                            response = _cold_response(lease, frame, request, stopped, record)
                        finally:
                            if cpu_diagnostics or detail_diagnostics:
                                record["submit_actor_cpu_finished_ns"] = thread_time_ns()
                                record["submit_actor_wall_finished_ns"] = monotonic_ns()
                    finally:
                        if detail_diagnostics:
                            record["context_exit_started_ns"] = monotonic_ns()
                        context.__exit__(None, None, None)
                        if detail_diagnostics:
                            record["context_exit_finished_ns"] = monotonic_ns()
                    if detail_diagnostics:
                        record["journal_bound_started_ns"] = monotonic_ns()
                    head = candidate.runtime.journal.read_bound()
                    if detail_diagnostics:
                        record["journal_bound_finished_ns"] = monotonic_ns()
                        record["response_check_started_ns"] = monotonic_ns()
                    decoded = decode_frame(response)
                    if decoded.payload.get("request_id") != identity.request_id or (
                        decoded.header.correlation_id != frame.header.correlation_id
                    ):
                        raise OwnerProtocolError("owner response correlation mismatch")
                    if detail_diagnostics:
                        record["response_check_finished_ns"] = monotonic_ns()
                        record["completed_emit_started_ns"] = monotonic_ns()
                        record["completed_emit_cpu_started_ns"] = thread_time_ns()
                    emit(
                        OwnerEventV1(
                            identity,
                            "COMPLETED",
                            monotonic_ns(),
                            response,
                            journal_id=head.journal_id,
                            source_lsn=head.committed_lsn,
                            source_prefix_sha256=ready.source_prefix_sha256,
                            source_anchor="RECOVERED_PREFIX",
                        )
                    )
                    if detail_diagnostics:
                        record["completed_emit_cpu_finished_ns"] = thread_time_ns()
                        record["completed_emit_finished_ns"] = monotonic_ns()
                    record.update(outcome="ipc_reply_sent", ipc_owner_reply_sent_ns=monotonic_ns())
            finally:
                # Separate single producer / explicitly different schema from
                # private-read socket traces. No raw IDs, peer names or frames.
                record["ipc_owner_finished_ns"] = monotonic_ns()
                trace_writer.submit(record)
    except BaseException:
        fence()  # possibly committed: no retryable receipt, no automatic new owner
    finally:
        channel.close()
        if profile is not None:
            profile.close()


class OwnerSubmissionPortV1(Generic[_T]):
    """Single-reactor synchronous adapter for the next async integration stage.

    There is one inherited channel and one outstanding delivery. No unbounded
    waiter/worker queue or reconnect. A result continues holding the ORIGINAL
    ready credit until acknowledge_delivery/dispose is explicitly called.
    """

    def __init__(
        self,
        channel: socket.socket,
        *,
        ready: OwnerReadyV1,
        ready_queue: BoundedFairOperationQueueV2[_T],
    ) -> None:
        self._channel, self._ready = channel, ready
        self._ledger = OwnerCreditLedgerV1(
            generation=ready.generation,
            scope_sha256=ready.scope_sha256,
            owner_pid=ready.owner_pid,
            ready_queue=ready_queue,
            maximum_inflight=1,
        )
        self._ledger.accept_ready(ready)
        self.last_timings: dict[str, int] = {}

    @property
    def failed(self) -> bool:
        return self._ledger.failed

    def submit(
        self,
        request: OwnerRequestV1,
        *,
        current_authority: OwnerAuthorityV1,
        timeout: float = 5,
    ) -> OwnerEventV1:
        if type(timeout) not in (int, float) or not 0 < timeout <= 60:
            raise OwnerProtocolError("owner business timeout must be finite")
        if request.identity.authority != current_authority:
            raise OwnerProtocolError("owner dispatch authority changed")
        if self._ledger.outstanding or not self._ledger.reserve(request):
            raise OwnerProtocolError("owner original ready credit unavailable/pending delivery")
        deadline = monotonic_ns() + int(timeout * 1_000_000_000)
        self.last_timings = {"ipc_frontend_send_started_ns": monotonic_ns()}
        try:
            send_owner_packet(self._channel, request, deadline_ns=deadline)
            self.last_timings["ipc_frontend_send_finished_ns"] = monotonic_ns()
            event = receive_owner_packet(self._channel, deadline_ns=deadline)
            if type(event) is not OwnerEventV1:
                raise OwnerProtocolError("owner business reply kind mismatch")
            self._ledger.observe(event)
            if event.phase == "STARTED":
                self.last_timings["ipc_owner_started_ns"] = event.observed_ns
                event = receive_owner_packet(self._channel, deadline_ns=deadline)
                if type(event) is not OwnerEventV1:
                    raise OwnerProtocolError("owner business terminal reply missing")
                self._ledger.observe(event)
            result = self._ledger.result(
                request.identity.serial, current_authority=current_authority
            )
            if result.phase == "COMPLETED":
                response = decode_frame(result.response_frame)
                original = decode_frame(request.request_frame)
                if (
                    response.header.message_kind not in {MessageKind.RESPONSE, MessageKind.ERROR}
                    or response.header.correlation_id != original.header.correlation_id
                    or response.payload.get("request_id") != request.identity.request_id
                ):
                    raise OwnerProtocolError("owner public response association invalid")
                if response.header.message_kind is MessageKind.RESPONSE:
                    body = response.payload.get("payload")
                    if not isinstance(body, dict):
                        raise OwnerProtocolError("owner receipt payload missing")
                    receipt = cast(dict[str, object], body)
                    lsn = receipt.get("journal_lsn")
                    if (
                        type(lsn) is not int
                        or not 0 < lsn <= result.source_lsn
                        or receipt.get("journal_id") != result.journal_id
                        or receipt.get("consumer_request_id") != request.identity.request_id
                    ):
                        raise OwnerProtocolError("owner receipt exceeds published source fence")
            self.last_timings["ipc_frontend_result_received_ns"] = monotonic_ns()
            return result
        except BaseException:
            self.fail()
            raise

    def acknowledge_delivery(self, serial: int, *, current_authority: OwnerAuthorityV1) -> None:
        self._ledger.result(serial, current_authority=current_authority)
        self._ledger.release(serial)

    def dispose(self, serial: int) -> None:
        self._ledger.release(serial)

    def fail(self) -> None:
        self._ledger.fail()
        self._channel.close()

    def require_drained(self) -> None:
        self._ledger.require_drained()

    def stop(self) -> None:
        self._ledger.stop()
        self._ledger.require_drained()
