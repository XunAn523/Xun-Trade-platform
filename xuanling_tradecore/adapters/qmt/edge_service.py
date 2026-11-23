"""Bounded mTLS listener: session and SQLite work belongs to one owner thread."""

from __future__ import annotations

import socket
import ssl
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Full, Queue
from typing import Callable, Protocol, TypeVar, cast

from .bridge_wire import (
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    decode_canonical_wire_payload,
    receive_wire_frame,
    send_wire_frame,
)
from .edge_controller import QmtEdgeCommandController
from .edge_runtime import QmtEdgeRuntime
from .edge_wire_endpoint import QmtEdgeHealthProjection, QmtEdgeWireEndpoint
from .mtls_channel import (
    QmtMtlsServerConfig,
    create_qmt_mtls_server_context,
    verify_qmt_mtls_client,
)


class QmtEdgeServiceError(RuntimeError):
    """Listener, actor, or session ownership failed."""


class QmtServiceOwner(Protocol):
    """Created, called and closed only in the storage actor."""

    def open_session(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame: ...
    def handle(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame: ...
    def next_event(self, session: int) -> QmtBridgeWireFrame: ...
    def close_session(self, session: int) -> None: ...
    def close(self) -> None: ...


class QmtCommandServiceOwner:
    """Legacy command composition. Raw ring events remain unavailable on the wire."""

    def __init__(self, runtime: QmtEdgeRuntime) -> None:
        self.runtime = runtime
        self.sessions: dict[int, QmtEdgeWireEndpoint] = {}

    def open_session(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        if self.sessions:
            raise QmtEdgeServiceError("QMT command owner already connected")
        snapshot = self.runtime.snapshot()
        ready = snapshot.mode == "ARMED" and snapshot.mutation_allowed
        endpoint = QmtEdgeWireEndpoint(
            scope_digest=self.runtime.config.scope_digest.hex(),
            command_acceptor=QmtEdgeCommandController(self.runtime),
            health=QmtEdgeHealthProjection(ready, "READY" if ready else "DISARMED"),
        )
        response = endpoint.handle(frame, hello_complete=False)
        self.sessions[session] = endpoint
        return response

    def handle(self, session: int, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        return self.sessions[session].handle(frame, hello_complete=True)

    def next_event(self, session: int) -> QmtBridgeWireFrame:
        raise QmtEdgeServiceError("QMT native event projector is not configured")

    def close_session(self, session: int) -> None:
        self.sessions.pop(session, None)

    def close(self) -> None:
        self.runtime.close()


_T = TypeVar("_T")


class _OwnerActor:
    def __init__(self, factory: Callable[[], QmtServiceOwner]) -> None:
        self.queue: Queue[tuple[Callable[[QmtServiceOwner], object], Future[object]]] = Queue(16)
        self.stopping = threading.Event()
        self.started: Future[None] = Future()
        self.thread = threading.Thread(target=self._run, args=(factory,), name="qmt-edge-writer")
        self.thread.start()
        try:
            self.started.result(timeout=30)
        except BaseException:
            self.stopping.set()
            self.thread.join(timeout=35)
            raise

    def call(self, action: Callable[[QmtServiceOwner], _T]) -> _T:
        if self.stopping.is_set() or not self.thread.is_alive():
            raise QmtEdgeServiceError("QMT storage owner is stopped")
        future: Future[object] = Future()
        try:
            self.queue.put_nowait((action, future))
        except Full as error:
            raise QmtEdgeServiceError("QMT storage queue is full") from error
        # Never retry an ambiguous timeout: the original operation may finish.
        return cast(_T, future.result(timeout=30))

    def close(self) -> None:
        self.stopping.set()
        self.thread.join(timeout=35)
        if self.thread.is_alive():
            raise QmtEdgeServiceError("QMT storage owner did not stop")

    def _run(self, factory: Callable[[], QmtServiceOwner]) -> None:
        try:
            owner = factory()
        except BaseException as error:
            self.started.set_exception(error)
            return
        self.started.set_result(None)
        try:
            while not self.stopping.is_set() or not self.queue.empty():
                try:
                    action, future = self.queue.get(timeout=0.1)
                except Empty:
                    continue
                try:
                    future.set_result(action(owner))
                except BaseException as error:
                    future.set_exception(error)
        finally:
            owner.close()


@dataclass(frozen=True)
class QmtEdgeServiceSnapshot:
    listening: bool
    accepted_connections: int
    rejected_connections: int
    active_connections: int = 0


class QmtEdgeService:
    """Two roles on one port, four peers maximum, bounded I/O and one writer.

    Pass a factory, never an already-open SQLite runtime. Stop interrupts and
    joins socket workers before closing storage. No background native caller.
    """

    def __init__(
        self,
        *,
        owner_factory: Callable[[], QmtServiceOwner],
        tls: QmtMtlsServerConfig,
        allowed_peer_host: str | None = None,
        io_timeout_s: float = 5.0,
    ) -> None:
        if io_timeout_s <= 0:
            raise QmtEdgeServiceError("QMT I/O timeout must be positive")
        self._factory, self._tls = owner_factory, tls
        self._allowed_peer_host, self._timeout = allowed_peer_host, io_timeout_s
        self._listener: socket.socket | None = None
        self._actor: _OwnerActor | None = None
        self._accept_thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._peers: dict[int, socket.socket] = {}
        self._threads: set[threading.Thread] = set()
        self._accepted_connections = self._rejected_connections = self._sequence = 0

    def start(self) -> None:
        if self._actor is not None or self._stopping.is_set():
            raise QmtEdgeServiceError("QMT service cannot be started twice")
        context = create_qmt_mtls_server_context(self._tls)
        actor = _OwnerActor(self._factory)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            exclusive = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
            if isinstance(exclusive, int):
                listener.setsockopt(socket.SOL_SOCKET, exclusive, 1)
            else:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self._tls.host, self._tls.port))
            listener.listen(4)
            listener.settimeout(0.2)
        except BaseException:
            listener.close()
            actor.close()
            raise
        self._listener, self._actor = listener, actor
        self._accept_thread = threading.Thread(target=self._accept_loop, args=(context,))
        self._accept_thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._listener is not None:
            self._listener.close()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
        with self._lock:
            peers, threads = list(self._peers.values()), list(self._threads)
        for peer in peers:
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            peer.close()
        for thread in threads:
            thread.join(timeout=35)
            if thread.is_alive():
                raise QmtEdgeServiceError("QMT peer did not stop; storage left open")
        if self._actor is not None:
            self._actor.close()
        self._actor = None
        self._listener = None

    def snapshot(self) -> QmtEdgeServiceSnapshot:
        with self._lock:
            return QmtEdgeServiceSnapshot(
                self._listener is not None and not self._stopping.is_set(),
                self._accepted_connections,
                self._rejected_connections,
                len(self._peers),
            )

    def _accept_loop(self, context: ssl.SSLContext) -> None:
        while not self._stopping.is_set():
            try:
                assert self._listener is not None
                raw, address = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with self._lock:
                if len(self._peers) >= 4 or (
                    self._allowed_peer_host is not None and address[0] != self._allowed_peer_host
                ):
                    raw.close()
                    self._rejected_connections += 1
                    continue
                self._sequence += 1
                session = self._sequence
                self._peers[session] = raw
                thread = threading.Thread(target=self._serve, args=(context, raw, session))
                self._threads.add(thread)
                thread.start()

    def _serve(self, context: ssl.SSLContext, raw: socket.socket, session: int) -> None:
        actor = self._actor
        assert actor is not None
        connection: socket.socket = raw
        try:
            raw.settimeout(self._timeout)
            secured = context.wrap_socket(raw, server_side=True, do_handshake_on_connect=False)
            connection = secured
            with self._lock:
                self._peers[session] = secured
            if self._stopping.is_set():
                return
            secured.do_handshake()
            verify_qmt_mtls_client(secured, self._tls)
            hello = receive_wire_frame(secured)
            response = actor.call(lambda owner: owner.open_session(session, hello))
            role = decode_canonical_wire_payload(hello.payload)["role"]
            send_wire_frame(secured, response)
            with self._lock:
                self._accepted_connections += 1
            while not self._stopping.is_set():
                if role == "EVENT":
                    frame = actor.call(lambda owner: owner.next_event(session))
                    send_wire_frame(secured, frame)
                    if frame.kind is QmtBridgeWireKind.HEALTH:
                        self._stopping.wait(0.25)
                        continue
                frame = receive_wire_frame(secured)
                response = actor.call(lambda owner: owner.handle(session, frame))
                send_wire_frame(secured, response)
        except Exception:
            # Storage/protocol failures close this session, without leaking
            # private payloads in an unhandled connection-thread traceback.
            with self._lock:
                self._rejected_connections += 1
        finally:
            connection.close()
            try:
                actor.call(lambda owner: owner.close_session(session))
            finally:
                with self._lock:
                    self._peers.pop(session, None)
                    self._threads.discard(threading.current_thread())
