"""Fixed, bounded readonly projection owners for the explicit Linux candidate.

Only inherited private parent/child channels use pickle, as with the existing
projector control channel. Neither socket bytes nor database blobs are ever
unpickled. The child runs the original complete store/row validators; transferring
the resulting domain objects avoids doing their construction on the reactor GIL.
No SQL text, arbitrary method, source writer or trading operation is accepted.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import pickle
import select
import struct
from contextlib import closing
from multiprocessing.connection import Connection
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, get_ident
from time import monotonic
from typing import cast

from ..domain import ConsumerOrderEvent
from ..oms.journal_audit import JournalOrderAuditPlan
from ..persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStale,
    OrderProjectionStore,
    ProjectionEventPage,
)
from ..persistence.runtime_files import require_private_runtime_file

_REQUEST_BYTES = 16 * 1024
_REPLY_BYTES = 4 * 1024**2
_METHODS = frozenset({"events_after", "partition_events", "lookup", "order_audit_plan"})
_LENGTH = struct.Struct(">I")


class ProjectionReadSession:
    """One readonly connection on one owner thread; no cached rows/transactions.

    A different inode/schema/generation poisons the session, never silently
    reopens it. A new generation requires an explicitly new read owner.
    """

    def __init__(self, *, root: Path, journal_id: str, profile_hash: str, budget: int):
        self.root, self.journal_id, self.profile_hash, self.budget = (
            root,
            journal_id,
            profile_hash,
            budget,
        )
        self._owner = get_ident()
        self._store: OrderProjectionStore | None = None
        self._identity: tuple[int, ...] | None = None
        self._schema: tuple[object, ...] | None = None
        self._failed = self._closed = False
        self.connection_opens = 0

    def _file_identity(self) -> tuple[int, ...]:
        if (
            any(path.is_symlink() for path in (self.root, *self.root.parents))
            or not self.root.is_dir()
        ):
            raise OrderProjectionError("projection read path is unsafe")
        directory = self.root.stat()
        if os.name == "posix" and (directory.st_uid != os.geteuid() or directory.st_mode & 0o077):
            raise OrderProjectionError("projection read directory permissions changed")
        path = self.root / "orders.sqlite"
        require_private_runtime_file(path)
        file = path.stat()
        if file.st_size > self.budget:
            raise OrderProjectionError("projection read database exceeds budget")
        return directory.st_dev, directory.st_ino, file.st_dev, file.st_ino

    def _schema_identity(self) -> tuple[object, ...]:
        assert self._store is not None
        db = self._store._db  # pyright: ignore[reportPrivateUsage]
        if db.in_transaction:
            raise OrderProjectionError("projection reader retained a transaction")
        cookie = db.execute("PRAGMA schema_version").fetchone()
        version = db.execute("PRAGMA user_version").fetchone()
        if version != (3,) or cookie is None:
            raise OrderProjectionError("projection reader schema changed")
        return *cookie, *version

    def _guard(self) -> None:
        if self._file_identity() != self._identity or self._schema_identity() != self._schema:
            raise OrderProjectionError("projection reader generation or schema changed")

    def call(self, method: str, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
        if get_ident() != self._owner or self._failed or self._closed or method not in _METHODS:
            raise OrderProjectionError("projection read session unavailable or foreign owner")
        try:
            if self._store is None:
                identity = self._file_identity()
                self._store = OrderProjectionStore(
                    self.root,
                    journal_id=self.journal_id,
                    profile_hash=self.profile_hash,
                    read_only=True,
                    maximum_database_bytes=self.budget,
                )
                self.connection_opens += 1
                self._identity = identity
                self._schema = self._schema_identity()
            self._guard()
            try:
                result = getattr(self._store, method)(*args, **kwargs)
            except OrderProjectionStale:
                self._guard()
                raise
            self._guard()
            return result
        except OrderProjectionStale:
            raise
        except BaseException:
            self._failed = True
            self.close()
            raise

    def close(self) -> None:
        if get_ident() != self._owner:
            raise OrderProjectionError("projection session closed outside owner thread")
        if not self._closed:
            self._closed = True
            if self._store is not None:
                self._store.close()


def _send(
    channel: Connection, value: object, maximum: int, *, deadline: float | None = None
) -> None:
    body = pickle.dumps(value, protocol=5)
    if len(body) > maximum:
        raise OrderProjectionError("projection IPC exceeds its fixed capacity")
    deadline = monotonic() + 5 if deadline is None else deadline
    data = memoryview(_LENGTH.pack(len(body)) + body)
    fd = channel.fileno()
    while data:
        remaining = deadline - monotonic()
        if remaining <= 0 or not select.select([], [fd], [], remaining)[1]:
            raise OrderProjectionError("projection IPC send deadline exceeded")
        try:
            size = os.write(fd, data)
        except (BlockingIOError, InterruptedError):
            continue
        if size <= 0:
            raise EOFError("projection IPC closed")
        data = data[size:]


def _receive(channel: Connection, maximum: int, *, deadline: float | None = None) -> object:
    # Trusted spawned sibling only. recv_bytes rejects oversized messages before
    # unpickling; an external request never controls this serialized envelope.
    # All fragments share one deadline. A live child stalled halfway through a
    # reply cannot trap a read worker inside Connection.recv_bytes indefinitely.
    fd = channel.fileno()

    def exact(size: int) -> bytes:
        nonlocal deadline
        result = bytearray()
        while len(result) < size:
            remaining = None if deadline is None else deadline - monotonic()
            if remaining is not None and remaining <= 0:
                raise OrderProjectionError("projection IPC receive deadline exceeded")
            if not select.select([fd], [], [], remaining)[0]:
                raise OrderProjectionError("projection IPC receive deadline exceeded")
            try:
                part = os.read(fd, size - len(result))
            except (BlockingIOError, InterruptedError):
                continue
            if not part:
                raise EOFError("projection IPC closed")
            if deadline is None:
                deadline = monotonic() + 5
            result.extend(part)
        return bytes(result)

    size = _LENGTH.unpack(exact(_LENGTH.size))[0]
    if not 1 <= size <= maximum:
        raise OrderProjectionError("projection IPC receive exceeds fixed capacity")
    return pickle.loads(exact(size))


def _child(
    channel: Connection,
    root: Path,
    journal_id: str,
    profile_hash: str,
    budget: int,
    cpu: int,
    reuse_connections: bool,
) -> None:
    session = None
    try:
        os.sched_setaffinity(0, {cpu})
        os.set_blocking(channel.fileno(), False)
        if reuse_connections:
            session = ProjectionReadSession(
                root=root, journal_id=journal_id, profile_hash=profile_hash, budget=budget
            )
        _send(channel, ("READY", os.getpid()), _REPLY_BYTES)
        while True:
            request = _receive(channel, _REQUEST_BYTES)
            if request == ("STOP",):
                return
            if not isinstance(request, tuple):
                raise OrderProjectionError("invalid private projection command")
            fields = cast(tuple[object, ...], request)
            if len(fields) != 4:
                raise OrderProjectionError("invalid private projection command")
            serial, method, args, kwargs = fields
            if (
                type(serial) is not int
                or serial < 1
                or not isinstance(method, str)
                or method not in _METHODS
                or not isinstance(args, tuple)
                or not isinstance(kwargs, dict)
            ):
                raise OrderProjectionError("invalid private projection command")
            try:
                if session is not None:
                    result = session.call(
                        method, cast(tuple[object, ...], args), cast(dict[str, object], kwargs)
                    )
                else:
                    with closing(
                        OrderProjectionStore(
                            root,
                            journal_id=journal_id,
                            profile_hash=profile_hash,
                            read_only=True,
                            maximum_database_bytes=budget,
                        )
                    ) as store:
                        # Method names come only from the four parent wrappers, not
                        # from a wire operation or caller-supplied SQL expression.
                        result = getattr(store, method)(*args, **kwargs)
                reply = (serial, "OK", result)
            except OrderProjectionStale:
                reply = (serial, "STALE", None)
            except Exception:
                reply = (serial, "UNAVAILABLE", None)
            _send(channel, reply, _REPLY_BYTES)
    finally:
        if session is not None:
            session.close()
        channel.close()


class _Owner:
    def __init__(self, *, args: tuple[object, ...]) -> None:
        context = mp.get_context("spawn")
        self.parent, self.child = context.Pipe()
        os.set_blocking(self.parent.fileno(), False)
        self.process = context.Process(
            target=_child, args=(self.child, *args), name="consumer-projection-read-owner"
        )
        self.serial = 0


class ProcessProjectionReads:
    """At most two private readers and four concurrent waiting read workers.

    Authorization and current Journal/health fences remain parent-owned. Every
    query opens/closes its own readonly connection by default. Explicit reuse
    keeps one validated readonly session per owner, never a long transaction.
    Snapshot/hash/scope/byte fences remain; no transparent retry on failure.
    """

    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        profile_hash: str,
        maximum_database_bytes: int,
        cpus: tuple[int, ...],
        reuse_connections: bool = False,
    ) -> None:
        if (
            not 1 <= len(cpus) <= 2
            or len(set(cpus)) != len(cpus)
            or any(type(cpu) is not int or cpu < 0 for cpu in cpus)
            or type(reuse_connections) is not bool
        ):
            raise OrderProjectionError("invalid fixed projection read owner CPUs")
        self._owners = tuple(
            _Owner(
                args=(
                    root,
                    journal_id,
                    profile_hash,
                    maximum_database_bytes,
                    cpu,
                    reuse_connections,
                )
            )
            for cpu in cpus
        )
        self._idle: Queue[_Owner] = Queue(len(cpus))
        self._lock = Lock()
        self._started = self._closed = self._failed = False
        self._calls = self._inflight = self._peak = 0
        self._reuse_connections = reuse_connections

    def start(self) -> None:
        if self._started or self._closed:
            raise OrderProjectionError("projection read owners cannot restart")
        self._started = True
        try:
            for owner in self._owners:
                owner.process.start()
                owner.child.close()
                if not owner.parent.poll(10):
                    raise OrderProjectionError("projection read owner startup timed out")
                if _receive(owner.parent, _REPLY_BYTES, deadline=monotonic() + 5) != (
                    "READY",
                    owner.process.pid,
                ):
                    raise OrderProjectionError("invalid projection read owner handshake")
                self._idle.put_nowait(owner)
        except BaseException:
            self._failed = True
            self.close()
            raise

    def stats(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "owners": len(self._owners),
                "calls": self._calls,
                "inflight": self._inflight,
                "peak_inflight": self._peak,
                "failed": self._failed,
                "closed": self._closed,
                "reuse_connections": self._reuse_connections,
                "alive": sum(owner.process.is_alive() for owner in self._owners),
            }

    def _call(self, method: str, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
        deadline = monotonic() + 5
        with self._lock:
            if not self._started or self._closed or self._failed or self._inflight >= 4:
                raise OrderProjectionError("projection read owners unavailable/capacity exhausted")
            self._inflight += 1
            self._peak = max(self._peak, self._inflight)
        owner = None
        reusable = False
        try:
            owner = self._idle.get(timeout=max(0, deadline - monotonic()))
            if self._closed or self._failed or not owner.process.is_alive():
                raise OrderProjectionError("projection read owner unavailable")
            owner.serial += 1
            _send(
                owner.parent,
                (owner.serial, method, args, kwargs),
                _REQUEST_BYTES,
                deadline=deadline,
            )
            response = _receive(owner.parent, _REPLY_BYTES, deadline=deadline)
            if not isinstance(response, tuple):
                raise OrderProjectionError("invalid projection read owner response")
            fields = cast(tuple[object, ...], response)
            if (
                len(fields) != 3
                or type(fields[0]) is not int
                or fields[0] != owner.serial
                or fields[1] not in ("OK", "STALE", "UNAVAILABLE")
            ):
                raise OrderProjectionError("invalid projection read owner response")
            reusable = fields[1] in ("OK", "STALE") or (
                fields[1] == "UNAVAILABLE" and not self._reuse_connections
            )
            with self._lock:
                self._calls += 1
            if fields[1] == "STALE":
                raise OrderProjectionStale("projection is behind requested fence")
            if fields[1] != "OK":
                raise OrderProjectionError("projection store validation unavailable")
            return fields[2]
        except (Empty, EOFError, BrokenPipeError, OSError, pickle.PickleError) as error:
            raise OrderProjectionError("projection read control unavailable") from error
        finally:
            with self._lock:
                self._inflight -= 1
                if not reusable:
                    self._failed = True
            if owner is not None and reusable:
                self._idle.put_nowait(owner)

    def events_after(
        self,
        scope: tuple[str, str, str],
        after_lsn: int,
        *,
        limit: int = 100,
        through_lsn: int | None = None,
        minimum_watermark: int = 0,
    ) -> ProjectionEventPage:
        result = self._call(
            "events_after",
            (scope, after_lsn),
            dict(limit=limit, through_lsn=through_lsn, minimum_watermark=minimum_watermark),
        )
        if not isinstance(result, ProjectionEventPage) or len(result.events) > min(limit, 200):
            self._failed = True
            raise OrderProjectionError("invalid projection event reply")
        return result

    def partition_events(
        self,
        scope: tuple[str, str, str],
        *,
        partition_count: int,
        minimum_watermark: int = 0,
        after_lsn: int = 0,
        through_lsn: int | None = None,
    ) -> tuple[tuple[int, int, int], ...]:
        result = self._call(
            "partition_events",
            (scope,),
            dict(
                partition_count=partition_count,
                minimum_watermark=minimum_watermark,
                after_lsn=after_lsn,
                through_lsn=through_lsn,
            ),
        )
        if not isinstance(result, tuple):
            self._failed = True
            raise OrderProjectionError("invalid projection partition reply")
        rows = cast(tuple[object, ...], result)
        if len(rows) > min(partition_count, 64) or any(
            not isinstance(row, tuple)
            or len(cast(tuple[object, ...], row)) != 3
            or any(type(v) is not int for v in cast(tuple[object, ...], row))
            for row in rows
        ):
            self._failed = True
            raise OrderProjectionError("invalid projection partition reply")
        return cast(tuple[tuple[int, int, int], ...], result)

    def lookup(
        self, scope: tuple[str, str, str], order_id: str, *, minimum_watermark: int = 0
    ) -> ConsumerOrderEvent | None:
        result = self._call("lookup", (scope, order_id), dict(minimum_watermark=minimum_watermark))
        if result is not None and not isinstance(result, ConsumerOrderEvent):
            self._failed = True
            raise OrderProjectionError("invalid projection lookup reply")
        return result

    def order_audit_plan(
        self,
        scope: tuple[str, str, str],
        oms_order_id: str,
        *,
        minimum_watermark: int,
        maximum_batches: int = 128,
        maximum_facts: int = 512,
    ) -> JournalOrderAuditPlan | None:
        result = self._call(
            "order_audit_plan",
            (scope, oms_order_id),
            dict(
                minimum_watermark=minimum_watermark,
                maximum_batches=maximum_batches,
                maximum_facts=maximum_facts,
            ),
        )
        if result is not None and not isinstance(result, JournalOrderAuditPlan):
            self._failed = True
            raise OrderProjectionError("invalid projection audit reply")
        return result

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        forced = False
        for owner in self._owners:
            if owner.process.pid is not None and owner.process.is_alive():
                if self._inflight == 0:
                    try:
                        _send(owner.parent, ("STOP",), _REQUEST_BYTES)
                    except OSError:
                        self._failed = True
                owner.process.join(2)
                if owner.process.is_alive():
                    forced = True
                    owner.process.terminate()
                    owner.process.join(2)
                if owner.process.is_alive():
                    owner.process.kill()
                    owner.process.join(2)
            owner.parent.close()
            owner.child.close()
        if forced:
            self._failed = True
            raise OrderProjectionError("projection read owner required forced drain")

    @property
    def shutdown_verified(self) -> bool:
        return (
            self._closed
            and not self._failed
            and all(
                owner.process.pid is not None and owner.process.exitcode == 0
                for owner in self._owners
            )
        )
