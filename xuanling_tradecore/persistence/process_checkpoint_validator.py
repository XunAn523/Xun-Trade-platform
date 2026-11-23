"""Bounded exact checkpoint recovery verification outside the admission GIL.

One trusted spawned child, one batch in flight, no broker/Journal writer.
Optional FULL SQL commit ownership also lives in this child, under the parent's
cold lease. Parent publishes bloom/MAC receipts only after validated commit.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import sqlite3
import struct
from hashlib import sha256
from pathlib import Path
from threading import Lock
from time import monotonic, monotonic_ns

import msgspec

from ..oms.cold_intent import ColdIntentIndexError, ColdIntentKey
from ..oms.order_book import OmsOrderBookError
from .cold_intent_index import (
    ColdArchiveOutcomeUnknown,
    ColdCommittedBatch,
    commit_cold_rows,
    validate_terminal_checkpoint,
)
from .runtime_files import require_private_runtime_file

_MAX = 130 * 1024**2


class _Request(msgspec.Struct, forbid_unknown_fields=True):
    serial: int
    payloads: tuple[bytes, ...]
    commit: bool = False


class _Response(msgspec.Struct, forbid_unknown_fields=True):
    serial: int
    ok: bool
    keys: tuple[ColdIntentKey, ...]
    digests: tuple[bytes, ...]
    generation: int = 0
    records: int = 0
    validation_ns: int = 0
    sql_commit_ns: int = 0


def _receive(channel: socket.socket, deadline: float | None = None) -> bytes | None:
    def read(size: int) -> bytes:
        body = bytearray()
        while len(body) < size:
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("checkpoint control deadline expired")
                channel.settimeout(remaining)
            block = channel.recv(min(65536, size - len(body)))
            if not block:
                if not body:
                    return b""
                raise ColdIntentIndexError("checkpoint control truncated")
            body.extend(block)
        return bytes(body)

    header = read(4)
    if not header:
        return None
    size = struct.unpack("!I", header)[0]
    if not 0 < size <= _MAX:
        raise ColdIntentIndexError("checkpoint control frame exceeds budget")
    result = read(size)
    if len(result) != size:
        raise ColdIntentIndexError("checkpoint control body truncated")
    return result


def _send(channel: socket.socket, value: _Request | _Response) -> None:
    encoded = msgspec.msgpack.encode(value)
    if len(encoded) > _MAX:
        raise ColdIntentIndexError("checkpoint control frame exceeds budget")
    channel.sendall(struct.pack("!I", len(encoded)) + encoded)


def _verify(
    channel: socket.socket,
    journal_id: str,
    database_path: Path | None,
    budget: int,
    maximum_batch: int,
    cpu: int | None,
) -> None:
    if cpu is not None:
        os.sched_setaffinity(0, {cpu})
    db = None
    try:
        while (message := _receive(channel)) is not None:
            request = msgspec.msgpack.decode(message, type=_Request)
            if not 1 <= len(request.payloads) <= maximum_batch or any(
                not 0 < len(p) <= 256 * 1024 for p in request.payloads
            ):
                raise ColdIntentIndexError("checkpoint verification budget exceeded")
            try:
                started = monotonic_ns()
                keys = tuple(validate_terminal_checkpoint(p, journal_id) for p in request.payloads)
                digests = tuple(sha256(p).digest() for p in request.payloads)
                validated = monotonic_ns()
                generation = records = 0
                if request.commit:
                    if database_path is None:
                        raise RuntimeError("SQL owner was not configured")
                    if db is None:
                        require_private_runtime_file(database_path)
                        db = sqlite3.connect(
                            database_path.as_uri() + "?mode=rw", uri=True, timeout=5
                        )
                        if db.execute("PRAGMA user_version").fetchone() != (1,) or db.execute(
                            "SELECT journal FROM meta WHERE id=1"
                        ).fetchone() != (journal_id,):
                            raise RuntimeError("cold SQL scope changed")
                        if db.execute("PRAGMA journal_mode=DELETE").fetchone() != ("delete",):
                            raise RuntimeError("cold SQL journal mode invalid")
                        db.execute("PRAGMA synchronous=FULL")
                        db.execute("PRAGMA cache_size=-8192")
                        pages = budget // db.execute("PRAGMA page_size").fetchone()[0]
                        if db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] > pages:
                            raise RuntimeError("cold SQL budget exceeded")
                    generation, records = commit_cold_rows(
                        db, tuple(zip(keys, request.payloads, digests, strict=True))
                    )
                committed = monotonic_ns()
            except OmsOrderBookError:
                _send(channel, _Response(request.serial, False, (), ()))
            else:
                _send(
                    channel,
                    _Response(
                        request.serial,
                        True,
                        keys,
                        digests,
                        generation,
                        records,
                        validated - started,
                        committed - validated,
                    ),
                )
    except (OSError, ColdIntentIndexError):
        pass
    finally:
        if db is not None:
            db.close()
        channel.close()


class ProcessCheckpointValidator:
    def __init__(
        self,
        journal_id: str,
        *,
        database_path: Path | None = None,
        maximum_database_bytes: int = 8 * 1024**3,
        maximum_batch: int = 128,
        cpu: int | None = None,
    ) -> None:
        if not journal_id or len(journal_id) > 128:
            raise ColdIntentIndexError("checkpoint validation scope is invalid")
        if (
            database_path is not None
            and not database_path.is_absolute()
            or type(maximum_database_bytes) is not int
            or not 1024**2 <= maximum_database_bytes <= 1024**4
        ):
            raise ColdIntentIndexError("checkpoint SQL owner budget/path invalid")
        if type(maximum_batch) is not int or not 1 <= maximum_batch <= 512:
            raise ColdIntentIndexError("checkpoint batch bound invalid")
        self._maximum_batch = maximum_batch
        if cpu is not None and (type(cpu) is not int or cpu < 0):
            raise ColdIntentIndexError("invalid cold owner CPU")
        self._database_path = database_path
        context = mp.get_context("spawn")
        self._parent, child = socket.socketpair()
        self._lock = Lock()
        self._closed = self._failed = False
        self._serial = 0
        self._process = context.Process(
            target=_verify,
            args=(child, journal_id, database_path, maximum_database_bytes, maximum_batch, cpu),
            name="cold-proof-owner",
        )
        try:
            self._process.start()
        except BaseException:
            self._parent.close()
            raise
        finally:
            child.close()

    def __call__(self, payloads: tuple[bytes, ...]) -> tuple[ColdIntentKey, ...]:
        return self._call(payloads, commit=False).keys

    def commit(self, payloads: tuple[bytes, ...]) -> ColdCommittedBatch:
        if self._database_path is None:
            raise ColdIntentIndexError("checkpoint SQL owner was not configured")
        response = self._call(payloads, commit=True)
        if (
            response.generation < 1
            or response.records < 1
            or response.validation_ns < 0
            or response.sql_commit_ns < 0
        ):
            self._failed = True
            raise ColdArchiveOutcomeUnknown("cold SQL commit response invalid")
        return ColdCommittedBatch(
            response.keys,
            response.digests,
            response.generation,
            response.records,
            response.validation_ns,
            response.sql_commit_ns,
        )

    def _call(self, payloads: tuple[bytes, ...], *, commit: bool) -> _Response:
        if (
            type(payloads) is not tuple
            or not 1 <= len(payloads) <= self._maximum_batch
            or any(type(p) is not bytes or not 0 < len(p) <= 256 * 1024 for p in payloads)
        ):
            raise ColdIntentIndexError("checkpoint verification exceeds bounded batch")
        with self._lock:
            if self._closed or self._failed:
                raise ColdIntentIndexError("checkpoint validator is closed/failed")
            try:
                deadline = monotonic() + 5
                self._serial += 1
                self._parent.settimeout(5)
                _send(self._parent, _Request(self._serial, payloads, commit))
                encoded = _receive(self._parent, deadline)
                if encoded is None:
                    raise ColdIntentIndexError("checkpoint child disappeared")
                response = msgspec.msgpack.decode(encoded, type=_Response)
                if response.serial == self._serial and response.ok is False:
                    raise _RejectedCheckpoint("checkpoint recovery verification rejected")
                if (
                    response.serial != self._serial
                    or not response.ok
                    or len(response.keys) != len(payloads)
                    or response.digests != tuple(sha256(p).digest() for p in payloads)
                ):
                    raise ColdIntentIndexError("checkpoint validation response mismatch")
                return response
            except _RejectedCheckpoint:
                raise ColdIntentIndexError("checkpoint recovery verification rejected") from None
            except BaseException:
                self._failed = True
                if commit:
                    raise ColdArchiveOutcomeUnknown("cold SQL commit outcome unknown") from None
                raise ColdIntentIndexError("checkpoint validator failed; no commit proof") from None

    def close(self) -> None:
        if self._closed:
            return
        if not self._lock.acquire(timeout=6):
            raise ColdIntentIndexError("checkpoint validator caller did not drain")
        forced = False
        try:
            try:
                self._parent.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            self._process.join(3)
            if self._process.is_alive():
                forced = True
                self._process.terminate()
                self._process.join(2)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(2)
            self._parent.close()
            self._closed = not self._process.is_alive()
        finally:
            self._lock.release()
        if forced or self._process.is_alive():
            self._failed = True
            raise ColdIntentIndexError("checkpoint validator required forced drain")


    @property
    def shutdown_verified(self) -> bool:
        return self._closed and not self._failed and self._process.exitcode == 0


class _RejectedCheckpoint(ColdIntentIndexError):
    pass
