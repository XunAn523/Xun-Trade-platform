"""Windows Edge implementation of the QMT fixed-size SPSC ring.

The class uses a pre-opened file-backed mapping for deterministic offline tests
and P0 fallback.  Named mappings are intentionally probe-only until P0 records
the Windows DACL and Interlocked evidence; an activation cannot claim that a
bare ``mmap`` tag name is a production-secure mapping.
"""

from __future__ import annotations

import ctypes
import mmap
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from time import monotonic_ns
from typing import Callable, cast

from .native_engine_contract import (
    RING_COMMIT_OFFSET,
    RING_HEADER_SIZE,
    RING_SLOT_SIZE,
    QmtNativeContractError,
    QmtNativeFrameV1,
    QmtRingHeaderV1,
)


class QmtNativeRingFull(QmtNativeContractError):
    """A producer attempted to exceed the bounded native command capacity."""


class QmtNativeRingEmpty(QmtNativeContractError):
    """A consumer attempted to read a ring without a committed frame."""


@dataclass(frozen=True)
class QmtNativeRingSnapshot:
    """Payload-free ring counters for health and backpressure projection."""

    producer_sequence: int
    consumer_sequence: int
    capacity: int
    producer_heartbeat_ns: int
    consumer_heartbeat_ns: int

    @property
    def depth(self) -> int:
        return self.producer_sequence - self.consumer_sequence


class QmtNativeAtomicLibrary:
    """The audited Windows C helper that exposes 64-bit Interlocked operations."""

    _load: Callable[..., int]
    _store: Callable[..., int]
    _aligned: Callable[..., int]

    def __init__(self, library_path: Path) -> None:
        if os.name != "nt":
            raise QmtNativeContractError("QMT native atomics require Windows")
        try:
            library = ctypes.WinDLL(str(library_path))
            load = library.tradecore_qmt_load_u64
            store = library.tradecore_qmt_store_u64
            aligned = library.tradecore_qmt_is_aligned_u64
        except (AttributeError, OSError) as error:
            raise QmtNativeContractError("QMT native atomic DLL is unavailable") from error
        load.argtypes = (ctypes.POINTER(ctypes.c_longlong),)
        load.restype = ctypes.c_longlong
        store.argtypes = (ctypes.POINTER(ctypes.c_longlong), ctypes.c_longlong)
        store.restype = ctypes.c_longlong
        aligned.argtypes = (ctypes.POINTER(ctypes.c_longlong),)
        aligned.restype = ctypes.c_int
        self._load = cast(Callable[..., int], load)
        self._store = cast(Callable[..., int], store)
        self._aligned = cast(Callable[..., int], aligned)

    def require_aligned(self, pointer: ctypes.c_longlong) -> None:
        if not self.is_aligned(pointer):
            raise QmtNativeContractError("QMT ring atomic pointer is not 8-byte aligned")

    def is_aligned(self, pointer: ctypes.c_longlong) -> bool:
        return ctypes.addressof(pointer) % 8 == 0 and bool(self._aligned(ctypes.byref(pointer)))

    def load(self, pointer: ctypes.c_longlong) -> int:
        self.require_aligned(pointer)
        return int(self._load(ctypes.byref(pointer)))

    def store(self, pointer: ctypes.c_longlong, value: int) -> None:
        self.require_aligned(pointer)
        if value < 0 or value > (1 << 63) - 1:
            raise QmtNativeContractError("QMT ring atomic value is outside signed 64-bit range")
        self._store(ctypes.byref(pointer), value)


class _AtomicU64:
    """Release/acquire-like counter access using Windows Interlocked on P0 hosts."""

    def __init__(
        self,
        mapping: mmap.mmap,
        offset: int,
        *,
        native_library: QmtNativeAtomicLibrary | None,
        test_only_unsafe_fallback: bool,
    ) -> None:
        self._mapping = mapping
        self._offset = offset
        self._fallback_lock = threading.Lock()
        self._native_library = native_library
        if native_library is None and not test_only_unsafe_fallback:
            raise QmtNativeContractError(
                "QMT native ring refuses production startup without the atomic DLL"
            )
        pointer = ctypes.c_longlong.from_buffer(mapping, offset)
        try:
            if ctypes.addressof(pointer) % 8:
                raise QmtNativeContractError("QMT ring atomic pointer is not 8-byte aligned")
            if native_library is not None and not native_library.is_aligned(pointer):
                raise QmtNativeContractError("QMT ring atomic DLL rejected pointer alignment")
        except BaseException:
            del pointer
            raise
        self._pointer = pointer

    def release_buffer(self) -> None:
        del self._pointer

    def load(self) -> int:
        if self._native_library is not None:
            return self._native_library.load(self._pointer)
        with self._fallback_lock:
            return int(self._pointer.value)

    def store(self, value: int) -> None:
        if value < 0:
            raise QmtNativeContractError("QMT ring atomic value must be nonnegative")
        if self._native_library is not None:
            self._native_library.store(self._pointer, value)
            return
        with self._fallback_lock:
            self._pointer.value = value


class QmtNativeRing:
    """One unidirectional, fixed-size, bounded SPSC mapping."""

    _PRODUCER_SEQUENCE_OFFSET = 72
    _CONSUMER_SEQUENCE_OFFSET = 80
    _PRODUCER_HEARTBEAT_OFFSET = 88
    _CONSUMER_HEARTBEAT_OFFSET = 96

    def __init__(
        self,
        mapping: mmap.mmap,
        header: QmtRingHeaderV1,
        *,
        native_library: QmtNativeAtomicLibrary | None,
        test_only_unsafe_fallback: bool,
    ) -> None:
        self._mapping = mapping
        self._header = header
        self._native_library = native_library
        self._test_only_unsafe_fallback = test_only_unsafe_fallback
        try:
            self._producer_sequence = self._atomic(self._PRODUCER_SEQUENCE_OFFSET)
            self._consumer_sequence = self._atomic(self._CONSUMER_SEQUENCE_OFFSET)
            self._producer_heartbeat = self._atomic(self._PRODUCER_HEARTBEAT_OFFSET)
            self._consumer_heartbeat = self._atomic(self._CONSUMER_HEARTBEAT_OFFSET)
        except BaseException:
            for atomic in tuple(self.__dict__.values()):
                if isinstance(atomic, _AtomicU64):
                    atomic.release_buffer()
            raise

    @property
    def generation(self) -> bytes:
        return self._header.generation

    @classmethod
    def create_file(
        cls,
        path: Path,
        header: QmtRingHeaderV1,
        *,
        native_library: QmtNativeAtomicLibrary | None = None,
        test_only_unsafe_fallback: bool = False,
    ) -> "QmtNativeRing":
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x+b") as file:
            file.truncate(header.mapping_size)
            mapping = mmap.mmap(file.fileno(), header.mapping_size, access=mmap.ACCESS_WRITE)
        mapping[:RING_HEADER_SIZE] = header.pack()
        mapping.flush()
        try:
            return cls(
                mapping,
                header,
                native_library=native_library,
                test_only_unsafe_fallback=test_only_unsafe_fallback,
            )
        except BaseException:
            mapping.close()
            raise

    @classmethod
    def open_file(
        cls,
        path: Path,
        *,
        expected_scope_digest: bytes,
        expected_generation: bytes,
        native_library: QmtNativeAtomicLibrary | None = None,
        test_only_unsafe_fallback: bool = False,
    ) -> "QmtNativeRing":
        with path.open("r+b") as file:
            mapping = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
        try:
            header = QmtRingHeaderV1.unpack(mapping[:RING_HEADER_SIZE])
            if header.mapping_size != mapping.size():
                raise QmtNativeContractError("QMT ring mapping size does not match header")
            if (
                header.scope_digest != expected_scope_digest
                or header.generation != expected_generation
            ):
                raise QmtNativeContractError("QMT ring scope or generation does not match")
            return cls(
                mapping,
                header,
                native_library=native_library,
                test_only_unsafe_fallback=test_only_unsafe_fallback,
            )
        except BaseException:
            mapping.close()
            raise

    @classmethod
    def probe_named_mapping(cls, *, name: str, mapping_size: int) -> bool:
        """Perform a non-production P0 named-mapping availability probe."""

        if os.name != "nt" or not name.startswith("Local\\") or mapping_size <= RING_HEADER_SIZE:
            return False
        try:
            mapping = mmap.mmap(-1, mapping_size, tagname=name, access=mmap.ACCESS_WRITE)
        except OSError:
            return False
        try:
            sentinel = b"TC"
            mapping[: len(sentinel)] = sentinel
            return mapping[: len(sentinel)] == sentinel
        finally:
            mapping.close()

    def publish(self, frame: QmtNativeFrameV1) -> int:
        producer_sequence = self._producer_sequence.load()
        consumer_sequence = self._consumer_sequence.load()
        if (
            producer_sequence < consumer_sequence
            or producer_sequence - consumer_sequence >= self._header.capacity
        ):
            raise QmtNativeRingFull("QMT native ring is full or its cursor is corrupt")
        sequence = producer_sequence + 1
        if frame.sequence != sequence:
            raise QmtNativeContractError("QMT native frame sequence does not match producer cursor")
        offset = self._slot_offset(sequence)
        raw = frame.encode_uncommitted()
        self._mapping[offset : offset + RING_SLOT_SIZE] = raw
        self._atomic(offset + RING_COMMIT_OFFSET).store(sequence)
        self._producer_sequence.store(sequence)
        self._producer_heartbeat.store(monotonic_ns())
        return sequence

    def consume(self) -> QmtNativeFrameV1:
        frame = self.peek()
        self.release_consumed(frame.sequence)
        return frame

    def peek(self) -> QmtNativeFrameV1:
        """Copy a committed frame without releasing it before durable storage."""

        consumer_sequence = self._consumer_sequence.load()
        producer_sequence = self._producer_sequence.load()
        if (
            producer_sequence < consumer_sequence
            or producer_sequence - consumer_sequence > self._header.capacity
        ):
            raise QmtNativeContractError("QMT event ring cursor is corrupt")
        if producer_sequence == consumer_sequence:
            raise QmtNativeRingEmpty("QMT native ring has no committed frame")
        sequence = consumer_sequence + 1
        offset = self._slot_offset(sequence)
        raw = bytes(self._mapping[offset : offset + RING_SLOT_SIZE])
        return QmtNativeFrameV1.decode_committed(raw, expected_sequence=sequence)

    def release_consumed(self, sequence: int) -> None:
        """Release only after the caller has durably handled this exact frame."""

        if (
            sequence != self._consumer_sequence.load() + 1
            or sequence > self._producer_sequence.load()
        ):
            raise QmtNativeContractError("QMT ring release sequence does not match")
        # Do not clear the slot: after consumer publication the producer may
        # immediately reuse it. Old commits are fenced by the expected sequence.
        self._consumer_sequence.store(sequence)
        self._consumer_heartbeat.store(monotonic_ns())

    def snapshot(self) -> QmtNativeRingSnapshot:
        return QmtNativeRingSnapshot(
            producer_sequence=self._producer_sequence.load(),
            consumer_sequence=self._consumer_sequence.load(),
            capacity=self._header.capacity,
            producer_heartbeat_ns=self._producer_heartbeat.load(),
            consumer_heartbeat_ns=self._consumer_heartbeat.load(),
        )

    def close(self) -> None:
        for attribute in (
            "_producer_sequence",
            "_consumer_sequence",
            "_producer_heartbeat",
            "_consumer_heartbeat",
        ):
            atomic = getattr(self, attribute)
            atomic.release_buffer()
        self._mapping.close()

    def _slot_offset(self, sequence: int) -> int:
        return RING_HEADER_SIZE + ((sequence - 1) % self._header.capacity) * RING_SLOT_SIZE

    def _atomic(self, offset: int) -> _AtomicU64:
        return _AtomicU64(
            self._mapping,
            offset,
            native_library=self._native_library,
            test_only_unsafe_fallback=self._test_only_unsafe_fallback,
        )
