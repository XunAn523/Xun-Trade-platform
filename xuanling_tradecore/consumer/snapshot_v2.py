"""Binding-scoped snapshot/reset guard around a Journal-derived projection."""

from __future__ import annotations

from typing import Callable, Protocol

from ..domain import ConsumerAccountBinding
from .base import ConsumerAccountOrderSnapshotV2, ConsumerGatewayError, ConsumerSession
from .projection_health_v2 import (
    ConsumerProjectionHealthTrackerV2,
    ConsumerSnapshotWithHealthV2,
)
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2


class ConsumerSnapshotResetErrorV2(ConsumerGatewayError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ConsumerSnapshotCapacityErrorV2(ConsumerSnapshotResetErrorV2):
    """The legacy single-frame snapshot cannot represent this account safely."""


class ConsumerSnapshotReaderV2(Protocol):
    """Read a consistent account snapshot from the verified active Journal generation."""

    def read_snapshot(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerAccountOrderSnapshotV2: ...


class ConsumerSnapshotProjectionV2:
    """Never let an unverified reset context select another Journal stream."""

    def __init__(
        self,
        *,
        registry_for: Callable[[ConsumerAccountBinding], CursorStreamRegistryV2],
        reader: ConsumerSnapshotReaderV2,
        health_tracker: ConsumerProjectionHealthTrackerV2 | None = None,
    ) -> None:
        self._registry_for, self._reader = registry_for, reader
        self._health_tracker = health_tracker

    def __call__(
        self,
        session: ConsumerSession,
        binding: ConsumerAccountBinding,
        reset_context: str | None,
    ) -> ConsumerAccountOrderSnapshotV2:
        del session
        try:
            registry = self._registry_for(binding)
        except Exception as exc:
            raise ConsumerSnapshotResetErrorV2(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer reset registry is unavailable"
            ) from exc
        if reset_context is not None and not any(
            edge.reset_context == reset_context for edge in registry.resets
        ):
            raise ConsumerSnapshotResetErrorV2(
                "STREAM_RESET_MISMATCH", "consumer reset context cannot be used"
            )
        try:
            snapshot = self._reader.read_snapshot(binding=binding, stream=registry.current)
        except ConsumerSnapshotCapacityErrorV2:
            raise
        except Exception as exc:
            raise ConsumerSnapshotResetErrorV2(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer snapshot is unavailable"
            ) from exc
        cursor = snapshot.next_cursor
        if (
            snapshot.consumer_account_id != binding.consumer_account_id
            or cursor.consumer_id != binding.consumer_id
            or cursor.consumer_account_id != binding.consumer_account_id
            or cursor.journal_id != registry.current.journal_id
            or cursor.stream_generation != registry.current.stream_generation
            or cursor.after_position != snapshot.snapshot_position
        ):
            raise ConsumerSnapshotResetErrorV2(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer snapshot is outside active Journal stream"
            )
        if self._health_tracker is not None:
            self._health_tracker.observe_snapshot(binding=binding, snapshot=snapshot)
        return snapshot

    def with_health(
        self,
        session: ConsumerSession,
        binding: ConsumerAccountBinding,
        reset_context: str | None,
    ) -> ConsumerSnapshotWithHealthV2:
        """Return a compatible snapshot paired with non-authoritative projector health."""

        snapshot = self(session, binding, reset_context)
        if self._health_tracker is None:
            raise ConsumerSnapshotResetErrorV2(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer projector health is unavailable"
            )
        return ConsumerSnapshotWithHealthV2(
            snapshot=snapshot, health=self._health_tracker.health_for(binding=binding)
        )
