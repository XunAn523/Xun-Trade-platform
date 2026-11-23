"""Binding-scoped projector watermark and stale-state tracking for Consumer V2."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from ..domain import ConsumerAccountBinding, ConsumerEventPositionV2
from ..domain._validation import require_text
from .base import ConsumerAccountOrderSnapshotV2, PositionedConsumerEventV2


class ConsumerProjectionHealthErrorV2(RuntimeError):
    """A projector observation crosses binding or ordering boundaries."""


@dataclass(frozen=True)
class ConsumerProjectionHealthV2:
    """Payload-free health for one binding's snapshot projector."""

    binding_id: str
    binding_generation: int
    source_watermark: ConsumerEventPositionV2 | None
    stale: bool
    reason_code: str | None

    def __post_init__(self) -> None:
        require_text(self.binding_id, "consumer projector health binding ID", max_length=256)
        if self.binding_generation <= 0:
            raise ConsumerProjectionHealthErrorV2("consumer projector generation is invalid")
        if self.stale != (self.reason_code is not None):
            raise ConsumerProjectionHealthErrorV2(
                "consumer projector stale state and reason are inconsistent"
            )
        if self.reason_code is not None:
            require_text(self.reason_code, "consumer projector stale reason", max_length=128)


@dataclass(frozen=True)
class ConsumerSnapshotWithHealthV2:
    """A snapshot paired with the watermark/stale evidence needed by Consumer."""

    snapshot: ConsumerAccountOrderSnapshotV2
    health: ConsumerProjectionHealthV2

    def __post_init__(self) -> None:
        if self.snapshot.next_cursor.after_position != self.snapshot.snapshot_position:
            raise ConsumerProjectionHealthErrorV2("consumer snapshot restart point is inconsistent")
        if self.health.source_watermark is not None and (
            self.health.source_watermark < self.snapshot.snapshot_position
        ):
            raise ConsumerProjectionHealthErrorV2(
                "consumer projector watermark precedes its returned snapshot"
            )


class ConsumerProjectionHealthTrackerV2:
    """Track monotonic per-binding event watermarks outside the execution hot path."""

    def __init__(self) -> None:
        self._health: dict[str, ConsumerProjectionHealthV2] = {}
        self._lock = Lock()

    def observe_event(
        self, *, binding: ConsumerAccountBinding, item: PositionedConsumerEventV2
    ) -> ConsumerProjectionHealthV2:
        """Advance one binding watermark or fail closed on an out-of-scope item."""

        event = item.event
        if (
            event.consumer_trace.consumer_id != binding.consumer_id
            or event.consumer_trace.consumer_account_id != binding.consumer_account_id
            or event.account_id != binding.account_id
        ):
            raise ConsumerProjectionHealthErrorV2("consumer projector event is outside its binding")
        with self._lock:
            current = self._health.get(binding.binding_id)
            self._require_generation(current, binding)
            if current is not None and current.stale:
                return current
            if (
                current is not None
                and current.source_watermark is not None
                and item.position <= current.source_watermark
            ):
                raise ConsumerProjectionHealthErrorV2(
                    "consumer projector event position is not strictly monotonic"
                )
            updated = ConsumerProjectionHealthV2(
                binding_id=binding.binding_id,
                binding_generation=binding.binding_version,
                source_watermark=item.position,
                stale=False,
                reason_code=None,
            )
            self._health[binding.binding_id] = updated
            return updated

    def observe_snapshot(
        self, *, binding: ConsumerAccountBinding, snapshot: ConsumerAccountOrderSnapshotV2
    ) -> ConsumerProjectionHealthV2:
        """Advance to a consistent snapshot restart point without accepting a rewind."""

        if snapshot.consumer_account_id != binding.consumer_account_id:
            raise ConsumerProjectionHealthErrorV2("consumer snapshot is outside its binding")
        with self._lock:
            current = self._health.get(binding.binding_id)
            self._require_generation(current, binding)
            if current is not None and current.stale:
                return current
            if (
                current is not None
                and current.source_watermark is not None
                and snapshot.snapshot_position < current.source_watermark
            ):
                raise ConsumerProjectionHealthErrorV2(
                    "consumer snapshot watermark moved backwards"
                )
            updated = ConsumerProjectionHealthV2(
                binding_id=binding.binding_id,
                binding_generation=binding.binding_version,
                source_watermark=snapshot.snapshot_position,
                stale=False,
                reason_code=None,
            )
            self._health[binding.binding_id] = updated
            return updated

    def mark_stale(
        self, *, binding: ConsumerAccountBinding, reason_code: str
    ) -> ConsumerProjectionHealthV2:
        """Latch a binding stale; recovery must supply a successor fact/snapshot."""

        reason = require_text(reason_code, "consumer projector stale reason", max_length=128)
        with self._lock:
            current = self._health.get(binding.binding_id)
            self._require_generation(current, binding)
            stale = ConsumerProjectionHealthV2(
                binding_id=binding.binding_id,
                binding_generation=binding.binding_version,
                source_watermark=(current.source_watermark if current is not None else None),
                stale=True,
                reason_code=reason,
            )
            self._health[binding.binding_id] = stale
            return stale

    def health_for(self, *, binding: ConsumerAccountBinding) -> ConsumerProjectionHealthV2:
        with self._lock:
            current = self._health.get(binding.binding_id)
            self._require_generation(current, binding)
            if current is not None:
                return current
            return ConsumerProjectionHealthV2(
                binding_id=binding.binding_id,
                binding_generation=binding.binding_version,
                source_watermark=None,
                stale=True,
                reason_code="PROJECTOR_WATERMARK_UNAVAILABLE",
            )

    @staticmethod
    def _require_generation(
        current: ConsumerProjectionHealthV2 | None, binding: ConsumerAccountBinding
    ) -> None:
        if current is not None and current.binding_generation != binding.binding_version:
            raise ConsumerProjectionHealthErrorV2(
                "consumer projector health belongs to another binding generation"
            )
