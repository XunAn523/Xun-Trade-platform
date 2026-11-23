"""Fail-closed, account-bound signed cursor replay service."""

from __future__ import annotations

from dataclasses import dataclass
from time import time_ns
from typing import Callable, Protocol, runtime_checkable

from ..domain import ConsumerAccountBinding, ConsumerEventCursorV2, ConsumerEventPositionV2
from ..domain._validation import DomainValidationError, require_positive
from .base import ConsumerGatewayError, PositionedConsumerEventV2
from .cursor_v2 import CursorKeyRingV2, CursorTokenError
from .request_trace_v2 import mark_request_stage
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2


class ConsumerCursorReplayError(ConsumerGatewayError):
    """A stable replay failure that never includes business-event payloads."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ConsumerReplaySliceV2:
    items: tuple[PositionedConsumerEventV2, ...]
    high_watermark_position: ConsumerEventPositionV2 | None
    has_more: bool

    def __post_init__(self) -> None:
        items = tuple(self.items)
        # Strict adjacency proves both order and uniqueness in O(n), without
        # sorting/hashing an already ordered page on every PollEvents request.
        if any(left.position >= right.position for left, right in zip(items, items[1:])):
            raise DomainValidationError("replay slice positions must be strictly ordered")
        if self.high_watermark_position is None:
            if items or self.has_more:
                raise DomainValidationError("empty replay watermark cannot contain events")
        elif items and items[-1].position > self.high_watermark_position:
            raise DomainValidationError("replay slice exceeds its watermark")
        if self.has_more and not items:
            raise DomainValidationError("replay slice cannot have_more without an item")
        object.__setattr__(self, "items", items)


@dataclass(frozen=True)
class SignedConsumerEventPageV2:
    items: tuple[PositionedConsumerEventV2, ...]
    high_watermark_position: ConsumerEventPositionV2 | None
    has_more: bool
    next_cursor_token: str


@dataclass(frozen=True)
class ConsumerReplayPartitionBoundsV2:
    after_position: ConsumerEventPositionV2 | None
    high_watermark_position: ConsumerEventPositionV2
    event_count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.high_watermark_position, ConsumerEventPositionV2)  # pyright: ignore[reportUnnecessaryIsInstance]
            or (
                self.after_position is not None
                and (
                    not isinstance(self.after_position, ConsumerEventPositionV2)  # pyright: ignore[reportUnnecessaryIsInstance]
                    or self.after_position >= self.high_watermark_position
                )
            )
            or type(self.event_count) is not int
            or self.event_count <= 0
        ):
            raise DomainValidationError("invalid replay partition bounds")


@dataclass(frozen=True)
class SignedConsumerReplayPartitionV2:
    bounds: ConsumerReplayPartitionBoundsV2
    cursor_token: str


@dataclass(frozen=True)
class SignedConsumerReplayPlanV2:
    stream: CursorStreamRefV2
    partitions: tuple[SignedConsumerReplayPartitionV2, ...]


class ConsumerReplayPartitionReaderV2(Protocol):
    def replay_partition_bounds(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2, partition_count: int
    ) -> tuple[ConsumerReplayPartitionBoundsV2, ...]: ...


class ConsumerEventReaderV2(Protocol):
    def read_events(
        self,
        *,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        after_position: ConsumerEventPositionV2 | None,
        high_watermark_position: ConsumerEventPositionV2 | None,
        limit: int,
    ) -> ConsumerReplaySliceV2: ...


@runtime_checkable
class ConsumerReplaySuffixPartitionReaderV2(Protocol):
    def replay_suffix_partition_bounds(
        self,
        *,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        partition_count: int,
        after_position: ConsumerEventPositionV2,
    ) -> tuple[ConsumerReplayPartitionBoundsV2, ...]: ...


class ConsumerCursorReplayServiceV2:
    def __init__(
        self,
        *,
        key_ring_for: Callable[[ConsumerAccountBinding], CursorKeyRingV2],
        registry_for: Callable[[ConsumerAccountBinding], CursorStreamRegistryV2],
        event_reader: ConsumerEventReaderV2,
        cursor_ttl_ns: int,
        wall_clock_ns: Callable[[], int] = time_ns,
        partition_reader: ConsumerReplayPartitionReaderV2 | None = None,
    ) -> None:
        require_positive(cursor_ttl_ns, "consumer cursor ttl ns")
        self._partition_reader = partition_reader
        self._key_ring_for, self._registry_for = key_ring_for, registry_for
        self._event_reader, self._cursor_ttl_ns, self._wall_clock_ns = (
            event_reader,
            cursor_ttl_ns,
            wall_clock_ns,
        )

    def issue_partitions(
        self,
        *,
        binding: ConsumerAccountBinding,
        authorization_epoch: int,
        partition_count: int,
        after_cursor: str | None = None,
    ) -> SignedConsumerReplayPlanV2:
        """Sign adjacent (after, high] ranges from one reader snapshot.

        No caller-supplied positions or signing material are accepted. A token
        is an independent read capability, not a distributed consumer lease.
        """
        if type(partition_count) is not int or not 1 <= partition_count <= 64:
            raise ConsumerCursorReplayError("CURSOR_INVALID", "invalid replay partition count")
        if self._partition_reader is None:
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "replay partition reader is unavailable"
            )
        stream = self._registry(binding).current
        after: ConsumerEventPositionV2 | None = None
        if after_cursor is not None:
            claims = self.validate_delivery(
                binding=binding, authorization_epoch=authorization_epoch, token=after_cursor
            )
            if (
                claims.high_watermark_position is None
                or claims.after_position != claims.high_watermark_position
                or (claims.journal_id, claims.stream_generation)
                != (stream.journal_id, stream.stream_generation)
            ):
                raise ConsumerCursorReplayError(
                    "CURSOR_INVALID", "suffix requires an exhausted current-stream cursor"
                )
            after = claims.after_position
            if not isinstance(self._partition_reader, ConsumerReplaySuffixPartitionReaderV2):
                raise ConsumerCursorReplayError(
                    "CURSOR_RECOVERY_UNAVAILABLE", "suffix planning is unavailable"
                )
        try:
            if after is not None and isinstance(
                self._partition_reader, ConsumerReplaySuffixPartitionReaderV2
            ):
                bounds = self._partition_reader.replay_suffix_partition_bounds(
                    binding=binding,
                    stream=stream,
                    partition_count=partition_count,
                    after_position=after,
                )
            else:
                bounds = self._partition_reader.replay_partition_bounds(
                    binding=binding, stream=stream, partition_count=partition_count
                )
            if len(bounds) > partition_count:
                raise ValueError("too many partitions")
            previous = after
            for bound in bounds:
                if (
                    bound.after_position != previous
                    or type(bound.event_count) is not int
                    or bound.event_count <= 0
                    or (previous is not None and bound.high_watermark_position <= previous)
                ):
                    raise ValueError("invalid partition coverage")
                previous = bound.high_watermark_position
        except Exception as exc:
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "replay partition snapshot is unavailable"
            ) from exc
        return SignedConsumerReplayPlanV2(
            stream,
            tuple(
                SignedConsumerReplayPartitionV2(
                    bound,
                    self._issue(
                        binding,
                        authorization_epoch,
                        stream,
                        bound.after_position,
                        bound.high_watermark_position,
                    ),
                )
                for bound in bounds
            ),
        )

    def issue_begin_cursor(
        self, *, binding: ConsumerAccountBinding, authorization_epoch: int
    ) -> str:
        registry = self._registry(binding)
        return self._issue(binding, authorization_epoch, registry.current, None, None)

    def issue_cursor(
        self,
        *,
        binding: ConsumerAccountBinding,
        authorization_epoch: int,
        after_position: ConsumerEventPositionV2 | None,
    ) -> str:
        """Issue a fresh cursor after a verified snapshot at the current stream tail."""

        return self._issue(
            binding, authorization_epoch, self._registry(binding).current, after_position, None
        )

    def poll(
        self, *, binding: ConsumerAccountBinding, authorization_epoch: int, token: str, limit: int
    ) -> SignedConsumerEventPageV2:
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ConsumerCursorReplayError("CURSOR_INVALID", "consumer replay limit is invalid")
        claims = self.validate_delivery(
            binding=binding, authorization_epoch=authorization_epoch, token=token
        )
        stream = CursorStreamRefV2(claims.journal_id, claims.stream_generation)
        mark_request_stage("cursor_authorized_ns")
        try:
            page = self._event_reader.read_events(
                binding=binding,
                stream=stream,
                after_position=claims.after_position,
                high_watermark_position=claims.high_watermark_position,
                limit=limit,
            )
            mark_request_stage("replay_read_finished_ns")
        except Exception as exc:
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer replay is unavailable"
            ) from exc
        if (
            claims.high_watermark_position is not None
            and page.high_watermark_position != claims.high_watermark_position
        ):
            raise ConsumerCursorReplayError("CURSOR_INVALID", "consumer replay watermark changed")
        self._validate_page(binding, stream, claims.after_position, page)
        mark_request_stage("replay_validated_ns")
        if not page.items:
            return SignedConsumerEventPageV2(page.items, page.high_watermark_position, False, token)
        next_token = self._issue(
            binding,
            authorization_epoch,
            stream,
            page.items[-1].position,
            page.high_watermark_position,
        )
        mark_request_stage("next_cursor_signed_ns")
        return SignedConsumerEventPageV2(
            page.items, page.high_watermark_position, page.has_more, next_token
        )

    def validate_delivery(
        self, *, binding: ConsumerAccountBinding, authorization_epoch: int, token: str
    ) -> ConsumerEventCursorV2:
        """Memory-only cursor/key/epoch/generation guard; no event reader call."""
        ring = self._ring(binding)
        try:
            claims = ring.verify(token, evaluated_wall_ns=self._wall_clock_ns())
        except CursorTokenError as exc:
            raise ConsumerCursorReplayError(exc.code, "consumer cursor cannot be used") from exc
        if (
            claims.consumer_id != binding.consumer_id
            or claims.consumer_account_id != binding.consumer_account_id
            or claims.authorization_epoch != authorization_epoch
        ):
            raise ConsumerCursorReplayError(
                "CURSOR_INVALID", "consumer cursor binding does not match"
            )
        stream = CursorStreamRefV2(claims.journal_id, claims.stream_generation)
        if stream != self._registry(binding).current:
            raise ConsumerCursorReplayError(
                "JOURNAL_GAP", "consumer cursor is from a prior generation"
            )
        return claims

    def _issue(
        self,
        binding: ConsumerAccountBinding,
        epoch: int,
        stream: CursorStreamRefV2,
        after: ConsumerEventPositionV2 | None,
        watermark: ConsumerEventPositionV2 | None,
    ) -> str:
        now = self._wall_clock_ns()
        return self._ring(binding).issue(
            consumer_id=binding.consumer_id,
            consumer_account_id=binding.consumer_account_id,
            journal_id=stream.journal_id,
            stream_generation=stream.stream_generation,
            after_position=after,
            authorization_epoch=epoch,
            issued_at_wall_ns=now,
            expires_at_wall_ns=now + self._cursor_ttl_ns,
            high_watermark_position=watermark,
        )

    def _ring(self, binding: ConsumerAccountBinding) -> CursorKeyRingV2:
        try:
            return self._key_ring_for(binding)
        except Exception as exc:
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer cursor key is unavailable"
            ) from exc

    def _registry(self, binding: ConsumerAccountBinding) -> CursorStreamRegistryV2:
        try:
            return self._registry_for(binding)
        except Exception as exc:
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer stream registry is unavailable"
            ) from exc

    @staticmethod
    def _validate_page(
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        after: ConsumerEventPositionV2 | None,
        page: ConsumerReplaySliceV2,
    ) -> None:
        if (after is not None and page.items and page.items[0].position <= after) or any(
            item.event.journal_id != stream.journal_id
            or item.event.account_id != binding.account_id
            or item.event.consumer_trace.account_id != binding.account_id
            or item.event.consumer_trace.consumer_id != binding.consumer_id
            or item.event.consumer_trace.consumer_account_id != binding.consumer_account_id
            for item in page.items
        ):
            raise ConsumerCursorReplayError(
                "CURSOR_RECOVERY_UNAVAILABLE", "consumer replay result is not bound to its cursor"
            )
