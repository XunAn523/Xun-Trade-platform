"""Bounded, best-effort Consumer live delivery; signed replay remains authoritative."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Callable

from ..domain import ConsumerAccountBinding
from ..domain._validation import DomainValidationError, require_positive, require_text
from .base import ConsumerEventSubscription, PositionedConsumerEventV2


class ConsumerSubscriptionClosedV2(RuntimeError):
    """The connection must replay its durable cursor instead of retaining memory."""


@dataclass(frozen=True)
class ConsumerLiveSubscriptionV2:
    """A bounded push handle plus a signed-cursor issuer for durable replay.

    The queue is best effort.  ``cursor_for`` is the recovery hint delivered
    with a push event; clients still use ``PollEvents`` as the authority after
    a disconnect, gap, or overflow.
    """

    subscription: ConsumerEventSubscription
    cursor_for: Callable[[PositionedConsumerEventV2], str]

    def drain(self, *, limit: int) -> tuple[PositionedConsumerEventV2, ...]:
        return self.subscription.drain(limit=limit)

    def cursor_for_event(self, item: PositionedConsumerEventV2) -> str:
        return require_text(
            self.cursor_for(item), "consumer live subscription event cursor", max_length=4_096
        )

    def close(self) -> None:
        self.subscription.close()


@dataclass(frozen=True)
class ConsumerSubscriptionLimitsV2:
    max_events: int
    max_bytes: int

    def __post_init__(self) -> None:
        require_positive(self.max_events, "consumer subscription event limit")
        require_positive(self.max_bytes, "consumer subscription byte limit")


@dataclass(frozen=True)
class _QueuedConsumerEventV2:
    item: PositionedConsumerEventV2
    encoded_bytes: int


class BoundedConsumerEventSubscriptionV2(ConsumerEventSubscription):
    """One connection's bounded queue; overflow closes only this subscriber."""

    def __init__(self, *, limits: ConsumerSubscriptionLimitsV2) -> None:
        self._limits = limits
        self._items: deque[_QueuedConsumerEventV2] = deque()
        self._bytes = 0
        self._closed = False
        self._lock = Lock()

    def offer(self, item: PositionedConsumerEventV2, *, encoded_bytes: int) -> bool:
        if encoded_bytes <= 0:
            raise DomainValidationError("consumer subscription encoded event size must be positive")
        with self._lock:
            if self._closed:
                return False
            if (
                len(self._items) >= self._limits.max_events
                or self._bytes + encoded_bytes > self._limits.max_bytes
            ):
                self._closed = True
                self._items.clear()
                self._bytes = 0
                return False
            self._items.append(_QueuedConsumerEventV2(item, encoded_bytes))
            self._bytes += encoded_bytes
            return True

    def drain(self, *, limit: int) -> tuple[PositionedConsumerEventV2, ...]:
        require_positive(limit, "consumer subscription drain limit")
        with self._lock:
            if self._closed:
                raise ConsumerSubscriptionClosedV2("consumer live subscription is closed")
            drained: list[PositionedConsumerEventV2] = []
            while self._items and len(drained) < limit:
                queued = self._items.popleft()
                self._bytes -= queued.encoded_bytes
                drained.append(queued.item)
            return tuple(drained)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._items.clear()
            self._bytes = 0

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed


class ConsumerSubscriptionRegistryV2:
    """Fan out a projector-confirmed event only to matching account subscribers."""

    def __init__(self) -> None:
        self._subscriptions: dict[
            str, tuple[ConsumerAccountBinding, BoundedConsumerEventSubscriptionV2]
        ] = {}
        self._lock = Lock()

    def register(
        self,
        *,
        subscription_id: str,
        binding: ConsumerAccountBinding,
        limits: ConsumerSubscriptionLimitsV2,
    ) -> BoundedConsumerEventSubscriptionV2:
        subscription = BoundedConsumerEventSubscriptionV2(limits=limits)
        with self._lock:
            if subscription_id in self._subscriptions:
                raise DomainValidationError("consumer subscription id is already active")
            self._subscriptions[subscription_id] = (binding, subscription)
        return subscription

    def unregister(self, subscription_id: str) -> None:
        with self._lock:
            record = self._subscriptions.pop(subscription_id, None)
        if record is not None:
            record[1].close()

    def publish(self, item: PositionedConsumerEventV2, *, encoded_bytes: int) -> int:
        event = item.event
        with self._lock:
            records = tuple(self._subscriptions.items())
        rejected: list[str] = []
        delivered = 0
        for subscription_id, (binding, subscription) in records:
            if (
                event.consumer_trace.consumer_id != binding.consumer_id
                or event.consumer_trace.consumer_account_id != binding.consumer_account_id
                or event.account_id != binding.account_id
            ):
                continue
            if subscription.offer(item, encoded_bytes=encoded_bytes):
                delivered += 1
            else:
                rejected.append(subscription_id)
        if rejected:
            with self._lock:
                for subscription_id in rejected:
                    self._subscriptions.pop(subscription_id, None)
        return delivered
