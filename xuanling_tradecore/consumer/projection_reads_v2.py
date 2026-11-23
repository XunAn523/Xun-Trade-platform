"""Storage-neutral cold read port and reviewed read-lane context.

Only the listener's explicit deferred entry enables this context. A command's
authorization lookup must never open cold storage on the reactor by accident.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator, Protocol

from ..domain import ConsumerAccountBinding, ConsumerAuditTrail, ConsumerOrderEvent
from .base import ConsumerGatewayError

_OPERATION: ContextVar[str | None] = ContextVar("consumer_projection_read", default=None)
PROJECTION_READ_OPERATIONS = frozenset(
    {"GetOrder", "GetOrderAuditTrail", "PollEvents", "CreateReplayPartitions"}
)


class ConsumerProjectionReadErrorV2(ConsumerGatewayError):
    """Retryable read unavailability, never an absence or trading authority."""

    code = "INTERNAL_ERROR"


class ConsumerAuditEvidenceUnavailableV2(ConsumerProjectionReadErrorV2):
    code = "AUDIT_EVIDENCE_UNAVAILABLE"


class ConsumerColdOrderReaderV2(Protocol):
    def get_order(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> ConsumerOrderEvent: ...

    def get_order_audit_trail(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]: ...


def projection_read_operation() -> str | None:
    return _OPERATION.get()


@contextmanager
def projection_read_scope(operation: str) -> Generator[None, None, None]:
    token = _OPERATION.set(operation if operation in PROJECTION_READ_OPERATIONS else None)
    try:
        yield
    finally:
        _OPERATION.reset(token)


def require_projection_read(operation: str) -> None:
    if operation not in PROJECTION_READ_OPERATIONS or _OPERATION.get() != operation:
        raise ConsumerProjectionReadErrorV2("cold read requires its reviewed read lane")
