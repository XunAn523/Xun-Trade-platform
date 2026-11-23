"""Internal-only M3 control-plane facade with no adapter SDK mutation surface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..domain._validation import require_nonnegative, require_text
from .external_order_v1 import (
    ExternalOrderAttachmentRequestV1,
    ExternalOrderAttachmentServiceV1,
    ExternalOrderAttachmentV1,
)


class ControlPlaneServiceErrorV1(RuntimeError):
    """The internal control plane cannot safely service a request."""


@dataclass(frozen=True)
class ControlAccountStatusV1:
    account_id: str
    lifecycle: str
    reconciliation_status: str
    control_version: int
    gate_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        require_text(self.account_id, "control status account_id", max_length=128)
        require_text(self.lifecycle, "control status lifecycle", max_length=64)
        require_text(
            self.reconciliation_status, "control status reconciliation_status", max_length=64
        )
        require_nonnegative(self.control_version, "control status version")
        object.__setattr__(
            self,
            "gate_codes",
            tuple(
                require_text(code, "control status gate", max_length=64) for code in self.gate_codes
            ),
        )


class ControlAccountStatusPortV1(Protocol):
    """Read-only account state projection, independent of adapter connectivity."""

    def read_account_status(self, account_id: str) -> ControlAccountStatusV1:
        """Return a current restricted status projection."""

        ...


class ControlPlaneServiceV1:
    """Facade intended for a loopback/internal transport adapter, never public SDK access."""

    def __init__(
        self,
        *,
        statuses: ControlAccountStatusPortV1,
        external_attachments: ExternalOrderAttachmentServiceV1,
    ) -> None:
        self._statuses = statuses
        self._external_attachments = external_attachments

    def get_account_status(self, account_id: str) -> ControlAccountStatusV1:
        """Expose only lifecycle, reconciliation and non-sensitive gate state."""

        require_text(account_id, "control status account_id", max_length=128)
        return self._statuses.read_account_status(account_id)

    def attach_external_order(
        self, request: ExternalOrderAttachmentRequestV1, *, evaluated_wall_ns: int
    ) -> ExternalOrderAttachmentV1:
        """Route only the audited read-verified attachment operation."""

        status = self.get_account_status(request.account_id)
        return self._external_attachments.attach(
            request,
            current_control_version=status.control_version,
            evaluated_wall_ns=evaluated_wall_ns,
        )
