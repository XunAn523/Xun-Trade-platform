"""Independent maker-checker approval records for Provisioning V1 actions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Protocol

from ..domain._validation import require_nonnegative, require_positive, require_text
from .provisioning_contracts_v1 import ControlPrincipalV1, ControlRoleV1, ProvisioningErrorV1


class ProvisioningApprovalActionV1(str, Enum):
    VALIDATE = "VALIDATE"
    ACTIVATE = "ACTIVATE"
    RETIRE = "RETIRE"


@dataclass(frozen=True)
class ProvisioningApprovalV1:
    """A no-secret approval bound to one profile revision and one action."""

    approval_id: str
    action: ProvisioningApprovalActionV1
    profile_id: str
    revision: int
    approver: ControlPrincipalV1
    issued_wall_ns: int
    expires_wall_ns: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.approval_id, "approval ID"),
            (self.profile_id, "approval profile ID"),
        ):
            require_text(value, label, max_length=256)
        require_positive(self.revision, "approval profile revision")
        require_nonnegative(self.issued_wall_ns, "approval issue time")
        require_positive(self.expires_wall_ns, "approval expiry time")
        if self.expires_wall_ns <= self.issued_wall_ns:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval interval is invalid")
        self.approver.require_role(ControlRoleV1.APPROVER)


class ProvisioningApprovalRegistryV1(Protocol):
    """Durable maker-checker approval repository boundary."""

    def record(self, approval: ProvisioningApprovalV1) -> None:
        """Persist one immutable approval fact or reject an ID conflict."""

        ...

    def require(
        self,
        *,
        approval_id: str,
        action: ProvisioningApprovalActionV1,
        profile_id: str,
        revision: int,
        evaluated_wall_ns: int,
    ) -> ControlPrincipalV1:
        """Return the approving principal only for an active exact approval."""

        ...


class InMemoryProvisioningApprovalRegistryV1:
    """Test/reference approval store; production stores equivalent signed facts."""

    def __init__(self) -> None:
        self._approvals: dict[str, ProvisioningApprovalV1] = {}
        self._lock = RLock()

    def record(self, approval: ProvisioningApprovalV1) -> None:
        with self._lock:
            existing = self._approvals.get(approval.approval_id)
            if existing is not None and existing != approval:
                raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval ID conflicts")
            self._approvals[approval.approval_id] = approval

    def require(
        self,
        *,
        approval_id: str,
        action: ProvisioningApprovalActionV1,
        profile_id: str,
        revision: int,
        evaluated_wall_ns: int,
    ) -> ControlPrincipalV1:
        require_text(approval_id, "approval ID", max_length=256)
        require_nonnegative(evaluated_wall_ns, "approval evaluation time")
        with self._lock:
            approval = self._approvals.get(approval_id)
        if approval is None:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval was not found")
        if (
            approval.action is not action
            or approval.profile_id != profile_id
            or approval.revision != revision
            or not approval.issued_wall_ns <= evaluated_wall_ns < approval.expires_wall_ns
        ):
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval is not valid for action")
        return approval.approver
