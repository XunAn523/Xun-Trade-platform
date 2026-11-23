"""Fail-closed M3 maker-checker authorization and replay protection."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from threading import RLock
from typing import Mapping

from ..domain._validation import (
    DomainValidationError,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)


class ControlApprovalErrorV1(DomainValidationError):
    """A high-risk control mutation has no current, independent authorization."""


class ControlActionV1(str, Enum):
    ATTACH_EXTERNAL_ORDER = "ATTACH_EXTERNAL_ORDER"
    STREAM_RESET = "STREAM_RESET"
    ARM_ACCOUNT = "ARM_ACCOUNT"
    RESET_KILL_SWITCH = "RESET_KILL_SWITCH"
    BLOCK_NEW = "BLOCK_NEW"


@dataclass(frozen=True)
class ControlApprovalPolicyV1:
    """A signed, versioned local projection of an OPA policy bundle decision domain."""

    policy_id: str
    policy_epoch: int
    policy_hash: str
    allowed_roles_by_action: Mapping[ControlActionV1, frozenset[str]]

    def __post_init__(self) -> None:
        require_text(self.policy_id, "control policy_id", max_length=128)
        require_positive(self.policy_epoch, "control policy_epoch")
        require_sha256(self.policy_hash, "control policy_hash")
        if set(self.allowed_roles_by_action) != set(ControlActionV1):
            raise ControlApprovalErrorV1("control policy must cover every supported action")
        normalized: dict[ControlActionV1, frozenset[str]] = {}
        for action, roles in self.allowed_roles_by_action.items():
            frozen = frozenset(require_text(role, "control role", max_length=64) for role in roles)
            if not frozen:
                raise ControlApprovalErrorV1("control policy action has no permitted role")
            normalized[action] = frozen
        object.__setattr__(self, "allowed_roles_by_action", normalized)


@dataclass(frozen=True)
class ControlMutationApprovalV1:
    """The identity, time, nonce and optimistic precondition for one control mutation."""

    command_id: str
    action: ControlActionV1
    account_id: str
    maker_id: str
    maker_roles: frozenset[str]
    checker_id: str | None
    checker_roles: frozenset[str]
    reason_code: str
    nonce: str
    issued_wall_ns: int
    expires_wall_ns: int
    expected_control_version: int
    policy_epoch: int
    policy_hash: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.command_id, "control command_id"),
            (self.account_id, "control account_id"),
            (self.maker_id, "control maker_id"),
            (self.reason_code, "control reason_code"),
            (self.nonce, "control nonce"),
        ):
            require_text(value, label, max_length=128)
        maker_roles = frozenset(
            require_text(role, "control maker role", max_length=64) for role in self.maker_roles
        )
        checker_roles = frozenset(
            require_text(role, "control checker role", max_length=64) for role in self.checker_roles
        )
        if not maker_roles:
            raise ControlApprovalErrorV1("control mutation requires maker roles")
        if self.action is ControlActionV1.BLOCK_NEW:
            if self.checker_id is not None or checker_roles:
                raise ControlApprovalErrorV1("emergency BLOCK_NEW must not carry a checker")
        else:
            if self.checker_id is None:
                raise ControlApprovalErrorV1("high-risk control mutation requires a checker")
            require_text(self.checker_id, "control checker_id", max_length=128)
            if self.checker_id == self.maker_id or not checker_roles:
                raise ControlApprovalErrorV1("control maker and checker must be independent")
        require_nonnegative(self.issued_wall_ns, "control approval issued_wall_ns")
        require_positive(self.expires_wall_ns, "control approval expires_wall_ns")
        if self.expires_wall_ns <= self.issued_wall_ns:
            raise ControlApprovalErrorV1("control approval lifetime is empty")
        require_nonnegative(self.expected_control_version, "control expected_control_version")
        require_positive(self.policy_epoch, "control approval policy_epoch")
        require_sha256(self.policy_hash, "control approval policy_hash")
        object.__setattr__(self, "maker_roles", maker_roles)
        object.__setattr__(self, "checker_roles", checker_roles)

    @property
    def decision_hash(self) -> str:
        payload = "|".join(
            (
                self.command_id,
                self.action.value,
                self.account_id,
                self.maker_id,
                ",".join(sorted(self.maker_roles)),
                self.checker_id or "",
                ",".join(sorted(self.checker_roles)),
                self.reason_code,
                self.nonce,
                str(self.issued_wall_ns),
                str(self.expires_wall_ns),
                str(self.expected_control_version),
                str(self.policy_epoch),
                self.policy_hash,
            )
        )
        return sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ControlAuthorizationReceiptV1:
    approval: ControlMutationApprovalV1
    policy_id: str
    decision_hash: str


class ControlApprovalAuthorizerV1:
    """Evaluate local OPA-result inputs and permanently consume accepted nonces."""

    def __init__(self, policy: ControlApprovalPolicyV1) -> None:
        self._policy = policy
        self._used_nonces: set[tuple[str, str]] = set()
        self._lock = RLock()

    def authorize(
        self,
        approval: ControlMutationApprovalV1,
        *,
        current_control_version: int,
        evaluated_wall_ns: int,
    ) -> ControlAuthorizationReceiptV1:
        """Validate then consume nonce before any mutation-capable port is reached."""

        require_nonnegative(current_control_version, "current control version")
        require_nonnegative(evaluated_wall_ns, "control evaluation time")
        if (
            approval.policy_epoch != self._policy.policy_epoch
            or approval.policy_hash != self._policy.policy_hash
        ):
            raise ControlApprovalErrorV1("control policy epoch or hash is stale")
        if not approval.issued_wall_ns <= evaluated_wall_ns < approval.expires_wall_ns:
            raise ControlApprovalErrorV1("control approval is expired or not yet valid")
        if approval.expected_control_version != current_control_version:
            raise ControlApprovalErrorV1("control optimistic precondition does not match")
        required_roles = self._policy.allowed_roles_by_action[approval.action]
        if not approval.maker_roles.intersection(required_roles):
            raise ControlApprovalErrorV1("control maker is not authorized for action")
        if (
            approval.action is not ControlActionV1.BLOCK_NEW
            and not approval.checker_roles.intersection(required_roles)
        ):
            raise ControlApprovalErrorV1("control checker is not authorized for action")
        nonce_key = (approval.account_id, approval.nonce)
        with self._lock:
            if nonce_key in self._used_nonces:
                raise ControlApprovalErrorV1("control approval nonce was already consumed")
            self._used_nonces.add(nonce_key)
        return ControlAuthorizationReceiptV1(
            approval=approval,
            policy_id=self._policy.policy_id,
            decision_hash=approval.decision_hash,
        )
