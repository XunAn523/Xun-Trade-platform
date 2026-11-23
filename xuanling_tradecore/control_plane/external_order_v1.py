"""M3 read-verified, non-trading attachment for external manual orders."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from threading import RLock
from typing import Protocol

from ..domain import TradingDayEpoch, VenueOrderIdentity
from ..domain._validation import DomainValidationError, require_nonnegative, require_text
from ..oms import OmsFactKind, OmsJournalCommit, OmsJournalFact
from .approval_v1 import (
    ControlActionV1,
    ControlApprovalAuthorizerV1,
    ControlAuthorizationReceiptV1,
    ControlMutationApprovalV1,
)


class ExternalOrderAttachmentErrorV1(DomainValidationError):
    """An external order cannot safely become locally controllable."""


class ExternalOrderHandlingAuthorityV1(str, Enum):
    MANUAL_CONTROL = "MANUAL_CONTROL"
    STRATEGY_CONTROL = "STRATEGY_CONTROL"


@dataclass(frozen=True)
class VerifiedExternalOrderV1:
    account_id: str
    trading_day: TradingDayEpoch
    identity: VenueOrderIdentity
    symbol: str
    side: str
    quantity: int
    observed_monotonic_ns: int
    terminal: bool
    broker_order_digest: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.account_id, "external order account_id"),
            (self.symbol, "external order symbol"),
            (self.side, "external order side"),
        ):
            require_text(value, label, max_length=128)
        if self.quantity <= 0:
            raise ExternalOrderAttachmentErrorV1("external order quantity must be positive")
        require_nonnegative(self.observed_monotonic_ns, "external order observed_monotonic_ns")
        if len(self.broker_order_digest) != 64 or any(
            char not in "0123456789abcdef" for char in self.broker_order_digest
        ):
            raise ExternalOrderAttachmentErrorV1("external order digest is invalid")


class ExternalOrderLookupPortV1(Protocol):
    """Read-only current-broker verification; it has no submit/cancel surface."""

    def lookup_external_order(
        self, *, account_id: str, trading_day: TradingDayEpoch, identity: VenueOrderIdentity
    ) -> VerifiedExternalOrderV1:
        """Return one current external Broker order or raise without side effects."""

        ...


class ExternalOrderAttachmentFactPortV1(Protocol):
    """Durable control-Journal boundary for one immutable attachment fact."""

    def append_external_order_attachment(self, fact: OmsJournalFact) -> OmsJournalCommit:
        """Append exactly one fact without any Broker side effect."""

        ...


@dataclass(frozen=True)
class ExternalOrderAttachmentRequestV1:
    attachment_id: str
    account_id: str
    trading_day: TradingDayEpoch
    identity: VenueOrderIdentity
    expected_broker_order_digest: str
    handling_authority: ExternalOrderHandlingAuthorityV1
    strategy_id: str | None
    approval: ControlMutationApprovalV1

    def __post_init__(self) -> None:
        require_text(self.attachment_id, "external attachment_id", max_length=128)
        require_text(self.account_id, "external attachment account_id", max_length=128)
        if len(self.expected_broker_order_digest) != 64 or any(
            char not in "0123456789abcdef" for char in self.expected_broker_order_digest
        ):
            raise ExternalOrderAttachmentErrorV1("external attachment digest is invalid")
        if self.identity.adapter_client_scope.strip() == "":
            raise ExternalOrderAttachmentErrorV1("external attachment requires native scope")
        if self.handling_authority is ExternalOrderHandlingAuthorityV1.STRATEGY_CONTROL:
            if self.strategy_id is None:
                raise ExternalOrderAttachmentErrorV1("strategy control requires strategy_id")
            require_text(self.strategy_id, "external attachment strategy_id", max_length=128)
        elif self.strategy_id is not None:
            raise ExternalOrderAttachmentErrorV1("manual control cannot bind strategy_id")
        if self.approval.action is not ControlActionV1.ATTACH_EXTERNAL_ORDER:
            raise ExternalOrderAttachmentErrorV1(
                "attachment requires ATTACH_EXTERNAL_ORDER approval"
            )
        if self.approval.account_id != self.account_id:
            raise ExternalOrderAttachmentErrorV1("attachment approval crosses account scope")


@dataclass(frozen=True)
class ExternalOrderAttachmentV1:
    request: ExternalOrderAttachmentRequestV1
    verified_order: VerifiedExternalOrderV1
    authorization: ControlAuthorizationReceiptV1
    journal_commit: OmsJournalCommit


class ExternalOrderAttachmentRegistryV1:
    """In-process duplicate guard; durable replay is defined by the attachment fact."""

    def __init__(self) -> None:
        self._by_venue_id: dict[tuple[str, str], ExternalOrderAttachmentV1] = {}
        self._lock = RLock()

    def contains(self, account_id: str, identity: VenueOrderIdentity) -> bool:
        return (account_id, _identity_key(identity)) in self._by_venue_id

    def record(self, attachment: ExternalOrderAttachmentV1) -> None:
        key = (attachment.request.account_id, _identity_key(attachment.request.identity))
        with self._lock:
            if key in self._by_venue_id:
                raise ExternalOrderAttachmentErrorV1("external order is already attached")
            self._by_venue_id[key] = attachment


class ExternalOrderAttachmentServiceV1:
    """Authorize, read-verify and journal an attachment without calling Broker mutation APIs."""

    def __init__(
        self,
        *,
        authorizer: ControlApprovalAuthorizerV1,
        lookup: ExternalOrderLookupPortV1,
        facts: ExternalOrderAttachmentFactPortV1,
        registry: ExternalOrderAttachmentRegistryV1,
    ) -> None:
        self._authorizer = authorizer
        self._lookup = lookup
        self._facts = facts
        self._registry = registry

    def attach(
        self,
        request: ExternalOrderAttachmentRequestV1,
        *,
        current_control_version: int,
        evaluated_wall_ns: int,
    ) -> ExternalOrderAttachmentV1:
        if self._registry.contains(request.account_id, request.identity):
            raise ExternalOrderAttachmentErrorV1("external order is already attached")
        authorization = self._authorizer.authorize(
            request.approval,
            current_control_version=current_control_version,
            evaluated_wall_ns=evaluated_wall_ns,
        )
        verified = self._lookup.lookup_external_order(
            account_id=request.account_id,
            trading_day=request.trading_day,
            identity=request.identity,
        )
        if (
            verified.account_id != request.account_id
            or verified.trading_day != request.trading_day
            or verified.identity != request.identity
            or verified.terminal
            or verified.broker_order_digest != request.expected_broker_order_digest
        ):
            raise ExternalOrderAttachmentErrorV1("external order query verification failed")
        fact = encode_external_order_attachment_fact(request, verified, authorization)
        commit = self._facts.append_external_order_attachment(fact)
        if commit.fact_count != 1:
            raise ExternalOrderAttachmentErrorV1("attachment Journal receipt is inconsistent")
        attachment = ExternalOrderAttachmentV1(
            request=request,
            verified_order=verified,
            authorization=authorization,
            journal_commit=commit,
        )
        self._registry.record(attachment)
        return attachment


def encode_external_order_attachment_fact(
    request: ExternalOrderAttachmentRequestV1,
    verified: VerifiedExternalOrderV1,
    authorization: ControlAuthorizationReceiptV1,
) -> OmsJournalFact:
    """Encode audit-only local control authority, never a synthetic OMS reservation."""

    return OmsJournalFact(
        fact_id=f"external-order-attachment:{request.attachment_id}",
        kind=OmsFactKind.ORDER_ATTACHED_EXTERNAL,
        account_id=request.account_id,
        oms_order_id=None,
        execution_id=None,
        attributes={
            "authority": request.handling_authority.value,
            "broker_order_digest": verified.broker_order_digest,
            "decision_hash": authorization.decision_hash,
            "native_identity_hash": sha256(
                _identity_key(request.identity).encode("utf-8")
            ).hexdigest(),
            "strategy_id": request.strategy_id or "",
            "trading_day": request.trading_day.value,
        },
    )


def _identity_key(identity: VenueOrderIdentity) -> str:
    return "|".join(
        (
            identity.venue_id,
            identity.adapter_instance_id,
            identity.adapter_client_scope,
            identity.client_order_ref or "",
            identity.venue_order_id or "",
            identity.venue_cancel_id or "",
        )
    )
