"""Fail-closed, non-trading M3 Journal stream-reset workflow."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Callable, Protocol

from ..consumer.control_root_v2 import AccountCursorControlRootV2
from ..consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamResetV2
from ..domain._validation import DomainValidationError, require_text
from ..oms import OmsFactKind, OmsJournalCommit, OmsJournalFact


class StreamResetApprovalErrorV1(DomainValidationError):
    """A reset request lacks the independent approvals required by M3."""


class StreamResetExecutionErrorV1(RuntimeError):
    """A reset sequence is incomplete and the account must remain DISARMED."""


class JournalGenerationReadinessPortV1(Protocol):
    """Proves that a newly created Journal generation is readable before publication."""

    def require_readable_generation(self, *, account_id: str, stream: CursorStreamRefV2) -> None:
        """Raise unless the exact target generation is independently readable."""


class AccountStreamResetFactPortV1(Protocol):
    """Append the reset fact through a DISARMED-only continuation Journal path."""

    def append_account_stream_reset(self, fact: OmsJournalFact) -> OmsJournalCommit:
        """Commit exactly one target-generation ``ACCOUNT_STREAM_RESET`` fact."""

        ...


@dataclass(frozen=True)
class ApprovedStreamResetV1:
    """Validated intent only; persistence and generation creation remain separate."""

    account_id: str
    maker_id: str
    checker_id: str
    approval_reference: str
    reset_context: str
    source: CursorStreamRefV2
    target: CursorStreamRefV2

    def __post_init__(self) -> None:
        for value, label in (
            (self.account_id, "reset account_id"),
            (self.maker_id, "reset maker_id"),
            (self.checker_id, "reset checker_id"),
            (self.approval_reference, "reset approval_reference"),
            (self.reset_context, "reset_context"),
        ):
            require_text(value, label, max_length=128)
        if self.maker_id == self.checker_id:
            raise StreamResetApprovalErrorV1("reset maker and checker must differ")
        if self.source == self.target:
            raise StreamResetApprovalErrorV1("reset target must be a new generation")

    def as_registry_edge(self) -> CursorStreamResetV2:
        """Return the immutable edge for durable control-root persistence."""

        return CursorStreamResetV2(
            reset_context=self.reset_context,
            source=self.source,
            target=self.target,
        )

    @property
    def decision_hash(self) -> str:
        """Return the stable hash referenced by the durable reset fact."""

        return sha256(
            json.dumps(
                {
                    "schema_version": 1,
                    "account_id": self.account_id,
                    "maker_id": self.maker_id,
                    "checker_id": self.checker_id,
                    "approval_reference": self.approval_reference,
                    "reset_context": self.reset_context,
                    "source": _stream_payload(self.source),
                    "target": _stream_payload(self.target),
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
        ).hexdigest()


def persist_approved_stream_reset(
    *, root: AccountCursorControlRootV2, request: ApprovedStreamResetV1
) -> None:
    """Durably append one approved edge only from the current registry tail."""

    registry = root.read_stream_registry()
    if request.source != registry.current:
        raise StreamResetApprovalErrorV1("reset source is not the current stream generation")
    if request.target in registry.streams:
        raise StreamResetApprovalErrorV1("reset target generation already exists")
    root.write_stream_registry(
        type(registry)(
            current=request.target,
            streams=(*registry.streams, request.target),
            resets=(*registry.resets, request.as_registry_edge()),
        )
    )


@dataclass(frozen=True)
class AccountStreamResetCommitV1:
    """The only successful result of the ordered M3 reset publication sequence."""

    request: ApprovedStreamResetV1
    journal_commit: OmsJournalCommit


class AccountStreamResetJournalV1:
    """Publish a reset only after the new generation and control edge are durable.

    This object deliberately has no ARM or lifecycle transition capability.  If
    any step fails, its caller receives a classified failure after the supplied
    fail-closed hook has been invoked; recovery must reconcile the durable
    registry edge before any later attempt can expose the target stream.
    """

    def __init__(
        self,
        *,
        root: AccountCursorControlRootV2,
        reset_fact_port: AccountStreamResetFactPortV1,
        readiness: JournalGenerationReadinessPortV1,
        fail_closed: Callable[[str], None],
    ) -> None:
        self._root = root
        self._reset_fact_port = reset_fact_port
        self._readiness = readiness
        self._fail_closed = fail_closed

    def persist(self, request: ApprovedStreamResetV1) -> AccountStreamResetCommitV1:
        """Execute ``readable target -> durable edge -> durable reset fact``.

        The final fact is accepted only when its receipt belongs to the target
        Journal generation and contains exactly the one reset fact.  The caller
        must treat an exception as ``DISARMED + RECONCILIATION``; this method
        intentionally cannot compensate by removing an already-durable edge.
        """

        try:
            self._readiness.require_readable_generation(
                account_id=request.account_id, stream=request.target
            )
            persist_approved_stream_reset(root=self._root, request=request)
            commit = self._reset_fact_port.append_account_stream_reset(
                encode_account_stream_reset_fact(request)
            )
            if commit.journal_id != request.target.journal_id or commit.fact_count != 1:
                raise StreamResetExecutionErrorV1("reset Journal receipt is inconsistent")
            return AccountStreamResetCommitV1(request=request, journal_commit=commit)
        except Exception as error:
            reason = f"account-stream-reset:{type(error).__name__}"
            try:
                self._fail_closed(reason)
            except Exception as hook_error:
                raise StreamResetExecutionErrorV1(
                    "reset failure could not be fail-closed"
                ) from hook_error
            if isinstance(error, StreamResetExecutionErrorV1):
                raise
            raise StreamResetExecutionErrorV1("account stream reset failed closed") from error


def encode_account_stream_reset_fact(request: ApprovedStreamResetV1) -> OmsJournalFact:
    """Encode the target-Journal evidence for one approved stream reset."""

    return OmsJournalFact(
        fact_id=f"account-stream-reset:{request.decision_hash}",
        kind=OmsFactKind.ACCOUNT_STREAM_RESET,
        account_id=request.account_id,
        oms_order_id=None,
        execution_id=None,
        attributes={
            "schema_version": "1",
            "approval_reference": request.approval_reference,
            "decision_hash": request.decision_hash,
            "reset_context_hash": sha256(request.reset_context.encode("utf-8")).hexdigest(),
            "source_journal_id": request.source.journal_id,
            "source_stream_generation": str(request.source.stream_generation),
            "target_journal_id": request.target.journal_id,
            "target_stream_generation": str(request.target.stream_generation),
        },
    )


def _stream_payload(stream: CursorStreamRefV2) -> dict[str, object]:
    return {"journal_id": stream.journal_id, "stream_generation": stream.stream_generation}
