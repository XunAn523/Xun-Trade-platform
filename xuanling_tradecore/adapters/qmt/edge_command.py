"""Canonical command records between the Linux adapter and Windows Edge.

The record is deliberately independent from both QMT's embedded Python and
the local binary ring.  It is the only JSON-compatible content permitted in a
mutual-TLS COMMAND frame; the Edge must turn it into a fixed native payload
only after its durable idempotency fence.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Mapping, cast

from ...domain import CancelRequest, RecoveryPlan, VenueOrderIdentity
from ...domain._validation import require_text
from ..base import BrokerQuery
from .bridge_protocol import QmtSubmitCommand
from .bridge_wire import QmtBridgeWireError, canonical_wire_payload


class QmtEdgeCommandError(RuntimeError):
    """A cross-host QMT command is incomplete, noncanonical, or cross-scope."""


class QmtEdgeCommandKind(str, Enum):
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    QUERY = "QUERY"
    RECOVERY_BEGIN = "RECOVERY_BEGIN"


_SCHEMA = "QmtEdgeCommandV1"


@dataclass(frozen=True)
class QmtEdgeCommandV1:
    """One canonical request that Edge can persist before native publication."""

    kind: QmtEdgeCommandKind
    request_id: str
    scope_digest: str
    account_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    fields: Mapping[str, object]

    def __post_init__(self) -> None:
        for name in (
            "request_id",
            "scope_digest",
            "account_id",
            "adapter_instance_id",
            "adapter_client_scope",
        ):
            require_text(getattr(self, name), f"QMT Edge command {name}", max_length=256)
        if len(self.scope_digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.scope_digest.lower()
        ):
            raise QmtEdgeCommandError("QMT Edge command scope digest is not SHA-256")
        try:
            canonical_wire_payload(self.fields)
        except QmtBridgeWireError as error:
            raise QmtEdgeCommandError("QMT Edge command fields are not canonical primitives") from error

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_wire_payload(self.to_mapping())

    @property
    def command_hash(self) -> str:
        return sha256(self.canonical_bytes).hexdigest()

    def to_mapping(self) -> Mapping[str, object]:
        return {
            "schema": _SCHEMA,
            "kind": self.kind.value,
            "request_id": self.request_id,
            "scope_digest": self.scope_digest.lower(),
            "account_id": self.account_id,
            "adapter_instance_id": self.adapter_instance_id,
            "adapter_client_scope": self.adapter_client_scope,
            "fields": dict(self.fields),
        }

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "QmtEdgeCommandV1":
        required = {
            "schema",
            "kind",
            "request_id",
            "scope_digest",
            "account_id",
            "adapter_instance_id",
            "adapter_client_scope",
            "fields",
        }
        if set(raw) != required or raw.get("schema") != _SCHEMA:
            raise QmtEdgeCommandError("QMT Edge command schema is invalid")
        try:
            kind = QmtEdgeCommandKind(require_text(raw["kind"], "QMT Edge command kind"))
        except ValueError as error:
            raise QmtEdgeCommandError("QMT Edge command kind is unsupported") from error
        raw_fields = raw["fields"]
        if not isinstance(raw_fields, Mapping):
            raise QmtEdgeCommandError("QMT Edge command fields are not an object")
        fields = cast(Mapping[object, object], raw_fields)
        if not all(isinstance(key, str) for key in fields):
            raise QmtEdgeCommandError("QMT Edge command field name is invalid")
        return cls(
            kind=kind,
            request_id=require_text(raw["request_id"], "QMT Edge command request_id", max_length=256),
            scope_digest=require_text(
                raw["scope_digest"], "QMT Edge command scope_digest", max_length=64
            ).lower(),
            account_id=require_text(raw["account_id"], "QMT Edge command account_id", max_length=256),
            adapter_instance_id=require_text(
                raw["adapter_instance_id"], "QMT Edge command adapter_instance_id", max_length=256
            ),
            adapter_client_scope=require_text(
                raw["adapter_client_scope"], "QMT Edge command adapter_client_scope", max_length=256
            ),
            fields=cast(Mapping[str, object], fields),
        )

    @classmethod
    def from_submit(cls, command: QmtSubmitCommand, *, scope_digest: str) -> "QmtEdgeCommandV1":
        instruction = command.instruction
        mapped = command.mapped_order
        fields: Mapping[str, object] = {
            "execution_id": instruction.execution_id,
            "oms_order_id": instruction.oms_order_id,
            "trading_day": instruction.trading_day.value,
            "symbol": instruction.symbol,
            "quantity": instruction.quantity,
            "limit_price_ticks": instruction.limit_price_ticks,
            "client_order_ref": command.client_order_ref,
            "mapping_manifest_hash": mapped.manifest_hash,
            "market": mapped.market,
            "side": mapped.side,
            "price_type": mapped.price_type,
            "business_type": mapped.business_type,
        }
        return cls(
            kind=QmtEdgeCommandKind.SUBMIT,
            request_id=instruction.execution_id,
            scope_digest=scope_digest,
            account_id=instruction.account_id,
            adapter_instance_id=command.adapter_instance_id,
            adapter_client_scope=command.adapter_client_scope,
            fields=fields,
        )

    @classmethod
    def from_cancel(
        cls,
        request: CancelRequest,
        identity: VenueOrderIdentity,
        *,
        scope_digest: str,
    ) -> "QmtEdgeCommandV1":
        fields: Mapping[str, object] = {
            "execution_id": request.execution_id,
            "oms_order_id": request.oms_order_id,
            "client_order_ref": identity.client_order_ref,
            "venue_order_id": identity.venue_order_id,
            "venue_cancel_id": identity.venue_cancel_id,
            "reason_digest": sha256(request.reason.encode("utf-8")).hexdigest(),
        }
        return cls(
            kind=QmtEdgeCommandKind.CANCEL,
            request_id=request.cancel_request_id,
            scope_digest=scope_digest,
            account_id=request.account_id,
            adapter_instance_id=identity.adapter_instance_id,
            adapter_client_scope=identity.adapter_client_scope,
            fields=fields,
        )

    @classmethod
    def from_query(cls, request: BrokerQuery, *, scope_digest: str, adapter_client_scope: str) -> "QmtEdgeCommandV1":
        return cls(
            kind=QmtEdgeCommandKind.QUERY,
            request_id=request.request_id,
            scope_digest=scope_digest,
            account_id=request.account_id,
            adapter_instance_id=request.adapter_instance_id,
            adapter_client_scope=adapter_client_scope,
            fields={
                "query_kind": request.query_kind,
                "page_size": request.page_size,
                "continuation": request.continuation,
                "capture_authoritative_order_rows": request.capture_authoritative_order_rows,
            },
        )

    @classmethod
    def from_recovery(
        cls, plan: RecoveryPlan, *, scope_digest: str, adapter_client_scope: str
    ) -> "QmtEdgeCommandV1":
        return cls(
            kind=QmtEdgeCommandKind.RECOVERY_BEGIN,
            request_id=plan.recovery_run_id,
            scope_digest=scope_digest,
            account_id=plan.account_id,
            adapter_instance_id=plan.adapter_instance_id,
            adapter_client_scope=adapter_client_scope,
            fields={
                "trading_day": plan.trading_day.value,
                "journal_id_digest": sha256(plan.journal_id.encode("utf-8")).hexdigest(),
                "expected_journal_lsn": plan.expected_journal_lsn,
            },
        )
