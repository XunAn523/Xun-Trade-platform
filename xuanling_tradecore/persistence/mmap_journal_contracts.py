"""Storage-neutral route contracts used by the file-backed mmap Journal.

These values deliberately contain the admission and callback identity proof
needed by the EMT path, but no datastore key, stream ID, transport, epoch or
server-side function detail. They are the only route records retained by the
file-backed Python profile.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Mapping

from ..oms import BrokerOrderObservationV1, OmsFactKind, OmsJournalFact, OmsRecoverySeedV1


class JournalIntegrityError(RuntimeError):
    """A local Journal identity or proof is malformed or inconsistent."""


class JournalAdmissionClosedError(RuntimeError):
    """A local Journal route may not start a new native side effect."""


class JournalCapacityError(JournalAdmissionClosedError):
    """A bounded local Journal table or file has reached its capacity."""


@dataclass(frozen=True)
class AccountJournalBinding:
    """One account's immutable local Journal namespace and logical lane."""

    account_id: str
    venue_id: str
    account_key_prefix: str
    shard_id: int

    def __post_init__(self) -> None:
        if not self.account_id.strip() or not self.venue_id.strip():
            raise JournalIntegrityError("Journal binding identity must not be empty")
        expected_prefix = f"xuanling:journal:{self.account_id}:"
        if self.account_key_prefix != expected_prefix:
            raise JournalIntegrityError("Journal binding namespace does not match account_id")
        if self.shard_id < 0:
            raise JournalIntegrityError("Journal binding lane must be non-negative")


@dataclass(frozen=True)
class MmapAdmissionEnvelopeV1:
    """Canonical admission facts retained as a local route proof.

    The frame itself stores only the final command hash.  This immutable
    envelope stays in the bounded process-local route directory so callback
    validation can still prove the original OMS admission facts without a
    network Journal round trip.
    """

    account_id: str
    facts: tuple[OmsJournalFact, ...]
    encoded: bytes

    @classmethod
    def build(
        cls,
        *,
        account_id: str,
        facts: tuple[OmsJournalFact, ...],
        maximum_fact_count: int,
        maximum_encoded_bytes: int,
    ) -> "MmapAdmissionEnvelopeV1":
        if not account_id or not account_id.isascii():
            raise JournalIntegrityError("mmap admission account is invalid")
        if not 1 <= len(facts) <= maximum_fact_count:
            raise JournalIntegrityError("mmap admission fact count is invalid")
        if any(fact.account_id != account_id for fact in facts):
            raise JournalIntegrityError("mmap admission facts belong to another account")
        payload = {
            "schema_version": 1,
            "account_id": account_id,
            "facts": [
                {
                    "fact_id": fact.fact_id,
                    "kind": fact.kind.value,
                    "account_id": fact.account_id,
                    "oms_order_id": fact.oms_order_id,
                    "execution_id": fact.execution_id,
                    "attributes": dict(fact.attributes),
                }
                for fact in facts
            ],
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        if len(encoded) > maximum_encoded_bytes:
            raise JournalIntegrityError("mmap admission envelope exceeds its bound")
        return cls(account_id=account_id, facts=facts, encoded=encoded)


@dataclass(frozen=True)
class PreNativeRouteCommand:
    """Complete local proof authorizing exactly one EMT submit call."""

    account_id: str
    command_id: str
    execution_id: str
    oms_order_id: str
    client_order_ref: str
    route_token_digest: str
    binding_manifest_hash: str
    intent_hash: str
    decision_context_id: str
    native_start_deadline_ns: int
    fact_envelope: MmapAdmissionEnvelopeV1
    recovery_seed: OmsRecoverySeedV1
    _encoded: bytes = field(init=False, repr=False, compare=False)
    _payload_hash: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "account_id"),
            (self.command_id, "command_id"),
            (self.execution_id, "execution_id"),
            (self.oms_order_id, "oms_order_id"),
            (self.client_order_ref, "client_order_ref"),
            (self.decision_context_id, "decision_context_id"),
        ):
            if not value or len(value) > 128 or not value.isascii():
                raise JournalIntegrityError(f"PRE_NATIVE route {name} is invalid")
        for value, name in (
            (self.route_token_digest, "route_token_digest"),
            (self.binding_manifest_hash, "binding_manifest_hash"),
            (self.intent_hash, "intent_hash"),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise JournalIntegrityError(f"PRE_NATIVE route {name} is invalid")
        if self.native_start_deadline_ns <= 0:
            raise JournalIntegrityError("PRE_NATIVE native deadline must be positive")
        if self.fact_envelope.account_id != self.account_id:
            raise JournalIntegrityError("PRE_NATIVE facts belong to another account")
        if tuple(fact.kind for fact in self.fact_envelope.facts) != (
            OmsFactKind.ORDER_ATTRIBUTED,
            OmsFactKind.RESERVATION_CREATED,
            OmsFactKind.CALL_STARTED,
        ):
            raise JournalIntegrityError("PRE_NATIVE requires ordered admission facts")
        recovered = self.recovery_seed.order
        if (
            recovered.order.account_id != self.account_id
            or recovered.order.oms_order_id != self.oms_order_id
            or recovered.execution.execution_id != self.execution_id
            or recovered.order.intent_hash != self.intent_hash
            or recovered.decision_context.decision_context_id != self.decision_context_id
        ):
            raise JournalIntegrityError("PRE_NATIVE recovery seed identity is inconsistent")
        encoded = json.dumps(
            {
                "schema_version": 1,
                "account_id": self.account_id,
                "command_id": self.command_id,
                "execution_id": self.execution_id,
                "oms_order_id": self.oms_order_id,
                "client_order_ref": self.client_order_ref,
                "route_token_digest": self.route_token_digest,
                "binding_manifest_hash": self.binding_manifest_hash,
                "intent_hash": self.intent_hash,
                "decision_context_id": self.decision_context_id,
                "native_start_deadline_ns": str(self.native_start_deadline_ns),
                "admission_facts": json.loads(self.fact_envelope.encoded),
                "recovery_seed_hash": self.recovery_seed.seed_hash,
                "recovery_seed": json.loads(self.recovery_seed.encoded),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        object.__setattr__(self, "_encoded", encoded)
        object.__setattr__(self, "_payload_hash", sha256(encoded).hexdigest())

    @property
    def encoded(self) -> bytes:
        return self._encoded

    @property
    def payload_hash(self) -> str:
        return self._payload_hash


@dataclass(frozen=True)
class CancelRouteCommand:
    """Complete local proof authorizing exactly one EMT cancel call."""

    account_id: str
    cancel_request_id: str
    oms_order_id: str
    execution_id: str
    target_client_order_ref: str
    target_route_payload_hash: str
    venue_order_identity_hash: str
    native_start_deadline_ns: int
    fact_envelope: MmapAdmissionEnvelopeV1

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "account_id"),
            (self.cancel_request_id, "cancel_request_id"),
            (self.oms_order_id, "oms_order_id"),
            (self.execution_id, "execution_id"),
            (self.target_client_order_ref, "target_client_order_ref"),
        ):
            if not value or len(value) > 128 or not value.isascii():
                raise JournalIntegrityError(f"cancel route {name} is invalid")
        for value, name in (
            (self.target_route_payload_hash, "target_route_payload_hash"),
            (self.venue_order_identity_hash, "venue_order_identity_hash"),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise JournalIntegrityError(f"cancel route {name} is invalid")
        if self.native_start_deadline_ns <= 0:
            raise JournalIntegrityError("cancel route native deadline must be positive")
        if self.fact_envelope.account_id != self.account_id or tuple(
            fact.kind for fact in self.fact_envelope.facts
        ) != (OmsFactKind.CANCEL_REQUESTED, OmsFactKind.CANCEL_STARTED):
            raise JournalIntegrityError("cancel route requires ordered start facts")
        if any(
            fact.oms_order_id != self.oms_order_id
            or fact.execution_id != self.execution_id
            or fact.attributes.get("cancel_request_id") != self.cancel_request_id
            for fact in self.fact_envelope.facts
        ):
            raise JournalIntegrityError("cancel route fact identity is inconsistent")

    @property
    def encoded(self) -> bytes:
        return json.dumps(
            {
                "schema_version": 1,
                "account_id": self.account_id,
                "cancel_request_id": self.cancel_request_id,
                "oms_order_id": self.oms_order_id,
                "execution_id": self.execution_id,
                "target_client_order_ref": self.target_client_order_ref,
                "target_route_payload_hash": self.target_route_payload_hash,
                "venue_order_identity_hash": self.venue_order_identity_hash,
                "native_start_deadline_ns": str(self.native_start_deadline_ns),
                "admission_facts": json.loads(self.fact_envelope.encoded),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")

    @property
    def payload_hash(self) -> str:
        return sha256(self.encoded).hexdigest()


@dataclass(frozen=True)
class NativeCallbackOutcomeRequest:
    """One independent mmap callback fact prepared for bounded batching."""

    binding: AccountJournalBinding
    command: PreNativeRouteCommand
    transition_id: str
    observation: BrokerOrderObservationV1
    attributes: Mapping[str, str] | None = None
