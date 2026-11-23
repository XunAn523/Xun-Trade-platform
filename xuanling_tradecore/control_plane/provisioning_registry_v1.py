"""In-memory reference registry for immutable Provisioning V1 profile revisions.

The repository protocol is deliberately separate from broker and secret access.
The in-memory implementation is used for contract tests; production adapters
must provide the same atomic/idempotent semantics against the control-plane
database specified in the provisioning migration.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from threading import RLock
from typing import Protocol

from ..domain._validation import require_nonnegative, require_sha256, require_text
from .provisioning_contracts_v1 import (
    AdapterProfileRevisionV1,
    BindingActivationSpecV1,
    ConsumerBindingRevisionV1,
    ProfileStateV1,
    ProvisioningErrorV1,
)


@dataclass(frozen=True)
class ControlRequestLedgerEntryV1:
    """Non-secret result for an idempotent control request."""

    request_id: str
    payload_digest: str
    profile_id: str
    revision: int
    state: ProfileStateV1
    authorization_epoch: int | None = None

    def __post_init__(self) -> None:
        require_text(self.request_id, "control ledger request ID", max_length=128)
        require_sha256(self.payload_digest, "control ledger payload digest")
        require_text(self.profile_id, "control ledger profile ID", max_length=128)
        if self.revision <= 0:
            raise ProvisioningErrorV1("CONTROL_REQUEST", "control ledger revision is invalid")
        if self.authorization_epoch is not None and self.authorization_epoch <= 0:
            raise ProvisioningErrorV1("CONTROL_REQUEST", "control ledger epoch is invalid")


@dataclass(frozen=True)
class ActiveProfileBindingV1:
    """One atomically read, active profile and its exact Consumer binding."""

    profile: AdapterProfileRevisionV1
    binding: ConsumerBindingRevisionV1

    def __post_init__(self) -> None:
        if (
            self.profile.state is not ProfileStateV1.ACTIVE_DISARMED
            or self.binding.profile_id != self.profile.identity.profile_id
            or self.binding.profile_revision != self.profile.revision
            or self.binding.retired_wall_ns is not None
        ):
            raise ProvisioningErrorV1("CONTROL_BINDING", "active profile and binding differ")


class ProvisioningRegistryV1(Protocol):
    """Durable no-secret profile/revision/ledger repository boundary."""

    def get_request(
        self, *, request_id: str, payload_digest: str
    ) -> ControlRequestLedgerEntryV1 | None: ...

    def claim_jti(
        self, *, jti: str, target: str, request_id: str, envelope_digest: str
    ) -> None: ...

    def create_prepared(
        self,
        *,
        request_id: str,
        payload_digest: str,
        profile: AdapterProfileRevisionV1,
    ) -> AdapterProfileRevisionV1: ...

    def get_revision(self, *, profile_id: str, revision: int) -> AdapterProfileRevisionV1: ...

    def active_binding_and_profile(
        self, *, binding_revision_id: str, now_wall_ns: int
    ) -> ActiveProfileBindingV1: ...

    def transition(
        self,
        *,
        request_id: str,
        payload_digest: str,
        profile_id: str,
        revision: int,
        target_state: ProfileStateV1,
        validation_evidence_id: str | None = None,
        approval_id: str | None = None,
        retired_wall_ns: int | None = None,
        authorization_epoch: int | None = None,
    ) -> AdapterProfileRevisionV1: ...

    def publish_binding(
        self,
        *,
        profile: AdapterProfileRevisionV1,
        spec: BindingActivationSpecV1,
        binding_revision_id: str,
        effective_wall_ns: int,
    ) -> ConsumerBindingRevisionV1: ...

    def retire_binding(
        self, *, profile_id: str, revision: int, retired_wall_ns: int
    ) -> tuple[ConsumerBindingRevisionV1, ...]: ...


class InMemoryProvisioningRegistryV1:
    """Thread-safe test registry with immutable revisions and global instance fencing."""

    def __init__(self) -> None:
        self._profiles: dict[str, list[AdapterProfileRevisionV1]] = {}
        self._used_instance_ids: set[str] = set()
        self._requests: dict[str, ControlRequestLedgerEntryV1] = {}
        self._used_jtis: dict[tuple[str, str], tuple[str, str]] = {}
        self._bindings: list[ConsumerBindingRevisionV1] = []
        # ConsumerBindingSnapshotV2 requires a positive bootstrap epoch.  A
        # successful first activation therefore publishes successor epoch 2.
        self._authorization_epoch = 1
        self._lock = RLock()

    def get_request(
        self, *, request_id: str, payload_digest: str
    ) -> ControlRequestLedgerEntryV1 | None:
        require_text(request_id, "control request ID", max_length=128)
        require_sha256(payload_digest, "control payload digest")
        with self._lock:
            entry = self._requests.get(request_id)
            if entry is None:
                return None
            if entry.payload_digest != payload_digest:
                raise ProvisioningErrorV1(
                    "CONTROL_IDEMPOTENCY_CONFLICT", "control request has another payload"
                )
            return entry

    def claim_jti(self, *, jti: str, target: str, request_id: str, envelope_digest: str) -> None:
        """Fence a JWE to one request while allowing its exact crash retry.

        A first attempt claims ``(jti, target)`` before secret storage.  If a
        process dies after that write but before the profile revision is
        persisted, exactly the same request/envelope may resume.  Any other
        request ID or ciphertext is a replay and remains fail-closed.
        """

        require_text(jti, "credential jti", max_length=128)
        require_text(target, "credential target", max_length=256)
        require_text(request_id, "control request ID", max_length=128)
        require_sha256(envelope_digest, "credential envelope digest")
        with self._lock:
            key = (jti, target)
            owner = (request_id, envelope_digest)
            previous = self._used_jtis.get(key)
            if previous is not None and previous != owner:
                raise ProvisioningErrorV1("CONTROL_ENVELOPE_REPLAY", "credential envelope replay")
            self._used_jtis[key] = owner

    def create_prepared(
        self,
        *,
        request_id: str,
        payload_digest: str,
        profile: AdapterProfileRevisionV1,
    ) -> AdapterProfileRevisionV1:
        """Persist a newly prepared revision exactly once after secret storage."""

        if profile.state is not ProfileStateV1.PREPARED:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "new profile is not prepared")
        with self._lock:
            existing = self._requests.get(request_id)
            if existing is not None:
                if existing.payload_digest != payload_digest:
                    raise ProvisioningErrorV1(
                        "CONTROL_IDEMPOTENCY_CONFLICT", "control request has another payload"
                    )
                return self.get_revision(profile_id=existing.profile_id, revision=existing.revision)
            identity = profile.identity
            if identity.adapter_instance_id in self._used_instance_ids:
                raise ProvisioningErrorV1("CONTROL_PROFILE", "adapter instance ID was already used")
            revisions = self._profiles.setdefault(identity.profile_id, [])
            expected_revision = len(revisions) + 1
            if profile.revision != expected_revision:
                raise ProvisioningErrorV1("CONTROL_PROFILE", "profile revision is not monotonic")
            revisions.append(profile)
            self._used_instance_ids.add(identity.adapter_instance_id)
            self._requests[request_id] = ControlRequestLedgerEntryV1(
                request_id=request_id,
                payload_digest=payload_digest,
                profile_id=identity.profile_id,
                revision=profile.revision,
                state=profile.state,
            )
            return profile

    def get_revision(self, *, profile_id: str, revision: int) -> AdapterProfileRevisionV1:
        require_text(profile_id, "profile ID", max_length=128)
        if revision <= 0:
            raise ProvisioningErrorV1("CONTROL_PROFILE", "profile revision is invalid")
        with self._lock:
            revisions = self._profiles.get(profile_id)
            if revisions is None or len(revisions) < revision:
                raise ProvisioningErrorV1("CONTROL_NOT_FOUND", "profile revision was not found")
            return revisions[revision - 1]

    def transition(
        self,
        *,
        request_id: str,
        payload_digest: str,
        profile_id: str,
        revision: int,
        target_state: ProfileStateV1,
        validation_evidence_id: str | None = None,
        approval_id: str | None = None,
        retired_wall_ns: int | None = None,
        authorization_epoch: int | None = None,
    ) -> AdapterProfileRevisionV1:
        """Apply one legal lifecycle edge and durably remember its request result."""

        with self._lock:
            existing = self._requests.get(request_id)
            if existing is not None:
                if existing.payload_digest != payload_digest:
                    raise ProvisioningErrorV1(
                        "CONTROL_IDEMPOTENCY_CONFLICT", "control request has another payload"
                    )
                return self.get_revision(profile_id=existing.profile_id, revision=existing.revision)
            current = self.get_revision(profile_id=profile_id, revision=revision)
            if not _is_legal_transition(current.state, target_state):
                raise ProvisioningErrorV1(
                    "CONTROL_PROFILE_STATE", "profile state transition is not permitted"
                )
            updated = replace(
                current,
                state=target_state,
                validation_evidence_id=validation_evidence_id,
                approval_id=approval_id,
                retired_wall_ns=retired_wall_ns,
            )
            self._profiles[profile_id][revision - 1] = updated
            self._requests[request_id] = ControlRequestLedgerEntryV1(
                request_id=request_id,
                payload_digest=payload_digest,
                profile_id=profile_id,
                revision=revision,
                state=target_state,
                authorization_epoch=authorization_epoch,
            )
            return updated

    def publish_binding(
        self,
        *,
        profile: AdapterProfileRevisionV1,
        spec: BindingActivationSpecV1,
        binding_revision_id: str,
        effective_wall_ns: int,
    ) -> ConsumerBindingRevisionV1:
        """Publish exactly one successor binding and globally advance its epoch."""

        require_text(binding_revision_id, "binding revision ID", max_length=256)
        require_nonnegative(effective_wall_ns, "binding effective time")
        if profile.state is not ProfileStateV1.VALIDATED:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "only validated profile can bind")
        with self._lock:
            existing_by_id = next(
                (
                    item
                    for item in self._bindings
                    if item.binding_revision_id == binding_revision_id
                ),
                None,
            )
            if existing_by_id is not None:
                if (
                    existing_by_id.profile_id == profile.identity.profile_id
                    and existing_by_id.profile_revision == profile.revision
                    and existing_by_id.account_id == spec.account_id
                    and existing_by_id.consumer_id == spec.consumer_id
                    and existing_by_id.consumer_account_id == spec.consumer_account_id
                    and existing_by_id.account_display_name == spec.account_display_name
                    and existing_by_id.strategy_ids == spec.strategy_ids
                    and existing_by_id.retired_wall_ns is None
                ):
                    return existing_by_id
                raise ProvisioningErrorV1("CONTROL_BINDING", "binding revision ID already exists")
            if any(
                item.consumer_id == spec.consumer_id
                and item.consumer_account_id == spec.consumer_account_id
                and item.retired_wall_ns is None
                for item in self._bindings
            ):
                raise ProvisioningErrorV1(
                    "CONTROL_BINDING", "consumer account binding is already active"
                )
            self._authorization_epoch += 1
            binding = ConsumerBindingRevisionV1(
                binding_revision_id=binding_revision_id,
                profile_id=profile.identity.profile_id,
                profile_revision=profile.revision,
                account_id=spec.account_id,
                consumer_id=spec.consumer_id,
                consumer_account_id=spec.consumer_account_id,
                account_display_name=spec.account_display_name,
                strategy_ids=spec.strategy_ids,
                authorization_epoch=self._authorization_epoch,
                effective_wall_ns=effective_wall_ns,
            )
            self._bindings.append(binding)
            return binding

    def retire_binding(
        self,
        *,
        profile_id: str,
        revision: int,
        retired_wall_ns: int,
    ) -> tuple[ConsumerBindingRevisionV1, ...]:
        """Retire only binding revisions owned by the exact profile revision."""

        require_nonnegative(retired_wall_ns, "binding retirement time")
        with self._lock:
            matching = tuple(
                binding
                for binding in self._bindings
                if binding.profile_id == profile_id
                and binding.profile_revision == revision
                and binding.retired_wall_ns is None
            )
            if matching:
                self._authorization_epoch += 1
            retired: list[ConsumerBindingRevisionV1] = []
            new_bindings: list[ConsumerBindingRevisionV1] = []
            for binding in self._bindings:
                if (
                    binding.profile_id == profile_id
                    and binding.profile_revision == revision
                    and binding.retired_wall_ns is None
                ):
                    retired_binding = replace(
                        binding,
                        retired_wall_ns=retired_wall_ns,
                        authorization_epoch=self._authorization_epoch,
                    )
                    retired.append(retired_binding)
                    new_bindings.append(retired_binding)
                else:
                    new_bindings.append(binding)
            self._bindings = new_bindings
            return tuple(retired)

    def active_bindings(self) -> tuple[ConsumerBindingRevisionV1, ...]:
        with self._lock:
            return tuple(item for item in self._bindings if item.retired_wall_ns is None)

    def active_binding_and_profile(
        self, *, binding_revision_id: str, now_wall_ns: int
    ) -> ActiveProfileBindingV1:
        require_text(binding_revision_id, "binding revision ID", max_length=256)
        require_nonnegative(now_wall_ns, "binding evaluation time")
        with self._lock:
            matches = [
                item for item in self._bindings
                if item.binding_revision_id == binding_revision_id
                and item.retired_wall_ns is None
                and item.effective_wall_ns <= now_wall_ns
            ]
            if len(matches) != 1:
                raise ProvisioningErrorV1("CONTROL_BINDING", "active binding is unavailable")
            binding = matches[0]
            revisions = self._profiles.get(binding.profile_id)
            if not revisions or revisions[-1].revision != binding.profile_revision:
                raise ProvisioningErrorV1("CONTROL_BINDING", "binding profile is not current")
            return ActiveProfileBindingV1(revisions[-1], binding)


def _is_legal_transition(source: ProfileStateV1, target: ProfileStateV1) -> bool:
    return (source, target) in {
        (ProfileStateV1.PREPARED, ProfileStateV1.VALIDATED),
        (ProfileStateV1.PREPARED, ProfileStateV1.REJECTED),
        (ProfileStateV1.VALIDATED, ProfileStateV1.ACTIVE_DISARMED),
        (ProfileStateV1.ACTIVE_DISARMED, ProfileStateV1.RETIRING),
        (ProfileStateV1.RETIRING, ProfileStateV1.RETIRED),
    }
