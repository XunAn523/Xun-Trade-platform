"""Explicit, scope-pinned EMT simulator-only admission, not recovery proof.

The host bootstrap must obtain the permit from protected operator-controlled
configuration. Digests pin that decision; they cannot independently prove
that a broker endpoint is a simulator. Never construct permits from requests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable

from xuanling_tradecore.adapters import ActivatedVenueMapping
from xuanling_tradecore.consumer.base import ConsumerAccountView
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import ProfileStateV1
from xuanling_tradecore.domain import (
    AccountLifecycle,
    ConsumerAccountBinding,
    ReconciliationStatus,
    RiskSnapshot,
)
from xuanling_tradecore.domain._validation import require_positive, require_sha256, require_text
from xuanling_tradecore.ems import canonical_broker_capabilities_hash

from .consumer_runtime_control_v1 import (
    ConsumerEmtRuntimeControlSnapshotV1,
    LinuxRootOwnedNamespaceFileV1,
)


class EmtSimulatorGateErrorV1(RuntimeError):
    """Simulator authority is missing, expired or belongs to another scope."""


@dataclass(frozen=True)
class EmtSimulatorPermitV1:
    """Operator-pinned test scope; not evidence of broker-side environment type."""

    approval_id: str
    account_id: str
    consumer_account_id: str
    adapter_instance_id: str
    profile_id: str
    emt_client_id: int
    credential_digest: str
    endpoint_digest: str
    namespace_manifest_hash: str
    namespace_release_hash: str
    expires_wall_ns: int

    def __post_init__(self) -> None:
        for value in (
            self.approval_id, self.account_id, self.consumer_account_id,
            self.adapter_instance_id, self.profile_id,
        ):
            require_text(value, "simulator permit scope", max_length=128)
        require_positive(self.emt_client_id, "simulator client id")
        require_positive(self.expires_wall_ns, "simulator permit expiry")
        for value in (
            self.credential_digest, self.endpoint_digest,
            self.namespace_manifest_hash, self.namespace_release_hash,
        ):
            require_sha256(value, "simulator permit digest")


def parse_emt_simulator_permit_v1(text: str) -> EmtSimulatorPermitV1:
    """Parse the exact non-secret approval schema, rejecting duplicate keys."""

    def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate simulator permit field")
            result[key] = value
        return result

    try:
        value = json.loads(text, object_pairs_hook=unique_pairs)
        if not isinstance(value, dict) or set(value) != {
            "schema_version", "approval_id", "account_id", "consumer_account_id",
            "adapter_instance_id", "profile_id", "emt_client_id", "credential_digest",
            "endpoint_digest", "namespace_manifest_hash", "namespace_release_hash",
            "expires_wall_ns",
        } or type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError("simulator permit schema is invalid")
        del value["schema_version"]
        return EmtSimulatorPermitV1(**value)
    except (TypeError, ValueError) as exc:
        raise EmtSimulatorGateErrorV1("simulator permit cannot be parsed") from exc


def load_emt_simulator_permit_v1(path: Path) -> EmtSimulatorPermitV1:
    """Read only from a root-owned non-writable path; never from a Consumer."""

    return parse_emt_simulator_permit_v1(LinuxRootOwnedNamespaceFileV1(path).read())


class ConsumerEmtSimulatorGateV1:
    """One-shot gate: an expiry, mismatch or failure permanently closes it."""

    def __init__(
        self,
        *,
        control: ConsumerEmtRuntimeControlSnapshotV1,
        permit: EmtSimulatorPermitV1,
        risk: RiskSnapshot,
        now_wall_ns: Callable[[], int],
        now_monotonic_ns: Callable[[], int],
    ) -> None:
        self._control, self._permit, self._risk = control, permit, risk
        self._wall, self._mono = now_wall_ns, now_monotonic_ns
        self._state = "new"
        self._started_wall = 0
        self._started_mono = 0
        self._lock = RLock()

    @property
    def gate(self) -> ConsumerEmtSimulatorGateV1:
        return self

    @property
    def control(self) -> ConsumerEmtRuntimeControlSnapshotV1:
        return self._control

    def start(self) -> None:
        with self._lock:
            if self._state != "new":
                raise EmtSimulatorGateErrorV1("simulator gate cannot be restarted")
            try:
                profile = self._control.profile_binding.profile
                binding = self._control.profile_binding.binding
                scope = self._control.namespace.directory.scope
                state = self._control.namespace.state
                permit, risk = self._permit, self._risk
                wall, mono = self._wall(), self._mono()
                if (
                    profile.identity.venue_id != "EMT"
                    or profile.state is not ProfileStateV1.ACTIVE_DISARMED
                    or profile.identity.profile_id != permit.profile_id
                    or profile.identity.adapter_instance_id != permit.adapter_instance_id
                    or binding.profile_id != profile.identity.profile_id
                    or binding.profile_revision != profile.revision
                    or binding.effective_wall_ns > wall
                    or binding.retired_wall_ns is not None
                    or binding.account_id != permit.account_id
                    or binding.consumer_account_id != permit.consumer_account_id
                    or profile.material_digest.credential_digest != permit.credential_digest
                    or profile.material_digest.endpoint_digest != permit.endpoint_digest
                    or scope.account_id != permit.account_id
                    or scope.emt_client_id != permit.emt_client_id
                    or state.scope != scope
                    or profile.material_digest.release_manifest_hash != state.release_manifest_hash
                    or not self._control.namespace.directory.own.active_at(wall)
                    or state.namespace_manifest_hash != permit.namespace_manifest_hash
                    or state.release_hash != permit.namespace_release_hash
                    or risk.account_id != permit.account_id
                    or risk.trading_day != scope.trading_day
                    or not risk.captured_monotonic_ns <= mono < risk.expires_monotonic_ns
                    or not 0 <= wall < min(permit.expires_wall_ns, self._control.namespace_not_after_wall_ns)
                ):
                    raise EmtSimulatorGateErrorV1("simulator permit does not match active scope")
                self._started_wall, self._started_mono = wall, mono
                self._state = "active"
            except BaseException:
                self._state = "failed"
                raise

    def admission_permitted(self) -> bool:
        with self._lock:
            if self._state != "active":
                return False
            try:
                wall, mono = self._wall(), self._mono()
                if not (
                    self._started_wall <= wall < min(
                        self._permit.expires_wall_ns, self._control.namespace_not_after_wall_ns
                    )
                    and self._started_mono <= mono < self._risk.expires_monotonic_ns
                ):
                    self._state = "failed"
                    return False
                self._started_wall, self._started_mono = wall, mono
                return True
            except Exception:
                self._state = "failed"
                return False

    def require_account(self, account_id: str) -> None:
        if account_id != self._permit.account_id or not self.admission_permitted():
            raise EmtSimulatorGateErrorV1("simulator submission is blocked")

    def require_mapping(self, mapping: ActivatedVenueMapping, *, wall_ns: int) -> None:
        """Authorize only the pinned simulator mapping, without inventing probes.

        Unlike normal READY eligibility, this explicitly accepts an operator
        assigned simulator scope whose venue-wide uniqueness/replay has not
        been proved. Signed namespace allocation and local ownership remain
        mandatory. Neither probe flag is changed by this method.
        """

        self.require_account(self._permit.account_id)
        manifest, runtime = mapping.manifest, mapping.runtime
        ownership = manifest.scope_ownership
        profile = self._control.profile_binding.profile
        if (
            ownership is None or manifest.venue_id != "EMT"
            or manifest.manifest_hash != profile.material_digest.mapping_manifest_hash
            or canonical_broker_capabilities_hash(manifest.capabilities)
            != profile.material_digest.capability_manifest_hash
            or ownership.account_id != self._permit.account_id
            or ownership.adapter_instance_id != self._permit.adapter_instance_id
            or ownership.adapter_client_scope != str(self._permit.emt_client_id)
            or not ownership.valid_from_wall_ns <= wall_ns < ownership.valid_until_wall_ns
            or (ownership.revoked_at_wall_ns is not None and wall_ns >= ownership.revoked_at_wall_ns)
            or ownership.signer_id not in runtime.trusted_scope_signer_ids
            or runtime.scope_ownership != ownership
            or runtime.account_capabilities.account_id != self._permit.account_id
            or runtime.fingerprint != manifest.fingerprint
            or runtime.capabilities != manifest.capabilities
            or runtime.route_token_binding != manifest.route_token_binding
            or not manifest.effective_interval.contains(wall_ns // 1_000_000_000)
        ):
            self.close()
            raise EmtSimulatorGateErrorV1("simulator mapping is outside the authorized scope")

    def risk_snapshot(self) -> RiskSnapshot:
        if not self.admission_permitted():
            raise EmtSimulatorGateErrorV1("simulator admission snapshot is unavailable")
        return self._risk

    def account_view(self, binding: ConsumerAccountBinding) -> ConsumerAccountView:
        if (
            binding not in self._control.consumer_bindings.accounts
            or binding.account_id != self._permit.account_id
            or binding.consumer_account_id != self._permit.consumer_account_id
            or binding.adapter_instance_id != self._permit.adapter_instance_id
            or binding.venue_id != "EMT"
        ):
            raise EmtSimulatorGateErrorV1("consumer binding is outside simulator permit")
        return ConsumerAccountView(
            binding.consumer_account_id, binding.account_display_name, "EMT",
            "emt-simulator-submit-only-v1",
            AccountLifecycle.SIMULATOR_ACTIVE if self.admission_permitted() else AccountLifecycle.DISARMED,
            ReconciliationStatus.NOT_REQUIRED,
        )

    def close(self) -> None:
        with self._lock:
            self._state = "failed"
