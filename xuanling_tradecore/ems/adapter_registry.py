"""Fail-closed Adapter profile registry and read-only recovery boundary."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from enum import Enum
from hashlib import sha256
from threading import RLock
from typing import Callable

from ..adapters import (
    ActivatedVenueMapping,
    BrokerAdapter,
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    SubmitAccepted,
)
from ..domain import (
    AccountLifecycle,
    BrokerCapabilities,
    BrokerRecoveryEvidence,
    ExecutionInstruction,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    RecoveryPlan,
    TradingDayEpoch,
)
from ..domain._validation import require_positive, require_sha256, require_text
from .routing import AccountExecutionBinding


class VenueAdapterRegistryError(RuntimeError):
    """A profile cannot be selected or constructed without exact proof."""


class AdapterProfileMode(str, Enum):
    ELIGIBLE_DISARMED = "ELIGIBLE_DISARMED"
    READ_ONLY_RECONCILIATION = "READ_ONLY_RECONCILIATION"


def canonical_broker_capabilities_hash(capabilities: BrokerCapabilities) -> str:
    """Hash the complete capability declaration used by one registry key."""

    payload = {
        "schema_version": 1,
        "capability_profile_id": capabilities.capability_profile_id,
        "version": capabilities.version,
        "venue_id": capabilities.venue_id,
        "supports_cash": capabilities.supports_cash,
        "supports_margin_buy": capabilities.supports_margin_buy,
        "supports_short_sell": capabilities.supports_short_sell,
        "supports_pagination": capabilities.supports_pagination,
        "supports_public_stream_replay": capabilities.supports_public_stream_replay,
        "echoes_client_order_ref": capabilities.echoes_client_order_ref,
        "has_stable_venue_order_id": capabilities.has_stable_venue_order_id,
        "has_stable_trade_id": capabilities.has_stable_trade_id,
        "supports_complete_recovery_queries": capabilities.supports_complete_recovery_queries,
        "supports_negative_confirmation": capabilities.supports_negative_confirmation,
        "supports_paper_matching": capabilities.supports_paper_matching,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class AdapterProfileKey:
    venue_id: str
    adapter_instance_id: str
    capability_manifest_hash: str
    mapping_manifest_hash: str

    def __post_init__(self) -> None:
        require_text(self.venue_id, "Adapter profile venue_id")
        require_text(self.adapter_instance_id, "Adapter profile adapter_instance_id")
        require_sha256(self.capability_manifest_hash, "Adapter capability_manifest_hash")
        require_sha256(self.mapping_manifest_hash, "Adapter mapping_manifest_hash")


AdapterFactory = Callable[[], BrokerAdapter]
RecoveryCertificateFactory = Callable[
    [str, TradingDayEpoch, RecoveryEvidenceProfile], RecoveryCompletenessCertificate
]


@dataclass(frozen=True)
class AdapterProfileDeclaration:
    profile_id: str
    version: int
    key: AdapterProfileKey
    mapping: ActivatedVenueMapping
    recovery_profile: RecoveryEvidenceProfile
    adapter_factory: AdapterFactory | None
    recovery_certificate_factory: RecoveryCertificateFactory | None

    def __post_init__(self) -> None:
        require_text(self.profile_id, "Adapter profile_id")
        require_positive(self.version, "Adapter profile version")
        manifest = self.mapping.manifest
        ownership = manifest.scope_ownership
        if manifest.venue_id != self.key.venue_id:
            raise VenueAdapterRegistryError("profile mapping belongs to another venue")
        if manifest.manifest_hash != self.key.mapping_manifest_hash:
            raise VenueAdapterRegistryError("profile mapping hash differs from its registry key")
        if (
            canonical_broker_capabilities_hash(self.mapping.runtime.capabilities)
            != self.key.capability_manifest_hash
        ):
            raise VenueAdapterRegistryError("profile capability hash differs from its registry key")
        if ownership is None or ownership.adapter_instance_id != self.key.adapter_instance_id:
            raise VenueAdapterRegistryError(
                "profile scope ownership differs from its Adapter instance key"
            )
        if self.mapping.runtime.account_capabilities.account_id != ownership.account_id:
            raise VenueAdapterRegistryError("profile runtime account differs from scope ownership")


@dataclass(frozen=True)
class AdapterProfileEligibility:
    eligible: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        reasons = tuple(
            sorted(require_text(reason, "profile reason") for reason in self.reason_codes)
        )
        if len(reasons) != len(set(reasons)):
            raise VenueAdapterRegistryError("profile eligibility reasons must be unique")
        if self.eligible == bool(reasons):
            raise VenueAdapterRegistryError("profile eligibility and reasons are inconsistent")
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True)
class AdapterProfileSelection:
    account_id: str
    key: AdapterProfileKey
    profile_id: str
    profile_version: int
    mode: AdapterProfileMode
    reason_codes: tuple[str, ...]
    profile_switched: bool
    authorizes_ready: bool = False


class ReadOnlyBrokerAdapter:
    """Prevent mutation while retaining query and recovery operations."""

    def __init__(self, adapter: BrokerAdapter) -> None:
        self._adapter = adapter

    @property
    def adapter_id(self) -> str:
        return self._adapter.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        return self._adapter.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._adapter.capabilities

    @property
    def wrapped_adapter(self) -> BrokerAdapter:
        return self._adapter

    def start(self, observer: BrokerEventObserver) -> None:
        self._adapter.start(observer)

    def stop(self) -> None:
        self._adapter.stop()

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        del instruction
        raise VenueAdapterRegistryError("read-only Adapter rejects submit")

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        del instruction
        raise VenueAdapterRegistryError("read-only Adapter rejects cancel")

    def query(self, request: BrokerQuery) -> QueryHandle:
        return self._adapter.query(request)

    def begin_recovery(self, plan: RecoveryPlan, observer: BrokerEventObserver) -> RecoveryHandle:
        return self._adapter.begin_recovery(plan, observer)

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        return self._adapter.collect_recovery_evidence(handle)


class VenueAdapterRegistry:
    """Select and construct one exact Adapter profile per DISARMED account."""

    def __init__(self, profiles: tuple[AdapterProfileDeclaration, ...] = ()) -> None:
        self._profiles: dict[AdapterProfileKey, AdapterProfileDeclaration] = {}
        self._profile_ids: set[tuple[str, int]] = set()
        self._selections: dict[str, AdapterProfileSelection] = {}
        self._switch_recovery_required: set[str] = set()
        self._adapters: dict[str, BrokerAdapter] = {}
        self._adapter_object_ids: set[int] = set()
        self._lock = RLock()
        for profile in profiles:
            self.register(profile)

    def register(self, profile: AdapterProfileDeclaration) -> None:
        with self._lock:
            profile_identity = (profile.profile_id, profile.version)
            if profile.key in self._profiles:
                raise VenueAdapterRegistryError("Adapter profile key is already registered")
            if profile_identity in self._profile_ids:
                raise VenueAdapterRegistryError("Adapter profile identity is already registered")
            self._profiles[profile.key] = profile
            self._profile_ids.add(profile_identity)

    def evaluate(
        self, key: AdapterProfileKey, *, evaluated_wall_ns: int
    ) -> AdapterProfileEligibility:
        if evaluated_wall_ns < 0:
            raise ValueError("profile evaluation time must be nonnegative")
        with self._lock:
            profile = self._require_profile(key)
            return self._evaluate_profile(profile, evaluated_wall_ns)

    def select(
        self,
        binding: AccountExecutionBinding,
        *,
        account_lifecycle: AccountLifecycle,
        adapter_session_active: bool,
        evaluated_wall_ns: int,
    ) -> AdapterProfileSelection:
        if account_lifecycle is not AccountLifecycle.DISARMED:
            raise VenueAdapterRegistryError("Adapter profile selection requires DISARMED account")
        if adapter_session_active:
            raise VenueAdapterRegistryError("active Adapter session blocks profile selection")
        if evaluated_wall_ns < 0:
            raise ValueError("profile selection time must be nonnegative")
        key = AdapterProfileKey(
            venue_id=binding.venue_id,
            adapter_instance_id=binding.adapter_instance_id,
            capability_manifest_hash=binding.capability_manifest_hash,
            mapping_manifest_hash=binding.mapping_manifest_hash,
        )
        with self._lock:
            profile = self._require_profile(key)
            ownership = profile.mapping.manifest.scope_ownership
            if ownership is None or (
                ownership.account_id != binding.account_id
                or ownership.venue_id != binding.venue_id
                or ownership.adapter_instance_id != binding.adapter_instance_id
                or ownership.adapter_client_scope != binding.adapter_client_scope
            ):
                raise VenueAdapterRegistryError(
                    "account route differs from profile scope ownership"
                )
            existing = self._selections.get(binding.account_id)
            eligibility = self._evaluate_profile(profile, evaluated_wall_ns)
            switched = existing is not None and existing.key != key
            if switched:
                self._switch_recovery_required.add(binding.account_id)
            recovery_pending = binding.account_id in self._switch_recovery_required
            reasons = set(eligibility.reason_codes)
            if recovery_pending:
                reasons.add("PROFILE_SWITCH_REQUIRES_RECOVERY")
            mode = (
                AdapterProfileMode.ELIGIBLE_DISARMED
                if not reasons
                else AdapterProfileMode.READ_ONLY_RECONCILIATION
            )
            selection = AdapterProfileSelection(
                account_id=binding.account_id,
                key=key,
                profile_id=profile.profile_id,
                profile_version=profile.version,
                mode=mode,
                reason_codes=tuple(sorted(reasons)),
                profile_switched=recovery_pending,
            )
            if existing is not None and existing == selection:
                return existing
            if binding.account_id in self._adapters:
                raise VenueAdapterRegistryError(
                    "constructed Adapter must be detached before profile change"
                )
            self._selections[binding.account_id] = selection
            return selection

    def create_selected_adapter(self, selection: AdapterProfileSelection) -> BrokerAdapter:
        with self._lock:
            current = self._selections.get(selection.account_id)
            if current != selection:
                raise VenueAdapterRegistryError("Adapter selection is stale or not registered")
            existing = self._adapters.get(selection.account_id)
            if existing is not None:
                return existing
            profile = self._require_profile(selection.key)
            factory = profile.adapter_factory
            if factory is None:
                raise VenueAdapterRegistryError("Adapter factory is unavailable")
            adapter = factory()
            self._validate_adapter(profile, adapter)
            if id(adapter) in self._adapter_object_ids:
                raise VenueAdapterRegistryError("Adapter object is reused by another account")
            exposed: BrokerAdapter = (
                adapter
                if selection.mode is AdapterProfileMode.ELIGIBLE_DISARMED
                else ReadOnlyBrokerAdapter(adapter)
            )
            self._adapters[selection.account_id] = exposed
            self._adapter_object_ids.add(id(adapter))
            return exposed

    def detach(
        self,
        account_id: str,
        *,
        account_lifecycle: AccountLifecycle,
        adapter_session_active: bool,
    ) -> None:
        require_text(account_id, "Adapter detach account_id")
        if account_lifecycle is not AccountLifecycle.DISARMED or adapter_session_active:
            raise VenueAdapterRegistryError("Adapter detach requires stopped DISARMED account")
        with self._lock:
            exposed = self._adapters.pop(account_id, None)
            if exposed is None:
                return
            underlying = (
                exposed.wrapped_adapter if isinstance(exposed, ReadOnlyBrokerAdapter) else exposed
            )
            self._adapter_object_ids.discard(id(underlying))

    def create_recovery_certificate(
        self,
        selection: AdapterProfileSelection,
        *,
        trading_day: TradingDayEpoch,
    ) -> RecoveryCompletenessCertificate:
        with self._lock:
            if self._selections.get(selection.account_id) != selection:
                raise VenueAdapterRegistryError("recovery certificate selection is stale")
            profile = self._require_profile(selection.key)
            factory = profile.recovery_certificate_factory
            if factory is None:
                raise VenueAdapterRegistryError("recovery certificate factory is unavailable")
            certificate = factory(selection.account_id, trading_day, profile.recovery_profile)
            self._validate_certificate(profile, selection, certificate, trading_day)
            return certificate

    def complete_profile_recovery(
        self,
        selection: AdapterProfileSelection,
        certificate: RecoveryCompletenessCertificate,
        *,
        evaluated_wall_ns: int,
    ) -> AdapterProfileSelection:
        if evaluated_wall_ns < 0:
            raise ValueError("profile recovery completion time must be nonnegative")
        with self._lock:
            if self._selections.get(selection.account_id) != selection:
                raise VenueAdapterRegistryError("profile recovery selection is stale")
            if selection.account_id not in self._switch_recovery_required:
                raise VenueAdapterRegistryError("profile switch recovery is not pending")
            if selection.account_id in self._adapters:
                raise VenueAdapterRegistryError(
                    "read-only Adapter must be stopped and detached after recovery"
                )
            profile = self._require_profile(selection.key)
            self._validate_certificate(profile, selection, certificate, certificate.trading_day)
            incomplete = certificate.incompleteness_reasons(profile.recovery_profile)
            eligibility = self._evaluate_profile(profile, evaluated_wall_ns)
            if incomplete or not eligibility.eligible:
                raise VenueAdapterRegistryError(
                    "profile recovery evidence is incomplete or profile became ineligible"
                )
            self._switch_recovery_required.remove(selection.account_id)
            promoted = replace(
                selection,
                mode=AdapterProfileMode.ELIGIBLE_DISARMED,
                reason_codes=(),
            )
            self._selections[selection.account_id] = promoted
            return promoted

    def _require_profile(self, key: AdapterProfileKey) -> AdapterProfileDeclaration:
        try:
            return self._profiles[key]
        except KeyError as exc:
            raise VenueAdapterRegistryError("exact Adapter profile is not registered") from exc

    @staticmethod
    def _validate_certificate(
        profile: AdapterProfileDeclaration,
        selection: AdapterProfileSelection,
        certificate: RecoveryCompletenessCertificate,
        trading_day: TradingDayEpoch,
    ) -> None:
        ownership = profile.mapping.manifest.scope_ownership
        if ownership is None or (
            certificate.account_id != selection.account_id
            or certificate.venue_id != selection.key.venue_id
            or certificate.adapter_instance_id != selection.key.adapter_instance_id
            or certificate.adapter_client_scope != ownership.adapter_client_scope
            or certificate.trading_day != trading_day
            or certificate.profile_id != profile.recovery_profile.profile_id
            or certificate.profile_hash != profile.recovery_profile.profile_hash
            or certificate.certificate_hash != certificate.content_sha256
        ):
            raise VenueAdapterRegistryError("recovery certificate identity or hash is invalid")

    @staticmethod
    def _evaluate_profile(
        profile: AdapterProfileDeclaration, evaluated_wall_ns: int
    ) -> AdapterProfileEligibility:
        mapping = profile.mapping
        manifest = mapping.manifest
        runtime = mapping.runtime
        ownership = manifest.scope_ownership
        binding = manifest.route_token_binding
        capabilities = manifest.capabilities
        recovery = manifest.recovery
        reasons: set[str] = set()
        if profile.adapter_factory is None:
            reasons.add("ADAPTER_FACTORY_UNAVAILABLE")
        if profile.recovery_certificate_factory is None:
            reasons.add("RECOVERY_CERTIFICATE_FACTORY_UNAVAILABLE")
        if (
            ownership is None
            or runtime.scope_ownership != ownership
            or ownership.signer_id not in runtime.trusted_scope_signer_ids
            or not ownership.is_active_at(evaluated_wall_ns)
        ):
            reasons.add("SCOPE_OWNERSHIP_INVALID")
        if (
            binding is None
            or runtime.route_token_binding != binding
            or not binding.supports_automatic_routing
            or not runtime.route_token_probe_succeeded
        ):
            reasons.add("ROUTE_TOKEN_BINDING_INCOMPLETE")
        if not capabilities.supports_automatic_recovery_validation:
            reasons.add("CAPABILITY_RECOVERY_INCOMPLETE")
        supported_queries = {
            name
            for name, supported in (
                ("orders", recovery.supports_order_query),
                ("trades", recovery.supports_trade_query),
                ("working_orders", recovery.supports_working_order_query),
                ("assets", recovery.supports_asset_query),
                ("positions", recovery.supports_position_query),
            )
            if supported
        }
        evidence = profile.recovery_profile
        if (
            not recovery.automatic_recovery_eligible
            or not evidence.required_query_kinds.issubset(supported_queries)
            or (evidence.requires_public_replay and not capabilities.supports_public_stream_replay)
            or (evidence.requires_route_token_echo and not capabilities.echoes_client_order_ref)
            or (
                evidence.requires_negative_confirmation
                and not capabilities.supports_negative_confirmation
            )
        ):
            reasons.add("RECOVERY_PROFILE_CAPABILITY_MISMATCH")
        return AdapterProfileEligibility(eligible=not reasons, reason_codes=tuple(reasons))

    @staticmethod
    def _validate_adapter(profile: AdapterProfileDeclaration, adapter: BrokerAdapter) -> None:
        manifest = profile.mapping.manifest
        try:
            mismatched = (
                adapter.adapter_id != manifest.fingerprint.adapter_id
                or adapter.adapter_instance_id != profile.key.adapter_instance_id
                or adapter.capabilities != profile.mapping.runtime.capabilities
                or adapter.capabilities.venue_id != profile.key.venue_id
            )
        except BaseException as exc:
            try:
                adapter.stop()
            except BaseException:
                pass
            raise VenueAdapterRegistryError("factory returned an invalid Adapter") from exc
        if not mismatched:
            return
        try:
            adapter.stop()
        except BaseException:
            pass
        raise VenueAdapterRegistryError("factory returned an Adapter outside its profile")
