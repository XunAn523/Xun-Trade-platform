"""Versioned, fail-closed mappings from TradeCore values to one venue profile.

The domain model deliberately uses strings such as ``SZ`` and ``LIMIT``.  A
broker adapter must not turn those strings into vendor enum values ad hoc.
Instead, a signed/approved mapping manifest is validated before the account
can leave DISARMED.  This module is pure: it neither imports a vendor binding
nor opens a file, network connection, or database.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from importlib import resources
from typing import Any, Iterable, Mapping, Sequence, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from ..domain import (
    AdapterClientScopeOwnership,
    BrokerCapabilities,
    ExecutionInstruction,
    RouteTokenBinding,
)
from ..domain._validation import (
    DomainValidationError,
    freeze_texts,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)

MANIFEST_SCHEMA_VERSION = 2
SUPPORTED_MANIFEST_SCHEMA_VERSIONS = frozenset({1, MANIFEST_SCHEMA_VERSION})


class MappingManifestError(DomainValidationError):
    """A mapping manifest or active venue profile violates a safety invariant."""


class ProfileValidationTrigger(str, Enum):
    """Lifecycle points that must repeat the complete profile validation."""

    STARTUP = "STARTUP"
    TRADING_DAY_ROLLOVER = "TRADING_DAY_ROLLOVER"
    PROFILE_SWITCH = "PROFILE_SWITCH"
    RECOVERY = "RECOVERY"


@dataclass(frozen=True)
class AdapterFingerprint:
    """The immutable software identities that one mapping profile approves."""

    adapter_id: str
    adapter_version: str
    sdk_version: str
    adapter_package_hash: str
    sdk_binary_hash: str
    binding_package_hash: str

    def __post_init__(self) -> None:
        require_text(self.adapter_id, "mapping adapter_id")
        require_text(self.adapter_version, "mapping adapter_version")
        require_text(self.sdk_version, "mapping sdk_version")
        require_sha256(self.adapter_package_hash, "mapping adapter_package_hash")
        require_sha256(self.sdk_binary_hash, "mapping sdk_binary_hash")
        require_sha256(self.binding_package_hash, "mapping binding_package_hash")


@dataclass(frozen=True)
class EffectiveInterval:
    """The inclusive interval in which one mapping profile may be activated."""

    not_before_unix_s: int
    not_after_unix_s: int | None = None

    def __post_init__(self) -> None:
        require_nonnegative(self.not_before_unix_s, "mapping effective not_before")
        if self.not_after_unix_s is not None:
            require_nonnegative(self.not_after_unix_s, "mapping effective not_after")
            if self.not_after_unix_s < self.not_before_unix_s:
                raise MappingManifestError("mapping effective interval ends before it starts")

    def contains(self, unix_s: int) -> bool:
        """Return whether ``unix_s`` is within this approved interval."""

        require_nonnegative(unix_s, "mapping activation time")
        return unix_s >= self.not_before_unix_s and (
            self.not_after_unix_s is None or unix_s <= self.not_after_unix_s
        )


@dataclass(frozen=True)
class EvidenceReference:
    """One reviewable vendor or controlled-test source for a mapping assertion."""

    evidence_id: str
    source_reference: str
    fixture_id: str
    test_id: str
    owner: str

    def __post_init__(self) -> None:
        require_text(self.evidence_id, "mapping evidence_id")
        require_text(self.source_reference, "mapping evidence source_reference", max_length=512)
        require_text(self.fixture_id, "mapping evidence fixture_id", max_length=512)
        require_text(self.test_id, "mapping evidence test_id", max_length=512)
        require_text(self.owner, "mapping evidence owner", max_length=128)


@dataclass(frozen=True)
class EnumMapping:
    """One canonical string to one adapter-native integer enum mapping."""

    domain_value: str
    venue_value: int
    evidence_ids: frozenset[str]

    def __post_init__(self) -> None:
        require_text(self.domain_value, "mapping domain value", max_length=64)
        require_nonnegative(self.venue_value, "mapping venue value")
        evidence_ids = freeze_texts(self.evidence_ids, "mapping enum evidence_ids")
        if not evidence_ids:
            raise MappingManifestError("every enum mapping requires evidence")
        object.__setattr__(self, "evidence_ids", evidence_ids)


@dataclass(frozen=True)
class VenueFieldMappings:
    """All independently versioned enum fields used by a plain order submit."""

    market: tuple[EnumMapping, ...]
    side: tuple[EnumMapping, ...]
    price_type: tuple[EnumMapping, ...]
    business_type: tuple[EnumMapping, ...]

    def __post_init__(self) -> None:
        for field_name in ("market", "side", "price_type", "business_type"):
            mappings = tuple(getattr(self, field_name))
            if not mappings:
                raise MappingManifestError(f"mapping field {field_name} cannot be empty")
            if not all(isinstance(mapping, EnumMapping) for mapping in mappings):
                raise MappingManifestError(f"mapping field {field_name} contains an invalid entry")
            domain_values = tuple(mapping.domain_value for mapping in mappings)
            venue_values = tuple(mapping.venue_value for mapping in mappings)
            if len(set(domain_values)) != len(domain_values):
                raise MappingManifestError(f"mapping field {field_name} repeats a domain value")
            if len(set(venue_values)) != len(venue_values):
                raise MappingManifestError(f"mapping field {field_name} repeats a venue value")
            object.__setattr__(self, field_name, mappings)

    def resolve(self, field_name: str, domain_value: str) -> EnumMapping:
        """Resolve one approved enum value; unknown values are never guessed."""

        if field_name not in {"market", "side", "price_type", "business_type"}:
            raise MappingManifestError(f"unknown mapping field {field_name}")
        for item in getattr(self, field_name):
            if item.domain_value == domain_value:
                return item
        raise MappingManifestError(
            f"unmapped {field_name} value {domain_value!r}; account must remain disarmed"
        )

    def all_evidence_ids(self) -> frozenset[str]:
        """Return every source referenced by a field mapping."""

        return frozenset(
            evidence_id
            for mappings in (self.market, self.side, self.price_type, self.business_type)
            for mapping in mappings
            for evidence_id in mapping.evidence_ids
        )


@dataclass(frozen=True)
class VenueOrderRoute:
    """An allowed business-type/side pair and the account capability it consumes."""

    business_type: str
    side: str
    required_capabilities: frozenset[str]
    evidence_ids: frozenset[str]

    def __post_init__(self) -> None:
        require_text(self.business_type, "mapping route business_type", max_length=64)
        require_text(self.side, "mapping route side", max_length=64)
        capabilities = freeze_texts(
            self.required_capabilities,
            "mapping route required_capabilities",
            max_length=64,
        )
        invalid_capabilities = capabilities - {"CASH", "MARGIN_BUY", "SHORT_SELL"}
        if invalid_capabilities:
            raise MappingManifestError("mapping route declares an unknown account capability")
        if not capabilities:
            raise MappingManifestError("mapping route requires at least one account capability")
        evidence_ids = freeze_texts(self.evidence_ids, "mapping route evidence_ids")
        if not evidence_ids:
            raise MappingManifestError("mapping route requires evidence")
        object.__setattr__(self, "required_capabilities", capabilities)
        object.__setattr__(self, "evidence_ids", evidence_ids)


@dataclass(frozen=True)
class VenueIdentityDeclaration:
    """The adapter fields used to create a normalized VenueOrderIdentity later."""

    client_order_ref_field: str
    client_order_ref_format: str
    venue_order_id_field: str
    venue_order_id_format: str
    venue_cancel_id_field: str
    venue_cancel_id_format: str
    adapter_client_scope_field: str
    adapter_client_scope_format: str
    evidence_ids: frozenset[str]

    def __post_init__(self) -> None:
        for field_name in (
            "client_order_ref_field",
            "client_order_ref_format",
            "venue_order_id_field",
            "venue_order_id_format",
            "venue_cancel_id_field",
            "venue_cancel_id_format",
            "adapter_client_scope_field",
            "adapter_client_scope_format",
        ):
            require_text(
                getattr(self, field_name), f"mapping identity {field_name}", max_length=128
            )
        evidence_ids = freeze_texts(self.evidence_ids, "mapping identity evidence_ids")
        if not evidence_ids:
            raise MappingManifestError("mapping identity declaration requires evidence")
        object.__setattr__(self, "evidence_ids", evidence_ids)


@dataclass(frozen=True)
class RecoveryCapabilityDeclaration:
    """Completeness facts required before automatic recovery may ever be enabled."""

    supports_order_query: bool
    supports_trade_query: bool
    supports_working_order_query: bool
    supports_asset_query: bool
    supports_position_query: bool
    automatic_recovery_eligible: bool
    evidence_ids: frozenset[str]

    def __post_init__(self) -> None:
        evidence_ids = freeze_texts(self.evidence_ids, "mapping recovery evidence_ids")
        if not evidence_ids:
            raise MappingManifestError("mapping recovery declaration requires evidence")
        object.__setattr__(self, "evidence_ids", evidence_ids)


@dataclass(frozen=True)
class AccountTradingCapabilities:
    """Per-account entitlement facts; a profile capability is never an entitlement."""

    account_id: str
    supports_cash: bool
    supports_margin_buy: bool
    supports_short_sell: bool

    def __post_init__(self) -> None:
        require_text(self.account_id, "mapping account_id")

    def has(self, capability: str) -> bool:
        """Return the explicit account entitlement for one normalized capability."""

        lookup = {
            "CASH": self.supports_cash,
            "MARGIN_BUY": self.supports_margin_buy,
            "SHORT_SELL": self.supports_short_sell,
        }
        try:
            return lookup[capability]
        except KeyError as error:
            raise MappingManifestError(f"unknown account capability {capability}") from error


@dataclass(frozen=True)
class VenueMappingManifest:
    """A complete, immutable profile approved for one adapter/SDK/binding tuple."""

    schema_version: int
    manifest_id: str
    manifest_version: int
    venue_id: str
    fingerprint: AdapterFingerprint
    effective_interval: EffectiveInterval
    owner: str
    evidence: tuple[EvidenceReference, ...]
    field_mappings: VenueFieldMappings
    order_routes: tuple[VenueOrderRoute, ...]
    identity: VenueIdentityDeclaration
    capabilities: BrokerCapabilities
    recovery: RecoveryCapabilityDeclaration
    scope_ownership: AdapterClientScopeOwnership | None = None
    route_token_binding: RouteTokenBinding | None = None

    def __post_init__(self) -> None:
        if self.schema_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
            raise MappingManifestError("unsupported mapping manifest schema version")
        require_text(self.manifest_id, "mapping manifest_id")
        require_positive(self.manifest_version, "mapping manifest_version")
        require_text(self.venue_id, "mapping venue_id")
        require_text(self.owner, "mapping owner", max_length=128)
        evidence = tuple(self.evidence)
        routes = tuple(self.order_routes)
        if not evidence:
            raise MappingManifestError("mapping manifest requires source evidence")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "order_routes", routes)
        evidence_ids = tuple(item.evidence_id for item in evidence)
        if len(set(evidence_ids)) != len(evidence_ids):
            raise MappingManifestError("mapping manifest repeats an evidence_id")
        all_evidence = frozenset(evidence_ids)
        referenced_evidence = (
            self.field_mappings.all_evidence_ids()
            | self.identity.evidence_ids
            | self.recovery.evidence_ids
            | frozenset(evidence_id for route in routes for evidence_id in route.evidence_ids)
        )
        unknown_evidence = referenced_evidence - all_evidence
        if unknown_evidence:
            raise MappingManifestError("mapping manifest references missing evidence")
        if not routes:
            raise MappingManifestError("mapping manifest requires at least one allowed order route")
        route_keys = tuple((route.business_type, route.side) for route in routes)
        if len(set(route_keys)) != len(route_keys):
            raise MappingManifestError("mapping manifest repeats an order route")
        for route in routes:
            self.field_mappings.resolve("business_type", route.business_type)
            self.field_mappings.resolve("side", route.side)
        if self.capabilities.venue_id != self.venue_id:
            raise MappingManifestError("mapping capability declaration has another venue")
        if self.schema_version >= 2:
            if self.scope_ownership is None or self.route_token_binding is None:
                raise MappingManifestError(
                    "V2 mapping manifest requires scope ownership and route token binding"
                )
            if self.scope_ownership.venue_id != self.venue_id:
                raise MappingManifestError("scope ownership has another venue")
            if self.route_token_binding.venue_id != self.venue_id:
                raise MappingManifestError("route token binding has another venue")
            if self.route_token_binding.native_field_name != self.identity.client_order_ref_field:
                raise MappingManifestError(
                    "route token binding field differs from identity declaration"
                )
        capability_flags = {
            "CASH": self.capabilities.supports_cash,
            "MARGIN_BUY": self.capabilities.supports_margin_buy,
            "SHORT_SELL": self.capabilities.supports_short_sell,
        }
        for route in routes:
            if any(not capability_flags[item] for item in route.required_capabilities):
                raise MappingManifestError(
                    "mapping route is not supported by its declared adapter capability"
                )
        strict_recovery = (
            self.capabilities.supports_automatic_recovery_validation
            and self.recovery.supports_order_query
            and self.recovery.supports_trade_query
            and self.recovery.supports_working_order_query
            and self.recovery.supports_asset_query
            and self.recovery.supports_position_query
        )
        if self.recovery.automatic_recovery_eligible != strict_recovery:
            raise MappingManifestError(
                "mapping automatic recovery eligibility must equal its complete capability proof"
            )

    @property
    def manifest_hash(self) -> str:
        """Return a deterministic digest for the exact approved manifest contents."""

        payload = _manifest_payload(self)
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return sha256(encoded).hexdigest()

    def find_route(self, business_type: str, side: str) -> VenueOrderRoute:
        """Return the explicitly allowed business/side pair or fail closed."""

        for route in self.order_routes:
            if route.business_type == business_type and route.side == side:
                return route
        raise MappingManifestError(
            f"unapproved business/side pair {business_type!r}/{side!r}; account must remain disarmed"
        )


@dataclass(frozen=True)
class RuntimeVenueProfile:
    """The adapter facts observed at startup before an account may be armed."""

    fingerprint: AdapterFingerprint
    capabilities: BrokerCapabilities
    account_capabilities: AccountTradingCapabilities
    scope_ownership: AdapterClientScopeOwnership | None = None
    route_token_binding: RouteTokenBinding | None = None
    trusted_scope_signer_ids: frozenset[str] = frozenset()
    route_token_probe_succeeded: bool = False

    def __post_init__(self) -> None:
        trusted = freeze_texts(
            self.trusted_scope_signer_ids,
            "runtime trusted scope signer ids",
            max_length=128,
        )
        object.__setattr__(self, "trusted_scope_signer_ids", trusted)


@dataclass(frozen=True)
class MappedVenueOrder:
    """A non-dispatchable native-value view produced by an activated manifest."""

    execution_id: str
    oms_order_id: str
    account_id: str
    manifest_id: str
    manifest_version: int
    manifest_hash: str
    market: int
    side: int
    price_type: int
    business_type: int
    identity: VenueIdentityDeclaration


@dataclass(frozen=True)
class ActivatedVenueMapping:
    """A startup-validated manifest bound to one running adapter/account profile."""

    manifest: VenueMappingManifest
    runtime: RuntimeVenueProfile
    validation_trigger: ProfileValidationTrigger = ProfileValidationTrigger.STARTUP

    def map_execution(self, instruction: ExecutionInstruction) -> MappedVenueOrder:
        """Map one canonical instruction after profile and account capability validation."""

        if instruction.account_id != self.runtime.account_capabilities.account_id:
            raise MappingManifestError(
                "instruction account does not match activated mapping account"
            )
        market = self.manifest.field_mappings.resolve("market", instruction.market)
        side = self.manifest.field_mappings.resolve("side", instruction.side)
        price_type = self.manifest.field_mappings.resolve("price_type", instruction.price_type)
        business_type = self.manifest.field_mappings.resolve(
            "business_type", instruction.business_type
        )
        route = self.manifest.find_route(instruction.business_type, instruction.side)
        for capability in route.required_capabilities:
            if not self.runtime.account_capabilities.has(capability):
                raise MappingManifestError(
                    f"account lacks required {capability} capability; account must remain disarmed"
                )
        return MappedVenueOrder(
            execution_id=instruction.execution_id,
            oms_order_id=instruction.oms_order_id,
            account_id=instruction.account_id,
            manifest_id=self.manifest.manifest_id,
            manifest_version=self.manifest.manifest_version,
            manifest_hash=self.manifest.manifest_hash,
            market=market.venue_value,
            side=side.venue_value,
            price_type=price_type.venue_value,
            business_type=business_type.venue_value,
            identity=self.manifest.identity,
        )


class VenueMappingRegistry:
    """In-memory registry that allows only exact profile activation.

    Manifest signature and release-bundle loading are control-plane work in a
    later milestone.  Once a manifest reaches this registry, every identity,
    evidence reference, effective interval, runtime hash, and account
    capability is still checked synchronously and fail-closed.
    """

    def __init__(self, manifests: Iterable[VenueMappingManifest] = ()) -> None:
        self._manifests: dict[tuple[str, str, int], VenueMappingManifest] = {}
        for manifest in manifests:
            self.register(manifest)

    def register(self, manifest: VenueMappingManifest) -> None:
        """Register exactly one immutable manifest version."""

        key = (manifest.venue_id, manifest.manifest_id, manifest.manifest_version)
        if key in self._manifests:
            raise MappingManifestError("mapping manifest version is already registered")
        self._manifests[key] = manifest

    def activate(
        self,
        *,
        venue_id: str,
        manifest_id: str,
        manifest_version: int,
        runtime: RuntimeVenueProfile,
        activated_at_unix_s: int,
        validation_trigger: ProfileValidationTrigger = ProfileValidationTrigger.STARTUP,
    ) -> ActivatedVenueMapping:
        """Verify an exact manifest/profile pair before it can map an order."""

        require_text(venue_id, "mapping activation venue_id")
        require_text(manifest_id, "mapping activation manifest_id")
        require_positive(manifest_version, "mapping activation manifest_version")
        require_nonnegative(activated_at_unix_s, "mapping activation time")
        try:
            manifest = self._manifests[(venue_id, manifest_id, manifest_version)]
        except KeyError as error:
            raise MappingManifestError("mapping manifest is not registered") from error
        if not manifest.effective_interval.contains(activated_at_unix_s):
            raise MappingManifestError("mapping manifest is expired or not yet effective")
        if manifest.fingerprint != runtime.fingerprint:
            raise MappingManifestError(
                "adapter, SDK, or binding fingerprint does not match manifest"
            )
        if manifest.capabilities != runtime.capabilities:
            raise MappingManifestError("adapter capability declaration does not match manifest")
        ownership = manifest.scope_ownership
        binding = manifest.route_token_binding
        if ownership is None or binding is None:
            raise MappingManifestError(
                "legacy mapping manifest lacks scope ownership or route token binding"
            )
        if runtime.scope_ownership != ownership:
            raise MappingManifestError("runtime scope ownership does not match manifest")
        if runtime.route_token_binding != binding:
            raise MappingManifestError("runtime route token binding does not match manifest")
        if ownership.signer_id not in runtime.trusted_scope_signer_ids:
            raise MappingManifestError("scope ownership signer is not trusted")
        if ownership.account_id != runtime.account_capabilities.account_id:
            raise MappingManifestError("scope ownership belongs to another account")
        activated_at_wall_ns = activated_at_unix_s * 1_000_000_000
        if not ownership.is_active_at(activated_at_wall_ns):
            raise MappingManifestError(
                "scope ownership is expired, revoked, or lacks a successful uniqueness probe"
            )
        if not binding.supports_automatic_routing:
            raise MappingManifestError("route token binding lacks a required echo or replay path")
        if not runtime.route_token_probe_succeeded:
            raise MappingManifestError("route token startup echo probe did not succeed")
        return ActivatedVenueMapping(
            manifest=manifest,
            runtime=runtime,
            validation_trigger=validation_trigger,
        )


def load_mapping_manifest_json(raw: str) -> VenueMappingManifest:
    """Parse, schema-check, and semantically validate a V1 JSON manifest."""

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise MappingManifestError("mapping manifest is not valid JSON") from error
    _validate_json_schema(value)
    root = _as_mapping(value, "mapping manifest")
    return _manifest_from_mapping(root)


def load_packaged_mapping_manifest(package: str, resource_name: str) -> VenueMappingManifest:
    """Load one installed manifest resource without importing an adapter SDK."""

    require_text(package, "mapping manifest package", max_length=256)
    require_text(resource_name, "mapping manifest resource_name", max_length=256)
    try:
        raw = resources.files(package).joinpath(resource_name).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as error:
        raise MappingManifestError("packaged mapping manifest is unavailable") from error
    return load_mapping_manifest_json(raw)


def _validate_json_schema(value: object) -> None:
    try:
        root = _as_mapping(value, "mapping manifest")
        schema_version = _as_int(root.get("schema_version"), "mapping schema_version")
        if schema_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
            raise MappingManifestError("unsupported mapping manifest schema version")
        schema_text = (
            resources.files("xuanling_tradecore.adapters.schemas")
            .joinpath(f"venue-mapping-manifest-v{schema_version}.schema.json")
            .read_text(encoding="utf-8")
        )
        schema = json.loads(schema_text)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(cast(dict[str, Any], schema))
        validator.validate(value)  # pyright: ignore[reportUnknownMemberType]
    except (FileNotFoundError, ModuleNotFoundError, json.JSONDecodeError, SchemaError) as error:
        raise MappingManifestError("mapping manifest schema is unavailable or invalid") from error
    except ValidationError as error:
        raise MappingManifestError("mapping manifest violates V1 JSON schema") from error


def _manifest_from_mapping(value: Mapping[str, object]) -> VenueMappingManifest:
    schema_version = _as_int(value["schema_version"], "mapping schema_version")
    fingerprint_raw = _as_mapping(value["fingerprint"], "mapping fingerprint")
    interval_raw = _as_mapping(value["effective_interval"], "mapping effective_interval")
    mappings_raw = _as_mapping(value["field_mappings"], "mapping field_mappings")
    capabilities_raw = _as_mapping(value["capabilities"], "mapping capabilities")
    identity_raw = _as_mapping(value["identity"], "mapping identity")
    recovery_raw = _as_mapping(value["recovery"], "mapping recovery")
    scope_raw = (
        _as_mapping(value["scope_ownership"], "mapping scope_ownership")
        if schema_version >= 2
        else None
    )
    binding_raw = (
        _as_mapping(value["route_token_binding"], "mapping route_token_binding")
        if schema_version >= 2
        else None
    )
    evidence = tuple(
        EvidenceReference(
            evidence_id=_as_text(item, "mapping evidence evidence_id"),
            source_reference=_as_text(source, "mapping evidence source_reference"),
            fixture_id=_as_text(fixture, "mapping evidence fixture_id"),
            test_id=_as_text(test, "mapping evidence test_id"),
            owner=_as_text(owner, "mapping evidence owner"),
        )
        for item, source, fixture, test, owner in (
            _evidence_fields(_as_mapping(item, "mapping evidence"))
            for item in _as_sequence(value["evidence"], "mapping evidence")
        )
    )
    return VenueMappingManifest(
        schema_version=schema_version,
        manifest_id=_as_text(value["manifest_id"], "mapping manifest_id"),
        manifest_version=_as_int(value["manifest_version"], "mapping manifest_version"),
        venue_id=_as_text(value["venue_id"], "mapping venue_id"),
        fingerprint=AdapterFingerprint(
            adapter_id=_as_text(fingerprint_raw["adapter_id"], "mapping adapter_id"),
            adapter_version=_as_text(fingerprint_raw["adapter_version"], "mapping adapter_version"),
            sdk_version=_as_text(fingerprint_raw["sdk_version"], "mapping sdk_version"),
            adapter_package_hash=_as_text(
                fingerprint_raw["adapter_package_hash"], "mapping adapter_package_hash"
            ),
            sdk_binary_hash=_as_text(fingerprint_raw["sdk_binary_hash"], "mapping sdk_binary_hash"),
            binding_package_hash=_as_text(
                fingerprint_raw["binding_package_hash"], "mapping binding_package_hash"
            ),
        ),
        effective_interval=EffectiveInterval(
            not_before_unix_s=_as_int(
                interval_raw["not_before_unix_s"], "mapping effective not_before"
            ),
            not_after_unix_s=_as_optional_int(
                interval_raw.get("not_after_unix_s"), "mapping effective not_after"
            ),
        ),
        owner=_as_text(value["owner"], "mapping owner"),
        evidence=evidence,
        field_mappings=VenueFieldMappings(
            market=_enum_mappings(mappings_raw["market"], "market"),
            side=_enum_mappings(mappings_raw["side"], "side"),
            price_type=_enum_mappings(mappings_raw["price_type"], "price_type"),
            business_type=_enum_mappings(mappings_raw["business_type"], "business_type"),
        ),
        order_routes=tuple(
            VenueOrderRoute(
                business_type=_as_text(item["business_type"], "mapping route business_type"),
                side=_as_text(item["side"], "mapping route side"),
                required_capabilities=frozenset(
                    _as_text(value, "mapping route required_capability")
                    for value in _as_sequence(
                        item["required_capabilities"], "mapping route required_capabilities"
                    )
                ),
                evidence_ids=frozenset(
                    _as_text(value, "mapping route evidence_id")
                    for value in _as_sequence(item["evidence_ids"], "mapping route evidence_ids")
                ),
            )
            for item in (
                _as_mapping(item, "mapping order route")
                for item in _as_sequence(value["order_routes"], "mapping order_routes")
            )
        ),
        identity=VenueIdentityDeclaration(
            client_order_ref_field=_as_text(
                identity_raw["client_order_ref_field"], "mapping identity client_order_ref_field"
            ),
            client_order_ref_format=_as_text(
                identity_raw["client_order_ref_format"], "mapping identity client_order_ref_format"
            ),
            venue_order_id_field=_as_text(
                identity_raw["venue_order_id_field"], "mapping identity venue_order_id_field"
            ),
            venue_order_id_format=_as_text(
                identity_raw["venue_order_id_format"], "mapping identity venue_order_id_format"
            ),
            venue_cancel_id_field=_as_text(
                identity_raw["venue_cancel_id_field"], "mapping identity venue_cancel_id_field"
            ),
            venue_cancel_id_format=_as_text(
                identity_raw["venue_cancel_id_format"], "mapping identity venue_cancel_id_format"
            ),
            adapter_client_scope_field=_as_text(
                identity_raw["adapter_client_scope_field"],
                "mapping identity adapter_client_scope_field",
            ),
            adapter_client_scope_format=_as_text(
                identity_raw["adapter_client_scope_format"],
                "mapping identity adapter_client_scope_format",
            ),
            evidence_ids=frozenset(
                _as_text(item, "mapping identity evidence_id")
                for item in _as_sequence(
                    identity_raw["evidence_ids"], "mapping identity evidence_ids"
                )
            ),
        ),
        capabilities=BrokerCapabilities(
            capability_profile_id=_as_text(
                capabilities_raw["capability_profile_id"], "mapping capability_profile_id"
            ),
            version=_as_int(capabilities_raw["version"], "mapping capability version"),
            venue_id=_as_text(capabilities_raw["venue_id"], "mapping capability venue_id"),
            supports_cash=_as_bool(capabilities_raw["supports_cash"], "mapping supports_cash"),
            supports_margin_buy=_as_bool(
                capabilities_raw["supports_margin_buy"], "mapping supports_margin_buy"
            ),
            supports_short_sell=_as_bool(
                capabilities_raw["supports_short_sell"], "mapping supports_short_sell"
            ),
            supports_pagination=_as_bool(
                capabilities_raw["supports_pagination"], "mapping supports_pagination"
            ),
            supports_public_stream_replay=_as_bool(
                capabilities_raw["supports_public_stream_replay"],
                "mapping supports_public_stream_replay",
            ),
            echoes_client_order_ref=_as_bool(
                capabilities_raw["echoes_client_order_ref"], "mapping echoes_client_order_ref"
            ),
            has_stable_venue_order_id=_as_bool(
                capabilities_raw["has_stable_venue_order_id"],
                "mapping has_stable_venue_order_id",
            ),
            has_stable_trade_id=_as_bool(
                capabilities_raw["has_stable_trade_id"], "mapping has_stable_trade_id"
            ),
            supports_complete_recovery_queries=_as_bool(
                capabilities_raw["supports_complete_recovery_queries"],
                "mapping supports_complete_recovery_queries",
            ),
            supports_negative_confirmation=_as_bool(
                capabilities_raw["supports_negative_confirmation"],
                "mapping supports_negative_confirmation",
            ),
            supports_paper_matching=_as_bool(
                capabilities_raw.get("supports_paper_matching", False),
                "mapping supports_paper_matching",
            ),
        ),
        recovery=RecoveryCapabilityDeclaration(
            supports_order_query=_as_bool(
                recovery_raw["supports_order_query"], "mapping recovery supports_order_query"
            ),
            supports_trade_query=_as_bool(
                recovery_raw["supports_trade_query"], "mapping recovery supports_trade_query"
            ),
            supports_working_order_query=_as_bool(
                recovery_raw["supports_working_order_query"],
                "mapping recovery supports_working_order_query",
            ),
            supports_asset_query=_as_bool(
                recovery_raw["supports_asset_query"], "mapping recovery supports_asset_query"
            ),
            supports_position_query=_as_bool(
                recovery_raw["supports_position_query"], "mapping recovery supports_position_query"
            ),
            automatic_recovery_eligible=_as_bool(
                recovery_raw["automatic_recovery_eligible"],
                "mapping recovery automatic_recovery_eligible",
            ),
            evidence_ids=frozenset(
                _as_text(item, "mapping recovery evidence_id")
                for item in _as_sequence(
                    recovery_raw["evidence_ids"], "mapping recovery evidence_ids"
                )
            ),
        ),
        scope_ownership=(
            _scope_ownership_from_mapping(scope_raw) if scope_raw is not None else None
        ),
        route_token_binding=(
            _route_token_binding_from_mapping(binding_raw) if binding_raw is not None else None
        ),
    )


def _scope_ownership_from_mapping(
    value: Mapping[str, object],
) -> AdapterClientScopeOwnership:
    return AdapterClientScopeOwnership(
        ownership_id=_as_text(value["ownership_id"], "scope ownership_id"),
        version=_as_int(value["version"], "scope ownership version"),
        venue_id=_as_text(value["venue_id"], "scope ownership venue_id"),
        account_id=_as_text(value["account_id"], "scope ownership account_id"),
        adapter_instance_id=_as_text(
            value["adapter_instance_id"], "scope ownership adapter_instance_id"
        ),
        adapter_client_scope=_as_text(
            value["adapter_client_scope"], "scope ownership adapter_client_scope"
        ),
        owner_instance_id=_as_text(value["owner_instance_id"], "scope ownership owner_instance_id"),
        config_source_hash=_as_text(
            value["config_source_hash"], "scope ownership config_source_hash"
        ),
        signer_id=_as_text(value["signer_id"], "scope ownership signer_id"),
        valid_from_wall_ns=_as_int(
            value["valid_from_wall_ns"], "scope ownership valid_from_wall_ns"
        ),
        valid_until_wall_ns=_as_int(
            value["valid_until_wall_ns"], "scope ownership valid_until_wall_ns"
        ),
        uniqueness_probe_hash=_as_text(
            value["uniqueness_probe_hash"], "scope ownership uniqueness_probe_hash"
        ),
        uniqueness_probe_succeeded=_as_bool(
            value["uniqueness_probe_succeeded"],
            "scope ownership uniqueness_probe_succeeded",
        ),
        revoked_at_wall_ns=_as_optional_int(
            value.get("revoked_at_wall_ns"), "scope ownership revoked_at_wall_ns"
        ),
    )


def _route_token_binding_from_mapping(value: Mapping[str, object]) -> RouteTokenBinding:
    def paths(field_name: str) -> tuple[str, ...]:
        return tuple(
            _as_text(item, f"route token binding {field_name}")
            for item in _as_sequence(value[field_name], f"route token binding {field_name}")
        )

    return RouteTokenBinding(
        binding_id=_as_text(value["binding_id"], "route token binding_id"),
        version=_as_int(value["version"], "route token binding version"),
        manifest_hash=_as_text(value["manifest_hash"], "route token binding manifest_hash"),
        venue_id=_as_text(value["venue_id"], "route token binding venue_id"),
        native_field_name=_as_text(
            value["native_field_name"], "route token binding native_field_name"
        ),
        native_field_type=_as_text(
            value["native_field_type"], "route token binding native_field_type"
        ),
        max_encoded_bytes=_as_int(
            value["max_encoded_bytes"], "route token binding max_encoded_bytes"
        ),
        allowed_characters=_as_text(
            value["allowed_characters"], "route token binding allowed_characters"
        ),
        write_path=_as_text(value["write_path"], "route token binding write_path"),
        order_callback_paths=paths("order_callback_paths"),
        trade_callback_paths=paths("trade_callback_paths"),
        cancel_error_paths=paths("cancel_error_paths"),
        query_paths=paths("query_paths"),
        replay_paths=paths("replay_paths"),
        normalization_version=_as_int(
            value["normalization_version"], "route token binding normalization_version"
        ),
    )


def _enum_mappings(value: object, field_name: str) -> tuple[EnumMapping, ...]:
    return tuple(
        EnumMapping(
            domain_value=_as_text(item["domain_value"], f"mapping {field_name} domain_value"),
            venue_value=_as_int(item["venue_value"], f"mapping {field_name} venue_value"),
            evidence_ids=frozenset(
                _as_text(evidence_id, f"mapping {field_name} evidence_id")
                for evidence_id in _as_sequence(
                    item["evidence_ids"], f"mapping {field_name} evidence_ids"
                )
            ),
        )
        for item in (
            _as_mapping(item, f"mapping {field_name} item")
            for item in _as_sequence(value, f"mapping {field_name}")
        )
    )


def _manifest_payload(manifest: VenueMappingManifest) -> dict[str, object]:
    """Build canonical JSON-compatible contents without dataclass implementation details."""

    payload: dict[str, object] = {
        "schema_version": manifest.schema_version,
        "manifest_id": manifest.manifest_id,
        "manifest_version": manifest.manifest_version,
        "venue_id": manifest.venue_id,
        "fingerprint": {
            "adapter_id": manifest.fingerprint.adapter_id,
            "adapter_version": manifest.fingerprint.adapter_version,
            "sdk_version": manifest.fingerprint.sdk_version,
            "adapter_package_hash": manifest.fingerprint.adapter_package_hash,
            "sdk_binary_hash": manifest.fingerprint.sdk_binary_hash,
            "binding_package_hash": manifest.fingerprint.binding_package_hash,
        },
        "effective_interval": {
            "not_before_unix_s": manifest.effective_interval.not_before_unix_s,
            "not_after_unix_s": manifest.effective_interval.not_after_unix_s,
        },
        "owner": manifest.owner,
        "evidence": [
            {
                "evidence_id": item.evidence_id,
                "source_reference": item.source_reference,
                "fixture_id": item.fixture_id,
                "test_id": item.test_id,
                "owner": item.owner,
            }
            for item in manifest.evidence
        ],
        "field_mappings": {
            name: [
                {
                    "domain_value": item.domain_value,
                    "venue_value": item.venue_value,
                    "evidence_ids": sorted(item.evidence_ids),
                }
                for item in getattr(manifest.field_mappings, name)
            ]
            for name in ("market", "side", "price_type", "business_type")
        },
        "order_routes": [
            {
                "business_type": route.business_type,
                "side": route.side,
                "required_capabilities": sorted(route.required_capabilities),
                "evidence_ids": sorted(route.evidence_ids),
            }
            for route in manifest.order_routes
        ],
        "identity": {
            "client_order_ref_field": manifest.identity.client_order_ref_field,
            "client_order_ref_format": manifest.identity.client_order_ref_format,
            "venue_order_id_field": manifest.identity.venue_order_id_field,
            "venue_order_id_format": manifest.identity.venue_order_id_format,
            "venue_cancel_id_field": manifest.identity.venue_cancel_id_field,
            "venue_cancel_id_format": manifest.identity.venue_cancel_id_format,
            "adapter_client_scope_field": manifest.identity.adapter_client_scope_field,
            "adapter_client_scope_format": manifest.identity.adapter_client_scope_format,
            "evidence_ids": sorted(manifest.identity.evidence_ids),
        },
        "capabilities": {
            "capability_profile_id": manifest.capabilities.capability_profile_id,
            "version": manifest.capabilities.version,
            "venue_id": manifest.capabilities.venue_id,
            "supports_cash": manifest.capabilities.supports_cash,
            "supports_margin_buy": manifest.capabilities.supports_margin_buy,
            "supports_short_sell": manifest.capabilities.supports_short_sell,
            "supports_pagination": manifest.capabilities.supports_pagination,
            "supports_public_stream_replay": manifest.capabilities.supports_public_stream_replay,
            "echoes_client_order_ref": manifest.capabilities.echoes_client_order_ref,
            "has_stable_venue_order_id": manifest.capabilities.has_stable_venue_order_id,
            "has_stable_trade_id": manifest.capabilities.has_stable_trade_id,
            "supports_complete_recovery_queries": (
                manifest.capabilities.supports_complete_recovery_queries
            ),
            "supports_negative_confirmation": manifest.capabilities.supports_negative_confirmation,
            "supports_paper_matching": manifest.capabilities.supports_paper_matching,
        },
        "recovery": {
            "supports_order_query": manifest.recovery.supports_order_query,
            "supports_trade_query": manifest.recovery.supports_trade_query,
            "supports_working_order_query": manifest.recovery.supports_working_order_query,
            "supports_asset_query": manifest.recovery.supports_asset_query,
            "supports_position_query": manifest.recovery.supports_position_query,
            "automatic_recovery_eligible": manifest.recovery.automatic_recovery_eligible,
            "evidence_ids": sorted(manifest.recovery.evidence_ids),
        },
    }
    ownership = manifest.scope_ownership
    if ownership is not None:
        payload["scope_ownership"] = {
            "ownership_id": ownership.ownership_id,
            "version": ownership.version,
            "venue_id": ownership.venue_id,
            "account_id": ownership.account_id,
            "adapter_instance_id": ownership.adapter_instance_id,
            "adapter_client_scope": ownership.adapter_client_scope,
            "owner_instance_id": ownership.owner_instance_id,
            "config_source_hash": ownership.config_source_hash,
            "signer_id": ownership.signer_id,
            "valid_from_wall_ns": ownership.valid_from_wall_ns,
            "valid_until_wall_ns": ownership.valid_until_wall_ns,
            "uniqueness_probe_hash": ownership.uniqueness_probe_hash,
            "uniqueness_probe_succeeded": ownership.uniqueness_probe_succeeded,
            "revoked_at_wall_ns": ownership.revoked_at_wall_ns,
        }
    binding = manifest.route_token_binding
    if binding is not None:
        payload["route_token_binding"] = {
            "binding_id": binding.binding_id,
            "version": binding.version,
            "manifest_hash": binding.manifest_hash,
            "venue_id": binding.venue_id,
            "native_field_name": binding.native_field_name,
            "native_field_type": binding.native_field_type,
            "max_encoded_bytes": binding.max_encoded_bytes,
            "allowed_characters": binding.allowed_characters,
            "write_path": binding.write_path,
            "order_callback_paths": list(binding.order_callback_paths),
            "trade_callback_paths": list(binding.trade_callback_paths),
            "cancel_error_paths": list(binding.cancel_error_paths),
            "query_paths": list(binding.query_paths),
            "replay_paths": list(binding.replay_paths),
            "normalization_version": binding.normalization_version,
        }
    return payload


def _evidence_fields(value: Mapping[str, object]) -> tuple[object, object, object, object, object]:
    return (
        value["evidence_id"],
        value["source_reference"],
        value["fixture_id"],
        value["test_id"],
        value["owner"],
    )


def _as_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise MappingManifestError(f"{field_name} must be an object")
    untyped_mapping = cast(Mapping[object, object], value)
    if not all(isinstance(key, str) for key in untyped_mapping):
        raise MappingManifestError(f"{field_name} has a non-text key")
    return cast(Mapping[str, object], untyped_mapping)


def _as_sequence(value: object, field_name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise MappingManifestError(f"{field_name} must be an array")
    return cast(Sequence[object], value)


def _as_text(value: object, field_name: str) -> str:
    try:
        return require_text(value, field_name, max_length=512)
    except DomainValidationError as error:
        raise MappingManifestError(str(error)) from error


def _as_int(value: object, field_name: str) -> int:
    try:
        return require_nonnegative(value, field_name)
    except DomainValidationError as error:
        raise MappingManifestError(str(error)) from error


def _as_optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return _as_int(value, field_name)


def _as_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise MappingManifestError(f"{field_name} must be boolean")
    return value
