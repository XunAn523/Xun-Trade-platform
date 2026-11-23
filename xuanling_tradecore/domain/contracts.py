"""Immutable M0 contracts for cursors, routing, deadlines, and recovery evidence.

All validation in this module is deterministic and side-effect free. Signature
verification, key access, clocks, Redis, filesystems, and broker SDK calls are
deliberately left to later runtime layers.
"""

from __future__ import annotations

import base64
import json
import re
import struct
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Mapping, cast

from ._validation import (
    DomainValidationError,
    freeze_texts,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)
from .model import ConsumerApiVersion, TradingDayEpoch

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_REDIS_STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
_ROUTE_TOKEN = re.compile(r"rt(?P<version>[1-9][0-9]*)_(?P<body>[A-Za-z0-9_-]+)\Z")


def _require_uint(value: object, field_name: str, maximum: int) -> int:
    result = require_nonnegative(value, field_name)
    if result > maximum:
        raise DomainValidationError(f"{field_name} exceeds its unsigned integer width")
    return result


def _require_instance(value: object, expected_type: type[object], message: str) -> None:
    if not isinstance(value, expected_type):
        raise DomainValidationError(message)


def _require_bytes(value: object, message: str) -> bytes:
    if not isinstance(value, bytes):
        raise DomainValidationError(message)
    return value


def _freeze_digest_mapping(
    value: Mapping[str, str],
    field_name: str,
    *,
    max_items: int = 32,
) -> Mapping[str, str]:
    if not value or len(value) > max_items:
        raise DomainValidationError(f"{field_name} must contain 1..{max_items} items")
    copied: dict[str, str] = {}
    for key, digest in value.items():
        canonical_key = require_text(key, f"{field_name} key", max_length=64)
        copied[canonical_key] = require_sha256(digest, f"{field_name}[{canonical_key}]")
    return cast(Mapping[str, str], MappingProxyType(copied))


@dataclass(frozen=True)
class ConsumerWireContractV2:
    """Identity of the versioned, local ConsumerGateway wire contract."""

    api_version: ConsumerApiVersion
    schema_hash: str
    golden_vectors_hash: str
    compatibility_matrix_hash: str
    header_size_bytes: int
    max_frame_bytes: int
    byte_order: str = "BIG_ENDIAN"
    json_schema_dialect: str = "https://json-schema.org/draft/2020-12/schema"

    def __post_init__(self) -> None:
        if self.api_version.major != 2:
            raise DomainValidationError("ConsumerWireContractV2 requires API major version 2")
        require_sha256(self.schema_hash, "consumer wire schema_hash")
        require_sha256(self.golden_vectors_hash, "consumer wire golden_vectors_hash")
        require_sha256(
            self.compatibility_matrix_hash,
            "consumer wire compatibility_matrix_hash",
        )
        require_positive(self.header_size_bytes, "consumer wire header_size_bytes")
        require_positive(self.max_frame_bytes, "consumer wire max_frame_bytes")
        if self.header_size_bytes >= self.max_frame_bytes:
            raise DomainValidationError("consumer wire header must be smaller than max frame")
        if self.byte_order != "BIG_ENDIAN":
            raise DomainValidationError("ConsumerWireContractV2 byte order must be BIG_ENDIAN")
        if self.json_schema_dialect != "https://json-schema.org/draft/2020-12/schema":
            raise DomainValidationError("ConsumerWireContractV2 requires JSON Schema Draft 2020-12")


@dataclass(frozen=True, order=True)
class RedisStreamIdV1:
    """Numeric Redis Stream member ID; strings never participate in ordering."""

    redis_ms: int
    redis_seq: int

    def __post_init__(self) -> None:
        _require_uint(self.redis_ms, "redis_ms", _UINT64_MAX)
        _require_uint(self.redis_seq, "redis_seq", _UINT64_MAX)

    def to_bytes(self) -> bytes:
        return struct.pack(">QQ", self.redis_ms, self.redis_seq)

    @classmethod
    def from_bytes(cls, value: object) -> RedisStreamIdV1:
        if not isinstance(value, bytes) or len(value) != 16:
            raise DomainValidationError(
                "RedisStreamIdV1 binary value must contain exactly 16 bytes"
            )
        redis_ms, redis_seq = struct.unpack(">QQ", value)
        return cls(redis_ms=redis_ms, redis_seq=redis_seq)

    @classmethod
    def parse_redis_id(cls, value: object) -> RedisStreamIdV1:
        if not isinstance(value, str):
            raise DomainValidationError("Redis Stream ID must be text")
        match = _REDIS_STREAM_ID.fullmatch(value)
        if match is None:
            raise DomainValidationError("Redis Stream ID must use canonical unsigned ms-seq form")
        return cls(redis_ms=int(match.group(1)), redis_seq=int(match.group(2)))

    def __str__(self) -> str:
        return f"{self.redis_ms}-{self.redis_seq}"


@dataclass(frozen=True, order=True)
class ConsumerEventPositionV2:
    """Numeric position of one visible fact inside one Redis envelope."""

    stream_id: RedisStreamIdV1
    fact_ordinal: int

    def __post_init__(self) -> None:
        _require_instance(
            self.stream_id,
            RedisStreamIdV1,
            "consumer event position requires RedisStreamIdV1",
        )
        _require_uint(self.fact_ordinal, "fact_ordinal", _UINT32_MAX)

    def to_bytes(self) -> bytes:
        return self.stream_id.to_bytes() + struct.pack(">I", self.fact_ordinal)

    def to_external_id(self) -> str:
        encoded = base64.urlsafe_b64encode(self.to_bytes()).rstrip(b"=").decode("ascii")
        return f"ce2_{encoded}"

    @classmethod
    def from_external_id(cls, value: object) -> ConsumerEventPositionV2:
        if not isinstance(value, str) or not value.startswith("ce2_"):
            raise DomainValidationError("consumer event position must use ce2_ canonical encoding")
        encoded = value[4:]
        if not encoded or "=" in encoded:
            raise DomainValidationError("consumer event position is not canonical base64url")
        try:
            padding = "=" * (-len(encoded) % 4)
            raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        except (ValueError, UnicodeError) as exc:
            raise DomainValidationError(
                "consumer event position contains invalid base64url"
            ) from exc
        if len(raw) != 20:
            raise DomainValidationError("consumer event position must contain exactly 20 bytes")
        position = cls(
            stream_id=RedisStreamIdV1.from_bytes(raw[:16]),
            fact_ordinal=struct.unpack(">I", raw[16:])[0],
        )
        if position.to_external_id() != value:
            raise DomainValidationError("consumer event position is not canonical")
        return position


@dataclass(frozen=True)
class ConsumerEventCursorV2:
    """Unsigned cursor claims bound to one consumer, account, Journal, and key epoch."""

    consumer_id: str
    consumer_account_id: str
    journal_id: str
    stream_generation: int
    after_position: ConsumerEventPositionV2 | None
    authorization_epoch: int
    issued_at_wall_ns: int
    expires_at_wall_ns: int
    cursor_key_id: str
    schema_version: int = 2
    high_watermark_position: ConsumerEventPositionV2 | None = None

    def __post_init__(self) -> None:
        require_text(self.consumer_id, "cursor consumer_id")
        require_text(self.consumer_account_id, "cursor consumer_account_id")
        require_text(self.journal_id, "cursor journal_id")
        require_positive(self.stream_generation, "cursor stream_generation")
        require_positive(self.authorization_epoch, "cursor authorization_epoch")
        require_nonnegative(self.issued_at_wall_ns, "cursor issued_at_wall_ns")
        require_positive(self.expires_at_wall_ns, "cursor expires_at_wall_ns")
        require_text(self.cursor_key_id, "cursor_key_id", max_length=64)
        if self.schema_version != 2:
            raise DomainValidationError("ConsumerEventCursorV2 schema_version must be 2")
        if self.after_position is not None:
            _require_instance(
                self.after_position,
                ConsumerEventPositionV2,
                "cursor after_position must be BEGIN or ConsumerEventPositionV2",
            )
        if self.high_watermark_position is not None:
            _require_instance(
                self.high_watermark_position,
                ConsumerEventPositionV2,
                "cursor high_watermark_position must be ConsumerEventPositionV2",
            )
            if (
                self.after_position is not None
                and self.after_position > self.high_watermark_position
            ):
                raise DomainValidationError("cursor after_position exceeds high_watermark_position")
        if self.expires_at_wall_ns <= self.issued_at_wall_ns:
            raise DomainValidationError("cursor expiry must be after issue time")

    @property
    def is_begin(self) -> bool:
        return self.after_position is None


@dataclass(frozen=True)
class ExecutionDeadlinePolicy:
    """Versioned native-start budget for a new execution instruction."""

    policy_id: str
    version: int
    policy_hash: str
    native_start_budget_ns: int

    def __post_init__(self) -> None:
        require_text(self.policy_id, "execution deadline policy_id")
        require_positive(self.version, "execution deadline policy version")
        require_sha256(self.policy_hash, "execution deadline policy_hash")
        require_positive(self.native_start_budget_ns, "native_start_budget_ns")

    def deadline_for(self, admitted_monotonic_ns: int) -> int:
        admitted = require_nonnegative(admitted_monotonic_ns, "admitted_monotonic_ns")
        deadline = admitted + self.native_start_budget_ns
        if deadline > _UINT64_MAX:
            raise DomainValidationError("execution deadline overflows uint64")
        return deadline


@dataclass(frozen=True)
class CancelDeadlinePolicy:
    """Versioned native-start budget for a cancellation instruction."""

    policy_id: str
    version: int
    policy_hash: str
    native_start_budget_ns: int

    def __post_init__(self) -> None:
        require_text(self.policy_id, "cancel deadline policy_id")
        require_positive(self.version, "cancel deadline policy version")
        require_sha256(self.policy_hash, "cancel deadline policy_hash")
        require_positive(self.native_start_budget_ns, "cancel native_start_budget_ns")

    def deadline_for(self, admitted_monotonic_ns: int) -> int:
        admitted = require_nonnegative(admitted_monotonic_ns, "admitted_monotonic_ns")
        deadline = admitted + self.native_start_budget_ns
        if deadline > _UINT64_MAX:
            raise DomainValidationError("cancel deadline overflows uint64")
        return deadline


@dataclass(frozen=True)
class AdapterClientScopeOwnership:
    """Signed ownership claims for one adapter client-reference namespace."""

    ownership_id: str
    version: int
    venue_id: str
    account_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    owner_instance_id: str
    config_source_hash: str
    signer_id: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    uniqueness_probe_hash: str
    uniqueness_probe_succeeded: bool
    revoked_at_wall_ns: int | None = None

    def __post_init__(self) -> None:
        require_text(self.ownership_id, "scope ownership_id")
        require_positive(self.version, "scope ownership version")
        for field_name, value in (
            ("venue_id", self.venue_id),
            ("account_id", self.account_id),
            ("adapter_instance_id", self.adapter_instance_id),
            ("adapter_client_scope", self.adapter_client_scope),
            ("owner_instance_id", self.owner_instance_id),
            ("signer_id", self.signer_id),
        ):
            require_text(value, f"scope ownership {field_name}")
        require_sha256(self.config_source_hash, "scope ownership config_source_hash")
        require_sha256(self.uniqueness_probe_hash, "scope ownership uniqueness_probe_hash")
        require_nonnegative(self.valid_from_wall_ns, "scope ownership valid_from_wall_ns")
        require_positive(self.valid_until_wall_ns, "scope ownership valid_until_wall_ns")
        if self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise DomainValidationError("scope ownership validity interval is empty")
        if self.revoked_at_wall_ns is not None:
            require_nonnegative(self.revoked_at_wall_ns, "scope ownership revoked_at_wall_ns")

    def is_active_at(self, wall_ns: int) -> bool:
        now = require_nonnegative(wall_ns, "scope ownership evaluation time")
        not_revoked = self.revoked_at_wall_ns is None or now < self.revoked_at_wall_ns
        return (
            self.uniqueness_probe_succeeded
            and self.valid_from_wall_ns <= now < self.valid_until_wall_ns
            and not_revoked
        )


@dataclass(frozen=True)
class AdapterRouteToken:
    """Opaque core-generated token; its binary payload cannot carry business text."""

    version: int
    token_bytes: bytes

    def __post_init__(self) -> None:
        require_positive(self.version, "adapter route token version")
        token_bytes = _require_bytes(
            self.token_bytes,
            "adapter route token must contain 16..32 opaque bytes",
        )
        if not 16 <= len(token_bytes) <= 32:
            raise DomainValidationError("adapter route token must contain 16..32 opaque bytes")
        if not any(self.token_bytes):
            raise DomainValidationError("adapter route token cannot be all zero")

    def to_text(self) -> str:
        body = base64.urlsafe_b64encode(self.token_bytes).rstrip(b"=").decode("ascii")
        return f"rt{self.version}_{body}"

    @classmethod
    def parse(cls, value: object) -> AdapterRouteToken:
        if not isinstance(value, str):
            raise DomainValidationError("adapter route token must be text")
        match = _ROUTE_TOKEN.fullmatch(value)
        if match is None or "=" in value:
            raise DomainValidationError("adapter route token is not canonical")
        body = match.group("body")
        try:
            raw = base64.b64decode(body + "=" * (-len(body) % 4), altchars=b"-_", validate=True)
        except (ValueError, UnicodeError) as exc:
            raise DomainValidationError("adapter route token contains invalid base64url") from exc
        token = cls(version=int(match.group("version")), token_bytes=raw)
        if token.to_text() != value:
            raise DomainValidationError("adapter route token is not canonical")
        return token


@dataclass(frozen=True)
class RouteTokenBinding:
    """Versioned encode/write/echo/normalize contract for one Adapter profile."""

    binding_id: str
    version: int
    manifest_hash: str
    venue_id: str
    native_field_name: str
    native_field_type: str
    max_encoded_bytes: int
    allowed_characters: str
    write_path: str
    order_callback_paths: tuple[str, ...]
    trade_callback_paths: tuple[str, ...]
    cancel_error_paths: tuple[str, ...]
    query_paths: tuple[str, ...]
    replay_paths: tuple[str, ...]
    normalization_version: int

    def __post_init__(self) -> None:
        require_text(self.binding_id, "route token binding_id")
        require_positive(self.version, "route token binding version")
        require_sha256(self.manifest_hash, "route token binding manifest_hash")
        require_text(self.venue_id, "route token binding venue_id")
        require_text(self.native_field_name, "route token native_field_name", max_length=64)
        require_text(self.native_field_type, "route token native_field_type", max_length=64)
        require_positive(self.max_encoded_bytes, "route token max_encoded_bytes")
        pattern = require_text(
            self.allowed_characters,
            "route token allowed_characters",
            max_length=128,
        )
        try:
            re.compile(pattern)
        except re.error as exc:
            raise DomainValidationError("route token allowed_characters is not a regex") from exc
        require_text(self.write_path, "route token write_path")
        for field_name, paths in (
            ("order_callback_paths", self.order_callback_paths),
            ("trade_callback_paths", self.trade_callback_paths),
            ("cancel_error_paths", self.cancel_error_paths),
            ("query_paths", self.query_paths),
            ("replay_paths", self.replay_paths),
        ):
            frozen = tuple(require_text(path, field_name) for path in paths)
            object.__setattr__(self, field_name, frozen)
        require_positive(self.normalization_version, "route token normalization_version")

    @property
    def supports_automatic_routing(self) -> bool:
        return all(
            (
                self.order_callback_paths,
                self.trade_callback_paths,
                self.cancel_error_paths,
                self.query_paths,
                self.replay_paths,
            )
        )


@dataclass(frozen=True)
class RouteDirectoryCapacityPlan:
    """Signed capacity boundary that stops only new CALL_STARTED creation."""

    plan_id: str
    version: int
    plan_hash: str
    max_active_entries: int
    max_tombstone_entries: int
    max_total_bytes: int
    warn_basis_points: int
    stop_new_basis_points: int
    max_gc_lag_seconds: int
    minimum_client_ref_headroom: int

    def __post_init__(self) -> None:
        require_text(self.plan_id, "route capacity plan_id")
        require_positive(self.version, "route capacity plan version")
        require_sha256(self.plan_hash, "route capacity plan_hash")
        require_positive(self.max_active_entries, "max_active_entries")
        require_positive(self.max_tombstone_entries, "max_tombstone_entries")
        require_positive(self.max_total_bytes, "max_total_bytes")
        warn = require_positive(self.warn_basis_points, "warn_basis_points")
        stop = require_positive(self.stop_new_basis_points, "stop_new_basis_points")
        if not warn < stop <= 10_000:
            raise DomainValidationError("capacity watermarks must satisfy warn < stop_new <= 10000")
        require_nonnegative(self.max_gc_lag_seconds, "max_gc_lag_seconds")
        require_positive(self.minimum_client_ref_headroom, "minimum_client_ref_headroom")
        if self.minimum_client_ref_headroom > _UINT32_MAX:
            raise DomainValidationError("client ref headroom exceeds uint32")

    def utilization_basis_points(self, *, active: int, tombstones: int) -> int:
        active_count = require_nonnegative(active, "active route count")
        tombstone_count = require_nonnegative(tombstones, "tombstone route count")
        active_ratio = active_count * 10_000 // self.max_active_entries
        tombstone_ratio = tombstone_count * 10_000 // self.max_tombstone_entries
        return max(active_ratio, tombstone_ratio)

    def stop_new_required(
        self,
        *,
        active: int,
        tombstones: int,
        total_bytes: int,
        gc_lag_seconds: int,
        client_ref_headroom: int,
    ) -> bool:
        used_bytes = require_nonnegative(total_bytes, "route total_bytes")
        gc_lag = require_nonnegative(gc_lag_seconds, "route gc_lag_seconds")
        headroom = require_nonnegative(client_ref_headroom, "route client_ref_headroom")
        byte_ratio = used_bytes * 10_000 // self.max_total_bytes
        return (
            max(self.utilization_basis_points(active=active, tombstones=tombstones), byte_ratio)
            >= self.stop_new_basis_points
            or gc_lag > self.max_gc_lag_seconds
            or headroom < self.minimum_client_ref_headroom
        )


@dataclass(frozen=True)
class RecoveryEvidenceProfile:
    """Inputs a profile must prove before automatic recovery is eligible."""

    profile_id: str
    version: int
    profile_hash: str
    required_query_kinds: frozenset[str]
    requires_public_replay: bool
    requires_callback_gap_coverage: bool
    requires_route_token_echo: bool
    requires_negative_confirmation: bool
    requires_stable_double_scan_without_as_of: bool
    max_snapshot_age_ns: int

    def __post_init__(self) -> None:
        require_text(self.profile_id, "recovery evidence profile_id")
        require_positive(self.version, "recovery evidence profile version")
        require_sha256(self.profile_hash, "recovery evidence profile_hash")
        query_kinds = freeze_texts(
            self.required_query_kinds,
            "required_query_kinds",
            max_items=32,
            max_length=64,
        )
        if not query_kinds:
            raise DomainValidationError("recovery evidence profile requires query kinds")
        object.__setattr__(self, "required_query_kinds", query_kinds)
        require_positive(self.max_snapshot_age_ns, "recovery max_snapshot_age_ns")


@dataclass(frozen=True)
class RecoveryCompletenessCertificate:
    """Hash-addressed proof summary for one bounded broker snapshot window."""

    certificate_id: str
    certificate_hash: str
    profile_id: str
    profile_hash: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    trading_day: TradingDayEpoch
    query_digests: Mapping[str, str]
    callback_replay_complete: bool
    callback_gaps_closed: bool
    route_token_echo_complete: bool
    negative_confirmation_available: bool
    snapshot_window_stable: bool
    captured_at_wall_ns: int
    completed_at_wall_ns: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("certificate_id", self.certificate_id),
            ("profile_id", self.profile_id),
            ("account_id", self.account_id),
            ("venue_id", self.venue_id),
            ("adapter_instance_id", self.adapter_instance_id),
            ("adapter_client_scope", self.adapter_client_scope),
        ):
            require_text(value, f"recovery certificate {field_name}")
        require_sha256(self.certificate_hash, "recovery certificate_hash")
        require_sha256(self.profile_hash, "recovery certificate profile_hash")
        object.__setattr__(
            self,
            "query_digests",
            _freeze_digest_mapping(self.query_digests, "recovery query_digests"),
        )
        require_nonnegative(self.captured_at_wall_ns, "certificate captured_at_wall_ns")
        require_positive(self.completed_at_wall_ns, "certificate completed_at_wall_ns")
        if self.completed_at_wall_ns < self.captured_at_wall_ns:
            raise DomainValidationError("recovery certificate completion precedes capture")

    def incompleteness_reasons(self, profile: RecoveryEvidenceProfile) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.profile_id != profile.profile_id or self.profile_hash != profile.profile_hash:
            reasons.append("PROFILE_MISMATCH")
        if not profile.required_query_kinds.issubset(self.query_digests):
            reasons.append("QUERY_INCOMPLETE")
        if profile.requires_public_replay and not self.callback_replay_complete:
            reasons.append("CALLBACK_REPLAY_INCOMPLETE")
        if profile.requires_callback_gap_coverage and not self.callback_gaps_closed:
            reasons.append("CALLBACK_GAP_UNCLOSED")
        if profile.requires_route_token_echo and not self.route_token_echo_complete:
            reasons.append("ROUTE_TOKEN_ECHO_INCOMPLETE")
        if profile.requires_negative_confirmation and not self.negative_confirmation_available:
            reasons.append("NEGATIVE_CONFIRMATION_UNAVAILABLE")
        if profile.requires_stable_double_scan_without_as_of and not self.snapshot_window_stable:
            reasons.append("WINDOW_UNSTABLE")
        if self.completed_at_wall_ns - self.captured_at_wall_ns > profile.max_snapshot_age_ns:
            reasons.append("SNAPSHOT_TOO_OLD")
        return tuple(sorted(reasons))

    @property
    def content_sha256(self) -> str:
        """Hash every certificate claim except the hash field itself."""

        payload = {
            "schema_version": 1,
            "certificate_id": self.certificate_id,
            "profile_id": self.profile_id,
            "profile_hash": self.profile_hash,
            "account_id": self.account_id,
            "venue_id": self.venue_id,
            "adapter_instance_id": self.adapter_instance_id,
            "adapter_client_scope": self.adapter_client_scope,
            "trading_day": self.trading_day.value,
            "query_digests": dict(self.query_digests),
            "callback_replay_complete": self.callback_replay_complete,
            "callback_gaps_closed": self.callback_gaps_closed,
            "route_token_echo_complete": self.route_token_echo_complete,
            "negative_confirmation_available": self.negative_confirmation_available,
            "snapshot_window_stable": self.snapshot_window_stable,
            "captured_at_wall_ns": self.captured_at_wall_ns,
            "completed_at_wall_ns": self.completed_at_wall_ns,
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RecoveryAutoResolutionEligibility:
    """The unique deterministic decision produced from recovery evidence inputs."""

    eligible: bool
    reason_codes: tuple[str, ...]
    certificate_hash: str
    eligibility_input_hash: str

    def __post_init__(self) -> None:
        reasons = tuple(
            sorted(
                require_text(reason, "eligibility reason", max_length=64)
                for reason in self.reason_codes
            )
        )
        if len(reasons) != len(set(reasons)):
            raise DomainValidationError("eligibility reasons must be unique")
        if self.eligible and reasons:
            raise DomainValidationError("eligible recovery cannot contain rejection reasons")
        if not self.eligible and not reasons:
            raise DomainValidationError("ineligible recovery requires at least one reason")
        require_sha256(self.certificate_hash, "eligibility certificate_hash")
        require_sha256(self.eligibility_input_hash, "eligibility_input_hash")
        object.__setattr__(self, "reason_codes", reasons)

    @classmethod
    def evaluate(
        cls,
        *,
        profile: RecoveryEvidenceProfile,
        certificate: RecoveryCompletenessCertificate,
        scope_ownership_valid: bool,
        route_directory_conflict: bool,
        eligibility_input_hash: str,
    ) -> RecoveryAutoResolutionEligibility:
        reasons = list(certificate.incompleteness_reasons(profile))
        if not scope_ownership_valid:
            reasons.append("SCOPE_OWNERSHIP_INVALID")
        if route_directory_conflict:
            reasons.append("ROUTE_DIRECTORY_CONFLICT")
        unique_reasons = tuple(sorted(set(reasons)))
        return cls(
            eligible=not unique_reasons,
            reason_codes=unique_reasons,
            certificate_hash=certificate.certificate_hash,
            eligibility_input_hash=eligibility_input_hash,
        )
