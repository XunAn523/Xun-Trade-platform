"""PostgreSQL repository implementations for Provisioning Control Plane V1.

The control plane is intentionally outside the OMS/EMS/Consumer execution
graph.  These repositories open a short PostgreSQL transaction per control
mutation; no runtime dispatch or order hot path imports this module.

The SQL is deliberately expressed against the reviewed
``xuanling_tradecore_cp`` migration rather than creating tables at application
startup.  A dedicated migrator applies DDL; the service account needs only
least-privilege DML rights.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import replace
from typing import Generator, Protocol, cast

from ..domain._validation import require_nonnegative, require_sha256, require_text
from .provisioning_approval_v1 import (
    ProvisioningApprovalActionV1,
    ProvisioningApprovalRegistryV1,
    ProvisioningApprovalV1,
)
from .provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    BindingActivationSpecV1,
    ConsumerBindingRevisionV1,
    ControlPrincipalV1,
    ControlRoleV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
    ProvisioningErrorV1,
    SecretReferenceV1,
)
from .provisioning_registry_v1 import (
    ActiveProfileBindingV1,
    ControlRequestLedgerEntryV1,
    ProvisioningRegistryV1,
)

_SCHEMA_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class _CursorV1(Protocol):
    def execute(self, query: str, params: tuple[object, ...] = ()) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def __enter__(self) -> "_CursorV1": ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


class _ConnectionV1(Protocol):
    def cursor(self) -> _CursorV1: ...

    def __enter__(self) -> "_ConnectionV1": ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


class PostgresProvisioningRegistryV1(ProvisioningRegistryV1):
    """Transactional durable equivalent of ``InMemoryProvisioningRegistryV1``.

    A transaction-scoped advisory lock fences one profile's revision sequence;
    unique keys then fence adapter instance IDs, JWE usage and binding IDs
    across service restarts.  Errors deliberately reveal no DSN, account,
    endpoint, JWE or SQL detail.
    """

    def __init__(self, *, dsn: str, schema: str = "xuanling_tradecore_cp") -> None:
        self._dsn = require_text(dsn, "control repository DSN", max_length=4_096)
        if not _SCHEMA_PATTERN.fullmatch(schema):
            raise ProvisioningErrorV1("CONTROL_REPOSITORY", "control repository schema is invalid")
        self._schema = schema

    def get_request(
        self, *, request_id: str, payload_digest: str
    ) -> ControlRequestLedgerEntryV1 | None:
        require_text(request_id, "control request ID", max_length=128)
        require_sha256(payload_digest, "control payload digest")
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT l.request_id::text, l.payload_digest, l.profile_id, "
                f"l.profile_revision, r.state, "
                f"NULLIF(l.result_code, '') "
                f"FROM {self._schema}.control_request_ledger l "
                f"JOIN {self._schema}.adapter_profile_revisions r "
                f"ON (r.profile_id = l.profile_id AND r.profile_revision = l.profile_revision) "
                "WHERE l.request_id = %s",
                (request_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        stored_digest = _row_text(row[1], "control payload digest")
        if stored_digest != payload_digest:
            raise ProvisioningErrorV1(
                "CONTROL_IDEMPOTENCY_CONFLICT", "control request has another payload"
            )
        epoch_raw = row[5]
        return ControlRequestLedgerEntryV1(
            request_id=_row_text(row[0], "control request ID"),
            payload_digest=stored_digest,
            profile_id=_row_text(row[2], "control profile ID"),
            revision=_row_positive_int(row[3], "control profile revision"),
            state=_profile_state(row[4]),
            authorization_epoch=_epoch_from_result_code(epoch_raw),
        )

    def claim_jti(self, *, jti: str, target: str, request_id: str, envelope_digest: str) -> None:
        require_text(jti, "credential jti", max_length=128)
        require_text(target, "credential target", max_length=256)
        require_text(request_id, "control request ID", max_length=128)
        require_sha256(envelope_digest, "credential envelope digest")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT envelope_digest FROM {self._schema}.consumed_jwe_ids "
                    "WHERE recipient_target = %s AND jti = %s FOR UPDATE",
                    (target, jti),
                )
                row = cursor.fetchone()
                if row is not None:
                    if _row_text(row[0], "credential envelope digest") != envelope_digest:
                        raise ProvisioningErrorV1(
                            "CONTROL_ENVELOPE_REPLAY", "credential envelope replay"
                        )
                    # Exact same JWE is intentionally allowed to resume the
                    # request after a crash before profile persistence.
                    return
                cursor.execute(
                    f"INSERT INTO {self._schema}.consumed_jwe_ids "
                    "(recipient_target, jti, envelope_digest, expires_at, consumed_at) "
                    "VALUES (%s, %s, %s, clock_timestamp() + interval '5 minutes', clock_timestamp())",
                    (target, jti, envelope_digest),
                )
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def create_prepared(
        self,
        *,
        request_id: str,
        payload_digest: str,
        profile: AdapterProfileRevisionV1,
    ) -> AdapterProfileRevisionV1:
        if profile.state is not ProfileStateV1.PREPARED:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "new profile is not prepared")
        identity = profile.identity
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                self._lock_profile(cursor, identity.profile_id)
                prior = self._request_row(cursor, request_id)
                if prior is not None:
                    self._require_digest(prior, payload_digest)
                    return self._get_revision_cursor(
                        cursor,
                        profile_id=_row_text(prior[2], "control profile ID"),
                        revision=_row_positive_int(prior[3], "control profile revision"),
                    )
                cursor.execute(
                    f"SELECT current_revision FROM {self._schema}.adapter_profiles "
                    "WHERE profile_id = %s FOR UPDATE",
                    (identity.profile_id,),
                )
                existing = cursor.fetchone()
                expected_revision = (
                    1 if existing is None else _row_positive_int(existing[0], "current revision") + 1
                )
                if profile.revision != expected_revision:
                    raise ProvisioningErrorV1(
                        "CONTROL_PROFILE", "profile revision is not monotonic"
                    )
                if existing is None:
                    cursor.execute(
                        f"INSERT INTO {self._schema}.adapter_profiles "
                        "(profile_id, adapter_kind, venue_id, account_alias, current_revision, lifecycle, "
                        "created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, "
                        "to_timestamp(%s / 1000000000.0), to_timestamp(%s / 1000000000.0))",
                        (
                            identity.profile_id,
                            identity.adapter_kind.value,
                            identity.venue_id,
                            identity.account_alias,
                            profile.revision,
                            profile.state.value,
                            profile.created_wall_ns,
                            profile.created_wall_ns,
                        ),
                    )
                else:
                    cursor.execute(
                        f"UPDATE {self._schema}.adapter_profiles "
                        "SET current_revision = %s, lifecycle = %s, updated_at = to_timestamp(%s / 1000000000.0) "
                        "WHERE profile_id = %s",
                        (profile.revision, profile.state.value, profile.created_wall_ns, identity.profile_id),
                    )
                self._insert_profile_revision(cursor, profile)
                self._insert_request(cursor, request_id=request_id, payload_digest=payload_digest, profile=profile)
                return profile
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def get_revision(self, *, profile_id: str, revision: int) -> AdapterProfileRevisionV1:
        require_text(profile_id, "profile ID", max_length=128)
        if revision <= 0:
            raise ProvisioningErrorV1("CONTROL_PROFILE", "profile revision is invalid")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                return self._get_revision_cursor(cursor, profile_id=profile_id, revision=revision)
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

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
        require_text(request_id, "control request ID", max_length=128)
        require_sha256(payload_digest, "control payload digest")
        require_text(profile_id, "profile ID", max_length=128)
        if revision <= 0:
            raise ProvisioningErrorV1("CONTROL_PROFILE", "profile revision is invalid")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                self._lock_profile(cursor, profile_id)
                prior = self._request_row(cursor, request_id)
                if prior is not None:
                    self._require_digest(prior, payload_digest)
                    return self._get_revision_cursor(
                        cursor,
                        profile_id=_row_text(prior[2], "control profile ID"),
                        revision=_row_positive_int(prior[3], "control profile revision"),
                    )
                current = self._get_revision_cursor(cursor, profile_id=profile_id, revision=revision)
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
                cursor.execute(
                    f"UPDATE {self._schema}.adapter_profile_revisions SET "
                    "state = %s, validation_evidence_id = %s, approval_id = %s, "
                    "retired_at = CASE WHEN %s::bigint IS NULL THEN NULL "
                    "ELSE to_timestamp(%s / 1000000000.0) END, retired_wall_ns = %s "
                    "WHERE profile_id = %s AND profile_revision = %s",
                    (
                        target_state.value,
                        validation_evidence_id,
                        approval_id,
                        retired_wall_ns,
                        retired_wall_ns,
                        retired_wall_ns,
                        profile_id,
                        revision,
                    ),
                )
                cursor.execute(
                    f"UPDATE {self._schema}.adapter_profiles SET lifecycle = %s, "
                    "updated_at = clock_timestamp() WHERE profile_id = %s",
                    (target_state.value, profile_id),
                )
                self._insert_request(cursor, request_id=request_id, payload_digest=payload_digest, profile=updated,
                                     authorization_epoch=authorization_epoch)
                return updated
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def publish_binding(
        self,
        *,
        profile: AdapterProfileRevisionV1,
        spec: BindingActivationSpecV1,
        binding_revision_id: str,
        effective_wall_ns: int,
    ) -> ConsumerBindingRevisionV1:
        require_text(binding_revision_id, "binding revision ID", max_length=256)
        require_nonnegative(effective_wall_ns, "binding effective time")
        if profile.state is not ProfileStateV1.VALIDATED:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "only validated profile can bind")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                self._lock_profile(cursor, profile.identity.profile_id)
                self._lock_binding_epoch(cursor)
                cursor.execute(
                    f"SELECT binding_revision_id, profile_id, profile_revision, account_id, consumer_id, "
                    "consumer_account_id, account_display_name, strategy_ids, authorization_epoch, "
                    "effective_wall_ns, retired_wall_ns "
                    f"FROM {self._schema}.consumer_binding_revisions WHERE binding_revision_id = %s FOR UPDATE",
                    (binding_revision_id,),
                )
                row = cursor.fetchone()
                if row is not None:
                    prior = _binding_from_row(row)
                    if _same_live_binding(prior, profile, spec):
                        return prior
                    raise ProvisioningErrorV1("CONTROL_BINDING", "binding revision ID already exists")
                cursor.execute(
                    f"SELECT 1 FROM {self._schema}.consumer_binding_revisions "
                    "WHERE consumer_id = %s AND consumer_account_id = %s AND retired_at IS NULL FOR UPDATE",
                    (spec.consumer_id, spec.consumer_account_id),
                )
                if cursor.fetchone() is not None:
                    raise ProvisioningErrorV1(
                        "CONTROL_BINDING", "consumer account binding is already active"
                    )
                cursor.execute(
                    f"SELECT COALESCE(MAX(authorization_epoch), 1) "
                    f"FROM {self._schema}.consumer_binding_revisions"
                )
                epoch_row = cursor.fetchone()
                if epoch_row is None:
                    raise ProvisioningErrorV1("CONTROL_REPOSITORY", "binding epoch is unavailable")
                authorization_epoch = _row_positive_int(epoch_row[0], "authorization epoch") + 1
                cursor.execute(
                    f"INSERT INTO {self._schema}.consumer_binding_revisions "
                    "(binding_revision_id, profile_id, profile_revision, account_id, consumer_id, "
                    "consumer_account_id, account_display_name, strategy_ids, authorization_epoch, effective_at, "
                    "effective_wall_ns) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, to_timestamp(%s / 1000000000.0), %s)",
                    (
                        binding_revision_id,
                        profile.identity.profile_id,
                        profile.revision,
                        spec.account_id,
                        spec.consumer_id,
                        spec.consumer_account_id,
                        spec.account_display_name,
                        _json_array(spec.strategy_ids),
                        authorization_epoch,
                        effective_wall_ns,
                        effective_wall_ns,
                    ),
                )
                return ConsumerBindingRevisionV1(
                    binding_revision_id=binding_revision_id,
                    profile_id=profile.identity.profile_id,
                    profile_revision=profile.revision,
                    account_id=spec.account_id,
                    consumer_id=spec.consumer_id,
                    consumer_account_id=spec.consumer_account_id,
                    account_display_name=spec.account_display_name,
                    strategy_ids=spec.strategy_ids,
                    authorization_epoch=authorization_epoch,
                    effective_wall_ns=effective_wall_ns,
                )
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def retire_binding(
        self, *, profile_id: str, revision: int, retired_wall_ns: int
    ) -> tuple[ConsumerBindingRevisionV1, ...]:
        require_nonnegative(retired_wall_ns, "binding retirement time")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                self._lock_profile(cursor, profile_id)
                self._lock_binding_epoch(cursor)
                cursor.execute(
                    f"SELECT binding_revision_id, profile_id, profile_revision, account_id, consumer_id, "
                    "consumer_account_id, account_display_name, strategy_ids, authorization_epoch, "
                    "effective_wall_ns, retired_wall_ns "
                    f"FROM {self._schema}.consumer_binding_revisions "
                    "WHERE profile_id = %s AND profile_revision = %s AND retired_at IS NULL FOR UPDATE",
                    (profile_id, revision),
                )
                active = tuple(_binding_from_row(row) for row in cursor.fetchall())
                if not active:
                    return ()
                cursor.execute(
                    f"SELECT COALESCE(MAX(authorization_epoch), 1) "
                    f"FROM {self._schema}.consumer_binding_revisions"
                )
                epoch_row = cursor.fetchone()
                if epoch_row is None:
                    raise ProvisioningErrorV1("CONTROL_REPOSITORY", "binding epoch is unavailable")
                successor_epoch = _row_positive_int(epoch_row[0], "authorization epoch") + 1
                cursor.execute(
                    f"UPDATE {self._schema}.consumer_binding_revisions "
                    "SET retired_at = to_timestamp(%s / 1000000000.0), retired_wall_ns = %s, "
                    "authorization_epoch = %s "
                    "WHERE profile_id = %s AND profile_revision = %s AND retired_at IS NULL",
                    (retired_wall_ns, retired_wall_ns, successor_epoch, profile_id, revision),
                )
                return tuple(
                    replace(
                        item,
                        authorization_epoch=successor_epoch,
                        retired_wall_ns=retired_wall_ns,
                    )
                    for item in active
                )
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def active_bindings(self) -> tuple[ConsumerBindingRevisionV1, ...]:
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT binding_revision_id, profile_id, profile_revision, account_id, consumer_id, "
                    "consumer_account_id, account_display_name, strategy_ids, authorization_epoch, "
                    "effective_wall_ns, retired_wall_ns "
                    f"FROM {self._schema}.consumer_binding_revisions WHERE retired_at IS NULL "
                    "ORDER BY authorization_epoch"
                )
                return tuple(_binding_from_row(row) for row in cursor.fetchall())
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def active_binding_and_profile(
        self, *, binding_revision_id: str, now_wall_ns: int
    ) -> ActiveProfileBindingV1:
        """Read an exact current profile/binding in a single DB snapshot.

        This control-plane read is off the trading path. Runtime must recheck
        retirement/expiry and latch closed on stale refresh; this read alone
        does not authorize a broker call or prove recovery.
        """

        require_text(binding_revision_id, "binding revision ID", max_length=256)
        require_nonnegative(now_wall_ns, "binding evaluation time")
        try:
            with self.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT b.binding_revision_id, b.profile_id, b.profile_revision, b.account_id, "
                    "b.consumer_id, b.consumer_account_id, b.account_display_name, b.strategy_ids, "
                    "b.authorization_epoch, b.effective_wall_ns, b.retired_wall_ns, "
                    "p.profile_id, p.adapter_kind, p.venue_id, r.adapter_instance_id, p.account_alias, "
                    "r.profile_revision, r.state, r.secret_provider_kind, r.secret_reference_id, r.secret_target, "
                    "r.recipient_kid, r.credential_digest, r.endpoint_digest, r.envelope_digest, "
                    "r.mapping_manifest_hash, r.capability_manifest_hash, r.release_manifest_hash, "
                    "r.created_wall_ns, r.validation_evidence_id, r.approval_id, r.retired_wall_ns "
                    f"FROM {self._schema}.consumer_binding_revisions b "
                    f"JOIN {self._schema}.adapter_profiles p ON p.profile_id = b.profile_id "
                    f"JOIN {self._schema}.adapter_profile_revisions r "
                    "ON r.profile_id = b.profile_id AND r.profile_revision = b.profile_revision "
                    "WHERE b.binding_revision_id = %s AND b.retired_at IS NULL "
                    "AND b.effective_wall_ns <= %s AND p.current_revision = r.profile_revision "
                    "AND p.lifecycle = %s AND r.state = %s",
                    (binding_revision_id, now_wall_ns, "ACTIVE_DISARMED", "ACTIVE_DISARMED"),
                )
                row = cursor.fetchone()
            if row is None:
                raise ProvisioningErrorV1("CONTROL_BINDING", "active binding is unavailable")
            return ActiveProfileBindingV1(_profile_from_row(row[11:]), _binding_from_row(row[:11]))
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def _get_revision_cursor(
        self, cursor: _CursorV1, *, profile_id: str, revision: int
    ) -> AdapterProfileRevisionV1:
        cursor.execute(
            f"SELECT p.profile_id, p.adapter_kind, p.venue_id, r.adapter_instance_id, p.account_alias, "
            "r.profile_revision, r.state, r.secret_provider_kind, r.secret_reference_id, r.secret_target, "
            "r.recipient_kid, r.credential_digest, r.endpoint_digest, r.envelope_digest, "
            "r.mapping_manifest_hash, r.capability_manifest_hash, r.release_manifest_hash, "
            "r.created_wall_ns, "
            "r.validation_evidence_id, r.approval_id, "
            "r.retired_wall_ns "
            f"FROM {self._schema}.adapter_profile_revisions r "
            f"JOIN {self._schema}.adapter_profiles p ON p.profile_id = r.profile_id "
            "WHERE r.profile_id = %s AND r.profile_revision = %s",
            (profile_id, revision),
        )
        row = cursor.fetchone()
        if row is None:
            raise ProvisioningErrorV1("CONTROL_NOT_FOUND", "profile revision was not found")
        return _profile_from_row(row)

    def _insert_profile_revision(self, cursor: _CursorV1, profile: AdapterProfileRevisionV1) -> None:
        identity = profile.identity
        digest = profile.material_digest
        reference = profile.secret_reference
        cursor.execute(
            f"INSERT INTO {self._schema}.adapter_profile_revisions "
            "(profile_id, profile_revision, adapter_instance_id, state, secret_provider_kind, "
            "secret_reference_id, secret_target, recipient_kid, credential_digest, endpoint_digest, "
            "envelope_digest, mapping_manifest_hash, capability_manifest_hash, release_manifest_hash, "
            "validation_evidence_id, approval_id, created_at, created_wall_ns, retired_at, retired_wall_ns) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "to_timestamp(%s / 1000000000.0), %s, NULL, NULL)",
            (
                identity.profile_id,
                profile.revision,
                identity.adapter_instance_id,
                profile.state.value,
                reference.provider_kind,
                reference.reference_id,
                reference.target,
                digest.recipient_key_id,
                digest.credential_digest,
                digest.endpoint_digest,
                digest.envelope_digest,
                digest.mapping_manifest_hash,
                digest.capability_manifest_hash,
                digest.release_manifest_hash,
                profile.validation_evidence_id,
                profile.approval_id,
                profile.created_wall_ns,
                profile.created_wall_ns,
            ),
        )

    def _insert_request(
        self,
        cursor: _CursorV1,
        *,
        request_id: str,
        payload_digest: str,
        profile: AdapterProfileRevisionV1,
        authorization_epoch: int | None = None,
    ) -> None:
        # The existing migration calls this ``result_code``.  Store the state
        # plus an optional epoch in a fixed, non-secret encoding so a retry can
        # reconstruct the exact local receipt without an extra table.
        result_code = profile.state.value
        if authorization_epoch is not None:
            result_code = f"{profile.state.value}:{authorization_epoch}"
        cursor.execute(
            f"INSERT INTO {self._schema}.control_request_ledger "
            "(request_id, principal_fingerprint, payload_digest, profile_id, profile_revision, "
            "operation, result_code, received_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, clock_timestamp())",
            (
                request_id,
                "0" * 64,
                payload_digest,
                profile.identity.profile_id,
                profile.revision,
                "PROFILE_MUTATION",
                result_code,
            ),
        )

    def _request_row(self, cursor: _CursorV1, request_id: str) -> tuple[object, ...] | None:
        cursor.execute(
            f"SELECT request_id::text, payload_digest, profile_id, profile_revision, result_code "
            f"FROM {self._schema}.control_request_ledger WHERE request_id = %s FOR UPDATE",
            (request_id,),
        )
        return cursor.fetchone()

    @staticmethod
    def _require_digest(row: tuple[object, ...], payload_digest: str) -> None:
        if _row_text(row[1], "control payload digest") != payload_digest:
            raise ProvisioningErrorV1(
                "CONTROL_IDEMPOTENCY_CONFLICT", "control request has another payload"
            )

    @staticmethod
    def _lock_profile(cursor: _CursorV1, profile_id: str) -> None:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (profile_id,))

    def _lock_binding_epoch(self, cursor: _CursorV1) -> None:
        # PostgreSQL forbids FOR UPDATE on an aggregate and an empty binding
        # set has no row to lock. A transaction-scoped two-key advisory lock
        # serializes both publication and retirement across all profiles in
        # this schema, in a lock keyspace distinct from the profile bigint.
        cursor.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (1937001, self._schema))

    @contextmanager
    def connection(self) -> Generator[_ConnectionV1, None, None]:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - declared production dependency.
            raise ProvisioningErrorV1("CONTROL_REPOSITORY_UNAVAILABLE", "psycopg is unavailable") from exc
        try:
            with psycopg.connect(self._dsn) as connection:
                yield cast(_ConnectionV1, connection)
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc


class PostgresProvisioningApprovalRegistryV1(ProvisioningApprovalRegistryV1):
    """Durable approval facts using migration 002's no-secret columns."""

    def __init__(self, *, dsn: str, schema: str = "xuanling_tradecore_cp") -> None:
        self._registry = PostgresProvisioningRegistryV1(dsn=dsn, schema=schema)
        self._schema = schema

    def record(self, approval: ProvisioningApprovalV1) -> None:
        try:
            with self._registry.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT approval_id, action, profile_id, profile_revision, approver_id, "
                    "approver_fingerprint, issued_at_wall_ns, expires_at_wall_ns "
                    f"FROM {self._schema}.control_approvals WHERE approval_id = %s FOR UPDATE",
                    (approval.approval_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if _approval_from_row(existing) != approval:
                        raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval ID conflicts")
                    return
                cursor.execute(
                    f"INSERT INTO {self._schema}.control_approvals "
                    "(approval_id, approver_fingerprint, profile_id, profile_revision, approval_digest, "
                    "expires_at, created_at, action, approver_id, issued_at_wall_ns, expires_at_wall_ns) "
                    "VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000000000.0), "
                    "to_timestamp(%s / 1000000000.0), %s, %s, %s, %s)",
                    (
                        approval.approval_id,
                        approval.approver.certificate_fingerprint,
                        approval.profile_id,
                        approval.revision,
                        _approval_digest(approval),
                        approval.expires_wall_ns,
                        approval.issued_wall_ns,
                        approval.action.value,
                        approval.approver.principal_id,
                        approval.issued_wall_ns,
                        approval.expires_wall_ns,
                    ),
                )
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc

    def require(
        self,
        *,
        approval_id: str,
        action: ProvisioningApprovalActionV1,
        profile_id: str,
        revision: int,
        evaluated_wall_ns: int,
    ) -> ControlPrincipalV1:
        require_text(approval_id, "approval ID", max_length=256)
        require_nonnegative(evaluated_wall_ns, "approval evaluation time")
        try:
            with self._registry.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT approval_id, action, profile_id, profile_revision, approver_id, "
                    "approver_fingerprint, issued_at_wall_ns, expires_at_wall_ns "
                    f"FROM {self._schema}.control_approvals WHERE approval_id = %s",
                    (approval_id,),
                )
                row = cursor.fetchone()
            if row is None:
                raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval was not found")
            approval = _approval_from_row(row)
            if (
                approval.action is not action
                or approval.profile_id != profile_id
                or approval.revision != revision
                or not approval.issued_wall_ns <= evaluated_wall_ns < approval.expires_wall_ns
            ):
                raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval is not valid for action")
            return approval.approver
        except ProvisioningErrorV1:
            raise
        except Exception as exc:
            raise _repository_error(exc) from exc


def _profile_from_row(row: tuple[object, ...]) -> AdapterProfileRevisionV1:
    identity = AdapterProfileIdentityV1(
        profile_id=_row_text(row[0], "profile ID"),
        adapter_kind=AdapterKindV1(_row_text(row[1], "adapter kind")),
        venue_id=_row_text(row[2], "venue ID"),
        adapter_instance_id=_row_text(row[3], "adapter instance ID"),
        account_alias=_row_text(row[4], "account alias"),
    )
    return AdapterProfileRevisionV1(
        identity=identity,
        revision=_row_positive_int(row[5], "profile revision"),
        state=_profile_state(row[6]),
        secret_reference=SecretReferenceV1(
            provider_kind=_row_text(row[7], "secret provider kind"),
            reference_id=_row_text(row[8], "secret reference ID"),
            target=_row_text(row[9], "secret target"),
        ),
        material_digest=ProfileMaterialDigestV1(
            recipient_key_id=_row_text(row[10], "recipient key ID"),
            credential_digest=_row_text(row[11], "credential digest"),
            endpoint_digest=_row_text(row[12], "endpoint digest"),
            envelope_digest=_row_text(row[13], "envelope digest"),
            mapping_manifest_hash=_row_text(row[14], "mapping manifest hash"),
            capability_manifest_hash=_row_text(row[15], "capability manifest hash"),
            release_manifest_hash=_row_text(row[16], "release manifest hash"),
        ),
        created_wall_ns=_row_nonnegative_int(row[17], "profile created time"),
        validation_evidence_id=_row_optional_text(row[18], "validation evidence"),
        approval_id=_row_optional_text(row[19], "approval ID"),
        retired_wall_ns=(
            _row_nonnegative_int(row[20], "retired time") if row[20] is not None else None
        ),
    )


def _binding_from_row(row: tuple[object, ...]) -> ConsumerBindingRevisionV1:
    raw_strategies = row[7]
    if not isinstance(raw_strategies, list):
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", "binding strategy record is invalid")
    strategy_values = cast(list[object], raw_strategies)
    if not all(isinstance(item, str) for item in strategy_values):
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", "binding strategy record is invalid")
    return ConsumerBindingRevisionV1(
        binding_revision_id=_row_text(row[0], "binding ID"),
        profile_id=_row_text(row[1], "profile ID"),
        profile_revision=_row_positive_int(row[2], "profile revision"),
        account_id=_row_text(row[3], "account ID"),
        consumer_id=_row_text(row[4], "consumer ID"),
        consumer_account_id=_row_text(row[5], "consumer account ID"),
        account_display_name=_row_text(row[6], "account display name"),
        strategy_ids=tuple(cast(str, item) for item in strategy_values),
        authorization_epoch=_row_positive_int(row[8], "authorization epoch"),
        effective_wall_ns=_row_nonnegative_int(row[9], "effective time"),
        retired_wall_ns=None,
    )


def _same_live_binding(
    binding: ConsumerBindingRevisionV1,
    profile: AdapterProfileRevisionV1,
    spec: BindingActivationSpecV1,
) -> bool:
    return (
        binding.profile_id == profile.identity.profile_id
        and binding.profile_revision == profile.revision
        and binding.account_id == spec.account_id
        and binding.consumer_id == spec.consumer_id
        and binding.consumer_account_id == spec.consumer_account_id
        and binding.account_display_name == spec.account_display_name
        and binding.strategy_ids == spec.strategy_ids
        and binding.retired_wall_ns is None
    )


def _approval_from_row(row: tuple[object, ...]) -> ProvisioningApprovalV1:
    principal = ControlPrincipalV1(
        principal_id=_row_text(row[4], "approver ID"),
        roles=frozenset({ControlRoleV1.APPROVER}),
        certificate_fingerprint=_row_text(row[5], "approver fingerprint"),
    )
    return ProvisioningApprovalV1(
        approval_id=_row_text(row[0], "approval ID"),
        action=ProvisioningApprovalActionV1(_row_text(row[1], "approval action")),
        profile_id=_row_text(row[2], "profile ID"),
        revision=_row_positive_int(row[3], "profile revision"),
        approver=principal,
        issued_wall_ns=_row_nonnegative_int(row[6], "approval issue time"),
        expires_wall_ns=_row_positive_int(row[7], "approval expiry time"),
    )


def _approval_digest(approval: ProvisioningApprovalV1) -> str:
    from hashlib import sha256

    return sha256(
        ("tradecore/provisioning-approval/v1\\0" + "\\n".join(
            (
                approval.approval_id,
                approval.action.value,
                approval.profile_id,
                str(approval.revision),
                approval.approver.certificate_fingerprint,
                str(approval.issued_wall_ns),
                str(approval.expires_wall_ns),
            )
        )).encode("utf-8")
    ).hexdigest()


def _json_array(values: tuple[str, ...]) -> str:
    # Values are already validated IDs; explicit escaping preserves parameterized SQL.
    import json

    return json.dumps(list(values), separators=(",", ":"))


def _profile_state(value: object) -> ProfileStateV1:
    try:
        return ProfileStateV1(_row_text(value, "profile state"))
    except ValueError as exc:
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", "profile state is invalid") from exc


def _row_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", f"{label} is invalid")
    return value


def _row_optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _row_text(value, label)


def _row_positive_int(value: object, label: str) -> int:
    value_int = _row_nonnegative_int(value, label)
    if value_int <= 0:
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", f"{label} is invalid")
    return value_int


def _row_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ProvisioningErrorV1("CONTROL_REPOSITORY", f"{label} is invalid")
    return value


def _repository_error(_cause: BaseException) -> ProvisioningErrorV1:
    return ProvisioningErrorV1("CONTROL_REPOSITORY_UNAVAILABLE", "control repository is unavailable")


def _epoch_from_result_code(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    _separator, marker, raw_epoch = value.partition(":")
    if not marker or not raw_epoch.isdecimal():
        return None
    epoch = int(raw_epoch)
    return epoch if epoch > 0 else None


def _is_legal_transition(source: ProfileStateV1, target: ProfileStateV1) -> bool:
    return (source, target) in {
        (ProfileStateV1.PREPARED, ProfileStateV1.VALIDATED),
        (ProfileStateV1.PREPARED, ProfileStateV1.REJECTED),
        (ProfileStateV1.VALIDATED, ProfileStateV1.ACTIVE_DISARMED),
        (ProfileStateV1.ACTIVE_DISARMED, ProfileStateV1.RETIRING),
        (ProfileStateV1.RETIRING, ProfileStateV1.RETIRED),
    }
