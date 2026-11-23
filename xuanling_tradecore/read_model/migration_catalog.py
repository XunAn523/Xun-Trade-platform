"""Explicit F3 migration catalog and checksum-gated PostgreSQL application.

Nothing in this module is invoked from a hot path.  A deployment operator must
call :func:`apply_read_model_migrations` with a dedicated migrator DSN; this
keeps PostgreSQL unavailable from C0--C7 and prevents application start-up
from silently altering schema state.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from importlib.resources import files
from typing import Any, Iterable, cast


class ReadModelMigrationError(RuntimeError):
    """The reviewed F3 migration chain is missing, reordered or has drifted."""


@dataclass(frozen=True)
class ReadModelMigrationV1:
    """One immutable SQL migration resource with a content checksum."""

    version: int
    name: str
    sql: str
    sha256_hex: str
    rollback_sql: str
    rollback_sha256_hex: str

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ReadModelMigrationError("read-model migration version is invalid")
        if not self.name or len(self.name) > 128 or not self.name.endswith(".sql"):
            raise ReadModelMigrationError("read-model migration name is invalid")
        if not self.sql.strip():
            raise ReadModelMigrationError("read-model migration SQL is empty")
        if not self.rollback_sql.strip():
            raise ReadModelMigrationError("read-model rollback SQL is empty")
        if len(self.sha256_hex) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256_hex
        ):
            raise ReadModelMigrationError("read-model migration checksum is invalid")
        if len(self.rollback_sha256_hex) != 64 or any(
            character not in "0123456789abcdef" for character in self.rollback_sha256_hex
        ):
            raise ReadModelMigrationError("read-model rollback checksum is invalid")


_STATE_SCHEMA = "xuanling_tradecore_meta"
_STATE_TABLE = "schema_migrations"
_STATE_BOOTSTRAP_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {_STATE_SCHEMA};
CREATE TABLE IF NOT EXISTS {_STATE_SCHEMA}.{_STATE_TABLE} (
    version integer PRIMARY KEY,
    migration_name varchar(128) NOT NULL UNIQUE,
    migration_sha256 char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_schema_migrations_sha256
        CHECK (migration_sha256 ~ '^[0-9a-f]{{64}}$')
);
"""


def load_read_model_migrations() -> tuple[ReadModelMigrationV1, ...]:
    """Load the reviewed chain from package data and reject ambiguity or gaps."""

    resource_root = files("xuanling_tradecore.read_model.migrations")
    migrations: list[ReadModelMigrationV1] = []
    for resource in resource_root.iterdir():
        if not resource.name.endswith(".sql") or resource.name.endswith(".down.sql"):
            continue
        prefix, separator, _suffix = resource.name.partition("_")
        if not separator or not prefix.isdecimal():
            raise ReadModelMigrationError("read-model migration file name is invalid")
        sql = resource.read_text(encoding="utf-8")
        rollback_resource = resource_root.joinpath(resource.name.removesuffix(".sql") + ".down.sql")
        if not rollback_resource.is_file():
            raise ReadModelMigrationError("read-model migration has no rollback resource")
        rollback_sql = rollback_resource.read_text(encoding="utf-8")
        migrations.append(
            ReadModelMigrationV1(
                version=int(prefix),
                name=resource.name,
                sql=sql,
                sha256_hex=sha256(sql.encode("utf-8")).hexdigest(),
                rollback_sql=rollback_sql,
                rollback_sha256_hex=sha256(rollback_sql.encode("utf-8")).hexdigest(),
            )
        )
    ordered = tuple(sorted(migrations, key=lambda item: item.version))
    _validate_chain(ordered)
    return ordered


def read_model_schema_hash(migrations: Iterable[ReadModelMigrationV1] | None = None) -> str:
    """Return a stable release-manifest hash for the exact SQL migration chain."""

    chain = tuple(load_read_model_migrations() if migrations is None else migrations)
    _validate_chain(chain)
    canonical = "".join(
        f"{migration.version}:{migration.name}:{migration.sha256_hex}:"
        f"{migration.rollback_sha256_hex}\n"
        for migration in chain
    )
    return sha256(canonical.encode("ascii")).hexdigest()


def apply_read_model_migrations(*, dsn: str) -> tuple[ReadModelMigrationV1, ...]:
    """Apply pending migrations through an operator-supplied PostgreSQL DSN.

    The function is deliberately opt-in and has no retry loop.  A connection,
    lock or permission failure is surfaced to deployment tooling instead of
    being hidden behind an application start, a background worker or an OMS
    operation.  Existing migration checksums are immutable: a modified applied
    resource is a deployment failure, never a forward overwrite.
    """

    if not dsn.strip():
        raise ReadModelMigrationError("read-model migrator DSN is invalid")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - runtime dependency is declared.
        raise ReadModelMigrationError("psycopg is unavailable for read-model migration") from exc

    chain = load_read_model_migrations()
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(_STATE_BOOTSTRAP_SQL)
            connection.commit()
            rows = connection.execute(
                f"SELECT version, migration_name, migration_sha256 "
                f"FROM {_STATE_SCHEMA}.{_STATE_TABLE} ORDER BY version"
            ).fetchall()
            applied = {int(version): (str(name), str(checksum)) for version, name, checksum in rows}
            expected_versions = {migration.version for migration in chain}
            unknown = set(applied).difference(expected_versions)
            if unknown:
                raise ReadModelMigrationError("database contains an unknown read-model migration")
            for migration in chain:
                prior = applied.get(migration.version)
                if prior is not None:
                    if prior != (migration.name, migration.sha256_hex):
                        raise ReadModelMigrationError("applied read-model migration checksum drift")
                    continue
                with connection.transaction():
                    # ``psycopg``'s optional typing exposes SQL templates for
                    # dynamically supplied statements.  Migration resources
                    # are reviewed immutable strings, so retain their runtime
                    # behavior while keeping the optional dependency boundary
                    # explicitly dynamic to the type checker.
                    cast(Any, connection).execute(migration.sql)
                    connection.execute(
                        f"INSERT INTO {_STATE_SCHEMA}.{_STATE_TABLE} "
                        "(version, migration_name, migration_sha256) VALUES (%s, %s, %s)",
                        (migration.version, migration.name, migration.sha256_hex),
                    )
    except ReadModelMigrationError:
        raise
    except Exception as exc:
        raise ReadModelMigrationError("read-model migration execution failed") from exc
    return chain


def rollback_read_model_migrations(
    *, dsn: str, target_version: int = 0
) -> tuple[ReadModelMigrationV1, ...]:
    """Explicitly roll the reviewed chain back to ``target_version``.

    Rollback is deployment tooling only.  It never runs as application start-up
    or as an OMS/EMS recovery reaction, and it rejects a non-reviewed applied
    chain before executing any destructive DDL.
    """

    if not dsn.strip():
        raise ReadModelMigrationError("read-model migrator DSN is invalid")
    chain = load_read_model_migrations()
    if type(target_version) is not int or not 0 <= target_version <= len(chain):
        raise ReadModelMigrationError("read-model rollback target version is invalid")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - runtime dependency is declared.
        raise ReadModelMigrationError("psycopg is unavailable for read-model rollback") from exc
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(_STATE_BOOTSTRAP_SQL)
            connection.commit()
            rows = connection.execute(
                f"SELECT version, migration_name, migration_sha256 "
                f"FROM {_STATE_SCHEMA}.{_STATE_TABLE} ORDER BY version"
            ).fetchall()
            applied = {int(version): (str(name), str(checksum)) for version, name, checksum in rows}
            expected = {item.version: (item.name, item.sha256_hex) for item in chain}
            if set(applied).difference(expected) or any(
                applied[version] != expected[version] for version in applied
            ):
                raise ReadModelMigrationError("database has an unknown read-model migration chain")
            rollback = tuple(
                item
                for item in reversed(chain)
                if item.version > target_version and item.version in applied
            )
            for migration in rollback:
                with connection.transaction():
                    cast(Any, connection).execute(migration.rollback_sql)
                    connection.execute(
                        f"DELETE FROM {_STATE_SCHEMA}.{_STATE_TABLE} WHERE version = %s",
                        (migration.version,),
                    )
    except ReadModelMigrationError:
        raise
    except Exception as exc:
        raise ReadModelMigrationError("read-model rollback execution failed") from exc
    return rollback


def _validate_chain(chain: tuple[ReadModelMigrationV1, ...]) -> None:
    if not chain:
        raise ReadModelMigrationError("read-model migration chain is empty")
    versions = tuple(migration.version for migration in chain)
    if versions != tuple(range(1, len(chain) + 1)):
        raise ReadModelMigrationError("read-model migration versions must be contiguous from one")
    names = tuple(migration.name for migration in chain)
    if len(names) != len(set(names)):
        raise ReadModelMigrationError("read-model migration names are duplicated")
