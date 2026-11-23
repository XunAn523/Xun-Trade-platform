from __future__ import annotations

import sys
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

from xuanling_tradecore.read_model import (
    ReadModelMigrationError,
    apply_read_model_migrations,
    load_read_model_migrations,
    read_model_schema_hash,
    rollback_read_model_migrations,
)


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _Connection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.sql: list[str] = []
        self.parameters: list[tuple[object, ...]] = []
        self.commits = 0

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> _Cursor:
        self.sql.append(sql)
        if parameters is not None:
            self.parameters.append(parameters)
        return _Cursor(self.rows if "SELECT version" in sql else [])

    def commit(self) -> None:
        self.commits += 1

    def transaction(self):
        return nullcontext()


class ReadModelMigrationTests(unittest.TestCase):
    def test_initial_catalog_is_contiguous_and_has_a_release_hash(self) -> None:
        migrations = load_read_model_migrations()
        self.assertEqual((1,), tuple(item.version for item in migrations))
        self.assertEqual("001_initial_read_model.sql", migrations[0].name)
        self.assertEqual(64, len(read_model_schema_hash(migrations)))
        self.assertEqual(read_model_schema_hash(migrations), read_model_schema_hash())

    def test_initial_schema_keeps_exactly_nine_read_model_tables_and_safe_views(self) -> None:
        sql = load_read_model_migrations()[0].sql.lower()
        expected_tables = {
            "accounts",
            "strategies",
            "orders",
            "order_executions",
            "order_actions",
            "trades",
            "assets",
            "positions",
            "projection_checkpoints",
        }
        for table in expected_tables:
            self.assertIn(f"create table xuanling_tradecore_rm.{table}", sql)
        self.assertEqual(9, sql.count("create table xuanling_tradecore_rm."))
        self.assertIn("force row level security", sql)
        self.assertIn("with (security_barrier = true, security_invoker = true)", sql)
        self.assertIn("uq_order_executions_venue_order", sql)
        self.assertIn("ck_order_executions_ref_composition", sql)
        self.assertNotIn("create table xuanling_tradecore_rm.active_orders", sql)

    def test_explicit_migrator_applies_once_and_persists_checksum(self) -> None:
        connection = _Connection([])
        module = SimpleNamespace(connect=lambda _dsn: connection)
        with patch.dict(sys.modules, {"psycopg": module}):
            applied = apply_read_model_migrations(dsn="postgresql://migrator")
        self.assertEqual(1, len(applied))
        self.assertEqual(1, connection.commits)
        self.assertEqual(1, len(connection.parameters))
        version, name, checksum = connection.parameters[0]
        self.assertEqual((1, "001_initial_read_model.sql"), (version, name))
        self.assertEqual(applied[0].sha256_hex, checksum)

    def test_applied_checksum_drift_is_rejected_before_ddl(self) -> None:
        connection = _Connection([(1, "001_initial_read_model.sql", "0" * 64)])
        module = SimpleNamespace(connect=lambda _dsn: connection)
        with patch.dict(sys.modules, {"psycopg": module}):
            with self.assertRaisesRegex(ReadModelMigrationError, "checksum drift"):
                apply_read_model_migrations(dsn="postgresql://migrator")
        self.assertEqual(2, len(connection.sql))
        self.assertEqual([], connection.parameters)

    def test_explicit_rollback_uses_reviewed_down_resource_only(self) -> None:
        migration = load_read_model_migrations()[0]
        connection = _Connection([(migration.version, migration.name, migration.sha256_hex)])
        module = SimpleNamespace(connect=lambda _dsn: connection)
        with patch.dict(sys.modules, {"psycopg": module}):
            rolled_back = rollback_read_model_migrations(
                dsn="postgresql://migrator", target_version=0
            )
        self.assertEqual((migration,), rolled_back)
        self.assertIn("DROP SCHEMA IF EXISTS xuanling_tradecore_rm", connection.sql[-2])
        self.assertEqual((migration.version,), connection.parameters[-1])


if __name__ == "__main__":
    unittest.main()
