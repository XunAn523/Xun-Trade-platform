"""Isolated PostgreSQL schemas for persistent-ledger integration tests."""

from __future__ import annotations

import os
import unittest
from contextlib import AbstractContextManager
from dataclasses import dataclass
from uuid import uuid4

import psycopg

TEST_POSTGRES_DSN = os.environ.get("EMT_GATEWAY_TEST_POSTGRES_DSN", "").strip()
HAS_TEST_POSTGRES = bool(TEST_POSTGRES_DSN)


@dataclass
class PostgresTestDatabase(AbstractContextManager["PostgresTestDatabase"]):
    """Own one generated schema, never a user supplied database or schema."""

    dsn: str = TEST_POSTGRES_DSN

    def __post_init__(self) -> None:
        if not self.dsn:
            raise unittest.SkipTest(
                "set EMT_GATEWAY_TEST_POSTGRES_DSN to run PostgreSQL ledger tests"
            )
        self.schema = f"emt_gateway_test_{uuid4().hex}"

    def __enter__(self) -> "PostgresTestDatabase":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        # The generated identifier cannot name a deployment schema.  The test
        # role needs CREATE/DROP SCHEMA only in the dedicated test database.
        with psycopg.connect(self.dsn, autocommit=True) as connection:
            connection.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
