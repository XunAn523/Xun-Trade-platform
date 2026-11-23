"""Operator-only entry point for F3 PostgreSQL read-model migrations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from xuanling_tradecore.read_model import (
    apply_read_model_migrations,
    read_model_schema_hash,
    rollback_read_model_migrations,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--confirm-f3-migration", action="store_true")
    parser.add_argument("--rollback-to", type=int)
    args = parser.parse_args()
    if not args.confirm_f3_migration:
        parser.error("--confirm-f3-migration is required")
    try:
        dsn = args.dsn_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        parser.error(f"cannot read --dsn-file: {exc.__class__.__name__}")
    if args.rollback_to is None:
        migrations = apply_read_model_migrations(dsn=dsn)
        operation = "upgrade"
    else:
        migrations = rollback_read_model_migrations(dsn=dsn, target_version=args.rollback_to)
        operation = "rollback"
    print(
        json.dumps(
            {
                "operation": operation,
                "migration_count": len(migrations),
                "schema_hash": read_model_schema_hash(migrations),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
