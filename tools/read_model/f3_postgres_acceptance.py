"""Run the isolated F3 PostgreSQL migration, projector and RLS acceptance checks.

The caller supplies a temporary, dedicated PostgreSQL DSN through a local file.
The script never logs the DSN, database identity, records, account values or
raw order data.  It is not an execution test and it must never target a live
application database.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg
from xuanling_tradecore.read_model import (
    PostgresReadModelProjectionStoreV1,
    PostgresReadModelQueryServiceV1,
    ReadModelCheckpointKeyV1,
    ReadModelCheckpointV1,
    ReadModelConsumerScopeV1,
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
    ReadModelRowMutationV1,
    ReadModelSnapshotReplacementV1,
    ReadModelTableV1,
    apply_read_model_migrations,
    read_model_schema_hash,
    rollback_read_model_migrations,
)

_JOURNAL_ID = UUID("00000000-0000-0000-0000-00000000f301")
_CONSUMER_ID = UUID("00000000-0000-0000-0000-00000000f302")
_OTHER_CONSUMER_ID = UUID("00000000-0000-0000-0000-00000000f303")
_STRATEGY_ID = UUID("00000000-0000-0000-0000-00000000f304")
_ORDER_A_ID = UUID("00000000-0000-0000-0000-00000000f305")
_ORDER_B_ID = UUID("00000000-0000-0000-0000-00000000f306")
_HASH_A = "a" * 64
_HASH_B = "b" * 64


def _checkpoint(ordinal: int, source_hash: str) -> ReadModelCheckpointV1:
    return ReadModelCheckpointV1(
        key=ReadModelCheckpointKeyV1("f3-acceptance", "shard-a", "account-scope-a"),
        projector_version="v1",
        position=ReadModelPositionV1(_JOURNAL_ID, 1, 1, 1, ordinal),
        source_fact_hash=source_hash,
    )


def _account(*, revision: int, source_hash: str, valid: bool) -> ReadModelRowMutationV1:
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return ReadModelRowMutationV1(
        ReadModelTableV1.ACCOUNTS,
        {
            "account_id": "f3-account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "f3-adapter-a",
            "consumer_id": _CONSUMER_ID,
            "account_display_name": "masked",
            "trading_day": date(2026, 1, 1),
            "trading_day_epoch": 1,
            "lifecycle": "DISARMED" if valid else "READY",
            "reconciliation_status": "REQUIRED",
            "active_gate_count": 1,
            "can_submit": False if valid else True,
            "submit_block_reason_code": "DISARMED" if valid else None,
            "capability_hash": _HASH_A,
            "revision": revision,
            "source_fact_hash": source_hash,
            "updated_at": timestamp,
            "projected_at": timestamp,
        },
    )


def _strategy() -> ReadModelRowMutationV1:
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return ReadModelRowMutationV1(
        ReadModelTableV1.STRATEGIES,
        {
            "strategy_id": _STRATEGY_ID,
            "consumer_id": _CONSUMER_ID,
            "strategy_code": "f3-test-strategy",
            "display_name": "F3 test strategy",
            "strategy_kind": "PROGRAM",
            "status": "ACTIVE",
            "revision": 1,
            "source_fact_hash": _HASH_A,
            "updated_at": timestamp,
            "projected_at": timestamp,
        },
    )


def _order(
    *, order_id: UUID, strategy_order_id: str, created_at: datetime
) -> ReadModelRowMutationV1:
    return ReadModelRowMutationV1(
        ReadModelTableV1.ORDERS,
        {
            "oms_order_id": order_id,
            "consumer_id": _CONSUMER_ID,
            "account_id": "f3-account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "f3-adapter-a",
            "strategy_id": _STRATEGY_ID,
            "strategy_order_id": strategy_order_id,
            "trader_id": _CONSUMER_ID,
            "origin": "PROGRAM",
            "trading_day": date(2026, 1, 1),
            "trading_day_epoch": 1,
            "market": "SH",
            "instrument_id": "test-instrument",
            "side": "BUY",
            "order_type": "LIMIT",
            "currency": "CNY",
            "order_qty": 1,
            "filled_qty": 0,
            "leaves_qty": 1,
            "status": "WORKING",
            "risk_result": "PASS",
            "decision_context_hash": _HASH_A,
            "intent_hash": _HASH_A,
            "revision": 1,
            "source_fact_hash": _HASH_A,
            "created_at": created_at,
            "updated_at": created_at,
            "projected_at": created_at,
        },
    )


def _asset(
    *, currency: str, generation: int, revision: int, source_hash: str, valid: bool = True
) -> ReadModelRowMutationV1:
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return ReadModelRowMutationV1(
        ReadModelTableV1.ASSETS,
        {
            "account_id": "f3-account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "f3-adapter-a",
            "trading_day": date(2026, 1, 1),
            "trading_day_epoch": 1,
            "snapshot_generation": generation,
            "currency": currency,
            "total_asset": Decimal("1") if valid else None,
            "net_asset": Decimal("1"),
            "cash_balance": Decimal("1"),
            "available_cash": Decimal("1"),
            "frozen_cash": Decimal("0"),
            "market_value": Decimal("0"),
            "margin_used": Decimal("0"),
            "total_debt": Decimal("0"),
            "credit_limit": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "total_fee": Decimal("0"),
            "revision": revision,
            "source_fact_hash": source_hash,
            "updated_at": timestamp,
            "projected_at": timestamp,
        },
    )


def _assets_snapshot(
    *,
    generation: int,
    revision: int,
    source_hash: str,
    currencies: tuple[str, ...],
    valid: bool = True,
) -> ReadModelSnapshotReplacementV1:
    return ReadModelSnapshotReplacementV1(
        table=ReadModelTableV1.ASSETS,
        scope={
            "account_id": "f3-account-a",
            "venue_id": "EMT",
            "adapter_instance_id": "f3-adapter-a",
            "trading_day_epoch": 1,
        },
        snapshot_generation=generation,
        rows=tuple(
            _asset(
                currency=currency,
                generation=generation,
                revision=revision,
                source_hash=source_hash,
                valid=valid,
            )
            for currency in currencies
        ),
    )


def _require_dsn(path: Path | None, env_name: str | None) -> str:
    if env_name is not None:
        dsn = os.environ.get(env_name, "").strip()
        if not dsn:
            raise SystemExit("--dsn-env does not name a non-empty environment variable")
        return dsn
    assert path is not None
    try:
        dsn = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(f"cannot read --dsn-file: {exc.__class__.__name__}") from exc
    if not dsn:
        raise SystemExit("--dsn-file is empty")
    return dsn


def _validate_rls(dsn: str) -> None:
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            connection.execute("SET LOCAL ROLE tradecore_query")
            connection.execute(
                "SELECT set_config('app.consumer_id', %s, true)", (str(_CONSUMER_ID),)
            )
            visible = connection.execute(
                "SELECT count(*) FROM xuanling_tradecore_rm.v_accounts"
            ).fetchone()
            if visible != (1,):
                raise RuntimeError("consumer-scoped account view did not return exactly one row")
        with connection.transaction():
            connection.execute("SET LOCAL ROLE tradecore_query")
            connection.execute(
                "SELECT set_config('app.consumer_id', %s, true)", (str(_OTHER_CONSUMER_ID),)
            )
            hidden = connection.execute(
                "SELECT count(*) FROM xuanling_tradecore_rm.v_accounts"
            ).fetchone()
            if hidden != (0,):
                raise RuntimeError("RLS exposed another consumer's account row")
        with connection.transaction():
            connection.execute("SET LOCAL ROLE tradecore_query")
            try:
                connection.execute(
                    "INSERT INTO xuanling_tradecore_rm.accounts (account_id) VALUES ('nope')"
                )
            except psycopg.errors.InsufficientPrivilege:
                return
            raise RuntimeError("query role unexpectedly wrote a base table")


def _check_table_count(dsn: str, expected: int) -> None:
    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'xuanling_tradecore_rm' AND table_type = 'BASE TABLE'"
        ).fetchone()
    if row != (expected,):
        raise RuntimeError("read-model table count differs from the F3 contract")


def run(*, dsn: str) -> dict[str, object]:
    migrations = apply_read_model_migrations(dsn=dsn)
    _check_table_count(dsn, 9)
    store = PostgresReadModelProjectionStoreV1(dsn=dsn)
    first = ReadModelProjectionBatchV1(
        None,
        _checkpoint(1, _HASH_A),
        (
            _account(revision=1, source_hash=_HASH_A, valid=True),
            _strategy(),
            _order(
                order_id=_ORDER_A_ID,
                strategy_order_id="f3-order-a",
                created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
            _order(
                order_id=_ORDER_B_ID,
                strategy_order_id="f3-order-b",
                created_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            ),
        ),
    )
    first_commit = store.apply(first)
    replay_commit = store.apply(first)
    asset_first = ReadModelProjectionBatchV1(
        first.checkpoint.position,
        _checkpoint(2, _HASH_B),
        snapshots=(
            _assets_snapshot(
                generation=1,
                revision=1,
                source_hash=_HASH_B,
                currencies=("CNY", "USD"),
            ),
        ),
    )
    store.apply(asset_first)
    asset_second = ReadModelProjectionBatchV1(
        asset_first.checkpoint.position,
        _checkpoint(3, "c" * 64),
        snapshots=(
            _assets_snapshot(
                generation=2,
                revision=2,
                source_hash="c" * 64,
                currencies=("CNY",),
            ),
        ),
    )
    store.apply(asset_second)
    with psycopg.connect(dsn) as connection:
        asset_count = connection.execute(
            "SELECT count(*) FROM xuanling_tradecore_rm.assets WHERE snapshot_generation = 2"
        ).fetchone()
    if asset_count != (1,):
        raise RuntimeError("asset snapshot replacement left a stale row visible")
    failed_batch = ReadModelProjectionBatchV1(
        asset_second.checkpoint.position,
        _checkpoint(4, "d" * 64),
        snapshots=(
            _assets_snapshot(
                generation=3,
                revision=3,
                source_hash="d" * 64,
                currencies=("CNY",),
                valid=False,
            ),
        ),
    )
    try:
        store.apply(failed_batch)
    except ReadModelProjectionErrorV1:
        pass
    else:
        raise RuntimeError("invalid domain projection unexpectedly committed")
    with psycopg.connect(dsn) as connection:
        checkpoint = connection.execute(
            "SELECT fact_ordinal FROM xuanling_tradecore_rm.projection_checkpoints"
        ).fetchone()
        revision = connection.execute(
            "SELECT revision FROM xuanling_tradecore_rm.accounts"
        ).fetchone()
    if checkpoint != (3,) or revision != (1,):
        raise RuntimeError("failed projection advanced checkpoint or published a row")
    with psycopg.connect(dsn) as connection:
        asset_generation = connection.execute(
            "SELECT max(snapshot_generation) FROM xuanling_tradecore_rm.assets"
        ).fetchone()
    if asset_generation != (2,):
        raise RuntimeError("failed snapshot projection changed the published generation")
    _validate_rls(dsn)
    query = PostgresReadModelQueryServiceV1(
        dsn=dsn,
        stale_after=timedelta(minutes=1),
        query_role="tradecore_query",
        now=lambda: datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    page_one = query.list_orders(scope=ReadModelConsumerScopeV1(_CONSUMER_ID), limit=1)
    if len(page_one.rows) != 1 or page_one.next_cursor is None:
        raise RuntimeError("keyset query did not return the first page and cursor")
    page_two = query.list_orders(
        scope=ReadModelConsumerScopeV1(_CONSUMER_ID), after=page_one.next_cursor, limit=1
    )
    if len(page_two.rows) != 1 or page_two.rows[0].oms_order_id == page_one.rows[0].oms_order_id:
        raise RuntimeError("keyset query is not continuous")
    if query.watermark(scope=ReadModelConsumerScopeV1(_CONSUMER_ID)).stale:
        raise RuntimeError("fresh F3 account projection was unexpectedly stale")
    rollback = rollback_read_model_migrations(dsn=dsn)
    with psycopg.connect(dsn) as connection:
        schema = connection.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'xuanling_tradecore_rm'"
        ).fetchone()
    if schema is not None:
        raise RuntimeError("F3 rollback left the read-model schema behind")
    return {
        "migration_versions": len(migrations),
        "rollback_versions": len(rollback),
        "schema_hash": read_model_schema_hash(migrations),
        "table_count": 9,
        "first_projection_rows": first_commit.inserted_or_updated_rows,
        "idempotent_replay": replay_commit.replayed,
        "transaction_rollback": True,
        "rls_and_query_role": True,
        "keyset_and_watermark": True,
        "snapshot_atomic_replace_and_rollback": True,
        "status": "PASS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    dsn_source = parser.add_mutually_exclusive_group(required=True)
    dsn_source.add_argument("--dsn-file", type=Path)
    dsn_source.add_argument("--dsn-env", type=str)
    parser.add_argument("--confirm-isolated-f3-acceptance", action="store_true")
    args = parser.parse_args()
    if not args.confirm_isolated_f3_acceptance:
        parser.error("--confirm-isolated-f3-acceptance is required")
    print(json.dumps(run(dsn=_require_dsn(args.dsn_file, args.dsn_env)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
