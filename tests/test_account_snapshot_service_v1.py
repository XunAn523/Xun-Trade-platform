from __future__ import annotations

import unittest
from hashlib import sha256

from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.snapshot import (
    AccountSnapshotRequestV1,
    AccountSnapshotServiceV1,
    InMemoryAccountSnapshotStoreV1,
    QueryCompletionModeV1,
    SnapshotCollectionErrorV1,
    SnapshotQueryKindV1,
    SnapshotQueryPageV1,
    SnapshotQuerySpecV1,
)


class _Executor:
    def __init__(self, pages: dict[str, list[SnapshotQueryPageV1]]) -> None:
        self.pages = pages
        self.calls: list[tuple[str, str | None]] = []

    def execute(self, request, *, cursor, completion):  # type: ignore[no-untyped-def]
        del completion
        self.calls.append((request.query_kind, cursor))
        return self.pages[request.query_kind].pop(0)


def _page(
    request_id: str,
    kind: SnapshotQueryKindV1,
    *,
    cursor: str | None = None,
    next_cursor: str | None = None,
    is_last: bool | None = True,
    succeeded: bool = True,
) -> SnapshotQueryPageV1:
    return SnapshotQueryPageV1(
        request_id=request_id,
        query_kind=kind,
        request_cursor=cursor,
        next_cursor=next_cursor,
        row_count=1,
        rows_digest=sha256(request_id.encode()).hexdigest(),
        succeeded=succeeded,
        is_last=is_last,
        received_monotonic_ns=1,
    )


class AccountSnapshotServiceTests(unittest.TestCase):
    def _request(self, specs):  # type: ignore[no-untyped-def]
        return AccountSnapshotRequestV1(
            "snapshot-1",
            "account-1",
            "EMT",
            "emt-fake-1",
            TradingDayEpoch("2026-09-04"),
            tuple(specs),
        )

    def test_commits_only_after_serial_complete_single_and_cursor_series(self) -> None:
        assets = SnapshotQuerySpecV1(SnapshotQueryKindV1.ASSETS, QueryCompletionModeV1.SINGLE)
        orders = SnapshotQuerySpecV1(SnapshotQueryKindV1.ORDERS, QueryCompletionModeV1.CURSOR_PAGED)
        executor = _Executor(
            {
                "assets": [_page("snapshot-1:assets:0", SnapshotQueryKindV1.ASSETS, is_last=None)],
                "orders": [
                    _page(
                        "snapshot-1:orders:0",
                        SnapshotQueryKindV1.ORDERS,
                        next_cursor="page-2",
                        is_last=False,
                    ),
                    _page(
                        "snapshot-1:orders:1",
                        SnapshotQueryKindV1.ORDERS,
                        cursor="page-2",
                        is_last=True,
                    ),
                ],
            }
        )
        store = InMemoryAccountSnapshotStoreV1()
        snapshot = AccountSnapshotServiceV1(executor=executor, store=store).capture(
            self._request((assets, orders))
        )
        self.assertEqual(1, snapshot.snapshot_generation)
        self.assertEqual(snapshot, store.latest("account-1"))
        self.assertEqual([("assets", None), ("orders", None), ("orders", "page-2")], executor.calls)

    def test_missing_terminal_or_repeated_cursor_never_commits(self) -> None:
        spec = SnapshotQuerySpecV1(
            SnapshotQueryKindV1.ORDERS, QueryCompletionModeV1.CURSOR_PAGED, max_pages=3
        )
        executor = _Executor(
            {
                "orders": [
                    _page(
                        "snapshot-1:orders:0",
                        SnapshotQueryKindV1.ORDERS,
                        next_cursor="same",
                        is_last=False,
                    ),
                    _page(
                        "snapshot-1:orders:1",
                        SnapshotQueryKindV1.ORDERS,
                        cursor="same",
                        next_cursor="same",
                        is_last=False,
                    ),
                ]
            }
        )
        store = InMemoryAccountSnapshotStoreV1()
        with self.assertRaises(SnapshotCollectionErrorV1):
            AccountSnapshotServiceV1(executor=executor, store=store).capture(self._request((spec,)))
        self.assertIsNone(store.latest("account-1"))

    def test_single_query_rejects_cursor_and_error(self) -> None:
        spec = SnapshotQuerySpecV1(SnapshotQueryKindV1.FUNDS, QueryCompletionModeV1.SINGLE)
        for page in (
            _page("snapshot-1:funds:0", SnapshotQueryKindV1.FUNDS, next_cursor="bad"),
            _page("snapshot-1:funds:0", SnapshotQueryKindV1.FUNDS, succeeded=False),
        ):
            with self.subTest(page=page):
                with self.assertRaises(SnapshotCollectionErrorV1):
                    AccountSnapshotServiceV1(
                        executor=_Executor({"funds": [page]}),
                        store=InMemoryAccountSnapshotStoreV1(),
                    ).capture(self._request((spec,)))
