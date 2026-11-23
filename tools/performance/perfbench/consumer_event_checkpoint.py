"""Private, bounded, FULL-committed consumer progress for rolling signed plans.

This is LOAD-GENERATOR evidence, not a TradeCore recovery authority. Tokens stay
inside the private DB. Only server-signed partitions are consumed; no raw-LSN
cursor construction, server book access, reset or SDK invocation exists here.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns

import msgspec
from xuanling_tradecore.consumer.replay_evidence_v2 import build_replay_partition_evidence_v2
from xuanling_tradecore.domain import ConsumerEventPositionV2
from xuanling_tradecore.persistence.runtime_files import (
    ExclusiveRuntimeLease,
    open_private,
    require_private_runtime_file,
)


@dataclass(frozen=True)
class CheckpointedConsumerPage:
    page_serial: int
    unique_events: int
    replayed_events: int
    checkpointed_ns: int
    frontier_position: str | None


@dataclass(frozen=True)
class _PreparedCheckpointPage:
    partition_id: int
    token: str
    position: str
    count: int
    origin_sha: bytes
    fingerprint: bytes
    rows: tuple[tuple[bytes, str, bytes], ...]


class ConsumerEventCheckpoint:
    def __init__(
        self, *, root: Path, scope_sha256: str, initialize: bool,
        maximum_bytes: int = 64 * 1024**3,
    ) -> None:
        if (
            not root.is_absolute()
            or not isinstance(scope_sha256, str)
            or len(scope_sha256) != 64 or any(c not in "0123456789abcdef" for c in scope_sha256)
            or type(maximum_bytes) is not int or not 1024**2 <= maximum_bytes <= 1024**4
            or type(initialize) is not bool
        ):
            raise ValueError("consumer checkpoint scope/budget invalid")
        root.mkdir(mode=0o700, exist_ok=True)
        if root.is_symlink() or not root.is_dir() or (
            os.name == "posix" and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        ):
            raise ValueError("consumer checkpoint root unsafe")
        self._lease = ExclusiveRuntimeLease(root / "consumer-owner.lock")
        self._closed = self._failed = False
        self.last_page_stages: dict[str, int | bool] | None = None
        self._db = None
        path = root / "consumer-progress.sqlite"
        self._path = path
        # WAL removes the rollback-journal create/sync/unlink cycle from every
        # page while retaining synchronous=FULL.  There is one checkpoint
        # owner and no long-lived reader, so a bounded RESTART checkpoint can
        # truncate the WAL without blocking a foreign transaction.  RESTART
        # leaves the already-allocated file at its high-water size, so file
        # length cannot prove the configured storage bound after it runs.
        # A periodic TRUNCATE is outside the TradeCore request hot path and
        # gives the private evidence ledger an exact, fail-closed byte bound.
        # This is a
        # load-generator durability ledger, not TradeCore recovery authority.
        self._wal_limit_bytes = min(64 * 1024**2, max(256 * 1024, maximum_bytes // 64))
        self._maximum_bytes = maximum_bytes
        self._checkpoint_count = 0
        try:
            if initialize:
                os.close(open_private(path, os.O_CREAT | os.O_EXCL | os.O_RDWR))
            require_private_runtime_file(path)
            self._db = db = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=5)
            if db.execute("PRAGMA journal_mode=WAL").fetchone() != ("wal",):
                raise ValueError("consumer checkpoint journal mode invalid")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA wal_autocheckpoint=0")
            db.execute(f"PRAGMA journal_size_limit={self._wal_limit_bytes}")
            db.execute("PRAGMA cache_size=-8192")
            pages = (maximum_bytes - self._wal_limit_bytes) // db.execute(
                "PRAGMA page_size"
            ).fetchone()[0]
            if db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] > pages:
                raise ValueError("consumer checkpoint budget exceeded")
            if initialize:
                db.executescript("""
                    BEGIN IMMEDIATE;
                    CREATE TABLE meta (
                      id INTEGER PRIMARY KEY CHECK(id=1), scope TEXT NOT NULL,
                      journal TEXT, generation INTEGER, frontier_token TEXT, frontier_position TEXT,
                      plan_serial INTEGER NOT NULL, page_serial INTEGER NOT NULL,
                      unique_events INTEGER NOT NULL);
                    CREATE TABLE parts (
                      id INTEGER PRIMARY KEY, high TEXT NOT NULL, expected INTEGER NOT NULL,
                      cursor TEXT NOT NULL, current_position TEXT, received INTEGER NOT NULL,
                      done INTEGER NOT NULL, last_origin BLOB, last_reply BLOB);
                    CREATE TABLE events (
                      event_sha BLOB PRIMARY KEY, position TEXT NOT NULL UNIQUE,
                      payload_sha BLOB NOT NULL, page_serial INTEGER NOT NULL);
                    PRAGMA user_version=1;
                    COMMIT;
                """)
                with db:
                    db.execute("INSERT INTO meta VALUES(1,?,NULL,NULL,NULL,NULL,0,0,0)", (scope_sha256,))
            if db.execute("PRAGMA user_version").fetchone() != (1,) or db.execute(
                "SELECT scope FROM meta WHERE id=1"
            ).fetchone() != (scope_sha256,):
                raise ValueError("consumer checkpoint scope/schema changed")
        except BaseException:
            self.close()
            raise

    def _database(self):
        if self._closed or self._failed or self._db is None:
            raise ValueError("consumer checkpoint is closed/failed")
        return self._db

    def frontier(self) -> tuple[str | None, str | None]:
        return self._database().execute(
            "SELECT frontier_token,frontier_position FROM meta WHERE id=1"
        ).fetchone()

    def pending(self) -> tuple[tuple[int, str], ...]:
        return tuple(self._database().execute("SELECT id,cursor FROM parts WHERE done=0 ORDER BY id"))

    def stats(self) -> dict:
        db = self._database()
        plans, pages, events = db.execute(
            "SELECT plan_serial,page_serial,unique_events FROM meta WHERE id=1"
        ).fetchone()
        wal_bytes = self._wal_bytes()
        return {
            "plan_serial": plans, "page_serial": pages, "unique_checkpointed_events": events,
            "pending_partitions": db.execute("SELECT count(*) FROM parts WHERE done=0").fetchone()[0],
            "database_bytes": db.execute("PRAGMA page_count").fetchone()[0]
            * db.execute("PRAGMA page_size").fetchone()[0],
            "wal_bytes": wal_bytes,
            "checkpoint_profile": "SQLITE_WAL_FULL_BOUNDED_TRUNCATE_V2",
            "wal_truncate_checkpoints": self._checkpoint_count,
            "tokens_exported": False, "qualification": False,
        }

    def _wal_bytes(self) -> int:
        wal = self._path.with_name(self._path.name + "-wal")
        return wal.stat().st_size if wal.exists() else 0

    def _bound_wal(self) -> tuple[bool, int]:
        """Reclaim an oversized WAL after the durable page transaction.

        TRUNCATE is periodic rather than per page.  It is required because a
        successful RESTART resets the logical frame sequence but deliberately
        retains the physical file size, making the byte budget unverifiable.
        With this class's sole connection and no retained read transaction,
        any busy frame is a lifecycle violation rather than a condition to
        retry indefinitely.
        """

        wal_bytes = self._wal_bytes()
        if wal_bytes <= self._wal_limit_bytes:
            return False, wal_bytes
        busy, log_frames, checkpointed_frames = self._database().execute(
            "PRAGMA wal_checkpoint(TRUNCATE)"
        ).fetchone()
        if busy or checkpointed_frames != log_frames:
            self._failed = True
            raise ValueError("consumer WAL checkpoint did not fully truncate")
        self._checkpoint_count += 1
        wal_bytes = self._wal_bytes()
        main_bytes = self._path.stat().st_size
        if wal_bytes > self._wal_limit_bytes or main_bytes + wal_bytes > self._maximum_bytes:
            self._failed = True
            raise ValueError("consumer page checkpoint outcome unavailable; storage budget exceeded")
        return True, wal_bytes

    def begin_plan(self, plan: dict) -> bool:
        db = self._database()
        if self.pending():
            raise ValueError("previous signed interval has not been durably consumed")
        _, previous = self.frontier()
        build_replay_partition_evidence_v2(plan, expected_after_position=previous)
        if any(len(p["cursor"]) > 4096 for p in plan["partitions"]):
            raise ValueError("consumer signed cursor exceeds transport bound")
        scope = db.execute("SELECT journal,generation FROM meta WHERE id=1").fetchone()
        if scope[0] is not None and scope != (plan["journal_id"], plan["stream_generation"]):
            raise ValueError("consumer stream generation changed")
        try:
            with db:
                db.execute(
                    "UPDATE meta SET journal=?,generation=? WHERE id=1",
                    (plan["journal_id"], plan["stream_generation"]),
                )
                if not plan["partitions"]:
                    return False  # Preserve the preceding exhausted token/position.
                db.execute("DELETE FROM parts")
                db.executemany(
                    "INSERT INTO parts VALUES(?,?,?,?,?,0,0,NULL,NULL)",
                    [(p["partition_id"], p["high_watermark_position"], p["event_count"],
                      p["cursor"], p["after_position"]) for p in plan["partitions"]],
                )
                db.execute("UPDATE meta SET plan_serial=plan_serial+1 WHERE id=1")
        except sqlite3.Error:
            self._failed = True
            raise ValueError("consumer plan commit outcome unavailable") from None
        return True

    def _prepare_page(
        self, *, partition_id: int, origin_cursor: str, page: dict
    ) -> _PreparedCheckpointPage | CheckpointedConsumerPage:
        self.last_page_stages = None
        db = self._database()
        row = db.execute(
            "SELECT high,expected,cursor,current_position,received,done,last_origin,last_reply "
            "FROM parts WHERE id=?", (partition_id,),
        ).fetchone()
        if row is None or not isinstance(origin_cursor, str) or not 1 <= len(origin_cursor) <= 4096:
            raise ValueError("consumer partition/origin missing")
        high, expected, cursor, previous, received, done, last_origin, last_reply = row
        events, token, more = page.get("events"), page.get("next_cursor"), page.get("has_more")
        if (
            page.get("high_watermark_position") != high or not isinstance(events, list)
            or not 1 <= len(events) <= 100 or not isinstance(token, str) or not 1 <= len(token) <= 4096
            or type(more) is not bool
        ):
            raise ValueError("consumer page shape/boundary invalid")
        fingerprint = sha256(msgspec.json.encode(page, order="sorted")).digest()
        origin_sha = sha256(origin_cursor.encode()).digest()
        if origin_cursor != cursor:
            if last_origin == origin_sha and last_reply == fingerprint:
                return CheckpointedConsumerPage(0, 0, len(events), monotonic_ns(), self.frontier()[1])
            raise ValueError("consumer stale origin differs from durable cursor")
        if done:
            raise ValueError("consumer partition already exhausted")
        after = None if previous is None else ConsumerEventPositionV2.from_external_id(previous)
        end = ConsumerEventPositionV2.from_external_id(high)
        rows = []
        for event in events:
            if not isinstance(event, dict) or not isinstance(event.get("event_id"), str) or not (
                1 <= len(event["event_id"]) <= 256
            ):
                raise ValueError("consumer event identity invalid")
            position = event.get("position")
            current = ConsumerEventPositionV2.from_external_id(position)
            if (after is not None and current <= after) or current > end:
                raise ValueError("consumer event positions overlap or exceed signed interval")
            rows.append((sha256(event["event_id"].encode()).digest(), position,
                         sha256(msgspec.json.encode(event, order="sorted")).digest()))
            after = current
        count = received + len(rows)
        if count > expected or (
            more and (count == expected or after == end)
        ) or (not more and (count != expected or after != end)) or token == cursor:
            raise ValueError("consumer count/final position/token progression invalid")
        return _PreparedCheckpointPage(
            partition_id,
            token,
            events[-1]["position"],
            count,
            origin_sha,
            fingerprint,
            tuple(rows),
        )

    def commit_pages(
        self, pages: tuple[tuple[int, str, dict], ...]
    ) -> tuple[CheckpointedConsumerPage, ...]:
        """FULL-commit distinct, already-arrived partition pages atomically.

        The caller may only pass responses that have already completed; this
        method never sleeps or waits to form a larger transaction.  A single
        partition is therefore never advanced concurrently, while independent
        signed partitions can share one existing SQLite FULL boundary.
        """

        if not 1 <= len(pages) <= 4 or len({item[0] for item in pages}) != len(pages):
            raise ValueError("consumer checkpoint batch must contain 1..4 distinct partitions")
        validation_started = monotonic_ns()
        prepared = tuple(
            self._prepare_page(partition_id=partition, origin_cursor=origin, page=page)
            for partition, origin, page in pages
        )
        if all(isinstance(item, CheckpointedConsumerPage) for item in prepared):
            return tuple(item for item in prepared if isinstance(item, CheckpointedConsumerPage))
        typed = tuple(item for item in prepared if isinstance(item, _PreparedCheckpointPage))
        db = self._database()
        first_serial = db.execute("SELECT page_serial FROM meta WHERE id=1").fetchone()[0] + 1
        sql_started = monotonic_ns()
        full_before = 0
        try:
            with db:
                unique = 0
                for offset, item in enumerate(typed):
                    serial = first_serial + offset
                    db.executemany(
                        "INSERT INTO events VALUES(?,?,?,?)",
                        [(*row, serial) for row in item.rows],
                    )
                    db.execute(
                        "UPDATE parts SET cursor=?,current_position=?,received=?,done=?,last_origin=?,last_reply=? "
                        "WHERE id=?",
                        (
                            item.token,
                            item.position,
                            item.count,
                            int(item.position == db.execute(
                                "SELECT high FROM parts WHERE id=?", (item.partition_id,)
                            ).fetchone()[0]),
                            item.origin_sha,
                            item.fingerprint,
                            item.partition_id,
                        ),
                    )
                    unique += len(item.rows)
                db.execute(
                    "UPDATE meta SET page_serial=?,unique_events=unique_events+? WHERE id=1",
                    (first_serial + len(typed) - 1, unique),
                )
                if db.execute("SELECT count(*) FROM parts WHERE done=0").fetchone()[0] == 0:
                    frontier, position = db.execute(
                        "SELECT cursor,current_position FROM parts ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                    db.execute(
                        "UPDATE meta SET frontier_token=?,frontier_position=? WHERE id=1",
                        (frontier, position),
                    )
                full_before = monotonic_ns()
            full_after = monotonic_ns()
            wal_truncated, wal_bytes = self._bound_wal()
            progress_after = monotonic_ns() if wal_truncated else full_after
        except sqlite3.Error:
            self._failed = True
            raise ValueError("consumer page checkpoint outcome unavailable") from None
        self.last_page_stages = {
            "validation_started_ns": validation_started, "sql_started_ns": sql_started,
            "full_before_ns": full_before, "full_after_ns": full_after,
            "progress_after_ns": progress_after,
            "unique_events": sum(len(item.rows) for item in typed),
            "pages": len(typed), "full_committed": True,
            "wal_truncated": wal_truncated, "wal_bytes": wal_bytes,
        }
        frontier = self.frontier()[1]
        committed = iter(
            CheckpointedConsumerPage(
                first_serial + offset, len(item.rows), 0, progress_after, frontier
            )
            for offset, item in enumerate(typed)
        )
        return tuple(
            CheckpointedConsumerPage(
                0, 0, item.replayed_events, progress_after, frontier
            )
            if isinstance(item, CheckpointedConsumerPage)
            else next(committed)
            for item in prepared
        )

    def commit_page(
        self, *, partition_id: int, origin_cursor: str, page: dict
    ) -> CheckpointedConsumerPage:
        return self.commit_pages(((partition_id, origin_cursor, page),))[0]

    def close(self) -> None:
        if self._closed:
            return
        failure: BaseException | None = None
        try:
            if self._db is not None:
                if not self._failed:
                    busy, log_frames, checkpointed_frames = self._db.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone()
                    if busy or checkpointed_frames != log_frames:
                        self._failed = True
                        failure = ValueError("consumer final WAL checkpoint did not drain")
                self._db.close()
        finally:
            self._lease.close()
            self._closed = True
        if failure is not None:
            raise failure


class RollingConsumerEventProbe:
    """One bounded public RPC per step; caller owns scheduling and raw timings.

    No producer timestamps are available through PollEvents today. This class
    therefore DOES NOT claim production-to-checkpoint latency or qualification.
    """

    def __init__(self, *, client, checkpoint: ConsumerEventCheckpoint, consumer_account_id: str):
        self._client, self._checkpoint, self._account = client, checkpoint, consumer_account_id
        self._sequence = self._partition_turn = 0
        self._request_prefix = uuid.uuid4().hex

    def _call(self, operation, payload):
        self._sequence += 1
        request_id = f"rolling-events-{self._request_prefix}-{self._sequence}"
        response = self._client.call({
            "message_type": "request", "schema_version": 2, "request_id": request_id,
            "operation": operation, "payload": payload,
        })
        if (
            response.get("message_type") != "response" or type(response.get("schema_version")) is not int
            or response.get("schema_version") != 2
            or response.get("status") != "COMPLETED" or response.get("request_id") != request_id
            or not isinstance(response.get("payload"), dict)
        ):
            raise ValueError("rolling consumer RPC failed; durable checkpoint retained")
        return response["payload"]

    def step(self) -> CheckpointedConsumerPage | None:
        pending = self._checkpoint.pending()
        if not pending:
            token, _ = self._checkpoint.frontier()
            payload = {"consumer_account_id": self._account, "partition_count": 4}
            if token is not None:
                payload["after_cursor"] = token
            self._checkpoint.begin_plan(self._call("CreateReplayPartitions", payload))
            return None
        partition_id, token = pending[self._partition_turn % len(pending)]
        self._partition_turn += 1
        page = self._call("PollEvents", {"cursor": token, "limit": 100})
        return self._checkpoint.commit_page(
            partition_id=partition_id, origin_cursor=token, page=page
        )


@dataclass(frozen=True)
class ConcurrentReplayCall:
    operation: str
    request_id: str
    started_ns: int
    returned_ns: int
    checkpointed_ns: int
    unique_events: int
    replayed_events: int
    page_serial: int


class ConcurrentRollingConsumerEventProbe:
    """Bounded signed-partition polling with one durable checkpoint owner.

    There is at most one RPC in flight per server-signed partition.  Only
    responses already complete when the owner wakes are combined in one FULL
    transaction; the implementation never sleeps to manufacture a larger
    batch and never exports or modifies an opaque cursor.
    """

    def __init__(
        self,
        *,
        client,
        checkpoint: ConsumerEventCheckpoint,
        consumer_account_id: str,
        maximum_inflight: int,
    ) -> None:
        if type(maximum_inflight) is not int or maximum_inflight not in {1, 2, 4}:
            raise ValueError("consumer replay concurrency must be 1, 2, or 4")
        self._client, self._checkpoint, self._account = client, checkpoint, consumer_account_id
        self._maximum_inflight = maximum_inflight
        self._executor = ThreadPoolExecutor(
            max_workers=maximum_inflight, thread_name_prefix="signed-replay"
        )
        self._sequence = 0
        self._request_prefix = uuid.uuid4().hex
        self._inflight: dict[Future[tuple[dict, int, int]], tuple[int, str, str]] = {}
        self.last_calls: tuple[ConcurrentReplayCall, ...] = ()
        self._closed = False

    def _request_id(self) -> str:
        self._sequence += 1
        return f"rolling-events-{self._request_prefix}-{self._sequence}"

    def _call(self, operation: str, payload: dict, request_id: str) -> tuple[dict, int, int]:
        started = monotonic_ns()
        response = self._client.call(
            {
                "message_type": "request",
                "schema_version": 2,
                "request_id": request_id,
                "operation": operation,
                "payload": payload,
            }
        )
        returned = monotonic_ns()
        if (
            response.get("message_type") != "response"
            or response.get("schema_version") != 2
            or response.get("status") != "COMPLETED"
            or response.get("request_id") != request_id
            or not isinstance(response.get("payload"), dict)
        ):
            raise ValueError("rolling consumer RPC failed; durable checkpoint retained")
        return response["payload"], started, returned

    def _begin_plan(self) -> None:
        token, _ = self._checkpoint.frontier()
        payload = {
            "consumer_account_id": self._account,
            "partition_count": self._maximum_inflight,
        }
        if token is not None:
            payload["after_cursor"] = token
        request_id = self._request_id()
        plan, started, returned = self._call("CreateReplayPartitions", payload, request_id)
        self._checkpoint.begin_plan(plan)
        checkpointed = monotonic_ns()
        self.last_calls = (
            ConcurrentReplayCall(
                "CreateReplayPartitions",
                request_id,
                started,
                returned,
                checkpointed,
                0,
                0,
                0,
            ),
        )

    def step(self) -> tuple[CheckpointedConsumerPage, ...]:
        if self._closed:
            raise ValueError("consumer replay probe is closed")
        self.last_calls = ()
        pending = self._checkpoint.pending()
        if not pending and not self._inflight:
            self._begin_plan()
            return ()
        active = {partition for partition, _origin, _request in self._inflight.values()}
        for partition, origin in pending:
            if len(self._inflight) >= self._maximum_inflight:
                break
            if partition in active:
                continue
            request_id = self._request_id()
            future = self._executor.submit(
                self._call, "PollEvents", {"cursor": origin, "limit": 100}, request_id
            )
            self._inflight[future] = (partition, origin, request_id)
            active.add(partition)
        if not self._inflight:
            return ()
        done, _ = wait(tuple(self._inflight), return_when=FIRST_COMPLETED)
        # Drain every response already available at this instant, but never
        # delay the first completed page just to fill a checkpoint batch.
        done |= {future for future in self._inflight if future.done()}
        ordered = sorted(done, key=lambda future: self._inflight[future][0])
        payloads: list[tuple[int, str, dict]] = []
        timings: list[tuple[str, int, int]] = []
        for future in ordered:
            partition, origin, request_id = self._inflight.pop(future)
            payload, started, returned = future.result()
            payloads.append((partition, origin, payload))
            timings.append((request_id, started, returned))
        committed = self._checkpoint.commit_pages(tuple(payloads))
        self.last_calls = tuple(
            ConcurrentReplayCall(
                "PollEvents",
                request_id,
                started,
                returned,
                receipt.checkpointed_ns,
                receipt.unique_events,
                receipt.replayed_events,
                receipt.page_serial,
            )
            for (request_id, started, returned), receipt in zip(timings, committed, strict=True)
        )
        return committed

    def close(self) -> None:
        if self._closed:
            return
        self._executor.shutdown(wait=True, cancel_futures=False)
        self._closed = True
