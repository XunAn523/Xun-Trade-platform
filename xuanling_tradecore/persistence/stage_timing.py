"""Opt-in, owner-local timing counters; the supplied sink MUST be nonblocking.

Only fixed stage names, counts, durations and source ranges are exported. No
SQL text, payload, account, cursor or exception string is accepted here.
Durations are inclusive (nested stages MUST NOT be added as disjoint spans).
"""

from __future__ import annotations

import os
import sqlite3
from threading import get_native_id
from time import monotonic_ns
from typing import Any, Callable, Iterable, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")
_STAGES = frozenset(
    {
        "accept",
        "read",
        "decode",
        "write",
        "completion",
        "select",
        "sql_execute",
        "sql_commit",
        "sql_rollback",
        "reducer",
        "encoding",
        "hashing",
        "batch_validation",
        "source_control",
        "tail_validation",
    }
)


class StageTiming:
    def __init__(self, component: str, sink: Callable[[dict[str, object]], None]) -> None:
        if component not in {"reactor", "projector", "projection-source"}:
            raise ValueError("unknown stage component")
        self.component, self._sink = component, sink
        self.started_ns = monotonic_ns()
        self.counts: dict[str, int] = {}
        self.total_ns: dict[str, int] = {}
        self.maximum_ns: dict[str, int] = {}
        self.closed = False

    def call(self, stage: str, operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        if self.closed or stage not in _STAGES:
            raise ValueError("invalid timing stage or closed observation")
        started = monotonic_ns()
        try:
            return operation(*args, **kwargs)
        finally:
            elapsed = monotonic_ns() - started
            self.counts[stage] = self.counts.get(stage, 0) + 1
            self.total_ns[stage] = self.total_ns.get(stage, 0) + elapsed
            self.maximum_ns[stage] = max(self.maximum_ns.get(stage, 0), elapsed)

    def finish(self, values: dict[str, int | bool]) -> None:
        if (
            self.closed
            or len(values) > 24
            or any(
                type(key) is not str or len(key) > 64 or type(value) not in (int, bool) or value < 0
                for key, value in values.items()
            )
        ):
            raise ValueError("invalid stage summary")
        self.closed = True
        self._sink(
            {
                "schema": "consumer-stage-timing-v1",
                "component": self.component,
                "pid": os.getpid(),
                "tid": get_native_id(),
                "start_ns": self.started_ns,
                "end_ns": monotonic_ns(),
                "inclusive_stages": True,
                "calls": dict(self.counts),
                "total_ns": dict(self.total_ns),
                "maximum_ns": dict(self.maximum_ns),
                "values": dict(values),
            }
        )


class TimedSqliteConnection(sqlite3.Connection):
    """Optional diagnostic connection. Never enabled on the source writer.

    execute excludes subsequent cursor fetching; commit is the actual explicit
    sqlite commit call. SQLite's context-manager exit is not claimed as commit
    here, since CPython can bypass this Python override for __exit__.
    """

    stage_timing: StageTiming | None = None

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        if self.stage_timing is None:
            return super().execute(sql, parameters)
        return self.stage_timing.call("sql_execute", super().execute, sql, parameters)

    def executemany(self, sql: str, parameters: Iterable[Any], /) -> sqlite3.Cursor:
        if self.stage_timing is None:
            return super().executemany(sql, parameters)
        return self.stage_timing.call("sql_execute", super().executemany, sql, parameters)

    def commit(self) -> None:
        if self.stage_timing is None:
            return super().commit()
        return self.stage_timing.call("sql_commit", super().commit)

    def rollback(self) -> None:
        if self.stage_timing is None:
            return super().rollback()
        return self.stage_timing.call("sql_rollback", super().rollback)
