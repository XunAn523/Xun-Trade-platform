"""Native EMT read-only benchmark driver.

The module intentionally has no import-time dependency on the vendor SDK and
never stores credentials. It measures a safe subset of real queries: the
native call return boundary (C0→C1) and terminal callback (C0→C5).
"""

from __future__ import annotations

import ctypes
import os
import platform
import sys
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock
from time import perf_counter_ns, sleep
from typing import Any

from .histogram import ExactHistogram


class NativeBenchmarkError(RuntimeError):
    """Raised when the native SDK cannot produce a valid benchmark sample."""


@contextmanager
def _suppress_native_console_output() -> Any:
    """Discard C/C++ stdout and stderr while the vendor SDK receives secrets.

    EMT's native binding can write login parameters directly through the C
    runtime on Linux as well as Windows, bypassing Python logging and normal
    ``redirect_stdout``. Redirect file descriptors 1 and 2 to the operating-
    system null device for this short critical section; no secret-bearing log
    file is created.
    """

    _flush_native_stdio()
    stdout_fd, stderr_fd = 1, 2
    saved_stdout, saved_stderr = os.dup(stdout_fd), os.dup(stderr_fd)
    null_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(null_fd, stdout_fd)
        os.dup2(null_fd, stderr_fd)
        yield
    finally:
        _flush_native_stdio()
        os.dup2(saved_stdout, stdout_fd)
        os.dup2(saved_stderr, stderr_fd)
        os.close(null_fd)
        os.close(saved_stdout)
        os.close(saved_stderr)


def _flush_native_stdio() -> None:
    """Flush C/C++ streams while their file descriptors point at null."""

    try:
        runtime = ctypes.CDLL("msvcrt") if os.name == "nt" else ctypes.CDLL(None)
        flush = runtime.fflush
        flush.argtypes = [ctypes.c_void_p]
        flush.restype = ctypes.c_int
        flush(None)
    except (AttributeError, OSError, TypeError):
        return


@dataclass(frozen=True)
class NativeSettings:
    sdk_root: Path
    host: str
    port: int
    account: str
    password: str
    client_id: int = 81
    local_ip: str = ""
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 0.25
    runtime_dir: Path = Path("artifacts/performance/native-runtime")

    def validate(self) -> None:
        if sys.version_info[:2] != (3, 10):
            raise NativeBenchmarkError(
                "the supplied Windows EMT binding requires CPython 3.10; "
                f"current interpreter is {sys.version.split()[0]}"
            )
        if not self.sdk_root.is_dir():
            raise NativeBenchmarkError(f"SDK root does not exist: {self.sdk_root}")
        if not self.host or not self.account or not self.password:
            raise NativeBenchmarkError("host, account, and password are required")
        if not 1 <= self.port <= 65535:
            raise NativeBenchmarkError("port must be in 1..65535")
        if not 1 <= self.client_id <= 255:
            raise NativeBenchmarkError("client_id must be in 1..255")
        if self.callback_timeout_seconds <= 0 or self.interval_seconds < 0:
            raise NativeBenchmarkError("timeout must be positive and interval non-negative")


@dataclass(frozen=True)
class QueryCase:
    name: str
    callback: str
    invoke: Callable[[Any, int, int], int]


def _query_orders(api: Any, session: int, request_id: int) -> int:
    return api.queryOrders({"ticker": "", "begin_time": 0, "end_time": 0}, session, request_id)


def _query_trades(api: Any, session: int, request_id: int) -> int:
    return api.queryTrades({"ticker": "", "begin_time": 0, "end_time": 0}, session, request_id)


def _query_callback_parts(args: tuple[Any, ...]) -> tuple[dict[str, Any], int, bool]:
    """Normalize the two callback shapes exposed by the Python 2.27 binding."""

    if len(args) == 4:  # data, error, request_id, session (QueryFund)
        error, request_id, is_last = args[1], args[2], True
    elif len(args) >= 5:  # data, error, request_id, is_last, session
        error, request_id, is_last = args[-4], args[-3], args[-2]
    else:
        raise NativeBenchmarkError(f"unexpected EMT query callback shape: {len(args)} arguments")
    return (error if isinstance(error, dict) else {}, int(request_id), bool(is_last))


# These calls retrieve state only. Operations that create, cancel, transfer,
# repay, exercise, or combine orders are deliberately absent.
READ_ONLY_CASES: tuple[QueryCase, ...] = (
    QueryCase(
        "QueryAsset", "onQueryAsset", lambda api, session, reqid: api.queryAsset(session, reqid)
    ),
    QueryCase(
        "QueryFund", "onQueryFund", lambda api, session, reqid: api.queryFund(session, reqid)
    ),
    QueryCase(
        "QueryPosition",
        "onQueryPosition",
        lambda api, session, reqid: api.queryPosition("", session, reqid),
    ),
    QueryCase(
        "QueryUnfinishedOrders",
        "onQueryOrder",
        lambda api, session, reqid: api.queryUnfinishedOrders(session, reqid),
    ),
    QueryCase("QueryOrders", "onQueryOrder", _query_orders),
    QueryCase("QueryTrades", "onQueryTrade", _query_trades),
    QueryCase(
        "QueryIPOInfoList",
        "onQueryIPOInfoList",
        lambda api, session, reqid: api.queryIPOInfoList(session, reqid),
    ),
    QueryCase(
        "QueryIPOQuotaInfo",
        "onQueryIPOQuotaInfo",
        lambda api, session, reqid: api.queryIPOQuotaInfo(session, reqid),
    ),
    QueryCase(
        "QueryIssueInfoList",
        "onQueryIssueInfoList",
        lambda api, session, reqid: api.queryIssueInfoList(session, reqid),
    ),
    QueryCase(
        "QueryYesterdayAsset",
        "onQueryYesterdayAsset",
        lambda api, session, reqid: api.queryYesterdayAsset(session, reqid),
    ),
)


@dataclass
class _PendingRequest:
    name: str
    started_ns: int
    done: Event = field(default_factory=Event)
    callback_ns: int | None = None
    error_id: int | None = None


def _load_trader_api(sdk_root: Path) -> type[Any]:
    """Load the platform native module only after its dynamic-library path is set."""

    sdk_root = sdk_root.resolve()
    system_name = platform.system()
    if system_name == "Windows":
        library_dir = sdk_root / "lib" / "windows"
        if not library_dir.is_dir():
            raise NativeBenchmarkError(f"Windows SDK library directory is missing: {library_dir}")
        os.add_dll_directory(str(library_dir))
        os.environ["PATH"] = f"{library_dir}{os.pathsep}{os.environ.get('PATH', '')}"
    elif system_name == "Linux":
        library_dir = sdk_root / "lib" / "linux"
        if not library_dir.is_dir():
            raise NativeBenchmarkError(f"Linux SDK library directory is missing: {library_dir}")
        os.environ["LD_LIBRARY_PATH"] = (
            f"{library_dir}{os.pathsep}{os.environ.get('LD_LIBRARY_PATH', '')}"
        )
    else:
        raise NativeBenchmarkError(f"unsupported OS for EMT native benchmark: {system_name}")
    if str(library_dir) not in sys.path:
        sys.path.insert(0, str(library_dir))
    try:
        from vnemttrader import (
            TraderApi,  # type: ignore[import-not-found]  # vendor SDK is supplied outside Git
        )
    except ImportError as exc:
        raise NativeBenchmarkError(f"cannot import vnemttrader from {library_dir}: {exc}") from exc
    return TraderApi


def _make_measured_api(base: type[Any]) -> type[Any]:
    """Build the SPI subclass after the native base class is available."""

    class MeasuredTraderApi(base):
        def __init__(self) -> None:
            super().__init__()
            self._pending: dict[int, _PendingRequest] = {}
            self._pending_lock = Lock()

        def begin(self, request_id: int, name: str, started_ns: int) -> _PendingRequest:
            pending = _PendingRequest(name=name, started_ns=started_ns)
            with self._pending_lock:
                self._pending[request_id] = pending
            return pending

        def abandon(self, request_id: int) -> None:
            with self._pending_lock:
                self._pending.pop(request_id, None)

        def _terminal_query(self, *args: Any) -> None:
            error, request_id, is_last = _query_callback_parts(args)
            if not is_last:
                return
            with self._pending_lock:
                pending = self._pending.get(request_id)
            if pending is None:
                return
            pending.callback_ns = perf_counter_ns()
            pending.error_id = int(error.get("error_id", 0)) if error else 0
            pending.done.set()

        # The Boost.Python wrapper requires all callbacks that can be emitted
        # during session lifecycle to be callable, even when this driver does
        # not use their payloads.
        def onConnected(self) -> None:
            return None

        def onDisconnected(self, *args: Any) -> None:
            return None

        def onError(self, *args: Any) -> None:
            return None

        def onQueryAsset(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryFund(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryPosition(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryOrder(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryTrade(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryIPOInfoList(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryIPOQuotaInfo(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryIssueInfoList(self, *args: Any) -> None:
            self._terminal_query(*args)

        def onQueryYesterdayAsset(self, *args: Any) -> None:
            self._terminal_query(*args)

    return MeasuredTraderApi


class NativeReadOnlyBench:
    """One sequential, rate-limited session against a real EMT test account."""

    def __init__(self, settings: NativeSettings) -> None:
        settings.validate()
        self.settings = settings
        self.api: Any | None = None
        self.session: int | None = None
        self._request_id = 10_000

    def connect(self) -> dict[str, Any]:
        trader_api = _load_trader_api(self.settings.sdk_root)
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.api = _make_measured_api(trader_api)()
        # EMT_LOG_LEVEL_FATAL(0): the vendor INFO logger prints login
        # parameters, which must never reach the benchmark console or report.
        with _suppress_native_console_output():
            self.api.createTraderApi(self.settings.client_id, str(self.settings.runtime_dir), 0)
            started_ns = perf_counter_ns()
            session = int(
                self.api.login(
                    self.settings.host,
                    self.settings.port,
                    self.settings.account,
                    self.settings.password,
                    1,
                    self.settings.local_ip,
                )
            )
        login_ns = perf_counter_ns() - started_ns
        if session == 0:
            error = self.api.getApiLastError()
            self.close()
            raise NativeBenchmarkError(f"EMT login failed: error_id={error.get('error_id')}")
        self.session = session
        return {
            "login_ns": login_ns,
            "api_version": str(self.api.getApiVersion()),
            "trading_day": str(self.api.getTradingDay()),
        }

    def close(self) -> None:
        if self.api is None:
            return
        try:
            if self.session:
                self.api.logout(self.session)
        finally:
            try:
                self.api.release()
            finally:
                self.api = None
                self.session = None

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def measure(self, case: QueryCase) -> dict[str, Any]:
        if self.api is None or self.session is None:
            raise NativeBenchmarkError("connect must complete before a query is measured")
        request_id = self._next_request_id()
        started_ns = perf_counter_ns()
        pending = self.api.begin(request_id, case.name, started_ns)
        return_code = int(case.invoke(self.api, self.session, request_id))
        returned_ns = perf_counter_ns()
        if return_code != 0:
            self.api.abandon(request_id)
            error = self.api.getApiLastError()
            return {
                "ok": False,
                "return_code": return_code,
                "error_id": error.get("error_id"),
                "return_ns": returned_ns - started_ns,
            }
        if not pending.done.wait(self.settings.callback_timeout_seconds):
            self.api.abandon(request_id)
            return {
                "ok": False,
                "return_code": return_code,
                "timeout": True,
                "return_ns": returned_ns - started_ns,
            }
        callback_ns = pending.callback_ns
        if callback_ns is None:
            raise NativeBenchmarkError("callback event completed without a timestamp")
        return {
            "ok": pending.error_id in (None, 0),
            "return_code": return_code,
            "error_id": pending.error_id,
            "return_ns": returned_ns - started_ns,
            "callback_ns": callback_ns - started_ns,
        }


def _empty_summary() -> dict[str, int | None]:
    return {
        "count": 0,
        "min_ns": None,
        "p50_ns": None,
        "p95_ns": None,
        "p99_ns": None,
        "p999_ns": None,
        "max_ns": None,
    }


def run_read_only(settings: NativeSettings, *, iterations: int) -> dict[str, Any]:
    """Run a low-rate, sequential baseline and return an account-free report."""

    if iterations <= 0:
        raise ValueError("iterations must be positive")
    bench = NativeReadOnlyBench(settings)
    try:
        login = bench.connect()
        operations: list[dict[str, Any]] = []
        for case in READ_ONLY_CASES:
            return_histogram = ExactHistogram()
            callback_histogram = ExactHistogram()
            failures: list[dict[str, Any]] = []
            for _ in range(iterations):
                result = bench.measure(case)
                return_histogram.record(int(result["return_ns"]))
                if result.get("callback_ns") is not None:
                    callback_histogram.record(int(result["callback_ns"]))
                if not result["ok"]:
                    failures.append(
                        {
                            key: result[key]
                            for key in ("return_code", "error_id", "timeout")
                            if key in result and result[key] is not None
                        }
                    )
                if settings.interval_seconds:
                    sleep(settings.interval_seconds)
            operations.append(
                {
                    "api": case.name,
                    "callback": case.callback,
                    "attempts": iterations,
                    "successes": iterations - len(failures),
                    "failures": failures,
                    "C0_to_C1": return_histogram.summary(),
                    "C0_to_C5": callback_histogram.summary()
                    if callback_histogram.count
                    else _empty_summary(),
                }
            )
        return {
            "schema_version": 2,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "driver": "native-readonly",
            "safety": {
                "mode": "sequential-read-only",
                "order_entry": False,
                "cancellation": False,
                "fund_or_credit_mutation": False,
                "interval_seconds": settings.interval_seconds,
            },
            "environment": {
                "python": sys.version,
                "platform": platform.platform(),
                "machine": platform.machine(),
            },
            "login": login,
            "operations": operations,
        }
    finally:
        bench.close()
