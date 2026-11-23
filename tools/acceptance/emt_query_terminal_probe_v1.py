"""Run a payload-free raw EMT query-terminal conformance probe.

This acceptance-only utility is deliberately narrower than the gateway query
driver.  It invokes one already-enabled, read-only query through the supplied
native Python binding and records only callback-shape facts needed to diagnose
terminal delivery: argument count, request/session correlation booleans,
``is_last`` state and numeric error IDs.  It never stores a response mapping,
account identifier, endpoint, business argument, order, balance or secret.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Lock
from time import monotonic, perf_counter_ns, sleep
from typing import Any, Mapping, Sequence

from perfbench.native_readonly import (
    NativeBenchmarkError,
    _load_trader_api,
    _suppress_native_console_output,
)
from perfbench.query_fixture import load_query_fixture, select_query_cases
from perfbench.simulated_accounts import SimulatedAccounts, load_simulated_accounts
from xuanling_tradecore.adapters.emt.trading_catalog_v1 import python_api_name
from xuanling_tradecore.compatibility.emt_trading_platform import (
    OFFICIAL_TRADER_SPI,
    python_callback_name,
)


class EmtQueryTerminalProbeError(RuntimeError):
    """The raw terminal probe cannot safely execute or persist evidence."""


_ALLOWED_CASES = frozenset(
    {
        "QueryIPOInfoList",
        "QueryYesterdayAsset",
        "QueryCreditPositionFullRate",
    }
)
_CLIENT_ID = {"ordinary": 91, "credit": 92}
# The raw probe exits through os._exit after persistence.  Retaining this
# native wrapper prevents its known-abort-prone destructor from running before
# the payload-free report reaches disk.
_KEEPALIVE_API: Any | None = None


@dataclass(frozen=True)
class TerminalProbeSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    profile: str
    api_name: str
    business_args: tuple[Any, ...]
    timeout_seconds: float
    output: Path

    def validate(self) -> None:
        if self.profile not in _CLIENT_ID:
            raise EmtQueryTerminalProbeError("profile must be ordinary or credit")
        if self.api_name not in _ALLOWED_CASES:
            raise EmtQueryTerminalProbeError("query API is not approved for raw terminal probing")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 60:
            raise EmtQueryTerminalProbeError("timeout_seconds must be in 0..60")


@dataclass
class _PendingQuery:
    request_id: int
    session_id: int
    callback_count: int = 0
    matching_callback_count: int = 0
    terminal_callback_count: int = 0
    final_error_id: int | None = None
    callback_shapes: list[dict[str, int | bool | None]] = field(default_factory=list)
    done: Event = field(default_factory=Event)


def _error_id(value: object) -> int | None:
    if not isinstance(value, Mapping):
        return None
    candidate = value.get("error_id")
    return candidate if type(candidate) is int else None


def _callback_shape(
    args: tuple[Any, ...],
    *,
    request_id: int,
    session_id: int,
) -> dict[str, int | bool | None]:
    """Return bounded correlation facts without retaining a native payload."""

    callback_request_id = args[-3] if len(args) >= 3 and type(args[-3]) is int else None
    callback_is_last = args[-2] if len(args) >= 2 and type(args[-2]) is bool else None
    callback_session_id = args[-1] if args and type(args[-1]) is int else None
    return {
        "arity": len(args),
        "data_mapping_present": bool(args and isinstance(args[0], Mapping)),
        "error_id": _error_id(args[1]) if len(args) >= 2 else None,
        "request_id_matches": callback_request_id == request_id
        if callback_request_id is not None
        else None,
        "is_last": callback_is_last,
        "session_id_matches": callback_session_id == session_id
        if callback_session_id is not None
        else None,
    }


def _terminal_success(pending: _PendingQuery, native_return_code: int | None) -> bool:
    return (
        native_return_code == 0
        and pending.terminal_callback_count > 0
        and pending.final_error_id in {None, 0}
    )


def _wait_for_terminal(pending: _PendingQuery, timeout_seconds: float) -> bool:
    """Bound terminal waiting despite vendor callback-thread interactions.

    ``threading.Event.wait`` can remain blocked in the vendor binding's
    callback lifecycle even after the requested timeout.  This acceptance
    probe polls only the Event's atomic flag and sleeps outside the GIL; it
    cannot turn an absent terminal callback into a success.
    """

    deadline = monotonic() + timeout_seconds
    while not pending.done.is_set():
        remaining_seconds = deadline - monotonic()
        if remaining_seconds <= 0:
            return False
        sleep(min(remaining_seconds, 0.01))
    return True


def _record_progress(runtime_dir: Path, phase: str) -> None:
    """Persist a payload-free native-call checkpoint for a timed-out probe."""

    (runtime_dir / "terminal-probe-progress.json").write_text(
        json.dumps({"phase": phase}, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _persist_report(path: Path, report: Mapping[str, object]) -> None:
    """Persist the payload-free evidence before returning through native code.

    The vendor wrapper can terminate a one-shot process as it unwinds after a
    completed native query.  The report therefore cannot depend on code after
    :func:`run_terminal_probe` returns.  ``path`` is rejected if it already
    exists by ``main`` so this write cannot overwrite prior evidence.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _make_probe_api(base: type[Any]) -> type[Any]:
    class TerminalProbeApi(base):
        def __init__(self) -> None:
            super().__init__()
            self._pending: _PendingQuery | None = None
            self._errors: list[int] = []
            self._lock = Lock()

        def begin(self, request_id: int, session_id: int) -> _PendingQuery:
            pending = _PendingQuery(request_id=request_id, session_id=session_id)
            with self._lock:
                if self._pending is not None:
                    raise EmtQueryTerminalProbeError("only one raw terminal query may be active")
                self._pending = pending
            return pending

        def native_errors(self) -> tuple[int, ...]:
            with self._lock:
                return tuple(self._errors)

        def _on_query(self, *args: Any) -> None:
            with self._lock:
                pending = self._pending
                if pending is None:
                    return
                pending.callback_count += 1
                shape = _callback_shape(
                    args, request_id=pending.request_id, session_id=pending.session_id
                )
                if len(pending.callback_shapes) < 4:
                    pending.callback_shapes.append(shape)
                if shape["request_id_matches"] is not True:
                    return
                pending.matching_callback_count += 1
                if shape["is_last"] is not True:
                    return
                pending.terminal_callback_count += 1
                pending.final_error_id = shape["error_id"]
                pending.done.set()

        def onConnected(self, *args: Any) -> None:
            return None

        def onDisconnected(self, *args: Any) -> None:
            return None

        def onError(self, *args: Any) -> None:
            error = _error_id(args[0]) if args else None
            if error is not None:
                with self._lock:
                    if len(self._errors) < 4:
                        self._errors.append(error)

        def onQueryIPOInfoList(self, *args: Any) -> None:
            self._on_query(*args)

        def onQueryYesterdayAsset(self, *args: Any) -> None:
            self._on_query(*args)

        def onQueryCreditPositionFullRate(self, *args: Any) -> None:
            self._on_query(*args)

    # Boost.Python resolves a callback attribute while executing a native
    # request.  A single-query probe may still receive a session/control
    # callback that it does not otherwise measure.  Leaving such an attribute
    # absent makes the native wrapper attempt to call ``None`` and turns a
    # useful terminal-semantics result into an opaque TypeError.  Complete the
    # official SPI surface with payload-discarding no-ops, while preserving the
    # three explicitly measured query handlers above.  This does not infer a
    # terminal callback: only ``_on_query`` can set the completion flag.
    def _discard_callback(self: Any, *args: Any) -> None:
        del self, args

    for official_name in OFFICIAL_TRADER_SPI:
        callback_name = python_callback_name(official_name)
        if callback_name not in TerminalProbeApi.__dict__:
            setattr(TerminalProbeApi, callback_name, _discard_callback)

    return TerminalProbeApi


def _account_for(accounts: SimulatedAccounts, profile: str) -> object:
    return getattr(accounts, profile)


def _safe_last_error(api: Any) -> int | None:
    try:
        return _error_id(api.getApiLastError())
    except Exception:
        return None


def run_terminal_probe(settings: TerminalProbeSettings) -> dict[str, object]:
    """Run one raw read-only query and return payload-free terminal evidence."""

    global _KEEPALIVE_API

    settings.validate()
    try:
        trader_api = _load_trader_api(settings.sdk_root)
    except NativeBenchmarkError as exc:
        raise EmtQueryTerminalProbeError(str(exc)) from exc
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    _record_progress(settings.runtime_dir, "binding_loaded")
    api = _make_probe_api(trader_api)()
    _KEEPALIVE_API = api
    account = _account_for(settings.accounts, settings.profile)
    session_id = 0
    try:
        with _suppress_native_console_output():
            _record_progress(settings.runtime_dir, "before_create_trader_api")
            api.createTraderApi(_CLIENT_ID[settings.profile], str(settings.runtime_dir), 0)
            _record_progress(settings.runtime_dir, "after_create_trader_api")
            _record_progress(settings.runtime_dir, "before_login")
            session_id = int(
                api.login(
                    settings.accounts.host,
                    settings.accounts.port,
                    account.account,
                    account.password,
                    1,
                    "",
                )
            )
            _record_progress(settings.runtime_dir, "after_login")
        if session_id == 0:
            raise EmtQueryTerminalProbeError("EMT login failed")
        request_id = 70_001
        pending = api.begin(request_id, session_id)
        started_ns = perf_counter_ns()
        _record_progress(settings.runtime_dir, "before_native_query")
        native_invocation_exception_kind: str | None = None
        try:
            native_return_code: int | None = int(
                getattr(api, python_api_name(settings.api_name))(
                    *settings.business_args, session_id, request_id
                )
            )
        except Exception as exc:
            # Native wrapper failures are bounded to their public Python class
            # name.  Do not retain text: vendor exceptions can echo arguments.
            native_return_code = None
            native_invocation_exception_kind = type(exc).__name__
        _record_progress(settings.runtime_dir, "after_native_query")
        native_return_elapsed_ns = perf_counter_ns() - started_ns
        native_last_error_id = _safe_last_error(api)
        if native_return_code == 0:
            _record_progress(settings.runtime_dir, "before_terminal_wait")
            _wait_for_terminal(pending, settings.timeout_seconds)
            _record_progress(settings.runtime_dir, "after_terminal_wait")
        _record_progress(settings.runtime_dir, "before_report")
        report: dict[str, object] = {
            "schema_version": 1,
            "driver": "emt-query-terminal-probe-v1",
            "safety": {
                "mode": "single-query-read-only",
                "order_entry": False,
                "cancellation": False,
                "fund_or_credit_mutation": False,
                "payload_persisted": False,
            },
            "environment": {
                "native_binding_loaded": True,
                "post_query_native_introspection": False,
            },
            "result": {
                "api": settings.api_name,
                "profile": settings.profile,
                "native_return_code": native_return_code,
                "native_invocation_exception_kind": native_invocation_exception_kind,
                "native_last_error_id": native_last_error_id,
                "native_return_elapsed_ns": native_return_elapsed_ns,
                "callback_count": pending.callback_count,
                "matching_callback_count": pending.matching_callback_count,
                "terminal_callback_count": pending.terminal_callback_count,
                "callback_shapes": pending.callback_shapes,
                "native_on_error_ids": list(api.native_errors()),
                "terminal_verified": _terminal_success(pending, native_return_code),
            },
        }
        _persist_report(settings.output, report)
        return report
    finally:
        # The vendor Python wrapper has exhibited teardown hangs on this host.
        # This one-shot process persists the report first and exits with
        # os._exit below, so an explicit native finalizer adds no evidence and
        # would make timeout evidence unobservable.
        pass


def _select_case(path: Path, *, profile: str, api_name: str) -> tuple[Any, ...]:
    cases = select_query_cases(
        load_query_fixture(path, profile=profile),
        (api_name,),
    )
    if len(cases) != 1:
        raise EmtQueryTerminalProbeError("query fixture selection is not unique")
    return cases[0].business_args


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--query-fixture", type=Path, required=True)
    parser.add_argument("--profile", choices=tuple(sorted(_CLIENT_ID)), required=True)
    parser.add_argument("--query-case", choices=tuple(sorted(_ALLOWED_CASES)), required=True)
    parser.add_argument("--callback-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise EmtQueryTerminalProbeError("output already exists")
    if args.query_case == "QueryCreditPositionFullRate" and args.profile != "credit":
        raise EmtQueryTerminalProbeError("QueryCreditPositionFullRate requires credit profile")
    if args.query_case != "QueryCreditPositionFullRate" and args.profile != "ordinary":
        raise EmtQueryTerminalProbeError("IPO and yesterday asset probes require ordinary profile")
    accounts = load_simulated_accounts(args.accounts_file)
    settings = TerminalProbeSettings(
        sdk_root=args.sdk_root,
        runtime_dir=args.output.parent / f"{args.query_case}-runtime",
        accounts=accounts,
        profile=args.profile,
        api_name=args.query_case,
        business_args=_select_case(
            args.query_fixture, profile=args.profile, api_name=args.query_case
        ),
        timeout_seconds=args.callback_timeout,
        output=args.output,
    )
    run_terminal_probe(settings)
    # ``run_terminal_probe`` persists before it returns so wrapper teardown
    # cannot erase evidence.  Keep this assertion to catch a broken local
    # implementation without rewriting an existing report.
    if not args.output.is_file():
        raise EmtQueryTerminalProbeError("terminal probe failed to persist its report")
    # This one-shot diagnostic must not hang or abort while finalizing the
    # vendor wrapper after evidence has been persisted.
    os._exit(0)


if __name__ == "__main__":  # pragma: no cover - subprocess entry point
    raise SystemExit(main())
