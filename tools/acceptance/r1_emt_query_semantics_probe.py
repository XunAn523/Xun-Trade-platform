"""Bounded, payload-free EMT query-completion semantics probe for R1.

The vendor API documents ``is_last=True`` as the completion signal for both
ordinary and paginated query callbacks.  This driver verifies that signal on
the exact deployed Python binding without retaining rows, credentials, order
identifiers, symbols, prices, or positions.  A timeout, a non-boolean finality
value, an incomplete page chain, or a non-progressing cursor is evidence of an
incomplete query -- never evidence of an empty snapshot.

It intentionally does not issue any mutation, subscribe to a public topic, or
promote an account to READY.  ``--isolate-after-report`` is available only to
avoid known vendor-wrapper finalization stalls after the payload-free report
has been written.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Event
from time import monotonic_ns, sleep
from typing import Any

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)
from tools.performance.perfbench.simulated_accounts import (
    SimulatedAccounts,
    load_simulated_accounts,
)
from tools.performance.s1a_emt_readonly_probe import (
    S1AProbePlan,
    S1AProbePlanError,
    S1AProbeTarget,
    _api_version_matches,
    _error_id,
    _mapping,
    load_s1a_probe_plan,
)


@dataclass(frozen=True)
class R1QuerySemanticsPlan:
    """Approved no-mutation plan for the fixed R1 query-completion matrix."""

    base: S1AProbePlan
    callback_timeout_seconds: float
    page_size: int
    page_inter_request_delay_ms: int
    max_page_requests: int


@dataclass(frozen=True)
class _QueryCase:
    name: str
    callback_name: str
    paged: bool
    invoke: Callable[[Any, int, int, int, int], int]
    profiles: frozenset[str] = frozenset({"ordinary", "credit", "option"})


_GENERIC_CASES: tuple[_QueryCase, ...] = (
    _QueryCase(
        "unfinished_orders",
        "onQueryOrder",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryUnfinishedOrders(
            session_id, request_id
        ),
    ),
    _QueryCase(
        "orders",
        "onQueryOrder",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryOrders(
            {"ticker": "", "begin_time": 0, "end_time": 0}, session_id, request_id
        ),
    ),
    _QueryCase(
        "positions",
        "onQueryPosition",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryPosition(
            "", session_id, request_id
        ),
    ),
    _QueryCase(
        "assets",
        "onQueryAsset",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryAsset(
            session_id, request_id
        ),
        frozenset({"ordinary", "credit"}),
    ),
    _QueryCase(
        "funds",
        "onQueryFund",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryFund(
            session_id, request_id
        ),
        frozenset({"ordinary", "credit"}),
    ),
    _QueryCase(
        "credit_fund_info",
        "onQueryCreditFundInfo",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryCreditFundInfo(
            session_id, request_id
        ),
        frozenset({"credit"}),
    ),
    _QueryCase(
        "credit_asset_debt",
        "onQueryCreditAssetDebtInfo",
        False,
        lambda api, session_id, request_id, _page_size, _reference: api.queryCreditAssetDebtInfo(
            session_id, request_id
        ),
        frozenset({"credit"}),
    ),
    _QueryCase(
        "credit_position_extra_info_sz",
        "onQueryCreditPositionExtraInfo",
        False,
        lambda api, session_id, request_id, _page_size, _reference: (
            api.queryCreditPositionExtraInfo(
                {"market": 1, "ticker": "000000"}, session_id, request_id
            )
        ),
        frozenset({"credit"}),
    ),
    _QueryCase(
        "credit_position_extra_info_sh",
        "onQueryCreditPositionExtraInfo",
        False,
        lambda api, session_id, request_id, _page_size, _reference: (
            api.queryCreditPositionExtraInfo(
                {"market": 2, "ticker": "000000"}, session_id, request_id
            )
        ),
        frozenset({"credit"}),
    ),
)

_PAGED_CASES: tuple[_QueryCase, ...] = (
    _QueryCase(
        "orders_by_page",
        "onQueryOrderByPage",
        True,
        lambda api, session_id, request_id, page_size, reference: api.queryOrdersByPage(
            {"req_count": page_size, "reference": reference, "reserved": 0},
            session_id,
            request_id,
        ),
    ),
    _QueryCase(
        "positions_by_page",
        "onQueryPositionByPage",
        True,
        lambda api, session_id, request_id, page_size, reference: api.queryPositionByPage(
            {"req_count": page_size, "reference": reference, "reserved": 0},
            session_id,
            request_id,
        ),
    ),
    _QueryCase(
        "trades_by_page",
        "onQueryTradeByPage",
        True,
        lambda api, session_id, request_id, page_size, reference: api.queryTradesByPage(
            {"req_count": page_size, "reference": reference, "reserved": 0},
            session_id,
            request_id,
        ),
    ),
)


def _cases_for_profile(profile: str) -> tuple[_QueryCase, ...]:
    return tuple(case for case in (*_GENERIC_CASES, *_PAGED_CASES) if profile in case.profiles)


def load_r1_query_semantics_plan(
    path: Path, *, now: datetime | None = None
) -> R1QuerySemanticsPlan:
    """Load an explicitly approved fixed query matrix; no arbitrary API calls."""

    base = load_s1a_probe_plan(path, now=now)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise S1AProbePlanError("R1 query-semantics plan cannot be reread") from exc
    root = _mapping(raw, "R1 query-semantics plan")
    probe = _mapping(root.get("query_semantics_probe"), "query semantics probe")
    if probe.get("allow_fixed_query_matrix") is not True:
        raise S1AProbePlanError("R1 query-semantics plan does not approve the fixed matrix")
    timeout = probe.get("callback_timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise S1AProbePlanError("R1 query-semantics callback timeout must be numeric")
    if not 1 <= float(timeout) <= 15:
        raise S1AProbePlanError("R1 query-semantics callback timeout must be in 1..15")
    page_size = probe.get("page_size")
    if isinstance(page_size, bool) or not isinstance(page_size, int):
        raise S1AProbePlanError("R1 query-semantics page size must be an integer")
    if not 1 <= page_size <= 200:
        raise S1AProbePlanError("R1 query-semantics page size must be in 1..200")
    delay_ms = probe.get("page_inter_request_delay_ms")
    if isinstance(delay_ms, bool) or not isinstance(delay_ms, int):
        raise S1AProbePlanError("R1 query-semantics page delay must be an integer")
    if not 0 <= delay_ms <= 1_000:
        raise S1AProbePlanError("R1 query-semantics page delay must be in 0..1000")
    max_page_requests = probe.get("max_page_requests")
    if isinstance(max_page_requests, bool) or not isinstance(max_page_requests, int):
        raise S1AProbePlanError("R1 query-semantics max pages must be an integer")
    if not 1 <= max_page_requests <= 512:
        raise S1AProbePlanError("R1 query-semantics max pages must be in 1..512")
    return R1QuerySemanticsPlan(
        base=base,
        callback_timeout_seconds=float(timeout),
        page_size=page_size,
        page_inter_request_delay_ms=delay_ms,
        max_page_requests=max_page_requests,
    )


@dataclass
class _PendingQuery:
    callback_name: str
    started_ns: int
    done: Event = field(default_factory=Event)
    callback_count: int = 0
    false_count: int = 0
    true_count: int = 0
    invalid_finality_count: int = 0
    first_callback_ns: int | None = None
    terminal_callback_ns: int | None = None
    terminal_error_id: int | None = None
    terminal_sequence: int | None = None
    terminal_reference: int | None = None

    def as_report(self) -> dict[str, object]:
        return {
            "callback_count": self.callback_count,
            "last_false_count": self.false_count,
            "last_true_count": self.true_count,
            "invalid_finality_count": self.invalid_finality_count,
            "first_callback_elapsed_ms": _elapsed_ms(self.first_callback_ns, self.started_ns),
            "terminal_callback_elapsed_ms": _elapsed_ms(self.terminal_callback_ns, self.started_ns),
            "terminal_error_id": self.terminal_error_id,
            "terminal_sequence": self.terminal_sequence,
            "terminal_reference": self.terminal_reference,
            "terminal_observed": self.done.is_set(),
        }


def run_r1_query_semantics_probe(
    plan: R1QuerySemanticsPlan,
    *,
    accounts: SimulatedAccounts,
    retain_native_reference: Callable[[object], None] | None = None,
) -> dict[str, object]:
    """Run the fixed terminal/cursor matrix for every approved profile."""

    base = plan.base
    loaded = EmtPlatformLoader(
        SdkLoadSettings(sdk_root=base.sdk_root, expected_hashes=base.expected_library_hashes)
    ).load()
    results = [
        _probe_target(
            loaded.trader_api_type,
            plan=plan,
            target=target,
            accounts=accounts,
            retain_native_reference=retain_native_reference,
        )
        for target in base.targets
    ]
    return {
        "schema_version": "tradecore-r1-query-semantics-probe-v1",
        "driver": "r1-emt-query-semantics-probe",
        "approval_reference": base.approval_reference,
        "operator_id": base.operator_id,
        "sdk": {
            "expected_api_version": base.expected_api_version,
            "library_files": sorted(base.expected_library_hashes),
        },
        "fixed_query_matrix": tuple(case.name for case in (*_GENERIC_CASES, *_PAGED_CASES)),
        "safety": {
            "queries_only": True,
            "public_topic_subscription": False,
            "submit": False,
            "cancel": False,
            "fund_or_credit_mutation": False,
            "callback_payload_persisted": False,
            "ready_promotion": False,
            "native_wrapper_retained_until_isolated_exit": retain_native_reference is not None,
        },
        "targets": results,
    }


def _probe_target(
    trader_api_type: type[Any],
    *,
    plan: R1QuerySemanticsPlan,
    target: S1AProbeTarget,
    accounts: SimulatedAccounts,
    retain_native_reference: Callable[[object], None] | None,
) -> dict[str, object]:
    base = plan.base
    account = getattr(accounts, target.account_config_profile)

    class QuerySemanticsProbeApi(trader_api_type):
        def __init__(self) -> None:
            super().__init__()
            self._pending: dict[int, _PendingQuery] = {}

        def begin(self, request_id: int, callback_name: str) -> _PendingQuery:
            pending = _PendingQuery(callback_name=callback_name, started_ns=monotonic_ns())
            self._pending[request_id] = pending
            return pending

        def abandon(self, request_id: int) -> None:
            self._pending.pop(request_id, None)

        def _generic(self, *args: Any) -> None:
            if len(args) < 5:
                return
            error, request_id, is_last = args[-4], args[-3], args[-2]
            if type(request_id) is not int:
                return
            pending = self._pending.get(request_id)
            if pending is None:
                return
            _record_callback(
                pending,
                is_last=is_last,
                error_id=_error_id_from_callback(error),
                sequence=None,
                reference=None,
            )

        def _page(self, *args: Any) -> None:
            if len(args) < 7:
                return
            sequence, reference, request_id, is_last = args[-5], args[-4], args[-3], args[-2]
            if (
                type(request_id) is not int
                or type(sequence) is not int
                or type(reference) is not int
            ):
                return
            pending = self._pending.get(request_id)
            if pending is None:
                return
            _record_callback(
                pending,
                is_last=is_last,
                error_id=None,
                sequence=sequence,
                reference=reference,
            )

        def _single(self, *args: Any) -> None:
            if len(args) < 4:
                return
            error, request_id = args[-3], args[-2]
            if type(request_id) is not int:
                return
            pending = self._pending.get(request_id)
            if pending is None:
                return
            _record_callback(
                pending,
                is_last=True,
                error_id=_error_id_from_callback(error),
                sequence=None,
                reference=None,
            )

        def onConnected(self) -> None:
            return None

        def onDisconnected(self, *args: Any) -> None:
            del args

        def onError(self, *args: Any) -> None:
            del args

        def onQueryOrder(self, *args: Any) -> None:
            self._generic(*args)

        def onQueryPosition(self, *args: Any) -> None:
            self._generic(*args)

        def onQueryAsset(self, *args: Any) -> None:
            self._generic(*args)

        def onQueryFund(self, *args: Any) -> None:
            self._single(*args)

        def onQueryCreditFundInfo(self, *args: Any) -> None:
            self._single(*args)

        def onQueryCreditAssetDebtInfo(self, *args: Any) -> None:
            self._single(*args)

        def onQueryCreditPositionExtraInfo(self, *args: Any) -> None:
            self._generic(*args)

        def onQueryOrderByPage(self, *args: Any) -> None:
            self._page(*args)

        def onQueryPositionByPage(self, *args: Any) -> None:
            self._page(*args)

        def onQueryTradeByPage(self, *args: Any) -> None:
            self._page(*args)

    api: Any | None = None
    session_id = 0
    api_version: str | None = None
    trading_day: str | None = None
    login_error_id: int | None = None
    failure_kind: str | None = None
    cases: dict[str, object] = {}
    try:
        api = QuerySemanticsProbeApi()
        with suppress_native_console_output():
            api.createTraderApi(target.client_id, str(_runtime_dir(target)), 0)
            api.setSoftwareVersion("tradecore/r1-query-semantics")
            api.setHeartBeatInterval(10)
            session_id = int(
                api.login(
                    accounts.host,
                    accounts.port,
                    account.account,
                    account.password,
                    1,
                    "",
                )
            )
        if session_id:
            api_version = str(api.getApiVersion())
            trading_day = str(api.getTradingDay())
            request_id = 700_000 + (target.client_id * 100)
            for case in _GENERIC_CASES:
                if target.account_config_profile not in case.profiles:
                    continue
                request_id += 1
                cases[case.name] = _run_one(
                    api=api,
                    session_id=session_id,
                    case=case,
                    request_id=request_id,
                    timeout_seconds=plan.callback_timeout_seconds,
                    page_size=plan.page_size,
                    reference=0,
                )
            for case in _PAGED_CASES:
                if target.account_config_profile not in case.profiles:
                    continue
                paged, request_id = _run_pages(
                    api=api,
                    session_id=session_id,
                    case=case,
                    first_request_id=request_id + 1,
                    timeout_seconds=plan.callback_timeout_seconds,
                    page_size=plan.page_size,
                    inter_request_delay_ms=plan.page_inter_request_delay_ms,
                    max_page_requests=plan.max_page_requests,
                )
                cases[case.name] = paged
        else:
            login_error_id = _error_id(api)
    except Exception as exc:
        failure_kind = type(exc).__name__
    finally:
        if api is not None:
            if retain_native_reference is not None:
                retain_native_reference(api)
            else:
                _close(api, session_id)

    return {
        "account_config_profile": target.account_config_profile,
        "adapter_profile_id": target.adapter_profile_id,
        "adapter_instance_id": target.adapter_instance_id,
        "adapter_client_scope": target.adapter_client_scope,
        "login_succeeded": session_id != 0,
        "api_version": api_version,
        "api_version_matches_expected": _api_version_matches(
            base.expected_api_version, api_version
        ),
        "trading_day": trading_day,
        "login_error_id": login_error_id,
        "failure_kind": failure_kind,
        "queries": cases,
    }


def _run_one(
    *,
    api: Any,
    session_id: int,
    case: _QueryCase,
    request_id: int,
    timeout_seconds: float,
    page_size: int,
    reference: int,
) -> dict[str, object]:
    pending = api.begin(request_id, case.callback_name)
    return_code = int(case.invoke(api, session_id, request_id, page_size, reference))
    if return_code != 0:
        api.abandon(request_id)
        return {
            "query_return_code": return_code,
            "native_error_id": _error_id(api),
            "request_id": request_id,
            "completion": "native_return_nonzero",
            **pending.as_report(),
        }
    pending.done.wait(timeout_seconds)
    api.abandon(request_id)
    completion = (
        "terminal"
        if pending.done.is_set() and pending.terminal_error_id in (None, 0)
        else "terminal_error"
        if pending.done.is_set()
        else "callback_timeout"
    )
    return {
        "query_return_code": return_code,
        "native_error_id": None,
        "request_id": request_id,
        "completion": completion,
        **pending.as_report(),
    }


def _run_pages(
    *,
    api: Any,
    session_id: int,
    case: _QueryCase,
    first_request_id: int,
    timeout_seconds: float,
    page_size: int,
    inter_request_delay_ms: int,
    max_page_requests: int,
) -> tuple[dict[str, object], int]:
    reference = 0
    page_request_count = 0
    last_page: dict[str, object] | None = None
    pages: list[dict[str, object]] = []
    for ordinal in range(max_page_requests):
        request_id = first_request_id + ordinal
        page_request_count += 1
        current = _run_one(
            api=api,
            session_id=session_id,
            case=case,
            request_id=request_id,
            timeout_seconds=timeout_seconds,
            page_size=page_size,
            reference=reference,
        )
        last_page = current
        pages.append(current)
        if current["completion"] != "terminal":
            completion = (
                "page_native_return_nonzero"
                if current["completion"] == "native_return_nonzero"
                else "page_terminal_missing"
            )
            return (
                {
                    "completion": completion,
                    "page_request_count": page_request_count,
                    "last_page": current,
                    "pages": pages,
                },
                first_request_id + page_request_count - 1,
            )
        sequence = current["terminal_sequence"]
        next_reference = current["terminal_reference"]
        if not isinstance(sequence, int) or not isinstance(next_reference, int):
            return (
                {
                    "completion": "page_metadata_invalid",
                    "page_request_count": page_request_count,
                    "last_page": current,
                    "pages": pages,
                },
                first_request_id + page_request_count - 1,
            )
        if sequence < page_size:
            return (
                {
                    "completion": "page_chain_complete",
                    "page_request_count": page_request_count,
                    "last_page": current,
                    "pages": pages,
                },
                first_request_id + page_request_count - 1,
            )
        if sequence != page_size or next_reference <= reference:
            return (
                {
                    "completion": "page_cursor_invalid",
                    "page_request_count": page_request_count,
                    "last_page": current,
                    "pages": pages,
                },
                first_request_id + page_request_count - 1,
            )
        reference = next_reference
        if inter_request_delay_ms:
            sleep(inter_request_delay_ms / 1_000)
    return (
        {
            "completion": "page_bound_exceeded",
            "page_request_count": page_request_count,
            "max_page_requests": max_page_requests,
            "last_page": last_page,
            "pages": pages,
        },
        first_request_id + page_request_count - 1,
    )


def _record_callback(
    pending: _PendingQuery,
    *,
    is_last: object,
    error_id: int | None,
    sequence: int | None,
    reference: int | None,
) -> None:
    now_ns = monotonic_ns()
    pending.callback_count += 1
    if pending.first_callback_ns is None:
        pending.first_callback_ns = now_ns
    if is_last is False:
        pending.false_count += 1
        return
    if is_last is not True:
        pending.invalid_finality_count += 1
        return
    pending.true_count += 1
    pending.terminal_callback_ns = now_ns
    pending.terminal_error_id = error_id
    pending.terminal_sequence = sequence
    pending.terminal_reference = reference
    pending.done.set()


def _error_id_from_callback(value: object) -> int | None:
    if not isinstance(value, Mapping):
        return None
    error_id = value.get("error_id")
    return error_id if type(error_id) is int else None


def _elapsed_ms(timestamp_ns: int | None, started_ns: int) -> float | None:
    return None if timestamp_ns is None else round((timestamp_ns - started_ns) / 1_000_000, 3)


def _runtime_dir(target: S1AProbeTarget) -> Path:
    directory = (
        Path("data/performance/probes/emt/r1-query-semantics") / target.account_config_profile
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _close(api: Any, session_id: int) -> None:
    with suppress_native_console_output():
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()


def _result_passed(item: Mapping[str, object]) -> bool:
    if (
        item.get("login_succeeded") is not True
        or item.get("api_version_matches_expected") is not True
        or item.get("failure_kind") is not None
    ):
        return False
    queries = item.get("queries")
    if not isinstance(queries, Mapping):
        return False
    profile = item.get("account_config_profile")
    if type(profile) is not str:
        return False
    return all(
        isinstance(queries.get(case.name), Mapping)
        and queries[case.name].get("completion") in ("terminal", "page_chain_complete")
        for case in _cases_for_profile(profile)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--isolate-after-report", action="store_true")
    args = parser.parse_args()
    plan = load_r1_query_semantics_plan(args.plan)
    retained: list[object] = []
    report = run_r1_query_semantics_probe(
        plan,
        accounts=load_simulated_accounts(args.accounts_file),
        retain_native_reference=retained.append if args.isolate_after_report else None,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    targets = report["targets"]
    if not isinstance(targets, list) or not all(isinstance(item, Mapping) for item in targets):
        raise RuntimeError("R1 query-semantics probe generated an invalid target report")
    passed = all(_result_passed(item) for item in targets)
    print(f"R1_QUERY_SEMANTICS_PASS={passed}", flush=True)
    exit_code = 0 if passed else 2
    if args.isolate_after_report:
        os._exit(exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
