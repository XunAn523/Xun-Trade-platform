from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path

from tools.performance.perfbench.tradecore_mmap_emt_burst import (
    Settings,
    TradeCoreMmapEmtPerfError,
    _aggregate_process_isolated_preflight_reports,
    _allocate_burst_capacity,
    _parse_session_client_ids,
    _partition_worker_cpu_set,
    _split_limit,
)


def _settings(*, scopes: tuple[int, ...] = (94,), ingress_capacity: int = 300) -> Settings:
    return Settings(
        sdk_root=Path("sdk"),
        accounts_file=Path("accounts.json"),
        fixture=Path("fixture.json"),
        output=Path("report.json"),
        runtime_dir=Path("runtime"),
        trading_day="20260902",
        burst_width=300,
        callback_timeout_seconds=30.0,
        native_start_budget_seconds=60.0,
        execute_simulated=True,
        ingress_capacity=ingress_capacity,
        native_inflight_limit=1_000,
        session_client_ids=scopes,
    )


class TradeCoreMmapEmtBurstShardingTests(unittest.TestCase):
    def test_session_limits_are_conserved_across_scopes(self) -> None:
        limits = _split_limit(1_000, ("scope-1", "scope-2", "scope-3"))
        self.assertEqual({"scope-1": 334, "scope-2": 333, "scope-3": 333}, limits)
        self.assertEqual(1_000, sum(limits.values()))

    def test_full_burst_capacity_tracks_hash_skew_without_expanding_cap(self) -> None:
        capacities = _allocate_burst_capacity(300, {"scope-1": 95, "scope-2": 97, "scope-3": 108})
        self.assertEqual({"scope-1": 95, "scope-2": 97, "scope-3": 108}, capacities)

    def test_settings_fail_closed_for_reused_scope_or_missing_ingress_slot(self) -> None:
        with self.assertRaises(TradeCoreMmapEmtPerfError):
            _settings(scopes=(94, 94), ingress_capacity=2).validate()
        with self.assertRaises(TradeCoreMmapEmtPerfError):
            _settings(scopes=(94, 95, 96), ingress_capacity=2).validate()
        _settings(scopes=(94, 95, 96), ingress_capacity=3).validate()

    def test_process_isolation_requires_multiple_sessions(self) -> None:
        with self.assertRaises(TradeCoreMmapEmtPerfError):
            replace(_settings(), isolate_session_processes=True).validate()
        replace(_settings(scopes=(94, 95, 96)), isolate_session_processes=True).validate()

    def test_foreign_cancel_callback_plan_cannot_overlap_own_scope(self) -> None:
        child = replace(
            _settings(),
            _partitioned_scope_worker=True,
            _partition_client_order_ref_base=100_000_000,
            _known_foreign_cancel_client_order_ids=frozenset({100_000_300}),
        )
        child.validate()
        with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "overlaps"):
            replace(
                child,
                _known_foreign_cancel_client_order_ids=frozenset({100_000_299}),
            ).validate()
        with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "outside"):
            replace(child, _known_foreign_cancel_client_order_ids=frozenset({1})).validate()

    def test_session_preflight_requires_isolated_sessions_and_has_no_orders(self) -> None:
        with self.assertRaises(TradeCoreMmapEmtPerfError):
            replace(_settings(), session_preflight_only=True).validate()
        settings = replace(
            _settings(scopes=(94, 95, 96)),
            isolate_session_processes=True,
            session_preflight_only=True,
            session_preflight_timeout_seconds=15.0,
        )
        settings.validate()
        child_settings = replace(
            settings,
            isolate_session_processes=False,
            _partitioned_scope_worker=True,
            session_client_ids=(94,),
            _session_preflight_scope_count=3,
        )
        child_settings.validate()
        self.assertIsNone(_partition_worker_cpu_set(child_settings))
        child = {
            "driver": "tradecore-mmap-emt-session-preflight-process-isolated-child",
            "complete": True,
            "session_preflight": {"mode": "adapter-start-isolated-exit-no-order"},
            "outcomes": {
                "adapter_started": 1,
                "isolated_worker_exit": 1,
                "outbound_mutation_attempted": 0,
            },
            "execution_model": {"worker_cpu_affinity": {}},
            "verification": {"accepted": True},
        }
        report = _aggregate_process_isolated_preflight_reports(
            settings,
            [
                ("scope-1", 94, 100, child),
                ("scope-2", 95, 100, child),
                ("scope-3", 96, 100, child),
            ],
        )
        self.assertTrue(report["complete"])
        self.assertEqual(0, report["outcomes"]["signals_target"])
        self.assertEqual(0, report["outcomes"]["outbound_mutation_attempted"])

    def test_client_scope_parser(self) -> None:
        self.assertEqual((94, 95, 96), _parse_session_client_ids("94,95,96"))
