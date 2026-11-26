from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from queue import Empty, Full
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast

from tools.performance.perfbench.tradecore_mmap_emt_burst import (
    Settings,
    _aggregate_process_isolated_reports,
    _batch_completion_summary,
    _BurstTiming,
    _callback_origin_classification,
    _callback_stage_coverage,
    _callback_substage_measurement,
    _durations,
    _fixture_for_run,
    _ForeignCancelOrderDirectory,
    _load_expected_library_hashes,
    _mmap_journal_frame_capacity,
    _native_lane_id,
    _parse_cpu_list,
    _partition_worker_cpu_set,
    _partition_worker_failure_report,
    _process_isolated_child_failure_report,
    _remaining_cleanup_cooldown_ns,
    _safe_partition_failure_kind,
    _scope_cpu_assignments,
    _ShardedCallbackObserver,
    _SpscCallbackRing,
    _summary,
    _summary_map,
)
from tools.performance.perfbench.tradecore_mmap_emt_support import (
    TradeCoreMmapEmtPerfError,
    load_tight_burst_order_plan,
    require_armed_simulated_fixture,
    require_armed_tight_burst_fixture,
)
from tools.performance.perfbench.tradecore_tight_burst_order_plan import (
    materialize_historical_controlled_fixture_order_plan,
)
from xuanling_tradecore.ems import CallbackPersistenceProfileV1


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "sdk_root": Path("sdk"),
        "accounts_file": Path("accounts"),
        "fixture": Path("fixture"),
        "output": Path("report.json"),
        "runtime_dir": Path("runtime"),
        "trading_day": "20260905",
        "burst_width": 300,
        "callback_timeout_seconds": 120.0,
        "native_start_budget_seconds": 180.0,
        "execute_simulated": True,
    }
    values.update(overrides)
    return Settings(**values)


class TradeCoreMmapEmtBurstTests(unittest.TestCase):
    def test_partition_failure_kind_retains_bounded_exception_type_chain(self) -> None:
        self.assertEqual(
            "TradeCoreMmapEmtPerfError:ValueError",
            _safe_partition_failure_kind("TradeCoreMmapEmtPerfError:ValueError"),
        )
        self.assertEqual(
            "TradeCoreMmapEmtPerfError:ValueError:error_id=7",
            _safe_partition_failure_kind("TradeCoreMmapEmtPerfError:ValueError:error_id=7"),
        )
        self.assertEqual(
            "CHILD_FAILURE_UNCLASSIFIED",
            _safe_partition_failure_kind("TradeCoreMmapEmtPerfError:unsafe detail 7"),
        )

    def test_direct_callback_candidate_requires_isolation_and_bypasses_the_target_spsc_lane(
        self,
    ) -> None:
        with self.assertRaisesRegex(Exception, "requires process-isolated"):
            _settings(
                callback_persistence_profile=CallbackPersistenceProfileV1.DIRECT_CANDIDATE
            ).validate()
        _settings(
            isolate_session_processes=True,
            session_client_ids=(94, 95),
            ingress_capacity=2,
            callback_persistence_profile=CallbackPersistenceProfileV1.DIRECT_CANDIDATE,
        ).validate()

        timing = _BurstTiming(1)
        timing.register(index=0, execution_id="exec-1", client_order_ref="101")
        event = SimpleNamespace(
            received_monotonic_ns=1,
            venue_order_identity=SimpleNamespace(client_order_ref="101"),
        )
        delivered: list[tuple[object, ...]] = []
        observer = _ShardedCallbackObserver(
            callback_router=cast(
                Any,
                SimpleNamespace(
                    route_event=lambda _event: SimpleNamespace(
                        event=event, binding=SimpleNamespace(shard_id="scope-1")
                    )
                ),
            ),
            callback_lanes_by_client_ref={},
            source_shard_id="scope-1",
            direct_target_observer=SimpleNamespace(
                on_broker_events=lambda events: delivered.append(events)
            ),
            persistence_profile=CallbackPersistenceProfileV1.DIRECT_CANDIDATE,
            downstream=cast(Any, SimpleNamespace(on_callback_gap=lambda _evidence: None)),
            timing=timing,
        )
        observer.on_broker_event(event)
        self.assertEqual([(event,)], delivered)
        stages = timing.consume("callback:101")
        self.assertIn("C5A_AFTER_SHARD_ENQUEUE", stages)
        self.assertIn("C5B_TARGET_WORKER_START", stages)

    def test_loads_sdk_hash_allowlist_for_isolated_workers(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "sdk-hashes.json"
            path.write_text(json.dumps({"vnemttrader.so": "a" * 64}), encoding="utf-8")
            self.assertEqual({"vnemttrader.so": "a" * 64}, _load_expected_library_hashes(path))
            path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "empty"):
                _load_expected_library_hashes(path)

    def test_prebuilt_submit_mapping_is_default_with_an_explicit_dynamic_rollback(self) -> None:
        self.assertTrue(_settings().prebuild_submit_mapping)
        self.assertFalse(_settings(prebuild_submit_mapping=False).prebuild_submit_mapping)

    def test_zero_order_session_preflight_does_not_need_a_performance_burst_width(self) -> None:
        settings = _settings(
            burst_width=3,
            ingress_capacity=3,
            native_inflight_limit=3,
            session_client_ids=(94, 95, 96),
            isolate_session_processes=True,
            session_preflight_only=True,
        )
        settings.validate()
        with self.assertRaisesRegex(Exception, "session preflight burst width"):
            _settings(
                burst_width=0,
                ingress_capacity=1,
                native_inflight_limit=1,
                session_client_ids=(94, 95, 96),
                isolate_session_processes=True,
                session_preflight_only=True,
            ).validate()

    def test_process_aggregate_requires_terminal_cleanup_evidence(self) -> None:
        settings = _settings(
            burst_width=300,
            ingress_capacity=300,
            native_inflight_limit=300,
            session_client_ids=(94, 95),
            isolate_session_processes=True,
        )
        base_report = {
            "burst_width": 150,
            "complete": True,
            "payload_free_timing_samples_ns": [
                {
                    "test_index": index,
                    "C0_INGRESS": 100,
                    "C4_AFTER_SDK": 120,
                    "C4A_AFTER_ACCEPTED_JOURNAL": 125,
                    "C5_CALLBACK_INGRESS": 130,
                    "C5D_PYTHON_DRAIN_BEGIN": 131,
                    "C5E_AFTER_SEALED_ROUTE": 132,
                    "C5A_AFTER_SHARD_ENQUEUE": 133,
                    "C5B_TARGET_WORKER_START": 134,
                    "C5B2_AFTER_ROUTE_STATE_VALIDATION": 135,
                    "C5B3_AFTER_CALLBACK_MMAP_HASH": 136,
                    "C5M0_BEFORE_CALLBACK_MMAP_COMMIT": 137,
                    "C5M1_AFTER_CALLBACK_MMAP_COMMIT": 138,
                    "C6_AFTER_CALLBACK_MMAP_COMMIT": 139,
                    "C7_AFTER_DOWNSTREAM": 150,
                }
                for index in range(150)
            ],
            "outcomes": {
                "native_accepted": 150,
                "callback_completed_C7": 150,
                "callback_seen_downstream": 150,
                "callback_gaps": 0,
                "foreign_cancel_directory_ready": True,
                "cleanup_cancel_attempted": 150,
                "cleanup_cancel_accepted": 150,
                "cleanup_terminal_callback_complete": True,
                "cleanup_terminal_inflight_remaining": 0,
                "producer_error_kinds": {},
                "producer_rejection_kinds": {},
                "dispatch_error_kinds": {},
                "callback_error_kinds": {},
                "cleanup_error_kinds": {},
                "expired_before_native": 0,
            },
            "verification": {"accepted": True},
            "execution_model": {
                "worker_cpu_affinity": {},
                "submit_route_preparation": {
                    "prepared_before_C0": True,
                    "sealed_lock_free_lookup_at_C1": True,
                    "per_session_shard": {
                        "prepared_submit_mapping": True,
                        "prepared_submit_side_effects": False,
                    },
                },
            },
            "persistence_profile": {},
            "runtime_attribution": {"capture": "test"},
            "runtime_control": {},
            "measurement_boundaries": {},
            "callback_note": "",
        }
        aggregate = _aggregate_process_isolated_reports(
            settings,
            [("scope-1", 94, 150, base_report), ("scope-2", 95, 150, base_report)],
        )
        self.assertTrue(aggregate["complete"])
        self.assertTrue(aggregate["outcomes"]["callback_stage_coverage"]["complete"])
        self.assertEqual(
            "COMPLETE",
            aggregate["outcomes"]["callback_stage_coverage"]["failure_classification"],
        )
        self.assertTrue(aggregate["outcomes"]["cleanup_terminal_callback_complete"])
        self.assertEqual(0, aggregate["outcomes"]["cleanup_terminal_inflight_remaining"])
        self.assertEqual(
            {"capture": "test"},
            aggregate["runtime_attribution"]["per_scope"]["scope-1"],
        )
        self.assertTrue(
            aggregate["execution_model"]["submit_route_preparation"]["per_scope"]["scope-1"][
                "per_session_shard"
            ]["prepared_submit_mapping"]
        )
        incomplete = dict(base_report)
        incomplete["outcomes"] = dict(base_report["outcomes"])
        incomplete["outcomes"]["cleanup_terminal_inflight_remaining"] = 1
        aggregate = _aggregate_process_isolated_reports(
            settings,
            [("scope-1", 94, 150, base_report), ("scope-2", 95, 150, incomplete)],
        )
        self.assertFalse(aggregate["complete"])

    def test_partition_scope_cpu_set_requires_every_role_cpu(self) -> None:
        settings = _settings(
            _partitioned_scope_worker=True,
            native_worker_cpu=180,
            ingress_producer_cpu=181,
            callback_ingress_cpu=182,
            callback_journal_cpu=183,
        )
        self.assertEqual((180, 181, 182, 183), _partition_worker_cpu_set(settings))
        missing_role = _settings(
            _partitioned_scope_worker=True,
            native_worker_cpu=180,
            ingress_producer_cpu=181,
            callback_ingress_cpu=182,
        )
        with self.assertRaisesRegex(Exception, "requires every role CPU"):
            _partition_worker_cpu_set(missing_role)

    def test_pre_c0_child_failure_is_reported_without_claiming_callback_loss(self) -> None:
        child = _partition_worker_failure_report(
            _settings(burst_width=167),
            "TradeCoreMmapEmtPerfError",
            "CPU_AFFINITY_PENDING",
        )
        self.assertEqual("CPU_AFFINITY_PENDING", child["failure_phase"])
        self.assertEqual("TradeCoreMmapEmtPerfError", child["failure_kind"])
        aggregate = _process_isolated_child_failure_report(
            _settings(burst_width=499),
            [("scope-1", 167, child)],
        )
        self.assertEqual("PRE_C0_CHILD_STARTUP", aggregate["failure_stage"])
        self.assertEqual(
            "NOT_STARTED_PRE_C0",
            aggregate["outcomes"]["callback_stage_coverage"]["failure_classification"],
        )
        self.assertEqual(
            "CPU_AFFINITY_PENDING",
            aggregate["outcomes"]["per_scope_failure"]["scope-1"]["failure_phase"],
        )

    def test_preallocated_callback_ring_preserves_fifo_and_bounds_capacity(self) -> None:
        ring = _SpscCallbackRing(capacity=2)
        ring.put_nowait("first")
        ring.put_nowait("second")
        with self.assertRaises(Full):
            ring.put_nowait("third")
        self.assertEqual("first", ring.get_nowait())
        self.assertEqual("second", ring.get(timeout=0.01))
        with self.assertRaises(Empty):
            ring.get_nowait()

    def test_foreign_cancel_directory_never_matches_own_or_unpublished_order(self) -> None:
        shared_order_ids = [0, 0, 0, 0]
        first = _ForeignCancelOrderDirectory(
            shared_order_ids=shared_order_ids, own_slot_start=0, own_slot_count=2
        )
        second = _ForeignCancelOrderDirectory(
            shared_order_ids=shared_order_ids, own_slot_start=2, own_slot_count=2
        )
        first.publish_owned_order_ids((101, 102))
        second.publish_owned_order_ids((201, 202))
        self.assertFalse(first.is_known_foreign_order_id(201))
        first.mark_ready()
        second.mark_ready()
        self.assertFalse(first.is_known_foreign_order_id(101))
        self.assertTrue(first.is_known_foreign_order_id(201))
        self.assertFalse(second.is_known_foreign_order_id(201))
        self.assertFalse(second.is_known_foreign_order_id(999))

    def test_summary_uses_nearest_rank(self) -> None:
        self.assertEqual(
            {"count": 4, "p50_ns": 20, "p95_ns": 40, "p99_ns": 40, "p100_ns": 40},
            _summary([10, 20, 30, 40]),
        )

    def test_mmap_frame_capacity_reserves_bounded_cleanup_outcomes(self) -> None:
        self.assertEqual(1_024, _mmap_journal_frame_capacity(1))
        self.assertEqual(2_124, _mmap_journal_frame_capacity(499))

    def test_duration_omits_incomplete_or_non_monotonic_pairs(self) -> None:
        self.assertEqual(
            [20], _durations([{"C0": 10, "C1": 30}, {"C0": 30, "C1": 10}, {"C0": 10}], "C0", "C1")
        )

    def test_callback_substage_measurement_requires_every_monotonic_pair(self) -> None:
        complete = {
            "C5_CALLBACK_INGRESS": 10,
            "C5D_PYTHON_DRAIN_BEGIN": 11,
            "C5E_AFTER_SEALED_ROUTE": 12,
            "C5A_AFTER_SHARD_ENQUEUE": 13,
            "C5B_TARGET_WORKER_START": 14,
            "C5B2_AFTER_ROUTE_STATE_VALIDATION": 15,
            "C5B3_AFTER_CALLBACK_MMAP_HASH": 16,
            "C5M0_BEFORE_CALLBACK_MMAP_COMMIT": 17,
            "C5M1_AFTER_CALLBACK_MMAP_COMMIT": 18,
            "C6_AFTER_CALLBACK_MMAP_COMMIT": 19,
            "C7_AFTER_DOWNSTREAM": 20,
        }
        self.assertTrue(_callback_substage_measurement([complete, dict(complete)])["complete"])
        incomplete = dict(complete)
        incomplete.pop("C5B_TARGET_WORKER_START")
        coverage = _callback_substage_measurement([complete, incomplete])
        self.assertFalse(coverage["complete"])
        self.assertEqual(
            1,
            coverage["adjacent_pair_counts"]["C5A_AFTER_SHARD_ENQUEUE->C5B_TARGET_WORKER_START"],
        )

    def test_callback_stage_coverage_classifies_the_first_missing_boundary(self) -> None:
        complete = {
            "C5_CALLBACK_INGRESS": 10,
            "C5D_PYTHON_DRAIN_BEGIN": 11,
            "C5E_AFTER_SEALED_ROUTE": 12,
            "C5A_AFTER_SHARD_ENQUEUE": 13,
            "C5B_TARGET_WORKER_START": 14,
            "C5B2_AFTER_ROUTE_STATE_VALIDATION": 15,
            "C5B3_AFTER_CALLBACK_MMAP_HASH": 16,
            "C5M0_BEFORE_CALLBACK_MMAP_COMMIT": 17,
            "C5M1_AFTER_CALLBACK_MMAP_COMMIT": 18,
            "C6_AFTER_CALLBACK_MMAP_COMMIT": 19,
            "C7_AFTER_DOWNSTREAM": 20,
        }
        no_callback = _callback_stage_coverage([{}])
        self.assertFalse(no_callback["complete"])
        self.assertEqual("C5_CALLBACK_INGRESS", no_callback["first_incomplete_stage"])
        self.assertEqual("NO_MATCHED_CALLBACK_TO_C5", no_callback["failure_classification"])

        incomplete = dict(complete)
        incomplete.pop("C5D_PYTHON_DRAIN_BEGIN")
        coverage = _callback_stage_coverage([complete, incomplete])
        self.assertFalse(coverage["complete"])
        self.assertEqual("C5D_PYTHON_DRAIN_BEGIN", coverage["first_incomplete_stage"])
        self.assertEqual(
            "CALLBACK_PIPELINE_INCOMPLETE_BEFORE_C5D_PYTHON_DRAIN_BEGIN",
            coverage["failure_classification"],
        )

    def test_native_callback_diagnostics_distinguish_no_sdk_delivery_from_later_loss(self) -> None:
        coverage = _callback_stage_coverage([{}])
        no_native_delivery = {
            "enabled": True,
            "order_event_entries": 0,
            "trade_event_entries": 0,
            "cancel_error_entries": 0,
        }
        self.assertEqual(
            "NO_NATIVE_ORDER_SIDE_CALLBACK_TO_PYTHON_BRIDGE",
            _callback_origin_classification(coverage, no_native_delivery),
        )
        bridge_seen = {**no_native_delivery, "order_event_entries": 1}
        self.assertEqual(
            "NATIVE_CALLBACK_NOT_DELIVERED_TO_C5",
            _callback_origin_classification(coverage, bridge_seen),
        )

    def test_batch_completion_requires_one_shared_c0_and_every_completion(self) -> None:
        summary = _batch_completion_summary(
            [
                {"C0_INGRESS": 100, "C4_AFTER_SDK": 120},
                {"C0_INGRESS": 100, "C4_AFTER_SDK": 150},
            ],
            "C4_AFTER_SDK",
        )
        self.assertEqual(50, summary["duration_ns"])
        self.assertTrue(summary["complete"])
        self.assertEqual(2, summary["signals_completed"])
        incomplete = _batch_completion_summary(
            [
                {"C0_INGRESS": 100, "C4_AFTER_SDK": 120},
                {"C0_INGRESS": 101},
            ],
            "C4_AFTER_SDK",
        )
        self.assertFalse(incomplete["complete"])
        self.assertIsNone(incomplete["duration_ns"])

    def test_summary_map_records_the_whole_burst_through_last_c7(self) -> None:
        summary = _summary_map(
            [
                {
                    "C0_INGRESS": 100,
                    "C0P_BEFORE_TIGHT_SUBMIT": 105,
                    "C4_AFTER_SDK": 120,
                    "C4A_AFTER_ACCEPTED_JOURNAL": 125,
                    "C5_CALLBACK_INGRESS": 130,
                    "C7_AFTER_DOWNSTREAM": 150,
                },
                {
                    "C0_INGRESS": 100,
                    "C0P_BEFORE_TIGHT_SUBMIT": 110,
                    "C4_AFTER_SDK": 130,
                    "C4A_AFTER_ACCEPTED_JOURNAL": 135,
                    "C5_CALLBACK_INGRESS": 150,
                    "C7_AFTER_DOWNSTREAM": 190,
                },
            ]
        )
        batch = summary["batch_completion"]
        self.assertEqual(90, batch["C0_to_last_C7_after_downstream"]["duration_ns"])
        self.assertTrue(batch["C0_to_last_C7_after_downstream"]["complete"])
        self.assertEqual(60, batch["first_C5_to_last_C7_after_downstream"]["duration_ns"])
        self.assertTrue(batch["first_C5_to_last_C7_after_downstream"]["complete"])
        self.assertEqual(25, batch["first_C0P_to_last_C4_after_sdk"]["duration_ns"])
        self.assertEqual(85, batch["first_C0P_to_last_C7_after_downstream"]["duration_ns"])

    def test_tight_single_producer_requires_one_session_and_full_capacity(self) -> None:
        settings = _settings(
            burst_width=499,
            ingress_capacity=499,
            native_inflight_limit=499,
            ingress_workers=1,
            submission_topology="tight-single-producer",
            tight_burst_order_plan=Path("order-plan.json"),
        )
        settings.validate()
        with self.assertRaisesRegex(Exception, "exactly one serial EMT session"):
            _settings(
                burst_width=499,
                ingress_capacity=499,
                native_inflight_limit=499,
                ingress_workers=1,
                session_client_ids=(94, 95),
                submission_topology="tight-single-producer",
                tight_burst_order_plan=Path("order-plan.json"),
            ).validate()

    def test_tight_burst_order_plan_is_fresh_passive_and_payload_free(self) -> None:
        raw = {
            "schema_version": 1,
            "purpose": "tradecore-tight-499-order-plan",
            "environment": "simulated",
            "selection_mode": "external-read-only-market-preflight",
            "expires_at_unix_s": 200,
            "ticker": "600000",
            "market": 2,
            "side": 1,
            "price_type": 1,
            "price": "9.90",
            "quantity": 100,
            "price_tick": "0.01",
            "best_bid_price": "9.91",
            "last_price": "10.00",
            "lower_limit_price": "9.00",
            "upper_limit_price": "11.00",
            "max_total_buy_notional": "500000",
        }
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            plan = load_tight_burst_order_plan(path, burst_width=499, now_unix_s=100)
        self.assertEqual(
            {"ticker", "market", "side", "price_type", "price", "quantity", "business_type"},
            set(plan.request),
        )
        self.assertEqual(64, len(plan.plan_sha256))
        self.assertEqual("external-read-only-market-preflight", plan.selection_mode)

    def test_tight_burst_historical_fixture_plan_is_distinguished_from_live_preflight(self) -> None:
        raw = {
            "schema_version": 1,
            "purpose": "tradecore-tight-499-order-plan",
            "environment": "simulated",
            "selection_mode": "historical-controlled-fixture",
            "expires_at_unix_s": 200,
            "ticker": "000010",
            "market": 1,
            "side": 1,
            "price_type": 1,
            "price": "1.57",
            "quantity": 100,
            "price_tick": "0.01",
            "price_min": "1.57",
            "price_max": "1.59",
            "max_total_buy_notional": "100000",
            "source_fixture_sha256": "311b56f81b3ad0fabe120d485b597704a5e9dd7badb63e0ab347bc318a4070e4",
        }
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            plan = load_tight_burst_order_plan(path, burst_width=499, now_unix_s=100)
        self.assertEqual(1, plan.request["market"])
        self.assertEqual("historical-controlled-fixture", plan.selection_mode)

    def test_historical_simulated_plan_may_be_indefinite_but_live_plan_may_not(self) -> None:
        historical = {
            "schema_version": 1,
            "purpose": "tradecore-tight-499-order-plan",
            "environment": "simulated",
            "selection_mode": "historical-controlled-fixture",
            "expires_at_unix_s": None,
            "ticker": "688981",
            "market": 2,
            "side": 1,
            "price_type": 1,
            "price": "1.57",
            "quantity": 100,
            "price_tick": "0.01",
            "price_min": "1.57",
            "price_max": "1.59",
            "max_total_buy_notional": "100000",
            "source_fixture_sha256": "311b56f81b3ad0fabe120d485b597704a5e9dd7badb63e0ab347bc318a4070e4",
        }
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "historical-plan.json"
            path.write_text(json.dumps(historical), encoding="utf-8")
            self.assertEqual(
                "688981", load_tight_burst_order_plan(path, burst_width=499).request["ticker"]
            )
            live = {**historical, "selection_mode": "external-read-only-market-preflight"}
            live.pop("price_min")
            live.pop("price_max")
            live.pop("source_fixture_sha256")
            live.update(
                {
                    "best_bid_price": "1.58",
                    "last_price": "1.60",
                    "lower_limit_price": "1.00",
                    "upper_limit_price": "2.00",
                }
            )
            live_path = Path(temporary) / "live-plan.json"
            live_path.write_text(json.dumps(live), encoding="utf-8")
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "requires an expiry"):
                load_tight_burst_order_plan(live_path, burst_width=499)

    def test_historical_plan_materializer_accepts_no_expiry(self) -> None:
        fixture = (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        with TemporaryDirectory() as temporary:
            fixture_path = Path(temporary) / "fixture.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            raw["safety"]["armed"] = True
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            output = Path(temporary) / "plan.json"
            materialize_historical_controlled_fixture_order_plan(
                fixture=fixture_path,
                output=output,
                expires_at_unix_s=None,
                now_unix_s=100,
            )
            self.assertIsNone(json.loads(output.read_text(encoding="utf-8"))["expires_at_unix_s"])

    def test_tight_burst_execution_requires_current_armed_matching_fixture(self) -> None:
        fixture = (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        with TemporaryDirectory() as temporary:
            fixture_path = Path(temporary) / "fixture.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "not armed"):
                require_armed_tight_burst_fixture(fixture_path, expected_sha256=None)

            raw["safety"]["armed"] = True
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            plan_path = Path(temporary) / "plan.json"
            materialize_historical_controlled_fixture_order_plan(
                fixture=fixture_path,
                output=plan_path,
                expires_at_unix_s=None,
                now_unix_s=100,
            )
            plan = load_tight_burst_order_plan(plan_path, burst_width=499, now_unix_s=100)
            require_armed_tight_burst_fixture(
                fixture_path, expected_sha256=plan.source_fixture_sha256
            )

            raw["safety"]["armed"] = False
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "not armed"):
                require_armed_tight_burst_fixture(
                    fixture_path, expected_sha256=plan.source_fixture_sha256
                )

    def test_all_real_order_fixture_paths_require_an_explicit_arm(self) -> None:
        fixture = (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        with TemporaryDirectory() as temporary:
            fixture_path = Path(temporary) / "fixture.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "not armed"):
                require_armed_simulated_fixture(fixture_path)
            with self.assertRaisesRegex(TradeCoreMmapEmtPerfError, "not armed"):
                _fixture_for_run(_settings(fixture=fixture_path))

            raw["safety"]["armed"] = True
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertIsInstance(require_armed_simulated_fixture(fixture_path), bytes)
            loaded, evidence = _fixture_for_run(_settings(fixture=fixture_path))
            self.assertEqual("000010", loaded["ticker"])
            self.assertIsNone(evidence)

    def test_local_historical_plan_is_materialized_from_an_armed_fixture(self) -> None:
        fixture = (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        with TemporaryDirectory() as temporary:
            fixture_path = Path(temporary) / "fixture.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            raw["safety"]["armed"] = True
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            output = Path(temporary) / "plan.json"
            plan_hash = materialize_historical_controlled_fixture_order_plan(
                fixture=fixture_path,
                output=output,
                expires_at_unix_s=200,
                now_unix_s=100,
            )
            loaded = load_tight_burst_order_plan(output, burst_width=499, now_unix_s=100)
        self.assertEqual(plan_hash, loaded.plan_sha256)
        self.assertEqual("historical-controlled-fixture", loaded.selection_mode)

    def test_local_historical_plan_accepts_the_constrained_legacy_fixture_shape(self) -> None:
        fixture = (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        with TemporaryDirectory() as temporary:
            fixture_path = Path(temporary) / "fixture.json"
            raw = json.loads(fixture.read_text(encoding="utf-8"))
            raw["safety"]["armed"] = True
            raw["safety"].pop("account_profile")
            raw["safety"].pop("allowed_tickers")
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            output = Path(temporary) / "plan.json"
            plan_hash = materialize_historical_controlled_fixture_order_plan(
                fixture=fixture_path,
                output=output,
                expires_at_unix_s=200,
                now_unix_s=100,
            )
            loaded = load_tight_burst_order_plan(output, burst_width=499, now_unix_s=100)
        self.assertEqual(plan_hash, loaded.plan_sha256)
        self.assertEqual("historical-controlled-fixture", loaded.selection_mode)

    def test_cleanup_cooldown_starts_after_last_native_submit(self) -> None:
        self.assertEqual(
            1_100_000_000, _remaining_cleanup_cooldown_ns(latest_native_return_ns=0, now_ns=10)
        )
        self.assertEqual(
            0,
            _remaining_cleanup_cooldown_ns(
                latest_native_return_ns=1_000_000_000, now_ns=2_100_000_000
            ),
        )

    def test_native_lane_affinity_is_stable(self) -> None:
        first = _native_lane_id(shard_id="scope-1", execution_id="exec-1", lane_count=4)
        self.assertEqual(
            first, _native_lane_id(shard_id="scope-1", execution_id="exec-1", lane_count=4)
        )
        self.assertIn(
            first, {"scope-1:native-1", "scope-1:native-2", "scope-1:native-3", "scope-1:native-4"}
        )

    def test_only_approved_burst_widths_and_one_caller_are_allowed(self) -> None:
        with self.assertRaisesRegex(Exception, "burst width"):
            _settings(burst_width=301).validate()
        _settings(burst_width=499).validate()
        with self.assertRaisesRegex(Exception, "exactly one native caller"):
            _settings(same_session_insert_callers=2).validate()

    def test_hot_single_order_baseline_requires_serial_isolated_contract(self) -> None:
        hot = _settings(
            burst_width=100,
            hot_single_order_samples=100,
            ingress_capacity=1,
            ingress_workers=1,
            native_inflight_limit=1,
            isolate_session_processes=True,
        )
        hot.validate()
        replace(hot, session_client_ids=(94, 95, 96, 97)).validate()
        child_hot = replace(
            hot,
            burst_width=25,
            hot_single_order_samples=25,
            isolate_session_processes=False,
            _partitioned_scope_worker=True,
            native_worker_cpu=1,
            ingress_producer_cpu=2,
            callback_ingress_cpu=3,
            callback_journal_cpu=4,
        )
        child_hot.validate()
        with self.assertRaisesRegex(Exception, "must equal burst width"):
            _settings(
                burst_width=300,
                hot_single_order_samples=100,
                ingress_capacity=1,
                ingress_workers=1,
                native_inflight_limit=1,
                isolate_session_processes=True,
            ).validate()
        with self.assertRaisesRegex(Exception, "requires one ingress"):
            _settings(
                burst_width=100,
                hot_single_order_samples=100,
                ingress_capacity=2,
                ingress_workers=1,
                native_inflight_limit=1,
                isolate_session_processes=True,
            ).validate()

    def test_process_isolated_cpu_plan_assigns_a_disjoint_affinity_quartet_per_scope(self) -> None:
        settings = _settings(
            session_client_ids=(94, 95, 96),
            isolate_session_processes=True,
            ingress_capacity=3,
            native_worker_cpus=(1, 4, 7),
            ingress_producer_cpus=(2, 5, 8),
            callback_ingress_cpus=(3, 6, 9),
            callback_journal_cpus=(10, 11, 12),
        )
        settings.validate()
        self.assertEqual(
            ((1, 2, 3, 10, None, None), (4, 5, 6, 11, None, None), (7, 8, 9, 12, None, None)),
            _scope_cpu_assignments(settings),
        )

    def test_process_isolated_sdk_cpu_plan_requires_a_disjoint_pair(self) -> None:
        base = {
            "session_client_ids": (94, 95),
            "isolate_session_processes": True,
            "ingress_capacity": 2,
            "native_worker_cpus": (1, 7),
            "ingress_producer_cpus": (2, 8),
            "callback_ingress_cpus": (3, 9),
            "callback_journal_cpus": (4, 10),
        }
        with self.assertRaisesRegex(Exception, "both receive and process lists"):
            _settings(**(base | {"sdk_receive_cpus": (5, 11)})).validate()
        with self.assertRaisesRegex(Exception, "distinct CPUs"):
            _settings(
                **(
                    base
                    | {
                        "sdk_receive_cpus": (5, 11),
                        "sdk_process_cpus": (6, 7),
                    }
                )
            ).validate()
        settings = _settings(**(base | {"sdk_receive_cpus": (5, 11), "sdk_process_cpus": (6, 12)}))
        settings.validate()
        self.assertEqual(
            ((1, 2, 3, 4, 5, 6), (7, 8, 9, 10, 11, 12)),
            _scope_cpu_assignments(settings),
        )

    def test_process_isolated_cpu_plan_fails_closed_when_incomplete_or_shared(self) -> None:
        with self.assertRaisesRegex(Exception, "require native, producer, ingress and journal"):
            _settings(
                session_client_ids=(94, 95),
                isolate_session_processes=True,
                ingress_capacity=2,
                native_worker_cpus=(1, 2),
            ).validate()
        with self.assertRaisesRegex(Exception, "distinct CPUs"):
            _settings(
                session_client_ids=(94, 95),
                isolate_session_processes=True,
                ingress_capacity=2,
                native_worker_cpus=(1, 2),
                ingress_producer_cpus=(3, 4),
                callback_ingress_cpus=(5, 6),
                callback_journal_cpus=(7, 2),
            ).validate()

    def test_cpu_list_parser_rejects_empty_duplicate_and_negative_values(self) -> None:
        self.assertEqual((1, 2, 3), _parse_cpu_list("1, 2,3"))
        for value in ("", "1,,2", "1,1", "-1"):
            with self.assertRaises(Exception):
                _parse_cpu_list(value)

    def test_zero_order_session_preflight_does_not_open_an_order_fixture(self) -> None:
        settings = _settings(
            fixture=Path("missing-controlled-order-fixture.json"),
            session_preflight_only=True,
        )
        fixture, evidence = _fixture_for_run(settings)
        self.assertEqual("000001", fixture["ticker"])
        self.assertIsNone(evidence)
