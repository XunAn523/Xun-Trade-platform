from __future__ import annotations

import dataclasses
import unittest

from xuanling_tradecore.domain import (
    AdapterClientScopeOwnership,
    AdapterRouteToken,
    CancelDeadlinePolicy,
    ConsumerApiVersion,
    ConsumerEventCursorV2,
    ConsumerEventPositionV2,
    ConsumerWireContractV2,
    DomainValidationError,
    ExecutionDeadlinePolicy,
    RecoveryAutoResolutionEligibility,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    RedisStreamIdV1,
    RouteDirectoryCapacityPlan,
    RouteTokenBinding,
    TradingDayEpoch,
)

SHA_A = "a" * 64
SHA_B = "b" * 64


def _profile() -> RecoveryEvidenceProfile:
    return RecoveryEvidenceProfile(
        profile_id="emt-recovery-v1",
        version=1,
        profile_hash=SHA_A,
        required_query_kinds=frozenset(
            {"orders", "trades", "working_orders", "assets", "positions"}
        ),
        requires_public_replay=True,
        requires_callback_gap_coverage=True,
        requires_route_token_echo=True,
        requires_negative_confirmation=True,
        requires_stable_double_scan_without_as_of=True,
        max_snapshot_age_ns=1_000,
    )


def _certificate(**overrides: object) -> RecoveryCompletenessCertificate:
    values: dict[str, object] = {
        "certificate_id": "certificate-1",
        "certificate_hash": SHA_B,
        "profile_id": "emt-recovery-v1",
        "profile_hash": SHA_A,
        "account_id": "account-1",
        "venue_id": "EMT",
        "adapter_instance_id": "emt-sim-v1",
        "adapter_client_scope": "7",
        "trading_day": TradingDayEpoch("2026-09-01"),
        "query_digests": {
            "orders": SHA_A,
            "trades": SHA_A,
            "working_orders": SHA_A,
            "assets": SHA_A,
            "positions": SHA_A,
        },
        "callback_replay_complete": True,
        "callback_gaps_closed": True,
        "route_token_echo_complete": True,
        "negative_confirmation_available": True,
        "snapshot_window_stable": True,
        "captured_at_wall_ns": 100,
        "completed_at_wall_ns": 200,
    }
    values.update(overrides)
    return RecoveryCompletenessCertificate(
        **values  # type: ignore[reportArgumentType] # Dynamic fixture overrides exercise validation branches.
    )


class TradeCoreDomainContractTests(unittest.TestCase):
    def test_wire_contract_requires_v2_big_endian_and_draft_2020_12(self) -> None:
        contract = ConsumerWireContractV2(
            api_version=ConsumerApiVersion(2, 0),
            schema_hash=SHA_A,
            golden_vectors_hash=SHA_B,
            compatibility_matrix_hash=SHA_A,
            header_size_bytes=32,
            max_frame_bytes=65_536,
        )
        self.assertEqual("BIG_ENDIAN", contract.byte_order)
        with self.assertRaises(DomainValidationError):
            dataclasses.replace(contract, api_version=ConsumerApiVersion(1, 0))
        with self.assertRaises(DomainValidationError):
            dataclasses.replace(contract, byte_order="LITTLE_ENDIAN")

    def test_numeric_event_position_orders_and_round_trips_canonically(self) -> None:
        position_9 = ConsumerEventPositionV2(RedisStreamIdV1(9, 0), 2)
        position_10 = ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 0)
        same_stream_next = ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 1)
        self.assertLess(position_9, position_10)
        self.assertLess(position_10, same_stream_next)
        encoded = same_stream_next.to_external_id()
        self.assertTrue(encoded.startswith("ce2_"))
        self.assertEqual(same_stream_next, ConsumerEventPositionV2.from_external_id(encoded))
        self.assertEqual(RedisStreamIdV1(10, 2), RedisStreamIdV1.parse_redis_id("10-2"))
        with self.assertRaises(DomainValidationError):
            RedisStreamIdV1.parse_redis_id("10:2")
        with self.assertRaises(DomainValidationError):
            ConsumerEventPositionV2.from_external_id("10-2:1")

    def test_cursor_is_binding_generation_and_authorization_epoch_scoped(self) -> None:
        cursor = ConsumerEventCursorV2(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            journal_id="journal-1",
            stream_generation=2,
            after_position=ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 1),
            authorization_epoch=4,
            issued_at_wall_ns=100,
            expires_at_wall_ns=200,
            cursor_key_id="cursor-key-2",
        )
        self.assertFalse(cursor.is_begin)
        self.assertTrue(dataclasses.replace(cursor, after_position=None).is_begin)
        with self.assertRaises(DomainValidationError):
            dataclasses.replace(cursor, expires_at_wall_ns=100)

    def test_deadline_policies_are_versioned_and_fail_on_uint64_overflow(self) -> None:
        execution = ExecutionDeadlinePolicy("execution-v1", 1, SHA_A, 50)
        cancel = CancelDeadlinePolicy("cancel-v1", 1, SHA_B, 75)
        self.assertEqual(150, execution.deadline_for(100))
        self.assertEqual(175, cancel.deadline_for(100))
        with self.assertRaises(DomainValidationError):
            execution.deadline_for((1 << 64) - 25)

    def test_scope_ownership_requires_probe_validity_and_revocation_window(self) -> None:
        ownership = AdapterClientScopeOwnership(
            ownership_id="ownership-1",
            version=1,
            venue_id="EMT",
            account_id="account-1",
            adapter_instance_id="emt-sim-v1",
            adapter_client_scope="7",
            owner_instance_id="tradecore-1",
            config_source_hash=SHA_A,
            signer_id="release-signer-1",
            valid_from_wall_ns=100,
            valid_until_wall_ns=300,
            uniqueness_probe_hash=SHA_B,
            uniqueness_probe_succeeded=True,
            revoked_at_wall_ns=250,
        )
        self.assertTrue(ownership.is_active_at(200))
        self.assertFalse(ownership.is_active_at(250))
        self.assertFalse(
            dataclasses.replace(ownership, uniqueness_probe_succeeded=False).is_active_at(200)
        )

    def test_route_token_is_opaque_and_binding_requires_all_echo_paths(self) -> None:
        token = AdapterRouteToken(1, bytes(range(1, 17)))
        self.assertEqual(token, AdapterRouteToken.parse(token.to_text()))
        self.assertNotIn("strategy", token.to_text())
        with self.assertRaises(DomainValidationError):
            AdapterRouteToken(1, b"short")

        binding = RouteTokenBinding(
            binding_id="emt-order-client-id-v1",
            version=1,
            manifest_hash=SHA_A,
            venue_id="EMT",
            native_field_name="order_client_id",
            native_field_type="uint32",
            max_encoded_bytes=10,
            allowed_characters="[0-9]+",
            write_path="InsertOrder.order_client_id",
            order_callback_paths=("OnOrderEvent.order_client_id",),
            trade_callback_paths=("OnTradeEvent.order_client_id",),
            cancel_error_paths=("OnCancelOrderError.order_client_id",),
            query_paths=("QueryOrdersByPage.order_client_id",),
            replay_paths=("SubscribePublicTopic.order_client_id",),
            normalization_version=1,
        )
        self.assertTrue(binding.supports_automatic_routing)
        self.assertFalse(dataclasses.replace(binding, replay_paths=()).supports_automatic_routing)

    def test_capacity_plan_stops_only_on_declared_bounds(self) -> None:
        plan = RouteDirectoryCapacityPlan(
            plan_id="route-capacity-v1",
            version=1,
            plan_hash=SHA_A,
            max_active_entries=1_000,
            max_tombstone_entries=2_000,
            max_total_bytes=1_000_000,
            warn_basis_points=7_000,
            stop_new_basis_points=8_500,
            max_gc_lag_seconds=60,
            minimum_client_ref_headroom=10_000,
        )
        self.assertFalse(
            plan.stop_new_required(
                active=800,
                tombstones=1_000,
                total_bytes=500_000,
                gc_lag_seconds=10,
                client_ref_headroom=20_000,
            )
        )
        self.assertTrue(
            plan.stop_new_required(
                active=850,
                tombstones=1_000,
                total_bytes=500_000,
                gc_lag_seconds=10,
                client_ref_headroom=20_000,
            )
        )
        self.assertTrue(
            plan.stop_new_required(
                active=1,
                tombstones=1,
                total_bytes=1,
                gc_lag_seconds=61,
                client_ref_headroom=20_000,
            )
        )

    def test_recovery_eligibility_has_one_deterministic_result(self) -> None:
        profile = _profile()
        certificate = _certificate()
        eligible = RecoveryAutoResolutionEligibility.evaluate(
            profile=profile,
            certificate=certificate,
            scope_ownership_valid=True,
            route_directory_conflict=False,
            eligibility_input_hash=SHA_A,
        )
        self.assertTrue(eligible.eligible)
        self.assertEqual((), eligible.reason_codes)

        incomplete = _certificate(
            callback_gaps_closed=False,
            route_token_echo_complete=False,
            query_digests={"orders": SHA_A},
        )
        first = RecoveryAutoResolutionEligibility.evaluate(
            profile=profile,
            certificate=incomplete,
            scope_ownership_valid=False,
            route_directory_conflict=True,
            eligibility_input_hash=SHA_B,
        )
        second = RecoveryAutoResolutionEligibility.evaluate(
            profile=profile,
            certificate=incomplete,
            scope_ownership_valid=False,
            route_directory_conflict=True,
            eligibility_input_hash=SHA_B,
        )
        self.assertEqual(first, second)
        self.assertFalse(first.eligible)
        self.assertEqual(
            (
                "CALLBACK_GAP_UNCLOSED",
                "QUERY_INCOMPLETE",
                "ROUTE_DIRECTORY_CONFLICT",
                "ROUTE_TOKEN_ECHO_INCOMPLETE",
                "SCOPE_OWNERSHIP_INVALID",
            ),
            first.reason_codes,
        )
        with self.assertRaises(DomainValidationError):
            dataclasses.replace(first, eligible=True)


if __name__ == "__main__":
    unittest.main()
