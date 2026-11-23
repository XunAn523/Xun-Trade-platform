from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from xuanling_tradecore.adapters import (
    AdapterRouteTokenCodec,
    ClientOrderRefScope,
    InMemoryRouteTokenDirectory,
    RouteDirectoryAdmission,
    RouteDirectoryCapacitySnapshot,
    RouteTokenCodecError,
    RouteTokenDirectoryEntry,
    RouteTokenDirectoryRecoveryInput,
    Sha256AdapterRouteTokenFactory,
    evaluate_route_directory_admission,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterRouteTokenCodec,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.adapters.qmt import QmtOrderRemarkCodec, qmt_route_token_binding_v1
from xuanling_tradecore.domain import (
    AdapterRouteToken,
    DomainValidationError,
    RouteDirectoryCapacityPlan,
    TradingDayEpoch,
)

ROOT = Path(__file__).resolve().parents[1]
VECTOR_ROOT = ROOT / "contracts" / "adapters" / "route-token" / "v1"
TOKEN = AdapterRouteToken(1, bytes.fromhex("00112233445566778899aabbccddeeff"))


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], value)


def _scope() -> ClientOrderRefScope:
    return ClientOrderRefScope(
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-01"),
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
    )


def _capacity_plan() -> RouteDirectoryCapacityPlan:
    return RouteDirectoryCapacityPlan(
        plan_id="route-capacity-v1",
        version=1,
        plan_hash="8b381c1ab5a796ceb5e3f099be11b197195981e5d0614a22926a05d24b161e5a",
        max_active_entries=100,
        max_tombstone_entries=50,
        max_total_bytes=10_000,
        warn_basis_points=7000,
        stop_new_basis_points=9000,
        max_gc_lag_seconds=60,
        minimum_client_ref_headroom=10,
    )


def _capacity(
    *,
    active: int = 0,
    tombstones: int = 0,
    total_bytes: int = 0,
    gc_lag_seconds: int = 0,
    headroom: int = 100,
) -> RouteDirectoryCapacitySnapshot:
    return RouteDirectoryCapacitySnapshot(
        active_entries=active,
        tombstone_entries=tombstones,
        total_bytes=total_bytes,
        gc_lag_seconds=gc_lag_seconds,
        client_ref_headroom=headroom,
    )


def _entry(
    *,
    execution_id: str = "execution-1",
    token: AdapterRouteToken = TOKEN,
    client_order_ref: str = "42",
) -> RouteTokenDirectoryEntry:
    return RouteTokenDirectoryEntry(
        execution_id=execution_id,
        scope=_scope(),
        token=token,
        client_order_ref=client_order_ref,
        binding_id="emt-order-client-id-v1",
        allocation_sequence=int(client_order_ref),
    )


class RouteTokenContractTests(unittest.TestCase):
    def test_emt_vectors_cover_write_callback_query_replay_and_normalize(self) -> None:
        manifest = load_builtin_emt_legacy_sim_manifest()
        binding = manifest.route_token_binding
        if binding is None:
            self.fail("EMT V2 manifest lacks a route token binding")
        codec = EmtAdapterRouteTokenCodec(binding)
        self.assertIsInstance(codec, AdapterRouteTokenCodec)
        fixture = _json(VECTOR_ROOT / "emt-vectors.json")
        vector = fixture["vectors"][0]
        token = AdapterRouteToken.parse(vector["token"])
        encoded = codec.encode(
            token,
            allocated_client_order_ref=vector["allocated_client_order_ref"],
        )
        self.assertEqual(vector["encoded"], encoded)
        for path in vector["extract_paths"]:
            self.assertEqual(
                token,
                codec.normalize_extracted(
                    path=path,
                    native_value=encoded,
                    expected_token=token,
                ),
            )
        for value in fixture["negative_native_values"]:
            with self.assertRaises((RouteTokenCodecError, DomainValidationError)):
                codec.normalize_extracted(
                    path=binding.order_callback_paths[0],
                    native_value=value,
                    expected_token=token,
                )
        with self.assertRaisesRegex(RouteTokenCodecError, "recovered route directory"):
            codec.normalize_extracted(
                path=binding.order_callback_paths[0],
                native_value=encoded,
                expected_token=None,
            )
        with self.assertRaisesRegex(RouteTokenCodecError, "not declared"):
            codec.normalize_extracted(
                path="Undeclared.free_text",
                native_value=encoded,
                expected_token=token,
            )

    def test_qmt_vectors_are_reversible_checksummed_and_match_fake_profile(self) -> None:
        fixture = _json(VECTOR_ROOT / "qmt-vectors.json")
        binding_fixture = fixture["binding"]
        binding = qmt_route_token_binding_v1()
        self.assertEqual(binding.binding_id, binding_fixture["binding_id"])
        self.assertEqual(binding.allowed_characters, binding_fixture["allowed_characters"])
        self.assertEqual(list(binding.query_paths), binding_fixture["query_paths"])
        codec = QmtOrderRemarkCodec(binding)
        self.assertIsInstance(codec, AdapterRouteTokenCodec)
        vector = fixture["vectors"][0]
        token = AdapterRouteToken.parse(vector["token"])
        encoded = codec.encode(token)
        self.assertEqual(vector["encoded"], encoded)
        for path in (
            binding.order_callback_paths
            + binding.trade_callback_paths
            + binding.cancel_error_paths
            + binding.query_paths
            + binding.replay_paths
        ):
            self.assertEqual(
                token,
                codec.normalize_extracted(
                    path=path,
                    native_value=encoded,
                    expected_token=token,
                ),
            )
        for value in fixture["negative_native_values"]:
            with self.assertRaises(RouteTokenCodecError):
                codec.normalize_extracted(
                    path=binding.order_callback_paths[0],
                    native_value=value,
                    expected_token=token,
                )
        with self.assertRaisesRegex(RouteTokenCodecError, "conflicts"):
            codec.normalize_extracted(
                path=binding.order_callback_paths[0],
                native_value=encoded,
                expected_token=AdapterRouteToken(1, b"x" * 16),
            )

    def test_factory_is_deterministic_opaque_and_scope_bound(self) -> None:
        factory = Sha256AdapterRouteTokenFactory("a" * 64)
        first = factory.create(execution_id="execution-1", scope=_scope(), allocation_nonce=1)
        retry = factory.create(execution_id="execution-1", scope=_scope(), allocation_nonce=1)
        changed = factory.create(execution_id="execution-1", scope=_scope(), allocation_nonce=2)
        self.assertEqual(first, retry)
        self.assertNotEqual(first, changed)
        self.assertNotIn("execution", first.to_text())
        self.assertNotIn("account", first.to_text())

    def test_capacity_warn_and_stop_new_cover_entries_bytes_gc_and_uint32_headroom(self) -> None:
        plan = _capacity_plan()
        self.assertEqual(
            RouteDirectoryAdmission.AVAILABLE,
            evaluate_route_directory_admission(plan, _capacity(active=69)),
        )
        self.assertEqual(
            RouteDirectoryAdmission.WARN,
            evaluate_route_directory_admission(plan, _capacity(active=70)),
        )
        stop_cases = (
            _capacity(active=90),
            _capacity(tombstones=45),
            _capacity(total_bytes=9000),
            _capacity(gc_lag_seconds=61),
            _capacity(headroom=9),
        )
        for snapshot in stop_cases:
            self.assertEqual(
                RouteDirectoryAdmission.STOP_NEW,
                evaluate_route_directory_admission(plan, snapshot),
            )

    def test_directory_requires_complete_journal_recovery_and_rejects_collisions(self) -> None:
        directory = InMemoryRouteTokenDirectory(_scope(), _capacity_plan())
        recovery = RouteTokenDirectoryRecoveryInput(
            recovery_evidence_id="journal-evidence-1",
            journal_id="journal-1",
            scope=_scope(),
            entries=(),
            is_complete=True,
        )
        with self.assertRaisesRegex(RouteTokenCodecError, "complete Journal recovery"):
            directory.register_new(_entry(), _capacity())
        directory.recover(recovery)
        first = directory.register_new(_entry(), _capacity())
        self.assertEqual(first, directory.register_new(first, _capacity(active=99)))
        self.assertEqual(first, directory.find_by_client_order_ref("42"))
        with self.assertRaisesRegex(RouteTokenCodecError, "STOP_NEW"):
            directory.register_new(
                _entry(
                    execution_id="execution-2",
                    token=AdapterRouteToken(1, b"y" * 16),
                    client_order_ref="43",
                ),
                _capacity(active=90),
            )
        self.assertEqual(
            first,
            directory.find_by_client_order_ref("42"),
            "STOP_NEW does not block callback/cancel/recovery lookups",
        )
        with self.assertRaisesRegex(RouteTokenCodecError, "collision"):
            directory.register_new(
                replace(_entry(), execution_id="execution-3"),
                _capacity(),
            )

    def test_directory_recovery_rejects_incomplete_cross_scope_and_duplicate_tokens(self) -> None:
        scope = _scope()
        directory = InMemoryRouteTokenDirectory(scope, _capacity_plan())
        with self.assertRaisesRegex(RouteTokenCodecError, "incomplete"):
            directory.recover(
                RouteTokenDirectoryRecoveryInput("evidence-1", "journal-1", scope, (), False)
            )
        duplicate = replace(
            _entry(),
            execution_id="execution-2",
            client_order_ref="43",
            allocation_sequence=43,
        )
        with self.assertRaisesRegex(RouteTokenCodecError, "repeats token"):
            RouteTokenDirectoryRecoveryInput(
                "evidence-1",
                "journal-1",
                scope,
                (_entry(), duplicate),
                True,
            )


if __name__ == "__main__":
    unittest.main()
