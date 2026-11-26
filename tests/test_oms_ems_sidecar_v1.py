from __future__ import annotations

import unittest
from types import SimpleNamespace

from xuanling_tradecore.algorithms import MaxQuantitySliceAlgorithm, SliceAlgorithmSpec
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    CallbackDirectWriteEligibilityV1,
    CallbackDirectWriteError,
    CallbackPersistenceProfileV1,
    EmsControlPlaneV1,
    EmsPendingPlanBufferV1,
    EmsPlanAdmissionOutcome,
    select_callback_persistence_profile_v1,
)
from xuanling_tradecore.oms import (
    ImmutableOmsSnapshotStoreV1,
    OmsSidecarError,
    OmsSidecarFactKind,
    OmsSidecarFactReaderV1,
    OmsSidecarFactV1,
    OmsSidecarManifestChainV1,
    OmsSidecarManifestSegmentV1,
    OmsSidecarManifestV1,
    OmsSidecarOrderState,
    OmsSnapshotQueryServiceV1,
    OrderStateProjectorV1,
)

_HASH = "a" * 64
_RISK_HASH = "b" * 64


def _segment(end_position: int) -> OmsSidecarManifestSegmentV1:
    return OmsSidecarManifestSegmentV1(
        scope_id="scope-a",
        journal_generation=1,
        start_position=1,
        end_position=end_position,
        schema_hash="c" * 64,
        segment_checksum="d" * 64,
        writer_instance_id="writer-a",
        activation_epoch=3,
    )


def _fact(
    position: int,
    kind: OmsSidecarFactKind,
    state: OmsSidecarOrderState | None,
    *,
    execution_id: str = "execution-1",
    client_ref: str = "client-1",
    external_id: str | None = None,
    state_version: int | None = None,
    trade_key: str | None = None,
    cumulative_quantity: int | None = None,
) -> OmsSidecarFactV1:
    return OmsSidecarFactV1(
        scope_id="scope-a",
        position=position,
        schema_version=1,
        activation_epoch=3,
        route_version=7,
        kind=kind,
        execution_id=execution_id,
        canonical_client_order_ref=client_ref,
        payload_hash=_HASH,
        observed_monotonic_ns=position * 10,
        external_order_id=external_id,
        order_state=state,
        state_version=position if state_version is None else state_version,
        trade_key=trade_key,
        cumulative_quantity=cumulative_quantity,
    )


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-parent-1",
        oms_order_id="oms-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-08"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            strategy_id="strategy-1",
            strategy_order_id="strategy-order-1",
            attribution_binding_id="binding-1",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        ),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=10,
        protection_price_ticks=100,
        limit_price_ticks=99,
    )


class _Router:
    """Control-plane test double; it deliberately exposes no submit/cancel API."""

    def route_executions(self, instructions, *, workers=1):
        return tuple(self.route_execution(item) for item in instructions)

    def route_execution(self, instruction: ExecutionInstruction):
        return SimpleNamespace(
            binding=SimpleNamespace(
                shard_id="scope-a",
                adapter_client_scope="1",
                ownership_scope_hash=_HASH,
                journal_routing_epoch=7,
            )
        )


class OmsEmsSidecarV1Tests(unittest.TestCase):
    def test_reader_replay_and_callback_before_native_return_are_deterministic(self) -> None:
        facts = (
            _fact(
                1,
                OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
            ),
            _fact(
                2,
                OmsSidecarFactKind.ORDER_CALLBACK,
                OmsSidecarOrderState.WORKING,
                external_id="external-1",
                state_version=10,
            ),
            _fact(
                3,
                OmsSidecarFactKind.NATIVE_ACCEPTED,
                OmsSidecarOrderState.NATIVE_ACCEPTED,
                external_id="external-1",
                state_version=3,
            ),
            _fact(
                4,
                OmsSidecarFactKind.TRADE_CALLBACK,
                OmsSidecarOrderState.PARTIALLY_FILLED,
                external_id="external-1",
                state_version=11,
                trade_key="trade-1",
                cumulative_quantity=4,
            ),
            _fact(
                5,
                OmsSidecarFactKind.TRADE_CALLBACK,
                OmsSidecarOrderState.PARTIALLY_FILLED,
                external_id="external-1",
                state_version=11,
                trade_key="trade-1",
                cumulative_quantity=4,
            ),
        )
        reader = OmsSidecarFactReaderV1(scope_id="scope-a", activation_epoch=3, route_version=7)
        committed = reader.read_committed(facts)
        projector = OrderStateProjectorV1()
        snapshot = projector.apply(committed)
        replay = OrderStateProjectorV1().apply(committed)
        order = snapshot.orders_by_execution_id["execution-1"]
        self.assertEqual(OmsSidecarOrderState.PARTIALLY_FILLED, order.state)
        self.assertEqual("external-1", order.external_order_id)
        self.assertEqual(4, order.cumulative_quantity)
        self.assertEqual(frozenset({"trade-1"}), order.trade_keys)
        self.assertEqual("execution-1", snapshot.trades_by_key["trade-1"].execution_id)
        self.assertEqual(snapshot.orders_by_execution_id, replay.orders_by_execution_id)
        self.assertEqual(snapshot.per_scope_applied_watermark, replay.per_scope_applied_watermark)
        self.assertEqual((), reader.read_committed(facts))

    def test_reader_and_projector_fail_closed_for_gaps_and_id_conflicts(self) -> None:
        reader = OmsSidecarFactReaderV1(scope_id="scope-a", activation_epoch=3, route_version=7)
        with self.assertRaisesRegex(OmsSidecarError, "position gap"):
            reader.read_committed(
                (
                    _fact(
                        2,
                        OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                        OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
                    ),
                )
            )
        projector = OrderStateProjectorV1()
        snapshot = projector.apply(
            (
                _fact(
                    1,
                    OmsSidecarFactKind.NATIVE_ACCEPTED,
                    OmsSidecarOrderState.NATIVE_ACCEPTED,
                    external_id="external-1",
                ),
                _fact(
                    2,
                    OmsSidecarFactKind.ORDER_CALLBACK,
                    OmsSidecarOrderState.WORKING,
                    external_id="external-2",
                ),
            )
        )
        self.assertFalse(snapshot.is_authoritative)
        self.assertEqual(
            OmsSidecarOrderState.RECONCILE_REQUIRED,
            snapshot.orders_by_execution_id["execution-1"].state,
        )

    def test_manifest_chain_snapshot_query_and_immutable_generation(self) -> None:
        projector = OrderStateProjectorV1()
        first = projector.apply(
            (
                _fact(
                    1,
                    OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                    OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
                ),
            )
        )
        store = ImmutableOmsSnapshotStoreV1(history_capacity=2)
        store.publish(first)
        second = projector.apply(
            (
                _fact(
                    2,
                    OmsSidecarFactKind.ORDER_CALLBACK,
                    OmsSidecarOrderState.WORKING,
                    external_id="external-1",
                ),
            )
        )
        store.publish(second)
        self.assertEqual(
            "external-1",
            OmsSnapshotQueryServiceV1(store).order_by_execution_id("execution-1").external_order_id,
        )
        self.assertEqual(2, len(store.recent()))
        chain = OmsSidecarManifestChainV1()
        root = OmsSidecarManifestV1.create(
            manifest_id="manifest-1",
            created_monotonic_ns=100,
            previous_manifest_hash=None,
            per_scope_segment_ranges={"scope-a": (1, 1)},
            per_scope_applied_watermark=first.per_scope_applied_watermark,
            snapshot_generation=first.generation,
            segments=(_segment(1),),
        )
        chain.append(root)
        child = OmsSidecarManifestV1.create(
            manifest_id="manifest-2",
            created_monotonic_ns=200,
            previous_manifest_hash=root.manifest_hash,
            per_scope_segment_ranges={"scope-a": (1, 2)},
            per_scope_applied_watermark=second.per_scope_applied_watermark,
            snapshot_generation=second.generation,
            segments=(_segment(2),),
        )
        chain.append(child)
        self.assertEqual(child, chain.latest())
        with self.assertRaises(OmsSidecarError):
            chain.append(root)

    def test_ems_only_plans_from_fresh_authoritative_snapshot_and_never_dispatches(self) -> None:
        projector = OrderStateProjectorV1()
        snapshot = projector.apply(
            (
                _fact(
                    1,
                    OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                    OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
                ),
            )
        )
        buffer = EmsPendingPlanBufferV1(capacity=1)
        control = EmsControlPlaneV1(session_router=_Router(), pending_plans=buffer)
        accepted = control.create_plan(
            plan_id="plan-1",
            parent_instruction=_instruction(),
            algorithm=MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 4)),
            snapshot=snapshot,
            required_scope_watermarks={"scope-a": 1},
            risk_policy_hash=_RISK_HASH,
            created_monotonic_ns=100,
            expires_monotonic_ns=200,
        )
        self.assertEqual(EmsPlanAdmissionOutcome.ACCEPTED, accepted.outcome)
        assert accepted.plan is not None
        self.assertEqual(
            [4, 4, 2], [child.instruction.quantity for child in accepted.plan.routed_children]
        )
        self.assertEqual(1, buffer.depth())
        self.assertEqual("plan-1", buffer.drain_next(monotonic_ns=101).plan_id)
        stale = control.create_plan(
            plan_id="plan-2",
            parent_instruction=_instruction(),
            algorithm=MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 10)),
            snapshot=snapshot,
            required_scope_watermarks={"scope-a": 2},
            risk_policy_hash=_RISK_HASH,
            created_monotonic_ns=100,
            expires_monotonic_ns=200,
        )
        self.assertEqual(EmsPlanAdmissionOutcome.SNAPSHOT_STALE, stale.outcome)

    def test_direct_write_candidate_requires_all_safety_proofs(self) -> None:
        missing = CallbackDirectWriteEligibilityV1(
            scope_id="scope-a",
            disarmed_simulated=True,
            sealed_route_map=True,
            source_scope_equals_target_scope=False,
            sole_drain_writer=True,
            bounded_projector_inbox=True,
            callback_journal_capacity_available=True,
        )
        with self.assertRaises(CallbackDirectWriteError):
            select_callback_persistence_profile_v1(
                requested=CallbackPersistenceProfileV1.DIRECT_CANDIDATE,
                eligibility=missing,
            )
        selected = select_callback_persistence_profile_v1(
            requested=CallbackPersistenceProfileV1.DIRECT_CANDIDATE,
            eligibility=CallbackDirectWriteEligibilityV1(
                scope_id="scope-a",
                disarmed_simulated=True,
                sealed_route_map=True,
                source_scope_equals_target_scope=True,
                sole_drain_writer=True,
                bounded_projector_inbox=True,
                callback_journal_capacity_available=True,
            ),
        )
        self.assertEqual(CallbackPersistenceProfileV1.DIRECT_CANDIDATE, selected)


if __name__ == "__main__":
    unittest.main()
