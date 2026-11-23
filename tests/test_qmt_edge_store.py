"""Internal SQLite acceptance for the QMT Windows Edge state machine."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.qmt import (
    QmtAdapterProfile,
    QmtEdgeCommandV1,
    QmtEdgeNativeEncoder,
    QmtNativeEngineClient,
    QmtNativeFrameKind,
    QmtNativeFrameV1,
    QmtNativePolicyV1,
    QmtNativeRing,
    QmtNativeSubmitPayloadV1,
    QmtNativeSubmitRouteV1,
    QmtRingHeaderV1,
    load_builtin_qmt_fake_manifest,
)
from xuanling_tradecore.adapters.qmt.bridge_protocol import QmtSubmitCommand
from xuanling_tradecore.adapters.qmt.edge_store import (
    QmtEdgeIdempotencyConflict,
    QmtEdgeStore,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)

DAY = TradingDayEpoch("2026-09-16")


def _runtime(manifest):
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("qmt-fake-account-1", True, False, False),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )


def _command(*, client_order_ref: str = "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000") -> QmtSubmitCommand:
    manifest = load_builtin_qmt_fake_manifest()
    mapping = QmtAdapterProfile(manifest).activate(
        runtime=_runtime(manifest), activated_at_unix_s=0
    )
    trace = ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="qmt-fake-account-1",
        strategy_id=None,
        strategy_order_id=None,
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.MANUAL,
    )
    instruction = ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="qmt-fake-account-1",
        trading_day=DAY,
        consumer_trace=trace,
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )
    ownership = manifest.scope_ownership
    assert ownership is not None
    return QmtSubmitCommand(
        instruction=instruction,
        mapped_order=mapping.map_execution(instruction),
        client_order_ref=client_order_ref,
        adapter_instance_id=ownership.adapter_instance_id,
        adapter_client_scope=ownership.adapter_client_scope,
    )


class QmtEdgeStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.store = QmtEdgeStore(Path(self._tempdir.name) / "edge.sqlite")

    def tearDown(self) -> None:
        self.store.close()
        self._tempdir.cleanup()

    def test_submit_is_idempotent_but_content_conflicts_fail_closed(self) -> None:
        first = self.store.accept_submit(_command())
        second = self.store.accept_submit(_command())
        self.assertEqual(first, second)
        with self.assertRaises(QmtEdgeIdempotencyConflict):
            self.store.accept_submit(_command(client_order_ref="Q1BBBBBBBBBBBBBBBBBBBBBBBBBB0000"))

    def test_native_publish_and_restart_boundary_become_unknown_effect(self) -> None:
        accepted = self.store.accept_submit(_command())
        prepared = self.store.record_publish_prepared(
            accepted.request_id, accepted.command_sequence
        )
        published = self.store.record_publish_attempt(
            accepted.request_id, accepted.command_sequence
        )
        started = self.store.record_native_call_started(
            accepted.request_id, accepted.command_sequence
        )
        self.assertEqual(
            (prepared.state, published.state, started.state),
            ("PUBLISH_PREPARED", "PUBLISH_ATTEMPTED", "CALL_STARTED"),
        )
        unknown = self.store.mark_unknown_effects()
        self.assertEqual([item.state for item in unknown], ["UNKNOWN_EFFECT"])

    def test_outbox_is_idempotent_and_ack_removes_only_the_exact_delivery(self) -> None:
        first = self.store.append_outbox(event_id="event-1", payload=b"event")
        self.assertEqual(first, self.store.append_outbox(event_id="event-1", payload=b"event"))
        with self.assertRaises(QmtEdgeIdempotencyConflict):
            self.store.append_outbox(event_id="event-1", payload=b"changed")
        self.assertEqual(self.store.pending_outbox(), (first,))
        self.store.acknowledge_outbox(first.delivery_id)
        self.assertEqual(self.store.pending_outbox(), ())

    def test_edge_client_marks_before_publish_and_durably_drains_events(self) -> None:
        header = QmtRingHeaderV1(
            scope_digest=b"s" * 32,
            generation=b"g" * 16,
            slot_size=512,
            capacity=4,
        )
        command_ring = QmtNativeRing.create_file(
            Path(self._tempdir.name) / "command.ring", header, test_only_unsafe_fallback=True
        )
        event_ring = QmtNativeRing.create_file(
            Path(self._tempdir.name) / "event.ring", header, test_only_unsafe_fallback=True
        )
        try:
            client = QmtNativeEngineClient(
                store=self.store, command_ring=command_ring, event_ring=event_ring
            )
            request = self.store.accept_submit(_command())
            frame = QmtNativeFrameV1(
                sequence=request.command_sequence,
                kind=QmtNativeFrameKind.SUBMIT,
                request_digest=b"r" * 32,
                intent_digest=b"i" * 32,
                timestamps_ns=(1, 2, 0, 0),
                payload=b"command",
            )
            published = client.publish_durable_request(request, frame)
            self.assertEqual(published.request.state, "PUBLISH_ATTEMPTED")
            self.assertEqual(command_ring.consume(), frame)
            event_frame = QmtNativeFrameV1(
                sequence=1,
                kind=QmtNativeFrameKind.ORDER_CALLBACK,
                request_digest=b"r" * 32,
                intent_digest=b"i" * 32,
                timestamps_ns=(9, 10, 0, 0),
                payload=b"callback",
            )
            event_ring.publish(event_frame)
            outbox = client.drain_one_event()
            self.assertEqual(outbox.event_id, "qmt-native:" + (b"g" * 16).hex() + ":1")
            self.assertEqual(self.store.pending_outbox(), (outbox,))
        finally:
            command_ring.close()
            event_ring.close()

    def test_generic_edge_command_is_idempotently_durable(self) -> None:
        command = QmtEdgeCommandV1.from_submit(_command(), scope_digest="a" * 64)
        first = self.store.accept_request(
            request_id=command.request_id, canonical_content=command.canonical_bytes
        )
        second = self.store.accept_request(
            request_id=command.request_id, canonical_content=command.canonical_bytes
        )
        self.assertEqual(first, second)

    def test_native_encoder_requires_an_approved_policy_route(self) -> None:
        command = QmtEdgeCommandV1.from_submit(_command(), scope_digest="a" * 64)
        policy = QmtNativePolicyV1(
            strategy_name="tradecore-qmt",
            account_type="STOCK",
            price_tick_divisor=1000,
            market_suffixes={1: ".SZ", 2: ".SH"},
            submit_routes=(
                QmtNativeSubmitRouteV1(
                    business_type=1,
                    side=1,
                    price_type=1,
                    op_type=23,
                    order_type=1101,
                    qmt_price_type=11,
                ),
            ),
            query_data_types={"orders": "ORDER"},
        )
        frame = QmtEdgeNativeEncoder(policy).encode(command, command_sequence=1)
        self.assertEqual(frame.kind, QmtNativeFrameKind.SUBMIT)
        payload = QmtNativeSubmitPayloadV1.decode(frame.payload)
        self.assertEqual(
            (payload.account_id, payload.order_code, payload.price),
            ("qmt-fake-account-1", "000001.SZ", 0.999),
        )


if __name__ == "__main__":
    unittest.main()
