"""Tests for the platform-only native mutation seam used by EmtBrokerAdapter."""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import cast

from emt_gateway.tradecore_mutation import (
    EmtPlatformMutationPort,
    EmtTradeCoreMutationError,
    build_tradecore_execution_trader_bridge,
)
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerEventIngress,
    EmtCancelRequest,
    EmtSubmitRequest,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.adapters.emt.namespace_recovery import (
    NamespaceOwnership,
    OrderClientIdNamespaceDirectoryV1,
)
from xuanling_tradecore.adapters.mapping import MappedVenueOrder, VenueIdentityDeclaration
from xuanling_tradecore.domain import (
    CancelRequest,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
    VenueOrderIdentity,
)


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-04"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            strategy_id=None,
            strategy_order_id=None,
            attribution_binding_id="binding-1",
            binding_version=1,
            order_origin=OrderOrigin.MANUAL,
        ),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )


def _submit_request() -> EmtSubmitRequest:
    return EmtSubmitRequest(
        instruction=_instruction(),
        mapped_order=MappedVenueOrder(
            execution_id="execution-1",
            oms_order_id="oms-1",
            account_id="account-1",
            manifest_id="emt-test",
            manifest_version=1,
            manifest_hash="a" * 64,
            market=1,
            side=1,
            price_type=1,
            business_type=0,
            identity=VenueIdentityDeclaration(
                client_order_ref_field="order_client_id",
                client_order_ref_format="uint32",
                venue_order_id_field="order_emt_id",
                venue_order_id_format="uint64",
                venue_cancel_id_field="order_cancel_emt_id",
                venue_cancel_id_format="uint64",
                adapter_client_scope_field="client_id",
                adapter_client_scope_format="uint8",
                evidence_ids=frozenset({"evidence-1"}),
            ),
        ),
        order_client_id=17,
    )


class _Api:
    def __init__(self, *, order_id: int = 91, cancel_id: int = 92) -> None:
        self.order_id = order_id
        self.cancel_id = cancel_id
        self.insert_calls = []
        self.cancel_calls = []

    def insertOrder(self, request, session_id: int) -> int:
        self.insert_calls.append((request, session_id))
        return self.order_id

    def cancelOrder(self, order_id: int, session_id: int) -> int:
        self.cancel_calls.append((order_id, session_id))
        return self.cancel_id

    def getApiLastError(self):
        return {"error_id": 7, "error_msg": "not retained"}


class _Journal:
    def record_submit_started(self, request) -> None:
        del request

    def record_submit_accepted(self, request, receipt) -> None:
        del request, receipt

    def record_cancel_started(self, request) -> None:
        del request

    def record_cancel_accepted(self, request, receipt) -> None:
        del request, receipt


class _Observer:
    def __init__(self) -> None:
        self.events = []
        self.gaps = []

    def on_broker_event(self, event) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence) -> None:
        self.gaps.append(evidence)


def _event_ingress(
    *, namespace_directory: OrderClientIdNamespaceDirectoryV1 | None = None
) -> tuple[EmtBrokerEventIngress, _Observer]:
    manifest = load_builtin_emt_legacy_sim_manifest()
    mapping = EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )
    ingress = EmtBrokerEventIngress(
        mapping,
        trading_day=TradingDayEpoch("2026-09-04"),
        namespace_directory=namespace_directory,
    )
    observer = _Observer()
    ingress.attach_observer(observer)
    return ingress, observer


class TradeCoreMutationPortTests(unittest.TestCase):
    def test_platform_port_never_exposes_vendor_object_and_returns_only_ids(self) -> None:
        api = _Api()
        port = EmtPlatformMutationPort(
            api,
            native_session_id=71,
            order_request_factory=lambda request: {"order_client_id": request.order_client_id},
            native_call_journal=_Journal(),
            clock=lambda: 100,
        )
        submitted = port.submit(_submit_request())
        self.assertEqual(submitted.order_emt_id, 91)
        self.assertEqual(api.insert_calls, [({"order_client_id": 17}, 71)])
        canceled = port.cancel(
            EmtCancelRequest(
                cancel_request=CancelRequest(
                    cancel_request_id="cancel-1",
                    oms_order_id="oms-1",
                    execution_id="execution-1",
                    account_id="account-1",
                    consumer_trace=_instruction().consumer_trace,
                    reason="operator request",
                    requested_monotonic_ns=1,
                ),
                order_emt_id=91,
            )
        )
        self.assertEqual(canceled.order_cancel_emt_id, 92)
        self.assertEqual(api.cancel_calls, [(91, 71)])

    def test_zero_native_return_is_a_payload_free_definite_rejection(self) -> None:
        port = EmtPlatformMutationPort(
            _Api(order_id=0, cancel_id=0),
            native_session_id=71,
            order_request_factory=lambda request: {},
            native_call_journal=_Journal(),
        )
        with self.assertRaisesRegex(EmtTradeCoreMutationError, "error_id=7"):
            port.submit(_submit_request())

    def test_native_bridge_extracts_only_order_side_scalars_and_latches_bad_callbacks(self) -> None:
        ingress, observer = _event_ingress()
        native_callback_entries: list[str] = []

        class _TraderBase:
            pass

        bridge = build_tradecore_execution_trader_bridge(
            _TraderBase,
            event_ingress=ingress,
            cancel_request_id_for_order=lambda order_emt_id: (
                "cancel-1" if order_emt_id == 91 else None
            ),
            native_callback_observer=native_callback_entries.append,
        )()
        bridge.onOrderEvent(
            {
                "order_emt_id": 91,
                "order_client_id": 17,
                "order_status": 0,
                "qty_traded": 0,
                "qty_left": 100,
                "unexpected_vendor_field": "discarded",
            }
        )
        bridge.onTradeEvent(
            {
                "order_emt_id": 91,
                "order_client_id": 17,
                "report_index": 3,
                "qty_traded": 100,
                "qty_left": 0,
            }
        )
        self.assertEqual(ingress.drain(), 2)
        self.assertEqual(observer.events[0].venue_order_identity.venue_order_id, "91")
        self.assertEqual(observer.events[0].normalized_summary["ledger_state"], "LIVE")
        self.assertEqual(observer.events[1].trade_key, "emt-report:91:3")
        bridge.onTradeEvent({"order_emt_id": 91, "order_client_id": 17})
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual({"bridge_trade_identity": 1}, ingress.callback_gap_reason_counts)
        bridge.onCancelOrderError({"order_cancel_emt_id": 92})
        self.assertEqual(len(observer.gaps), 2)
        self.assertEqual(
            {
                "bridge_trade_identity": 1,
                "bridge_cancel_order_id_missing": 1,
            },
            ingress.callback_gap_reason_counts,
        )
        self.assertEqual(
            ["onOrderEvent", "onTradeEvent", "onTradeEvent", "onCancelOrderError"],
            native_callback_entries,
        )

    def test_native_bridge_enriches_cancel_error_from_durable_target_identity(self) -> None:
        ingress, observer = _event_ingress()

        class _TraderBase:
            pass

        target = VenueOrderIdentity(
            "EMT", "emt-sim-instance-1", "7", client_order_ref="17", venue_order_id="91"
        )
        bridge = build_tradecore_execution_trader_bridge(
            _TraderBase,
            event_ingress=ingress,
            cancel_request_id_for_order=lambda order_emt_id: (
                "cancel-1" if order_emt_id == 91 else None
            ),
            cancel_target_identity_for_order=lambda order_emt_id: (
                target if order_emt_id == 91 else None
            ),
        )()
        bridge.onCancelOrderError(
            {"order_emt_id": 91, "order_cancel_emt_id": 92}, {"error_id": 1}, 7
        )
        self.assertEqual(1, ingress.drain())
        self.assertEqual("cancel-1", observer.events[0].cancel_request_id)
        self.assertEqual("17", observer.events[0].venue_order_identity.client_order_ref)

    def test_native_bridge_counts_only_presealed_foreign_cancel_fanout(self) -> None:
        ingress, observer = _event_ingress()

        class _TraderBase:
            pass

        bridge = build_tradecore_execution_trader_bridge(
            _TraderBase,
            event_ingress=ingress,
            cancel_request_id_for_order=lambda _order_emt_id: None,
            known_foreign_cancel_order_id=lambda order_emt_id: order_emt_id == 92,
        )()
        bridge.onCancelOrderError({"order_emt_id": 92})
        self.assertEqual(0, ingress.drain())
        self.assertEqual(0, len(observer.gaps))
        self.assertEqual(1, ingress.verified_foreign_cancel_callback_count)

        # An unregistered client reference remains a callback integrity gap.
        bridge.onCancelOrderError({"order_emt_id": 93, "order_client_id": 19})
        self.assertEqual(1, len(observer.gaps))
        self.assertEqual({"bridge_cancel_route_absent": 1}, ingress.callback_gap_reason_counts)

    def test_native_bridge_uses_signed_foreign_client_namespace_for_cancel_fanout(self) -> None:
        ingress, observer = _event_ingress()

        class _TraderBase:
            pass

        bridge = build_tradecore_execution_trader_bridge(
            _TraderBase,
            event_ingress=ingress,
            cancel_request_id_for_order=lambda _order_emt_id: None,
            known_foreign_cancel_client_order_id=lambda client_order_id: client_order_id == 456,
        )()
        bridge.onCancelOrderError({"order_emt_id": 92, "order_client_id": 456})
        self.assertEqual(0, ingress.drain())
        self.assertEqual(0, len(observer.gaps))
        self.assertEqual(1, ingress.verified_foreign_cancel_callback_count)

        # A foreign order-side callback bypasses local-only normalization
        # before a missing trade key can be misclassified as a gap.
        bridge.onTradeEvent({"order_client_id": 456})
        self.assertEqual(0, ingress.drain())
        self.assertEqual(0, len(observer.gaps))
        self.assertEqual(2, ingress.verified_foreign_cancel_callback_count)

        bridge.onCancelOrderError({"order_emt_id": 93, "order_client_id": 457})
        self.assertEqual(1, len(observer.gaps))
        self.assertEqual({"bridge_cancel_route_absent": 1}, ingress.callback_gap_reason_counts)

    def test_native_bridge_correlates_clientless_foreign_cancel_from_signed_order(self) -> None:
        foreign_directory = SimpleNamespace(
            lookup=lambda _client_order_id: SimpleNamespace(
                ownership=NamespaceOwnership.FOREIGN_RESERVED
            )
        )
        ingress, observer = _event_ingress(
            namespace_directory=cast(OrderClientIdNamespaceDirectoryV1, foreign_directory)
        )

        class _TraderBase:
            pass

        bridge = build_tradecore_execution_trader_bridge(
            _TraderBase,
            event_ingress=ingress,
            cancel_request_id_for_order=lambda _order_emt_id: None,
        )()
        bridge.onOrderEvent({"order_emt_id": 92, "order_client_id": 456})
        bridge.onCancelOrderError({"order_emt_id": 92})
        self.assertEqual(0, ingress.drain())
        self.assertEqual(0, len(observer.gaps))
        self.assertEqual(2, ingress.verified_foreign_namespace_callback_count)
        self.assertEqual(1, ingress.verified_foreign_cancel_callback_count)


if __name__ == "__main__":
    unittest.main()
