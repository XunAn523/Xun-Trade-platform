"""Tests for the dual-ingress controlled EMT execution-session composition."""

from __future__ import annotations

import unittest
from threading import Thread
from time import sleep

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    CancelExecutionInstruction,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtCancelReceipt,
    EmtCancelRequest,
    EmtControlledExecutionAdapter,
    EmtControlledSessionError,
    EmtMutationPort,
    EmtNativeBrokerEvent,
    EmtSubmitReceipt,
    EmtSubmitRequest,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    BrokerEventKind,
    CancelRequest,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
    VenueOrderIdentity,
)

DAY = TradingDayEpoch("2026-09-04")


class _Observer:
    def __init__(self) -> None:
        self.events = []
        self.gaps = []

    def on_broker_event(self, event) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence) -> None:
        self.gaps.append(evidence)


class _ForwardingObserver:
    def __init__(self, downstream) -> None:
        self._downstream = downstream
        self.events = 0
        self.gaps = 0

    def on_broker_event(self, event) -> None:
        self.events += 1
        self._downstream.on_broker_event(event)

    def on_callback_gap(self, evidence) -> None:
        self.gaps += 1
        self._downstream.on_callback_gap(evidence)


class _MutationPort(EmtMutationPort):
    def submit(self, request: EmtSubmitRequest) -> EmtSubmitReceipt:
        self.last_submit = request
        return EmtSubmitReceipt(order_emt_id=91, accepted_monotonic_ns=101)

    def cancel(self, request: EmtCancelRequest) -> EmtCancelReceipt:
        self.last_cancel = request
        return EmtCancelReceipt(order_cancel_emt_id=92, accepted_monotonic_ns=102)


class _Session:
    native_session_id = 71
    api_version = "2.27.0.0"
    trading_day = "2026-09-04"

    def __init__(self) -> None:
        self.mutation_port = _MutationPort()
        self.close_count = 0

    def query_unfinished_orders(self, native_request_id: int) -> int:
        del native_request_id
        return 0

    def query_orders(self, native_request_id: int) -> int:
        del native_request_id
        return 0

    def query_trades(self, native_request_id: int) -> int:
        del native_request_id
        return 0

    def query_orders_by_page(
        self, page_size: int, continuation: int, native_request_id: int
    ) -> int:
        del page_size, continuation, native_request_id
        return 0

    def query_trades_by_page(
        self, page_size: int, continuation: int, native_request_id: int
    ) -> int:
        del page_size, continuation, native_request_id
        return 0

    def close(self) -> None:
        self.close_count += 1


class _LateCallbackSession(_Session):
    """Emulate one vendor callback emitted while native logout unwinds."""

    def __init__(self) -> None:
        super().__init__()
        self.event_ingress = None

    def close(self) -> None:
        assert self.event_ingress is not None
        self.event_ingress.offer(
            EmtNativeBrokerEvent(
                event_kind=BrokerEventKind.ORDER,
                account_id="account-1",
                trading_day=DAY,
                received_monotonic_ns=112,
                order_emt_id=91,
                order_client_id=17,
                order_status=50,
                leaves_qty=100,
            )
        )
        super().close()


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session
        self.query_ingress = None
        self.event_ingress = None

    def open(self, *, query_callback_ingress, broker_event_ingress) -> _Session:
        self.query_ingress = query_callback_ingress
        self.event_ingress = broker_event_ingress
        assert query_callback_ingress.snapshot().observer_attached
        return self.session


class _LateCallbackFactory(_Factory):
    def open(self, *, query_callback_ingress, broker_event_ingress) -> _LateCallbackSession:
        session = super().open(
            query_callback_ingress=query_callback_ingress,
            broker_event_ingress=broker_event_ingress,
        )
        assert isinstance(session, _LateCallbackSession)
        session.event_ingress = broker_event_ingress
        return session


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
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


def _trace() -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="account-1",
        strategy_id=None,
        strategy_order_id=None,
        attribution_binding_id="binding-1",
        binding_version=1,
        order_origin=OrderOrigin.MANUAL,
    )


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="account-1",
        trading_day=DAY,
        consumer_trace=_trace(),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )


class EmtControlledExecutionAdapterTests(unittest.TestCase):
    def test_session_binds_mutation_and_event_ingress_without_relaxing_disarmed_gates(self) -> None:
        session = _Session()
        factory = _Factory(session)
        lifecycle: list[str] = []
        submit_stages: list[str] = []
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=factory,
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
            lifecycle_observer=lifecycle.append,
            submit_stage_observer=lambda _instruction, stage, _observed_ns: submit_stages.append(
                stage
            ),
        )
        observer = _Observer()
        adapter.start(observer)
        self.assertEqual(
            [
                "controlled_ingresses_attaching",
                "controlled_ingresses_attached",
                "controlled_session_opened",
                "controlled_api_version_verified",
                "controlled_trading_day_verified",
                "controlled_query_ingress_bound",
                "controlled_delegate_starting",
                "controlled_delegate_started",
            ],
            lifecycle,
        )
        accepted = adapter.submit(_instruction())
        self.assertEqual(accepted.venue_order_identity.venue_order_id, "91")
        self.assertEqual(
            [
                "C1P_AFTER_CONTROLLED_DELEGATE",
                "C1Q_AFTER_ADAPTER_REQUIREMENTS",
                "C1R_AFTER_CLIENT_REFERENCE",
                "C1S_AFTER_MAPPED_REQUEST",
            ],
            submit_stages,
        )
        assert factory.event_ingress is not None
        factory.event_ingress.offer(
            EmtNativeBrokerEvent(
                event_kind=BrokerEventKind.ORDER,
                account_id="account-1",
                trading_day=DAY,
                received_monotonic_ns=111,
                order_emt_id=91,
                order_client_id=17,
                order_status=50,
                leaves_qty=100,
            )
        )
        self.assertEqual(adapter.drain_broker_events(), 1)
        self.assertEqual(observer.events[0].venue_order_identity.client_order_ref, "17")
        canceled = adapter.cancel(
            CancelExecutionInstruction(
                cancel_request=CancelRequest(
                    cancel_request_id="cancel-1",
                    oms_order_id="oms-1",
                    execution_id="execution-1",
                    account_id="account-1",
                    consumer_trace=_trace(),
                    reason="operator request",
                    requested_monotonic_ns=1,
                ),
                venue_order_identity=VenueOrderIdentity(
                    venue_id="EMT",
                    adapter_instance_id="emt-sim-instance-1",
                    adapter_client_scope="7",
                    venue_order_id="91",
                ),
            )
        )
        self.assertEqual(canceled.venue_order_identity.venue_cancel_id, "92")
        adapter.stop()
        self.assertEqual(session.close_count, 1)

    def test_callback_wrapper_is_attached_before_session_open_and_forwards_events(self) -> None:
        session = _Session()
        factory = _Factory(session)
        wrappers: list[_ForwardingObserver] = []

        def wrap(observer):
            wrapper = _ForwardingObserver(observer)
            wrappers.append(wrapper)
            return wrapper

        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=factory,
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
            broker_event_observer_wrapper=wrap,
        )
        observer = _Observer()
        adapter.start(observer)
        assert factory.event_ingress is not None
        factory.event_ingress.offer(
            EmtNativeBrokerEvent(
                event_kind=BrokerEventKind.ORDER,
                account_id="account-1",
                trading_day=DAY,
                received_monotonic_ns=111,
                order_emt_id=91,
                order_client_id=17,
                order_status=50,
                leaves_qty=100,
            )
        )
        self.assertEqual(1, adapter.drain_broker_events())
        self.assertEqual((1, 1), (len(wrappers), wrappers[0].events))
        self.assertEqual(1, len(observer.events))
        adapter.stop()

    def test_execution_adapter_prepares_without_sdk_call_then_dispatches_once(self) -> None:
        session = _Session()
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=_Factory(session),
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
        )
        adapter.start(_Observer())
        prepared = adapter.prepare_submit(_instruction())
        self.assertFalse(hasattr(session.mutation_port, "last_submit"))
        accepted = adapter.submit_prepared(prepared)
        self.assertEqual("91", accepted.venue_order_identity.venue_order_id)
        self.assertEqual(17, session.mutation_port.last_submit.order_client_id)
        adapter.stop()

    def test_callback_drain_delegates_a_bounded_wait_to_the_ingress(self) -> None:
        session = _Session()
        factory = _Factory(session)
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=factory,
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
        )
        observer = _Observer()
        adapter.start(observer)
        assert factory.event_ingress is not None

        def offer_callback() -> None:
            sleep(0.01)
            factory.event_ingress.offer(
                EmtNativeBrokerEvent(
                    event_kind=BrokerEventKind.ORDER,
                    account_id="account-1",
                    trading_day=DAY,
                    received_monotonic_ns=111,
                    order_emt_id=91,
                    order_client_id=17,
                    order_status=50,
                    leaves_qty=100,
                )
            )

        producer = Thread(target=offer_callback)
        producer.start()
        self.assertEqual(1, adapter.drain_broker_events(wait_timeout_seconds=1.0))
        producer.join(timeout=1.0)
        self.assertFalse(producer.is_alive())
        self.assertEqual(1, len(observer.events))
        adapter.stop()

    def test_stop_keeps_event_ingress_attached_until_native_session_closes(self) -> None:
        session = _LateCallbackSession()
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=_LateCallbackFactory(session),
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
        )
        adapter.start(_Observer())
        adapter.stop()
        self.assertEqual(1, session.close_count)

    def test_isolated_worker_can_retain_closed_native_delegate_until_process_exit(self) -> None:
        session = _Session()
        lifecycle: list[str] = []
        retained: list[object] = []
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=_Factory(session),
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
            lifecycle_observer=lifecycle.append,
            native_reference_retainer=retained.append,
        )
        adapter.start(_Observer())
        adapter.stop()
        self.assertEqual(1, session.close_count)
        self.assertEqual(1, len(retained))
        self.assertEqual(
            [
                "controlled_native_reference_retain_starting",
                "controlled_native_reference_retained",
            ],
            lifecycle[-2:],
        )

    def test_isolated_worker_retains_released_session_after_startup_validation_failure(
        self,
    ) -> None:
        session = _Session()
        session.api_version = "incompatible"
        lifecycle: list[str] = []
        retained: list[object] = []
        adapter = EmtControlledExecutionAdapter(
            _mapping(),
            session_factory=_Factory(session),
            client_order_ref_factory=lambda instruction: "17",
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=5,
            lifecycle_observer=lifecycle.append,
            native_reference_retainer=retained.append,
        )

        with self.assertRaises(EmtControlledSessionError):
            adapter.start(_Observer())

        self.assertEqual(1, session.close_count)
        self.assertEqual([session], retained)
        self.assertIn("controlled_native_session_retain_starting", lifecycle)
        self.assertIn("controlled_native_session_retained", lifecycle)


if __name__ == "__main__":
    unittest.main()
