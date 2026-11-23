from __future__ import annotations

import unittest
from dataclasses import replace
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

from xuanling_tradecore.consumer import (
    ConsumerAccountView,
    ConsumerAuthorizationError,
    ConsumerGatewayConfigurationError,
    ConsumerGatewayV2,
    ConsumerOrderSignal,
    ConsumerSchemaNegotiationError,
    ConsumerVersionRange,
    CursorKeyRingV2,
    CursorSigningKeyV2,
    IntentReceipt,
    PositionedConsumerEventV2,
)
from xuanling_tradecore.consumer.bindings_v2 import ConsumerBindingSnapshotV2
from xuanling_tradecore.consumer.projection_reads_v2 import (
    ConsumerProjectionReadErrorV2,
    projection_read_operation,
)
from xuanling_tradecore.consumer.replay_v2 import (
    ConsumerCursorReplayError,
    ConsumerCursorReplayServiceV2,
    ConsumerReplaySliceV2,
)
from xuanling_tradecore.consumer.stream_registry_v2 import (
    CursorStreamRefV2,
    CursorStreamRegistryV2,
)
from xuanling_tradecore.domain import (
    AccountLifecycle,
    ConsumerAccountBinding,
    ConsumerApiVersion,
    ConsumerEvent,
    ConsumerEventPositionV2,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
    ConsumerTrace,
    ConsumerWireContractV2,
    OrderOrigin,
    ReconciliationStatus,
    RedisStreamIdV1,
    TradingDayEpoch,
)

_SHA = "a" * 64


def _snapshot(epoch: int = 3) -> ConsumerBindingSnapshotV2:
    return ConsumerBindingSnapshotV2(
        authorization_epoch=epoch,
        accounts=(
            ConsumerAccountBinding(
                "account-bind", 1, "consumer-1", "ca-1", "account-1", "sim", "EMT", "emt-1"
            ),
        ),
        strategies=(
            ConsumerStrategyBinding(
                "strategy-bind", 1, "consumer-1", "ca-1", "account-bind", "strategy-1"
            ),
        ),
    )


def _contract() -> ConsumerWireContractV2:
    return ConsumerWireContractV2(
        api_version=ConsumerApiVersion(2, 0),
        schema_hash=_SHA,
        golden_vectors_hash=_SHA,
        compatibility_matrix_hash=_SHA,
        header_size_bytes=32,
        max_frame_bytes=1_048_608,
    )


def _versions() -> ConsumerVersionRange:
    return ConsumerVersionRange(
        minimum_api_version=ConsumerApiVersion(2, 0),
        maximum_api_version=ConsumerApiVersion(2, 0),
        minimum_event_schema_version=2,
        maximum_event_schema_version=2,
    )


class ConsumerGatewayV2Tests(unittest.TestCase):
    def test_replay_account_search_does_not_disguise_reader_failure_or_stream_gap(self):
        first = _snapshot().accounts[0]
        second = replace(
            first, binding_id="account-bind-2", consumer_account_id="ca-2", account_id="account-2"
        )
        snapshot = replace(_snapshot(), accounts=(first, second))
        for failure in ("CURSOR_RECOVERY_UNAVAILABLE", "JOURNAL_GAP"):
            replay = Mock()
            replay.poll.side_effect = [
                ConsumerCursorReplayError("CURSOR_INVALID", "scope mismatch"),
                ConsumerCursorReplayError(failure, "private store error"),
            ]
            gateway = ConsumerGatewayV2(
                binding_snapshot=lambda: snapshot,
                account_view_for=lambda _: None,
                wire_contract=_contract(),
                event_replay=replay,
            )
            session = gateway.open_session(
                ConsumerPrincipal("consumer-1", "login", "name"), _versions()
            )
            with self.assertRaises(ConsumerCursorReplayError) as caught:
                gateway.poll_event_cursor(session, token="private", limit=100)
            self.assertEqual(failure, caught.exception.code)
            self.assertEqual(2, replay.poll.call_count)

    def test_replay_scope_mismatch_still_tries_next_bound_account(self):
        first = _snapshot().accounts[0]
        second = replace(
            first, binding_id="account-bind-2", consumer_account_id="ca-2", account_id="account-2"
        )
        snapshot = replace(_snapshot(), accounts=(first, second))
        replay = Mock()
        replay.poll.side_effect = [
            ConsumerCursorReplayError("CURSOR_INVALID", "scope mismatch"),
            "page",
        ]
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: snapshot,
            account_view_for=lambda _: None,
            wire_contract=_contract(),
            event_replay=replay,
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        self.assertEqual("page", gateway.poll_event_cursor(session, token="private", limit=100))
        self.assertEqual(second, replay.poll.call_args.kwargs["binding"])

    def test_final_delivery_guard_sees_operation_context_and_always_restores_it(self):
        seen = []
        reject = [False]

        def guard():
            seen.append(projection_read_operation())
            if reject[0]:
                raise ConsumerProjectionReadErrorV2("paused planner")

        gateway = ConsumerGatewayV2(
            binding_snapshot=_snapshot,
            account_view_for=lambda _: None,
            wire_contract=_contract(),
            projection_read_delivery_guard=guard,
            event_replay=cast(ConsumerCursorReplayServiceV2, SimpleNamespace()),
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        gateway.validate_projection_read_delivery(
            session, operation="CreateReplayPartitions", tokens=()
        )
        self.assertEqual(["CreateReplayPartitions"], seen)
        self.assertIsNone(projection_read_operation())
        reject[0] = True
        with self.assertRaises(ConsumerProjectionReadErrorV2):
            gateway.validate_projection_read_delivery(
                session, operation="CreateReplayPartitions", tokens=()
            )
        self.assertIsNone(projection_read_operation())

    def test_authorizes_bound_signal_before_delegating_to_oms_operations(self) -> None:
        binding = _snapshot().accounts[0]
        trace = ConsumerTrace(
            "consumer-1",
            "ca-1",
            "account-1",
            "strategy-1",
            "strategy-order-1",
            "attrib-1",
            1,
            OrderOrigin.STRATEGY,
        )

        class Operations:
            invoked = False

            def submit_signal(self, **kwargs: object) -> IntentReceipt:
                self.invoked = True
                self.kwargs = kwargs
                return IntentReceipt(
                    "request-1", "event-1", "order-1", "execution-1", "journal-1", 7, trace
                )

        operations = Operations()
        gateway = ConsumerGatewayV2(
            binding_snapshot=_snapshot,
            account_view_for=lambda _: self._ready_view(binding),
            wire_contract=_contract(),
            operations=operations,  # type: ignore[arg-type]
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        signal = ConsumerOrderSignal(
            "request-1",
            "ca-1",
            "strategy-1",
            "strategy-order-1",
            TradingDayEpoch("2026-09-03"),
            "000001",
            "SZ",
            "BUY",
            "LIMIT",
            "CASH",
            100,
            1000,
        )
        self.assertEqual("order-1", gateway.submit_signal(session, signal).oms_order_id)
        self.assertTrue(operations.invoked)
        self.assertEqual("strategy-bind", operations.kwargs["strategy_binding_id"])

        with self.assertRaises(ConsumerAuthorizationError):
            gateway.submit_signal(
                session,
                ConsumerOrderSignal(
                    "request-2",
                    "ca-1",
                    "other",
                    "strategy-order-2",
                    TradingDayEpoch("2026-09-03"),
                    "000001",
                    "SZ",
                    "BUY",
                    "LIMIT",
                    "CASH",
                    100,
                    1000,
                ),
            )

    def test_uses_live_session_to_issue_and_replay_account_cursor(self) -> None:
        state = {"snapshot": _snapshot()}
        binding = state["snapshot"].accounts[0]
        stream = CursorStreamRefV2("journal-1", 1)

        class Reader:
            def read_events(self, **kwargs: object) -> ConsumerReplaySliceV2:
                position = ConsumerEventPositionV2(RedisStreamIdV1(1, 0), 0)
                event = SimpleNamespace(
                    journal_id="journal-1",
                    account_id=binding.account_id,
                    consumer_trace=SimpleNamespace(
                        consumer_id="consumer-1",
                        consumer_account_id="ca-1",
                        account_id=binding.account_id,
                    ),
                )
                return ConsumerReplaySliceV2(
                    (PositionedConsumerEventV2(position, cast(ConsumerEvent, event)),),
                    position,
                    False,
                )

        replay = ConsumerCursorReplayServiceV2(
            key_ring_for=lambda _: CursorKeyRingV2(current=CursorSigningKeyV2("key", b"x" * 32)),
            registry_for=lambda _: CursorStreamRegistryV2(stream, (stream,)),
            event_reader=Reader(),
            cursor_ttl_ns=100,
            wall_clock_ns=lambda: 10,
        )
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: state["snapshot"],
            account_view_for=lambda _: self._ready_view(binding),
            wire_contract=_contract(),
            event_replay=replay,
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        page = gateway.poll_event_cursor(
            session,
            token=gateway.issue_event_cursor(session, consumer_account_id="ca-1"),
            limit=1,
        )
        self.assertEqual(1, len(page.items))

    @staticmethod
    def _ready_view(binding: ConsumerAccountBinding) -> ConsumerAccountView:
        return ConsumerAccountView(
            binding.consumer_account_id,
            binding.account_display_name,
            binding.venue_id,
            "emt-sim-v1",
            AccountLifecycle.READY,
            ReconciliationStatus.VALIDATED,
        )

    def test_current_binding_opens_session_and_lists_only_bound_account(self) -> None:
        state = {"snapshot": _snapshot()}
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: state["snapshot"],
            account_view_for=lambda binding: ConsumerAccountView(
                consumer_account_id=binding.consumer_account_id,
                account_display_name=binding.account_display_name,
                venue_id=binding.venue_id,
                capability_profile_id="emt-sim-v1",
                lifecycle=AccountLifecycle.READY,
                reconciliation_status=ReconciliationStatus.VALIDATED,
            ),
            wire_contract=_contract(),
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        self.assertEqual(3, session.binding_epoch)
        self.assertEqual(_SHA, gateway.contract_schema_hash(session))
        self.assertEqual(
            ("ca-1",), tuple(view.consumer_account_id for view in gateway.list_accounts(session))
        )

    def test_epoch_change_revokes_old_session_before_account_data_is_exposed(self) -> None:
        state = {"snapshot": _snapshot()}
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: state["snapshot"],
            account_view_for=lambda binding: ConsumerAccountView(
                binding.consumer_account_id,
                binding.account_display_name,
                binding.venue_id,
                "emt-sim-v1",
                AccountLifecycle.READY,
                ReconciliationStatus.VALIDATED,
            ),
            wire_contract=_contract(),
        )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        state["snapshot"] = _snapshot(epoch=4)
        with self.assertRaisesRegex(ConsumerAuthorizationError, "epoch is stale"):
            gateway.list_accounts(session)

    def test_transient_session_is_scoped_and_still_checks_current_epoch(self) -> None:
        state = {"snapshot": _snapshot()}
        gateway = ConsumerGatewayV2(
            binding_snapshot=lambda: state["snapshot"],
            account_view_for=lambda binding: self._ready_view(binding),
            wire_contract=_contract(),
        )
        principal = ConsumerPrincipal("consumer-1", "login", "name")
        session = gateway.open_transient_session(principal, _versions())
        self.assertEqual(
            ("ca-1",), tuple(v.consumer_account_id for v in gateway.list_accounts(session))
        )
        state["snapshot"] = _snapshot(epoch=4)
        with self.assertRaisesRegex(ConsumerAuthorizationError, "epoch is stale"):
            gateway.list_accounts(session)
        gateway.close_transient_session(session)
        with self.assertRaisesRegex(ConsumerAuthorizationError, "not active"):
            gateway.list_accounts(session)

    def test_unbound_principal_incompatible_version_and_mismatched_view_fail_closed(self) -> None:
        gateway = ConsumerGatewayV2(
            binding_snapshot=_snapshot,
            account_view_for=lambda binding: ConsumerAccountView(
                "other",
                binding.account_display_name,
                binding.venue_id,
                "emt-sim-v1",
                AccountLifecycle.READY,
                ReconciliationStatus.VALIDATED,
            ),
            wire_contract=_contract(),
        )
        with self.assertRaises(ConsumerAuthorizationError):
            gateway.open_session(ConsumerPrincipal("other", "login", "name"), _versions())
        with self.assertRaises(ConsumerSchemaNegotiationError):
            gateway.open_session(
                ConsumerPrincipal("consumer-1", "login", "name"),
                ConsumerVersionRange(ConsumerApiVersion(1, 0), ConsumerApiVersion(1, 0), 2, 2),
            )
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login", "name"), _versions()
        )
        with self.assertRaises(ConsumerGatewayConfigurationError):
            gateway.list_accounts(session)


if __name__ == "__main__":
    unittest.main()
