from __future__ import annotations

import unittest

from xuanling_tradecore.control_plane import (
    ControlActionV1,
    ControlApprovalAuthorizerV1,
    ControlApprovalErrorV1,
    ControlApprovalPolicyV1,
    ControlMutationApprovalV1,
    ExternalOrderAttachmentErrorV1,
    ExternalOrderAttachmentRegistryV1,
    ExternalOrderAttachmentRequestV1,
    ExternalOrderAttachmentServiceV1,
    ExternalOrderHandlingAuthorityV1,
    VerifiedExternalOrderV1,
)
from xuanling_tradecore.domain import TradingDayEpoch, VenueOrderIdentity
from xuanling_tradecore.oms import OmsJournalCommit


class _Lookup:
    def __init__(self, value: VerifiedExternalOrderV1) -> None:
        self.value = value
        self.calls = 0

    def lookup_external_order(self, *, account_id, trading_day, identity):
        self.calls += 1
        assert (
            account_id == self.value.account_id
            and trading_day == self.value.trading_day
            and identity == self.value.identity
        )
        return self.value


class _Facts:
    def __init__(self) -> None:
        self.facts = []

    def append_external_order_attachment(self, fact):
        self.facts.append(fact)
        return OmsJournalCommit("journal-new", 0, 0, 1)


def _policy() -> ControlApprovalPolicyV1:
    roles = frozenset({"SYSTEM_OPERATOR"})
    return ControlApprovalPolicyV1(
        "control-policy", 2, "a" * 64, {action: roles for action in ControlActionV1}
    )


def _approval(
    *, nonce: str = "nonce-1", checker: str | None = "checker"
) -> ControlMutationApprovalV1:
    return ControlMutationApprovalV1(
        "command-1",
        ControlActionV1.ATTACH_EXTERNAL_ORDER,
        "account-1",
        "maker",
        frozenset({"SYSTEM_OPERATOR"}),
        checker,
        frozenset({"SYSTEM_OPERATOR"}) if checker is not None else frozenset(),
        "MANUAL_ATTACH",
        nonce,
        10,
        20,
        4,
        2,
        "a" * 64,
    )


def _identity() -> VenueOrderIdentity:
    return VenueOrderIdentity("EMT", "emt-fake", "scope-1", venue_order_id="venue-1")


def _verified(*, terminal: bool = False) -> VerifiedExternalOrderV1:
    return VerifiedExternalOrderV1(
        "account-1",
        TradingDayEpoch("2026-09-04"),
        _identity(),
        "000001",
        "BUY",
        100,
        1,
        terminal,
        "b" * 64,
    )


class ExternalOrderAttachmentTests(unittest.TestCase):
    def _request(self, **overrides):
        values = {
            "attachment_id": "attachment-1",
            "account_id": "account-1",
            "trading_day": TradingDayEpoch("2026-09-04"),
            "identity": _identity(),
            "expected_broker_order_digest": "b" * 64,
            "handling_authority": ExternalOrderHandlingAuthorityV1.STRATEGY_CONTROL,
            "strategy_id": "strategy-1",
            "approval": _approval(),
        }
        values.update(overrides)
        return ExternalOrderAttachmentRequestV1(**values)

    def test_authorizes_queries_then_writes_one_attachment_fact_without_broker_mutation(
        self,
    ) -> None:
        facts = _Facts()
        lookup = _Lookup(_verified())
        service = ExternalOrderAttachmentServiceV1(
            authorizer=ControlApprovalAuthorizerV1(_policy()),
            lookup=lookup,
            facts=facts,
            registry=ExternalOrderAttachmentRegistryV1(),
        )
        attached = service.attach(self._request(), current_control_version=4, evaluated_wall_ns=15)
        self.assertEqual(1, lookup.calls)
        self.assertEqual(1, len(facts.facts))
        self.assertEqual("ORDER_ATTACHED_EXTERNAL", facts.facts[0].kind.value)
        self.assertEqual("STRATEGY_CONTROL", facts.facts[0].attributes["authority"])
        self.assertEqual("strategy-1", attached.request.strategy_id)

    def test_rejects_replay_and_terminal_or_digest_mismatch_before_attachment_fact(self) -> None:
        for verified in (_verified(terminal=True), _verified()):
            with self.subTest(terminal=verified.terminal):
                facts = _Facts()
                request = self._request(
                    expected_broker_order_digest=("c" * 64 if not verified.terminal else "b" * 64)
                )
                service = ExternalOrderAttachmentServiceV1(
                    authorizer=ControlApprovalAuthorizerV1(_policy()),
                    lookup=_Lookup(verified),
                    facts=facts,
                    registry=ExternalOrderAttachmentRegistryV1(),
                )
                with self.assertRaises(ExternalOrderAttachmentErrorV1):
                    service.attach(request, current_control_version=4, evaluated_wall_ns=15)
                self.assertEqual([], facts.facts)
        authorizer = ControlApprovalAuthorizerV1(_policy())
        authorizer.authorize(_approval(), current_control_version=4, evaluated_wall_ns=15)
        with self.assertRaises(ControlApprovalErrorV1):
            authorizer.authorize(_approval(), current_control_version=4, evaluated_wall_ns=15)

    def test_maker_checker_and_precondition_fail_closed_before_lookup(self) -> None:
        with self.assertRaises(ControlApprovalErrorV1):
            _approval(checker="maker")
        lookup = _Lookup(_verified())
        service = ExternalOrderAttachmentServiceV1(
            authorizer=ControlApprovalAuthorizerV1(_policy()),
            lookup=lookup,
            facts=_Facts(),
            registry=ExternalOrderAttachmentRegistryV1(),
        )
        with self.assertRaises(ControlApprovalErrorV1):
            service.attach(self._request(), current_control_version=3, evaluated_wall_ns=15)
        self.assertEqual(0, lookup.calls)
