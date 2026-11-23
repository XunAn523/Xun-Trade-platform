"""Native side-effect Journal fencing tests for the TradeCore EMT port."""

from __future__ import annotations

import unittest

from emt_gateway.tradecore_mutation import EmtPlatformMutationPort, EmtTradeCoreMutationError
from xuanling_tradecore.adapters.emt import EmtCancelRequest, EmtSubmitRequest


class _Api:
    def __init__(self) -> None:
        self.insert_calls = 0
        self.cancel_calls = 0

    def insertOrder(self, request, session_id) -> int:
        del request, session_id
        self.insert_calls += 1
        return 91

    def cancelOrder(self, order_emt_id, session_id) -> int:
        del order_emt_id, session_id
        self.cancel_calls += 1
        return 92

    def getApiLastError(self) -> dict[str, int]:
        return {"error_id": 7}


class _Journal:
    def __init__(self, *, fail_at: str | None = None) -> None:
        self.events: list[str] = []
        self.fail_at = fail_at

    def _record(self, event: str) -> None:
        self.events.append(event)
        if self.fail_at == event:
            raise RuntimeError(event)

    def record_submit_started(self, request) -> None:
        del request
        self._record("submit_started")

    def record_submit_accepted(self, request, receipt) -> None:
        del request, receipt
        self._record("submit_accepted")

    def record_cancel_started(self, request) -> None:
        del request
        self._record("cancel_started")

    def record_cancel_accepted(self, request, receipt) -> None:
        del request, receipt
        self._record("cancel_accepted")


class TradeCoreMutationJournalTests(unittest.TestCase):
    def test_submit_and_cancel_are_fenced_by_started_and_accepted_records(self) -> None:
        api, journal = _Api(), _Journal()
        port = EmtPlatformMutationPort(
            api,
            native_session_id=7,
            order_request_factory=lambda request: {"order_client_id": request.order_client_id},
            native_call_journal=journal,
            clock=lambda: 101,
        )
        submit = port.submit(_submit_request())
        cancel = port.cancel(_cancel_request())
        self.assertEqual((submit.order_emt_id, cancel.order_cancel_emt_id), (91, 92))
        self.assertEqual(api.insert_calls, 1)
        self.assertEqual(api.cancel_calls, 1)
        self.assertEqual(
            journal.events,
            ["submit_started", "submit_accepted", "cancel_started", "cancel_accepted"],
        )

    def test_pre_native_journal_failure_blocks_the_sdk_call(self) -> None:
        api = _Api()
        port = EmtPlatformMutationPort(
            api,
            native_session_id=7,
            order_request_factory=lambda request: {"order_client_id": request.order_client_id},
            native_call_journal=_Journal(fail_at="submit_started"),
        )
        with self.assertRaisesRegex(EmtTradeCoreMutationError, "CALL_STARTED"):
            port.submit(_submit_request())
        self.assertEqual(api.insert_calls, 0)

    def test_post_native_journal_failure_never_reports_false_success(self) -> None:
        api = _Api()
        port = EmtPlatformMutationPort(
            api,
            native_session_id=7,
            order_request_factory=lambda request: {"order_client_id": request.order_client_id},
            native_call_journal=_Journal(fail_at="submit_accepted"),
        )
        with self.assertRaisesRegex(EmtTradeCoreMutationError, "was accepted.*reconcile"):
            port.submit(_submit_request())
        self.assertEqual(api.insert_calls, 1)


def _submit_request() -> EmtSubmitRequest:
    from tests.test_tradecore_order_request import _request

    return _request()


def _cancel_request() -> EmtCancelRequest:
    from tests.test_emt_adapter_profile import _cancel_instruction

    return EmtCancelRequest(cancel_request=_cancel_instruction().cancel_request, order_emt_id=91)


if __name__ == "__main__":
    unittest.main()
