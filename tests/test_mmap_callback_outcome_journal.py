from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from threading import Thread

from xuanling_tradecore.persistence.mmap_callback_outcome_journal import (
    MmapCallbackOutcomeJournal,
)
from xuanling_tradecore.persistence.mmap_pre_native_journal import (
    MmapJournalRecordKind,
    MmapPreNativeJournal,
)


@dataclass(frozen=True)
class _Binding:
    account_id: str = "account-1"


@dataclass(frozen=True)
class _CancelCommand:
    account_id: str = "account-1"
    cancel_request_id: str = "cancel-1"
    execution_id: str = "execution-1"
    target_client_order_ref: str = "client-1"
    payload_hash: str = "a" * 64


class MmapCallbackOutcomeJournalTests(unittest.TestCase):
    def test_cancel_command_uses_a_dedicated_native_writer_journal(self) -> None:
        """A callback-owned mmap file must never be reused by cancel dispatch."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            callback_lane = MmapPreNativeJournal(path=root / "callback.journal", capacity=4)
            cancel_lane = MmapPreNativeJournal(path=root / "cancel.journal", capacity=4)
            outcomes = MmapCallbackOutcomeJournal(
                journals={"callback": callback_lane},
                cancel_command_journals={"cancel": cancel_lane},
            )
            callback_errors: list[BaseException] = []

            def claim_callback_writer() -> None:
                try:
                    callback_lane.append_callback_observed(
                        monotonic_ns=10,
                        account_id="account-1",
                        execution_id="execution-1",
                        transition_id="transition-1",
                        payload_hash="b" * 64,
                        client_order_ref="client-1",
                        native_order_id="91",
                    )
                except BaseException as error:
                    callback_errors.append(error)

            callback_thread = Thread(target=claim_callback_writer)
            callback_thread.start()
            callback_thread.join()
            self.assertEqual([], callback_errors)

            outcomes.append_cancel_started(_Binding(), _CancelCommand(), native_order_id="91")
            outcomes.append_cancel_native_accepted(
                _Binding(), _CancelCommand(), native_order_id="92"
            )
            self.assertEqual(
                [
                    MmapJournalRecordKind.CANCEL_STARTED,
                    MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED,
                ],
                [record.kind for record in cancel_lane.records()],
            )
            outcomes.close()


if __name__ == "__main__":
    unittest.main()
