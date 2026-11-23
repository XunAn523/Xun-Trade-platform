from __future__ import annotations

from dataclasses import replace

import pytest
from xuanling_tradecore.consumer.emt_submit_disposition_v1 import (
    EmtSubmitDispositionErrorV1,
    MmapEmtSubmitDispositionJournalV1,
)
from xuanling_tradecore.consumer.execution_runtime_v1 import (
    ConsumerExecutionDispatchRecordV1,
    ConsumerExecutionDispatchStateV1,
)
from xuanling_tradecore.oms import MmapOmsFactJournalRecoveryRequired, MmapOmsFactJournalV1


def _record():
    return ConsumerExecutionDispatchRecordV1(
        command_id="execution-1",
        execution_id="execution-1",
        binding_id="binding-1",
        binding_generation=1,
        state=ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED,
        result_code="NATIVE_SUBMIT_ACCEPTED",
    )


def _sink(journal):
    return MmapEmtSubmitDispositionJournalV1(
        journal=journal, account_id="account-1", binding_id="binding-1", binding_generation=1
    )


def test_committed_disposition_reopens_without_duplicate(tmp_path):
    path = tmp_path / "journal.mmap"
    journal = MmapOmsFactJournalV1(path=path, journal_id="j", capacity=8)
    sink = _sink(journal)
    sink.record(_record())
    sink.record(_record())
    assert sink.get("execution-1") == _record()
    assert len(journal.scan().records) == 1
    journal.close()
    reopened = MmapOmsFactJournalV1(path=path, journal_id="j", capacity=8)
    try:
        sink = _sink(reopened)
        assert sink.get("execution-1") == _record()
        with pytest.raises(EmtSubmitDispositionErrorV1, match="conflicts"):
            sink.record(replace(_record(), result_code="OTHER"))
        with pytest.raises(EmtSubmitDispositionErrorV1, match="closed"):
            sink.get("execution-1")
    finally:
        reopened.close()


def test_wrong_binding_and_non_submit_dispositions_are_rejected(tmp_path):
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        sink = _sink(journal)
        with pytest.raises(EmtSubmitDispositionErrorV1):
            sink.record(replace(_record(), binding_id="foreign"))
        with pytest.raises(EmtSubmitDispositionErrorV1):
            sink.record(replace(_record(), state=ConsumerExecutionDispatchStateV1.CANCEL_ACCEPTED))
        with pytest.raises(EmtSubmitDispositionErrorV1):
            sink.record(replace(_record(), result_code="SECRET:VALUE"))
        assert journal.scan().records == ()
    finally:
        journal.close()


def test_torn_result_fails_closed_on_read(tmp_path):
    path = tmp_path / "journal.mmap"
    journal = MmapOmsFactJournalV1(path=path, journal_id="j", capacity=8)
    try:
        sink = _sink(journal)
        sink.record(_record())
        with path.open("r+b", buffering=0) as handle:
            handle.seek(4096 + 4096 - 8)
            handle.write(b"\0" * 8)
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            sink.get("execution-1")
        with pytest.raises(EmtSubmitDispositionErrorV1, match="closed"):
            sink.record(_record())
    finally:
        journal.close()
