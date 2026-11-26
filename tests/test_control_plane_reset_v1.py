from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.consumer.control_root_v2 import AccountCursorControlRootV2
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.control_plane import (
    AccountStreamResetJournalV1,
    ApprovedStreamResetV1,
    StreamResetApprovalErrorV1,
    StreamResetExecutionErrorV1,
    encode_account_stream_reset_fact,
    persist_approved_stream_reset,
)
from xuanling_tradecore.oms import OmsJournalCommit


class _ReadableGeneration:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[str, CursorStreamRefV2]] = []

    def require_readable_generation(self, *, account_id: str, stream: CursorStreamRefV2) -> None:
        self.calls.append((account_id, stream))
        if self.fail:
            raise RuntimeError("unreadable")


class _RecordingResetFactPort:
    def __init__(self, *, journal_id: str, fail: bool = False) -> None:
        self.journal_id = journal_id
        self.fail = fail
        self.batches = []

    def append_account_stream_reset(self, fact):
        self.batches.append(fact)
        if self.fail:
            raise RuntimeError("Journal unavailable")
        return OmsJournalCommit(journal_id=self.journal_id, first_lsn=0, final_lsn=0, fact_count=1)


class StreamResetApprovalTests(unittest.TestCase):
    def test_independent_approval_renders_one_immutable_registry_edge(self) -> None:
        request = ApprovedStreamResetV1(
            account_id="sim-ordinary",
            maker_id="maker-a",
            checker_id="checker-b",
            approval_reference="M3-RESET-1",
            reset_context="reset-context-1",
            source=CursorStreamRefV2("journal-old", 1),
            target=CursorStreamRefV2("journal-new", 2),
        )
        edge = request.as_registry_edge()
        self.assertEqual("reset-context-1", edge.reset_context)
        self.assertEqual(request.source, edge.source)
        self.assertEqual(request.target, edge.target)

    def test_rejects_self_approval_and_non_new_target(self) -> None:
        source = CursorStreamRefV2("journal", 1)
        with self.assertRaises(StreamResetApprovalErrorV1):
            ApprovedStreamResetV1(
                "a", "operator", "operator", "ref", "ctx", source, CursorStreamRefV2("journal", 2)
            )
        with self.assertRaises(StreamResetApprovalErrorV1):
            ApprovedStreamResetV1("a", "maker", "checker", "ref", "ctx", source, source)

    def test_persists_only_from_current_tail_and_never_reuses_a_generation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = AccountCursorControlRootV2(Path(directory).resolve() / "account")
            source = CursorStreamRefV2("journal", 1)
            root.write_stream_registry(CursorStreamRegistryV2(current=source, streams=(source,)))
            request = ApprovedStreamResetV1(
                "a", "maker", "checker", "ref", "ctx", source, CursorStreamRefV2("journal", 2)
            )
            persist_approved_stream_reset(root=root, request=request)
            self.assertEqual(request.target, root.read_stream_registry().current)
            with self.assertRaises(StreamResetApprovalErrorV1):
                persist_approved_stream_reset(root=root, request=request)

    def test_executes_readable_target_then_edge_then_target_journal_fact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = AccountCursorControlRootV2(Path(directory).resolve() / "account")
            source = CursorStreamRefV2("journal-old", 1)
            target = CursorStreamRefV2("journal-new", 2)
            root.write_stream_registry(CursorStreamRegistryV2(current=source, streams=(source,)))
            request = ApprovedStreamResetV1(
                "a", "maker", "checker", "approval", "ctx", source, target
            )
            readiness = _ReadableGeneration()
            reset_fact_port = _RecordingResetFactPort(journal_id=target.journal_id)
            failures: list[str] = []
            result = AccountStreamResetJournalV1(
                root=root,
                reset_fact_port=reset_fact_port,
                readiness=readiness,
                fail_closed=failures.append,
            ).persist(request)
            self.assertEqual([(request.account_id, target)], readiness.calls)
            self.assertEqual(target, root.read_stream_registry().current)
            self.assertEqual([], failures)
            self.assertEqual(target.journal_id, result.journal_commit.journal_id)
            fact = reset_fact_port.batches[0]
            self.assertEqual("ACCOUNT_STREAM_RESET", fact.kind.value)
            self.assertEqual(request.decision_hash, fact.attributes["decision_hash"])
            self.assertEqual(fact, encode_account_stream_reset_fact(request))

    def test_journal_failure_calls_fail_closed_after_edge_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = AccountCursorControlRootV2(Path(directory).resolve() / "account")
            source = CursorStreamRefV2("journal-old", 1)
            target = CursorStreamRefV2("journal-new", 2)
            root.write_stream_registry(CursorStreamRegistryV2(current=source, streams=(source,)))
            request = ApprovedStreamResetV1(
                "a", "maker", "checker", "approval", "ctx", source, target
            )
            failures: list[str] = []
            worker = AccountStreamResetJournalV1(
                root=root,
                reset_fact_port=_RecordingResetFactPort(journal_id=target.journal_id, fail=True),
                readiness=_ReadableGeneration(),
                fail_closed=failures.append,
            )
            with self.assertRaises(StreamResetExecutionErrorV1):
                worker.persist(request)
            self.assertEqual(target, root.read_stream_registry().current)
            self.assertEqual(["account-stream-reset:RuntimeError"], failures)
