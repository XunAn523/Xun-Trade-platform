from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.fernet import Fernet
from emt_gateway.journal import FileJournal, JournalFailure, JournalRecord
from emt_gateway.recovery import (
    CommandCustody,
    EncryptedReplayStore,
    RecoveryDisposition,
    RecoveryError,
    recovery_plan,
)

_HASH = "a" * 64


class RecoveryTests(unittest.TestCase):
    def test_file_journal_exclusive_lease_rejects_second_owner(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "account.emtj"
            first = FileJournal(path)
            try:
                with self.assertRaises(JournalFailure):
                    FileJournal(path)
            finally:
                first.close()
            with FileJournal(path):
                pass

    def test_encrypted_payload_round_trip_never_uses_plaintext_file_name(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            store = EncryptedReplayStore(
                Path(temporary_directory), fernet_key=Fernet.generate_key()
            )
            payload = {"order": {"ticker": "example", "quantity": 100}}
            digest = store.preserve(command_id="command-sensitive", payload=payload)
            self.assertEqual(
                payload, store.load(command_id="command-sensitive", expected_payload_hash=digest)
            )
            contents = next(Path(temporary_directory).glob("*.fernet")).read_bytes()
            self.assertNotIn(b"example", contents)
            with self.assertRaises(RecoveryError):
                store.load(command_id="command-sensitive", expected_payload_hash="b" * 64)

    def test_only_not_sent_and_ambiguous_commands_are_returned_for_manual_decision(self) -> None:
        records = (
            JournalRecord(1, "COMMAND_PREPARED", "not-sent", "c1", _HASH, 1),
            JournalRecord(2, "COMMAND_PREPARED", "ambiguous", "c2", _HASH, 2),
            JournalRecord(3, "CALL_STARTED", "ambiguous", "c2", _HASH, 3),
            JournalRecord(4, "NATIVE_CALLBACK", "ambiguous", "callback", _HASH, 4),
            JournalRecord(5, "COMMAND_PREPARED", "accepted", "c3", _HASH, 5),
            JournalRecord(6, "NATIVE_ACCEPTED", "accepted", "c3", _HASH, 6),
        )
        plan = recovery_plan(records)
        self.assertEqual(
            {"not-sent": RecoveryDisposition.NOT_SENT, "ambiguous": RecoveryDisposition.RECONCILE},
            {entry.command_id: entry.disposition for entry in plan},
        )

    def test_command_custody_preserves_before_prepare_and_only_decrypts_ambiguous_commands(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            journal = FileJournal(root / "commands.emtj")
            custody = CommandCustody(
                journal=journal,
                payloads=EncryptedReplayStore(root / "payloads", fernet_key=Fernet.generate_key()),
            )
            try:
                prepared = custody.prepare(
                    command_id="ambiguous-command",
                    correlation_id="correlation",
                    payload={"quantity": 100},
                )
                custody.record_state(
                    kind="CALL_STARTED",
                    command_id="ambiguous-command",
                    correlation_id="correlation",
                    payload_hash=prepared.payload_hash,
                )
                pending = custody.pending()
                self.assertEqual(RecoveryDisposition.RECONCILE, pending[0].disposition)
                self.assertEqual({"quantity": 100}, custody.load_for_reconciliation(pending[0]))
            finally:
                journal.close()
