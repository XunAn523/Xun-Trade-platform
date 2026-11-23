from __future__ import annotations

import json
import mmap
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from xuanling_tradecore.oms.sidecar_v1 import MmapSidecarFactReaderV1, OrderStateProjectorV1
from xuanling_tradecore.persistence import (
    MergeManifestV2Store,
    MmapJournalCapacityPolicyV2,
    MmapJournalShardInputV2,
    MmapPreNativeJournal,
    MmapReconciliationKindV2,
    MmapRecoveryCoordinatorV2,
    MmapRecoveryGateReasonV2,
    MmapRecoveryGateV2,
    MmapWriterOwnershipRegistryV2,
)
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapJournalRecoveryStatus

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_ROOT = Path(__file__).resolve().parents[1]
_MANIFEST_SCHEMA = (
    _ROOT / "contracts" / "evidence" / "v1" / "schemas" / "merge-manifest-v2.schema.json"
)


def _input(scope: str, shard: str, journal: MmapPreNativeJournal, digest: str = _DIGEST_A):
    return MmapJournalShardInputV2(
        scope_id=scope,
        shard_id=shard,
        journal=journal,
        route_directory_digest=digest,
        writer_identity=f"writer-{scope}-{shard}",
    )


def _pre(journal: MmapPreNativeJournal, *, execution: str = "execution-1", ref: str = "ref-1"):
    return journal.append_pre_native(
        monotonic_ns=10,
        account_id="account-1",
        execution_id=execution,
        command_id="command-1",
        payload_hash=_DIGEST_A,
        client_order_ref=ref,
    )


class MmapRecoveryCoordinatorV2Tests(unittest.TestCase):
    def test_crash_matrix_preserves_facts_and_never_exposes_auto_resend(self) -> None:
        """Exercise the F1 C2/C4/C5 interruption matrix with no venue port."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before_c2 = MmapPreNativeJournal(path=root / "before-c2.journal", capacity=4)
            after_c2 = MmapPreNativeJournal(path=root / "after-c2.journal", capacity=4)
            after_c4 = MmapPreNativeJournal(path=root / "after-c4.journal", capacity=4)
            _pre(after_c2)
            _pre(after_c4)
            after_c4.append_call_started(
                monotonic_ns=11,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
            )
            result = MmapRecoveryCoordinatorV2().recover(
                (
                    _input("scope-a", "before-c2", before_c2),
                    _input("scope-a", "after-c2", after_c2),
                    _input("scope-b", "after-c4", after_c4),
                )
            )
            self.assertEqual("DISARMED", result.gate.runtime_state)
            kinds = {item.kind for item in result.reconciliation_items}
            self.assertIn(MmapReconciliationKindV2.PRE_NATIVE, kinds)
            self.assertIn(MmapReconciliationKindV2.SUBMIT_UNKNOWN, kinds)
            # There is intentionally no send/retry API on the recovery
            # result. Its guard rejects rather than performing a side effect.
            with self.assertRaisesRegex(Exception, "DISARMED"):
                MmapRecoveryGateV2().require_submit(scope_id="scope-a")
            for journal in (before_c2, after_c2, after_c4):
                journal.close()

    def test_merges_scopes_seals_hash_chain_and_replays_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            native = MmapPreNativeJournal(path=root / "native.journal", capacity=8, journal_epoch=7)
            callback = MmapPreNativeJournal(
                path=root / "callback.journal", capacity=8, journal_epoch=7
            )
            _pre(native)
            native.append_call_started(
                monotonic_ns=11,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
            )
            native.append_native_accepted(
                monotonic_ns=12,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
                native_order_id="native-1",
            )
            callback.append_callback_observed(
                monotonic_ns=13,
                account_id="account-1",
                execution_id="execution-1",
                transition_id="transition-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
                native_order_id="native-1",
            )
            result = MmapRecoveryCoordinatorV2().recover(
                (_input("scope-a", "callback-0", callback), _input("scope-a", "native-0", native))
            )
            self.assertTrue(result.manifest.generated)
            self.assertTrue(result.manifest.sealed)
            self.assertEqual("DISARMED", result.gate.runtime_state)
            self.assertEqual((), result.gate.events)
            self.assertTrue(result.gate.admission_closed)
            self.assertEqual(1, len(result.recovered_routes))
            self.assertEqual(
                MmapReconciliationKindV2.NATIVE_ACCEPTED, result.reconciliation_items[0].kind
            )
            self.assertEqual(7, result.manifest.shards[0].journal_epoch)
            stored = MergeManifestV2Store(path=root / "merge-manifest.json")
            stored.write(result.manifest)
            self.assertEqual(result.manifest.manifest_hash, stored.read().manifest_hash)
            schema = json.loads(_MANIFEST_SCHEMA.read_text(encoding="utf-8"))
            self.assertEqual(
                [],
                list(
                    Draft202012Validator(schema).iter_errors(
                        json.loads(result.manifest.encoded().decode("ascii"))
                    )
                ),
            )

            native_facts = MmapSidecarFactReaderV1(
                scope_id="scope-a/native-0", activation_epoch=1, route_version=1
            ).read(native.records())
            projector = OrderStateProjectorV1()
            projector.apply(native_facts)
            replay = projector.apply(native_facts)
            self.assertEqual(3, replay.per_scope_applied_watermark["scope-a/native-0"])
            native.close()
            callback.close()

    def test_call_started_without_receipt_is_submit_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = MmapPreNativeJournal(path=Path(temporary) / "native.journal", capacity=4)
            _pre(journal)
            journal.append_call_started(
                monotonic_ns=11,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
            )
            result = MmapRecoveryCoordinatorV2().recover((_input("scope-a", "native-0", journal),))
            self.assertEqual(
                MmapReconciliationKindV2.SUBMIT_UNKNOWN, result.reconciliation_items[0].kind
            )
            journal.close()

    def test_tail_corruption_and_noncontiguous_frame_close_integrity_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "native.journal"
            journal = MmapPreNativeJournal(path=path, capacity=3)
            _pre(journal)
            journal.close()
            with path.open("r+b", buffering=0) as file:
                mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                mapped[4_096 + 512 - 8 : 4_096 + 512] = b"\0" * 8
                mapped.close()
            reopened = MmapPreNativeJournal(path=path, capacity=3)
            scan = reopened.scan_recovery_prefix()
            self.assertEqual(MmapJournalRecoveryStatus.UNTRUSTED_TAIL, scan.status)
            result = MmapRecoveryCoordinatorV2().recover((_input("scope-a", "native-0", reopened),))
            self.assertFalse(result.manifest.sealed)
            self.assertIn(
                MmapRecoveryGateReasonV2.JOURNAL_INTEGRITY,
                {event.reason for event in result.gate.events},
            )
            reopened.close()

            non_contiguous = MmapPreNativeJournal(
                path=Path(temporary) / "second.journal", capacity=3
            )
            _pre(non_contiguous)
            non_contiguous.append_call_started(
                monotonic_ns=11,
                account_id="account-1",
                execution_id="execution-1",
                command_id="command-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
            )
            non_contiguous.close()
            second_path = Path(temporary) / "second.journal"
            with second_path.open("r+b", buffering=0) as file:
                mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                mapped[4_096 : 4_096 + 512] = b"\0" * 512
                mapped.close()
            reopened_second = MmapPreNativeJournal(path=second_path, capacity=3)
            self.assertEqual(
                MmapJournalRecoveryStatus.NONCONTIGUOUS_TAIL,
                reopened_second.scan_recovery_prefix().status,
            )
            reopened_second.close()

    def test_callback_and_cancel_midwrite_are_recovery_gates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            callback_path = root / "callback.journal"
            cancel_path = root / "cancel.journal"
            callback = MmapPreNativeJournal(path=callback_path, capacity=3)
            callback.append_callback_observed(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                transition_id="transition-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
                native_order_id="native-1",
            )
            cancel = MmapPreNativeJournal(path=cancel_path, capacity=3)
            cancel.append_cancel_started(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="execution-1",
                cancel_request_id="cancel-1",
                payload_hash=_DIGEST_A,
                client_order_ref="ref-1",
                native_order_id="native-1",
            )
            callback.close()
            cancel.close()
            for path in (callback_path, cancel_path):
                with path.open("r+b", buffering=0) as file:
                    mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                    mapped[4_096 + 512 - 8 : 4_096 + 512] = b"\0" * 8
                    mapped.close()
            callback = MmapPreNativeJournal(path=callback_path, capacity=3)
            cancel = MmapPreNativeJournal(path=cancel_path, capacity=3)
            result = MmapRecoveryCoordinatorV2().recover(
                (_input("scope-a", "callback", callback), _input("scope-a", "cancel", cancel))
            )
            integrity_events = [
                event
                for event in result.gate.events
                if event.reason is MmapRecoveryGateReasonV2.JOURNAL_INTEGRITY
            ]
            self.assertEqual(2, len(integrity_events))
            callback.close()
            cancel.close()

    def test_crc_route_conflict_callback_gap_and_control_root_are_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            crc = MmapPreNativeJournal(path=root / "crc.journal", capacity=3)
            _pre(crc)
            crc.close()
            with (root / "crc.journal").open("r+b", buffering=0) as file:
                mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                mapped[4_100] ^= 0x01
                mapped.close()
            crc = MmapPreNativeJournal(path=root / "crc.journal", capacity=3)
            left = MmapPreNativeJournal(path=root / "left.journal", capacity=3)
            right = MmapPreNativeJournal(path=root / "right.journal", capacity=3)
            _pre(left, execution="execution-left", ref="same-ref")
            _pre(right, execution="execution-right", ref="same-ref")
            callback = MmapPreNativeJournal(path=root / "callback.journal", capacity=3)
            callback.append_callback_observed(
                monotonic_ns=10,
                account_id="account-1",
                execution_id="orphan-execution",
                transition_id="transition-1",
                payload_hash=_DIGEST_A,
                client_order_ref="orphan-ref",
                native_order_id="native-1",
            )
            result = MmapRecoveryCoordinatorV2().recover(
                (
                    _input("scope-a", "crc", crc),
                    _input("scope-a", "left", left),
                    _input("scope-a", "right", right),
                    _input("scope-a", "callback", callback),
                ),
                callback_stream_complete=False,
                control_root_complete=False,
            )
            reasons = {event.reason for event in result.gate.events}
            self.assertTrue(
                {
                    MmapRecoveryGateReasonV2.JOURNAL_INTEGRITY,
                    MmapRecoveryGateReasonV2.ROUTE_CONFLICT,
                    MmapRecoveryGateReasonV2.CALLBACK_GAP,
                    MmapRecoveryGateReasonV2.CONTROL_ROOT,
                }.issubset(reasons)
            )
            for journal in (crc, left, right, callback):
                journal.close()

    def test_capacity_projector_lag_and_manifest_drift_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            journal = MmapPreNativeJournal(path=root / "native.journal", capacity=4)
            _pre(journal)
            first = MmapRecoveryCoordinatorV2(
                capacity_policy=MmapJournalCapacityPolicyV2(
                    stop_new_at_percent=100, maximum_projector_lag_records=10
                )
            ).recover((_input("scope-a", "native", journal),))
            result = MmapRecoveryCoordinatorV2(
                capacity_policy=MmapJournalCapacityPolicyV2(
                    stop_new_at_percent=25, maximum_projector_lag_records=0
                )
            ).recover(
                (_input("scope-a", "native", journal, _DIGEST_B),),
                previous_manifest=first.manifest,
                projector_positions={("scope-a", "native"): 0},
            )
            self.assertTrue(
                {
                    MmapRecoveryGateReasonV2.CAPACITY,
                    MmapRecoveryGateReasonV2.PROJECTOR_LAG,
                    MmapRecoveryGateReasonV2.MANIFEST_DRIFT,
                }.issubset({event.reason for event in result.gate.events})
            )
            journal.close()

    def test_writer_identity_collision_closes_the_recovery_gate(self) -> None:
        gate = MmapRecoveryGateV2()
        ownership = MmapWriterOwnershipRegistryV2(gate=gate)
        ownership.claim(scope_id="scope-a", shard_id="native-0", writer_identity="worker-a")
        with self.assertRaisesRegex(Exception, "another writer"):
            ownership.claim(scope_id="scope-a", shard_id="native-0", writer_identity="worker-b")
        self.assertIn(
            MmapRecoveryGateReasonV2.WRITER_COLLISION,
            {event.reason for event in gate.snapshot().events},
        )


if __name__ == "__main__":
    unittest.main()
