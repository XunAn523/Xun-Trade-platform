from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.acceptance.f2_controlled_execution_cycle import (
    F2ControlledExecutionCycleError,
    Settings,
    _load_expected_library_hashes,
    _load_owned_source_native_acceptance_targets,
    _R1CrashAfterNativeReturnTelemetry,
    _required_check_names,
)
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapPreNativeJournal


class F2ControlledExecutionCycleTests(unittest.TestCase):
    def test_owned_source_query_only_requires_preflight_and_no_approval(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk_root = root / "sdk"
            sdk_root.mkdir()
            expected = root / "expected.json"
            accounts = root / "accounts.json"
            fixture = root / "fixture.json"
            source = root / "source.journal"
            for path in (expected, accounts, fixture, source):
                path.write_text("{}", encoding="utf-8")
            common = dict(
                sdk_root=sdk_root,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
                recovery_source_pre_native_journal=source,
                recovery_source_query_only=True,
            )
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "query-only"):
                    Settings(**common).validate()
                Settings(**common, preflight_only=True).validate()
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "no approval"):
                    Settings(
                        **common,
                        preflight_only=True,
                        recovery_approval_id="unexpected",
                    ).validate()

    def test_owned_source_target_loader_rejects_ambiguous_client_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.journal"
            journal = MmapPreNativeJournal(path=path, capacity=4)
            journal.append_native_accepted(
                monotonic_ns=1,
                account_id="emt_01",
                execution_id="execution-a",
                command_id="command-a",
                payload_hash="a" * 64,
                client_order_ref="101",
                native_order_id="201",
            )
            journal.append_native_accepted(
                monotonic_ns=2,
                account_id="emt_01",
                execution_id="execution-b",
                command_id="command-b",
                payload_hash="b" * 64,
                client_order_ref="101",
                native_order_id="202",
            )
            journal.close()

            with self.assertRaisesRegex(F2ControlledExecutionCycleError, "ambiguous"):
                _load_owned_source_native_acceptance_targets(path)

    def test_owned_source_target_loader_returns_only_native_acceptances(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.journal"
            journal = MmapPreNativeJournal(path=path, capacity=4)
            journal.append_pre_native(
                monotonic_ns=1,
                account_id="emt_01",
                execution_id="execution-a",
                command_id="command-a",
                payload_hash="a" * 64,
                client_order_ref="101",
            )
            journal.append_native_accepted(
                monotonic_ns=2,
                account_id="emt_01",
                execution_id="execution-a",
                command_id="command-a",
                payload_hash="a" * 64,
                client_order_ref="101",
                native_order_id="201",
            )
            journal.close()

            targets = _load_owned_source_native_acceptance_targets(path)

            self.assertEqual(1, len(targets))
            self.assertEqual((101, 201), (targets[0].order_client_id, targets[0].order_emt_id))

    def test_loads_a_nonempty_string_hash_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sdk-hashes.json"
            path.write_text(json.dumps({"vnemttrader.so": "a" * 64}), encoding="utf-8")
            self.assertEqual({"vnemttrader.so": "a" * 64}, _load_expected_library_hashes(path))

    def test_rejects_empty_or_non_string_hash_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sdk-hashes.json"
            path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(F2ControlledExecutionCycleError, "empty"):
                _load_expected_library_hashes(path)
            path.write_text(json.dumps({"vnemttrader.so": 1}), encoding="utf-8")
            with self.assertRaisesRegex(F2ControlledExecutionCycleError, "entry"):
                _load_expected_library_hashes(path)

    def test_credit_preflight_and_fault_mode_are_explicitly_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk_root = root / "sdk"
            sdk_root.mkdir()
            expected = root / "expected.json"
            accounts = root / "accounts.json"
            fixture = root / "fixture.json"
            for path in (expected, accounts, fixture):
                path.write_text("{}", encoding="utf-8")
            common = dict(
                sdk_root=sdk_root,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
            )
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "query-only"):
                    Settings(**common, account_profile="credit").validate()
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "unsupported"):
                    Settings(**common, fault_mode="unexpected").validate()
                Settings(
                    **common,
                    account_profile="credit",
                    preflight_only=True,
                    fault_mode="host-health-integrity",
                ).validate()
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "query-only"):
                    Settings(**common, r1_authoritative_query_only=True).validate()
                Settings(
                    **common,
                    preflight_only=True,
                    r1_authoritative_recovery_probe=True,
                    r1_authoritative_query_only=True,
                ).validate()

    def test_required_checks_exclude_fault_sentinels_from_normal_cycle(self) -> None:
        """A normal F2 cycle must not require checks for a non-injected fault."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            common = dict(
                sdk_root=root,
                expected_library_hashes=root / "expected.json",
                accounts_file=root / "accounts.json",
                fixture=root / "fixture.json",
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
            )
            normal = _required_check_names(Settings(**common))
            self.assertIn("submit_through_guard_route_scheduler", normal)
            self.assertIn("preflight_target_ref_absent", normal)
            self.assertIn("post_cleanup_target_ref_absent", normal)
            self.assertNotIn("preflight_unfinished_zero", normal)
            self.assertNotIn("fault_admission_closed", normal)
            fault = _required_check_names(
                Settings(**common, preflight_only=True, fault_mode="host-health-integrity")
            )
            self.assertIn("fault_admission_closed", fault)
            self.assertNotIn("submit_through_guard_route_scheduler", fault)
            replay_seed = _required_check_names(
                Settings(
                    **common,
                    r1_authoritative_recovery_probe=True,
                    r1_replay_seed_only=True,
                )
            )
            self.assertIn("submit_through_guard_route_scheduler", replay_seed)
            self.assertNotIn("query_after_submit_terminal", replay_seed)
            self.assertNotIn("r1_authoritative_double_scan", replay_seed)
            r1_full = _required_check_names(
                Settings(**common, r1_authoritative_recovery_probe=True)
            )
            self.assertIn("r1_controlled_callback_echo", r1_full)

    def test_replay_seed_requires_the_normal_external_r1_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            expected, accounts, fixture = (
                root / "expected.json",
                root / "accounts.json",
                root / "fixture.json",
            )
            for path in (expected, accounts, fixture):
                path.write_text("{}", encoding="utf-8")
            settings = Settings(
                sdk_root=sdk,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
                r1_replay_seed_only=True,
            )
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "replay seed"):
                    settings.validate()

    def test_external_r1_release_requires_complete_control_plane_and_bootstrap_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            release, trust, expected, accounts, fixture = (
                root / "release.json",
                root / "trust.json",
                root / "expected.json",
                root / "accounts.json",
                root / "fixture.json",
            )
            for path in (release, trust, expected, accounts, fixture):
                path.write_text("{}", encoding="utf-8")
            common = dict(
                sdk_root=sdk,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
                r1_authoritative_recovery_probe=True,
                r1_namespace_release=release,
                r1_namespace_trust=trust,
                r1_namespace_state=root / "state.json",
                r1_runtime_release_hash="a" * 64,
            )
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "bootstrap"):
                    Settings(**common).validate()
                Settings(
                    **common,
                    r1_bootstrap_approval_id="r1-test-approval",
                    r1_bootstrap_approver_id="r1-test-operator",
                ).validate()

    def test_replay_witness_proposal_requires_a_new_full_external_r1_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            release, trust, expected, accounts, fixture = (
                root / "release.json",
                root / "trust.json",
                root / "expected.json",
                root / "accounts.json",
                root / "fixture.json",
            )
            for path in (release, trust, expected, accounts, fixture):
                path.write_text("{}", encoding="utf-8")
            common = dict(
                sdk_root=sdk,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
                r1_authoritative_recovery_probe=True,
                r1_namespace_release=release,
                r1_namespace_trust=trust,
                r1_namespace_state=root / "state.json",
                r1_runtime_release_hash="a" * 64,
                r1_bootstrap_approval_id="r1-test-approval",
                r1_bootstrap_approver_id="r1-test-operator",
            )
            witness = root / "evidence" / "restart-witness.json"
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                Settings(**common, r1_replay_witness_proposal_output=witness).validate()
                witness.parent.mkdir()
                witness.write_text("{}", encoding="utf-8")
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "new full R1"):
                    Settings(**common, r1_replay_witness_proposal_output=witness).validate()
                without_external = dict(common)
                for name in (
                    "r1_namespace_release",
                    "r1_namespace_trust",
                    "r1_namespace_state",
                    "r1_runtime_release_hash",
                ):
                    without_external[name] = None
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "new full R1"):
                    Settings(
                        **without_external,
                        r1_replay_witness_proposal_output=root / "other-witness.json",
                    ).validate()

    def test_r2_marker_is_payload_free_and_single_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "evidence" / "r2-marker.json"
            telemetry = _R1CrashAfterNativeReturnTelemetry(marker)
            telemetry._write_marker(123)  # type: ignore[attr-defined]
            document = json.loads(marker.read_text(encoding="utf-8"))
            self.assertEqual(
                "R2_AFTER_INSERT_RETURN_BEFORE_NATIVE_ACCEPTED_PERSISTENCE",
                document["boundary"],
            )
            self.assertEqual(False, document["payloads_retained"])
            self.assertEqual(False, document["credentials_retained"])

    def test_manual_r2_cleanup_requires_approval_and_has_its_own_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            release, trust, expected, accounts, fixture, route_ledger = (
                root / "release.json",
                root / "trust.json",
                root / "expected.json",
                root / "accounts.json",
                root / "fixture.json",
                root / "r2-route-ledger.mmap",
            )
            for path in (release, trust, expected, accounts, fixture, route_ledger):
                path.write_text("{}", encoding="utf-8")
            common = dict(
                sdk_root=sdk,
                expected_library_hashes=expected,
                accounts_file=accounts,
                fixture=fixture,
                output=root / "report.json",
                runtime_dir=root / "runtime",
                trading_day="2026-09-09",
                session_client_id=1,
                callback_timeout_seconds=5.0,
                execute_simulated=True,
                isolated_native_process=True,
                r1_authoritative_recovery_probe=True,
                r1_namespace_release=release,
                r1_namespace_trust=trust,
                r1_namespace_state=root / "state.json",
                r1_runtime_release_hash="a" * 64,
                r1_bootstrap_approval_id="r1-test-approval",
                r1_bootstrap_approver_id="r1-test-operator",
                r1_manual_cleanup_route_ledger=route_ledger,
                r1_manual_cleanup_source_release=release,
                r1_manual_cleanup_source_trust=trust,
            )
            with patch(
                "tools.acceptance.f2_controlled_execution_cycle.platform.system",
                return_value="Linux",
            ):
                with self.assertRaisesRegex(F2ControlledExecutionCycleError, "explicit approval"):
                    Settings(**common).validate()
                settings = Settings(
                    **common,
                    recovery_approval_id="r1-manual-cleanup",
                    recovery_approver_id="test-operator",
                )
                settings.validate()
            checks = _required_check_names(settings)
            self.assertIn("r1_manual_target_double_scan", checks)
            self.assertIn("r1_manual_cleanup_only", checks)
            self.assertNotIn("r1_rehydration_persisted", checks)


if __name__ == "__main__":
    unittest.main()
