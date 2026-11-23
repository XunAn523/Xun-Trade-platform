"""F6 R0 tool contracts which are safe to exercise without an EMT session."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.acceptance.f6_r0_linux_attestation import (
    F6R0LinuxAttestationError,
    _normalized_elf_dependencies,
    _only_non_source_untracked_files,
    _parse_profiles,
    _require_disarmed_profiles,
    _validate_systemd_unit,
    build_evidence,
)
from xuanling_tradecore.runtime import ReleaseMaterialV1


def _digest(letter: str) -> str:
    return letter * 64


def _manifest():
    return ReleaseMaterialV1(
        artifact_hash=_digest("a"),
        emt_sdk_hash=_digest("b"),
        native_extension_hash=_digest("c"),
        elf_abi_hash=_digest("d"),
        systemd_unit_hash=_digest("e"),
        cgroup_policy_hash=_digest("f"),
        host_health_policy_hash=_digest("1"),
        journal_policy_hash=_digest("2"),
        control_root_policy_hash=_digest("3"),
        archive_policy_hash=_digest("4"),
        bundle_trust_policy_hash=_digest("5"),
        account_profile_hashes={"profile-a": _digest("6")},
    ).unsigned_manifest(
        manifest_id="f6-r0-test-1",
        release_epoch=1,
        signer_id="signer-a",
        valid_from_wall_ns=1,
        valid_until_wall_ns=2,
    )


class F6R0LinuxAttestationTests(unittest.TestCase):
    def test_disarmed_profile_and_path_free_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "profile.json"
            profile.write_text(json.dumps({"approval": {"approved": False}}), encoding="utf-8")
            profiles = _parse_profiles([f"profile-a={profile}"])
            _require_disarmed_profiles(profiles)
            evidence = build_evidence(
                manifest=_manifest(),
                checks={"clock_synchronized": True, "control_free_bytes": 1024},
            )
            rendered = json.dumps(evidence)
            self.assertEqual(evidence["runtime_mode"], "DISARMED")
            self.assertEqual(evidence["result"], "PASS")
            self.assertNotIn(str(profile), rendered)

    def test_rejects_armed_profile_and_bad_profile_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "profile.json"
            profile.write_text(json.dumps({"approval": {"approved": True}}), encoding="utf-8")
            with self.assertRaisesRegex(F6R0LinuxAttestationError, "DISARMED"):
                _require_disarmed_profiles({"profile-a": profile})
        with self.assertRaisesRegex(F6R0LinuxAttestationError, "label=path"):
            _parse_profiles(["invalid"])

    def test_systemd_parser_requires_hardening_and_disarmed_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            unit = Path(directory) / "runtime.service"
            unit.write_text(
                "\n".join(
                    (
                        "[Service]",
                        "ExecStart=/opt/project/bin/runtime --disarmed",
                        "LimitNOFILE=65536",
                        "MemoryMax=2G",
                        "ReadWritePaths=/var/lib/project",
                        "TasksMax=512",
                        "NoNewPrivileges=yes",
                        "PrivateTmp=yes",
                        "ProtectSystem=strict",
                        "ProtectHome=yes",
                    )
                ),
                encoding="utf-8",
            )
            self.assertEqual(_validate_systemd_unit(unit)["MemoryMax"], "2G")
            unit.write_text("[Service]\nExecStart=/opt/project/bin/runtime\n", encoding="utf-8")
            with self.assertRaisesRegex(F6R0LinuxAttestationError, "hardening"):
                _validate_systemd_unit(unit)

    def test_only_non_source_untracked_diagnostics_are_allowed(self) -> None:
        self.assertTrue(
            _only_non_source_untracked_files("?? artifacts/acceptance/f6/run.json\n?? lshw.txt\n")
        )
        self.assertFalse(
            _only_non_source_untracked_files(" M xuanling_tradecore/runtime/service.py\n")
        )
        self.assertFalse(_only_non_source_untracked_files("?? xuanling_tradecore/override.py\n"))

    def test_elf_fingerprint_normalizes_aslr_addresses_but_keeps_dependency_identity(self) -> None:
        before = "linux-vdso.so.1 (0x00007ffc00001000)\nlibc.so.6 => /lib/libc.so.6 (0x7f010000)\n"
        after = "linux-vdso.so.1 (0x00007ffcffff1000)\nlibc.so.6 => /lib/libc.so.6 (0x7f02ffff)\n"
        normalized = _normalized_elf_dependencies(before)
        self.assertEqual(normalized, _normalized_elf_dependencies(after))
        self.assertIn("/lib/libc.so.6", normalized)


if __name__ == "__main__":
    unittest.main()
