"""Tests for fail-closed Linux EMT binding compatibility evidence."""

from __future__ import annotations

import unittest
from pathlib import Path

from emt_gateway.linux_attestation import _attest_loaded_binding
from emt_gateway.platform_loader import LoadedSdk, PlatformLoadError

SHA_A = "a" * 64
SHA_B = "b" * 64


class _ReadOnlyTraderApi:
    def createTraderApi(self) -> None: ...

    def setSoftwareVersion(self) -> None: ...

    def setHeartBeatInterval(self) -> None: ...

    def login(self) -> None: ...

    def logout(self) -> None: ...

    def release(self) -> None: ...

    def getApiVersion(self) -> None: ...

    def getTradingDay(self) -> None: ...

    def queryUnfinishedOrders(self) -> None: ...


def _loaded(trader_api_type: type[object] = _ReadOnlyTraderApi) -> LoadedSdk:
    module = type("SyntheticBinding", (), {"TraderApi": trader_api_type})
    return LoadedSdk(library_dir=Path("/opt/emt/sdk/lib/linux"), module=module)


class EmtLinuxBindingAttestationTests(unittest.TestCase):
    def test_attestation_is_sanitized_and_binds_the_declared_sdk_hash_set(self) -> None:
        attestation = _attest_loaded_binding(
            _loaded(),
            expected_library_hashes={"vnemttrader.so": SHA_A, "libemt_api.so": SHA_B},
            system="Linux",
            machine="x86_64",
            project_venv_active=True,
            python_implementation="cpython",
            python_version="3.10.18",
            host_id="linux-host-001",
        )

        self.assertEqual("Linux", attestation.system)
        self.assertEqual("x86_64", attestation.machine)
        self.assertEqual(".venv", attestation.python_runtime_scope)
        self.assertEqual(("libemt_api.so", "vnemttrader.so"), attestation.verified_library_files)
        self.assertEqual(64, len(attestation.host_id_sha256))
        self.assertEqual(64, len(attestation.sdk_package_sha256))
        rendered = str(attestation.as_dict())
        self.assertNotIn("linux-host-001", rendered)
        self.assertNotIn("/opt/emt", rendered)

    def test_wrong_platform_or_missing_read_only_method_fails_closed(self) -> None:
        with self.assertRaisesRegex(PlatformLoadError, "Linux x86_64"):
            _attest_loaded_binding(
                _loaded(),
                expected_library_hashes={"vnemttrader.so": SHA_A},
                system="Windows",
                machine="x86_64",
                project_venv_active=True,
                python_implementation="cpython",
                python_version="3.10.18",
                host_id="host",
            )

        incomplete = type("IncompleteTraderApi", (), {})
        with self.assertRaisesRegex(PlatformLoadError, "lacks required"):
            _attest_loaded_binding(
                _loaded(incomplete),
                expected_library_hashes={"vnemttrader.so": SHA_A},
                system="Linux",
                machine="x86_64",
                project_venv_active=True,
                python_implementation="cpython",
                python_version="3.10.18",
                host_id="host",
            )

    def test_server_default_python_cannot_produce_an_attestation(self) -> None:
        with self.assertRaisesRegex(PlatformLoadError, "project .venv"):
            _attest_loaded_binding(
                _loaded(),
                expected_library_hashes={"vnemttrader.so": SHA_A},
                system="Linux",
                machine="x86_64",
                project_venv_active=False,
                python_implementation="cpython",
                python_version="3.10.18",
                host_id="host",
            )


if __name__ == "__main__":
    unittest.main()
