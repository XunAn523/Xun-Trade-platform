from __future__ import annotations

import unittest
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.platform_loader import (
    PlatformLoadError,
    is_project_venv_runtime,
    sdk_library_dir,
    verify_sdk_hashes,
)


class PlatformLoaderTests(unittest.TestCase):
    def test_project_venv_runtime_is_distinct_from_server_default_python(self) -> None:
        self.assertTrue(
            is_project_venv_runtime(
                prefix="/srv/emt-gateway/.venv",
                base_prefix="/opt/cpython-3.10",
            )
        )
        self.assertFalse(
            is_project_venv_runtime(
                prefix="/usr",
                base_prefix="/usr",
            )
        )

    def test_resolves_only_the_requested_platform_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lib" / "windows").mkdir(parents=True)
            (root / "lib" / "linux").mkdir()
            self.assertEqual(root / "lib" / "windows", sdk_library_dir(root, system="Windows"))
            self.assertEqual(root / "lib" / "linux", sdk_library_dir(root, system="Linux"))
            with self.assertRaises(PlatformLoadError):
                sdk_library_dir(root, system="Darwin")

    def test_hash_manifest_accepts_only_exact_plain_file_hashes(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "vnemttrader.pyd"
            binary.write_bytes(b"not-a-real-sdk")
            digest = sha256(binary.read_bytes()).hexdigest()
            verify_sdk_hashes(root, {"vnemttrader.pyd": digest})
            with self.assertRaises(PlatformLoadError):
                verify_sdk_hashes(root, {"../vnemttrader.pyd": digest})
            with self.assertRaises(PlatformLoadError):
                verify_sdk_hashes(root, {"vnemttrader.pyd": "0" * 64})
