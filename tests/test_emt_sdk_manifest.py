from __future__ import annotations

import unittest
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.quality.emit_emt_sdk_manifest import build_manifest, write_manifest


class EmtSdkManifestTests(unittest.TestCase):
    def test_records_only_native_binaries_with_stable_relative_hashes(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "sdk"
            native = root / "lib" / "libvnemttrader.so"
            native.parent.mkdir(parents=True)
            native.write_bytes(b"native-binary")
            (root / "README.txt").write_text("not a binary", encoding="utf-8")

            manifest = build_manifest(sdk_root=root, sdk_version="2.27.0")
            output = Path(directory) / "manifest.json"
            write_manifest(manifest=manifest, output=output)
            persisted = output.read_text(encoding="utf-8")

        self.assertEqual(1, manifest["schema_version"])
        self.assertEqual("2.27.0", manifest["sdk_version"])
        self.assertEqual(
            [
                {
                    "path": "lib/libvnemttrader.so",
                    "bytes": len(b"native-binary"),
                    "sha256": sha256(b"native-binary").hexdigest(),
                }
            ],
            manifest["files"],
        )
        self.assertIn('"sdk_version": "2.27.0"', persisted)

    def test_rejects_missing_version_or_binary(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "sdk"
            root.mkdir()
            with self.assertRaisesRegex(ValueError, "sdk_version"):
                build_manifest(sdk_root=root, sdk_version=" ")
            with self.assertRaisesRegex(ValueError, "native EMT SDK binary"):
                build_manifest(sdk_root=root, sdk_version="2.27.0")
