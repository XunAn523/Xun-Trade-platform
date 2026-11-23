"""Tests for the non-secret hash-manifest input to Linux attestation collection."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.platform_loader import PlatformLoadError
from tools.performance.collect_emt_linux_binding_attestation import load_expected_library_hashes

SHA_A = "a" * 64


class CollectEmtLinuxBindingAttestationTests(unittest.TestCase):
    def test_accepts_only_a_standalone_nonempty_string_hash_mapping(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "hashes.json"
            path.write_text(json.dumps({"vnemttrader.so": SHA_A}), encoding="utf-8")
            self.assertEqual({"vnemttrader.so": SHA_A}, load_expected_library_hashes(path))

            path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(PlatformLoadError, "non-empty object"):
                load_expected_library_hashes(path)

            path.write_text(json.dumps({"vnemttrader.so": 42}), encoding="utf-8")
            with self.assertRaisesRegex(PlatformLoadError, "string pairs"):
                load_expected_library_hashes(path)


if __name__ == "__main__":
    unittest.main()
