from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

_PATH = Path(__file__).resolve().parents[1] / "tools" / "quality" / "check_toolchain_manifest.py"
_SPEC = importlib.util.spec_from_file_location("check_toolchain_manifest", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


class ToolchainManifestTests(unittest.TestCase):
    def test_committed_manifest_has_the_expected_shape(self) -> None:
        manifest = _MODULE.load_manifest(_PATH.with_name("toolchain-manifest.v1.json"))
        self.assertEqual(1, manifest["schema_version"])
        self.assertEqual("0.12.7", manifest["tools"]["uv"]["version"])
        self.assertEqual(64, len(manifest["tools"]["uv"]["sha256"]))

    def test_missing_required_tool_fails_closed(self) -> None:
        manifest = _MODULE.load_manifest(_PATH.with_name("toolchain-manifest.v1.json"))
        with (
            patch.object(_MODULE.platform, "system", return_value="Linux"),
            patch.object(_MODULE.platform, "machine", return_value="x86_64"),
            patch.object(
                _MODULE,
                "_require_version",
                side_effect=_MODULE.ToolchainManifestError("missing"),
            ),
            self.assertRaisesRegex(_MODULE.ToolchainManifestError, "missing"),
        ):
            _MODULE.verify_manifest(manifest)
