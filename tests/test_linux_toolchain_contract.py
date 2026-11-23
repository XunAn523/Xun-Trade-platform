from __future__ import annotations

import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


class LinuxToolchainContractTests(unittest.TestCase):
    def test_lock_and_project_allow_only_supported_linux_runtime(self) -> None:
        project = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        lock = (_ROOT / "uv.lock").read_text(encoding="utf-8")

        self.assertIn('requires-python = ">=3.10,<3.11"', project)
        self.assertIn("sys_platform == 'linux' and platform_machine == 'x86_64'", project)
        self.assertNotIn("sys_platform == 'win32'", project)
        self.assertIn('requires-python = "==3.10.*"', lock)
        self.assertIn("platform_machine == 'x86_64' and sys_platform == 'linux'", lock)

    def test_ci_uses_frozen_system_python_and_emits_quality_evidence(self) -> None:
        pipeline = (_ROOT / ".gitlab-ci.yml").read_text(encoding="utf-8")

        self.assertIn("uv sync --frozen --no-managed-python --extra test", pipeline)
        self.assertIn("uv export --frozen --extra test --no-emit-project --no-hashes", pipeline)
        self.assertIn(
            "pip-audit --strict --no-deps --disable-pip --requirement "
            "artifacts/quality/third-party-requirements.txt",
            pipeline,
        )
        self.assertIn("cyclonedx-py environment --of JSON --output-reproducible", pipeline)
        self.assertIn("emt-sdk-manifest:", pipeline)
        self.assertIn('--sdk-root "$EMT_SDK_ROOT"', pipeline)
