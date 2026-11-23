"""Tests for local-only three-scope EMT DISARMED material initialization."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.performance.bootstrap_emt_disarmed_profile import (
    EmtDisarmedBootstrapError,
    build_bootstrap_artifacts,
)
from tools.performance.build_emt_disarmed_profile import load_signing_request
from xuanling_tradecore.adapters.emt import EmtDisarmedProfileError


class BootstrapEmtDisarmedProfileTests(unittest.TestCase):
    def _paths(self, root: Path) -> tuple[Path, Path, Path, Path]:
        return (
            root / ".local-secrets" / "private-keys.json",
            root / "data" / "public-key.json",
            root / "data" / "revocations.json",
            root / "data" / "request-template.json",
        )

    def test_creates_three_scope_template_and_separates_private_material(self) -> None:
        with TemporaryDirectory() as temporary:
            paths = self._paths(Path(temporary))
            build_bootstrap_artifacts(
                signer_id="emt-sim-disarmed-signer-v1",
                profile_id="emt-sim-three-scope-disarmed-v1",
                private_keys_path=paths[0],
                public_key_path=paths[1],
                revocations_path=paths[2],
                request_path=paths[3],
                now_wall_ns=1_000_000_000,
            )
            private_text = paths[0].read_text(encoding="utf-8")
            public_text = paths[1].read_text(encoding="utf-8")
            request = json.loads(paths[3].read_text(encoding="utf-8"))
            self.assertNotIn("private", public_text.lower())
            self.assertNotIn(private_text.strip(), public_text)
            self.assertTrue(request["template"])
            self.assertEqual(3, len(request["scope_ownerships"]))
            self.assertEqual(
                ["7001", "7002", "7003"],
                [item["adapter_client_scope"] for item in request["scope_ownerships"]],
            )
            with self.assertRaisesRegex(EmtDisarmedProfileError, "is a template"):
                load_signing_request(paths[3])

    def test_refuses_to_replace_any_existing_material(self) -> None:
        with TemporaryDirectory() as temporary:
            paths = self._paths(Path(temporary))
            build_bootstrap_artifacts(
                signer_id="emt-sim-disarmed-signer-v1",
                profile_id="emt-sim-three-scope-disarmed-v1",
                private_keys_path=paths[0],
                public_key_path=paths[1],
                revocations_path=paths[2],
                request_path=paths[3],
                now_wall_ns=1_000_000_000,
            )
            with self.assertRaisesRegex(EmtDisarmedBootstrapError, "refuses to replace"):
                build_bootstrap_artifacts(
                    signer_id="emt-sim-disarmed-signer-v1",
                    profile_id="emt-sim-three-scope-disarmed-v1",
                    private_keys_path=paths[0],
                    public_key_path=paths[1],
                    revocations_path=paths[2],
                    request_path=paths[3],
                    now_wall_ns=1_000_000_000,
                )


if __name__ == "__main__":
    unittest.main()
