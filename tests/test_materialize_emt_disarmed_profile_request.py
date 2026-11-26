"""Tests for local, payload-silent EMT DISARMED request materialization."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.performance.bootstrap_emt_disarmed_profile import build_bootstrap_artifacts
from tools.performance.build_emt_disarmed_profile import load_signing_request
from tools.performance.materialize_emt_disarmed_profile_request import (
    materialize_disarmed_profile_request,
)
from xuanling_tradecore.adapters.emt import EmtDisarmedProfileError


class MaterializeEmtDisarmedProfileRequestTests(unittest.TestCase):
    def _bootstrap(self, root: Path) -> tuple[Path, Path]:
        private_path = root / ".local-secrets" / "private.json"
        public_path = root / "data" / "public.json"
        revocations_path = root / "data" / "revocations.json"
        template_path = root / "data" / "template.json"
        build_bootstrap_artifacts(
            signer_id="emt-sim-disarmed-signer-v1",
            profile_id="emt-sim-three-scope-disarmed-v1",
            private_keys_path=private_path,
            public_key_path=public_path,
            revocations_path=revocations_path,
            request_path=template_path,
            now_wall_ns=1_000_000_000,
        )
        return template_path, private_path

    @staticmethod
    def _accounts(path: Path) -> tuple[str, str, str]:
        values = ("ordinary-account", "credit-account", "option-account")
        path.write_text(
            json.dumps(
                {
                    "gateway": {"host": "127.0.0.1", "port": 19088},
                    "ordinary": {"account": values[0], "password": "synthetic-ordinary-password"},
                    "credit": {"account": values[1], "password": "synthetic-credit-password"},
                    "option": {"account": values[2], "password": "synthetic-option-password"},
                }
            ),
            encoding="utf-8",
        )
        return values

    def test_materializes_three_scopes_without_serializing_passwords(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            template_path, _ = self._bootstrap(root)
            accounts_path = root / "accounts.json"
            account_values = self._accounts(accounts_path)
            output_path = root / "materialized.json"
            materialize_disarmed_profile_request(
                template_path=template_path,
                accounts_path=accounts_path,
                output_path=output_path,
                now_wall_ns=1_000_000_001,
            )
            rendered = output_path.read_text(encoding="utf-8")
            raw = json.loads(rendered)
            self.assertFalse(raw["template"])
            self.assertEqual(
                account_values, tuple(item["account_id"] for item in raw["scope_ownerships"])
            )
            self.assertNotIn("synthetic-ordinary-password", rendered)
            self.assertNotIn("synthetic-credit-password", rendered)
            self.assertNotIn("synthetic-option-password", rendered)
            self.assertTrue(
                all(not item["uniqueness_probe_succeeded"] for item in raw["scope_ownerships"])
            )
            self.assertEqual(3, len(load_signing_request(output_path)[1]))

    def test_rejects_expired_templates_and_existing_output(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            template_path, _ = self._bootstrap(root)
            accounts_path = root / "accounts.json"
            self._accounts(accounts_path)
            output_path = root / "materialized.json"
            with self.assertRaisesRegex(EmtDisarmedProfileError, "not active"):
                materialize_disarmed_profile_request(
                    template_path=template_path,
                    accounts_path=accounts_path,
                    output_path=output_path,
                    now_wall_ns=1_000_000_000 + 8 * 24 * 60 * 60 * 1_000_000_000,
                )
            output_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "refuses to replace"):
                materialize_disarmed_profile_request(
                    template_path=template_path,
                    accounts_path=accounts_path,
                    output_path=output_path,
                    now_wall_ns=1_000_000_001,
                )


if __name__ == "__main__":
    unittest.main()
