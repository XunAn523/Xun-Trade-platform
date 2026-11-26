from __future__ import annotations

import json
import unittest
from pathlib import Path

from xuanling_tradecore.control_plane import ControlAccountStatusV1, ControlPlaneServiceV1


class _Status:
    def read_account_status(self, account_id: str) -> ControlAccountStatusV1:
        return ControlAccountStatusV1(account_id, "DISARMED", "REQUIRED", 4, ("RECONCILIATION",))


class _Attachments:
    def attach(self, request, *, current_control_version, evaluated_wall_ns):
        return (request, current_control_version, evaluated_wall_ns)


class ControlPlaneOpenApiTests(unittest.TestCase):
    def test_openapi_is_internal_only_and_declares_draft_2020_12_control_mutation(self) -> None:
        root = Path(__file__).resolve().parents[1]
        spec = json.loads(
            (root / "contracts" / "control_plane" / "v1" / "openapi.yaml").read_text()
        )
        self.assertEqual("3.2.0", spec["openapi"])
        self.assertEqual("https://json-schema.org/draft/2020-12/schema", spec["jsonSchemaDialect"])
        self.assertEqual("http://127.0.0.1", spec["servers"][0]["url"])
        self.assertIn("/v1/accounts/{account_id}/external-orders:attach", spec["paths"])
        self.assertNotIn("/submit", " ".join(spec["paths"]))
        required = spec["components"]["schemas"]["ControlMutation"]["required"]
        self.assertTrue({"maker_id", "checker_id", "nonce", "reason_code"}.issubset(required))

    def test_facade_only_routes_status_and_attachment_with_current_control_version(self) -> None:
        service = ControlPlaneServiceV1(statuses=_Status(), external_attachments=_Attachments())
        status = service.get_account_status("account-1")
        self.assertEqual("DISARMED", status.lifecycle)
        request = type("Request", (), {"account_id": "account-1"})()
        self.assertEqual(
            (request, 4, 15), service.attach_external_order(request, evaluated_wall_ns=15)
        )
