from __future__ import annotations

import base64
import json
import tempfile
import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.acceptance.r1_release_preflight import (
    R1ReleasePreflightError,
    Settings,
    load_namespace_trust_v1,
    run,
)
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    OrderClientIdNamespaceLeaseV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.domain import TradingDayEpoch

_NOW = 2_000_000
_RUNTIME_HASH = sha256(b"r1-preflight-runtime").hexdigest()


def _scope() -> EmtOrderClientNamespaceScope:
    return EmtOrderClientNamespaceScope("logical-emt-account", TradingDayEpoch("2026-09-11"), 17)


def _signed_lease(
    private_key: Ed25519PrivateKey, *, namespace_id: str, owner: str, start: int, end: int
) -> SignedOrderClientIdNamespaceLeaseV1:
    unsigned = SignedOrderClientIdNamespaceLeaseV1(
        lease=OrderClientIdNamespaceLeaseV1(
            scope=_scope(),
            namespace_id=namespace_id,
            owner_system_id=owner,
            adapter_instance_id=f"adapter-{owner}",
            range_start=start,
            range_end=end,
            manifest_version=1,
            valid_from_wall_ns=_NOW - 1,
            valid_until_wall_ns=_NOW + 1,
            release_manifest_hash=_RUNTIME_HASH,
            signer_id="r1-preflight-signer",
        ),
        certificate_reference="test-certificate",
        revocation_reference="test-revocations",
        signature_b64="AA==",
    )
    return replace(
        unsigned,
        signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
            "ascii"
        ),
    )


def _release(private_key: Ed25519PrivateKey) -> tuple[SignedNamespaceReleaseV1, bytes]:
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    unsigned = SignedNamespaceReleaseV1(
        release_id="r1-preflight-release",
        release_epoch=7,
        release_manifest_hash=_RUNTIME_HASH,
        trust_anchor_hash=namespace_trust_anchor_hash_v1(
            {"r1-preflight-signer": public_key}, revoked_signer_ids=frozenset()
        ),
        valid_from_wall_ns=_NOW - 1,
        valid_until_wall_ns=_NOW + 1,
        own=_signed_lease(
            private_key, namespace_id="tradecore-a", owner="tradecore-a", start=1000, end=1999
        ),
        foreign=(
            _signed_lease(
                private_key, namespace_id="tradecore-b", owner="tradecore-b", start=2000, end=2999
            ),
        ),
        signer_id="r1-preflight-signer",
        certificate_reference="test-certificate",
        revocation_reference="test-revocations",
        signature_b64="AA==",
    )
    return (
        replace(
            unsigned,
            signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
                "ascii"
            ),
        ),
        public_key,
    )


class R1ReleasePreflightTests(unittest.TestCase):
    def _write_inputs(self, root: Path) -> tuple[Path, Path]:
        release, public_key = _release(Ed25519PrivateKey.generate())
        release_path = root / "namespace-release.json"
        trust_path = root / "namespace-trust.json"
        release_path.write_text(json.dumps(release.as_dict()), encoding="utf-8")
        trust_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "trusted_public_keys": [
                        {
                            "signer_id": "r1-preflight-signer",
                            "public_key_b64": base64.b64encode(public_key).decode("ascii"),
                        }
                    ],
                    "revoked_signer_ids": [],
                }
            ),
            encoding="utf-8",
        )
        return release_path, trust_path

    def _settings(self, root: Path, *, runtime_hash: str = _RUNTIME_HASH) -> Settings:
        release_path, trust_path = self._write_inputs(root)
        return Settings(
            namespace_release=release_path,
            namespace_trust=trust_path,
            expected_runtime_release_hash=runtime_hash,
            expected_account_id="logical-emt-account",
            expected_trading_day="2026-09-11",
            expected_emt_client_id=17,
            output=root / "report.json",
        )

    def test_preflight_verifies_public_release_without_sdk_or_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = run(self._settings(root), now_wall_ns=_NOW)
            self.assertEqual("prepared", report["preflight_status"])
            self.assertEqual(False, report["safety"]["emt_sdk_imported"])  # type: ignore[index]
            self.assertEqual(False, report["safety"]["account_file_read"])  # type: ignore[index]
            rendered = (root / "report.json").read_text(encoding="utf-8")
            self.assertNotIn("logical-emt-account", rendered)
            self.assertIn("scope_hash", rendered)

    def test_preflight_rejects_wrong_runtime_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(R1ReleasePreflightError, "runtime revision"):
                run(
                    self._settings(root, runtime_hash=sha256(b"other").hexdigest()),
                    now_wall_ns=_NOW,
                )

    def test_trust_document_rejects_private_or_extra_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, trust_path = self._write_inputs(root)
            document = json.loads(trust_path.read_text(encoding="utf-8"))
            document["private_key_b64"] = "must-not-be-accepted"
            trust_path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(R1ReleasePreflightError, "fields differ"):
                load_namespace_trust_v1(trust_path)


if __name__ == "__main__":
    unittest.main()
