from __future__ import annotations

import json
import tempfile
import unittest
from base64 import b64decode
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters.emt.replay_witness_v1 import (
    EmtReplayWitnessErrorV1,
    issue_emt_replay_witness_v1,
    load_emt_replay_witness_proposal_v1,
    load_signed_emt_replay_witness_v1,
    sign_emt_replay_witness_v1,
    write_emt_replay_witness_proposal_v1,
    write_signed_emt_replay_witness_v1,
)
from xuanling_tradecore.domain import TradingDayEpoch


class EmtReplayWitnessTests(unittest.TestCase):
    def _witness(self):
        return issue_emt_replay_witness_v1(
            witness_id="r1-replay-witness-1",
            account_config_profile="ordinary",
            account_id="account-private",
            adapter_instance_id="emt-r1-ordinary",
            adapter_client_scope="101",
            trading_day=TradingDayEpoch("2026-09-11"),
            namespace_release_hash="a" * 64,
            namespace_manifest_hash="b" * 64,
            order_client_id=1_000_001,
            order_emt_id=5_000_001,
            issued_at_wall_ns=100,
            expires_at_wall_ns=200,
        )

    def test_witness_persists_only_hash_and_matches_exact_identity(self) -> None:
        witness = self._witness()
        rendered = json.dumps(witness.as_public_document())
        self.assertNotIn("account-private", rendered)
        self.assertNotIn("1000001", rendered)
        self.assertNotIn("5000001", rendered)
        self.assertTrue(
            witness.matches_callback(
                account_id="account-private",
                order_client_id=1_000_001,
                order_emt_id=5_000_001,
                observed_at_wall_ns=150,
            )
        )
        self.assertFalse(
            witness.matches_callback(
                account_id="account-private",
                order_client_id=1_000_002,
                order_emt_id=5_000_001,
                observed_at_wall_ns=150,
            )
        )
        self.assertFalse(
            witness.matches_callback(
                account_id="account-private",
                order_client_id=1_000_001,
                order_emt_id=5_000_001,
                observed_at_wall_ns=200,
            )
        )

    def test_proposal_and_signed_witness_are_hash_checked_and_never_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            proposal_path = Path(temporary) / "proposal.json"
            signed_path = Path(temporary) / "signed-witness.json"
            witness = self._witness()
            write_emt_replay_witness_proposal_v1(proposal_path, witness)
            self.assertEqual(witness, load_emt_replay_witness_proposal_v1(proposal_path))
            with self.assertRaises(FileExistsError):
                write_emt_replay_witness_proposal_v1(proposal_path, witness)
            document = json.loads(proposal_path.read_text(encoding="ascii"))
            document["identity_digest"] = "c" * 64
            proposal_path.write_text(json.dumps(document), encoding="ascii")
            with self.assertRaisesRegex(EmtReplayWitnessErrorV1, "content hash"):
                load_emt_replay_witness_proposal_v1(proposal_path)

            private_key = Ed25519PrivateKey.generate()
            signed = sign_emt_replay_witness_v1(
                witness,
                signer_id="r1-signer",
                certificate_reference="local://certificate",
                revocation_reference="local://revocations",
                private_key=private_key,
            )
            private_key.public_key().verify(
                b64decode(signed.signature_b64), signed.canonical_payload()
            )
            write_signed_emt_replay_witness_v1(signed_path, signed)
            self.assertEqual(signed, load_signed_emt_replay_witness_v1(signed_path))
            signed_document = json.loads(signed_path.read_text(encoding="ascii"))
            signed_document["signer_id"] = "another-signer"
            signed_path.write_text(json.dumps(signed_document), encoding="ascii")
            with self.assertRaisesRegex(EmtReplayWitnessErrorV1, "content hash"):
                load_signed_emt_replay_witness_v1(signed_path)


if __name__ == "__main__":
    unittest.main()
