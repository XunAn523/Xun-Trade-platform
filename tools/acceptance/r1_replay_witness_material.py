"""Offline signer for one payload-free EMT RESTART replay witness proposal.

The online F2 worker can create only a hash-only proposal.  This utility runs
outside the EMT runtime, verifies the reviewed namespace release and proposal
binding, then signs with an ignored offline key.  It never reads accounts,
loads an SDK, opens a broker session, or performs a broker mutation.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Sequence

from cryptography.hazmat.primitives import serialization
from tools.acceptance.r1_release_preflight import load_namespace_trust_v1
from tools.performance.build_emt_disarmed_profile import load_private_keys
from xuanling_tradecore.adapters.emt.namespace_recovery import (
    EmtNamespaceRecoveryError,
    OrderClientIdNamespaceDirectoryV1,
)
from xuanling_tradecore.adapters.emt.namespace_release_v1 import (
    NamespaceReleaseActivationErrorV1,
    load_signed_namespace_release_v1_json,
)
from xuanling_tradecore.adapters.emt.replay_witness_v1 import (
    EmtReplayWitnessErrorV1,
    load_emt_replay_witness_proposal_v1,
    sign_emt_replay_witness_v1,
    write_signed_emt_replay_witness_v1,
)


class R1ReplayWitnessMaterialError(ValueError):
    """A proposal cannot be safely bound to the reviewed R1 release."""


def sign_replay_witness(
    *,
    proposal_path: Path,
    namespace_release_path: Path,
    namespace_trust_path: Path,
    private_keys_path: Path,
    output: Path,
    now_wall_ns: int | None = None,
):
    """Sign one active proposal after strict release/namespace verification."""

    if output.exists():
        raise R1ReplayWitnessMaterialError("signed replay witness output already exists")
    now = time.time_ns() if now_wall_ns is None else now_wall_ns
    try:
        proposal = load_emt_replay_witness_proposal_v1(proposal_path)
        trust = load_namespace_trust_v1(namespace_trust_path)
        release = load_signed_namespace_release_v1_json(
            Path(namespace_release_path).read_text(encoding="utf-8")
        )
        if not proposal.active_at(now) or not release.active_at(now):
            raise R1ReplayWitnessMaterialError("replay proposal or namespace release is expired")
        verifier = trust.verifier()
        verifier.verify_detached(
            signer_id=release.signer_id,
            signature_b64=release.signature_b64,
            payload=release.canonical_payload(),
        )
        directory = OrderClientIdNamespaceDirectoryV1(
            own=release.own,
            foreign=release.foreign,
            verifier=verifier,
            wall_ns=now,
        )
        if (
            proposal.namespace_release_hash != release.content_hash
            or proposal.namespace_manifest_hash != directory.manifest_hash
            or proposal.trading_day != release.scope.trading_day
            or proposal.adapter_client_scope != str(release.scope.emt_client_id)
            or proposal.adapter_instance_id != release.own.lease.adapter_instance_id
        ):
            raise R1ReplayWitnessMaterialError(
                "replay proposal does not bind this namespace release and scope"
            )
        private_key = load_private_keys(private_keys_path)[release.signer_id]
        expected_public_key = trust.public_keys.get(release.signer_id)
        actual_public_key = private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        if expected_public_key != actual_public_key:
            raise R1ReplayWitnessMaterialError(
                "replay witness signer does not match namespace trust"
            )
        signed = sign_emt_replay_witness_v1(
            proposal,
            signer_id=release.signer_id,
            certificate_reference=release.certificate_reference,
            revocation_reference=release.revocation_reference,
            private_key=private_key,
        )
        write_signed_emt_replay_witness_v1(output, signed)
        return signed
    except (
        KeyError,
        OSError,
        EmtReplayWitnessErrorV1,
        EmtNamespaceRecoveryError,
        NamespaceReleaseActivationErrorV1,
        ValueError,
    ) as exc:
        if isinstance(exc, R1ReplayWitnessMaterialError):
            raise
        raise R1ReplayWitnessMaterialError("replay witness material verification failed") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposal", type=Path, required=True)
    parser.add_argument("--namespace-release", type=Path, required=True)
    parser.add_argument("--namespace-trust", type=Path, required=True)
    parser.add_argument("--private-keys", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    signed = sign_replay_witness(
        proposal_path=args.proposal,
        namespace_release_path=args.namespace_release,
        namespace_trust_path=args.namespace_trust,
        private_keys_path=args.private_keys,
        output=args.output,
    )
    print(
        json.dumps(
            {
                "signed_witness_sha256": signed.content_sha256,
                "proposal_sha256": signed.witness.content_sha256,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
