"""Produce a redacted local acceptance report for a three-scope DISARMED profile.

The tool does not import an EMT SDK, open a session, read credentials, start a
server process, or write a profile.  Its report intentionally omits all
account identifiers, passwords, endpoints, key bytes and signatures.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Sequence

from xuanling_tradecore.adapters.emt import (
    EmtDisarmedProfile,
    EmtDisarmedProfileError,
    load_emt_disarmed_profile,
    load_emt_disarmed_signer_trust,
)

_EXPECTED_SCOPES = {
    "7001": "ordinary",
    "7002": "credit",
    "7003": "option",
}
_EXPECTED_PROFILE_ID = "emt-sim-three-scope-disarmed-v1"
_EXPECTED_SIGNER_ID = "emt-sim-disarmed-signer-v1"
_EXPECTED_ADAPTER_INSTANCE_ID = "emt-sim-linux-x86_64-v1"
_EXPECTED_OWNER_INSTANCE_ID = "fengyu"


def verify_three_scope_disarmed_profile(
    *,
    profile_path: Path,
    public_key_path: Path,
    revocations_path: Path,
    now_wall_ns: int,
) -> dict[str, object]:
    """Verify external trust and emit only a redacted DISARMED acceptance result."""

    profile = load_emt_disarmed_profile(profile_path)
    trust = load_emt_disarmed_signer_trust(
        public_key_path=public_key_path,
        revocations_path=revocations_path,
    )
    trust.verify_profile(profile, now_wall_ns=now_wall_ns)
    return _acceptance_report(profile, now_wall_ns=now_wall_ns)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify a local simulated three-scope EMT DISARMED profile without disclosure"
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--public-key", type=Path, required=True)
    parser.add_argument("--revocations", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise EmtDisarmedProfileError("EMT DISARMED acceptance refuses to replace output")
    report = verify_three_scope_disarmed_profile(
        profile_path=args.profile,
        public_key_path=args.public_key,
        revocations_path=args.revocations,
        now_wall_ns=time.time_ns(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)
        file.write("\n")
    print("EMT_DISARMED_PROFILE_ACCEPTED=3")
    return 0


def _acceptance_report(profile: EmtDisarmedProfile, *, now_wall_ns: int) -> dict[str, object]:
    if profile.profile_id != _EXPECTED_PROFILE_ID:
        raise EmtDisarmedProfileError("EMT DISARMED profile id does not match the test profile")
    if len(profile.signed_scope_ownerships) != len(_EXPECTED_SCOPES):
        raise EmtDisarmedProfileError("EMT DISARMED profile must contain exactly three scopes")
    claims: list[dict[str, object]] = []
    seen_scopes: set[str] = set()
    seen_accounts: set[str] = set()
    for signed in profile.signed_scope_ownerships:
        ownership = signed.ownership
        kind = _EXPECTED_SCOPES.get(ownership.adapter_client_scope)
        if kind is None or ownership.adapter_client_scope in seen_scopes:
            raise EmtDisarmedProfileError("EMT DISARMED profile has an unexpected client scope")
        if ownership.ownership_id != (f"emt-sim-{kind}-client-{ownership.adapter_client_scope}-v1"):
            raise EmtDisarmedProfileError("EMT DISARMED profile ownership id does not match scope")
        if ownership.venue_id != "EMT":
            raise EmtDisarmedProfileError("EMT DISARMED profile venue is invalid")
        if ownership.adapter_instance_id != _EXPECTED_ADAPTER_INSTANCE_ID:
            raise EmtDisarmedProfileError("EMT DISARMED profile adapter instance is invalid")
        if ownership.owner_instance_id != _EXPECTED_OWNER_INSTANCE_ID:
            raise EmtDisarmedProfileError("EMT DISARMED profile owner is invalid")
        if ownership.signer_id != _EXPECTED_SIGNER_ID:
            raise EmtDisarmedProfileError("EMT DISARMED profile signer is invalid")
        if not ownership.valid_from_wall_ns <= now_wall_ns < ownership.valid_until_wall_ns:
            raise EmtDisarmedProfileError("EMT DISARMED profile scope is not currently valid")
        if ownership.revoked_at_wall_ns is not None:
            raise EmtDisarmedProfileError("EMT DISARMED profile scope is revoked")
        if ownership.uniqueness_probe_succeeded:
            raise EmtDisarmedProfileError("EMT DISARMED profile must retain an unsatisfied probe")
        if ownership.config_source_hash == ownership.uniqueness_probe_hash:
            raise EmtDisarmedProfileError(
                "EMT DISARMED profile source and probe digests must differ"
            )
        if ownership.account_id in seen_accounts:
            raise EmtDisarmedProfileError(
                "EMT DISARMED profile must use three distinct account identifiers"
            )
        seen_scopes.add(ownership.adapter_client_scope)
        seen_accounts.add(ownership.account_id)
        claims.append(
            {
                "ownership_id": ownership.ownership_id,
                "client_scope": ownership.adapter_client_scope,
                "signer_id": ownership.signer_id,
                "valid_from_wall_ns": ownership.valid_from_wall_ns,
                "valid_until_wall_ns": ownership.valid_until_wall_ns,
                "account_identifier_disclosed": False,
            }
        )
    if seen_scopes != set(_EXPECTED_SCOPES):
        raise EmtDisarmedProfileError("EMT DISARMED profile does not cover every expected scope")
    return {
        "schema_version": 1,
        "implementation_id": "M2-E-02M",
        "result": "EMT_THREE_SCOPE_DISARMED_PROFILE_ACCEPTED",
        "profile_id": profile.profile_id,
        "generated_wall_ns": now_wall_ns,
        "approval": {"approved": False, "lifecycle": "DISARMED"},
        "scope_count": len(claims),
        "account_identifiers_are_distinct": True,
        "activation_permitted": False,
        "sdk_operations": "NOT_PERFORMED",
        "scope_claims": sorted(claims, key=lambda claim: str(claim["client_scope"])),
    }


if __name__ == "__main__":
    raise SystemExit(main())
