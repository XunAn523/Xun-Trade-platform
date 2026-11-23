"""Verify non-secret R1 namespace-release material before a Linux drill.

This is deliberately an offline preparation tool.  It does not import the EMT
SDK, read an account file, create a session, write release state, or perform a
broker mutation.  Its only output is a payload-free readiness report.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Sequence, cast

from xuanling_tradecore.adapters.emt import (
    EmtNamespaceRecoveryError,
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    NamespaceReleaseActivationErrorV1,
    SignedNamespaceReleaseV1,
    load_signed_namespace_release_v1_json,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.domain import TradingDayEpoch


class R1ReleasePreflightError(ValueError):
    """A public R1 release package is incomplete, malformed, or unsafe."""


@dataclass(frozen=True)
class NamespaceTrustV1:
    """The exact public verification material for an R1 namespace release."""

    public_keys: Mapping[str, bytes]
    revoked_signer_ids: frozenset[str]

    @property
    def trust_anchor_hash(self) -> str:
        return namespace_trust_anchor_hash_v1(
            self.public_keys, revoked_signer_ids=self.revoked_signer_ids
        )

    def verifier(self) -> EmtOrderClientNamespaceVerifier:
        return EmtOrderClientNamespaceVerifier(
            self.public_keys, revoked_signer_ids=self.revoked_signer_ids
        )


@dataclass(frozen=True)
class Settings:
    """All inputs are public control-plane values; no credential path is accepted."""

    namespace_release: Path
    namespace_trust: Path
    expected_runtime_release_hash: str
    expected_account_id: str
    expected_trading_day: str
    expected_emt_client_id: int
    output: Path

    def validate(self) -> None:
        if not self.namespace_release.is_file() or not self.namespace_trust.is_file():
            raise R1ReleasePreflightError("R1 namespace release or trust file is unavailable")
        _require_sha256(self.expected_runtime_release_hash, "expected runtime release hash")
        if not self.expected_account_id:
            raise R1ReleasePreflightError("expected logical account id is empty")
        try:
            TradingDayEpoch(self.expected_trading_day)
        except ValueError as exc:
            raise R1ReleasePreflightError("expected trading day is invalid") from exc
        if not 0 <= self.expected_emt_client_id <= 255:
            raise R1ReleasePreflightError("expected EMT client id is outside uint8")
        if self.output.exists():
            raise R1ReleasePreflightError("R1 preflight refuses to replace evidence output")


def load_namespace_trust_v1(path: Path) -> NamespaceTrustV1:
    """Load a strict public-key-only trust record without accepting secrets."""

    document = _read_object(path, "R1 namespace trust")
    if set(document) != {"schema_version", "trusted_public_keys", "revoked_signer_ids"}:
        raise R1ReleasePreflightError("R1 namespace trust fields differ")
    if document["schema_version"] != 1:
        raise R1ReleasePreflightError("R1 namespace trust schema differs")
    rows = document["trusted_public_keys"]
    if not isinstance(rows, list) or not rows:
        raise R1ReleasePreflightError("R1 namespace trust lacks public keys")
    keys: dict[str, bytes] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"signer_id", "public_key_b64"}:
            raise R1ReleasePreflightError("R1 namespace trust key fields differ")
        signer_id = row["signer_id"]
        encoded_key = row["public_key_b64"]
        if not isinstance(signer_id, str) or not signer_id:
            raise R1ReleasePreflightError("R1 namespace trust signer id is invalid")
        if signer_id in keys:
            raise R1ReleasePreflightError("R1 namespace trust repeats signer id")
        if not isinstance(encoded_key, str):
            raise R1ReleasePreflightError("R1 namespace trust public key is invalid")
        try:
            raw_key = base64.b64decode(encoded_key, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise R1ReleasePreflightError("R1 namespace trust public key is not base64") from exc
        if len(raw_key) != 32:
            raise R1ReleasePreflightError("R1 namespace trust public key is not Ed25519")
        keys[signer_id] = raw_key
    revoked = document["revoked_signer_ids"]
    if not isinstance(revoked, list) or any(
        not isinstance(item, str) or not item for item in revoked
    ):
        raise R1ReleasePreflightError("R1 namespace revoked signer ids are invalid")
    if len(set(revoked)) != len(revoked):
        raise R1ReleasePreflightError("R1 namespace revoked signer ids repeat")
    return NamespaceTrustV1(public_keys=keys, revoked_signer_ids=frozenset(revoked))


def run(settings: Settings, *, now_wall_ns: int | None = None) -> dict[str, object]:
    """Validate one R1 package and emit only hashes, counts, and readiness facts."""

    settings.validate()
    now = time.time_ns() if now_wall_ns is None else now_wall_ns
    if now < 0:
        raise R1ReleasePreflightError("R1 preflight time is invalid")
    release = _load_release(settings.namespace_release)
    trust = load_namespace_trust_v1(settings.namespace_trust)
    expected_scope = EmtOrderClientNamespaceScope(
        settings.expected_account_id,
        TradingDayEpoch(settings.expected_trading_day),
        settings.expected_emt_client_id,
    )
    if release.scope != expected_scope:
        raise R1ReleasePreflightError("R1 namespace release scope differs from the drill scope")
    if release.release_manifest_hash != settings.expected_runtime_release_hash:
        raise R1ReleasePreflightError("R1 namespace release binds another runtime revision")
    if release.trust_anchor_hash != trust.trust_anchor_hash:
        raise R1ReleasePreflightError("R1 namespace release trust anchor differs")
    if not release.active_at(now):
        raise R1ReleasePreflightError("R1 namespace release is outside its validity interval")
    try:
        verifier = trust.verifier()
        verifier.verify_detached(
            signer_id=release.signer_id,
            signature_b64=release.signature_b64,
            payload=release.canonical_payload(),
        )
        directory = _validated_directory(release, verifier=verifier, now_wall_ns=now)
    except (EmtNamespaceRecoveryError, NamespaceReleaseActivationErrorV1) as exc:
        raise R1ReleasePreflightError("R1 namespace release signature verification failed") from exc
    required_tools = _required_tools()
    unavailable = tuple(name for name, path in required_tools.items() if not path.is_file())
    if unavailable:
        raise R1ReleasePreflightError("R1 acceptance tool is unavailable")
    report = {
        "schema_version": "tradecore-r1-release-preflight-v1",
        "driver": "r1-release-preflight",
        "preflight_status": "prepared",
        "safety": {
            "emt_sdk_imported": False,
            "account_file_read": False,
            "network_used": False,
            "release_state_written": False,
            "broker_mutation": False,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "release": {
            "release_hash": release.content_hash,
            "release_epoch": release.release_epoch,
            "runtime_release_hash": release.release_manifest_hash,
            "trust_anchor_hash": release.trust_anchor_hash,
            "namespace_manifest_hash": directory.manifest_hash,
            "scope_hash": _scope_hash(release.scope),
            "own_range_size": release.own.lease.range_end - release.own.lease.range_start + 1,
            "foreign_range_count": len(release.foreign),
        },
        "checks": {
            "release_document_loaded": True,
            "expected_scope_matches": True,
            "runtime_revision_matches": True,
            "trust_anchor_matches": True,
            "release_and_lease_signatures_valid": True,
            "release_active_at_preflight": True,
            "linux_r1_toolchain_present": True,
        },
        "prepared_stages": ("R1-A1", "R1-A2", "R1-A3", "R1-A4", "R1-A6"),
        "external_evidence_required_before_completion": (
            "R1-P2 position-query terminal semantics",
            "R1-P2 public RESTART callback replay",
            "R1-A5 complete query/callback-gap evidence",
        ),
    }
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def _load_release(path: Path) -> SignedNamespaceReleaseV1:
    try:
        return load_signed_namespace_release_v1_json(path.read_text(encoding="utf-8"))
    except (OSError, NamespaceReleaseActivationErrorV1) as exc:
        raise R1ReleasePreflightError("R1 namespace release is unreadable") from exc


def _validated_directory(
    release: SignedNamespaceReleaseV1,
    *,
    verifier: EmtOrderClientNamespaceVerifier,
    now_wall_ns: int,
) -> Any:
    """Reuse the production directory verifier without activating release state."""

    from xuanling_tradecore.adapters.emt.namespace_recovery import OrderClientIdNamespaceDirectoryV1

    return OrderClientIdNamespaceDirectoryV1(
        own=release.own,
        foreign=release.foreign,
        verifier=verifier,
        wall_ns=now_wall_ns,
    )


def _required_tools() -> Mapping[str, Path]:
    root = Path(__file__).resolve().parent
    return {
        "f1_mmap_physical_kill_drill": root / "f1_mmap_physical_kill_drill.py",
        "f2_controlled_execution_cycle": root / "f2_controlled_execution_cycle.py",
        "r1_dual_system_cycle": root / "r1_dual_system_cycle.py",
        "r1_r2_crash_boundary": root / "r1_r2_crash_boundary.py",
        "r1_namespace_release_material": root / "r1_namespace_release_material.py",
        "r1_replay_witness_material": root / "r1_replay_witness_material.py",
        "m3_emt_public_replay_probe": root.parent / "performance" / "m3_emt_public_replay_probe.py",
        "m3_emt_correlated_replay_probe": root / "m3_emt_correlated_replay_probe.py",
    }


def _read_object(path: Path, name: str) -> Mapping[str, object]:
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise R1ReleasePreflightError(f"{name} is unreadable") from exc
    if not isinstance(value, dict):
        raise R1ReleasePreflightError(f"{name} is not an object")
    return cast(Mapping[str, object], value)


def _require_sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise R1ReleasePreflightError(f"{name} is invalid")


def _scope_hash(scope: EmtOrderClientNamespaceScope) -> str:
    encoded = "\x00".join(
        (scope.account_id, scope.trading_day.value, str(scope.emt_client_id))
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    """Run an offline R1 release preflight and write payload-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace-release", type=Path, required=True)
    parser.add_argument("--namespace-trust", type=Path, required=True)
    parser.add_argument("--expected-runtime-release-hash", required=True)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--expected-trading-day", required=True)
    parser.add_argument("--expected-emt-client-id", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = run(
        Settings(
            namespace_release=args.namespace_release,
            namespace_trust=args.namespace_trust,
            expected_runtime_release_hash=args.expected_runtime_release_hash,
            expected_account_id=args.expected_account_id,
            expected_trading_day=args.expected_trading_day,
            expected_emt_client_id=args.expected_emt_client_id,
            output=args.output,
        )
    )
    print(json.dumps({"preflight_status": report["preflight_status"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
