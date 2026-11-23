"""Offline R1 lifecycle drill for day rollover, successor leases, and rollback.

This exercise uses an in-memory ephemeral Ed25519 signer and temporary state
files.  It never reads account configuration, imports the EMT binding, opens a
network connection, creates a session, or emits a broker mutation.  The output
is payload-free evidence of control-plane behaviour, not a production-release
or automatic-recovery claim.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Sequence

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseActivationErrorV1,
    NamespaceReleaseBootstrapApprovalV1,
    OrderClientIdNamespaceLeaseV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.domain import AccountLifecycle, TradingDayEpoch
from xuanling_tradecore.runtime import (
    AccountTradingDayStateV1,
    TradingDayOrchestratorV1,
    TradingDayRolloverEvidenceV1,
)


class R1LifecycleDrillError(RuntimeError):
    """The offline lifecycle drill did not observe a required fail-closed gate."""


_NOW = 1_000_000
_RUNTIME_HASH = sha256(b"r1-lifecycle-offline-runtime").hexdigest()
_SIGNER_ID = "offline-r1-lifecycle-signer"


class _RolloverFactPort:
    def __init__(self) -> None:
        self.evidence: list[TradingDayRolloverEvidenceV1] = []

    def append_rollover(self, evidence: TradingDayRolloverEvidenceV1) -> None:
        self.evidence.append(evidence)


def run(*, output: Path) -> dict[str, object]:
    """Run the complete SDK-free lifecycle drill and create a new evidence file."""

    if output.exists():
        raise R1LifecycleDrillError("R1 lifecycle evidence output already exists")
    sdk_previously_imported = "vnemttrader" in sys.modules
    signer = Ed25519PrivateKey.generate()
    public_key = signer.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    public_keys = {_SIGNER_ID: public_key}
    verifier = EmtOrderClientNamespaceVerifier(public_keys, revoked_signer_ids=frozenset())
    trust_hash = namespace_trust_anchor_hash_v1(public_keys, revoked_signer_ids=frozenset())
    day_one = TradingDayEpoch("20260915")
    day_two = TradingDayEpoch("20260916")
    with TemporaryDirectory(prefix="tradecore-r1-lifecycle-") as temporary:
        root = Path(temporary)
        scope_one = _scope(day_one)
        coordinator_one = _coordinator(root / "day-one-state.json", verifier)
        initial = _release(
            signer,
            scope=scope_one,
            epoch=1,
            own_start=1000,
            own_end=1002,
            trust_hash=trust_hash,
        )
        active_initial = _activate(coordinator_one, initial, scope_one, trust_hash, bootstrap=True)
        checkpoint = coordinator_one.checkpoint_high_watermark(active_initial, high_watermark=1001)
        _expect_rejection(
            "reuses the recorded own id prefix",
            lambda: _activate(
                coordinator_one,
                _release(
                    signer,
                    scope=scope_one,
                    epoch=2,
                    own_start=1001,
                    own_end=1004,
                    trust_hash=trust_hash,
                ),
                scope_one,
                trust_hash,
            ),
        )
        successor = _release(
            signer,
            scope=scope_one,
            epoch=2,
            own_start=1002,
            own_end=1004,
            trust_hash=trust_hash,
        )
        active_successor = _activate(coordinator_one, successor, scope_one, trust_hash)
        exhausted = coordinator_one.checkpoint_high_watermark(active_successor, high_watermark=1004)
        _expect_rejection(
            "outside lease",
            lambda: coordinator_one.checkpoint_high_watermark(exhausted, high_watermark=1005),
        )
        _expect_rejection(
            "epoch rollback",
            lambda: _activate(coordinator_one, initial, scope_one, trust_hash),
        )
        rollover_facts = _RolloverFactPort()
        rollover = TradingDayOrchestratorV1(rollover_facts)
        rollover.register(
            AccountTradingDayStateV1(
                account_id="offline-r1-lifecycle-account",
                trading_day=day_one,
                lifecycle=AccountLifecycle.DISARMED,
                adapter_session_active=False,
                unresolved_work_count=0,
            )
        )
        next_state = rollover.rollover(
            TradingDayRolloverEvidenceV1(
                account_id="offline-r1-lifecycle-account",
                previous_day=day_one,
                next_day=day_two,
                calendar_evidence_hash=sha256(b"offline-calendar").hexdigest(),
                adapter_evidence_hash=sha256(b"offline-adapter").hexdigest(),
                recovery_certificate_hash=sha256(b"offline-recovery").hexdigest(),
                observed_wall_ns=_NOW,
            )
        )
        scope_two = _scope(day_two)
        coordinator_two = _coordinator(root / "day-two-state.json", verifier)
        next_day_release = _release(
            signer,
            scope=scope_two,
            epoch=1,
            own_start=1000,
            own_end=1002,
            trust_hash=trust_hash,
        )
        next_day_active = _activate(
            coordinator_two,
            next_day_release,
            scope_two,
            trust_hash,
            bootstrap=True,
        )
        _expect_rejection(
            "another EMT scope",
            lambda: _activate(coordinator_two, initial, scope_two, trust_hash),
        )
        report = {
            "schema_version": "tradecore-r1-lifecycle-drill-v1",
            "driver": "r1-lifecycle-drill",
            "conclusion": "OFFLINE_CONTROL_PLANE_PASS_DISARMED_NOT_READY",
            "safety": {
                "emt_sdk_imported_before": sdk_previously_imported,
                "emt_sdk_imported_after": "vnemttrader" in sys.modules,
                "account_file_read": False,
                "network_used": False,
                "session_created": False,
                "broker_mutation": False,
                "private_key_persisted": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "checks": {
                "initial_release_activated_disarmed": active_initial.state.high_watermark == 999,
                "successor_prefix_reuse_rejected": True,
                "successor_release_activated": checkpoint.state.high_watermark == 1001,
                "range_exhaustion_rejected": True,
                "old_release_rollback_rejected": True,
                "trading_day_rollover_stays_disarmed": (
                    next_state.trading_day == day_two
                    and next_state.lifecycle is AccountLifecycle.DISARMED
                    and len(rollover_facts.evidence) == 1
                ),
                "next_day_scope_allows_fresh_range": next_day_active.state.high_watermark == 999,
                "prior_day_scope_rejected_on_next_day": True,
            },
            "evidence": {
                "day_one_successor_epoch": active_successor.state.release_epoch,
                "day_one_high_watermark": exhausted.state.high_watermark,
                "day_two_epoch": next_day_active.state.release_epoch,
                "public_trust_anchor_hash": trust_hash,
                "runtime_release_hash": _RUNTIME_HASH,
            },
        }
    if not all(report["checks"].values()):
        raise R1LifecycleDrillError("R1 lifecycle drill checks are incomplete")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def _scope(day: TradingDayEpoch) -> EmtOrderClientNamespaceScope:
    return EmtOrderClientNamespaceScope("offline-r1-lifecycle-account", day, 101)


def _coordinator(
    state_path: Path,
    verifier: EmtOrderClientNamespaceVerifier,
) -> NamespaceReleaseActivationCoordinatorV1:
    return NamespaceReleaseActivationCoordinatorV1(
        verifier=verifier,
        state_store=FileNamespaceReleaseStateStoreV1(state_path),
    )


def _release(
    signer: Ed25519PrivateKey,
    *,
    scope: EmtOrderClientNamespaceScope,
    epoch: int,
    own_start: int,
    own_end: int,
    trust_hash: str,
) -> SignedNamespaceReleaseV1:
    own = _signed_lease(
        signer,
        scope=scope,
        namespace_id="offline-own",
        owner="offline-own",
        start=own_start,
        end=own_end,
    )
    foreign = _signed_lease(
        signer,
        scope=scope,
        namespace_id="offline-foreign",
        owner="offline-foreign",
        start=2000,
        end=2999,
    )
    unsigned = SignedNamespaceReleaseV1(
        release_id=f"offline-r1-lifecycle-{scope.trading_day.value}-{epoch}",
        release_epoch=epoch,
        release_manifest_hash=_RUNTIME_HASH,
        trust_anchor_hash=trust_hash,
        valid_from_wall_ns=0,
        valid_until_wall_ns=_NOW + 1,
        own=own,
        foreign=(foreign,),
        signer_id=_SIGNER_ID,
        certificate_reference="offline://r1-lifecycle-public-certificate",
        revocation_reference="offline://r1-lifecycle-revocations",
        signature_b64="AA==",
    )
    return replace(
        unsigned,
        signature_b64=base64.b64encode(signer.sign(unsigned.canonical_payload())).decode("ascii"),
    )


def _signed_lease(
    signer: Ed25519PrivateKey,
    *,
    scope: EmtOrderClientNamespaceScope,
    namespace_id: str,
    owner: str,
    start: int,
    end: int,
) -> SignedOrderClientIdNamespaceLeaseV1:
    unsigned = SignedOrderClientIdNamespaceLeaseV1(
        lease=OrderClientIdNamespaceLeaseV1(
            scope=scope,
            namespace_id=namespace_id,
            owner_system_id=owner,
            adapter_instance_id=f"adapter-{owner}",
            range_start=start,
            range_end=end,
            manifest_version=1,
            valid_from_wall_ns=0,
            valid_until_wall_ns=_NOW + 1,
            release_manifest_hash=_RUNTIME_HASH,
            signer_id=_SIGNER_ID,
        ),
        certificate_reference="offline://r1-lifecycle-public-certificate",
        revocation_reference="offline://r1-lifecycle-revocations",
        signature_b64="AA==",
    )
    return replace(
        unsigned,
        signature_b64=base64.b64encode(signer.sign(unsigned.canonical_payload())).decode("ascii"),
    )


def _activate(
    coordinator: NamespaceReleaseActivationCoordinatorV1,
    release: SignedNamespaceReleaseV1,
    expected_scope: EmtOrderClientNamespaceScope,
    trust_hash: str,
    *,
    bootstrap: bool = False,
):
    return coordinator.activate(
        release,
        expected_scope=expected_scope,
        expected_release_manifest_hash=_RUNTIME_HASH,
        expected_trust_anchor_hash=trust_hash,
        now_wall_ns=_NOW,
        bootstrap_approval=(
            NamespaceReleaseBootstrapApprovalV1(
                approval_id="offline-r1-lifecycle-bootstrap",
                release_hash=release.content_hash,
                approver_id="offline-r1-lifecycle-operator",
                approved_at_wall_ns=_NOW,
            )
            if bootstrap
            else None
        ),
    )


def _expect_rejection(expected_fragment: str, action: Callable[[], object]) -> None:
    try:
        action()
    except NamespaceReleaseActivationErrorV1 as exc:
        if expected_fragment in str(exc):
            return
        raise R1LifecycleDrillError("R1 lifecycle rejected for an unexpected reason") from exc
    raise R1LifecycleDrillError("R1 lifecycle unsafe transition was accepted")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = run(output=args.output)
    print(json.dumps({"conclusion": report["conclusion"], "checks": report["checks"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
