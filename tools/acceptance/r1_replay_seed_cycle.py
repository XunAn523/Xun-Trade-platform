"""Create one signed-namespace, payload-free EMT replay-witness proposal.

This is a deliberately narrow bridge for the credit and option scopes.  It
uses the existing bounded simulated order/cancel driver, but takes ownership
of the EMT ``order_client_id`` from a signed R1 namespace before the SDK is
loaded.  A proposal is written only after the one cycle has obtained a native
order id, received its correlated terminal callback, cancelled that exact
order and reconciled it by EMT order id.  The proposal contains a digest of
the two native identifiers, never either identifier or business payload.

It is not a READY transition, a recovery certificate, or a generic public
callback proof.  An offline signer and the RESTART-only correlated observer
remain separate required stages.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import secrets
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from typing import Any, Sequence

from emt_gateway.session import normalize_emt_trading_day
from tools.acceptance.r1_release_preflight import R1ReleasePreflightError, load_namespace_trust_v1
from tools.performance.perfbench.gateway_mutation import (
    GatewayMutationError,
    GatewayMutationSettings,
    run_gateway_mutation,
)
from tools.performance.perfbench.simulated_accounts import (
    load_simulated_accounts,
)
from tools.performance.perfbench.tradecore_mmap_emt_support import load_tight_burst_order_plan
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseActivationErrorV1,
    NamespaceReleaseBootstrapApprovalV1,
    load_signed_namespace_release_v1_json,
)
from xuanling_tradecore.adapters.emt.replay_witness_v1 import (
    EmtReplayWitnessErrorV1,
    issue_emt_replay_witness_v1,
    write_emt_replay_witness_proposal_v1,
)
from xuanling_tradecore.domain import TradingDayEpoch

_LOGICAL_ACCOUNT_ID = "emt_01"


class R1ReplaySeedCycleError(RuntimeError):
    """The bounded source-order cycle cannot safely issue a replay witness."""


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    accounts_file: Path
    fixture: Path | None
    tight_burst_order_plan: Path | None
    profile: str
    session_client_id: int
    trading_day: str
    namespace_release: Path
    namespace_trust: Path
    namespace_state: Path
    runtime_release_hash: str
    output: Path
    proposal_output: Path
    runtime_dir: Path
    callback_timeout_seconds: float
    execute_simulated: bool
    bootstrap_approval_id: str | None = None
    bootstrap_approver_id: str | None = None
    isolate_after_report: bool = False

    def validate(self) -> None:
        if platform.system() != "Linux":
            raise R1ReplaySeedCycleError("R1 replay seed cycle requires Linux")
        if not self.execute_simulated:
            raise R1ReplaySeedCycleError("--execute-simulated is required")
        if self.profile not in {"ordinary", "credit", "option"}:
            raise R1ReplaySeedCycleError("R1 replay seed profile is unsupported")
        if not 1 <= self.session_client_id <= 255:
            raise R1ReplaySeedCycleError("R1 replay seed EMT session client id is invalid")
        if not 5.0 <= self.callback_timeout_seconds <= 60.0:
            raise R1ReplaySeedCycleError("R1 replay seed callback timeout must be in 5..60 seconds")
        if len(self.runtime_release_hash) != 64 or any(
            value not in "0123456789abcdef" for value in self.runtime_release_hash
        ):
            raise R1ReplaySeedCycleError("R1 replay seed runtime release hash is invalid")
        try:
            normalize_emt_trading_day(self.trading_day)
        except Exception as exc:
            raise R1ReplaySeedCycleError("R1 replay seed trading day is invalid") from exc
        for path, label in (
            (self.sdk_root, "SDK root"),
            (self.accounts_file, "account file"),
            (self.namespace_release, "namespace release"),
            (self.namespace_trust, "namespace trust"),
        ):
            if not path.exists():
                raise R1ReplaySeedCycleError(f"R1 replay seed {label} is unavailable")
        if (self.fixture is None) == (self.tight_burst_order_plan is None):
            raise R1ReplaySeedCycleError(
                "R1 replay seed requires exactly one mutation fixture or tight order plan"
            )
        if self.fixture is not None and not self.fixture.is_file():
            raise R1ReplaySeedCycleError("R1 replay seed mutation fixture is unavailable")
        if self.tight_burst_order_plan is not None:
            if self.profile == "option":
                raise R1ReplaySeedCycleError(
                    "R1 option replay seed requires a reviewed option mutation fixture"
                )
            if not self.tight_burst_order_plan.is_file():
                raise R1ReplaySeedCycleError("R1 replay seed tight order plan is unavailable")
        if self.output.exists() or self.proposal_output.exists():
            raise R1ReplaySeedCycleError("R1 replay seed output must be a new path")
        if self.namespace_state.exists() and not self.namespace_state.is_file():
            raise R1ReplaySeedCycleError("R1 replay seed namespace state is invalid")
        if self.namespace_state.exists() and (
            self.bootstrap_approval_id is not None or self.bootstrap_approver_id is not None
        ):
            raise R1ReplaySeedCycleError(
                "bootstrap approval is only valid for an uninitialized namespace state"
            )
        if not self.namespace_state.exists() and (
            not self.bootstrap_approval_id or not self.bootstrap_approver_id
        ):
            raise R1ReplaySeedCycleError(
                "initial R1 replay seed namespace activation requires bootstrap approval"
            )


def _activate_namespace(settings: Settings, *, trading_day: TradingDayEpoch) -> tuple[Any, Any]:
    """Activate and reserve exactly one client id before importing EMT."""

    try:
        release = load_signed_namespace_release_v1_json(
            settings.namespace_release.read_text(encoding="utf-8")
        )
        trust = load_namespace_trust_v1(settings.namespace_trust)
        state_store = FileNamespaceReleaseStateStoreV1(settings.namespace_state)
        now = time_ns()
        approval = (
            NamespaceReleaseBootstrapApprovalV1(
                approval_id=str(settings.bootstrap_approval_id),
                release_hash=release.content_hash,
                approver_id=str(settings.bootstrap_approver_id),
                approved_at_wall_ns=now,
            )
            if state_store.load() is None
            else None
        )
        coordinator = NamespaceReleaseActivationCoordinatorV1(
            verifier=trust.verifier(), state_store=state_store
        )
        activated = coordinator.activate(
            release,
            expected_scope=EmtOrderClientNamespaceScope(
                _LOGICAL_ACCOUNT_ID, trading_day, settings.session_client_id
            ),
            expected_release_manifest_hash=settings.runtime_release_hash,
            expected_trust_anchor_hash=trust.trust_anchor_hash,
            now_wall_ns=now,
            bootstrap_approval=approval,
        )
        candidate = activated.state.high_watermark + 1
        if candidate > activated.directory.own.range_end:
            raise R1ReplaySeedCycleError("R1 replay seed namespace lease is exhausted")
        return coordinator.checkpoint_high_watermark(activated, high_watermark=candidate), release
    except (
        OSError,
        R1ReleasePreflightError,
        NamespaceReleaseActivationErrorV1,
        ValueError,
    ) as exc:
        raise R1ReplaySeedCycleError("R1 replay seed namespace activation failed") from exc


def run(settings: Settings) -> dict[str, object]:
    """Run one bounded source cycle and emit a digest-only witness proposal."""

    settings.validate()
    trading_day = TradingDayEpoch(normalize_emt_trading_day(settings.trading_day))
    activated, _release = _activate_namespace(settings, trading_day=trading_day)
    account = getattr(load_simulated_accounts(settings.accounts_file), settings.profile)
    emitted: list[str] = []
    order_request_override = (
        load_tight_burst_order_plan(settings.tight_burst_order_plan, burst_width=1).request
        if settings.tight_burst_order_plan is not None
        else None
    )
    source_fixture_path = settings.fixture or settings.tight_burst_order_plan
    assert source_fixture_path is not None  # Proven by Settings.validate().

    def issue_after_verified_cycle(
        observed_trading_day: str, order_client_id: int, order_emt_id: int
    ) -> None:
        if normalize_emt_trading_day(observed_trading_day) != trading_day.value:
            raise R1ReplaySeedCycleError("EMT replay seed session trading day differs")
        if order_client_id != activated.state.high_watermark:
            raise R1ReplaySeedCycleError("EMT replay seed client id differs from signed allocation")
        now = time_ns()
        witness = issue_emt_replay_witness_v1(
            witness_id=f"r1-replay-seed-{secrets.token_hex(16)}",
            account_config_profile=settings.profile,
            account_id=account.account,
            adapter_instance_id=activated.directory.own.adapter_instance_id,
            adapter_client_scope=str(settings.session_client_id),
            trading_day=trading_day,
            namespace_release_hash=activated.state.release_hash,
            namespace_manifest_hash=activated.directory.manifest_hash,
            order_client_id=order_client_id,
            order_emt_id=order_emt_id,
            issued_at_wall_ns=now,
            expires_at_wall_ns=now + 30 * 60 * 1_000_000_000,
        )
        write_emt_replay_witness_proposal_v1(settings.proposal_output, witness)
        emitted.append(witness.content_sha256)

    try:
        mutation = run_gateway_mutation(
            GatewayMutationSettings(
                sdk_root=settings.sdk_root,
                runtime_dir=settings.runtime_dir,
                accounts=load_simulated_accounts(settings.accounts_file),
                # The mutation driver does not read this placeholder when the
                # reviewed tight plan supplies the in-memory request.  Its
                # path remains required by the legacy fixture-only interface.
                fixture=source_fixture_path,
                profile=settings.profile,
                cycles=1,
                callback_timeout_seconds=settings.callback_timeout_seconds,
                interval_seconds=1.0,
                execute_simulated=True,
                session_client_id=settings.session_client_id,
                fixed_native_order_client_id=activated.state.high_watermark,
                order_request_override=order_request_override,
                on_cycle_completed=issue_after_verified_cycle,
            )
        )
    except (GatewayMutationError, EmtReplayWitnessErrorV1, ValueError) as exc:
        raise R1ReplaySeedCycleError("R1 replay seed native cycle failed") from exc
    if len(emitted) != 1:
        raise R1ReplaySeedCycleError("R1 replay seed did not issue exactly one witness proposal")
    report = {
        "schema_version": "tradecore-r1-replay-seed-cycle-v1",
        "acceptance_status": "passed",
        "environment": "simulated",
        "account_config_profile": settings.profile,
        "adapter_instance_id": activated.directory.own.adapter_instance_id,
        "adapter_client_scope": str(settings.session_client_id),
        "namespace_release_hash": activated.state.release_hash,
        "namespace_manifest_hash": activated.directory.manifest_hash,
        "replay_witness_proposal_sha256": emitted[0],
        "controlled_cycle_completed": mutation.get("run", {}).get("completed_cycles") == 1,
        "safety": {
            "one_order": True,
            "one_exact_cancel": True,
            "automatic_retry": False,
            "payloads_retained": False,
            "credentials_retained": False,
            "ready_promotion": False,
        },
    }
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    settings.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", type=Path)
    source.add_argument("--tight-burst-order-plan", type=Path)
    parser.add_argument("--profile", choices=("ordinary", "credit", "option"), required=True)
    parser.add_argument("--session-client-id", type=int, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--namespace-release", type=Path, required=True)
    parser.add_argument("--namespace-trust", type=Path, required=True)
    parser.add_argument("--namespace-state", type=Path, required=True)
    parser.add_argument("--runtime-release-hash", required=True)
    parser.add_argument("--bootstrap-approval-id")
    parser.add_argument("--bootstrap-approver-id")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--proposal-output", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--callback-timeout", type=float, default=20.0)
    parser.add_argument("--execute-simulated", action="store_true")
    parser.add_argument("--isolate-after-report", action="store_true")
    args = parser.parse_args(argv)
    report = run(
        Settings(
            sdk_root=args.sdk_root,
            accounts_file=args.accounts_file,
            fixture=args.fixture,
            tight_burst_order_plan=args.tight_burst_order_plan,
            profile=args.profile,
            session_client_id=args.session_client_id,
            trading_day=args.trading_day,
            namespace_release=args.namespace_release,
            namespace_trust=args.namespace_trust,
            namespace_state=args.namespace_state,
            runtime_release_hash=args.runtime_release_hash,
            bootstrap_approval_id=args.bootstrap_approval_id,
            bootstrap_approver_id=args.bootstrap_approver_id,
            output=args.output,
            proposal_output=args.proposal_output,
            runtime_dir=args.runtime_dir,
            callback_timeout_seconds=args.callback_timeout,
            execute_simulated=args.execute_simulated,
            isolate_after_report=args.isolate_after_report,
        )
    )
    print(f"R1_REPLAY_SEED_PASS={report['acceptance_status'] == 'passed'}", flush=True)
    if args.isolate_after_report:
        os._exit(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
