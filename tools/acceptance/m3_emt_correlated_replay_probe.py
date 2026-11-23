"""Verify one exact EMT public-stream ``RESTART`` witness without payload retention."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from threading import Event
from time import monotonic, time_ns
from typing import Any, Callable, Mapping

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)
from emt_gateway.session import normalize_emt_trading_day
from tools.acceptance.r1_release_preflight import load_namespace_trust_v1
from tools.performance.m3_emt_public_replay_probe import (
    _RESTART_RESUME_TYPE,
    _api_version_matches,
    _error_id,
    _error_id_from_callback,
    _runtime_dir,
    load_m3_public_replay_plan,
)
from tools.performance.perfbench.simulated_accounts import (
    SimulatedAccounts,
    load_simulated_accounts,
)
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
    EmtReplayWitnessV1,
    load_signed_emt_replay_witness_v1,
)


class M3CorrelatedReplayProbeError(RuntimeError):
    """The exact, no-mutation replay witness probe cannot be safely run."""


def load_verified_replay_witness(
    *, witness_path: Path, namespace_release_path: Path, namespace_trust_path: Path
) -> EmtReplayWitnessV1:
    """Verify offline witness provenance before importing the EMT SDK.

    The signature, signer identity, release hash, namespace manifest and EMT
    client scope must all agree.  A proposal alone is deliberately unusable.
    """

    try:
        signed = load_signed_emt_replay_witness_v1(witness_path)
        trust = load_namespace_trust_v1(namespace_trust_path)
        release = load_signed_namespace_release_v1_json(
            Path(namespace_release_path).read_text(encoding="utf-8")
        )
        now_wall_ns = time_ns()
        if not signed.witness.active_at(now_wall_ns) or not release.active_at(now_wall_ns):
            raise M3CorrelatedReplayProbeError("replay witness or namespace release is expired")
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
            wall_ns=now_wall_ns,
        )
        if (
            signed.signer_id != release.signer_id
            or signed.certificate_reference != release.certificate_reference
            or signed.revocation_reference != release.revocation_reference
        ):
            raise M3CorrelatedReplayProbeError(
                "replay witness signer differs from namespace release"
            )
        verifier.verify_detached(
            signer_id=signed.signer_id,
            signature_b64=signed.signature_b64,
            payload=signed.canonical_payload(),
        )
        witness = signed.witness
        if (
            witness.namespace_release_hash != release.content_hash
            or witness.namespace_manifest_hash != directory.manifest_hash
            or witness.trading_day != release.scope.trading_day
            or witness.adapter_client_scope != str(release.scope.emt_client_id)
            or witness.adapter_instance_id != release.own.lease.adapter_instance_id
        ):
            raise M3CorrelatedReplayProbeError(
                "replay witness does not bind this namespace release and scope"
            )
        return witness
    except (
        OSError,
        EmtReplayWitnessErrorV1,
        EmtNamespaceRecoveryError,
        NamespaceReleaseActivationErrorV1,
        ValueError,
    ) as exc:
        raise M3CorrelatedReplayProbeError("replay witness authority verification failed") from exc


def run_m3_correlated_replay_probe(
    *,
    plan_path: Path,
    accounts: SimulatedAccounts,
    witness: EmtReplayWitnessV1,
    retain_native_reference: Callable[[object], None] | None = None,
) -> dict[str, object]:
    """Subscribe before login, then match only the pre-issued witness digest."""

    plan = load_m3_public_replay_plan(plan_path)
    target = next(
        (
            item
            for item in plan.base.targets
            if item.account_config_profile == witness.account_config_profile
        ),
        None,
    )
    if target is None:
        raise M3CorrelatedReplayProbeError("replay witness profile is not approved by the plan")
    if (
        target.adapter_instance_id != witness.adapter_instance_id
        or target.adapter_client_scope != witness.adapter_client_scope
        or str(target.client_id) != witness.adapter_client_scope
    ):
        raise M3CorrelatedReplayProbeError("replay witness belongs to another adapter scope")
    loaded = EmtPlatformLoader(
        SdkLoadSettings(
            sdk_root=plan.base.sdk_root, expected_hashes=plan.base.expected_library_hashes
        )
    ).load()
    account = getattr(accounts, target.account_config_profile)

    class CorrelatedReplayApi(loaded.trader_api_type):
        def __init__(self) -> None:
            super().__init__()
            self.callback_count = 0
            self.matched_callback_count = 0
            self.error_id: int | None = None
            self.callback_seen = Event()

        def _observe(self, *args: Any) -> None:
            self.callback_count += 1
            self.callback_seen.set()
            data = next((item for item in args if isinstance(item, Mapping)), None)
            if data is None:
                return
            client_id = data.get("order_client_id")
            emt_id = data.get("order_emt_id")
            if type(client_id) is not int or type(emt_id) is not int:
                return
            if witness.matches_callback(
                account_id=account.account,
                order_client_id=client_id,
                order_emt_id=emt_id,
                observed_at_wall_ns=time_ns(),
            ):
                self.matched_callback_count += 1

        def onOrderEvent(self, *args: Any) -> None:
            self._observe(*args)

        def onTradeEvent(self, *args: Any) -> None:
            self._observe(*args)

        def onError(self, data: Any) -> None:
            self.error_id = _error_id_from_callback(data)

    api: Any | None = None
    session_id = 0
    api_version: str | None = None
    trading_day: str | None = None
    login_error_id: int | None = None
    wait_ms = 0
    try:
        api = CorrelatedReplayApi()
        with suppress_native_console_output():
            api.createTraderApi(target.client_id, str(_runtime_dir(target)), 0)
            api.setSoftwareVersion("tradecore/r1-correlated-restart-replay")
            api.setHeartBeatInterval(10)
            api.subscribePublicTopic(_RESTART_RESUME_TYPE)
            session_id = int(
                api.login(accounts.host, accounts.port, account.account, account.password, 1, "")
            )
            if session_id:
                api_version = str(api.getApiVersion())
                # EMT bindings may spell the same trading day as either
                # YYYYMMDD or YYYY-MM-DD.  Witnesses deliberately use the
                # typed core's ISO form, so normalize at this vendor boundary
                # before deciding whether a RESTART callback is admissible.
                trading_day = normalize_emt_trading_day(str(api.getTradingDay()))
                start = monotonic()
                # Wait the whole approved window: an early unrelated callback must
                # not truncate observation before the exact replay can arrive.
                api.callback_seen.wait(plan.callback_observation_seconds)
                remaining = plan.callback_observation_seconds - (monotonic() - start)
                if remaining > 0:
                    Event().wait(remaining)
                wait_ms = int((monotonic() - start) * 1_000)
            else:
                login_error_id = _error_id(api)
    finally:
        if api is not None:
            if retain_native_reference is not None:
                retain_native_reference(api)
            else:
                with suppress_native_console_output():
                    try:
                        if session_id:
                            api.logout(session_id)
                    finally:
                        api.release()
    return {
        "schema_version": "tradecore-m3-correlated-replay-v1",
        "approval_reference": plan.base.approval_reference,
        "operator_id": plan.base.operator_id,
        "account_config_profile": target.account_config_profile,
        "adapter_instance_id": target.adapter_instance_id,
        "adapter_client_scope": target.adapter_client_scope,
        "witness_proposal_sha256": witness.content_sha256,
        "witness_namespace_release_hash": witness.namespace_release_hash,
        "witness_namespace_manifest_hash": witness.namespace_manifest_hash,
        "login_succeeded": session_id != 0,
        "api_version_matches_expected": _api_version_matches(
            plan.base.expected_api_version, api_version
        ),
        "trading_day_matches_witness": trading_day == witness.trading_day.value,
        "login_error_id": login_error_id,
        "public_stream_error_id": None if api is None else api.error_id,
        "callback_observation_elapsed_ms": wait_ms,
        "public_callback_count": 0 if api is None else api.callback_count,
        "exact_witness_callback_count": 0 if api is None else api.matched_callback_count,
        "replay_correlated": api is not None and api.matched_callback_count > 0,
        "safety": {
            "resume_type": "RESTART",
            "queries": False,
            "submit": False,
            "cancel": False,
            "callback_payload_persisted": False,
            "ready_promotion": False,
            "native_wrapper_retained_until_isolated_exit": retain_native_reference is not None,
        },
    }


def _passed(report: Mapping[str, object]) -> bool:
    return (
        report.get("login_succeeded") is True
        and report.get("api_version_matches_expected") is True
        and report.get("trading_day_matches_witness") is True
        and report.get("public_stream_error_id") in (None, 0)
        and report.get("replay_correlated") is True
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--witness", type=Path, required=True)
    parser.add_argument("--namespace-release", type=Path, required=True)
    parser.add_argument("--namespace-trust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--isolate-after-report", action="store_true")
    args = parser.parse_args()
    retained: list[object] = []
    report = run_m3_correlated_replay_probe(
        plan_path=args.plan,
        accounts=load_simulated_accounts(args.accounts_file),
        witness=load_verified_replay_witness(
            witness_path=args.witness,
            namespace_release_path=args.namespace_release,
            namespace_trust_path=args.namespace_trust,
        ),
        retain_native_reference=retained.append if args.isolate_after_report else None,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    passed = _passed(report)
    print(f"M3_CORRELATED_REPLAY_PASS={passed}", flush=True)
    if args.isolate_after_report:
        os._exit(0 if passed else 2)
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
