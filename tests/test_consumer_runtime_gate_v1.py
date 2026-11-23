from dataclasses import replace
from threading import Event
from types import SimpleNamespace

import pytest
from emt_gateway.consumer_runtime_gate_v1 import (
    ConsumerEmtRuntimeGateErrorV1,
    ConsumerEmtRuntimeGateEvidenceV1,
    ConsumerEmtRuntimeGateV1,
    ManagedConsumerEmtGateRefreshV1,
)
from xuanling_tradecore.consumer.base import ConsumerAccountView
from xuanling_tradecore.domain import (
    AccountLifecycle,
    ReconciliationStatus,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    RiskSnapshot,
    TradingDayEpoch,
)


def _fixture(*, refresh_ttl=10):
    day = TradingDayEpoch("2026-10-30")
    control = SimpleNamespace(
        profile_binding=SimpleNamespace(
            binding=SimpleNamespace(account_id="account", consumer_account_id="ca"),
            profile=SimpleNamespace(identity=SimpleNamespace(adapter_instance_id="adapter")),
        ),
        namespace=SimpleNamespace(
            directory=SimpleNamespace(scope=SimpleNamespace(trading_day=day, emt_client_id=101)),
            state="state",
        ),
        namespace_not_after_wall_ns=1_000,
    )
    recovery_profile = RecoveryEvidenceProfile("rp", 1, "a" * 64, frozenset({"orders"}),
                                               True, True, True, True, True, 100)
    certificate = RecoveryCompletenessCertificate(
        "cert", "0" * 64, "rp", "a" * 64, "account", "EMT", "adapter", "101", day,
        {"orders": "b" * 64}, True, True, True, True, True, 90, 95,
    )
    certificate = replace(certificate, certificate_hash=certificate.content_sha256)
    view = ConsumerAccountView("ca", "account", "EMT", "profile", AccountLifecycle.READY,
                               ReconciliationStatus.VALIDATED)
    risk = RiskSnapshot("risk", "account", day, "c" * 64, 10, 100, 1000, 0)
    clock = {"wall": 100, "mono": 20}
    gate = ConsumerEmtRuntimeGateV1(
        control=control, recovery_profile=recovery_profile,
        verify_recovery_authority=lambda _cert: True, now_wall_ns=lambda: clock["wall"],
        now_monotonic_ns=lambda: clock["mono"], refresh_ttl_ns=refresh_ttl, recovery_ttl_ns=100,
    )
    evidence = dict(control=control, account_view=view, risk=risk, certificate=certificate)
    return gate, evidence, clock


def test_gate_starts_closed_and_expires_without_automatic_reopen():
    gate, evidence, clock = _fixture()
    assert not gate.admission_permitted()
    gate.refresh(**evidence)
    assert gate.admission_permitted() and gate.risk_snapshot() is evidence["risk"]
    gate.require_account("account")
    with pytest.raises(ConsumerEmtRuntimeGateErrorV1):
        gate.require_account("another")
    clock["mono"] = 30
    assert not gate.admission_permitted()
    with pytest.raises(ConsumerEmtRuntimeGateErrorV1):
        gate.refresh(**evidence)


@pytest.mark.parametrize("case", ["gap", "wrong_hash", "wrong_account", "expired_risk", "retired"])
def test_invalid_evidence_latches_gate_closed(case):
    gate, evidence, _clock = _fixture()
    if case == "gap":
        cert = replace(evidence["certificate"], callback_replay_complete=False)
        evidence["certificate"] = replace(cert, certificate_hash=cert.content_sha256)
    elif case == "wrong_hash":
        evidence["certificate"] = replace(evidence["certificate"], certificate_hash="f" * 64)
    elif case == "wrong_account":
        evidence["risk"] = replace(evidence["risk"], account_id="foreign")
    elif case == "expired_risk":
        evidence["risk"] = replace(evidence["risk"], expires_monotonic_ns=20)
    else:
        evidence["control"] = replace_namespace(evidence["control"])
    with pytest.raises(ConsumerEmtRuntimeGateErrorV1):
        gate.refresh(**evidence)
    assert not gate.admission_permitted()


def replace_namespace(control):
    return SimpleNamespace(
        profile_binding=control.profile_binding,
        namespace=SimpleNamespace(state="another", directory=control.namespace.directory),
        namespace_not_after_wall_ns=control.namespace_not_after_wall_ns,
    )


def test_refresh_worker_installs_verified_evidence_and_closes_on_failure():
    gate, evidence, _clock = _fixture(refresh_ttl=1_000_000_000)
    first = Event()
    second = Event()
    calls = []

    def read_evidence():
        calls.append(1)
        if len(calls) == 1:
            first.set()
            return ConsumerEmtRuntimeGateEvidenceV1(**evidence)
        second.set()
        raise RuntimeError("real source unavailable")

    worker = ManagedConsumerEmtGateRefreshV1(
        gate=gate, read_evidence=read_evidence, refresh_interval_seconds=0.1
    )
    worker.start()
    assert first.wait(2)
    assert second.wait(2)
    worker.close()
    assert not gate.admission_permitted()
    with pytest.raises(ConsumerEmtRuntimeGateErrorV1):
        gate.refresh(**evidence)
