import json
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from emt_gateway.consumer_simulator_gate_v1 import (
    ConsumerEmtSimulatorGateV1,
    EmtSimulatorGateErrorV1,
    EmtSimulatorPermitV1,
    parse_emt_simulator_permit_v1,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import ProfileStateV1
from xuanling_tradecore.domain import (
    AccountLifecycle,
    ConsumerAccountBinding,
    ReconciliationStatus,
    RiskSnapshot,
    TradingDayEpoch,
)


def fixture():
    day = TradingDayEpoch("2026-10-31")
    account = ConsumerAccountBinding("bind", 1, "consumer", "ca", "account", "sim", "EMT", "adapter")
    profile = SimpleNamespace(
        identity=SimpleNamespace(venue_id="EMT", profile_id="profile", adapter_instance_id="adapter"),
        state=ProfileStateV1.ACTIVE_DISARMED,
        revision=1,
        material_digest=SimpleNamespace(credential_digest="a" * 64, endpoint_digest="b" * 64, release_manifest_hash="e" * 64),
    )
    scope = SimpleNamespace(account_id="account", emt_client_id=101, trading_day=day)
    control = SimpleNamespace(
        profile_binding=SimpleNamespace(
            profile=profile, binding=SimpleNamespace(account_id="account", consumer_account_id="ca", profile_id="profile", profile_revision=1, effective_wall_ns=0, retired_wall_ns=None)
        ),
        consumer_bindings=SimpleNamespace(accounts=(account,)),
        namespace=SimpleNamespace(
            directory=SimpleNamespace(scope=scope, own=SimpleNamespace(active_at=lambda _: True)),
            state=SimpleNamespace(scope=scope, namespace_manifest_hash="c" * 64, release_hash="d" * 64, release_manifest_hash="e" * 64),
        ),
        namespace_not_after_wall_ns=1_000,
    )
    permit = EmtSimulatorPermitV1(
        "approval", "account", "ca", "adapter", "profile", 101,
        "a" * 64, "b" * 64, "c" * 64, "d" * 64, 900,
    )
    risk = RiskSnapshot("sim-policy", "account", day, "e" * 64, 10, 100, 1000, 0)
    clock = {"wall": 100, "mono": 20}
    gate = ConsumerEmtSimulatorGateV1(
        control=control, permit=permit, risk=risk,
        now_wall_ns=lambda: clock["wall"], now_monotonic_ns=lambda: clock["mono"],
    )
    return gate, control, permit, risk, account, clock


def test_simulator_gate_admits_only_pinned_scope_and_never_claims_validated():
    gate, _control, _permit, risk, account, clock = fixture()
    assert gate.account_view(account).lifecycle is AccountLifecycle.DISARMED
    gate.start()
    view = gate.account_view(account)
    assert view.lifecycle is AccountLifecycle.SIMULATOR_ACTIVE
    assert view.reconciliation_status is ReconciliationStatus.NOT_REQUIRED
    assert gate.risk_snapshot() == risk
    gate.require_account("account")
    with pytest.raises(EmtSimulatorGateErrorV1):
        gate.require_account("other")
    with pytest.raises(EmtSimulatorGateErrorV1):
        gate.account_view(replace(account, consumer_id="other"))
    clock["mono"] = 100
    assert not gate.admission_permitted()
    with pytest.raises(EmtSimulatorGateErrorV1):
        gate.start()


@pytest.mark.parametrize("field", ["credential_digest", "endpoint_digest", "namespace_manifest_hash", "namespace_release_hash"])
def test_simulator_gate_rejects_other_endpoint_credentials_and_namespace(field):
    gate, control, permit, risk, _account, clock = fixture()
    gate = ConsumerEmtSimulatorGateV1(
        control=control, permit=replace(permit, **{field: "f" * 64}), risk=risk,
        now_wall_ns=lambda: clock["wall"], now_monotonic_ns=lambda: clock["mono"],
    )
    with pytest.raises(EmtSimulatorGateErrorV1):
        gate.start()
    assert not gate.admission_permitted()


def test_simulator_gate_latches_on_clock_regression():
    gate, _control, _permit, _risk, _account, clock = fixture()
    gate.start()
    clock["wall"] += 10
    assert gate.admission_permitted()
    clock["wall"] -= 1
    assert not gate.admission_permitted()
    clock["wall"] += 1
    assert not gate.admission_permitted()


def test_simulator_permit_json_is_exact_and_rejects_duplicate_or_wrong_types():
    _gate, _control, permit, _risk, _account, _clock = fixture()
    data = {"schema_version": 1, **asdict(permit)}
    assert parse_emt_simulator_permit_v1(json.dumps(data)) == permit
    invalid = [
        json.dumps({**data, "unknown": "ignored"}),
        json.dumps({**data, "emt_client_id": True}),
        json.dumps({**data, "schema_version": True}),
        json.dumps(data)[:-1] + ', "approval_id": "second"}',
    ]
    for text in invalid:
        with pytest.raises(EmtSimulatorGateErrorV1):
            parse_emt_simulator_permit_v1(text)
