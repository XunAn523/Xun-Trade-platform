"""Contract tests for the provisioned EMT execution-session composition seam."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest
from emt_gateway.provisioned_execution_factory_v1 import (
    ProvisionedEmtExecutionFactoryErrorV1,
    ProvisionedEmtExecutionProfileV1,
    ProvisionedEmtExecutionSessionFactoryV1,
)
from emt_gateway.session import SessionSettings
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import InMemorySecretProviderV1


class _DigestOnlyNativeFactory:
    """A no-network outer factory that stores a Login argument digest only."""

    def __init__(self, settings: SessionSettings) -> None:
        self.settings_digest = sha256(
            "\0".join(
                (
                    settings.host,
                    str(settings.port),
                    settings.account,
                    settings.password,
                    str(settings.client_id),
                    settings.local_ip,
                )
            ).encode("utf-8")
        ).hexdigest()
        self.open_calls = 0

    def open(self, *, query_callback_ingress: object, broker_event_ingress: object) -> object:
        assert query_callback_ingress == "query-observer-attached"
        assert broker_event_ingress == "broker-observer-attached"
        self.open_calls += 1
        return object()


def _profile(
    provider: InMemorySecretProviderV1,
    *,
    adapter_instance_id: str = "emt-adapter-a",
    account_scope: str = "profile-emt-a",
    client_id: str = "107",
) -> AdapterProfileRevisionV1:
    material = BrokerCredentialMaterialV1(
        broker_account="sim-account-stub",
        password="stub-password-never-log",
        client_id=client_id,
        connection=BrokerConnectionV1("BROKER_TCP", "198.51.100.80", 19000),
    )
    reference = provider.store(
        target="linux-emt-secret-provider/test-host",
        material=material,
        adapter_instance_id=adapter_instance_id,
        account_scope=account_scope,
        idempotency_key=f"credential:{adapter_instance_id}:{account_scope}:{client_id}",
    )
    return AdapterProfileRevisionV1(
        identity=AdapterProfileIdentityV1(
            profile_id=account_scope,
            adapter_kind=AdapterKindV1.EMT,
            venue_id="EMT",
            adapter_instance_id=adapter_instance_id,
            account_alias="sim-alias",
        ),
        revision=1,
        state=ProfileStateV1.ACTIVE_DISARMED,
        secret_reference=reference,
        material_digest=ProfileMaterialDigestV1(
            credential_digest=material.credential_digest(),
            endpoint_digest=material.connection.endpoint_digest(),
            envelope_digest="a" * 64,
            recipient_key_id="test-k1",
            mapping_manifest_hash="b" * 64,
            capability_manifest_hash="c" * 64,
            release_manifest_hash="d" * 64,
        ),
        created_wall_ns=1,
        validation_evidence_id="evidence-1",
        approval_id="approval-1",
    )


def _runtime_profile(profile: AdapterProfileRevisionV1) -> ProvisionedEmtExecutionProfileV1:
    return ProvisionedEmtExecutionProfileV1(
        profile=profile,
        sdk_log_dir=Path("/nonsecret/runtime/sdk-log"),
        expected_client_id=107,
    )


def test_active_profile_lease_reaches_one_digest_only_native_factory() -> None:
    provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
    profile = _profile(provider)
    native_factories: list[_DigestOnlyNativeFactory] = []

    def build(settings: SessionSettings) -> _DigestOnlyNativeFactory:
        factory = _DigestOnlyNativeFactory(settings)
        native_factories.append(factory)
        return factory

    factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=_runtime_profile(profile),
        secret_provider=provider,
        factory_builder=build,
        now_wall_ns=lambda: 100,
    )
    assert factory.profile_id == "profile-emt-a"
    assert "sim-account-stub" not in repr(_runtime_profile(profile))
    assert "stub-password-never-log" not in repr(_runtime_profile(profile))

    assert factory.open(
        query_callback_ingress="query-observer-attached",
        broker_event_ingress="broker-observer-attached",
    ) is not None
    assert len(native_factories) == 1
    assert native_factories[0].open_calls == 1
    expected = sha256(
        "\0".join(
            ("198.51.100.80", "19000", "sim-account-stub", "stub-password-never-log", "107", "")
        ).encode("utf-8")
    ).hexdigest()
    assert native_factories[0].settings_digest == expected
    assert "sim-account-stub" not in repr(native_factories[0].__dict__)
    assert "stub-password-never-log" not in repr(native_factories[0].__dict__)
    with pytest.raises(ProvisionedEmtExecutionFactoryErrorV1, match="already consumed"):
        factory.open(
            query_callback_ingress="query-observer-attached",
            broker_event_ingress="broker-observer-attached",
        )
    assert len(native_factories) == 1


def test_mismatched_profile_scope_is_rejected_before_native_factory_creation() -> None:
    provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
    profile = _profile(provider)
    wrong_identity = AdapterProfileIdentityV1(
        profile_id=profile.identity.profile_id,
        adapter_kind=AdapterKindV1.EMT,
        venue_id="EMT",
        adapter_instance_id="different-emt-adapter",
        account_alias=profile.identity.account_alias,
    )
    wrong_profile = AdapterProfileRevisionV1(
        identity=wrong_identity,
        revision=profile.revision,
        state=profile.state,
        secret_reference=profile.secret_reference,
        material_digest=profile.material_digest,
        created_wall_ns=profile.created_wall_ns,
        validation_evidence_id=profile.validation_evidence_id,
        approval_id=profile.approval_id,
    )
    builder_calls = 0

    def build(_settings: SessionSettings) -> _DigestOnlyNativeFactory:
        nonlocal builder_calls
        builder_calls += 1
        return _DigestOnlyNativeFactory(_settings)

    factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=_runtime_profile(wrong_profile),
        secret_provider=provider,
        factory_builder=build,
        now_wall_ns=lambda: 100,
    )
    with pytest.raises(ProvisionedEmtExecutionFactoryErrorV1, match="secret lease is unavailable"):
        factory.open(query_callback_ingress=object(), broker_event_ingress=object())
    assert builder_calls == 0


def test_invalid_emt_client_scope_is_rejected_before_native_factory_creation() -> None:
    provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
    profile = _profile(provider, client_id="256")
    builder_calls = 0

    def build(_settings: SessionSettings) -> _DigestOnlyNativeFactory:
        nonlocal builder_calls
        builder_calls += 1
        return _DigestOnlyNativeFactory(_settings)

    factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=_runtime_profile(profile),
        secret_provider=provider,
        factory_builder=build,
        now_wall_ns=lambda: 100,
    )
    with pytest.raises(ProvisionedEmtExecutionFactoryErrorV1, match="client scope is invalid"):
        factory.open(query_callback_ingress=object(), broker_event_ingress=object())
    assert builder_calls == 0


@pytest.mark.parametrize("different_digest", ["credential_digest", "endpoint_digest"])
def test_leased_material_must_match_activated_profile_before_native_factory(different_digest):
    provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
    profile = _profile(provider)
    profile = replace(
        profile,
        material_digest=replace(profile.material_digest, **{different_digest: "f" * 64}),
    )
    built = []
    factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=_runtime_profile(profile),
        secret_provider=provider,
        factory_builder=lambda settings: built.append(settings),
        now_wall_ns=lambda: 100,
    )
    with pytest.raises(ProvisionedEmtExecutionFactoryErrorV1, match="does not match"):
        factory.open(query_callback_ingress=object(), broker_event_ingress=object())
    assert built == []


def test_leased_material_client_id_must_match_exact_signed_session_scope():
    provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
    profile = _profile(provider, client_id="108")
    built = []
    factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=_runtime_profile(profile),
        secret_provider=provider,
        factory_builder=lambda settings: built.append(settings),
        now_wall_ns=lambda: 100,
    )
    with pytest.raises(ProvisionedEmtExecutionFactoryErrorV1, match="client scope does not match"):
        factory.open(query_callback_ingress=object(), broker_event_ingress=object())
    assert built == []
