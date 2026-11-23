from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerQuery,
    QueryHandle,
    RecoveryCapabilityDeclaration,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtMappingRegistry,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    AccountLifecycle,
    BrokerCapabilities,
    RecoveryCompletenessCertificate,
    RecoveryEvidenceProfile,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountFenceScope,
    AdapterProfileDeclaration,
    AdapterProfileKey,
    AdapterProfileMode,
    ReadOnlyBrokerAdapter,
    VenueAdapterRegistry,
    VenueAdapterRegistryError,
    canonical_broker_capabilities_hash,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
DAY = TradingDayEpoch("2026-09-02")
DEFAULT_CERTIFICATE_FACTORY = object()


def _recovery_profile() -> RecoveryEvidenceProfile:
    return RecoveryEvidenceProfile(
        profile_id="recovery-v1",
        version=1,
        profile_hash=SHA_B,
        required_query_kinds=frozenset(
            {"orders", "trades", "working_orders", "assets", "positions"}
        ),
        requires_public_replay=False,
        requires_callback_gap_coverage=True,
        requires_route_token_echo=True,
        requires_negative_confirmation=True,
        requires_stable_double_scan_without_as_of=True,
        max_snapshot_age_ns=1_000,
    )


def _activated(*, instance_id: str = "emt-sim-instance-1", complete: bool = True):
    base = load_builtin_emt_legacy_sim_manifest()
    ownership = replace(
        base.scope_ownership,
        ownership_id=f"ownership-{instance_id}",
        adapter_instance_id=instance_id,
    )
    capabilities = (
        replace(
            base.capabilities,
            supports_public_stream_replay=True,
            supports_complete_recovery_queries=True,
            supports_negative_confirmation=True,
        )
        if complete
        else base.capabilities
    )
    recovery = (
        RecoveryCapabilityDeclaration(
            supports_order_query=True,
            supports_trade_query=True,
            supports_working_order_query=True,
            supports_asset_query=True,
            supports_position_query=True,
            automatic_recovery_eligible=True,
            evidence_ids=base.recovery.evidence_ids,
        )
        if complete
        else base.recovery
    )
    manifest = replace(
        base,
        manifest_id=f"manifest-{instance_id}",
        capabilities=capabilities,
        recovery=recovery,
        scope_ownership=ownership,
    )
    runtime = RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )
    return EmtMappingRegistry((manifest,)).activate(
        venue_id="EMT",
        manifest_id=manifest.manifest_id,
        manifest_version=manifest.manifest_version,
        runtime=runtime,
        activated_at_unix_s=0,
    )


class _Adapter:
    def __init__(self, mapping) -> None:
        self.mapping = mapping
        self.submit_count = 0
        self.cancel_count = 0
        self.query_count = 0
        self.stop_count = 0

    @property
    def adapter_id(self) -> str:
        return self.mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        return self.mapping.manifest.scope_ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self.mapping.runtime.capabilities

    def start(self, observer) -> None:
        del observer

    def stop(self) -> None:
        self.stop_count += 1

    def submit(self, instruction):
        del instruction
        self.submit_count += 1
        raise AssertionError("fake submit result not needed")

    def cancel(self, instruction):
        del instruction
        self.cancel_count += 1
        raise AssertionError("fake cancel result not needed")

    def query(self, request: BrokerQuery) -> QueryHandle:
        self.query_count += 1
        return QueryHandle(request.request_id, self.adapter_id, self.adapter_instance_id)

    def begin_recovery(self, plan, observer):
        raise AssertionError("not needed")

    def collect_recovery_evidence(self, handle):
        raise AssertionError("not needed")


class _WrongAdapter(_Adapter):
    @property
    def adapter_instance_id(self) -> str:
        return "wrong-instance"


def _certificate_factory(account_id, trading_day, profile):
    certificate = RecoveryCompletenessCertificate(
        certificate_id="certificate-1",
        certificate_hash=SHA_A,
        profile_id=profile.profile_id,
        profile_hash=profile.profile_hash,
        account_id=account_id,
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
        trading_day=trading_day,
        query_digests={kind: SHA_A for kind in profile.required_query_kinds},
        callback_replay_complete=True,
        callback_gaps_closed=True,
        route_token_echo_complete=True,
        negative_confirmation_available=True,
        snapshot_window_stable=True,
        captured_at_wall_ns=100,
        completed_at_wall_ns=200,
    )
    return replace(certificate, certificate_hash=certificate.content_sha256)


def _declaration(
    *,
    instance_id: str = "emt-sim-instance-1",
    complete: bool = True,
    adapter_factory=None,
    certificate_factory=DEFAULT_CERTIFICATE_FACTORY,
):
    mapping = _activated(instance_id=instance_id, complete=complete)
    adapter = _Adapter(mapping)
    factory = adapter_factory or (lambda: adapter)
    if certificate_factory is DEFAULT_CERTIFICATE_FACTORY:

        def certificate_factory(account_id, trading_day, profile):
            certificate = _certificate_factory(account_id, trading_day, profile)
            if instance_id == certificate.adapter_instance_id:
                return certificate
            changed = replace(certificate, adapter_instance_id=instance_id)
            return replace(changed, certificate_hash=changed.content_sha256)

    key = AdapterProfileKey(
        venue_id="EMT",
        adapter_instance_id=instance_id,
        capability_manifest_hash=canonical_broker_capabilities_hash(mapping.runtime.capabilities),
        mapping_manifest_hash=mapping.manifest.manifest_hash,
    )
    declaration = AdapterProfileDeclaration(
        profile_id=f"profile-{instance_id}",
        version=1,
        key=key,
        mapping=mapping,
        recovery_profile=_recovery_profile(),
        adapter_factory=factory,
        recovery_certificate_factory=certificate_factory,
    )
    return declaration, adapter


def _binding(declaration: AdapterProfileDeclaration) -> AccountExecutionBinding:
    ownership = declaration.mapping.manifest.scope_ownership
    return AccountExecutionBinding(
        binding_id=f"binding-{declaration.key.adapter_instance_id}",
        binding_version=1,
        account_id=ownership.account_id,
        venue_id=declaration.key.venue_id,
        adapter_instance_id=declaration.key.adapter_instance_id,
        adapter_client_scope=ownership.adapter_client_scope,
        ownership_scope_hash=AccountFenceScope(
            ownership.account_id,
            ownership.venue_id,
            ownership.adapter_client_scope,
        ).scope_hash,
        mapping_manifest_hash=declaration.key.mapping_manifest_hash,
        capability_manifest_hash=declaration.key.capability_manifest_hash,
        journal_plan_hash=SHA_A,
        journal_routing_epoch=3,
        journal_shard_id=0,
    )


class VenueAdapterRegistryTests(unittest.TestCase):
    def test_exact_eligible_profile_creates_one_adapter_concurrently(self) -> None:
        calls = 0
        mapping = _activated()
        adapter = _Adapter(mapping)

        def factory():
            nonlocal calls
            calls += 1
            return adapter

        key = AdapterProfileKey(
            "EMT",
            "emt-sim-instance-1",
            canonical_broker_capabilities_hash(mapping.runtime.capabilities),
            mapping.manifest.manifest_hash,
        )
        declaration = AdapterProfileDeclaration(
            "profile-1",
            1,
            key,
            mapping,
            _recovery_profile(),
            factory,
            _certificate_factory,
        )
        registry = VenueAdapterRegistry((declaration,))
        self.assertTrue(registry.evaluate(key, evaluated_wall_ns=1).eligible)
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertEqual(selection.mode, AdapterProfileMode.ELIGIBLE_DISARMED)
        self.assertFalse(selection.authorizes_ready)
        with ThreadPoolExecutor(max_workers=8) as executor:
            adapters = list(
                executor.map(lambda _: registry.create_selected_adapter(selection), range(32))
            )
        self.assertEqual(calls, 1)
        self.assertTrue(all(item is adapter for item in adapters))

    def test_duplicate_unknown_and_route_hash_mismatch_fail_closed(self) -> None:
        declaration, _ = _declaration()
        registry = VenueAdapterRegistry((declaration,))
        with self.assertRaises(VenueAdapterRegistryError):
            registry.register(declaration)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "capability hash"):
            replace(declaration, key=replace(declaration.key, capability_manifest_hash=SHA_A))
        unknown = replace(_binding(declaration), capability_manifest_hash=SHA_B)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "exact Adapter profile"):
            registry.select(
                unknown,
                account_lifecycle=AccountLifecycle.DISARMED,
                adapter_session_active=False,
                evaluated_wall_ns=1,
            )

    def test_reselection_revalidates_scope_expiry(self) -> None:
        declaration, _ = _declaration()
        registry = VenueAdapterRegistry((declaration,))
        binding = _binding(declaration)
        initial = registry.select(
            binding,
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertEqual(initial.mode, AdapterProfileMode.ELIGIBLE_DISARMED)
        expired = registry.select(
            binding,
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=declaration.mapping.manifest.scope_ownership.valid_until_wall_ns,
        )
        self.assertEqual(expired.mode, AdapterProfileMode.READ_ONLY_RECONCILIATION)
        self.assertIn("SCOPE_OWNERSHIP_INVALID", expired.reason_codes)

    def test_profile_selection_requires_disarmed_and_no_active_session(self) -> None:
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            raise AssertionError("factory must not run")

        declaration, _ = _declaration(adapter_factory=factory)
        registry = VenueAdapterRegistry((declaration,))
        for lifecycle, active in (
            (AccountLifecycle.READY, False),
            (AccountLifecycle.DISARMED, True),
        ):
            with self.subTest(lifecycle=lifecycle, active=active):
                with self.assertRaises(VenueAdapterRegistryError):
                    registry.select(
                        _binding(declaration),
                        account_lifecycle=lifecycle,
                        adapter_session_active=active,
                        evaluated_wall_ns=1,
                    )
        self.assertEqual(calls, 0)

    def test_factory_identity_mismatch_is_stopped_and_not_installed(self) -> None:
        declaration, _ = _declaration()
        wrong = _WrongAdapter(declaration.mapping)
        declaration = replace(declaration, adapter_factory=lambda: wrong)
        registry = VenueAdapterRegistry((declaration,))
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        with self.assertRaisesRegex(VenueAdapterRegistryError, "outside its profile"):
            registry.create_selected_adapter(selection)
        self.assertEqual(wrong.stop_count, 1)

    def test_incomplete_profile_is_wrapped_read_only(self) -> None:
        declaration, adapter = _declaration(complete=False)
        registry = VenueAdapterRegistry((declaration,))
        eligibility = registry.evaluate(declaration.key, evaluated_wall_ns=1)
        self.assertFalse(eligibility.eligible)
        self.assertIn("CAPABILITY_RECOVERY_INCOMPLETE", eligibility.reason_codes)
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        exposed = registry.create_selected_adapter(selection)
        self.assertIsInstance(exposed, ReadOnlyBrokerAdapter)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects submit"):
            exposed.submit(None)  # type: ignore[reportArgumentType]  # Wrapper must reject before inspection.
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects cancel"):
            exposed.cancel(None)  # type: ignore[reportArgumentType]  # Wrapper must reject before inspection.
        handle = exposed.query(
            BrokerQuery("query-1", "account-1", "EMT", "emt-sim-instance-1", "orders", 10)
        )
        self.assertEqual(handle.request_id, "query-1")
        self.assertEqual(
            (adapter.submit_count, adapter.cancel_count, adapter.query_count), (0, 0, 1)
        )

    def test_profile_switch_requires_detach_and_forces_recovery_mode(self) -> None:
        first, first_adapter = _declaration()
        second, _ = _declaration(instance_id="emt-sim-instance-2")
        registry = VenueAdapterRegistry((first, second))
        selected_first = registry.select(
            _binding(first),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        registry.create_selected_adapter(selected_first)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "detached"):
            registry.select(
                _binding(second),
                account_lifecycle=AccountLifecycle.DISARMED,
                adapter_session_active=False,
                evaluated_wall_ns=1,
            )
        first_adapter.stop()
        registry.detach(
            "account-1",
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
        )
        selected_second = registry.select(
            _binding(second),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertTrue(selected_second.profile_switched)
        self.assertEqual(selected_second.mode, AdapterProfileMode.READ_ONLY_RECONCILIATION)
        self.assertIn("PROFILE_SWITCH_REQUIRES_RECOVERY", selected_second.reason_codes)
        selected_again = registry.select(
            _binding(second),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertEqual(selected_again.mode, AdapterProfileMode.READ_ONLY_RECONCILIATION)
        exposed = registry.create_selected_adapter(selected_second)
        self.assertIsInstance(exposed, ReadOnlyBrokerAdapter)
        certificate = registry.create_recovery_certificate(selected_second, trading_day=DAY)
        exposed.stop()
        registry.detach(
            "account-1",
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
        )
        promoted = registry.complete_profile_recovery(
            selected_second,
            certificate,
            evaluated_wall_ns=1,
        )
        self.assertEqual(promoted.mode, AdapterProfileMode.ELIGIBLE_DISARMED)
        self.assertFalse(promoted.authorizes_ready)

    def test_recovery_certificate_must_match_selected_profile_and_content_hash(self) -> None:
        declaration, _ = _declaration()
        registry = VenueAdapterRegistry((declaration,))
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        certificate = registry.create_recovery_certificate(selection, trading_day=DAY)
        self.assertEqual(certificate.certificate_hash, certificate.content_sha256)

        def wrong_factory(account_id, trading_day, profile):
            return replace(
                _certificate_factory(account_id, trading_day, profile),
                adapter_instance_id="wrong-instance",
            )

        wrong, _ = _declaration(certificate_factory=wrong_factory)
        wrong_registry = VenueAdapterRegistry((wrong,))
        wrong_selection = wrong_registry.select(
            _binding(wrong),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        with self.assertRaisesRegex(VenueAdapterRegistryError, "certificate identity"):
            wrong_registry.create_recovery_certificate(wrong_selection, trading_day=DAY)


if __name__ == "__main__":
    unittest.main()
