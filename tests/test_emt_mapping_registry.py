from __future__ import annotations

import ast
import json
import unittest
from dataclasses import replace
from pathlib import Path

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    EffectiveInterval,
    MappingManifestError,
    ProfileValidationTrigger,
    RuntimeVenueProfile,
    load_mapping_manifest_json,
)
from xuanling_tradecore.adapters.emt import (
    EmtMappingRegistry,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)

ROOT = Path(__file__).resolve().parents[1]
ADAPTERS_ROOT = ROOT / "xuanling_tradecore" / "adapters"


def _instruction(
    *,
    market: str = "SZ",
    side: str = "BUY",
    price_type: str = "LIMIT",
    business_type: str = "CASH",
) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-order-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-08-31"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            strategy_id="strategy-1",
            strategy_order_id="strategy-order-1",
            attribution_binding_id="attribution-1",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        ),
        symbol="000010",
        market=market,
        side=side,
        price_type=price_type,
        business_type=business_type,
        quantity=100,
        protection_price_ticks=163,
        limit_price_ticks=162,
    )


def _activate(
    *,
    supports_cash: bool = True,
    supports_margin_buy: bool = True,
    supports_short_sell: bool = True,
):
    manifest = load_builtin_emt_legacy_sim_manifest()
    registry = EmtMappingRegistry((manifest,))
    return registry.activate(
        venue_id="EMT",
        manifest_id=manifest.manifest_id,
        manifest_version=manifest.manifest_version,
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities(
                account_id="account-1",
                supports_cash=supports_cash,
                supports_margin_buy=supports_margin_buy,
                supports_short_sell=supports_short_sell,
            ),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


class EmtMappingRegistryTests(unittest.TestCase):
    def test_builtin_manifest_maps_only_documented_codes(self) -> None:
        activated = _activate()

        cash = activated.map_execution(_instruction())
        self.assertEqual(
            (1, 1, 1, 0), (cash.market, cash.side, cash.price_type, cash.business_type)
        )
        self.assertEqual("order_client_id", cash.identity.client_order_ref_field)
        self.assertEqual("order_emt_id", cash.identity.venue_order_id_field)
        self.assertEqual("order_cancel_emt_id", cash.identity.venue_cancel_id_field)
        self.assertEqual("client_id", cash.identity.adapter_client_scope_field)

        margin = activated.map_execution(_instruction(side="MARGIN_BUY", business_type="MARGIN"))
        short = activated.map_execution(_instruction(side="SHORT_SELL", business_type="MARGIN"))
        self.assertEqual((21, 4), (margin.side, margin.business_type))
        self.assertEqual((22, 4), (short.side, short.business_type))
        self.assertFalse(activated.manifest.recovery.automatic_recovery_eligible)
        self.assertFalse(activated.manifest.capabilities.supports_automatic_recovery_validation)
        self.assertEqual(2, activated.manifest.schema_version)
        self.assertEqual("7", activated.manifest.scope_ownership.adapter_client_scope)
        self.assertEqual(
            "order_client_id", activated.manifest.route_token_binding.native_field_name
        )
        self.assertTrue(activated.manifest.route_token_binding.supports_automatic_routing)
        self.assertEqual(64, len(cash.manifest_hash))

    def test_account_capability_and_unknown_values_fail_closed(self) -> None:
        activated = _activate(supports_margin_buy=False)
        with self.assertRaisesRegex(MappingManifestError, "MARGIN_BUY"):
            activated.map_execution(_instruction(side="MARGIN_BUY", business_type="MARGIN"))
        with self.assertRaisesRegex(MappingManifestError, "unmapped market"):
            activated.map_execution(_instruction(market="BJ"))
        with self.assertRaisesRegex(MappingManifestError, "unapproved business/side"):
            activated.map_execution(_instruction(side="BUY", business_type="MARGIN"))

    def test_fingerprint_capability_and_validity_mismatch_fail_closed(self) -> None:
        manifest = load_builtin_emt_legacy_sim_manifest()
        registry = EmtMappingRegistry((manifest,))
        runtime = RuntimeVenueProfile(
            fingerprint=replace(manifest.fingerprint, sdk_version="different-sdk"),
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
        )
        with self.assertRaisesRegex(MappingManifestError, "fingerprint"):
            registry.activate(
                venue_id="EMT",
                manifest_id=manifest.manifest_id,
                manifest_version=manifest.manifest_version,
                runtime=runtime,
                activated_at_unix_s=0,
            )
        expired_manifest = replace(
            manifest,
            effective_interval=EffectiveInterval(not_before_unix_s=1, not_after_unix_s=2),
        )
        expired_registry = EmtMappingRegistry((expired_manifest,))
        with self.assertRaisesRegex(MappingManifestError, "expired"):
            expired_registry.activate(
                venue_id="EMT",
                manifest_id=expired_manifest.manifest_id,
                manifest_version=expired_manifest.manifest_version,
                runtime=RuntimeVenueProfile(
                    expired_manifest.fingerprint,
                    expired_manifest.capabilities,
                    AccountTradingCapabilities("account-1", True, True, True),
                ),
                activated_at_unix_s=3,
            )
        with self.assertRaisesRegex(MappingManifestError, "capability declaration"):
            registry.activate(
                venue_id="EMT",
                manifest_id=manifest.manifest_id,
                manifest_version=manifest.manifest_version,
                runtime=RuntimeVenueProfile(
                    manifest.fingerprint,
                    replace(manifest.capabilities, supports_short_sell=False),
                    AccountTradingCapabilities("account-1", True, True, True),
                ),
                activated_at_unix_s=0,
            )

    def test_schema_and_semantic_evidence_guards_fail_closed(self) -> None:
        resource = (
            ROOT
            / "xuanling_tradecore"
            / "adapters"
            / "emt"
            / "manifests"
            / "emt-legacy-sim-2.20.0-v2.json"
        )
        raw = json.loads(resource.read_text(encoding="utf-8"))
        del raw["evidence"]
        with self.assertRaisesRegex(MappingManifestError, "JSON schema"):
            load_mapping_manifest_json(json.dumps(raw))

        raw = json.loads(resource.read_text(encoding="utf-8"))
        raw["field_mappings"]["market"][0]["evidence_ids"] = ["missing-evidence"]
        with self.assertRaisesRegex(MappingManifestError, "missing evidence"):
            load_mapping_manifest_json(json.dumps(raw))

        raw = json.loads(resource.read_text(encoding="utf-8"))
        raw["capabilities"]["supports_margin_buy"] = False
        with self.assertRaisesRegex(MappingManifestError, "not supported"):
            load_mapping_manifest_json(json.dumps(raw))

    def test_scope_ownership_and_route_binding_fail_closed(self) -> None:
        manifest = load_builtin_emt_legacy_sim_manifest()

        def activate_with(runtime: RuntimeVenueProfile) -> None:
            EmtMappingRegistry((manifest,)).activate(
                venue_id="EMT",
                manifest_id=manifest.manifest_id,
                manifest_version=manifest.manifest_version,
                runtime=runtime,
                activated_at_unix_s=1,
            )

        base = RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        )
        with self.assertRaisesRegex(MappingManifestError, "signer"):
            activate_with(replace(base, trusted_scope_signer_ids=frozenset({"other-signer"})))
        with self.assertRaisesRegex(MappingManifestError, "startup echo probe"):
            activate_with(replace(base, route_token_probe_succeeded=False))
        with self.assertRaisesRegex(MappingManifestError, "scope ownership does not match"):
            activate_with(
                replace(
                    base,
                    scope_ownership=replace(
                        manifest.scope_ownership,
                        uniqueness_probe_succeeded=False,
                    ),
                )
            )

        for inactive_ownership in (
            replace(manifest.scope_ownership, revoked_at_wall_ns=500_000_000),
            replace(manifest.scope_ownership, valid_until_wall_ns=500_000_000),
            replace(manifest.scope_ownership, uniqueness_probe_succeeded=False),
        ):
            inactive_manifest = replace(manifest, scope_ownership=inactive_ownership)
            with self.assertRaisesRegex(MappingManifestError, "expired, revoked"):
                EmtMappingRegistry((inactive_manifest,)).activate(
                    venue_id="EMT",
                    manifest_id=inactive_manifest.manifest_id,
                    manifest_version=inactive_manifest.manifest_version,
                    runtime=replace(base, scope_ownership=inactive_ownership),
                    activated_at_unix_s=1,
                )

        incomplete_binding = replace(manifest.route_token_binding, replay_paths=())
        incomplete_manifest = replace(manifest, route_token_binding=incomplete_binding)
        with self.assertRaisesRegex(MappingManifestError, "echo or replay path"):
            EmtMappingRegistry((incomplete_manifest,)).activate(
                venue_id="EMT",
                manifest_id=incomplete_manifest.manifest_id,
                manifest_version=incomplete_manifest.manifest_version,
                runtime=replace(base, route_token_binding=incomplete_binding),
                activated_at_unix_s=1,
            )

    def test_v1_manifest_remains_readable_but_cannot_be_activated(self) -> None:
        resource = (
            ROOT
            / "xuanling_tradecore"
            / "adapters"
            / "emt"
            / "manifests"
            / "emt-legacy-sim-2.20.0-v1.json"
        )
        legacy = load_mapping_manifest_json(resource.read_text(encoding="utf-8"))
        self.assertEqual(1, legacy.schema_version)
        self.assertIsNone(legacy.scope_ownership)
        with self.assertRaisesRegex(MappingManifestError, "legacy mapping manifest"):
            EmtMappingRegistry((legacy,)).activate(
                venue_id="EMT",
                manifest_id=legacy.manifest_id,
                manifest_version=legacy.manifest_version,
                runtime=RuntimeVenueProfile(
                    legacy.fingerprint,
                    legacy.capabilities,
                    AccountTradingCapabilities("account-1", True, True, True),
                ),
                activated_at_unix_s=1,
            )

    def test_all_lifecycle_triggers_repeat_the_same_complete_validation(self) -> None:
        manifest = load_builtin_emt_legacy_sim_manifest()
        runtime = RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        )
        for trigger in ProfileValidationTrigger:
            activated = EmtMappingRegistry((manifest,)).activate(
                venue_id="EMT",
                manifest_id=manifest.manifest_id,
                manifest_version=manifest.manifest_version,
                runtime=runtime,
                activated_at_unix_s=1,
                validation_trigger=trigger,
            )
            self.assertEqual(trigger, activated.validation_trigger)

    def test_mapping_layer_does_not_import_sdk_or_legacy_gateway(self) -> None:
        # This purity boundary is the vendor-neutral mapping core, not every
        # future Adapter implementation.  QMT Edge deliberately owns a local
        # SQLite WAL outbox outside this layer, while EMT owns a separately
        # gated SDK boundary; scanning all ``adapters/**`` made that intended
        # architecture fail a mapping-only contract.
        forbidden_roots = {"emt_gateway", "vnemttrader", "qmt", "psycopg", "sqlite3"}
        mapping_sources = (
            ADAPTERS_ROOT / "base.py",
            ADAPTERS_ROOT / "identity.py",
            ADAPTERS_ROOT / "mapping.py",
            ADAPTERS_ROOT / "route_token.py",
        )
        for source in mapping_sources:
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported_names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported_names = (node.module,)
                else:
                    continue
                for imported_name in imported_names:
                    self.assertNotIn(
                        imported_name.split(".", maxsplit=1)[0],
                        forbidden_roots,
                        f"{source.relative_to(ROOT)} imports a forbidden dependency",
                    )


if __name__ == "__main__":
    unittest.main()
