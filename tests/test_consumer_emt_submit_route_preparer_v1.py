from __future__ import annotations

import os
import sys
from dataclasses import replace
from hashlib import sha256

import pytest
from emt_gateway.tradecore_mmap_journal import PreparedEmtNativeRouteResolver
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import EmtAdapterProfile
from xuanling_tradecore.adapters.emt.identity import EmtClientOrderRefScope
from xuanling_tradecore.adapters.emt.mapping import load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.adapters.emt.namespace_recovery import (
    VerifiedNamespaceBoundEmtClientOrderIdAllocator,
)
from xuanling_tradecore.adapters.emt.namespace_release_v1 import (
    ActivatedNamespaceReleaseV1,
    NamespaceReleaseStateV1,
)
from xuanling_tradecore.adapters.emt.route_token import EmtAdapterRouteTokenCodec
from xuanling_tradecore.consumer import OrderBookConsumerOperationsV2
from xuanling_tradecore.consumer.emt_submit_route_preparer_v1 import (
    EmtConsumerSubmitRoutePreparerV1,
    EmtSubmitRoutePreparationErrorV1,
)
from xuanling_tradecore.ems.ownership import AccountFenceScope, canonical_account_scope_hash
from xuanling_tradecore.ems.routing import AccountExecutionBinding
from xuanling_tradecore.oms import MmapOmsFactJournalV1, OmsFactKind, OrderBook
from xuanling_tradecore.oms.mmap_client_ref_journal_v1 import MmapClientRefJournalV1
from xuanling_tradecore.persistence import AccountJournalBinding

from test_consumer_oms_operations_v2 import ConsumerOmsOperationsV2Tests
from test_emt_namespace_recovery import _DAY, _NOW, _directory

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux namespace Journal")


def _test_mapping():
    base = load_builtin_emt_legacy_sim_manifest()
    ownership = replace(
        base.scope_ownership,
        account_id="sim-account",
        adapter_instance_id="adapter-tradecore-a",
        adapter_client_scope="101",
        owner_instance_id="fengyu",
        valid_from_wall_ns=0,
        valid_until_wall_ns=10**18,
        uniqueness_probe_succeeded=True,
    )
    manifest = replace(base, scope_ownership=ownership)
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("sim-account", True, False, False),
            scope_ownership=ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({ownership.signer_id}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _composition(root, *, authorized=True, wall_ns=_NOW, mono_ns=21, include_operations=False):
    fixture = ConsumerOmsOperationsV2Tests()
    fixture.setUp()
    fixture.account = replace(
        fixture.account,
        account_id="sim-account",
        venue_id="EMT",
        adapter_instance_id="adapter-tradecore-a",
    )
    context = fixture._context()
    context = replace(
        context,
        risk_snapshot=replace(context.risk_snapshot, account_id="sim-account", trading_day=_DAY),
        decision_context=replace(
            context.decision_context, account_id="sim-account", trading_day=_DAY
        ),
    )
    journal = MmapOmsFactJournalV1(path=root / "oms.mmap", journal_id="oms-journal", capacity=16)
    wakeups = []
    operations = OrderBookConsumerOperationsV2(
        order_book=OrderBook(journal=journal, persist_recovery_seed=True),
        admission_context_for=lambda *_: context,
        now_monotonic_ns=lambda: 20,
        on_execution_commit_evidence=lambda order, commit: wakeups.append((order, commit)),
        on_execution_wakeup_failed=lambda: None,
    )
    operations.submit_signal(
        session=fixture.session,
        account_binding=fixture.account,
        strategy_binding_id="strategy-bind",
        signal=replace(fixture._signal(), trading_day=_DAY),
    )
    directory, _key = _directory()
    refs_root = root / "refs"
    refs_root.mkdir(mode=0o700)
    os.chmod(refs_root, 0o700)
    scope = EmtClientOrderRefScope("sim-account", _DAY, "adapter-tradecore-a", 101)
    refs = MmapClientRefJournalV1(
        root=refs_root, scope=scope.canonical_scope, capacity=8, bootstrap=True
    )
    allocator = VerifiedNamespaceBoundEmtClientOrderIdAllocator(
        emt_scope=scope, directory=directory, journal=refs
    )
    allocator.recover_from_journal()
    digest = sha256(b"test").hexdigest()
    state = NamespaceReleaseStateV1(
        scope=directory.scope,
        release_epoch=1,
        release_hash=digest,
        namespace_manifest_hash=directory.manifest_hash,
        release_manifest_hash=directory.own.release_manifest_hash,
        trust_anchor_hash=digest,
        high_watermark=999,
    )
    activated = ActivatedNamespaceReleaseV1(directory, state)
    fence = canonical_account_scope_hash(AccountFenceScope("sim-account", "EMT", "101"))
    binding = AccountExecutionBinding(
        fixture.account.binding_id,
        1,
        "sim-account",
        "EMT",
        "adapter-tradecore-a",
        "101",
        fence,
        _test_mapping().manifest.manifest_hash,
        digest,
        digest,
        1,
        0,
    )
    resolver = PreparedEmtNativeRouteResolver(maximum_pending_routes=2)
    errors = []
    codec = EmtAdapterRouteTokenCodec(load_builtin_emt_legacy_sim_manifest().route_token_binding)
    preparer = EmtConsumerSubmitRoutePreparerV1(
        oms_journal=journal,
        client_ref_journal=refs,
        allocator=allocator,
        activated_release=activated,
        expected_release_manifest_hash=directory.own.release_manifest_hash,
        consumer_binding=fixture.account,
        execution_binding=binding,
        journal_binding=AccountJournalBinding(
            "sim-account", "EMT", "xuanling:journal:sim-account:", 0
        ),
        codec=codec,
        resolver=resolver,
        admission_authorized=lambda: authorized,
        close_admission=errors.append,
        wall_clock_ns=lambda: wall_ns,
        monotonic_ns=lambda: mono_ns,
        native_start_budget_ns=1000,
    )
    result = journal, refs, preparer, resolver, wakeups[0], errors
    if include_operations:
        return (*result, operations, fixture, context)
    return result


def test_real_oms_fact_becomes_signed_scope_prepared_route_without_native_call(tmp_path):
    journal, refs, preparer, resolver, (order, commit), errors = _composition(tmp_path)
    try:
        route = preparer.prepare(order, commit)
        assert route.command.recovery_seed.order == order
        assert route.command.client_order_ref == "1000"
        assert resolver.client_order_ref_for(order.execution) == "1000"
        assert tuple(f.kind for f in route.command.fact_envelope.facts) == (
            OmsFactKind.ORDER_ATTRIBUTED,
            OmsFactKind.RESERVATION_CREATED,
            OmsFactKind.CALL_STARTED,
        )
        assert route.command.fact_envelope.facts[-1].attributes["source_first_lsn"] == str(
            commit.first_lsn
        )
        assert (
            refs.read_committed_lease(preparer._allocator.scope, order.execution.execution_id)
            is not None
        )
        assert errors == []
        with pytest.raises(EmtSubmitRoutePreparationErrorV1):
            preparer.prepare(order, commit)
        assert errors == ["consumer-emt-route-preparation-failed"]
    finally:
        refs.close()
        journal.close()


def test_closed_gate_fails_before_reference_allocation(tmp_path):
    journal, refs, preparer, _resolver, (order, commit), errors = _composition(
        tmp_path, authorized=False
    )
    try:
        with pytest.raises(EmtSubmitRoutePreparationErrorV1):
            preparer.prepare(order, commit)
        assert refs.read_recovery(preparer._allocator.scope).leases == ()
        assert errors == ["consumer-emt-route-preparation-failed"]
    finally:
        refs.close()
        journal.close()


@pytest.mark.parametrize("condition", ["foreign-commit", "expired-order", "expired-lease"])
def test_invalid_evidence_or_expiry_never_allocates(tmp_path, condition):
    options = {"mono_ns": 1020} if condition == "expired-order" else {}
    if condition == "expired-lease":
        options["wall_ns"] = _NOW + 2
    journal, refs, preparer, _resolver, (order, commit), errors = _composition(tmp_path, **options)
    try:
        if condition == "foreign-commit":
            commit = replace(commit, journal_id="foreign-journal")
        with pytest.raises(EmtSubmitRoutePreparationErrorV1):
            preparer.prepare(order, commit)
        assert refs.read_recovery(preparer._allocator.scope).leases == ()
        assert errors == ["consumer-emt-route-preparation-failed"]
    finally:
        refs.close()
        journal.close()


def test_failure_after_reference_commit_latches_and_never_retries(tmp_path, monkeypatch):
    journal, refs, preparer, resolver, (order, commit), errors = _composition(tmp_path)
    try:

        def fail_prepare(_route, _instruction):
            raise OSError("injected prepared-route failure")

        monkeypatch.setattr(resolver, "prepare_submit", fail_prepare)
        with pytest.raises(EmtSubmitRoutePreparationErrorV1):
            preparer.prepare(order, commit)
        lease = refs.read_committed_lease(preparer._allocator.scope, order.execution.execution_id)
        assert lease is not None and lease.client_order_ref == "1000"
        with pytest.raises(EmtSubmitRoutePreparationErrorV1, match="latched"):
            preparer.prepare(order, commit)
        assert len(refs.read_recovery(preparer._allocator.scope).leases) == 1
        assert errors == ["consumer-emt-route-preparation-failed"]
    finally:
        refs.close()
        journal.close()
