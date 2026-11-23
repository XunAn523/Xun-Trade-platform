from __future__ import annotations

import dataclasses
import sys
import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerAdapter,
    BrokerEventObserver,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import EmtMappingRegistry, load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.domain import CallbackGapEvidence
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountFenceScope,
    AccountOwnershipFence,
    AccountOwnershipFenceContendedError,
    OwnershipFenceProof,
    SessionSupervisor,
    SessionSupervisorError,
    SessionSupervisorState,
)

SHA_A = "a" * 64


def _activated():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtMappingRegistry((manifest,)).activate(
        venue_id="EMT",
        manifest_id=manifest.manifest_id,
        manifest_version=manifest.manifest_version,
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities(
                account_id="account-1",
                supports_cash=True,
                supports_margin_buy=True,
                supports_short_sell=True,
            ),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _binding(mapping) -> AccountExecutionBinding:
    return AccountExecutionBinding(
        binding_id="account-1-emt-v1",
        binding_version=1,
        account_id="account-1",
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
        ownership_scope_hash=AccountFenceScope("account-1", "EMT", "7").scope_hash,
        mapping_manifest_hash=mapping.manifest.manifest_hash,
        capability_manifest_hash=SHA_A,
        journal_plan_hash=SHA_A,
        journal_routing_epoch=3,
        journal_shard_id=0,
    )


class _Fence:
    def __init__(self, scope_hash: str, events: list[str]) -> None:
        self.scope_hash = scope_hash
        self.events = events
        self.valid = True

    def require_held(self) -> OwnershipFenceProof:
        self.events.append("fence")
        if not self.valid:
            raise RuntimeError("not held")
        return OwnershipFenceProof(
            scope_hash=self.scope_hash,
            instance_id="tradecore-single-host-1",
            owner_pid=1,
            acquired_monotonic_ns=100,
        )


class _Observer:
    def on_broker_event(self, event) -> None:
        pass

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        pass


class _Adapter:
    def __init__(self, mapping, events: list[str]) -> None:
        self._mapping = mapping
        self._events = events
        self.fail_start = False
        self.fail_stop = False
        self.stop_count = 0

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        return "emt-sim-instance-1"

    @property
    def capabilities(self):
        return self._mapping.runtime.capabilities

    def start(self, observer: BrokerEventObserver) -> None:
        self._events.append("start-with-observer")
        if self.fail_start:
            raise RuntimeError("start failed")

    def stop(self) -> None:
        self._events.append("stop")
        self.stop_count += 1
        if self.fail_stop:
            raise RuntimeError("stop failed")

    def submit(self, instruction):
        raise AssertionError("not used")

    def cancel(self, instruction):
        raise AssertionError("not used")

    def query(self, request):
        raise AssertionError("not used")

    def begin_recovery(self, plan, observer):
        raise AssertionError("not used")

    def collect_recovery_evidence(self, handle):
        raise AssertionError("not used")


class SessionSupervisorTests(unittest.TestCase):
    def _supervisor(self, *, mapping=None, adapter=None):
        mapping = mapping or _activated()
        binding = _binding(mapping)
        events: list[str] = []
        fence = _Fence(binding.ownership_scope_hash, events)
        adapter = adapter or _Adapter(mapping, events)

        def factory() -> BrokerAdapter:
            events.append("factory")
            return adapter

        supervisor = SessionSupervisor(
            binding=binding,
            activated_mapping=mapping,
            ownership_fence=fence,
            adapter_factory=factory,
        )
        return supervisor, fence, adapter, events

    def test_fence_and_profile_are_checked_before_adapter_factory_and_start(self) -> None:
        supervisor, _, _, events = self._supervisor()
        supervisor.start(_Observer(), evaluated_wall_ns=1)
        self.assertEqual(["fence", "factory", "start-with-observer"], events)
        snapshot = supervisor.snapshot()
        self.assertEqual(SessionSupervisorState.RUNNING_DISARMED, snapshot.state)
        self.assertTrue(snapshot.adapter_created)
        self.assertTrue(snapshot.adapter_started)
        self.assertFalse(snapshot.authorizes_new_orders)
        with self.assertRaises(SessionSupervisorError):
            supervisor.start(_Observer(), evaluated_wall_ns=1)

    def test_missing_fence_or_inactive_scope_never_calls_factory(self) -> None:
        supervisor, fence, _, events = self._supervisor()
        fence.valid = False
        with self.assertRaises(SessionSupervisorError):
            supervisor.start(_Observer(), evaluated_wall_ns=1)
        self.assertEqual(["fence"], events)
        self.assertEqual(SessionSupervisorState.FAILED_DISARMED, supervisor.snapshot().state)

        mapping = _activated()
        ownership = dataclasses.replace(mapping.manifest.scope_ownership, revoked_at_wall_ns=1)
        inactive = dataclasses.replace(
            mapping,
            manifest=dataclasses.replace(mapping.manifest, scope_ownership=ownership),
            runtime=dataclasses.replace(mapping.runtime, scope_ownership=ownership),
        )
        supervisor, _, _, events = self._supervisor(mapping=inactive)
        with self.assertRaises(SessionSupervisorError):
            supervisor.start(_Observer(), evaluated_wall_ns=1)
        self.assertEqual(["fence"], events)

    def test_adapter_identity_or_start_failure_stops_created_adapter(self) -> None:
        mapping = _activated()
        events: list[str] = []
        wrong = _Adapter(mapping, events)
        wrong._mapping = dataclasses.replace(
            mapping,
            runtime=dataclasses.replace(
                mapping.runtime,
                capabilities=dataclasses.replace(mapping.runtime.capabilities, venue_id="QMT"),
            ),
        )
        supervisor, _, wrong, outer_events = self._supervisor(mapping=mapping, adapter=wrong)
        wrong._events = outer_events
        with self.assertRaises(SessionSupervisorError):
            supervisor.start(_Observer(), evaluated_wall_ns=1)
        self.assertEqual(1, wrong.stop_count)
        self.assertNotIn("start-with-observer", outer_events)

        supervisor, _, adapter, events = self._supervisor()
        adapter.fail_start = True
        with self.assertRaises(SessionSupervisorError):
            supervisor.start(_Observer(), evaluated_wall_ns=1)
        self.assertEqual(["fence", "factory", "start-with-observer", "stop"], events)
        self.assertEqual(SessionSupervisorState.FAILED_DISARMED, supervisor.snapshot().state)

    @unittest.skipUnless(sys.platform == "linux", "real session fence requires Linux flock")
    def test_real_flock_is_held_before_factory_and_outlives_adapter_stop(self) -> None:
        mapping = _activated()
        binding = _binding(mapping)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            account_root = base / "control" / "accounts" / binding.ownership_scope_hash
            account_root.mkdir(parents=True)
            (base / "control").chmod(0o700)
            (base / "control" / "accounts").chmod(0o700)
            account_root.chmod(0o700)
            journal_root = base / "journals" / "generation-1"
            journal_root.mkdir(parents=True)
            fence = AccountOwnershipFence(
                runtime_control_root=base / "control",
                scope=AccountFenceScope("account-1", "EMT", "7"),
                instance_id="tradecore-single-host-1",
                host_id="linux-test-host",
                process_started_monotonic_ns=100,
                journal_id="journal-1",
                journal_root=journal_root,
                stream_generation=1,
            )
            fence.acquire(acquired_monotonic_ns=110)
            events: list[str] = []
            adapter = _Adapter(mapping, events)

            def factory() -> BrokerAdapter:
                fence.require_held()
                events.append("factory-while-held")
                return adapter

            supervisor = SessionSupervisor(
                binding=binding,
                activated_mapping=mapping,
                ownership_fence=fence,
                adapter_factory=factory,
            )
            supervisor.start(_Observer(), evaluated_wall_ns=1)
            contender = AccountOwnershipFence(
                runtime_control_root=base / "control",
                scope=AccountFenceScope("account-1", "EMT", "7"),
                instance_id="tradecore-contender",
                host_id="linux-test-host",
                process_started_monotonic_ns=100,
                journal_id="journal-2",
                journal_root=base / "journals" / "generation-2",
                stream_generation=2,
            )
            with self.assertRaises(AccountOwnershipFenceContendedError):
                contender.acquire(acquired_monotonic_ns=120)
            supervisor.stop_adapter()
            self.assertTrue(fence.held)
            fence.release()
            self.assertEqual(["factory-while-held", "start-with-observer", "stop"], events)

    def test_stop_is_idempotent_and_does_not_release_outer_fence(self) -> None:
        supervisor, fence, adapter, events = self._supervisor()
        supervisor.start(_Observer(), evaluated_wall_ns=1)
        supervisor.stop_adapter()
        supervisor.stop_adapter()
        self.assertEqual(1, adapter.stop_count)
        self.assertTrue(fence.valid)
        self.assertEqual(SessionSupervisorState.STOPPED_DISARMED, supervisor.snapshot().state)
        self.assertEqual(["fence", "factory", "start-with-observer", "stop"], events)


if __name__ == "__main__":
    unittest.main()
