"""Explicit operator-authorized simulator supervisor, never real READY.

Normal SessionSupervisor remains unchanged. This opt-in path permits a
signed, operator-assigned simulator scope without claiming venue-wide
uniqueness or replay/echo probes. The native factory still independently
verifies the signed scope and the pinned credential/endpoint digests.
"""

from __future__ import annotations

from xuanling_tradecore.ems.session import SessionSupervisor, SessionSupervisorError

from .consumer_simulator_gate_v1 import ConsumerEmtSimulatorGateV1


class SimulatorSessionSupervisorV1(SessionSupervisor):
    def __init__(self, *, simulator_gate: ConsumerEmtSimulatorGateV1, **kwargs) -> None:
        super().__init__(**kwargs)
        self._simulator_gate = simulator_gate

    def _validate_pre_factory(self, evaluated_wall_ns: int) -> None:
        binding, mapping = self._binding, self._mapping
        proof = self._fence.require_held()
        self._simulator_gate.require_account(binding.account_id)
        self._simulator_gate.require_mapping(mapping, wall_ns=evaluated_wall_ns)
        ownership = mapping.manifest.scope_ownership
        if (
            self._fence.scope_hash != binding.ownership_scope_hash
            or proof.scope_hash != binding.ownership_scope_hash
            or proof.authorizes_ready
            or ownership is None
            or ownership.owner_instance_id != proof.instance_id
            or ownership.venue_id != binding.venue_id
            or ownership.adapter_instance_id != binding.adapter_instance_id
            or ownership.adapter_client_scope != binding.adapter_client_scope
            or mapping.manifest.manifest_hash != binding.mapping_manifest_hash
        ):
            raise SessionSupervisorError("simulator ownership fence or route is invalid")
