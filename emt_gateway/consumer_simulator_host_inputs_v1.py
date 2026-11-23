"""Load protected simulator inputs after the host acquires ownership.

No SDK is imported, no session is opened and no broker order is sent here.
This is not a recovery certificate or a complete running service.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from xuanling_tradecore.adapters.emt import FileNamespaceReleaseStateStoreV1
from xuanling_tradecore.domain import RiskSnapshot, TradingDayEpoch

from .consumer_runtime_control_v1 import (
    ConsumerEmtRuntimeControlSnapshotV1,
    LinuxRootOwnedNamespaceFileV1,
    load_consumer_emt_runtime_control_v1,
)
from .consumer_simulator_gate_v1 import ConsumerEmtSimulatorGateV1, load_emt_simulator_permit_v1
from .consumer_simulator_namespace_trust_v1 import load_simulator_namespace_trust_v1
from .consumer_simulator_profile_v1 import load_simulator_profile_binding_v1
from .systemd_simulator_secret_v1 import SystemdSimulatorSecretProviderV1


class SimulatorHostInputsErrorV1(RuntimeError):
    """The simulator startup inputs are not protected or do not agree."""


@dataclass(frozen=True)
class SimulatorHostInputPathsV1:
    profile: Path
    namespace_release: Path
    namespace_trust: Path
    namespace_state: Path
    permit: Path
    credential_directory: Path
    credential_name: str


@dataclass(frozen=True)
class SimulatorHostInputsV1:
    control: ConsumerEmtRuntimeControlSnapshotV1
    gate: ConsumerEmtSimulatorGateV1
    secret_provider: SystemdSimulatorSecretProviderV1


def load_simulator_host_inputs_v1(
    *,
    paths: SimulatorHostInputPathsV1,
    binding_revision_id: str,
    trading_day: TradingDayEpoch,
    risk: RiskSnapshot,
    require_ownership: Callable[[], None],
    now_wall_ns: Callable[[], int],
    now_monotonic_ns: Callable[[], int],
) -> SimulatorHostInputsV1:
    """Load exact protected inputs, failing closed on missing prior state.

    The caller owns the fence lifetime, supplies a simulator policy snapshot,
    and starts/monitors the returned gate with the actual session. A locally
    selected simulator does not provide authority over broker recovery.
    """

    try:
        require_ownership()
        state_path = paths.namespace_state
        if not state_path.is_absolute() or ".." in state_path.parts:
            raise SimulatorHostInputsErrorV1("namespace state path is unsafe")
        for directory in (state_path.parent, *state_path.parent.parents):
            parent = directory.lstat()
            if (
                not stat.S_ISDIR(parent.st_mode)
                or parent.st_uid not in {0, os.geteuid()}
                or parent.st_mode & 0o022
            ):
                raise SimulatorHostInputsErrorV1("namespace state directory is not protected")
        state = state_path.lstat()
        if (
            not stat.S_ISREG(state.st_mode)
            or state.st_uid not in {0, os.geteuid()}
            or state.st_mode & 0o077
        ):
            raise SimulatorHostInputsErrorV1("namespace state is not protected")
        registry = load_simulator_profile_binding_v1(paths.profile)
        trust = load_simulator_namespace_trust_v1(paths.namespace_trust)
        permit = load_emt_simulator_permit_v1(paths.permit)
        control = load_consumer_emt_runtime_control_v1(
            registry=registry,
            binding_revision_id=binding_revision_id,
            release_reader=LinuxRootOwnedNamespaceFileV1(paths.namespace_release),
            namespace_verifier=trust.verifier(),
            namespace_state_store=FileNamespaceReleaseStateStoreV1(state_path),
            expected_trust_anchor_hash=trust.trust_anchor_hash,
            trading_day=trading_day,
            emt_client_id=permit.emt_client_id,
            now_wall_ns=now_wall_ns(),
        )
        require_ownership()
        gate = ConsumerEmtSimulatorGateV1(
            control=control, permit=permit, risk=risk,
            now_wall_ns=now_wall_ns, now_monotonic_ns=now_monotonic_ns,
        )
        identity = control.profile_binding.profile.identity
        secret_provider = SystemdSimulatorSecretProviderV1(
            credential_directory=paths.credential_directory,
            credential_name=paths.credential_name,
            reference=control.profile_binding.profile.secret_reference,
            adapter_instance_id=identity.adapter_instance_id,
            account_scope=identity.profile_id,
        )
        return SimulatorHostInputsV1(control, gate, secret_provider)
    except SimulatorHostInputsErrorV1:
        raise
    except Exception as exc:
        raise SimulatorHostInputsErrorV1("simulator host inputs could not be loaded") from exc
