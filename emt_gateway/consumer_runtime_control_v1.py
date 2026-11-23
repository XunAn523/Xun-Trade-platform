"""Load the actual control-plane binding and signed namespace before EMT startup.

This module does not create a broker session or make an account READY. The
loader must run under an already-held ownership fence. Missing state is not a
bootstrap approval and never triggers automatic client-id range initialization.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from xuanling_tradecore.adapters.emt import (
    ActivatedNamespaceReleaseV1,
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    load_signed_namespace_release_v1_json,
)
from xuanling_tradecore.consumer.bindings_v2 import ConsumerBindingSnapshotV2
from xuanling_tradecore.control_plane.provisioning_registry_v1 import ActiveProfileBindingV1
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerStrategyBinding,
    TradingDayEpoch,
)


class ConsumerEmtRuntimeControlErrorV1(RuntimeError):
    """The real control-plane/namespace inputs cannot safely be loaded."""


class ActiveProfileBindingReaderV1(Protocol):
    def active_binding_and_profile(
        self, *, binding_revision_id: str, now_wall_ns: int
    ) -> ActiveProfileBindingV1: ...


class NamespaceReleaseReaderV1(Protocol):
    def read(self) -> str: ...


class LinuxRootOwnedNamespaceFileV1:
    """Read one bounded public release from root-owned, non-writable paths."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def read(self) -> str:
        if os.name != "posix" or not self._path.is_absolute() or ".." in self._path.parts:
            raise ConsumerEmtRuntimeControlErrorV1("namespace release path is unsafe")
        try:
            for part in (self._path.parent, *self._path.parent.parents):
                meta = part.lstat()
                if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != 0 or meta.st_mode & 0o022:
                    raise ConsumerEmtRuntimeControlErrorV1("namespace release directory is unsafe")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            if not getattr(os, "O_NOFOLLOW", 0):
                raise ConsumerEmtRuntimeControlErrorV1("safe namespace file open is unavailable")
            fd = os.open(self._path, flags)
            try:
                meta = os.fstat(fd)
                if (
                    not stat.S_ISREG(meta.st_mode)
                    or meta.st_uid != 0
                    or meta.st_mode & 0o022
                    or meta.st_size > 1_048_576
                ):
                    raise ConsumerEmtRuntimeControlErrorV1("namespace release file is unsafe")
                with os.fdopen(fd, "rb", closefd=False) as handle:
                    data = handle.read(1_048_577)
                if len(data) > 1_048_576:
                    raise ConsumerEmtRuntimeControlErrorV1("namespace release is too large")
                return data.decode("utf-8")
            finally:
                os.close(fd)
        except (OSError, UnicodeError) as exc:
            raise ConsumerEmtRuntimeControlErrorV1("namespace release cannot be read") from exc


@dataclass(frozen=True)
class ConsumerEmtRuntimeControlSnapshotV1:
    profile_binding: ActiveProfileBindingV1
    namespace: ActivatedNamespaceReleaseV1
    consumer_bindings: ConsumerBindingSnapshotV2
    namespace_not_after_wall_ns: int


def load_consumer_emt_runtime_control_v1(
    *,
    registry: ActiveProfileBindingReaderV1,
    binding_revision_id: str,
    release_reader: NamespaceReleaseReaderV1,
    namespace_verifier: EmtOrderClientNamespaceVerifier,
    namespace_state_store: FileNamespaceReleaseStateStoreV1,
    expected_trust_anchor_hash: str,
    trading_day: TradingDayEpoch,
    emt_client_id: int,
    now_wall_ns: int,
) -> ConsumerEmtRuntimeControlSnapshotV1:
    """Read the current published binding and verify an existing namespace state.

    Trust roots and state storage must themselves come from protected host
    configuration. Revocation must be refreshed by the host's control plane;
    this snapshot alone is not an ongoing authorization or recovery proof.
    """

    try:
        selected = registry.active_binding_and_profile(
            binding_revision_id=binding_revision_id, now_wall_ns=now_wall_ns
        )
        binding = selected.binding
        profile = selected.profile
        if profile.identity.venue_id != "EMT" or namespace_state_store.load() is None:
            raise ConsumerEmtRuntimeControlErrorV1("EMT namespace state or profile is unavailable")
        release = load_signed_namespace_release_v1_json(release_reader.read())
        if release.own.lease.adapter_instance_id != profile.identity.adapter_instance_id:
            raise ConsumerEmtRuntimeControlErrorV1("EMT namespace has another Adapter instance")
        scope = EmtOrderClientNamespaceScope(binding.account_id, trading_day, emt_client_id)
        activated = NamespaceReleaseActivationCoordinatorV1(
            verifier=namespace_verifier, state_store=namespace_state_store
        ).activate(
            release,
            expected_scope=scope,
            expected_release_manifest_hash=profile.material_digest.release_manifest_hash,
            expected_trust_anchor_hash=expected_trust_anchor_hash,
            now_wall_ns=now_wall_ns,
        )
        if activated.directory.own.adapter_instance_id != profile.identity.adapter_instance_id:
            raise ConsumerEmtRuntimeControlErrorV1("EMT namespace has another Adapter instance")
        account = ConsumerAccountBinding(
            binding.binding_revision_id,
            binding.profile_revision,
            binding.consumer_id,
            binding.consumer_account_id,
            binding.account_id,
            binding.account_display_name,
            "EMT",
            profile.identity.adapter_instance_id,
        )
        strategies = tuple(
            ConsumerStrategyBinding(
                f"{binding.binding_revision_id}:strategy:{index}",
                binding.profile_revision,
                binding.consumer_id,
                binding.consumer_account_id,
                binding.binding_revision_id,
                strategy_id,
            )
            for index, strategy_id in enumerate(binding.strategy_ids, start=1)
        )
        return ConsumerEmtRuntimeControlSnapshotV1(
            selected,
            activated,
            ConsumerBindingSnapshotV2(binding.authorization_epoch, (account,), strategies),
            min(release.valid_until_wall_ns, activated.directory.own.valid_until_wall_ns),
        )
    except ConsumerEmtRuntimeControlErrorV1:
        raise
    except Exception as exc:
        raise ConsumerEmtRuntimeControlErrorV1("EMT control inputs could not be verified") from exc
