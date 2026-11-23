"""Fail-closed Adapter session lifecycle gated by ownership and profile proof."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Callable, Protocol

from ..adapters import ActivatedVenueMapping, BrokerAdapter, BrokerEventObserver
from .ownership import OwnershipFenceProof
from .routing import AccountExecutionBinding


class SessionSupervisorError(RuntimeError):
    """An Adapter session cannot be created or its state cannot be proven."""


class SessionSupervisorState(str, Enum):
    DISARMED = "DISARMED"
    STARTING = "STARTING"
    RUNNING_DISARMED = "RUNNING_DISARMED"
    STOPPING = "STOPPING"
    STOPPED_DISARMED = "STOPPED_DISARMED"
    FAILED_DISARMED = "FAILED_DISARMED"


class OwnershipFencePort(Protocol):
    @property
    def scope_hash(self) -> str: ...

    def require_held(self) -> OwnershipFenceProof: ...


BrokerAdapterFactory = Callable[[], BrokerAdapter]


@dataclass(frozen=True)
class SessionSupervisorSnapshot:
    account_id: str
    venue_id: str
    adapter_instance_id: str
    state: SessionSupervisorState
    adapter_created: bool
    adapter_started: bool
    ownership_scope_hash: str
    mapping_manifest_hash: str
    authorizes_new_orders: bool = False


class SessionSupervisor:
    """Create/start one Adapter only after every local ownership gate passes."""

    def __init__(
        self,
        *,
        binding: AccountExecutionBinding,
        activated_mapping: ActivatedVenueMapping,
        ownership_fence: OwnershipFencePort,
        adapter_factory: BrokerAdapterFactory,
    ) -> None:
        self._binding = binding
        self._mapping = activated_mapping
        self._fence = ownership_fence
        self._adapter_factory = adapter_factory
        self._state = SessionSupervisorState.DISARMED
        self._adapter: BrokerAdapter | None = None
        self._adapter_started = False
        self._lock = RLock()

    def start(self, observer: BrokerEventObserver, *, evaluated_wall_ns: int) -> None:
        """Validate fence/profile before factory; Adapter.start attaches ingress."""

        if evaluated_wall_ns < 0:
            raise ValueError("session evaluated_wall_ns must be nonnegative")
        with self._lock:
            if self._state is not SessionSupervisorState.DISARMED:
                raise SessionSupervisorError("session supervisor is not in initial DISARMED state")
            try:
                self._validate_pre_factory(evaluated_wall_ns)
                self._state = SessionSupervisorState.STARTING
                adapter = self._adapter_factory()
                self._adapter = adapter
                self._validate_adapter(adapter)
                adapter.start(observer)
                self._adapter_started = True
                self._state = SessionSupervisorState.RUNNING_DISARMED
            except BaseException as exc:
                adapter = self._adapter
                if adapter is not None:
                    try:
                        adapter.stop()
                    except BaseException:
                        pass
                self._adapter_started = False
                self._state = SessionSupervisorState.FAILED_DISARMED
                if isinstance(exc, SessionSupervisorError):
                    raise
                raise SessionSupervisorError(
                    "Adapter session startup failed and remains DISARMED"
                ) from exc

    def _validate_pre_factory(self, evaluated_wall_ns: int) -> None:
        binding = self._binding
        if self._fence.scope_hash != binding.ownership_scope_hash:
            raise SessionSupervisorError("ownership fence scope does not match account route")
        proof = self._fence.require_held()
        if proof.scope_hash != binding.ownership_scope_hash or proof.authorizes_ready:
            raise SessionSupervisorError("ownership fence proof is invalid or over-authorizing")
        manifest = self._mapping.manifest
        runtime = self._mapping.runtime
        ownership = manifest.scope_ownership
        if manifest.manifest_hash != binding.mapping_manifest_hash:
            raise SessionSupervisorError("activated mapping hash does not match account route")
        if (
            manifest.venue_id != binding.venue_id
            or runtime.account_capabilities.account_id != binding.account_id
            or ownership is None
            or runtime.scope_ownership != ownership
        ):
            raise SessionSupervisorError("activated mapping identity does not match account route")
        if (
            ownership.account_id != binding.account_id
            or ownership.venue_id != binding.venue_id
            or ownership.adapter_instance_id != binding.adapter_instance_id
            or ownership.adapter_client_scope != binding.adapter_client_scope
            or ownership.owner_instance_id != proof.instance_id
            or not ownership.is_active_at(evaluated_wall_ns)
            or ownership.signer_id not in runtime.trusted_scope_signer_ids
            or not runtime.route_token_probe_succeeded
        ):
            raise SessionSupervisorError(
                "scope ownership, signer, validity or startup probe is not trusted"
            )

    def _validate_adapter(self, adapter: BrokerAdapter) -> None:
        binding = self._binding
        fingerprint = self._mapping.manifest.fingerprint
        if (
            adapter.adapter_id != fingerprint.adapter_id
            or adapter.adapter_instance_id != binding.adapter_instance_id
            or adapter.capabilities != self._mapping.runtime.capabilities
            or adapter.capabilities.venue_id != binding.venue_id
        ):
            raise SessionSupervisorError("created Adapter does not match the activated profile")

    def stop_adapter(self) -> None:
        """Stop Adapter work; fence release remains the outer shutdown coordinator's job."""

        with self._lock:
            if self._state is SessionSupervisorState.STOPPED_DISARMED:
                return
            adapter = self._adapter
            self._state = SessionSupervisorState.STOPPING
            try:
                if adapter is not None:
                    adapter.stop()
            except BaseException as exc:
                self._adapter_started = False
                self._state = SessionSupervisorState.FAILED_DISARMED
                raise SessionSupervisorError(
                    "Adapter stop failed; ownership fence must remain held"
                ) from exc
            self._adapter_started = False
            self._state = SessionSupervisorState.STOPPED_DISARMED

    def require_started_adapter(self) -> BrokerAdapter:
        """Return the verified Adapter only while this session is running.

        This is a composition seam, not an admission decision.  Callers still
        need their EMS Guard, static route and Journal-first dispatch policy;
        a ``RUNNING_DISARMED`` session never authorizes a new order merely by
        exposing its already-started Adapter.
        """

        with self._lock:
            if (
                self._state is not SessionSupervisorState.RUNNING_DISARMED
                or not self._adapter_started
                or self._adapter is None
            ):
                raise SessionSupervisorError("session supervisor has no started Adapter")
            return self._adapter

    def snapshot(self) -> SessionSupervisorSnapshot:
        with self._lock:
            return SessionSupervisorSnapshot(
                account_id=self._binding.account_id,
                venue_id=self._binding.venue_id,
                adapter_instance_id=self._binding.adapter_instance_id,
                state=self._state,
                adapter_created=self._adapter is not None,
                adapter_started=self._adapter_started,
                ownership_scope_hash=self._binding.ownership_scope_hash,
                mapping_manifest_hash=self._binding.mapping_manifest_hash,
            )
