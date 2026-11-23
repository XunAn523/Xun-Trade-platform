"""M1 CANCEL_REJECTED persistence for normalized fastspi V2 error callbacks."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from typing import Protocol

from xuanling_tradecore.adapters import canonical_venue_order_target_hash
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    ErrorCategory,
    VenueOrderIdentity,
)
from xuanling_tradecore.persistence import AccountJournalBinding, CancelRouteCommand


class FastSpiV2CancelOutcomeError(RuntimeError):
    """A V2 cancel-error callback cannot safely close an M1 cancel route."""


class FastSpiV2CancelOutcomeJournalPort(Protocol):
    def record_rejected(
        self,
        binding: AccountJournalBinding,
        command: CancelRouteCommand,
        *,
        transition_id: str,
        error_category: str,
    ) -> object: ...


@dataclass(frozen=True)
class FastSpiV2CancelOutcomeDirectoryConfig:
    account_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    maximum_cancel_routes: int = 4096

    def __post_init__(self) -> None:
        if (
            not self.account_id
            or not self.adapter_instance_id
            or not self.adapter_client_scope
            or not 1 <= self.maximum_cancel_routes <= 1_000_000
        ):
            raise FastSpiV2CancelOutcomeError(
                "fastspi v2 cancel outcome directory config is invalid"
            )


@dataclass(frozen=True)
class _CancelRouteState:
    binding: AccountJournalBinding
    command: CancelRouteCommand
    target_identity: VenueOrderIdentity


class FastSpiV2CancelOutcomeObserver:
    """Map only proven CANCEL_ERROR events to the matching M1 cancel route."""

    def __init__(
        self,
        *,
        config: FastSpiV2CancelOutcomeDirectoryConfig,
        journal: FastSpiV2CancelOutcomeJournalPort,
    ) -> None:
        self._config = config
        self._journal = journal
        self._routes: dict[str, _CancelRouteState] = {}
        self._committed_sources: dict[str, str] = {}
        self._lock = RLock()

    def register_cancel_route(
        self,
        *,
        binding: AccountJournalBinding,
        command: CancelRouteCommand,
        target_identity: VenueOrderIdentity,
    ) -> None:
        """Register the already-durable cancel request before its SDK call."""

        self._validate_registration(binding, command, target_identity)
        with self._lock:
            existing = self._routes.get(command.cancel_request_id)
            candidate = _CancelRouteState(binding, command, target_identity)
            if existing is not None:
                if existing != candidate:
                    raise FastSpiV2CancelOutcomeError(
                        "fastspi v2 cancel route registration conflicts"
                    )
                return
            if len(self._routes) >= self._config.maximum_cancel_routes:
                raise FastSpiV2CancelOutcomeError("fastspi v2 cancel outcome directory is full")
            self._routes[command.cancel_request_id] = candidate

    def on_broker_event(self, event: BrokerEvent) -> None:
        """Append CANCEL_REJECTED exactly through the registered request route."""

        if event.event_kind is not BrokerEventKind.CANCEL_ERROR:
            return
        cancel_request_id = event.cancel_request_id
        if cancel_request_id is None:
            raise FastSpiV2CancelOutcomeError(
                "fastspi v2 cancel error lacks cancel request identity"
            )
        with self._lock:
            route = self._routes.get(cancel_request_id)
            if route is None:
                raise FastSpiV2CancelOutcomeError("fastspi v2 cancel error has no registered route")
            self._validate_event(event, route)
            transition_id = self._committed_sources.get(event.source_key)
            if transition_id is None:
                transition_id = (
                    "fastspi-v2-cancel:" + sha256(event.source_key.encode("utf-8")).hexdigest()
                )
                self._committed_sources[event.source_key] = transition_id
            self._journal.record_rejected(
                route.binding,
                route.command,
                transition_id=transition_id,
                error_category=(event.error_category or ErrorCategory.VENUE_REJECTED).value,
            )

    def _validate_registration(
        self,
        binding: AccountJournalBinding,
        command: CancelRouteCommand,
        target: VenueOrderIdentity,
    ) -> None:
        if (
            binding.account_id != self._config.account_id
            or command.account_id != binding.account_id
        ):
            raise FastSpiV2CancelOutcomeError("fastspi v2 cancel route has another account")
        if binding.venue_id != "EMT":
            raise FastSpiV2CancelOutcomeError("fastspi v2 cancel route has another venue")
        if (
            target.venue_id != "EMT"
            or target.adapter_instance_id != self._config.adapter_instance_id
            or target.adapter_client_scope != self._config.adapter_client_scope
            or target.client_order_ref != command.target_client_order_ref
            or canonical_venue_order_target_hash(target) != command.venue_order_identity_hash
        ):
            raise FastSpiV2CancelOutcomeError("fastspi v2 cancel route target identity is invalid")

    def _validate_event(self, event: BrokerEvent, route: _CancelRouteState) -> None:
        identity = event.venue_order_identity
        if identity is None:
            raise FastSpiV2CancelOutcomeError("fastspi v2 cancel error lacks venue order identity")
        if (
            event.account_id != self._config.account_id
            or event.venue_id != "EMT"
            or event.adapter_instance_id != self._config.adapter_instance_id
            or identity.adapter_client_scope != self._config.adapter_client_scope
            or canonical_venue_order_target_hash(identity)
            != route.command.venue_order_identity_hash
            or (event.execution_id is not None and event.execution_id != route.command.execution_id)
        ):
            raise FastSpiV2CancelOutcomeError(
                "fastspi v2 cancel callback identity does not match route"
            )
