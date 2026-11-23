"""Bounded M1 route directory for fastspi V2 callback outcomes.

This in-memory directory is populated only when a command has already
received a durable PRE_NATIVE route record. It does not query a datastore, invoke
the SDK, or infer a route from an EMT native ID.  If the callback stream lacks
the echoed client reference or enough ordered state to form a complete M1
observation, the resolver rejects it and the caller's V2 gap latch closes.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock

from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    OmsOrderState,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import BrokerOrderObservationV1
from xuanling_tradecore.persistence import AccountJournalBinding, PreNativeRouteCommand

from .fastspi_v2_outcome import FastSpiV2NativeOutcomeError, FastSpiV2OutcomeResolution


@dataclass(frozen=True)
class FastSpiV2RouteOutcomeDirectoryConfig:
    """Immutable identity and bounded-memory policy for one V2 session."""

    account_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    maximum_routes: int = 4096

    def __post_init__(self) -> None:
        if (
            not self.account_id
            or not self.adapter_instance_id
            or not self.adapter_client_scope
            or not 1 <= self.maximum_routes <= 1_000_000
        ):
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 route outcome directory config is invalid"
            )


@dataclass
class _RouteState:
    binding: AccountJournalBinding
    command: PreNativeRouteCommand
    identity: VenueOrderIdentity
    observation: BrokerOrderObservationV1 | None = None


class FastSpiV2RouteOutcomeDirectory:
    """Resolve only registered callback routes and advance them after commit."""

    def __init__(self, *, config: FastSpiV2RouteOutcomeDirectoryConfig) -> None:
        self._config = config
        self._routes: dict[str, _RouteState] = {}
        self._committed_by_source: dict[str, FastSpiV2OutcomeResolution] = {}
        self._lock = RLock()

    def register_pre_native_route(
        self,
        *,
        binding: AccountJournalBinding,
        command: PreNativeRouteCommand,
        venue_order_identity: VenueOrderIdentity,
        recovered_observation: BrokerOrderObservationV1 | None = None,
    ) -> None:
        """Register exactly one already-durable route before callbacks are consumed."""

        self._validate_registration(binding, command, venue_order_identity, recovered_observation)
        client_ref = venue_order_identity.client_order_ref
        assert client_ref is not None
        with self._lock:
            existing = self._routes.get(client_ref)
            if existing is not None:
                if (
                    existing.binding != binding
                    or existing.command.payload_hash != command.payload_hash
                    or existing.identity != venue_order_identity
                    or existing.observation != recovered_observation
                ):
                    raise FastSpiV2NativeOutcomeError("fastspi v2 route registration conflicts")
                return
            if len(self._routes) >= self._config.maximum_routes:
                raise FastSpiV2NativeOutcomeError("fastspi v2 route outcome directory is full")
            self._routes[client_ref] = _RouteState(
                binding=binding,
                command=command,
                identity=venue_order_identity,
                observation=recovered_observation,
            )

    def resolve_outcome(self, event: BrokerEvent) -> FastSpiV2OutcomeResolution | None:
        """Return a complete observation only when the callback proves one."""

        if event.event_kind not in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}:
            return None
        identity = event.venue_order_identity
        if identity is None or identity.client_order_ref is None:
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 outcome callback lacks echoed client reference"
            )
        with self._lock:
            committed = self._committed_by_source.get(event.source_key)
            if committed is not None:
                self._validate_committed_replay(event, committed)
                return committed
            state = self._routes.get(identity.client_order_ref)
            if state is None:
                raise FastSpiV2NativeOutcomeError(
                    "fastspi v2 outcome callback has no registered route"
                )
            self._validate_event_identity(event, state)
            observation = self._build_observation(event, state)
            return FastSpiV2OutcomeResolution(
                binding=state.binding,
                command=state.command,
                observation=observation,
                transition_id="fastspi-v2:" + sha256(event.source_key.encode("utf-8")).hexdigest(),
            )

    def has_registered_callback_route(self, event: BrokerEvent) -> bool:
        """Return whether this process has an exact pre-registered order route.

        EMT can fan account-level order callbacks to every logged-in client
        session.  A session must not turn an unrelated callback into an error
        for its own route lane.  This is only an ownership filter: callbacks
        for a registered reference continue through ``resolve_outcome`` and
        retain every identity/state check there.  Callbacks with no usable
        reference return ``True`` so they still fail closed downstream.
        """

        if event.event_kind not in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}:
            return True
        identity = event.venue_order_identity
        if identity is None or identity.client_order_ref is None:
            return True
        with self._lock:
            return identity.client_order_ref in self._routes

    def record_outcome_committed(
        self,
        event: BrokerEvent,
        resolution: FastSpiV2OutcomeResolution,
    ) -> None:
        """Advance local state only after the M1 atomic journal receipt succeeds."""

        identity = event.venue_order_identity
        if identity is None or identity.client_order_ref is None:
            raise FastSpiV2NativeOutcomeError("fastspi v2 committed outcome lacks route reference")
        with self._lock:
            state = self._routes.get(identity.client_order_ref)
            if state is None:
                raise FastSpiV2NativeOutcomeError("fastspi v2 committed outcome route disappeared")
            self._validate_event_identity(event, state)
            if resolution.binding != state.binding or resolution.command != state.command:
                raise FastSpiV2NativeOutcomeError("fastspi v2 committed outcome has another route")
            existing = self._committed_by_source.get(event.source_key)
            if existing is not None:
                if existing != resolution:
                    raise FastSpiV2NativeOutcomeError(
                        "fastspi v2 callback replay changed its outcome"
                    )
                return
            resolution.observation.validate_successor(
                state.observation,
                total_quantity=state.command.recovery_seed.order.execution.quantity,
            )
            state.observation = resolution.observation
            if state.identity.venue_order_id is None and identity.venue_order_id is not None:
                state.identity = VenueOrderIdentity(
                    venue_id=state.identity.venue_id,
                    adapter_instance_id=state.identity.adapter_instance_id,
                    adapter_client_scope=state.identity.adapter_client_scope,
                    client_order_ref=state.identity.client_order_ref,
                    venue_order_id=identity.venue_order_id,
                    venue_cancel_id=state.identity.venue_cancel_id,
                )
            self._committed_by_source[event.source_key] = resolution

    def execution_id_for_callback(self, event: BrokerEvent) -> str | None:
        """Return the pre-admitted execution identity for one proven order callback.

        EMT order callbacks are permitted to omit TradeCore's execution ID.  The
        callback directory is the bounded, identity-checked local authority for
        resolving that omission after the callback fact has been persisted.
        """

        if event.event_kind is not BrokerEventKind.ORDER:
            return None
        identity = event.venue_order_identity
        if identity is None or identity.client_order_ref is None:
            return None
        with self._lock:
            state = self._routes.get(identity.client_order_ref)
            if state is None:
                return None
            self._validate_event_identity(event, state)
            return state.command.execution_id

    def _validate_registration(
        self,
        binding: AccountJournalBinding,
        command: PreNativeRouteCommand,
        identity: VenueOrderIdentity,
        observation: BrokerOrderObservationV1 | None,
    ) -> None:
        if (
            binding.account_id != self._config.account_id
            or command.account_id != binding.account_id
        ):
            raise FastSpiV2NativeOutcomeError("fastspi v2 registered route has another account")
        if binding.venue_id != "EMT":
            raise FastSpiV2NativeOutcomeError("fastspi v2 registered route has another venue")
        if (
            identity.venue_id != "EMT"
            or identity.adapter_instance_id != self._config.adapter_instance_id
            or identity.adapter_client_scope != self._config.adapter_client_scope
            or identity.client_order_ref != command.client_order_ref
        ):
            raise FastSpiV2NativeOutcomeError("fastspi v2 registered route identity is invalid")
        if observation is not None:
            observation.validate_total_quantity(command.recovery_seed.order.execution.quantity)
            if (
                identity.venue_order_id is not None
                and observation.venue_order_id is not None
                and identity.venue_order_id != observation.venue_order_id
            ):
                raise FastSpiV2NativeOutcomeError(
                    "fastspi v2 recovered route has another venue order identity"
                )

    def _validate_event_identity(self, event: BrokerEvent, state: _RouteState) -> None:
        identity = event.venue_order_identity
        assert identity is not None
        expected = state.identity
        if (
            event.account_id != self._config.account_id
            or event.venue_id != "EMT"
            or event.adapter_instance_id != self._config.adapter_instance_id
            or identity.adapter_client_scope != self._config.adapter_client_scope
            or identity.client_order_ref != expected.client_order_ref
            or (
                expected.venue_order_id is not None
                and identity.venue_order_id is not None
                and expected.venue_order_id != identity.venue_order_id
            )
            or (event.execution_id is not None and event.execution_id != state.command.execution_id)
        ):
            raise FastSpiV2NativeOutcomeError("fastspi v2 callback identity does not match route")

    def _build_observation(
        self, event: BrokerEvent, state: _RouteState
    ) -> BrokerOrderObservationV1:
        if event.event_kind is BrokerEventKind.ORDER:
            return self._order_observation(event, state)
        return self._trade_observation(event, state)

    @staticmethod
    def _order_observation(event: BrokerEvent, state: _RouteState) -> BrokerOrderObservationV1:
        if event.leaves_qty is None:
            raise FastSpiV2NativeOutcomeError("fastspi v2 order callback lacks leaves quantity")
        total = state.command.recovery_seed.order.execution.quantity
        cumulative = total - event.leaves_qty
        if cumulative < 0:
            raise FastSpiV2NativeOutcomeError("fastspi v2 order callback exceeds route quantity")
        previous = state.observation
        trade_keys = previous.trade_keys if previous is not None else ()
        if cumulative > 0 and not trade_keys:
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 order callback lacks proven cumulative trade history"
            )
        status = event.normalized_summary.get("ledger_state")
        if not isinstance(status, str):
            raise FastSpiV2NativeOutcomeError("fastspi v2 order callback state is not M1 complete")
        state_map = {
            "LIVE": OmsOrderState.WORKING,
            "PART_FILLED": OmsOrderState.PARTIALLY_FILLED,
            "FILLED": OmsOrderState.FILLED,
            "CANCELED": OmsOrderState.CANCELED,
            "REJECTED": OmsOrderState.REJECTED,
        }
        target = state_map.get(status)
        if target is None:
            raise FastSpiV2NativeOutcomeError("fastspi v2 order callback state is not M1 complete")
        if target is OmsOrderState.WORKING and cumulative > 0:
            target = OmsOrderState.PARTIALLY_FILLED
        if target is OmsOrderState.FILLED and cumulative != total:
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 FILLED callback does not conserve quantity"
            )
        identity = event.venue_order_identity
        assert identity is not None
        result = BrokerOrderObservationV1(
            broker_event_id=event.event_id,
            source_key=event.source_key,
            order_state=target,
            cumulative_filled_quantity=cumulative,
            leaves_quantity=event.leaves_qty,
            trade_keys=trade_keys,
            venue_order_id=identity.venue_order_id,
        )
        result.validate_successor(previous, total_quantity=total)
        return result

    @staticmethod
    def _trade_observation(event: BrokerEvent, state: _RouteState) -> BrokerOrderObservationV1:
        previous = state.observation
        if (
            previous is None
            or event.filled_qty is None
            or event.filled_qty <= 0
            or event.trade_key is None
        ):
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 trade callback lacks prior state, quantity or execution key"
            )
        total = state.command.recovery_seed.order.execution.quantity
        if event.trade_key in previous.trade_keys:
            raise FastSpiV2NativeOutcomeError("fastspi v2 trade callback reuses an execution key")
        cumulative = previous.cumulative_filled_quantity + event.filled_qty
        leaves = total - cumulative
        if leaves < 0:
            raise FastSpiV2NativeOutcomeError("fastspi v2 trade callback exceeds route quantity")
        identity = event.venue_order_identity
        assert identity is not None
        result = BrokerOrderObservationV1(
            broker_event_id=event.event_id,
            source_key=event.source_key,
            order_state=OmsOrderState.FILLED if leaves == 0 else OmsOrderState.PARTIALLY_FILLED,
            cumulative_filled_quantity=cumulative,
            leaves_quantity=leaves,
            trade_keys=previous.trade_keys + (event.trade_key,),
            venue_order_id=identity.venue_order_id or previous.venue_order_id,
        )
        result.validate_successor(previous, total_quantity=total)
        return result

    @staticmethod
    def _validate_committed_replay(
        event: BrokerEvent, resolution: FastSpiV2OutcomeResolution
    ) -> None:
        observation = resolution.observation
        if (
            observation.broker_event_id != event.event_id
            or observation.source_key != event.source_key
        ):
            raise FastSpiV2NativeOutcomeError("fastspi v2 callback source replay changed identity")
