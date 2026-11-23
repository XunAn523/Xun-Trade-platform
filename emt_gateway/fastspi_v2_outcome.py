"""M1 native-outcome observer for normalized fastspi V2 BrokerEvents.

The callback itself is not a complete OMS state snapshot.  A caller must
therefore resolve it to an already-owned pre-native command and a complete
``BrokerOrderObservationV1`` before this observer can append the existing M1
atomic outcome.  Missing proof fails closed instead of deriving quantities or
route ownership from callback scalars.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable, Protocol

from xuanling_tradecore.domain import BrokerEvent, BrokerEventKind
from xuanling_tradecore.oms import BrokerOrderObservationV1
from xuanling_tradecore.persistence import NativeCallbackOutcomeRequest


class FastSpiV2NativeOutcomeError(RuntimeError):
    """A V2 broker event cannot safely become an M1 native outcome."""


class FastSpiV2OutcomeBindingPort(Protocol):
    @property
    def account_id(self) -> str: ...


class FastSpiV2OutcomeCommandPort(Protocol):
    @property
    def account_id(self) -> str: ...


class FastSpiV2NativeOutcomeJournalPort(Protocol):
    def record_callback_observed(
        self,
        binding: FastSpiV2OutcomeBindingPort,
        command: FastSpiV2OutcomeCommandPort,
        *,
        transition_id: str,
        observation: BrokerOrderObservationV1,
        attributes: dict[str, str] | None = None,
    ) -> object: ...


class FastSpiV2NativeOutcomeBatchJournalPort(FastSpiV2NativeOutcomeJournalPort, Protocol):
    def record_callback_observed_batch(
        self, requests: tuple[NativeCallbackOutcomeRequest, ...]
    ) -> tuple[object, ...]: ...


class FastSpiV2BrokerEventObserverPort(Protocol):
    def on_broker_event(self, event: BrokerEvent) -> None: ...


@dataclass(frozen=True)
class FastSpiV2OutcomeResolution:
    """Complete, pre-existing ownership and state proof for one BrokerEvent."""

    binding: FastSpiV2OutcomeBindingPort
    command: FastSpiV2OutcomeCommandPort
    observation: BrokerOrderObservationV1
    transition_id: str

    def __post_init__(self) -> None:
        if (
            not self.transition_id
            or not self.transition_id.isascii()
            or len(self.transition_id) > 256
        ):
            raise FastSpiV2NativeOutcomeError("fastspi v2 native outcome transition id is invalid")


FastSpiV2OutcomeResolver = Callable[[BrokerEvent], FastSpiV2OutcomeResolution | None]
FastSpiV2OutcomeCommitHook = Callable[[BrokerEvent, FastSpiV2OutcomeResolution], None]


class FastSpiV2NativeOutcomeObserver:
    """Append M1 callback outcomes only after exact source and ownership proof."""

    def __init__(
        self,
        *,
        journal: FastSpiV2NativeOutcomeJournalPort,
        resolve_outcome: FastSpiV2OutcomeResolver,
        outcome_committed: FastSpiV2OutcomeCommitHook | None = None,
        callback_stage_observer: Callable[[BrokerEvent, str, int], None] | None = None,
    ) -> None:
        self._journal = journal
        self._resolve_outcome = resolve_outcome
        self._outcome_committed = outcome_committed
        self._callback_stage_observer = callback_stage_observer

    def on_broker_event(self, event: BrokerEvent) -> None:
        """Persist proven order/trade observations; leave non-state events alone."""

        if event.event_kind not in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}:
            return
        resolution = self._resolve_outcome(event)
        if resolution is None:
            raise FastSpiV2NativeOutcomeError("fastspi v2 BrokerEvent has no proven M1 outcome")
        self._validate(event, resolution)
        self._callback_stage(event, "C5B2_AFTER_ROUTE_STATE_VALIDATION")
        self._journal.record_callback_observed(
            resolution.binding,
            resolution.command,
            transition_id=resolution.transition_id,
            observation=resolution.observation,
            attributes={
                "broker_event_kind": event.event_kind.value,
                "broker_event_source_key": event.source_key,
            },
        )
        if self._outcome_committed is not None:
            self._outcome_committed(event, resolution)

    def on_broker_events(self, events: tuple[BrokerEvent, ...]) -> None:
        """Persist independent callback routes with one bounded Journal pipeline.

        Two observations of the same client order retain single-event order so
        the second observation is validated against the first committed state.
        Distinct routes can safely be sent as independently idempotent FCALLs
        before their individual commit hooks advance local state.
        """

        state_events = tuple(
            event
            for event in events
            if event.event_kind in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}
        )
        if not state_events:
            return
        client_refs = []
        for event in state_events:
            identity = event.venue_order_identity
            client_refs.append(identity.client_order_ref if identity is not None else None)
        if (
            len(state_events) == 1
            or None in client_refs
            or len(client_refs) != len(set(client_refs))
        ):
            for event in state_events:
                self.on_broker_event(event)
            return
        prepared: list[tuple[BrokerEvent, FastSpiV2OutcomeResolution]] = []
        for event in state_events:
            resolution = self._resolve_outcome(event)
            if resolution is None:
                raise FastSpiV2NativeOutcomeError("fastspi v2 BrokerEvent has no proven M1 outcome")
            self._validate(event, resolution)
            self._callback_stage(event, "C5B2_AFTER_ROUTE_STATE_VALIDATION")
            prepared.append((event, resolution))
        record_batch = getattr(self._journal, "record_callback_observed_batch", None)
        if not callable(record_batch):
            for event in state_events:
                self.on_broker_event(event)
            return
        record_batch(
            tuple(
                NativeCallbackOutcomeRequest(
                    binding=resolution.binding,
                    command=resolution.command,
                    transition_id=resolution.transition_id,
                    observation=resolution.observation,
                    attributes={
                        "broker_event_kind": event.event_kind.value,
                        "broker_event_source_key": event.source_key,
                    },
                )
                for event, resolution in prepared
            )
        )
        if self._outcome_committed is not None:
            for event, resolution in prepared:
                self._outcome_committed(event, resolution)

    def _callback_stage(self, event: BrokerEvent, stage: str) -> None:
        if self._callback_stage_observer is None:
            return
        try:
            self._callback_stage_observer(event, stage, monotonic_ns())
        except BaseException:
            # Attribution is observational only; it must not perturb the
            # fail-closed state proof or durable commit.
            return

    @staticmethod
    def _validate(event: BrokerEvent, resolution: FastSpiV2OutcomeResolution) -> None:
        observation = resolution.observation
        if (
            resolution.binding.account_id != event.account_id
            or resolution.command.account_id != event.account_id
            or observation.broker_event_id != event.event_id
            or observation.source_key != event.source_key
        ):
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 M1 outcome identity does not match its BrokerEvent"
            )
        venue_identity = event.venue_order_identity
        if venue_identity is None:
            raise FastSpiV2NativeOutcomeError("fastspi v2 order outcome lacks venue identity")
        if (
            observation.venue_order_id is not None
            and venue_identity.venue_order_id is not None
            and observation.venue_order_id != venue_identity.venue_order_id
        ):
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 M1 outcome venue order identity does not match"
            )
        if (
            event.event_kind is BrokerEventKind.TRADE
            and event.trade_key not in observation.trade_keys
        ):
            raise FastSpiV2NativeOutcomeError(
                "fastspi v2 M1 trade outcome omits the BrokerEvent trade key"
            )


class FastSpiV2BrokerEventObserverMultiplexer:
    """Deliver one normalized event to ordered fail-closed outcome observers."""

    def __init__(self, *observers: FastSpiV2BrokerEventObserverPort) -> None:
        if not observers:
            raise ValueError("fastspi v2 outcome observer multiplexer requires observers")
        self._observers = tuple(observers)

    def on_broker_event(self, event: BrokerEvent) -> None:
        for observer in self._observers:
            observer.on_broker_event(event)
