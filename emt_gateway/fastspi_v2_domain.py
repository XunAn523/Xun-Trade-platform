"""Post-ring V2 callback normalization into the TradeCore BrokerEvent contract.

This module is intentionally outside C5.  It accepts only the bounded
``CallbackRecord`` materialized after a native slot was popped, and refuses to
invent account routing, execution identity, cancel ownership, or query
correlation when the supplied resolver cannot prove them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    TradingDayEpoch,
    VenueOrderIdentity,
)

from .gateway import CallbackRecord
from .order_ledger import LedgerCallbackKind


class FastSpiV2DomainNormalizationError(RuntimeError):
    """A popped callback cannot be safely normalized to a BrokerEvent."""


@dataclass(frozen=True)
class FastSpiV2CallbackDomainIdentity:
    """Pre-existing route evidence for one callback-owned native identity."""

    venue_order_identity: VenueOrderIdentity
    execution_id: str | None = None
    cancel_request_id: str | None = None


@dataclass(frozen=True)
class FastSpiV2DomainEventConfig:
    """Static, DISARMED-verified scope for one V2 callback normalizer."""

    account_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    trading_day: TradingDayEpoch
    session_epoch: int

    def __post_init__(self) -> None:
        if (
            not self.account_id
            or not self.adapter_instance_id
            or not self.adapter_client_scope
            or self.session_epoch <= 0
        ):
            raise FastSpiV2DomainNormalizationError("fastspi v2 domain scope is invalid")


FastSpiV2CallbackIdentityResolver = Callable[
    [CallbackRecord], FastSpiV2CallbackDomainIdentity | None
]


class FastSpiV2BrokerEventNormalizer:
    """Build source-keyed BrokerEvents only from bounded records and proof."""

    def __init__(
        self,
        *,
        config: FastSpiV2DomainEventConfig,
        identity_resolver: FastSpiV2CallbackIdentityResolver,
    ) -> None:
        self._config = config
        self._identity_resolver = identity_resolver

    def normalize(self, record: CallbackRecord) -> BrokerEvent:
        """Return one normalized event or reject without guessing a domain fact."""

        if record.callback_entry_ns < 0 or record.callback_seq <= 0:
            raise FastSpiV2DomainNormalizationError("fastspi v2 callback record is invalid")
        fact = record.ledger_fact
        if fact is not None:
            identity = self._require_identity(record)
            if fact.kind is LedgerCallbackKind.ORDER_UPDATE:
                return self._order_event(record, identity)
            if fact.kind is LedgerCallbackKind.TRADE:
                return self._trade_event(record, identity)
        if record.callback_name == "OnCancelOrderError":
            return self._cancel_error_event(record, self._require_identity(record))
        if record.callback_name.startswith("OnQuery"):
            return self._query_event(record)
        raise FastSpiV2DomainNormalizationError(
            "fastspi v2 callback has no target-domain normalization contract"
        )

    def _order_event(
        self,
        record: CallbackRecord,
        identity: FastSpiV2CallbackDomainIdentity,
    ) -> BrokerEvent:
        fact = record.ledger_fact
        assert fact is not None and fact.kind is LedgerCallbackKind.ORDER_UPDATE
        if fact.state is None:
            raise FastSpiV2DomainNormalizationError("order callback lacks normalized state")
        source_key = self._source_key("order", fact.source_key)
        return self._event(
            source_key=source_key,
            event_kind=BrokerEventKind.ORDER,
            record=record,
            identity=identity,
            filled_qty=None,
            leaves_qty=fact.leaves_qty,
            normalized_summary={
                "callback_name": record.callback_name,
                "ledger_state": fact.state.value,
                "order_emt_id": str(fact.order_emt_id),
            },
        )

    def _trade_event(
        self,
        record: CallbackRecord,
        identity: FastSpiV2CallbackDomainIdentity,
    ) -> BrokerEvent:
        fact = record.ledger_fact
        assert fact is not None and fact.kind is LedgerCallbackKind.TRADE
        if fact.execution_key is None or fact.qty is None:
            raise FastSpiV2DomainNormalizationError(
                "trade callback lacks execution identity or quantity"
            )
        source_key = self._source_key("trade", fact.source_key)
        return self._event(
            source_key=source_key,
            event_kind=BrokerEventKind.TRADE,
            record=record,
            identity=identity,
            trade_key=fact.execution_key,
            filled_qty=fact.qty,
            normalized_summary={
                "callback_name": record.callback_name,
                "order_emt_id": str(fact.order_emt_id),
                "market": fact.market or "",
            },
        )

    def _cancel_error_event(
        self,
        record: CallbackRecord,
        identity: FastSpiV2CallbackDomainIdentity,
    ) -> BrokerEvent:
        if identity.cancel_request_id is None:
            raise FastSpiV2DomainNormalizationError(
                "cancel callback lacks proven cancel request identity"
            )
        source_key = self._source_key(
            "cancel-error",
            f"{identity.cancel_request_id}:{record.operation_native_id or 0}:{record.callback_seq}",
        )
        return self._event(
            source_key=source_key,
            event_kind=BrokerEventKind.CANCEL_ERROR,
            record=record,
            identity=identity,
            normalized_summary={
                "callback_name": record.callback_name,
                "operation_native_id": str(record.operation_native_id or 0),
            },
        )

    def _query_event(self, record: CallbackRecord) -> BrokerEvent:
        if record.request_id is None or record.request_id <= 0:
            raise FastSpiV2DomainNormalizationError(
                "query callback lacks registered request identity"
            )
        source_key = self._source_key(
            "query",
            ":".join(
                (
                    record.callback_name,
                    str(record.request_id),
                    str(record.query_reference or 0),
                    str(record.callback_seq),
                )
            ),
        )
        return BrokerEvent(
            event_id=source_key,
            schema_version=1,
            source_key_schema_version=1,
            source_key=source_key,
            event_kind=BrokerEventKind.QUERY,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id=self._config.adapter_instance_id,
            account_id=self._config.account_id,
            trading_day=self._config.trading_day,
            received_monotonic_ns=record.callback_entry_ns,
            normalized_summary={
                "callback_name": record.callback_name,
                "request_id": str(record.request_id),
                "terminal": str(bool(record.is_last)).lower(),
                "item_present": str(bool(record.query_item_present)).lower(),
            },
        )

    def _event(
        self,
        *,
        source_key: str,
        event_kind: BrokerEventKind,
        record: CallbackRecord,
        identity: FastSpiV2CallbackDomainIdentity,
        trade_key: str | None = None,
        filled_qty: int | None = None,
        leaves_qty: int | None = None,
        normalized_summary: dict[str, str],
    ) -> BrokerEvent:
        venue_identity = identity.venue_order_identity
        if (
            venue_identity.venue_id != "EMT"
            or venue_identity.adapter_instance_id != self._config.adapter_instance_id
            or venue_identity.adapter_client_scope != self._config.adapter_client_scope
        ):
            raise FastSpiV2DomainNormalizationError(
                "callback identity does not match the configured EMT adapter scope"
            )
        return BrokerEvent(
            event_id=source_key,
            schema_version=1,
            source_key_schema_version=1,
            source_key=source_key,
            event_kind=event_kind,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id=self._config.adapter_instance_id,
            account_id=self._config.account_id,
            trading_day=self._config.trading_day,
            received_monotonic_ns=record.callback_entry_ns,
            venue_order_identity=venue_identity,
            execution_id=identity.execution_id,
            cancel_request_id=identity.cancel_request_id
            if event_kind is BrokerEventKind.CANCEL_ERROR
            else None,
            trade_key=trade_key,
            filled_qty=filled_qty,
            leaves_qty=leaves_qty,
            normalized_summary=normalized_summary,
        )

    def _require_identity(self, record: CallbackRecord) -> FastSpiV2CallbackDomainIdentity:
        identity = self._identity_resolver(record)
        if identity is None:
            raise FastSpiV2DomainNormalizationError("callback has no proven target-domain identity")
        return identity

    def _source_key(self, family: str, value: str) -> str:
        if not value or len(value) > 160:
            raise FastSpiV2DomainNormalizationError("callback source key material is invalid")
        return f"emt-v2-callback:{family}:v1:{self._config.session_epoch}:{value}"
